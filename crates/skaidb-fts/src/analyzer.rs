//! Analyzer registry: named token pipelines, resolved from the spec strings
//! written in `CREATE SEARCH INDEX ... WITH (...)`.
//!
//! Phase 2 surface (docs/FTS_TODO.md §2 "Analysis"): the core analyzers,
//! Snowball language analyzers for the usual European set, `folding`
//! (ASCII-folded standard), and parametrized `ngram(min,max)` /
//! `edge_ngram(min,max)`. Synonyms with hot-reload and ICU normalization
//! are later phases.

use std::fmt;

use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter,
    Stemmer, StopWordFilter, TextAnalyzer, Token, TokenStream, Tokenizer, WhitespaceTokenizer,
};
use tantivy::Index;
use unicode_segmentation::{UnicodeSegmentation, UnicodeWordIndices};

use crate::FtsError;

/// Token-length cap, matching Lucene/ES `standard` behavior of dropping
/// absurdly long tokens rather than bloating the term dictionary.
const MAX_TOKEN_LEN: usize = 255;

/// Ngram sizes above this produce pathological index blowup.
const MAX_NGRAM: usize = 32;

/// UAX §29 word tokenizer — the same Unicode segmentation Elasticsearch's
/// `standard` analyzer uses, so `dog's` stays one token and CJK/accented
/// boundaries match. Replaced tantivy's `SimpleTokenizer` (split on every
/// non-alphanumeric) after the phase-3 parity suite traced most of the
/// result-set divergence vs ES to tokenization.
#[derive(Debug, Clone, Default)]
struct UnicodeWordTokenizer {
    token: Token,
}

struct UnicodeWordTokenStream<'a> {
    words: UnicodeWordIndices<'a>,
    token: &'a mut Token,
}

impl Tokenizer for UnicodeWordTokenizer {
    type TokenStream<'a> = UnicodeWordTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> UnicodeWordTokenStream<'a> {
        self.token.reset();
        UnicodeWordTokenStream {
            words: text.unicode_word_indices(),
            token: &mut self.token,
        }
    }
}

impl TokenStream for UnicodeWordTokenStream<'_> {
    fn advance(&mut self) -> bool {
        // `unicode_word_indices` yields only word-like segments (skipping
        // whitespace/punctuation), which is exactly the `standard` contract.
        match self.words.next() {
            Some((offset, word)) => {
                self.token.position = self.token.position.wrapping_add(1);
                self.token.offset_from = offset;
                self.token.offset_to = offset + word.len();
                self.token.text.clear();
                self.token.text.push_str(word);
                true
            }
            None => false,
        }
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

/// A named analysis pipeline. `parse`/`Display` round-trip the spec string
/// stored in the catalog (`'english'`, `'edge_ngram(2,15)'`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analyzer {
    /// Unicode word split (UAX §29, like ES `standard`) + lowercase (the
    /// default).
    Standard,
    /// `standard` + ASCII folding (`café` → `cafe`), for accent-insensitive
    /// matching without stemming.
    Folding,
    /// `standard` + language stopwords (where a list exists) + Snowball
    /// stemmer.
    Language(Language),
    /// Whitespace split only, case preserved.
    Whitespace,
    /// The whole value as a single token (exact-match field).
    Keyword,
    /// Lowercased character ngrams of the given size range (substring
    /// matching).
    Ngram { min: usize, max: usize },
    /// Lowercased prefix ngrams (search-as-you-type).
    EdgeNgram { min: usize, max: usize },
}

/// The Snowball languages exposed as analyzer names, `('name', Language)`.
const LANGUAGES: &[(&str, Language)] = &[
    ("arabic", Language::Arabic),
    ("danish", Language::Danish),
    ("dutch", Language::Dutch),
    ("english", Language::English),
    ("finnish", Language::Finnish),
    ("french", Language::French),
    ("german", Language::German),
    ("greek", Language::Greek),
    ("hungarian", Language::Hungarian),
    ("italian", Language::Italian),
    ("norwegian", Language::Norwegian),
    ("portuguese", Language::Portuguese),
    ("romanian", Language::Romanian),
    ("russian", Language::Russian),
    ("spanish", Language::Spanish),
    ("swedish", Language::Swedish),
    ("tamil", Language::Tamil),
    ("turkish", Language::Turkish),
];

impl Analyzer {
    pub fn parse(spec: &str) -> Result<Analyzer, FtsError> {
        let spec = spec.trim().to_ascii_lowercase();
        match spec.as_str() {
            "standard" => return Ok(Analyzer::Standard),
            "folding" => return Ok(Analyzer::Folding),
            "whitespace" => return Ok(Analyzer::Whitespace),
            "keyword" => return Ok(Analyzer::Keyword),
            _ => {}
        }
        if let Some((_, lang)) = LANGUAGES.iter().find(|(name, _)| *name == spec) {
            return Ok(Analyzer::Language(*lang));
        }
        for (prefix, edge) in [("ngram", false), ("edge_ngram", true)] {
            if let Some(args) = spec
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_prefix('('))
                .and_then(|rest| rest.strip_suffix(')'))
            {
                let parts: Vec<&str> = args.split(',').map(str::trim).collect();
                let (min, max) = match parts.as_slice() {
                    [min, max] => (
                        min.parse::<usize>().map_err(|_| bad_ngram(&spec))?,
                        max.parse::<usize>().map_err(|_| bad_ngram(&spec))?,
                    ),
                    _ => return Err(bad_ngram(&spec)),
                };
                if min == 0 || min > max || max > MAX_NGRAM {
                    return Err(FtsError::Config(format!(
                        "ngram sizes must satisfy 1 <= min <= max <= {MAX_NGRAM}, got ({min},{max})"
                    )));
                }
                return Ok(if edge {
                    Analyzer::EdgeNgram { min, max }
                } else {
                    Analyzer::Ngram { min, max }
                });
            }
        }
        Err(FtsError::Config(format!(
            "unknown analyzer '{spec}' (expected standard, folding, whitespace, keyword, \
             ngram(min,max), edge_ngram(min,max), or a language: {})",
            LANGUAGES
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    /// The tokenizer name this analyzer registers under — namespaced so we
    /// never collide with Tantivy's built-ins, unique per spec so different
    /// parameters coexist in one index. The name is persisted in the index
    /// schema, so **changing a pipeline's output must change its name**:
    /// the schema then mismatches on open and the index rebuilds from the
    /// table instead of silently mixing token streams.
    pub(crate) fn tokenizer_name(&self) -> String {
        match self {
            // `.u1`: the standard-based pipelines switched from simple
            // (split on non-alphanumeric) to UAX §29 Unicode-word
            // tokenization (v0.39, phase-3 ES parity).
            Analyzer::Standard | Analyzer::Folding | Analyzer::Language(_) => {
                format!("skaidb.{self}.u1")
            }
            _ => format!("skaidb.{self}"),
        }
    }

    pub(crate) fn build(&self) -> TextAnalyzer {
        let standard = || {
            TextAnalyzer::builder(UnicodeWordTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .filter(LowerCaser)
                .dynamic()
        };
        match self {
            Analyzer::Standard => standard().build(),
            Analyzer::Folding => standard().filter_dynamic(AsciiFoldingFilter).build(),
            Analyzer::Language(lang) => {
                let mut builder = standard();
                // Not every Snowball language ships a stopword list; stem-only
                // is the correct fallback.
                if let Some(stops) = StopWordFilter::new(*lang) {
                    builder = builder.filter_dynamic(stops);
                }
                builder.filter_dynamic(Stemmer::new(*lang)).build()
            }
            Analyzer::Whitespace => TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .build(),
            Analyzer::Keyword => TextAnalyzer::builder(RawTokenizer::default()).build(),
            Analyzer::Ngram { min, max } => TextAnalyzer::builder(
                NgramTokenizer::new(*min, *max, false).expect("sizes validated at parse"),
            )
            .filter(LowerCaser)
            .build(),
            Analyzer::EdgeNgram { min, max } => TextAnalyzer::builder(
                NgramTokenizer::new(*min, *max, true).expect("sizes validated at parse"),
            )
            .filter(LowerCaser)
            .build(),
        }
    }
}

fn bad_ngram(spec: &str) -> FtsError {
    FtsError::Config(format!(
        "'{spec}' must be of the form ngram(min,max) / edge_ngram(min,max)"
    ))
}

impl fmt::Display for Analyzer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Analyzer::Standard => f.write_str("standard"),
            Analyzer::Folding => f.write_str("folding"),
            Analyzer::Language(lang) => {
                let name = LANGUAGES
                    .iter()
                    .find(|(_, l)| l == lang)
                    .map(|(n, _)| *n)
                    .expect("every exposed language is in LANGUAGES");
                f.write_str(name)
            }
            Analyzer::Whitespace => f.write_str("whitespace"),
            Analyzer::Keyword => f.write_str("keyword"),
            Analyzer::Ngram { min, max } => write!(f, "ngram({min},{max})"),
            Analyzer::EdgeNgram { min, max } => write!(f, "edge_ngram({min},{max})"),
        }
    }
}

impl serde::Serialize for Analyzer {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Analyzer {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let spec = String::deserialize(d)?;
        Analyzer::parse(&spec).map_err(serde::de::Error::custom)
    }
}

/// Register every analyzer an index's fields reference. Tokenizer
/// registrations are process-local, not persisted, so this runs on every
/// open.
pub(crate) fn register(index: &Index, analyzers: impl Iterator<Item = Analyzer>) {
    for analyzer in analyzers {
        index
            .tokenizers()
            .register(&analyzer.tokenizer_name(), analyzer.build());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(analyzer: &Analyzer, text: &str) -> Vec<String> {
        let mut pipeline = analyzer.build();
        let mut stream = pipeline.token_stream(text);
        let mut out = Vec::new();
        while let Some(tok) = stream.next() {
            out.push(tok.text.clone());
        }
        out
    }

    /// Conformance fixtures: the token streams Elasticsearch produces for
    /// its reference sentence with the equivalent analyzer — identical
    /// since the UAX §29 tokenizer landed (`dog's` stays one token, like
    /// ES `standard`).
    #[test]
    fn standard_analyzer_conformance() {
        assert_eq!(
            tokens(
                &Analyzer::Standard,
                "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
            ),
            ["the", "2", "quick", "brown", "foxes", "jumped", "over", "the", "lazy", "dog's",
             "bone"]
        );
    }

    #[test]
    fn english_analyzer_conformance() {
        // ES `english` for the same sentence: stopwords dropped, stemmed,
        // possessive removed by the Snowball stemmer.
        assert_eq!(
            tokens(
                &Analyzer::parse("english").unwrap(),
                "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
            ),
            ["2", "quick", "brown", "fox", "jump", "over", "lazi", "dog", "bone"]
        );
    }

    #[test]
    fn language_analyzers_stem() {
        // French: plural + accent handling via the Snowball stemmer.
        assert_eq!(
            tokens(&Analyzer::parse("french").unwrap(), "les châteaux magnifiques"),
            ["château", "magnif"]
        );
        // German: compound-ish plural stems.
        assert_eq!(
            tokens(&Analyzer::parse("german").unwrap(), "die Häuser und Gärten"),
            ["haus", "gart"]
        );
    }

    #[test]
    fn folding_strips_diacritics() {
        assert_eq!(
            tokens(&Analyzer::Folding, "Crème Brûlée at the café"),
            ["creme", "brulee", "at", "the", "cafe"]
        );
    }

    #[test]
    fn ngram_and_edge_ngram() {
        assert_eq!(
            tokens(&Analyzer::parse("edge_ngram(2,4)").unwrap(), "Rust"),
            ["ru", "rus", "rust"]
        );
        assert_eq!(
            tokens(&Analyzer::parse("ngram(3,3)").unwrap(), "Rust"),
            ["rus", "ust"]
        );
    }

    #[test]
    fn spec_round_trip_and_validation() {
        for spec in ["standard", "folding", "whitespace", "keyword", "english", "portuguese",
                     "ngram(2,4)", "edge_ngram(1,15)"] {
            assert_eq!(Analyzer::parse(spec).unwrap().to_string(), spec);
        }
        assert!(Analyzer::parse("klingon").is_err());
        assert!(Analyzer::parse("ngram(0,3)").is_err());
        assert!(Analyzer::parse("ngram(5,3)").is_err());
        assert!(Analyzer::parse("edge_ngram(1,99)").is_err());
        assert!(Analyzer::parse("ngram(a,b)").is_err());
    }
}
