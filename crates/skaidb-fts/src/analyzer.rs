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
    SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::Index;

use crate::FtsError;

/// Token-length cap, matching Lucene/ES `standard` behavior of dropping
/// absurdly long tokens rather than bloating the term dictionary.
const MAX_TOKEN_LEN: usize = 255;

/// Ngram sizes above this produce pathological index blowup.
const MAX_NGRAM: usize = 32;

/// A named analysis pipeline. `parse`/`Display` round-trip the spec string
/// stored in the catalog (`'english'`, `'edge_ngram(2,15)'`, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analyzer {
    /// Word split + lowercase (the default; ES `standard` minus Unicode
    /// segmentation subtleties — see docs/SEARCH.md).
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
    /// parameters coexist in one index.
    pub(crate) fn tokenizer_name(&self) -> String {
        format!("skaidb.{self}")
    }

    pub(crate) fn build(&self) -> TextAnalyzer {
        let standard = || {
            TextAnalyzer::builder(SimpleTokenizer::default())
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
    /// its reference sentence with the equivalent analyzer. One documented
    /// divergence — ES `standard` uses Unicode word segmentation and keeps
    /// `dog's` whole; our simple tokenizer splits on the apostrophe (noted
    /// in docs/SEARCH.md).
    #[test]
    fn standard_analyzer_conformance() {
        assert_eq!(
            tokens(
                &Analyzer::Standard,
                "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
            ),
            ["the", "2", "quick", "brown", "foxes", "jumped", "over", "the", "lazy", "dog", "s",
             "bone"]
        );
    }

    #[test]
    fn english_analyzer_conformance() {
        // ES `english` for the same sentence: stopwords dropped, stemmed.
        assert_eq!(
            tokens(
                &Analyzer::parse("english").unwrap(),
                "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
            ),
            ["2", "quick", "brown", "fox", "jump", "over", "lazi", "dog", "s", "bone"]
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
