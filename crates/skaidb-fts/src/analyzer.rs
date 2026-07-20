//! Analyzer registry: named token pipelines, resolved from the spec strings
//! written in `CREATE SEARCH INDEX ... WITH (...)`.
//!
//! The analysis surface (docs/SEARCH.md "Analyzers"): the core analyzers,
//! Snowball language analyzers for the usual European set, `folding`
//! (ASCII-folded standard), and parametrized `ngram(min,max)` /
//! `edge_ngram(min,max)`. Synonyms with hot-reload and ICU normalization
//! are later phases.

use std::fmt;

use tantivy::tokenizer::{
    AlphaNumOnlyFilter, AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RawTokenizer,
    RegexTokenizer, RemoveLongFilter, Stemmer, StopWordFilter, TextAnalyzer, Token, TokenStream,
    Tokenizer, WhitespaceTokenizer,
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
    /// A user-composed pipeline: `'<tokenizer> | <filter> | …'` (ES custom
    /// analyzers). E.g. `'unicode | lowercase | stop(english) |
    /// stem(english)'` or `'whitespace | lowercase | ascii_folding'`.
    Custom {
        tokenizer: PipeTokenizer,
        filters: Vec<PipeFilter>,
    },
}

/// The first stage of a custom pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipeTokenizer {
    /// UAX §29 Unicode word split (the `standard` base).
    Unicode,
    Whitespace,
    Keyword,
    /// Character ngrams (`edge = true` → prefix ngrams).
    Ngram { min: usize, max: usize, edge: bool },
    /// Every match of the pattern becomes a token.
    Regex(String),
}

/// A token filter stage of a custom pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipeFilter {
    Lowercase,
    AsciiFolding,
    /// Drop tokens containing non-alphanumeric characters.
    AlphanumOnly,
    RemoveLong(usize),
    /// The built-in stopword list of a language (a language without one is a
    /// no-op stage).
    Stop(Language),
    /// An explicit stopword list.
    Stopwords(Vec<String>),
    Stem(Language),
}

/// Stage cap for custom pipelines — a runaway spec is a config error, not an
/// index build.
const MAX_PIPELINE_FILTERS: usize = 16;

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
        // A custom pipeline keeps the ORIGINAL case (regex patterns and
        // stopwords are payload); stage names are matched case-insensitively
        // inside `parse_pipeline`. `|` inside a `regex(...)` stage is payload,
        // so the pipeline detector splits paren-aware.
        let raw = spec.trim();
        if split_stages(raw).len() > 1 || raw.to_ascii_lowercase().starts_with("regex(") {
            return Self::parse_pipeline(raw);
        }
        let spec = raw.to_ascii_lowercase();
        match spec.as_str() {
            "standard" => return Ok(Analyzer::Standard),
            "folding" => return Ok(Analyzer::Folding),
            "whitespace" => return Ok(Analyzer::Whitespace),
            "keyword" => return Ok(Analyzer::Keyword),
            // A bare `unicode` is the un-filtered UAX §29 tokenizer — only
            // expressible as a (single-stage) pipeline.
            "unicode" => {
                return Ok(Analyzer::Custom {
                    tokenizer: PipeTokenizer::Unicode,
                    filters: Vec::new(),
                })
            }
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
             ngram(min,max), edge_ngram(min,max), a language ({}), or a custom pipeline \
             '<tokenizer> | <filter> | …' — see docs/SEARCH.md)",
            LANGUAGES
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }

    /// Parse a `'<tokenizer> | <filter> | …'` pipeline spec.
    fn parse_pipeline(raw: &str) -> Result<Analyzer, FtsError> {
        let stages = split_stages(raw);
        let mut it = stages.iter();
        let head = it
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| FtsError::Config("empty analyzer pipeline".into()))?;
        let (name, args) = stage_parts(head)?;
        let tokenizer = match name.as_str() {
            "unicode" => no_args(&name, args, PipeTokenizer::Unicode)?,
            "whitespace" => no_args(&name, args, PipeTokenizer::Whitespace)?,
            "keyword" => no_args(&name, args, PipeTokenizer::Keyword)?,
            "ngram" | "edge_ngram" => {
                let (min, max) = ngram_args(&name, args)?;
                PipeTokenizer::Ngram { min, max, edge: name == "edge_ngram" }
            }
            "regex" => {
                let pattern = args
                    .ok_or_else(|| FtsError::Config("regex(<pattern>) needs a pattern".into()))?;
                RegexTokenizer::new(pattern).map_err(|e| {
                    FtsError::Config(format!("bad regex tokenizer pattern '{pattern}': {e}"))
                })?;
                PipeTokenizer::Regex(pattern.to_string())
            }
            other => {
                return Err(FtsError::Config(format!(
                    "unknown pipeline tokenizer '{other}' (expected unicode, whitespace, \
                     keyword, ngram(min,max), edge_ngram(min,max), or regex(pattern))"
                )))
            }
        };
        let mut filters = Vec::new();
        for stage in it {
            if stage.is_empty() {
                return Err(FtsError::Config("empty stage in analyzer pipeline".into()));
            }
            let (name, args) = stage_parts(stage)?;
            filters.push(match name.as_str() {
                "lowercase" => no_args(&name, args, PipeFilter::Lowercase)?,
                "ascii_folding" | "folding" => no_args(&name, args, PipeFilter::AsciiFolding)?,
                "alphanum_only" => no_args(&name, args, PipeFilter::AlphanumOnly)?,
                "remove_long" => {
                    let n: usize = args
                        .and_then(|a| a.trim().parse().ok())
                        .filter(|n| *n >= 1)
                        .ok_or_else(|| {
                            FtsError::Config("remove_long(<max chars>) needs a length >= 1".into())
                        })?;
                    PipeFilter::RemoveLong(n)
                }
                "stop" => PipeFilter::Stop(language_arg(&name, args)?),
                "stem" => PipeFilter::Stem(language_arg(&name, args)?),
                "stopwords" => {
                    let words: Vec<String> = args
                        .map(|a| {
                            a.split(',')
                                .map(|w| w.trim().to_string())
                                .filter(|w| !w.is_empty())
                                .collect()
                        })
                        .unwrap_or_default();
                    if words.is_empty() {
                        return Err(FtsError::Config(
                            "stopwords(w1, w2, …) needs at least one word".into(),
                        ));
                    }
                    PipeFilter::Stopwords(words)
                }
                other => {
                    return Err(FtsError::Config(format!(
                        "unknown pipeline filter '{other}' (expected lowercase, ascii_folding, \
                         alphanum_only, remove_long(n), stop(<language>), stopwords(w1, w2, …), \
                         or stem(<language>))"
                    )))
                }
            });
        }
        if filters.len() > MAX_PIPELINE_FILTERS {
            return Err(FtsError::Config(format!(
                "analyzer pipeline has {} filters (max {MAX_PIPELINE_FILTERS})",
                filters.len()
            )));
        }
        Ok(Analyzer::Custom { tokenizer, filters })
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
            Analyzer::Custom { tokenizer, filters } => {
                let mut b = match tokenizer {
                    PipeTokenizer::Unicode => {
                        TextAnalyzer::builder(UnicodeWordTokenizer::default()).dynamic()
                    }
                    PipeTokenizer::Whitespace => {
                        TextAnalyzer::builder(WhitespaceTokenizer::default()).dynamic()
                    }
                    PipeTokenizer::Keyword => {
                        TextAnalyzer::builder(RawTokenizer::default()).dynamic()
                    }
                    PipeTokenizer::Ngram { min, max, edge } => TextAnalyzer::builder(
                        NgramTokenizer::new(*min, *max, *edge).expect("sizes validated at parse"),
                    )
                    .dynamic(),
                    PipeTokenizer::Regex(p) => TextAnalyzer::builder(
                        RegexTokenizer::new(p).expect("pattern validated at parse"),
                    )
                    .dynamic(),
                };
                for f in filters {
                    b = match f {
                        PipeFilter::Lowercase => b.filter_dynamic(LowerCaser),
                        PipeFilter::AsciiFolding => b.filter_dynamic(AsciiFoldingFilter),
                        PipeFilter::AlphanumOnly => b.filter_dynamic(AlphaNumOnlyFilter),
                        PipeFilter::RemoveLong(n) => {
                            b.filter_dynamic(RemoveLongFilter::limit(*n))
                        }
                        // A language without a built-in stopword list is a
                        // no-op stage (matches the Language analyzer policy).
                        PipeFilter::Stop(lang) => match StopWordFilter::new(*lang) {
                            Some(s) => b.filter_dynamic(s),
                            None => b,
                        },
                        PipeFilter::Stopwords(words) => {
                            b.filter_dynamic(StopWordFilter::remove(words.clone()))
                        }
                        PipeFilter::Stem(lang) => b.filter_dynamic(Stemmer::new(*lang)),
                    };
                }
                b.build()
            }
        }
    }
}

fn bad_ngram(spec: &str) -> FtsError {
    FtsError::Config(format!(
        "'{spec}' must be of the form ngram(min,max) / edge_ngram(min,max)"
    ))
}

/// Split a pipeline spec on `|` at paren depth 0 (a `|` inside `regex(...)`
/// is pattern payload), each stage trimmed.
fn split_stages(spec: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for c in spec.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.push(c);
            }
            '|' if depth == 0 => out.push(std::mem::take(&mut cur).trim().to_string()),
            _ => cur.push(c),
        }
    }
    out.push(cur.trim().to_string());
    out
}

/// `name(args)` → (lowercased name, Some(args)); bare `name` → (name, None).
fn stage_parts(stage: &str) -> Result<(String, Option<&str>), FtsError> {
    match stage.find('(') {
        Some(open) => {
            let inner = stage[open + 1..]
                .strip_suffix(')')
                .ok_or_else(|| FtsError::Config(format!("unbalanced parens in '{stage}'")))?;
            Ok((stage[..open].trim().to_ascii_lowercase(), Some(inner)))
        }
        None => Ok((stage.trim().to_ascii_lowercase(), None)),
    }
}

fn no_args<T>(name: &str, args: Option<&str>, value: T) -> Result<T, FtsError> {
    match args {
        None => Ok(value),
        Some(_) => Err(FtsError::Config(format!("'{name}' takes no arguments"))),
    }
}

fn ngram_args(name: &str, args: Option<&str>) -> Result<(usize, usize), FtsError> {
    let bad = || FtsError::Config(format!("'{name}' must be of the form {name}(min,max)"));
    let args = args.ok_or_else(bad)?;
    let parts: Vec<&str> = args.split(',').map(str::trim).collect();
    let [min, max] = parts.as_slice() else {
        return Err(bad());
    };
    let (min, max) = (
        min.parse::<usize>().map_err(|_| bad())?,
        max.parse::<usize>().map_err(|_| bad())?,
    );
    if min == 0 || min > max || max > MAX_NGRAM {
        return Err(FtsError::Config(format!(
            "ngram sizes must satisfy 1 <= min <= max <= {MAX_NGRAM}, got ({min},{max})"
        )));
    }
    Ok((min, max))
}

fn language_arg(name: &str, args: Option<&str>) -> Result<Language, FtsError> {
    let arg = args
        .map(|a| a.trim().to_ascii_lowercase())
        .ok_or_else(|| FtsError::Config(format!("'{name}(<language>)' needs a language")))?;
    LANGUAGES
        .iter()
        .find(|(n, _)| *n == arg)
        .map(|(_, l)| *l)
        .ok_or_else(|| {
            FtsError::Config(format!(
                "unknown language '{arg}' for '{name}' (expected one of: {})",
                LANGUAGES.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
            ))
        })
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
            // Canonical pipeline form — must re-parse to the same value
            // (serde round-trips through this string).
            Analyzer::Custom { tokenizer, filters } => {
                match tokenizer {
                    PipeTokenizer::Unicode => f.write_str("unicode")?,
                    PipeTokenizer::Whitespace => f.write_str("whitespace")?,
                    PipeTokenizer::Keyword => f.write_str("keyword")?,
                    PipeTokenizer::Ngram { min, max, edge } => {
                        let name = if *edge { "edge_ngram" } else { "ngram" };
                        write!(f, "{name}({min},{max})")?;
                    }
                    PipeTokenizer::Regex(p) => write!(f, "regex({p})")?,
                }
                for pf in filters {
                    f.write_str(" | ")?;
                    match pf {
                        PipeFilter::Lowercase => f.write_str("lowercase")?,
                        PipeFilter::AsciiFolding => f.write_str("ascii_folding")?,
                        PipeFilter::AlphanumOnly => f.write_str("alphanum_only")?,
                        PipeFilter::RemoveLong(n) => write!(f, "remove_long({n})")?,
                        PipeFilter::Stop(lang) => write!(f, "stop({})", lang_name(lang))?,
                        PipeFilter::Stopwords(words) => {
                            write!(f, "stopwords({})", words.join(","))?
                        }
                        PipeFilter::Stem(lang) => write!(f, "stem({})", lang_name(lang))?,
                    }
                }
                Ok(())
            }
        }
    }
}

fn lang_name(lang: &Language) -> &'static str {
    LANGUAGES
        .iter()
        .find(|(_, l)| l == lang)
        .map(|(n, _)| *n)
        .expect("every exposed language is in LANGUAGES")
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

    /// Custom pipelines: parse, canonical-Display round-trip, and the token
    /// streams the composed stages actually produce.
    #[test]
    fn custom_pipeline_parse_and_tokens() {
        // whitespace + lowercase: case-folded but punctuation preserved.
        let a = Analyzer::parse("whitespace | lowercase").unwrap();
        assert_eq!(tokens(&a, "Foo-Bar BAZ"), vec!["foo-bar", "baz"]);
        // unicode + folding + stopwords + stemmer ≈ a custom english.
        let a = Analyzer::parse("unicode | lowercase | stopwords(the,a) | stem(english)").unwrap();
        assert_eq!(tokens(&a, "The running Cafés"), vec!["run", "café"]);
        // regex tokenizer: `|` inside the pattern is payload, not a stage
        // split. (Matching happens before any filter, so it is
        // case-sensitive as written.)
        let a = Analyzer::parse("regex((cat|dog)) | lowercase").unwrap();
        assert_eq!(tokens(&a, "cat chases dog"), vec!["cat", "dog"]);
        // ascii folding as a pipeline stage.
        let a = Analyzer::parse("unicode | lowercase | ascii_folding").unwrap();
        assert_eq!(tokens(&a, "Café"), vec!["cafe"]);
        // Bare unicode = un-filtered UAX §29 (case preserved).
        let a = Analyzer::parse("unicode").unwrap();
        assert_eq!(tokens(&a, "Foo's Bar"), vec!["Foo's", "Bar"]);

        // Display round-trips through parse (the serde path).
        for spec in [
            "whitespace | lowercase",
            "unicode | lowercase | stopwords(the,a) | stem(english)",
            "regex((cat|dog)) | lowercase",
            "unicode",
            "ngram(2,3) | lowercase",
            "unicode | remove_long(64) | stop(english)",
            "unicode | alphanum_only",
        ] {
            let a = Analyzer::parse(spec).unwrap();
            let shown = a.to_string();
            assert_eq!(Analyzer::parse(&shown).unwrap(), a, "round-trip of '{spec}'");
        }

        // Errors: unknown stages, bad args, bad regex, empty stages.
        assert!(Analyzer::parse("unicode | frobnicate").is_err());
        assert!(Analyzer::parse("sideways | lowercase").is_err());
        assert!(Analyzer::parse("unicode | stop(klingon)").is_err());
        assert!(Analyzer::parse("unicode | remove_long(0)").is_err());
        assert!(Analyzer::parse("regex(() | lowercase").is_err());
        assert!(Analyzer::parse("unicode | | lowercase").is_err());
        assert!(Analyzer::parse("unicode | stopwords()").is_err());
        assert!(Analyzer::parse("whitespace(8) | lowercase").is_err());
    }

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
