//! Analyzer registry: named token pipelines registered with every index.
//!
//! Phase 1 ships the four core analyzers; the WITH-option registry with
//! per-field analyzers, stopword lists, and more languages is phase 2
//! (docs/FTS_TODO.md §2 "Analysis").

use tantivy::tokenizer::{
    Language, LowerCaser, RawTokenizer, RemoveLongFilter, SimpleTokenizer, Stemmer,
    StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::Index;

use crate::FtsError;

/// Token-length cap, matching Lucene/ES `standard` behavior of dropping
/// absurdly long tokens rather than bloating the term dictionary.
const MAX_TOKEN_LEN: usize = 255;

/// A named analysis pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Analyzer {
    /// Word split + lowercase (the default; ES `standard` minus Unicode
    /// segmentation subtleties).
    Standard,
    /// `standard` + English stopwords + Snowball English stemmer.
    English,
    /// Whitespace split only, case preserved.
    Whitespace,
    /// The whole value as a single token (exact-match field).
    Keyword,
}

impl Analyzer {
    pub fn parse(name: &str) -> Result<Analyzer, FtsError> {
        match name.to_ascii_lowercase().as_str() {
            "standard" => Ok(Analyzer::Standard),
            "english" => Ok(Analyzer::English),
            "whitespace" => Ok(Analyzer::Whitespace),
            "keyword" => Ok(Analyzer::Keyword),
            other => Err(FtsError::Config(format!(
                "unknown analyzer '{other}' (expected standard, english, whitespace, or keyword)"
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Analyzer::Standard => "standard",
            Analyzer::English => "english",
            Analyzer::Whitespace => "whitespace",
            Analyzer::Keyword => "keyword",
        }
    }

    /// The tokenizer name this analyzer registers under. Namespaced so we
    /// never collide with Tantivy's built-ins and their defaults can't
    /// drift under us.
    pub(crate) fn tokenizer_name(&self) -> &'static str {
        match self {
            Analyzer::Standard => "skaidb.standard",
            Analyzer::English => "skaidb.english",
            Analyzer::Whitespace => "skaidb.whitespace",
            Analyzer::Keyword => "skaidb.keyword",
        }
    }

    fn build(&self) -> TextAnalyzer {
        match self {
            Analyzer::Standard => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .filter(LowerCaser)
                .build(),
            Analyzer::English => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .filter(LowerCaser)
                .filter(StopWordFilter::new(Language::English).expect("english stopwords"))
                .filter(Stemmer::new(Language::English))
                .build(),
            Analyzer::Whitespace => TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .build(),
            Analyzer::Keyword => TextAnalyzer::builder(RawTokenizer::default()).build(),
        }
    }
}

/// Register all skaidb analyzers on an index (opened or created). Tokenizer
/// registrations are process-local, not persisted, so this must run on every
/// open.
pub(crate) fn register_all(index: &Index) {
    for analyzer in [
        Analyzer::Standard,
        Analyzer::English,
        Analyzer::Whitespace,
        Analyzer::Keyword,
    ] {
        index
            .tokenizers()
            .register(analyzer.tokenizer_name(), analyzer.build());
    }
}
