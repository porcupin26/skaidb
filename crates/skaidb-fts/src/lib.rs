//! Full-text search for skaidb.
//!
//! Wraps [Tantivy](https://github.com/quickwit-oss/tantivy) behind an
//! engine-agnostic API: skaidb types in, skaidb types out. No Tantivy types
//! leak past this crate, so the engine and SQL layers stay independent of
//! the search core (see docs/FTS_TODO.md §0–1).
//!
//! The index is derived data over an LSM table. The table's WAL is the
//! translog: puts/deletes are applied here immediately (visible to searches
//! after the next commit) but only made durable by [`SearchIndex::commit`],
//! which persists the max row HLC seen (the [`Watermark`]) atomically with
//! the segment data. After a crash the engine replays table rows newer than
//! the watermark, or rebuilds from scratch if the index is missing/corrupt.

mod analyzer;
mod index;
mod query;

pub use analyzer::Analyzer;
pub use index::{SearchIndex, SearchIndexStats};
pub use query::SearchQuery;

/// Errors surfaced by the search crate. Engine code maps these onto
/// `EngineError`; none of them wrap Tantivy types directly.
#[derive(Debug, thiserror::Error)]
pub enum FtsError {
    /// Bad index configuration or query (user error).
    #[error("{0}")]
    Config(String),
    /// The on-disk index does not match the catalog definition (or is
    /// damaged) and must be rebuilt from the table.
    #[error("search index needs rebuild: {0}")]
    NeedsRebuild(String),
    /// Internal engine failure (I/O, corrupt segment, ...).
    #[error("search engine error: {0}")]
    Engine(String),
}

impl From<tantivy::TantivyError> for FtsError {
    fn from(e: tantivy::TantivyError) -> Self {
        FtsError::Engine(e.to_string())
    }
}

/// Durability watermark: the max row HLC included in the last index commit.
/// Mirrors the engine's `Hlc` without depending on the storage crate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Watermark {
    pub physical: u64,
    pub logical: u32,
}

/// One search result: the row's primary-key bytes and its BM25 score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub key: Vec<u8>,
    pub score: f32,
}

/// Configuration for a search index, derived from the
/// `CREATE SEARCH INDEX ... WITH (...)` declaration.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchIndexConfig {
    /// Dotted paths into the row document to index as text.
    pub fields: Vec<String>,
    /// Analyzer applied to all fields (per-field analyzers are phase 2).
    pub analyzer: Analyzer,
}
