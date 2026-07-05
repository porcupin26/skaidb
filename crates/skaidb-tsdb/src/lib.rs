//! Time-series storage engine (docs/TODO.md — time-series tables, phase 1).
//!
//! A per-table store shaped like the Prometheus TSDB: samples append into an
//! in-memory **head** (per-series Gorilla-compressed chunks) backed by a
//! **WAL**; completed time windows flush into immutable **blocks** on disk;
//! blocks compact into larger tiers and expire wholesale under retention.
//!
//! Phase 1 is the storage core only — SQL surface, label postings, and
//! cluster placement are later phases. Samples are `(labels, timestamp-ms,
//! f64)`; timestamps must be strictly increasing per series (an out-of-order
//! ingest window is phase 4).

mod bitstream;
mod block;
mod chunk;
mod compact;
mod head;
mod store;
mod varenc;
mod wal;

pub use chunk::{decode as decode_chunk, Sample};
pub use head::SealedChunk;
pub use store::{AppendResult, FlushedSeries, Matcher, Tsdb, TsdbOptions, TsdbStats};

/// A series identity: label pairs sorted by key (the caller sorts; the store
/// asserts in debug builds).
pub type Labels = Vec<(String, String)>;

/// Errors surfaced by the time-series store.
#[derive(Debug, thiserror::Error)]
pub enum TsdbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("out-of-order sample: ts {ts} <= last {last} for series")]
    OutOfOrder { ts: i64, last: i64 },
    #[error("series limit reached ({0}); raise max_series or reduce label cardinality")]
    SeriesLimit(usize),
    #[error("invalid argument: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, TsdbError>;
