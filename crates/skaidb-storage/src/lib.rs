//! skaidb storage engine (SPEC §12): an LSM tree.
//!
//! - [`wal`] — a CRC-checked write-ahead log with torn-tail recovery,
//! - [`memtable`] — an ordered, multi-version in-memory table,
//! - [`sstable`] — immutable, key-sorted on-disk tables with a Bloom filter,
//! - [`hlc`] — hybrid logical clocks providing version stamps,
//! - [`engine`] — the LSM façade: WAL + memtable + SSTables, flush, compaction,
//!   and crash recovery.

mod bloom;
mod cache;
pub mod compress;
mod crc;
pub mod crypto;
mod posfile;
pub mod engine;
pub mod hlc;
pub mod memtable;
pub mod sstable;
pub mod wal;

mod error;

pub use cache::CacheStats;
pub use compress::Codec;
pub use engine::{CompactJob, Engine, EngineOptions, EngineStats, FlushJob, KeyStats, DEFAULT_FLUSH_THRESHOLD_BYTES, DEFAULT_SCAN_ROW_BUDGET, DEFAULT_SEARCH_WRITER_HEAP, DEFAULT_STATEMENT_TIMEOUT_SECS};
pub use error::{Result, StorageError};
pub use hlc::{Hlc, HlcClock};
pub use memtable::{Memtable, VersionValue};
pub use sstable::{SsTable, SstEntry};
pub use wal::{Wal, WalCommit, WalOp, WalRecord, WalSync};
