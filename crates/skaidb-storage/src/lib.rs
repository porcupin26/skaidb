//! skaidb storage engine (SPEC §12): an LSM tree.
//!
//! - [`wal`] — a CRC-checked write-ahead log with torn-tail recovery,
//! - [`memtable`] — an ordered, multi-version in-memory table,
//! - [`sstable`] — immutable, key-sorted on-disk tables with a Bloom filter,
//! - [`hlc`] — hybrid logical clocks providing version stamps,
//! - [`engine`] — the LSM façade: WAL + memtable + SSTables, flush, compaction,
//!   and crash recovery.

mod bloom;
mod crc;
pub mod engine;
pub mod hlc;
pub mod memtable;
pub mod sstable;
pub mod wal;

mod error;

pub use engine::{Engine, EngineOptions, DEFAULT_FLUSH_THRESHOLD_BYTES};
pub use error::{Result, StorageError};
pub use hlc::{Hlc, HlcClock};
pub use memtable::{Memtable, VersionValue};
pub use sstable::{SsTable, SstEntry};
pub use wal::{Wal, WalCommit, WalOp, WalRecord, WalSync};
