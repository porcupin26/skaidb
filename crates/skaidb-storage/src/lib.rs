//! skaidb storage engine (SPEC §12): an LSM core.
//!
//! Phase 1 implements the durable, MVCC-versioned front of the LSM:
//! - [`wal`] — a CRC-checked write-ahead log with torn-tail recovery,
//! - [`memtable`] — an ordered, multi-version in-memory table,
//! - [`hlc`] — hybrid logical clocks providing version stamps,
//! - [`engine`] — the façade tying them together with crash recovery.
//!
//! Immutable SSTables and lazy-leveled compaction (the on-disk, durable tiers)
//! build on this foundation in a later phase.

mod crc;
pub mod engine;
pub mod hlc;
pub mod memtable;
pub mod wal;

mod error;

pub use engine::{Engine, DEFAULT_FLUSH_THRESHOLD_BYTES};
pub use error::{Result, StorageError};
pub use hlc::{Hlc, HlcClock};
pub use memtable::{Memtable, VersionValue};
pub use wal::{Wal, WalOp, WalRecord};
