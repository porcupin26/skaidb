//! The storage engine façade (SPEC §12).
//!
//! Ties together the [`Wal`] (durability), the [`Memtable`] (ordered in-memory
//! state with MVCC), and an [`HlcClock`] (version stamps). Writes are logged
//! before being applied; on open, the WAL is replayed to reconstruct the
//! memtable and the clock is advanced past every recovered stamp.
//!
//! SSTable flush and compaction are deliberately out of scope for this phase —
//! see [`Engine::needs_flush`]. Until they land, durability rests entirely on
//! the WAL, and restart recovery replays it in full.

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::hlc::{Hlc, HlcClock};
use crate::memtable::{Memtable, VersionValue};
use crate::wal::{Wal, WalOp, WalRecord};

/// Default memtable size that signals a flush is due (SPEC §9.1: 256 MiB).
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;

/// A single-node, single-keyspace storage engine.
#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    mem: Memtable,
    clock: HlcClock,
    flush_threshold_bytes: usize,
}

impl Engine {
    /// Open (creating if needed) an engine rooted at `dir`, replaying its WAL.
    pub fn open(dir: impl AsRef<Path>) -> Result<Engine> {
        Engine::open_with(dir, DEFAULT_FLUSH_THRESHOLD_BYTES)
    }

    /// Open with an explicit flush threshold (bytes). Useful in tests.
    pub fn open_with(dir: impl AsRef<Path>, flush_threshold_bytes: usize) -> Result<Engine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let (wal, records) = Wal::open(dir.join("wal.log"))?;

        let mut mem = Memtable::new();
        let mut max_hlc = Hlc::MIN;
        for rec in records {
            max_hlc = max_hlc.max(rec.hlc);
            let value = match rec.op {
                WalOp::Put(bytes) => VersionValue::Put(bytes),
                WalOp::Delete => VersionValue::Delete,
            };
            mem.insert(rec.key, rec.hlc, value);
        }

        let clock = HlcClock::new();
        if max_hlc > Hlc::MIN {
            // Ensure freshly-issued stamps are strictly greater than recovered ones.
            clock.observe(max_hlc);
        }

        Ok(Engine {
            dir,
            wal,
            mem,
            clock,
            flush_threshold_bytes,
        })
    }

    /// Write `value` under `key`, returning the version stamp assigned.
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<Hlc> {
        let hlc = self.clock.now();
        self.wal.append(&WalRecord {
            hlc,
            key: key.to_vec(),
            op: WalOp::Put(value.clone()),
        })?;
        self.mem.insert(key.to_vec(), hlc, VersionValue::Put(value));
        Ok(hlc)
    }

    /// Delete `key` (writes a tombstone), returning the version stamp assigned.
    pub fn delete(&mut self, key: &[u8]) -> Result<Hlc> {
        let hlc = self.clock.now();
        self.wal.append(&WalRecord {
            hlc,
            key: key.to_vec(),
            op: WalOp::Delete,
        })?;
        self.mem.insert(key.to_vec(), hlc, VersionValue::Delete);
        Ok(hlc)
    }

    /// Latest committed value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.mem.get(key).map(|b| b.to_vec())
    }

    /// Value for `key` as visible at snapshot `as_of` (MVCC read).
    pub fn get_as_of(&self, key: &[u8], as_of: Hlc) -> Option<Vec<u8>> {
        self.mem.get_as_of(key, as_of).map(|b| b.to_vec())
    }

    /// Take a read snapshot: any write already applied is visible at or before
    /// the returned stamp, and writes made afterward are not.
    pub fn snapshot(&self) -> Hlc {
        self.clock.now()
    }

    /// Whether the memtable has grown past its flush threshold. Flushing to an
    /// SSTable is implemented in a later phase; for now this only reports the
    /// condition so callers and tests can observe back-pressure.
    pub fn needs_flush(&self) -> bool {
        self.mem.approx_bytes() >= self.flush_threshold_bytes
    }

    /// Approximate in-memory footprint of the memtable.
    pub fn memtable_bytes(&self) -> usize {
        self.mem.approx_bytes()
    }

    /// Directory backing this engine.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tempdir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-engine-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn put_get_delete() {
        let dir = tempdir();
        let mut e = Engine::open(&dir).unwrap();
        e.put(b"k", b"v".to_vec()).unwrap();
        assert_eq!(e.get(b"k"), Some(b"v".to_vec()));
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k"), None);
    }

    #[test]
    fn durability_across_reopen() {
        let dir = tempdir();
        {
            let mut e = Engine::open(&dir).unwrap();
            e.put(b"alpha", b"1".to_vec()).unwrap();
            e.put(b"beta", b"2".to_vec()).unwrap();
            e.put(b"alpha", b"3".to_vec()).unwrap();
            e.delete(b"beta").unwrap();
        }
        let e = Engine::open(&dir).unwrap();
        assert_eq!(e.get(b"alpha"), Some(b"3".to_vec()));
        assert_eq!(e.get(b"beta"), None);
    }

    #[test]
    fn snapshot_isolation_reads_old_value() {
        let dir = tempdir();
        let mut e = Engine::open(&dir).unwrap();
        e.put(b"k", b"old".to_vec()).unwrap();
        let snap = e.snapshot();
        e.put(b"k", b"new".to_vec()).unwrap();
        assert_eq!(e.get(b"k"), Some(b"new".to_vec()));
        assert_eq!(e.get_as_of(b"k", snap), Some(b"old".to_vec()));
    }

    #[test]
    fn clock_advances_past_recovered_writes() {
        let dir = tempdir();
        let first = {
            let mut e = Engine::open(&dir).unwrap();
            e.put(b"k", b"v".to_vec()).unwrap()
        };
        let mut e = Engine::open(&dir).unwrap();
        let second = e.put(b"k", b"v2".to_vec()).unwrap();
        assert!(
            second > first,
            "stamp {second:?} must exceed recovered {first:?}"
        );
    }

    #[test]
    fn flush_threshold_reports_pressure() {
        let dir = tempdir();
        let mut e = Engine::open_with(&dir, 1024).unwrap();
        assert!(!e.needs_flush());
        for i in 0..100u32 {
            e.put(format!("key{i}").as_bytes(), vec![0u8; 64]).unwrap();
        }
        assert!(e.needs_flush());
    }
}
