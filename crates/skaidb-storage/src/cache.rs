//! A bounded in-memory cache for point reads that miss the memtable.
//!
//! Recent writes already live in the [`Memtable`](crate::memtable) (RAM), so a
//! point read only touches disk once a key has been flushed to an SSTable. This
//! cache remembers the resolved latest version (or absence) of such keys so a
//! repeat read is served from RAM without a Bloom probe + block decompress.
//!
//! **Correctness rests on one rule: every write invalidates its key here.** All
//! writes funnel through the engine's `append_buffered`, which calls
//! [`ReadCache::invalidate`]. Because a cached entry is only ever populated from
//! the (immutable) SSTable layer on a memtable miss, and any later write to that
//! key removes the entry, the cache can never hold a stale version — flush and
//! compaction preserve the latest stored value, so untouched entries stay valid.
//!
//! Eviction is FIFO with a fixed capacity; the cache is `Send + Sync` (a single
//! `Mutex`) so it can be shared across reader threads behind the engine.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use crate::hlc::Hlc;
use crate::memtable::VersionValue;

/// A cached point-read result: the latest version of a key, or `None` when the
/// key is absent everywhere below the memtable (a cached negative lookup).
pub type CachedRead = Option<(Hlc, VersionValue)>;

#[derive(Debug)]
pub struct ReadCache {
    capacity: usize,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    map: HashMap<Vec<u8>, CachedRead>,
    /// Insertion order, for FIFO eviction. May contain keys already removed by
    /// `invalidate`; those are skipped (they're no longer in `map`).
    fifo: VecDeque<Vec<u8>>,
}

impl ReadCache {
    /// Create a cache holding at most `capacity` entries (0 disables it).
    pub fn new(capacity: usize) -> ReadCache {
        ReadCache {
            capacity,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Look up a key. `Some(_)` is a cache hit (carrying the cached result, which
    /// may itself be `None` for a known-absent key); the outer `None` is a miss.
    pub fn get(&self, key: &[u8]) -> Option<CachedRead> {
        if self.capacity == 0 {
            return None;
        }
        self.inner.lock().expect("read cache").map.get(key).cloned()
    }

    /// Record the resolved result for `key`.
    pub fn insert(&self, key: &[u8], value: CachedRead) {
        if self.capacity == 0 {
            return;
        }
        let mut guard = self.inner.lock().expect("read cache");
        let inner = &mut *guard;
        if inner.map.insert(key.to_vec(), value).is_none() {
            inner.fifo.push_back(key.to_vec());
        }
        // Evict oldest live entries until within capacity (skip stale fifo keys).
        while inner.map.len() > self.capacity {
            match inner.fifo.pop_front() {
                Some(old) => {
                    inner.map.remove(&old);
                }
                None => break,
            }
        }
        // Trim stale fifo heads so it can't grow without bound under churn.
        while inner.fifo.len() > self.capacity {
            match inner.fifo.front() {
                Some(front) if !inner.map.contains_key(front) => {
                    inner.fifo.pop_front();
                }
                _ => break,
            }
        }
    }

    /// Drop any cached result for `key`. Called on every write to the key.
    pub fn invalidate(&self, key: &[u8]) {
        if self.capacity == 0 {
            return;
        }
        self.inner.lock().expect("read cache").map.remove(key);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().expect("read cache").map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(v: u64) -> CachedRead {
        Some((Hlc::new(v, 0), VersionValue::Put(vec![v as u8])))
    }

    #[test]
    fn hit_miss_and_invalidate() {
        let c = ReadCache::new(8);
        assert!(c.get(b"a").is_none()); // miss
        c.insert(b"a", put(1));
        assert_eq!(c.get(b"a"), Some(put(1))); // hit
        c.invalidate(b"a");
        assert!(c.get(b"a").is_none()); // gone after a write
    }

    #[test]
    fn caches_negative_lookups() {
        let c = ReadCache::new(8);
        c.insert(b"missing", None);
        assert_eq!(c.get(b"missing"), Some(None)); // hit carrying "absent"
    }

    #[test]
    fn fifo_eviction_bounds_size() {
        let c = ReadCache::new(4);
        for i in 0..100u64 {
            c.insert(&i.to_le_bytes(), put(i));
        }
        assert!(c.len() <= 4, "cache must stay within capacity");
        // The most recent insert is still present.
        assert_eq!(c.get(&99u64.to_le_bytes()), Some(put(99)));
    }

    #[test]
    fn invalidate_churn_keeps_fifo_bounded() {
        let c = ReadCache::new(4);
        for i in 0..1000u64 {
            c.insert(&i.to_le_bytes(), put(i));
            c.invalidate(&i.to_le_bytes()); // immediately invalidate
        }
        assert_eq!(c.len(), 0);
        let inner = c.inner.lock().unwrap();
        assert!(inner.fifo.len() <= 4, "fifo must not grow unbounded under churn");
    }

    #[test]
    fn capacity_zero_disables() {
        let c = ReadCache::new(0);
        c.insert(b"a", put(1));
        assert!(c.get(b"a").is_none());
    }
}
