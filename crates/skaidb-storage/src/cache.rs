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
//! Eviction is FIFO with a fixed capacity; the cache is `Send + Sync` and
//! **sharded** — keys hash to one of up to 64 independently locked shards, so
//! concurrent readers don't serialize on a single mutex.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::hlc::Hlc;
use crate::memtable::VersionValue;

/// A cached point-read result: the latest version of a key, or `None` when the
/// key is absent everywhere below the memtable (a cached negative lookup).
pub type CachedRead = Option<(Hlc, VersionValue)>;

/// A point-in-time snapshot of read-cache effectiveness, surfaced as metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
}

/// Upper bound on lock shards; small caches use fewer so the total entry
/// bound stays exact. Widened from 16 (PG's buffer-mapping table uses 128
/// partitions for the same reason — this cache's total budget is far larger
/// than the block cache's, so more shards cost little per-shard capacity).
const MAX_SHARDS: usize = 64;

#[derive(Debug)]
pub struct ReadCache {
    /// One FIFO map per shard; empty when the cache is disabled (capacity 0).
    shards: Vec<Mutex<Inner>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Default)]
struct Inner {
    /// This shard's entry budget (the total capacity split across shards).
    capacity: usize,
    /// This shard's byte budget (0 = none) — the entry cap alone is
    /// byte-blind, and multi-KB rows pinned an order of magnitude more RAM
    /// than the entry-derived budget assumed (the witness ramp incident).
    byte_capacity: usize,
    /// Live weight of `map` (keys + payloads + per-entry overhead).
    bytes: usize,
    map: HashMap<Vec<u8>, CachedRead>,
    /// Insertion order, for FIFO eviction. May contain keys already removed by
    /// `invalidate`; those are skipped (they're no longer in `map`).
    fifo: VecDeque<Vec<u8>>,
}

/// Approximate resident weight of one entry: the key is held twice (map +
/// fifo), plus the payload and fixed map/tuple overhead.
fn weight(key: &[u8], value: &CachedRead) -> usize {
    let payload = match value {
        Some((_, VersionValue::Put(bytes))) => bytes.len(),
        _ => 0,
    };
    key.len() * 2 + payload + 80
}

impl ReadCache {
    /// Create a cache holding at most `capacity` entries AND `byte_cap`
    /// resident bytes (0 entries disables the cache; 0 bytes = no byte
    /// ceiling).
    pub fn new(capacity: usize, byte_cap: usize) -> ReadCache {
        /// Floor on a shard's byte budget: the even split assumes keys
        /// spread across shards, and a small byte cap sliced 64 ways
        /// leaves shards too small to hold even one row. Fewer, bigger
        /// shards keep small caches usable (production-sized caps still
        /// get all 64).
        const MIN_SHARD_BYTES: usize = 64 * 1024;
        let mut n_shards = capacity.min(MAX_SHARDS);
        if byte_cap > 0 {
            n_shards = n_shards.min((byte_cap / MIN_SHARD_BYTES).max(1));
        }
        let shards = (0..n_shards)
            .map(|i| {
                // Split the budgets evenly; the first shards absorb the
                // remainders so the totals sum exactly.
                let cap = capacity / n_shards + usize::from(i < capacity % n_shards);
                let bcap = if byte_cap == 0 {
                    0
                } else {
                    (byte_cap / n_shards + usize::from(i < byte_cap % n_shards)).max(1)
                };
                Mutex::new(Inner {
                    capacity: cap,
                    byte_capacity: bcap,
                    ..Inner::default()
                })
            })
            .collect();
        ReadCache {
            shards,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn shard(&self, key: &[u8]) -> &Mutex<Inner> {
        // FNV-1a; cheap and good enough to spread keys across shards.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        &self.shards[(h % self.shards.len() as u64) as usize]
    }

    /// Look up a key. `Some(_)` is a cache hit (carrying the cached result, which
    /// may itself be `None` for a known-absent key); the outer `None` is a miss.
    pub fn get(&self, key: &[u8]) -> Option<CachedRead> {
        if self.shards.is_empty() {
            return None;
        }
        let found = self
            .shard(key)
            .lock()
            .expect("read cache")
            .map
            .get(key)
            .cloned();
        if found.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    /// A snapshot of cumulative hit/miss/eviction counts and the live entry count.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: self
                .shards
                .iter()
                .map(|s| s.lock().expect("read cache").map.len())
                .sum(),
        }
    }

    /// Record the resolved result for `key`.
    pub fn insert(&self, key: &[u8], value: CachedRead) {
        if self.shards.is_empty() {
            return;
        }
        let mut guard = self.shard(key).lock().expect("read cache");
        let inner = &mut *guard;
        inner.bytes = inner.bytes.saturating_add(weight(key, &value));
        match inner.map.insert(key.to_vec(), value) {
            None => inner.fifo.push_back(key.to_vec()),
            Some(old) => {
                inner.bytes = inner.bytes.saturating_sub(weight(key, &old));
            }
        }
        // Evict oldest live entries until within BOTH caps (skip stale fifo
        // keys). The byte cap makes eviction weight-aware: one multi-KB row
        // displaces many small ones instead of hiding behind the entry count.
        while inner.map.len() > inner.capacity
            || (inner.byte_capacity > 0 && inner.bytes > inner.byte_capacity)
        {
            match inner.fifo.pop_front() {
                Some(old) => {
                    if let Some(v) = inner.map.remove(&old) {
                        inner.bytes = inner.bytes.saturating_sub(weight(&old, &v));
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    }
                }
                None => break,
            }
        }
        // Trim stale fifo heads so it can't grow without bound under churn.
        while inner.fifo.len() > inner.capacity {
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
        if self.shards.is_empty() {
            return;
        }
        let mut guard = self.shard(key).lock().expect("read cache");
        let inner = &mut *guard;
        if let Some(v) = inner.map.remove(key) {
            inner.bytes = inner.bytes.saturating_sub(weight(key, &v));
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().expect("read cache").map.len())
            .sum()
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
        let c = ReadCache::new(8, 0);
        assert!(c.get(b"a").is_none()); // miss
        c.insert(b"a", put(1));
        assert_eq!(c.get(b"a"), Some(put(1))); // hit
        c.invalidate(b"a");
        assert!(c.get(b"a").is_none()); // gone after a write
    }

    #[test]
    fn caches_negative_lookups() {
        let c = ReadCache::new(8, 0);
        c.insert(b"missing", None);
        assert_eq!(c.get(b"missing"), Some(None)); // hit carrying "absent"
    }

    #[test]
    fn fifo_eviction_bounds_size() {
        let c = ReadCache::new(4, 0);
        for i in 0..100u64 {
            c.insert(&i.to_le_bytes(), put(i));
        }
        assert!(c.len() <= 4, "cache must stay within capacity");
        // The most recent insert is still present.
        assert_eq!(c.get(&99u64.to_le_bytes()), Some(put(99)));
    }

    #[test]
    fn invalidate_churn_keeps_fifo_bounded() {
        let c = ReadCache::new(4, 0);
        for i in 0..1000u64 {
            c.insert(&i.to_le_bytes(), put(i));
            c.invalidate(&i.to_le_bytes()); // immediately invalidate
        }
        assert_eq!(c.len(), 0);
        let fifo_total: usize = c
            .shards
            .iter()
            .map(|s| s.lock().unwrap().fifo.len())
            .sum();
        assert!(fifo_total <= 4, "fifo must not grow unbounded under churn");
    }

    #[test]
    fn capacity_zero_disables() {
        let c = ReadCache::new(0, 0);
        c.insert(b"a", put(1));
        assert!(c.get(b"a").is_none());
    }

    /// Not a correctness test — a throughput probe for shard-count A/B
    /// (`MAX_SHARDS`), isolating the cache's own lock from everything else
    /// on the read path (memtable check, key formatting, engine dispatch)
    /// that a full-Engine benchmark can't help but include. `#[ignore]`d so
    /// normal `cargo test` runs stay fast; invoke explicitly:
    /// `cargo test --release -p skaidb-storage -- --ignored --nocapture
    /// read_cache_contention_probe`.
    /// The byte cap evicts by WEIGHT: a few multi-KB payloads displace many
    /// small entries, and one oversized payload cannot pin the cache past
    /// its ceiling (the entry cap alone was byte-blind — the witness ramp).
    #[test]
    fn byte_cap_evicts_by_weight() {
        let c = ReadCache::new(100, 4_096);
        let big = vec![0u8; 1_500];
        for i in 0u8..4 {
            c.insert(&[i], Some((Hlc::new(1, 0), VersionValue::Put(big.clone()))));
        }
        let stats = c.stats();
        assert!(stats.entries < 4, "byte cap must evict: {stats:?}");
        assert!(stats.evictions > 0, "{stats:?}");
        // Small entries still fit in numbers the entry budget allows.
        let c = ReadCache::new(100, 64 * 1024);
        for i in 0u8..50 {
            c.insert(&[i], Some((Hlc::new(1, 0), VersionValue::Put(vec![7; 16]))));
        }
        assert_eq!(c.stats().entries, 50);
        // Invalidation returns weight: churning one key must not leak
        // accounted bytes into permanent eviction pressure.
        let c = ReadCache::new(100, 8_192);
        for _ in 0..100 {
            c.insert(b"k", Some((Hlc::new(1, 0), VersionValue::Put(vec![0; 1_000]))));
            c.invalidate(b"k");
        }
        c.insert(b"k2", Some((Hlc::new(1, 0), VersionValue::Put(vec![0; 100]))));
        assert_eq!(c.stats().entries, 1);
    }

    /// See the module doc: run explicitly with `--ignored`.
    #[test]
    #[ignore]
    fn read_cache_contention_probe() {
        let hot_keys: usize = std::env::var("HOT_KEYS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
        let threads: usize = std::env::var("THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(32);
        let ops: usize = std::env::var("OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(2_000_000);

        let cache = std::sync::Arc::new(ReadCache::new(16_384, 0));
        let keys: Vec<Vec<u8>> = (0..hot_keys).map(|i| format!("key{i:08}").into_bytes()).collect();
        for k in &keys {
            cache.insert(k, put(1));
        }

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(threads));
        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let cache = cache.clone();
                let keys = keys.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut x: u64 = 0x9E37_79B9 ^ (t as u64);
                    barrier.wait();
                    for _ in 0..ops {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let k = &keys[(x as usize) % keys.len()];
                        std::hint::black_box(cache.get(k));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let elapsed = start.elapsed();
        let total = (threads * ops) as f64;
        eprintln!(
            "MAX_SHARDS={MAX_SHARDS} threads={threads} hot_keys={hot_keys} ops={ops} \
             total={total:.0} elapsed={:.3}s throughput={:.0} ops/s",
            elapsed.as_secs_f64(),
            total / elapsed.as_secs_f64(),
        );
    }
}
