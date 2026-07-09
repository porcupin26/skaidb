//! The storage engine façade (SPEC §12) — an LSM tree.
//!
//! Writes go to the [`Wal`] then the active [`Memtable`]. When the memtable
//! grows past its threshold it is flushed to an immutable [`SsTable`] at level 0
//! and the WAL is truncated. Background-style compaction merges tables: level 0
//! is size-tiered (overlapping runs from successive flushes), and deeper levels
//! hold a single merged, non-overlapping run each (lazy leveling, SPEC §12).
//!
//! Reads consult the memtable first, then level-0 tables newest-first, then each
//! deeper level — the first source holding the key wins, since newer sources are
//! consulted first. Each table stores only the latest version per key, so MVCC
//! snapshot history is bounded to the live memtable.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cache::{CacheStats, ReadCache};
use crate::compress::Codec;
use crate::error::{Result, StorageError};
use crate::hlc::{Hlc, HlcClock};
use crate::memtable::{Memtable, VersionValue};
use crate::sstable::{SsTable, SstEntry};
use crate::wal::{Wal, WalCommit, WalOp, WalSync};

/// One key's winning `(key, stamp, version)` as produced by the k-way merge.
type MergeItem = (Vec<u8>, Hlc, VersionValue);
/// A key-ordered source feeding the k-way merge (memtable or one SSTable).
type MergeSource<'a> = Box<dyn Iterator<Item = Result<MergeItem>> + 'a>;

/// A live key/value pair together with its version stamp.
pub type VersionedRow = (Vec<u8>, Vec<u8>, Hlc);

/// A key with its stamp and value, where `None` marks a tombstone (delete).
pub type VersionedTombstoneRow = (Vec<u8>, Hlc, Option<Vec<u8>>);

/// Default memtable size that triggers a flush (SPEC §9.1: 256 MiB).
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;
/// Number of level-0 tables that triggers compaction.
const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
/// Entry capacity of level 1; each deeper level holds 10× more.
const DEFAULT_LEVEL1_CAPACITY: u64 = 1024;

/// Tuning knobs for the engine (mainly to make tests exercise flush/compaction).
#[derive(Debug, Clone, Copy)]
pub struct EngineOptions {
    pub flush_threshold_bytes: usize,
    pub l0_compaction_trigger: usize,
    pub level1_capacity: u64,
    /// Codec for freshly flushed and upper-level SSTables (fast path).
    pub compression: Codec,
    /// Codec for the deepest (cold, write-once) level (high ratio).
    pub bottom_compression: Codec,
    /// Capacity (entries) of the RAM read cache for memtable-miss point reads.
    /// `0` disables it. Recent data is already served from the memtable, so this
    /// only helps reads of keys that have been flushed to SSTables.
    pub read_cache_capacity: usize,
    /// Tantivy writer heap per full-text search index (bytes). Consumed by
    /// the engine layer, not storage — carried here so the one options bag
    /// the server assembles reaches `Database::open`. Sized from
    /// `memory_target` when one is set.
    pub search_writer_heap_bytes: usize,
    /// Byte budget for each time-series table's in-memory head (0 =
    /// unbounded). Consumed by the engine layer like the search heap;
    /// sized from `memory_target` when one is set.
    pub ts_head_max_bytes: u64,
}

/// Default read-cache size (entries). Modest so it can't dominate a small node's
/// RAM; only populated by reads that fall through to SSTables.
pub const DEFAULT_READ_CACHE_CAPACITY: usize = 16_384;

/// Default full-text writer heap per index (peak RSS during a bulk build is
/// ≈ 1.5× the heap — measured in the phase-0 spike).
pub const DEFAULT_SEARCH_WRITER_HEAP: usize = 64 * 1024 * 1024;

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            level1_capacity: DEFAULT_LEVEL1_CAPACITY,
            compression: Codec::Lz4,
            bottom_compression: Codec::Brotli,
            read_cache_capacity: DEFAULT_READ_CACHE_CAPACITY,
            search_writer_heap_bytes: DEFAULT_SEARCH_WRITER_HEAP,
            ts_head_max_bytes: 0,
        }
    }
}

/// A single-node, single-keyspace LSM storage engine.
#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    mem: Memtable,
    clock: HlcClock,
    /// Row time-to-live in ms (`Some` = expiring table). A row is invisible
    /// once its stamp's physical age exceeds this; compaction drops it. A
    /// pure read-visibility rule (the raw stamped data still replicates, so
    /// expiry converges across replicas as each applies the same TTL).
    ttl_ms: Option<u64>,
    opts: EngineOptions,
    /// Level-0 tables, newest first.
    l0: Vec<SsTable>,
    /// Deeper levels: `levels[0]` is L1, `levels[1]` is L2, … each a single run.
    levels: Vec<SsTable>,
    next_seq: u64,
    /// RAM cache for point reads that fall through to SSTables.
    read_cache: ReadCache,
    /// Number of compaction passes completed (cumulative).
    compactions: u64,
    /// Bytes written out by compaction outputs (cumulative).
    compaction_bytes: u64,
    /// Point reads that fell through to the SSTable layer and found nothing —
    /// i.e. Bloom-filtered negative lookups served without touching a data block.
    sst_negative_lookups: AtomicU64,
}

/// A point-in-time snapshot of a single storage engine's internal state,
/// surfaced as Prometheus gauges/counters by the server at scrape time.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    /// Approximate live memtable footprint, bytes.
    pub memtable_bytes: usize,
    /// Versions held in the active memtable (includes superseded versions).
    pub memtable_versions: usize,
    /// Total SSTable count across all levels.
    pub sstable_count: usize,
    /// SSTable count per level; index 0 is L0, index 1 is L1, …
    pub sstables_per_level: Vec<usize>,
    /// On-disk bytes across all SSTables.
    pub disk_bytes: u64,
    /// Live WAL size, bytes.
    pub wal_bytes: u64,
    /// Cumulative WAL fsyncs.
    pub wal_fsyncs: u64,
    /// Cumulative compaction passes.
    pub compactions: u64,
    /// Cumulative bytes written by compaction.
    pub compaction_bytes: u64,
    /// Read-cache effectiveness.
    pub cache: CacheStats,
    /// Bloom-filtered negative point lookups (cumulative).
    pub bloom_negatives: u64,
}

/// Live-key and tombstone counts for an engine (a full merged scan — only
/// computed when per-table metrics are enabled, since it is O(rows)).
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyStats {
    pub live_keys: usize,
    pub tombstones: usize,
}

impl Engine {
    /// Open (creating if needed) an engine rooted at `dir` with defaults.
    pub fn open(dir: impl AsRef<Path>) -> Result<Engine> {
        Engine::open_with_options(dir, EngineOptions::default())
    }

    /// Open with an explicit flush threshold (bytes); other knobs default.
    pub fn open_with(dir: impl AsRef<Path>, flush_threshold_bytes: usize) -> Result<Engine> {
        Engine::open_with_options(
            dir,
            EngineOptions {
                flush_threshold_bytes,
                ..EngineOptions::default()
            },
        )
    }

    /// Open with full options control.
    pub fn open_with_options(dir: impl AsRef<Path>, opts: EngineOptions) -> Result<Engine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join("sst"))?;

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

        let (l0, levels, next_seq) = load_manifest(&dir)?;

        let clock = HlcClock::new();
        if max_hlc > Hlc::MIN {
            clock.observe(max_hlc);
        }

        let read_cache = ReadCache::new(opts.read_cache_capacity);

        Ok(Engine {
            dir,
            wal,
            mem,
            clock,
            ttl_ms: None,
            opts,
            l0,
            levels,
            next_seq,
            read_cache,
            compactions: 0,
            compaction_bytes: 0,
            sst_negative_lookups: AtomicU64::new(0),
        })
    }

    /// Append a record to the WAL and apply it to the memtable, returning the
    /// commit point — **without** fsync. Callers make it durable separately
    /// (immediately, or batched outside a lock for group commit). The value is
    /// encoded into the WAL frame from a borrow and moved into the memtable —
    /// no intermediate copies.
    fn append_buffered(&mut self, key: &[u8], hlc: Hlc, value: VersionValue) -> Result<WalCommit> {
        let wal_value = match &value {
            VersionValue::Put(bytes) => Some(bytes.as_slice()),
            VersionValue::Delete => None,
        };
        let commit = self.wal.append_op(hlc, key, wal_value)?;
        self.mem.insert(key.to_vec(), hlc, value);
        // Every write supersedes any cached SSTable result for this key. This is
        // what keeps the read cache correct across flush/compaction.
        self.read_cache.invalidate(key);
        self.maybe_flush()?;
        Ok(commit)
    }

    /// Write `value` under `key`, returning the version stamp assigned.
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<Hlc> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(key, hlc, VersionValue::Put(value))?;
        self.wal.commit_sync(commit)?;
        Ok(hlc)
    }

    /// Delete `key` (writes a tombstone), returning the version stamp assigned.
    pub fn delete(&mut self, key: &[u8]) -> Result<Hlc> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(key, hlc, VersionValue::Delete)?;
        self.wal.commit_sync(commit)?;
        Ok(hlc)
    }

    /// Like [`Engine::put`] but **without** the fsync: returns the commit
    /// point so a caller writing many rows in one statement can make them all
    /// durable with a single [`WalSync::sync_through`] (group commit) instead
    /// of one fsync per row. Pair with [`Engine::wal_sync_handle`].
    pub fn put_deferred(&mut self, key: &[u8], value: Vec<u8>) -> Result<(Hlc, WalCommit)> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(key, hlc, VersionValue::Put(value))?;
        Ok((hlc, commit))
    }

    /// Deferred-durability [`Engine::delete`]; see [`Engine::put_deferred`].
    pub fn delete_deferred(&mut self, key: &[u8]) -> Result<(Hlc, WalCommit)> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(key, hlc, VersionValue::Delete)?;
        Ok((hlc, commit))
    }

    /// Apply a write at a caller-supplied stamp (replication: a replica stores
    /// the coordinator's stamp). The local clock is advanced past `hlc`.
    pub fn put_with_hlc(&mut self, key: &[u8], value: Vec<u8>, hlc: Hlc) -> Result<()> {
        let commit = self.append_put_buffered(key, value, hlc)?;
        self.wal.commit_sync(commit)
    }

    /// Apply a delete at a caller-supplied stamp (replication).
    pub fn delete_with_hlc(&mut self, key: &[u8], hlc: Hlc) -> Result<()> {
        let commit = self.append_delete_buffered(key, hlc)?;
        self.wal.commit_sync(commit)
    }

    /// Buffered replicated write (no fsync): append + apply, returning the
    /// commit point so the caller can fsync outside its write lock (group
    /// commit). Pair with [`Engine::wal_sync_handle`].
    pub fn append_put_buffered(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        hlc: Hlc,
    ) -> Result<WalCommit> {
        self.clock.observe(hlc);
        self.append_buffered(key, hlc, VersionValue::Put(value))
    }

    /// Buffered replicated delete (no fsync); see [`Engine::append_put_buffered`].
    pub fn append_delete_buffered(&mut self, key: &[u8], hlc: Hlc) -> Result<WalCommit> {
        self.clock.observe(hlc);
        self.append_buffered(key, hlc, VersionValue::Delete)
    }

    /// Durability coordinator handle, to `sync_through` a buffered commit after
    /// releasing the write lock.
    pub fn wal_sync_handle(&self) -> std::sync::Arc<WalSync> {
        self.wal.sync_handle()
    }

    /// Set (or clear) the row TTL. Applied to all read paths immediately;
    /// compaction reclaims expired rows on its next pass.
    pub fn set_ttl(&mut self, ttl_ms: Option<u64>) {
        self.ttl_ms = ttl_ms;
    }

    /// Whether a row stamped at `hlc` has outlived the table's TTL.
    fn is_expired(&self, hlc: Hlc) -> bool {
        match self.ttl_ms {
            Some(ttl) => now_wall_ms().saturating_sub(hlc.physical) > ttl,
            None => false,
        }
    }

    /// Latest committed value for `key`, or `None` if absent, deleted, or
    /// TTL-expired.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.get_versioned(key)? {
            Some((hlc, value)) if !self.is_expired(hlc) => Ok(version_to_value(value)),
            _ => Ok(None),
        }
    }

    /// Latest stored version for `key` (including tombstones) with its stamp,
    /// across memtable and SSTables. Used for last-writer-wins point reads.
    ///
    /// The memtable is authoritative for recently written keys. On a memtable
    /// miss the result is served from (and recorded in) the RAM read cache,
    /// avoiding repeated SSTable Bloom probes + block decompression for hot cold
    /// keys. Writes invalidate the cache, so it never returns a stale version.
    pub fn get_versioned(&self, key: &[u8]) -> Result<Option<(Hlc, VersionValue)>> {
        if let Some((hlc, entry)) = self.mem.get_entry_versioned(key) {
            return Ok(Some((hlc, entry)));
        }
        if let Some(cached) = self.read_cache.get(key) {
            return Ok(cached);
        }
        let mut found = None;
        for sst in self.sstables_newest_first() {
            if let Some((hlc, value)) = sst.get(key)? {
                found = Some((hlc, value));
                break;
            }
        }
        if found.is_none() {
            self.sst_negative_lookups.fetch_add(1, Ordering::Relaxed);
        }
        self.read_cache.insert(key, found.clone());
        Ok(found)
    }

    /// Value for `key` as visible at snapshot `as_of` (MVCC read).
    ///
    /// Full historical versions live only in the memtable; once flushed, only
    /// the latest version survives, so this falls back to the latest value when
    /// its stamp is `<= as_of`.
    pub fn get_as_of(&self, key: &[u8], as_of: Hlc) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.get_as_of(key, as_of) {
            return Ok(Some(v.to_vec()));
        }
        if self.mem.get_entry(key).is_some() {
            // The memtable has a version, but none at or before `as_of`.
            return Ok(None);
        }
        for sst in self.sstables_newest_first() {
            if let Some((hlc, value)) = sst.get(key)? {
                return Ok(if hlc <= as_of {
                    version_to_value(value)
                } else {
                    None
                });
            }
        }
        Ok(None)
    }

    /// Full scan of the latest live key/value pairs across all sources, in key
    /// order.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_iter().collect()
    }

    /// Streaming [`Engine::scan`]: yields rows one at a time via a k-way merge
    /// of the memtable and SSTable block iterators — O(sources × block) memory
    /// instead of materializing the table, and early-stop friendly.
    pub fn scan_iter(&self) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.merged_iter().filter_map(|item| match item {
            Ok((_, hlc, VersionValue::Put(_))) if self.is_expired(hlc) => None,
            Ok((k, _, VersionValue::Put(bytes))) => Some(Ok((k, bytes))),
            Ok((_, _, VersionValue::Delete)) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// Like [`Engine::scan`] but also returns each row's version stamp, so a
    /// coordinator can resolve replicas by last-writer-wins (SPEC §5).
    pub fn scan_versioned(&self) -> Result<Vec<VersionedRow>> {
        self.scan_versioned_iter().collect()
    }

    /// Streaming [`Engine::scan_versioned`] — yields live `(key, value, hlc)`
    /// rows lazily instead of collecting the whole shard. A bulk consumer that
    /// reads the whole table (search-index backfill/rebuild) must use this:
    /// collecting a large shard into a `Vec` first is unbounded and OOM'd a
    /// small node building an index over 100k+ rows.
    pub fn scan_versioned_iter(&self) -> impl Iterator<Item = Result<VersionedRow>> + '_ {
        self.merged_iter().filter_map(|item| match item {
            Ok((_, hlc, VersionValue::Put(_))) if self.is_expired(hlc) => None,
            Ok((k, hlc, VersionValue::Put(bytes))) => Some(Ok((k, bytes, hlc))),
            Ok((_, _, VersionValue::Delete)) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// Like [`Engine::scan_versioned`] but **includes tombstones** (as
    /// `(key, hlc, None)`). A coordinator gathering a table from several replicas
    /// must see deletes to resolve them by last-writer-wins — otherwise a stale
    /// `Put` on one replica could mask a newer delete on another.
    pub fn scan_versioned_with_tombstones(&self) -> Result<Vec<VersionedTombstoneRow>> {
        self.scan_versioned_with_tombstones_iter().collect()
    }

    /// Streaming [`Engine::scan_versioned_with_tombstones`].
    pub fn scan_versioned_with_tombstones_iter(
        &self,
    ) -> impl Iterator<Item = Result<VersionedTombstoneRow>> + '_ {
        self.merged_iter().map(|item| {
            item.map(|(k, hlc, v)| match v {
                VersionValue::Put(bytes) => (k, hlc, Some(bytes)),
                VersionValue::Delete => (k, hlc, None),
            })
        })
    }

    /// One bounded page of the merged versioned view, tombstones included: up
    /// to `limit` rows with key strictly greater than `after`, in key order.
    /// Every source is seeked (memtable BTree range, SSTable block index), so
    /// cost is proportional to the page — the building block for incremental,
    /// memory-bounded anti-entropy over tables of any size.
    pub fn scan_versioned_page(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<VersionedTombstoneRow>> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        // The memtable page is capped at `limit` entries: the merge emits at
        // most `limit` rows, and consuming a source entry requires an emission,
        // so a deeper page could never be reached.
        sources.push(Box::new(
            self.mem.range_latest_page(after, limit).into_iter().map(Ok),
        ));
        for sst in self.sstables_newest_first() {
            let iter = match after {
                Some(a) => sst.iter_from(a),
                None => sst.iter(),
            };
            sources.push(Box::new(iter.map(|r| r.map(|e| (e.key, e.hlc, e.value)))));
        }
        KWayMerge::new(sources)
            .filter(|item| match (item, after) {
                (Ok((k, _, _)), Some(a)) => k.as_slice() > a,
                _ => true,
            })
            .take(limit)
            .map(|item| {
                item.map(|(k, hlc, v)| match v {
                    VersionValue::Put(bytes) => (k, hlc, Some(bytes)),
                    VersionValue::Delete => (k, hlc, None),
                })
            })
            .collect()
    }

    /// Scan only the live keys that start with `prefix`, in key order. Used by
    /// secondary indexes, whose entries are prefixed by the indexed value.
    /// Delegates to [`Engine::scan_range`], so cost is proportional to the
    /// matching key range — not the table.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let end = prefix_upper_bound(prefix);
        self.scan_range(Some(prefix), end.as_deref())
    }

    /// Scan live keys in the half-open byte range `[start, end)`, in key order.
    /// `None` bounds are unbounded. Seeks each source (memtable BTree range +
    /// SSTable block index) so cost is proportional to the range, not the table
    /// — this is what makes index range / `ORDER BY` scans fast on large data.
    pub fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        sources.push(Box::new(self.mem.range_latest(start, end).into_iter().map(Ok)));
        for sst in self.sstables_newest_first() {
            let entries = sst.range(start, end)?;
            sources.push(Box::new(
                entries.into_iter().map(|e| Ok((e.key, e.hlc, e.value))),
            ));
        }
        KWayMerge::new(sources)
            .filter_map(|item| match item {
                Ok((k, _, VersionValue::Put(bytes))) => Some(Ok((k, bytes))),
                Ok((_, _, VersionValue::Delete)) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    /// Stream the latest version per key (newest stamp wins) across the
    /// memtable and all SSTables, in key order.
    fn merged_iter(&self) -> KWayMerge<'_> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        sources.push(Box::new(
            self.mem
                .iter_latest_lazy()
                .map(|(k, hlc, v)| Ok((k.to_vec(), hlc, v.clone()))),
        ));
        for sst in self.sstables_newest_first() {
            sources.push(Box::new(
                sst.iter().map(|r| r.map(|e| (e.key, e.hlc, e.value))),
            ));
        }
        KWayMerge::new(sources)
    }

    /// Force the active memtable to flush to an SSTable (no-op if empty).
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let path = self.next_table_path();
        let codec = self.opts.compression;
        let entries = self.mem.iter_latest_lazy().map(|(key, hlc, value)| {
            Ok(SstEntry {
                key: key.to_vec(),
                hlc,
                value: value.clone(),
            })
        });
        let sst = SsTable::write_stream(&path, entries, self.mem.version_count(), codec)?;
        self.l0.insert(0, sst);

        self.mem = Memtable::new();
        self.wal.truncate()?;
        self.persist_manifest()?;
        self.maybe_compact()?;
        Ok(())
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.mem.approx_bytes() >= self.opts.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Physically drop every key for which `keep` returns false, leaving **no
    /// tombstone**: the retained latest versions are rewritten into one fresh
    /// SSTable and all prior tables + the WAL are discarded, so dropped keys
    /// vanish from every scan and cannot resurrect via compaction. This is the
    /// "cleanup" a node runs to reclaim space for keys it no longer owns after
    /// resharding (unlike `delete`, which writes a replicable tombstone that
    /// would re-enter migration and LWW merges). Returns the number of keys
    /// dropped. Crash-safe: the new table is written and the manifest repointed
    /// before any old file is removed.
    pub fn retain(&mut self, keep: impl Fn(&[u8]) -> bool) -> Result<usize> {
        // Stream the merged view, keeping only survivors in memory.
        let mut dropped = 0usize;
        let mut entries: Vec<SstEntry> = Vec::new();
        for row in self.scan_versioned_with_tombstones_iter() {
            let (key, hlc, value) = row?;
            if !keep(&key) {
                dropped += 1;
                continue;
            }
            entries.push(SstEntry {
                key,
                hlc,
                value: match value {
                    Some(bytes) => VersionValue::Put(bytes),
                    None => VersionValue::Delete,
                },
            });
        }
        if dropped == 0 {
            return Ok(0);
        }

        let old_paths: Vec<PathBuf> = self
            .l0
            .iter()
            .chain(self.levels.iter())
            .map(|t| t.path().to_path_buf())
            .collect();

        // Write survivors into one new table *before* dropping the old ones.
        let new_table = if entries.is_empty() {
            None
        } else {
            let path = self.next_table_path();
            let count = entries.len();
            Some(SsTable::write_stream(
                &path,
                entries.into_iter().map(Ok),
                count,
                self.opts.bottom_compression,
            )?)
        };

        self.l0.clear();
        self.levels.clear();
        if let Some(sst) = new_table {
            self.levels.push(sst);
        }
        self.mem = Memtable::new();
        self.wal.truncate()?;
        self.read_cache = ReadCache::new(self.opts.read_cache_capacity);
        self.persist_manifest()?;
        remove_files(&old_paths);
        Ok(dropped)
    }

    /// Whether the memtable has grown past its flush threshold.
    pub fn needs_flush(&self) -> bool {
        self.mem.approx_bytes() >= self.opts.flush_threshold_bytes
    }

    /// Approximate in-memory footprint of the memtable.
    pub fn memtable_bytes(&self) -> usize {
        self.mem.approx_bytes()
    }

    /// Number of on-disk SSTables across all levels.
    pub fn sstable_count(&self) -> usize {
        self.l0.len() + self.levels.len()
    }

    /// A cheap snapshot of this engine's internal state for metrics. Does **not**
    /// scan rows — use [`Engine::key_stats`] for live-key/tombstone counts.
    pub fn stats(&self) -> EngineStats {
        let mut per_level = Vec::with_capacity(1 + self.levels.len());
        per_level.push(self.l0.len());
        // Each deeper level holds exactly one merged run.
        per_level.extend(std::iter::repeat_n(1, self.levels.len()));
        let disk_bytes = self
            .l0
            .iter()
            .chain(self.levels.iter())
            .map(|t| t.disk_len())
            .sum();
        EngineStats {
            memtable_bytes: self.mem.approx_bytes(),
            memtable_versions: self.mem.version_count(),
            sstable_count: self.sstable_count(),
            sstables_per_level: per_level,
            disk_bytes,
            wal_bytes: self.wal.size_bytes(),
            wal_fsyncs: self.wal.fsync_count(),
            compactions: self.compactions,
            compaction_bytes: self.compaction_bytes,
            cache: self.read_cache.stats(),
            bloom_negatives: self.sst_negative_lookups.load(Ordering::Relaxed),
        }
    }

    /// Live-key and tombstone counts via a full merged scan (O(rows) time,
    /// O(block) memory — the scan streams).
    pub fn key_stats(&self) -> Result<KeyStats> {
        let mut stats = KeyStats::default();
        for item in self.merged_iter() {
            match item?.2 {
                VersionValue::Put(_) => stats.live_keys += 1,
                VersionValue::Delete => stats.tombstones += 1,
            }
        }
        Ok(stats)
    }

    /// Directory backing this engine.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ---- internal ----

    fn sstables_newest_first(&self) -> impl Iterator<Item = &SsTable> {
        self.l0.iter().chain(self.levels.iter())
    }

    /// Pick the codec for a compaction output: the high-ratio bottom codec for
    /// the deepest (cold) level, the fast codec otherwise.
    fn codec_for(&self, deepest: bool) -> Codec {
        if deepest {
            self.opts.bottom_compression
        } else {
            self.opts.compression
        }
    }

    /// Reserve the next SSTable sequence number and return its path.
    fn next_table_path(&mut self) -> PathBuf {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.dir.join("sst").join(format!("{seq:020}.sst"))
    }

    /// Compact when level 0 has accumulated too many tables, cascading deeper.
    fn maybe_compact(&mut self) -> Result<()> {
        if self.l0.len() < self.opts.l0_compaction_trigger {
            return Ok(());
        }

        // Merge all L0 tables and the existing L1 (if any) into a new L1 run.
        let path = self.next_table_path();
        let deepest = self.levels.len() <= 1;
        let codec = self.codec_for(deepest);
        let mut sources: Vec<&SsTable> = self.l0.iter().collect();
        if let Some(l1) = self.levels.first() {
            sources.push(l1);
        }
        let old_paths = collect_paths(&self.l0, self.levels.first());
        let expire_before = self.ttl_ms.map(|ttl| now_wall_ms().saturating_sub(ttl));
        let new_l1 = merge_write(&path, &sources, deepest, codec, expire_before)?;
        self.compactions += 1;
        self.compaction_bytes += new_l1.disk_len();
        self.l0.clear();
        if self.levels.is_empty() {
            self.levels.push(new_l1);
        } else {
            self.levels[0] = new_l1;
        }
        remove_files(&old_paths);

        // Cascade: push a level down when it exceeds its capacity.
        let mut level = 0;
        loop {
            let capacity = self.opts.level1_capacity * 10u64.pow(level as u32);
            if level >= self.levels.len() || self.levels[level].len() <= capacity {
                break;
            }
            let has_next = level + 1 < self.levels.len();
            let deepest = !has_next;
            let path = self.next_table_path();
            let codec = self.codec_for(deepest);
            let mut sources: Vec<&SsTable> = vec![&self.levels[level]];
            if has_next {
                sources.push(&self.levels[level + 1]);
            }
            let mut old_paths = vec![self.levels[level].path().to_path_buf()];
            if has_next {
                old_paths.push(self.levels[level + 1].path().to_path_buf());
            }

            let new_run = merge_write(&path, &sources, deepest, codec, expire_before)?;
            self.compactions += 1;
            self.compaction_bytes += new_run.disk_len();
            if has_next {
                self.levels[level + 1] = new_run;
                self.levels.remove(level);
            } else {
                self.levels.push(new_run);
                self.levels.remove(level);
            }
            remove_files(&old_paths);
            level += 1;
        }

        self.persist_manifest()?;
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let mut lines = String::new();
        for sst in &self.l0 {
            lines.push_str(&format!("0 {}\n", file_name(sst.path())));
        }
        for (i, sst) in self.levels.iter().enumerate() {
            lines.push_str(&format!("{} {}\n", i + 1, file_name(sst.path())));
        }
        let sst_dir = self.dir.join("sst");
        let manifest = sst_dir.join("MANIFEST");
        let tmp = sst_dir.join("MANIFEST.tmp");
        // fsync the tmp file before the rename and the directory after, so a
        // crash can't leave the manifest pointing at unwritten data or lose
        // the rename itself.
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(lines.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &manifest)?;
        std::fs::File::open(&sst_dir)?.sync_all()?;
        Ok(())
    }
}

fn version_to_value(v: VersionValue) -> Option<Vec<u8>> {
    match v {
        VersionValue::Put(bytes) => Some(bytes),
        VersionValue::Delete => None,
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string()
}

/// A k-way merge over key-ordered sources, each yielding unique keys. Sources
/// are ordered newest → oldest; for a key held by several sources the highest
/// stamp wins, and on equal stamps the earlier (newer) source wins — the same
/// resolution the old map-based merge applied. Streams: holds one buffered
/// item per source, never the whole dataset.
struct KWayMerge<'a> {
    iters: Vec<MergeSource<'a>>,
    /// One buffered head per source (`None` = needs refill or exhausted).
    heads: Vec<Option<MergeItem>>,
}

impl<'a> KWayMerge<'a> {
    fn new(iters: Vec<MergeSource<'a>>) -> KWayMerge<'a> {
        let heads = iters.iter().map(|_| None).collect();
        KWayMerge { iters, heads }
    }
}

impl Iterator for KWayMerge<'_> {
    type Item = Result<MergeItem>;

    fn next(&mut self) -> Option<Self::Item> {
        for (iter, head) in self.iters.iter_mut().zip(self.heads.iter_mut()) {
            if head.is_none() {
                match iter.next() {
                    Some(Ok(item)) => *head = Some(item),
                    Some(Err(e)) => return Some(Err(e)),
                    None => {}
                }
            }
        }
        // Winner: smallest key; among equal keys the highest stamp, with the
        // earlier (newer) source breaking stamp ties.
        let mut win: Option<usize> = None;
        for i in 0..self.heads.len() {
            let Some((key, hlc, _)) = self.heads[i].as_ref() else {
                continue;
            };
            win = Some(match win {
                None => i,
                Some(w) => {
                    let (wkey, whlc, _) = self.heads[w].as_ref().expect("winner head");
                    if key < wkey || (key == wkey && hlc > whlc) {
                        i
                    } else {
                        w
                    }
                }
            });
        }
        let item = self.heads[win?].take().expect("winner head");
        // Discard superseded versions of the same key in the other sources.
        for head in &mut self.heads {
            if head.as_ref().is_some_and(|(k, _, _)| *k == item.0) {
                *head = None;
            }
        }
        Some(Ok(item))
    }
}

/// Merge `sources` (ordered newest → oldest) into a new key-sorted table at
/// `path`, keeping the highest-stamped version of each key. When
/// `drop_tombstones` is set (merging into the deepest level), delete markers
/// are discarded. Fully streaming: block-in, block-out.
fn merge_write(
    path: &Path,
    sources: &[&SsTable],
    drop_tombstones: bool,
    codec: Codec,
    expire_before: Option<u64>,
) -> Result<SsTable> {
    let approx: u64 = sources.iter().map(|s| s.len()).sum();
    let iters: Vec<MergeSource<'_>> = sources
        .iter()
        .map(|sst| {
            Box::new(sst.iter().map(|r| r.map(|e| (e.key, e.hlc, e.value)))) as MergeSource<'_>
        })
        .collect();
    let entries = KWayMerge::new(iters).filter_map(move |item| match item {
        Ok((_, _, VersionValue::Delete)) if drop_tombstones => None,
        // TTL reclaim: a row whose stamp predates the cutoff is dropped
        // outright (it is already invisible on every read path).
        Ok((_, hlc, _)) if expire_before.is_some_and(|c| hlc.physical < c) => None,
        Ok((key, hlc, value)) => Some(Ok(SstEntry { key, hlc, value })),
        Err(e) => Some(Err(e)),
    });
    SsTable::write_stream(path, entries, approx as usize, codec)
}

/// Wall-clock milliseconds since the Unix epoch (TTL comparisons).
fn now_wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Smallest byte string greater than every key starting with `prefix`, or
/// `None` (unbounded) when the prefix is empty or all `0xFF`.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(&last) = end.last() {
        if last < 0xFF {
            *end.last_mut().expect("non-empty") = last + 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

fn collect_paths(l0: &[SsTable], l1: Option<&SsTable>) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = l0.iter().map(|s| s.path().to_path_buf()).collect();
    if let Some(t) = l1 {
        paths.push(t.path().to_path_buf());
    }
    paths
}

fn remove_files(paths: &[PathBuf]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}

/// Load the SSTable set described by the manifest. Returns (L0 newest-first,
/// deeper levels, next sequence number).
#[allow(clippy::type_complexity)]
fn load_manifest(dir: &Path) -> Result<(Vec<SsTable>, Vec<SsTable>, u64)> {
    let manifest = dir.join("sst").join("MANIFEST");
    if !manifest.exists() {
        return Ok((Vec::new(), Vec::new(), 0));
    }
    let text = std::fs::read_to_string(&manifest)?;

    let mut l0: Vec<SsTable> = Vec::new();
    let mut leveled: Vec<(usize, SsTable)> = Vec::new();
    let mut max_seq: u64 = 0;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let (Some(level), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let level: usize = level.parse().map_err(|_| StorageError::Corruption {
            offset: 0,
            detail: "bad manifest level",
        })?;
        let path = dir.join("sst").join(name);
        if let Some(seq) = name
            .strip_suffix(".sst")
            .and_then(|s| s.parse::<u64>().ok())
        {
            max_seq = max_seq.max(seq + 1);
        }
        let sst = SsTable::open(&path)?;
        if level == 0 {
            l0.push(sst);
        } else {
            leveled.push((level, sst));
        }
    }

    // Manifest lists L0 newest-first already (we write it that way).
    leveled.sort_by_key(|(lvl, _)| *lvl);
    let levels = leveled.into_iter().map(|(_, sst)| sst).collect();
    Ok((l0, levels, max_seq))
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

    fn small_opts() -> EngineOptions {
        // Tiny thresholds so a handful of writes exercise flush + compaction.
        EngineOptions {
            flush_threshold_bytes: 256,
            l0_compaction_trigger: 3,
            level1_capacity: 8,
            ..Default::default()
        }
    }

    #[test]
    fn put_get_delete() {
        let mut e = Engine::open(tempdir()).unwrap();
        e.put(b"k", b"v".to_vec()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v".to_vec()));
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
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
        assert_eq!(e.get(b"alpha").unwrap(), Some(b"3".to_vec()));
        assert_eq!(e.get(b"beta").unwrap(), None);
    }

    #[test]
    fn snapshot_isolation_reads_old_value() {
        let mut e = Engine::open(tempdir()).unwrap();
        e.put(b"k", b"old".to_vec()).unwrap();
        let snap = e.clock_snapshot();
        e.put(b"k", b"new".to_vec()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(e.get_as_of(b"k", snap).unwrap(), Some(b"old".to_vec()));
    }

    #[test]
    fn flush_creates_sstable_and_reads_still_work() {
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        for i in 0..20u32 {
            e.put(format!("key{i:03}").as_bytes(), vec![i as u8; 32])
                .unwrap();
        }
        assert!(
            e.sstable_count() >= 1,
            "expected at least one flushed table"
        );
        // Reads merge memtable + SSTables.
        assert_eq!(e.get(b"key000").unwrap(), Some(vec![0u8; 32]));
        assert_eq!(e.get(b"key019").unwrap(), Some(vec![19u8; 32]));
    }

    #[test]
    fn read_cache_stays_correct_across_flush_overwrite_delete() {
        // Force flushes so reads fall through to SSTables (where the cache lives)
        // and prove a write is never masked by a stale cached value.
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        e.put(b"k", b"v1".to_vec()).unwrap();
        // Fill enough to flush k out of the memtable into an SSTable.
        for i in 0..20u32 {
            e.put(format!("pad{i:03}").as_bytes(), vec![i as u8; 32]).unwrap();
        }
        e.flush().unwrap();
        // First read populates the cache from the SSTable.
        assert_eq!(e.get(b"k").unwrap(), Some(b"v1".to_vec()));

        // Overwrite, flush again: the memtable no longer holds k, so a correct
        // read must come from the new SSTable — never the cached "v1".
        e.put(b"k", b"v2".to_vec()).unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v2".to_vec()), "stale cache hit!");

        // Delete + flush: read must report absence, not the cached value.
        e.delete(b"k").unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap(), None, "stale cache hid a delete!");

        // Re-create the key and confirm it resurfaces.
        e.put(b"k", b"v3".to_vec()).unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn compaction_reduces_tables_and_preserves_data() {
        let dir = tempdir();
        let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
        for i in 0..60u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        // Overwrite some keys; newest value must win after compaction.
        e.put(b"k0000", vec![9u8; 40]).unwrap();
        e.flush().unwrap();

        let count = e.sstable_count();
        let scanned = e.scan().unwrap();
        assert_eq!(
            scanned.len(),
            60,
            "all distinct keys present after compaction"
        );
        assert_eq!(e.get(b"k0000").unwrap(), Some(vec![9u8; 40]));

        // Survives reopen via the manifest.
        drop(e);
        let e = Engine::open_with_options(&dir, small_opts()).unwrap();
        assert_eq!(e.scan().unwrap().len(), 60);
        assert!(count >= 1);
    }

    #[test]
    fn retain_physically_drops_keys_without_resurrection() {
        let dir = tempdir();
        let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
        for i in 0..40u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![7u8; 40]).unwrap();
        }
        e.flush().unwrap();

        // Keep only even-numbered keys; drop the rest physically.
        let dropped = e
            .retain(|k| {
                let n: u32 = std::str::from_utf8(&k[1..]).unwrap().parse().unwrap();
                n.is_multiple_of(2)
            })
            .unwrap();
        assert_eq!(dropped, 20);

        let live = e.scan().unwrap();
        assert_eq!(live.len(), 20, "only retained keys remain");
        assert_eq!(e.get(b"k0001").unwrap(), None, "dropped key is gone");
        assert_eq!(e.get(b"k0002").unwrap(), Some(vec![7u8; 40]));

        // Dropped keys leave no tombstone and never resurrect across reopen.
        drop(e);
        let e = Engine::open_with_options(&dir, small_opts()).unwrap();
        assert_eq!(e.scan().unwrap().len(), 20);
        assert_eq!(e.get(b"k0001").unwrap(), None);

        // Idempotent: retaining the same predicate again drops nothing.
        let mut e = e;
        let again = e.retain(|_| true).unwrap();
        assert_eq!(again, 0);
    }

    #[test]
    fn deletes_are_dropped_after_compaction() {
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        for i in 0..30u32 {
            e.put(format!("d{i:03}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        e.delete(b"d000").unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"d000").unwrap(), None);
        assert!(!e.scan().unwrap().iter().any(|(k, _)| k == b"d000"));
    }

    impl Engine {
        /// Test helper: a snapshot stamp (exposed only in tests).
        fn clock_snapshot(&self) -> Hlc {
            self.clock.now()
        }
    }
}
