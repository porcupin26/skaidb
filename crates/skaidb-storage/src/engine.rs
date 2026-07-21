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
use std::sync::Arc;

use crate::cache::{CacheStats, ReadCache};
use crate::compress::Codec;
use crate::error::{Result, StorageError};
use crate::hlc::{Hlc, HlcClock};
use crate::memtable::{Memtable, VersionValue};
use crate::sstable::{SsTable, SstEntry};
use crate::wal::{Wal, WalCommit, WalOp, WalSync};
use crate::crypto::Kek;

/// One key's winning `(key, stamp, version)` as produced by the k-way merge.
type MergeItem = (Vec<u8>, Hlc, VersionValue);
/// A key-ordered source feeding the k-way merge (memtable or one SSTable).
type MergeSource<'a> = Box<dyn Iterator<Item = Result<MergeItem>> + 'a>;

/// A live key/value pair together with its version stamp.
pub type VersionedRow = (Vec<u8>, Vec<u8>, Hlc);

/// A key with its stamp and value, where `None` marks a tombstone (delete).
pub type VersionedTombstoneRow = (Vec<u8>, Hlc, Option<Vec<u8>>);

/// Default memtable size that triggers a flush (SPEC §9.1: 256 MiB).
// 32 MB, not 256: the flush brotli-compresses the whole memtable while the
// caller holds the engine write lock, so threshold size IS stall length.
// 256 MB of large JSON rows stalled writes for multiple seconds per flush —
// enough to dent write quorum and time out interactive reads during bulk
// loads (measured 2026-07-12). Smaller memtables flush sub-second; leveled
// compaction absorbs the extra file count. The real fix (flush outside the
// lock via immutable-memtable handoff) is tracked in #75.
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 32 * 1024 * 1024;
/// Number of level-0 tables that triggers compaction.
const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
/// Entry capacity of level 1; each deeper level holds 10× more.
const DEFAULT_LEVEL1_CAPACITY: u64 = 1024;

/// Tuning knobs for the engine (mainly to make tests exercise flush/compaction).
#[derive(Debug, Clone)]
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
    /// Memory (ephemeral) engine: unlinked WAL with no fsync, never flushes
    /// to SSTables, empty on every open. For short-lived bounded data
    /// (stats, caches) where restart loss is fine — pair with a table TTL.
    pub ephemeral: bool,
    /// At-rest encryption key (from a keyfile). `Some` = present, so encrypted
    /// files can be opened; new files are encrypted only when `at_rest_enabled`.
    pub kek: Option<Kek>,
    /// Encrypt newly written WAL/SSTable files. Requires `kek`. Existing
    /// plaintext files stay readable (mixed migration).
    pub at_rest_enabled: bool,
    /// Per-statement scan budget: the maximum rows a single statement may
    /// examine (decode + filter) across all its gathers before it errors.
    /// Guards the node against filters that match (almost) nothing walking
    /// entire large tables per query — LIMIT bounds output, not scan work.
    /// `0` disables. Consumed by the engine layer via the scan meter.
    pub scan_row_budget: usize,
    /// Per-statement byte budget: the maximum bytes a single statement may
    /// MATERIALIZE into a result set (retained rows, across every gather)
    /// before it errors. `scan_row_budget` bounds rows *examined*; this bounds
    /// *memory held* — a scan under the row budget can still materialize
    /// gigabytes of multi-KB rows on the coordinator. `0` disables. Consumed
    /// by the engine layer via the scan meter.
    pub scan_byte_budget: usize,
    /// Defer search-index catch-up/rebuild at open to a background worker
    /// (the server sets this; a full FTS rebuild blocked node startup for
    /// ~15 minutes on a large table). Single-node/Session keeps the inline
    /// behavior so an opened database is immediately fully queryable.
    pub defer_search_startup: bool,
    /// Wall-clock ceiling per statement in seconds; past it the statement
    /// errors at its next scan-meter check. Kills zombie queries whose
    /// client has long since timed out and disconnected (the server used
    /// to keep executing them to completion). `0` disables.
    pub statement_timeout_secs: u64,
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
/// Default per-statement scan budget (rows examined). Generous enough for a
/// full sweep of the largest production table, small enough that a
/// runaway filter cannot examine the table many times over per statement.
pub const DEFAULT_SCAN_ROW_BUDGET: usize = 250_000;
/// Default per-statement byte budget (bytes materialized into a result set).
/// Bounds coordinator memory when a within-row-budget scan retains many
/// multi-KB rows; 256 MB clears any legitimate paged result while capping the
/// unbounded gather that OOM-killed 4 GB nodes.
pub const DEFAULT_SCAN_BYTE_BUDGET: usize = 256 * 1024 * 1024;
/// Default per-statement wall-clock ceiling (seconds).
pub const DEFAULT_STATEMENT_TIMEOUT_SECS: u64 = 120;

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            level1_capacity: DEFAULT_LEVEL1_CAPACITY,
            compression: Codec::Lz4,
            // Lz4, not Brotli: compaction compresses the whole deepest
            // level under the engine write lock, and brotli's ~10 MB/s made
            // every L1 rewrite a tens-of-seconds write stall (profiled live
            // 2026-07-12: the hot thread in BrotliCreateBackwardReferences
            // while quorum dented). Lz4 rewrites the same level in ~2s for
            // ~2x the disk. Revisit when compaction moves off the write
            // lock (#75) — existing brotli blocks stay readable either way
            // (per-block codec byte).
            bottom_compression: Codec::Lz4,
            read_cache_capacity: DEFAULT_READ_CACHE_CAPACITY,
            ephemeral: false,
            kek: None,
            at_rest_enabled: false,
            defer_search_startup: false,
            scan_row_budget: DEFAULT_SCAN_ROW_BUDGET,
            scan_byte_budget: DEFAULT_SCAN_BYTE_BUDGET,
            statement_timeout_secs: DEFAULT_STATEMENT_TIMEOUT_SECS,
            search_writer_heap_bytes: DEFAULT_SEARCH_WRITER_HEAP,
            ts_head_max_bytes: 0,
        }
    }
}

/// A memtable sealed for background flush: immutable, still serving reads
/// (newer than every SSTable, older than the active memtable), backed by its
/// own sealed WAL segment until the flush lands.
#[derive(Debug)]
pub struct FrozenMem {
    mem: Arc<Memtable>,
    /// Sealed WAL segment holding exactly this memtable's records.
    wal_seq: u64,
    /// Newest stamp inside (drives the maintenance truncation gate).
    max_hlc: Option<Hlc>,
    /// A flush job for this memtable is in flight.
    building: bool,
}

/// A background flush work order: build the SSTable OUTSIDE the engine lock
/// from the immutable frozen memtable, then install under a brief lock.
#[derive(Debug)]
pub struct FlushJob {
    pub mem: Arc<Memtable>,
    pub path: PathBuf,
    pub codec: Codec,
    /// Which frozen entry this belongs to (its WAL segment number).
    pub wal_seq: u64,
    /// At-rest key (for encrypting the output when `encrypt_new`).
    pub kek: Option<Kek>,
    pub encrypt_new: bool,
}

/// A background compaction work order: inputs are pinned by path and
/// re-opened read-only by the builder; the install validates them by path
/// (a vanished input means an overlapping compaction already ran).
#[derive(Debug)]
pub struct CompactJob {
    pub inputs: Vec<PathBuf>,
    pub output: PathBuf,
    pub deepest: bool,
    pub codec: Codec,
    pub expire_before: Option<u64>,
    /// Deepest-level tombstone drop cutoff (wall ms): a delete marker
    /// stamped at-or-after this instant SURVIVES the merge. Cutoff =
    /// now − the engine's tombstone retention; retention 0 (default) →
    /// cutoff = now → every existing tombstone drops, the historical
    /// behavior. Retention exists for witness-bearing deployments: a
    /// tombstone purged before a witness pulls it resurrects the deleted
    /// row on the backup forever.
    pub tombstone_keep_after: u64,
    /// At-rest key: opens (possibly encrypted) inputs, and encrypts the output
    /// when `encrypt_new`.
    pub kek: Option<Kek>,
    pub encrypt_new: bool,
}

/// A single-node, single-keyspace LSM storage engine.
#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    mem: Memtable,
    /// Sealed memtables awaiting background flush, oldest first.
    frozen: Vec<FrozenMem>,
    /// Next WAL segment number for a freeze.
    next_wal_seq: u64,
    /// Monotonic count of mutations applied this process lifetime (every
    /// `append_buffered`, plus `retain` rewrites). NOT persisted and NOT
    /// advanced by WAL replay at open — it is a session-local change stamp:
    /// two reads of [`Engine::write_seq`] bracketing an operation return the
    /// same value iff the data could not have changed in between. Used to
    /// validate RAM-only caches derived from a scan (`DESCRIBE … FULL EXACT`'s
    /// field registry), which reset with the process too.
    write_seq: u64,
    clock: HlcClock,
    /// Row time-to-live in ms (`Some` = expiring table). A row is invisible
    /// once its stamp's physical age exceeds this; compaction drops it. A
    /// pure read-visibility rule (the raw stamped data still replicates, so
    /// expiry converges across replicas as each applies the same TTL).
    ttl_ms: Option<u64>,
    /// Deepest-level tombstone retention (wall ms, default 0 = drop
    /// immediately as always): markers younger than this survive
    /// compaction so a witness can still pull the delete. Pushed
    /// periodically by the server from the witness registry.
    tombstone_retention_ms: u64,
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
    /// Deferred-maintenance watermark: every write stamped `<=` this has had
    /// its secondary-index/vector/search maintenance applied. Flush must not
    /// truncate a WAL still holding writes past it — after a crash they are
    /// the only replay source for the deferred half. `Hlc::MAX` (the default)
    /// means no deferred consumers: truncate freely.
    maint_watermark: Hlc,
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

        let (wal, records) = if opts.ephemeral {
            // Memory table: fresh unlinked WAL, and any stale on-disk state
            // from a previous life as a persistent table is discarded.
            let _ = std::fs::remove_file(dir.join("wal.log"));
            let _ = std::fs::remove_dir_all(dir.join("sst"));
            std::fs::create_dir_all(dir.join("sst"))?;
            (Wal::open_ephemeral(dir.join("wal.ephemeral"))?, Vec::new())
        } else {
            // Grow-ahead chunk for the WAL file (see WAL_PREALLOC_CHUNK_BYTES
            // for the fsync-cost rationale). Capped by flush_threshold_bytes
            // so tests with tiny thresholds don't reserve more space than the
            // whole segment will ever hold.
            let chunk =
                (opts.flush_threshold_bytes as u64).min(crate::wal::WAL_PREALLOC_CHUNK_BYTES);
            Wal::open(dir.join("wal.log"), chunk, opts.kek.as_ref(), opts.at_rest_enabled)?
        };
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

        let (l0, levels, next_seq) = if opts.ephemeral {
            (Vec::new(), Vec::new(), 0)
        } else {
            load_manifest(&dir, opts.kek.as_ref())?
        };

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
            tombstone_retention_ms: 0,
            opts,
            l0,
            levels,
            next_seq,
            read_cache,
            compactions: 0,
            compaction_bytes: 0,
            sst_negative_lookups: AtomicU64::new(0),
            maint_watermark: Hlc::MAX,
            frozen: Vec::new(),
            next_wal_seq: 1,
            write_seq: 0,
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
        self.write_seq += 1;
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

    /// Session-local change stamp: unchanged between two reads iff no mutation
    /// was applied in between (see the field doc). Resets to 0 at open.
    pub fn write_seq(&self) -> u64 {
        self.write_seq
    }

    /// Whether a row TTL is set — visibility then changes with wall time even
    /// without writes, so scan-derived caches must not be trusted.
    pub fn has_ttl(&self) -> bool {
        self.ttl_ms.is_some()
    }

    /// Set (or clear) the row TTL. Applied to all read paths immediately;
    /// compaction reclaims expired rows on its next pass.
    pub fn set_ttl(&mut self, ttl_ms: Option<u64>) {
        self.ttl_ms = ttl_ms;
    }

    /// Set the deepest-level tombstone retention window (see the field).
    pub fn set_tombstone_retention_ms(&mut self, ms: u64) {
        self.tombstone_retention_ms = ms;
    }

    /// Whether a row stamped at `hlc` has outlived the table's TTL.
    fn is_expired(&self, hlc: Hlc) -> bool {
        row_expired(self.ttl_ms, hlc)
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
        for f in self.frozen.iter().rev() {
            if let Some((hlc, entry)) = f.mem.get_entry_versioned(key) {
                return Ok(Some((hlc, entry)));
            }
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
        for f in self.frozen.iter().rev() {
            if let Some(v) = f.mem.get_as_of(key, as_of) {
                return Ok(Some(v.to_vec()));
            }
            if f.mem.get_entry(key).is_some() {
                return Ok(None);
            }
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
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(
                f.mem.range_latest_page(after, limit).into_iter().map(Ok),
            ));
        }
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

    /// Value-free [`Engine::scan_versioned_page`]: one bounded page of
    /// `(key, hlc, is_put)` stamps, tombstones included, in key order. SSTable
    /// sources read the stamps sidecar where present, so a whole-table pass
    /// (anti-entropy digests) never decompresses value bytes — the dominant
    /// cost of the versioned page scan on wide rows.
    pub fn scan_stamps_page(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Hlc, bool)>> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        // Memtable pages carry their (in-RAM) values; the merge needs a
        // uniform item type, so map them to value-free markers. See
        // scan_versioned_page for the page-cap argument.
        let strip = |(k, hlc, v): (Vec<u8>, Hlc, VersionValue)| {
            let marker = match v {
                VersionValue::Put(_) => VersionValue::Put(Vec::new()),
                VersionValue::Delete => VersionValue::Delete,
            };
            Ok((k, hlc, marker))
        };
        sources.push(Box::new(
            self.mem.range_latest_page(after, limit).into_iter().map(strip),
        ));
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(
                f.mem.range_latest_page(after, limit).into_iter().map(strip),
            ));
        }
        for sst in self.sstables_newest_first() {
            sources.push(Box::new(sst.stamps_iter_from(after).map(|r| {
                r.map(|(k, hlc, is_put)| {
                    let marker = if is_put {
                        VersionValue::Put(Vec::new())
                    } else {
                        VersionValue::Delete
                    };
                    (k, hlc, marker)
                })
            })));
        }
        KWayMerge::new(sources)
            .filter(|item| match (item, after) {
                (Ok((k, _, _)), Some(a)) => k.as_slice() > a,
                _ => true,
            })
            .take(limit)
            .map(|item| {
                item.map(|(k, hlc, v)| (k, hlc, matches!(v, VersionValue::Put(_))))
            })
            .collect()
    }

    /// Versioned scan of the half-open range `[start, end)`, **tombstones
    /// included**, up to `limit` rows in key order. The routed global-index
    /// probe reads an entry range from each replica and LWW-merges per key —
    /// which needs deletes, exactly like the whole-table versioned scans.
    /// Seeks every source, so cost is proportional to the range.
    pub fn scan_versioned_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<VersionedTombstoneRow>> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        sources.push(Box::new(self.mem.range_latest(start, end).into_iter().map(Ok)));
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(f.mem.range_latest(start, end).into_iter().map(Ok)));
        }
        for sst in self.sstables_newest_first() {
            let entries = sst.range(start, end)?;
            sources.push(Box::new(
                entries.into_iter().map(|e| Ok((e.key, e.hlc, e.value))),
            ));
        }
        KWayMerge::new(sources)
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
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(f.mem.range_latest(start, end).into_iter().map(Ok)));
        }
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

    /// Streaming [`Engine::scan_range`]: live keys in `[start, end)` in key
    /// order, one entry in flight at a time. An unbounded index-ordered
    /// `ORDER BY` walk used to materialize its whole entry range up front —
    /// O(range) memory for O(1) benefit; this holds one merge head per
    /// source instead. Same sources and bounds as `scan_range`.
    pub fn scan_range_iter<'a>(
        &'a self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
        let mut sources: Vec<MergeSource<'a>> = Vec::with_capacity(1 + self.sstable_count());
        sources.push(Box::new(self.mem.range_latest(start, end).into_iter().map(Ok)));
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(f.mem.range_latest(start, end).into_iter().map(Ok)));
        }
        for sst in self.sstables_newest_first() {
            // Seek via the block index, then trim: `iter_from` starts at the
            // block that may hold `start`, so entries before it (and past
            // `end`) are filtered per source — each SSTable contributes a
            // stream, never a collected range.
            let iter = match start {
                Some(s) => sst.iter_from(s),
                None => sst.iter(),
            };
            let start_v = start.map(|s| s.to_vec());
            let end_v = end.map(|e| e.to_vec());
            sources.push(Box::new(
                iter.map(|r| r.map(|e| (e.key, e.hlc, e.value)))
                    .skip_while(move |item| {
                        matches!(item, Ok((k, _, _))
                            if start_v.as_deref().is_some_and(|s| k.as_slice() < s))
                    })
                    .take_while(move |item| match (item, &end_v) {
                        (Ok((k, _, _)), Some(e)) => k.as_slice() < e.as_slice(),
                        _ => true,
                    }),
            ));
        }
        KWayMerge::new(sources).filter_map(|item| match item {
            Ok((k, _, VersionValue::Put(bytes))) => Some(Ok((k, bytes))),
            Ok((_, _, VersionValue::Delete)) => None,
            Err(e) => Some(Err(e)),
        })
    }

    /// Count live keys in the half-open byte range `[start, end)` without
    /// materializing entries — index-only `COUNT(*)` pushdown wants the
    /// cardinality of an index range, not its contents. Same sources and
    /// merge as [`Engine::scan_range`], so cost is proportional to the range.
    pub fn count_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<usize> {
        let mut sources: Vec<MergeSource<'_>> = Vec::with_capacity(1 + self.sstable_count());
        sources.push(Box::new(self.mem.range_latest(start, end).into_iter().map(Ok)));
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(f.mem.range_latest(start, end).into_iter().map(Ok)));
        }
        for sst in self.sstables_newest_first() {
            let entries = sst.range(start, end)?;
            sources.push(Box::new(
                entries.into_iter().map(|e| Ok((e.key, e.hlc, e.value))),
            ));
        }
        let mut n = 0usize;
        for item in KWayMerge::new(sources) {
            if let (_, _, VersionValue::Put(_)) = item? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// "Is `[start, end)` small?" for the query planner: returns
    /// `min(live_count, cap + 1)`, so `result <= cap` means the range holds
    /// at most `cap` live keys. Cost is O(cap) regardless of the range —
    /// [`Engine::count_range`] is O(range), which a per-statement planner
    /// probe cannot afford. First bounds the RAW version count with lazy
    /// per-source iterators (raw ≥ live, so overshooting on shadowed or
    /// deleted versions only flips small→big — the conservative direction);
    /// only a provably small range pays for the exact merged count.
    pub fn count_range_at_most(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        cap: usize,
    ) -> Result<usize> {
        let in_range =
            |key: &[u8]| start.is_none_or(|s| key >= s) && end.is_none_or(|e| key < e);
        let mut raw = self.mem.range_latest(start, end).len();
        for f in self.frozen.iter() {
            if raw > cap {
                return Ok(cap + 1);
            }
            raw += f.mem.range_latest(start, end).len();
        }
        if raw > cap {
            return Ok(cap + 1);
        }
        for sst in self.sstables_newest_first() {
            let iter: Box<dyn Iterator<Item = _>> = match start {
                Some(s) => Box::new(sst.iter_from(s)),
                None => Box::new(sst.iter()),
            };
            for item in iter {
                let entry = item?;
                if end.is_some_and(|e| entry.key.as_slice() >= e) {
                    break;
                }
                if !in_range(&entry.key) {
                    continue;
                }
                raw += 1;
                if raw > cap {
                    return Ok(cap + 1);
                }
            }
        }
        Ok(self.count_range(start, end)?.min(cap + 1))
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
        for f in self.frozen.iter().rev() {
            sources.push(Box::new(
                f.mem
                    .iter_latest_lazy()
                    .map(|(k, hlc, v)| Ok((k.to_vec(), hlc, v.clone()))),
            ));
        }
        for sst in self.sstables_newest_first() {
            sources.push(Box::new(
                sst.iter().map(|r| r.map(|e| (e.key, e.hlc, e.value))),
            ));
        }
        KWayMerge::new(sources)
    }

    /// Force everything in memory (active + frozen) into SSTables, inline.
    /// The synchronous path for shutdown, memory pressure, and tests; the
    /// hot write path never calls this — it freezes and lets the background
    /// flusher do the work (see [`Engine::take_flush_job`]).
    pub fn flush(&mut self) -> Result<()> {
        if self.opts.ephemeral {
            // Memory table: the memtable IS the table; flushing would move
            // the data to disk (or, worse, truncate the WAL it lives behind).
            return Ok(());
        }
        if !self.mem.is_empty() {
            self.freeze()?;
        }
        while let Some(job) = self.take_flush_job() {
            let sst = Engine::build_flush(&job)?;
            self.install_flush(job, sst)?;
        }
        self.maybe_compact()?;
        Ok(())
    }

    /// Seal the active memtable behind its own WAL segment and open a fresh
    /// one — the write-path half of a flush (an fsync + rename, no SSTable
    /// build). The heavy work happens off the write path.
    fn freeze(&mut self) -> Result<()> {
        let seq = self.next_wal_seq;
        self.next_wal_seq += 1;
        self.wal.rotate(seq)?;
        let mem = std::mem::take(&mut self.mem);
        self.frozen.push(FrozenMem {
            max_hlc: mem.max_hlc(),
            mem: Arc::new(mem),
            wal_seq: seq,
            building: false,
        });
        Ok(())
    }

    /// The oldest frozen memtable not yet being built, as a work order for
    /// the background flusher. Single in-flight job per engine — installs
    /// must land oldest-first to keep L0 newest-first.
    pub fn take_flush_job(&mut self) -> Option<FlushJob> {
        if self.opts.ephemeral {
            return None;
        }
        let path = if self.frozen.first().is_some_and(|f| !f.building) {
            self.next_table_path()
        } else {
            return None;
        };
        let f = self.frozen.first_mut().expect("checked above");
        f.building = true;
        Some(FlushJob {
            mem: Arc::clone(&f.mem),
            path,
            kek: self.opts.kek.clone(),
            encrypt_new: self.opts.at_rest_enabled,
            codec: self.opts.compression,
            wal_seq: f.wal_seq,
        })
    }

    /// Build the SSTable for a flush job. Pure I/O over an immutable
    /// memtable — call WITHOUT holding the engine lock.
    pub fn build_flush(job: &FlushJob) -> Result<SsTable> {
        // Borrowed entries: the writer copies bytes straight from the
        // (Arc-shared, still readable) memtable into its block buffer — no
        // per-entry key/value clone, which used to double the memtable's
        // footprint for the duration of the build.
        let entries = job.mem.iter_latest_lazy().map(Ok);
        let enc = if job.encrypt_new { job.kek.as_ref() } else { None };
        SsTable::write_stream(&job.path, entries, job.mem.version_count(), job.codec, enc)
    }

    /// Install a built flush: L0 gains the table, the manifest persists
    /// BEFORE the sealed WAL segment drops (a kill between the two leaves
    /// the segment to replay — LWW dedupes the overlap; the ordering that
    /// prevented the 2026-07-12 manifest tear, kept). The maintenance
    /// watermark gates segment deletion exactly as it gated truncation.
    pub fn install_flush(&mut self, job: FlushJob, sst: SsTable) -> Result<()> {
        self.l0.insert(0, sst);
        self.persist_manifest()?;
        if let Some(idx) = self.frozen.iter().position(|f| f.wal_seq == job.wal_seq) {
            let f = self.frozen.remove(idx);
            if f.max_hlc.is_none_or(|h| h <= self.maint_watermark) {
                self.wal.drop_segments_through(f.wal_seq)?;
            }
        }
        Ok(())
    }

    /// Whether the background flusher has work here (a freeze to build or a
    /// compaction trigger met).
    pub fn has_background_work(&self) -> bool {
        (!self.frozen.is_empty() && !self.frozen[0].building)
            || self.l0.len() >= self.opts.l0_compaction_trigger
    }

    /// Un-mark a flush job whose build failed so a later cycle retries it.
    pub fn abort_flush(&mut self, job: &FlushJob) {
        if let Some(f) = self.frozen.iter_mut().find(|f| f.wal_seq == job.wal_seq) {
            f.building = false;
        }
        remove_files(std::slice::from_ref(&job.path));
    }

    /// One background compaction step as a work order, if a trigger is met:
    /// the L0→L1 merge first, else the first over-capacity level cascade
    /// step. Inputs are pinned by path (the builder reopens them read-only);
    /// `epoch` invalidates the install if the table set changed meanwhile.
    pub fn take_compact_job(&mut self) -> Option<CompactJob> {
        if self.opts.ephemeral {
            return None;
        }
        let expire_before = self.ttl_ms.map(|ttl| now_wall_ms().saturating_sub(ttl));
        let tombstone_keep_after = now_wall_ms().saturating_sub(self.tombstone_retention_ms);
        if self.l0.len() >= self.opts.l0_compaction_trigger {
            let deepest = self.levels.len() <= 1;
            let mut inputs: Vec<PathBuf> =
                self.l0.iter().map(|t| t.path().to_path_buf()).collect();
            if let Some(l1) = self.levels.first() {
                inputs.push(l1.path().to_path_buf());
            }
            return Some(CompactJob {
                inputs,
                output: self.next_table_path(),
                deepest,
                codec: self.codec_for(deepest),
                expire_before,
                tombstone_keep_after,
                kek: self.opts.kek.clone(),
                encrypt_new: self.opts.at_rest_enabled,
            });
        }
        for level in 0..self.levels.len() {
            // Only cascade a level DOWN into an existing next level. The
            // deepest level has nowhere to go — it absorbs data and is
            // allowed to exceed the (per-level) capacity budget, exactly as
            // an LSM's last level does. Rewriting it in place doesn't shrink
            // it, so a background compactor that re-checked capacity every
            // tick rewrote a 900 MB+ deepest level forever (~40 MB/s of pure
            // churn, prod 2026-07-15). The inline compactor never hit this:
            // it ran once per flush and its level counter terminated the
            // cascade after one pass.
            if level + 1 >= self.levels.len() {
                break;
            }
            let capacity = self.opts.level1_capacity * 10u64.pow(level as u32);
            if self.levels[level].len() <= capacity {
                continue;
            }
            let inputs = vec![
                self.levels[level].path().to_path_buf(),
                self.levels[level + 1].path().to_path_buf(),
            ];
            return Some(CompactJob {
                inputs,
                output: self.next_table_path(),
                deepest: level + 1 == self.levels.len() - 1,
                codec: self.codec_for(level + 1 == self.levels.len() - 1),
                expire_before,
                tombstone_keep_after,
                kek: self.opts.kek.clone(),
                encrypt_new: self.opts.at_rest_enabled,
            });
        }
        None
    }

    /// Build a compaction job's output OUTSIDE the engine lock, reopening
    /// the pinned inputs read-only.
    pub fn build_compact(job: &CompactJob) -> Result<SsTable> {
        let inputs: Vec<SsTable> = job
            .inputs
            .iter()
            .map(|p| SsTable::open(p, job.kek.as_ref()))
            .collect::<Result<_>>()?;
        let refs: Vec<&SsTable> = inputs.iter().collect();
        let enc = if job.encrypt_new { job.kek.as_ref() } else { None };
        merge_write(
            enc,
            &job.output,
            &refs,
            job.deepest,
            job.codec,
            job.expire_before,
            job.tombstone_keep_after,
        )
    }

    /// Install a built compaction. Discards the output (returning `false`)
    /// when the table set changed since the job was taken — a flush landed
    /// or an inline compaction ran; the next cycle re-plans from the new
    /// state. Retired inputs are deleted only after the manifest points at
    /// the replacement (the ordering that ended the manifest tears).
    pub fn install_compact(&mut self, job: CompactJob, new_run: SsTable) -> Result<bool> {
        let is_input = |t: &SsTable| job.inputs.iter().any(|p| p == t.path());
        // Path-based validity, NOT a global epoch: a flush landing during the
        // build adds a NEW L0 table that is not among these inputs, so the
        // compaction is still valid — only clobber it if one of its OWN
        // inputs vanished (an overlapping compaction already consumed it).
        // The global-epoch check invalidated on every concurrent flush,
        // discarding the freshly built SSTable and re-planning the same
        // merge — a build/discard loop that burned ~20 MB/s of disk on the
        // write-coordinating nodes (.3/.6, 2026-07-15). The wasted output
        // was the entire load.
        let inputs_present = job.inputs.iter().all(|p| {
            self.l0.iter().any(|t| t.path() == p)
                || self.levels.iter().any(|t| t.path() == p)
        });
        if !inputs_present {
            remove_files(std::slice::from_ref(&job.output));
            return Ok(false);
        }
        // The output level: one deeper than the shallowest level the inputs
        // touched. An L0 merge (any L0 input) produces an L1 run; an Ln
        // cascade produces Ln+1.
        let l0_input = self.l0.iter().any(is_input);
        let removed_levels: Vec<usize> = self
            .levels
            .iter()
            .enumerate()
            .filter(|(_, t)| is_input(t))
            .map(|(i, _)| i)
            .collect();
        let target = if l0_input {
            1
        } else {
            removed_levels.first().copied().unwrap_or(0) + 1
        };
        self.compactions += 1;
        self.compaction_bytes += new_run.disk_len();
        // Remove exactly the input tables (keep any flushed-since L0 table).
        self.l0.retain(|t| !is_input(t));
        // Remove input levels high-index-first so earlier indices stay valid.
        for &i in removed_levels.iter().rev() {
            self.levels.remove(i);
        }
        // Place the output at `target` (levels index target-1), replacing an
        // existing run there or extending the ladder.
        let idx = target - 1;
        if idx < self.levels.len() {
            self.levels.insert(idx, new_run);
        } else {
            self.levels.push(new_run);
        }
        self.persist_manifest()?;
        remove_files(&job.inputs);
        Ok(true)
    }

    /// Advance the deferred-maintenance watermark (see the field docs).
    /// The first call switches the engine from "no deferred consumers"
    /// (`Hlc::MAX`) into tracking mode; later calls only move forward.
    pub fn set_maintenance_watermark(&mut self, hlc: Hlc) {
        if self.maint_watermark == Hlc::MAX || hlc > self.maint_watermark {
            self.maint_watermark = hlc;
        }
    }

    /// Every memtable version: keys ascending, newest-first per key. The
    /// crash-recovery replay of deferred maintenance walks this (the WAL has
    /// re-populated the memtable by the time it runs).
    pub fn mem_versions(&self) -> impl Iterator<Item = (&[u8], Hlc, &VersionValue)> {
        self.mem.iter_versions()
    }

    /// Latest version of `key` from the flushed (SSTable) layer only — the
    /// state a key had before everything currently in the memtable. The
    /// maintenance replay uses it as the "previous document" base for the
    /// first in-memtable version of a key.
    pub fn get_flushed(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for sst in self.sstables_newest_first() {
            if let Some((hlc, value)) = sst.get(key)? {
                if self.is_expired(hlc) {
                    return Ok(None);
                }
                return Ok(version_to_value(value));
            }
        }
        Ok(None)
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.mem.approx_bytes() < self.opts.flush_threshold_bytes || self.opts.ephemeral {
            return Ok(());
        }
        // Backpressure: if the background flusher has fallen this far
        // behind, degrade to the old inline flush instead of hoarding
        // frozen memtables without bound.
        if self.frozen.len() >= 4 {
            return self.flush();
        }
        self.freeze()
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
                None,
            )?)
        };

        self.l0.clear();
        self.levels.clear();
        if let Some(sst) = new_table {
            self.levels.push(sst);
        }
        self.mem = Memtable::new();
        self.frozen.clear();
        self.wal.truncate()?;
        let _ = self.wal.drop_segments_through(self.next_wal_seq);
        self.read_cache = ReadCache::new(self.opts.read_cache_capacity);
        self.write_seq += 1; // data changed without an append — stamp it
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
            memtable_bytes: self.mem.approx_bytes()
                + self.frozen.iter().map(|f| f.mem.approx_bytes()).sum::<usize>(),
            memtable_versions: self.mem.version_count()
                + self.frozen.iter().map(|f| f.mem.version_count()).sum::<usize>(),
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
    /// Approximate key statistics in O(memtable + #sstables): sums each
    /// immutable file's cached version counts plus the memtable's. Counts
    /// VERSIONS, not merged uniques — a key rewritten across files counts
    /// once per file until compaction collapses it, so this drifts high
    /// under overwrite churn and self-heals at compaction. Use for
    /// dashboards/metrics; `key_stats` stays exact for `COUNT(*)`.
    pub fn key_stats_fast(&self) -> KeyStats {
        let mut stats = KeyStats::default();
        for sst in self.sstables_newest_first() {
            let (puts, dels) = sst.version_counts();
            stats.live_keys += puts as usize;
            stats.tombstones += dels as usize;
        }
        for f in self.frozen.iter() {
            for (_k, _hlc, v) in f.mem.iter_latest_lazy() {
                match v {
                    VersionValue::Put(_) => stats.live_keys += 1,
                    VersionValue::Delete => stats.tombstones += 1,
                }
            }
        }
        for (_k, _hlc, v) in self.mem.iter_latest_lazy() {
            match v {
                VersionValue::Put(_) => stats.live_keys += 1,
                VersionValue::Delete => stats.tombstones += 1,
            }
        }
        stats
    }

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
        let tombstone_keep_after = now_wall_ms().saturating_sub(self.tombstone_retention_ms);
        let enc = if self.opts.at_rest_enabled {
            self.opts.kek.as_ref()
        } else {
            None
        };
        let new_l1 = merge_write(enc, &path, &sources, deepest, codec, expire_before, tombstone_keep_after)?;
        self.compactions += 1;
        self.compaction_bytes += new_l1.disk_len();
        self.l0.clear();
        if self.levels.is_empty() {
            self.levels.push(new_l1);
        } else {
            self.levels[0] = new_l1;
        }

        // Retired inputs (from the L0 merge above AND the cascade below) are
        // deleted only AFTER the manifest durably points at their
        // replacements. The L0 merge deleting first was the third (and
        // forensically confirmed) manifest tear: MANIFEST mtime 16:16, the
        // merged 0076.sst written 16:26 with its five inputs removed, and
        // the manifest never rewritten before the process exited — the node
        // crash-looped on ENOENT at open (2026-07-09, 2026-07-12 x2, all
        // during bulk-load L0 churn).
        let mut retired: Vec<PathBuf> = old_paths;
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

            let enc = if self.opts.at_rest_enabled {
                self.opts.kek.as_ref()
            } else {
                None
            };
            let new_run =
                merge_write(enc, &path, &sources, deepest, codec, expire_before, tombstone_keep_after)?;
            self.compactions += 1;
            self.compaction_bytes += new_run.disk_len();
            if has_next {
                self.levels[level + 1] = new_run;
                self.levels.remove(level);
            } else {
                self.levels.push(new_run);
                self.levels.remove(level);
            }
            retired.extend(old_paths);
            level += 1;
        }
        self.persist_manifest()?;
        remove_files(&retired);
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
    enc: Option<&Kek>,
    path: &Path,
    sources: &[&SsTable],
    drop_tombstones: bool,
    codec: Codec,
    expire_before: Option<u64>,
    tombstone_keep_after: u64,
) -> Result<SsTable> {
    let approx: u64 = sources.iter().map(|s| s.len()).sum();
    let iters: Vec<MergeSource<'_>> = sources
        .iter()
        .map(|sst| {
            Box::new(sst.iter().map(|r| r.map(|e| (e.key, e.hlc, e.value)))) as MergeSource<'_>
        })
        .collect();
    let entries = KWayMerge::new(iters).filter_map(move |item| match item {
        // Deepest-level tombstone drop, gated by the retention cutoff: a
        // marker stamped at-or-after `tombstone_keep_after` is retained so
        // a registered witness still pulls the delete (keeping longer is
        // always safe — there is nothing below the deepest level).
        Ok((_, hlc, VersionValue::Delete))
            if drop_tombstones && hlc.physical < tombstone_keep_after =>
        {
            None
        }
        // TTL reclaim: a row whose stamp predates the cutoff is dropped
        // outright (it is already invisible on every read path).
        Ok((_, hlc, _)) if expire_before.is_some_and(|c| hlc.physical < c) => None,
        Ok((key, hlc, value)) => Some(Ok(SstEntry { key, hlc, value })),
        Err(e) => Some(Err(e)),
    });
    SsTable::write_stream(path, entries, approx as usize, codec, enc)
}

/// The TTL expiry rule, shared with cluster coordinators (which merge
/// VERSIONED replica copies — those paths must stay TTL-blind so stamped
/// data replicates and converges, and filter the LWW winner with this
/// exact predicate instead).
pub fn row_expired(ttl_ms: Option<u64>, hlc: Hlc) -> bool {
    ttl_ms.is_some_and(|ttl| now_wall_ms().saturating_sub(hlc.physical) > ttl)
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
        let _ = std::fs::remove_file(crate::sstable::stamps_path(p));
    }
}

/// Load the SSTable set described by the manifest. Returns (L0 newest-first,
/// deeper levels, next sequence number).
#[allow(clippy::type_complexity)]
fn load_manifest(dir: &Path, kek: Option<&Kek>) -> Result<(Vec<SsTable>, Vec<SsTable>, u64)> {
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
        let sst = SsTable::open(&path, kek).map_err(|e| {
            skaidb_types::slog!(
                "skaidb: manifest {} references {} which failed to open: {e}",
                manifest.display(),
                path.display()
            );
            e
        })?;
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
mod forensics {
    use super::*;

    /// Not a test of the code — a debug dump tool: set SKAIDB_DUMP_DIR to a
    /// copied table directory and this prints every version of every key
    /// (superseded ones included) from all SSTables and the memtable/WAL,
    /// with HLC wall-clock timestamps. Used for the 2026-07-14
    /// gmail_accounts tombstone investigation.
    #[test]
    fn dump_all_versions() {
        let Ok(dir) = std::env::var("SKAIDB_DUMP_DIR") else {
            return; // no-op in normal test runs
        };
        let engine = Engine::open(&dir).expect("open copied table dir");
        for sst in engine.sstables_newest_first() {
            eprintln!("== sstable {:?}", sst.path());
            for row in sst.iter() {
                let e = row.expect("entry");
                let kind = match e.value {
                    VersionValue::Put(ref b) => format!("PUT {}B", b.len()),
                    VersionValue::Delete => "DELETE".into(),
                };
                eprintln!(
                    "  hlc={}({}) key={} {}",
                    e.hlc.physical,
                    chrono_ish(e.hlc.physical),
                    String::from_utf8_lossy(&e.key).chars().take(60).collect::<String>(),
                    kind
                );
            }
        }
        eprintln!("== memtable (WAL replayed)");
        for (key, hlc, v) in engine.mem.iter_latest_lazy() {
            let kind = match v {
                VersionValue::Put(b) => format!("PUT {}B", b.len()),
                VersionValue::Delete => "DELETE".into(),
            };
            eprintln!(
                "  hlc={}({}) key={} {}",
                hlc.physical,
                chrono_ish(hlc.physical),
                String::from_utf8_lossy(key).chars().take(60).collect::<String>(),
                kind
            );
        }
    }

    fn chrono_ish(ms: u64) -> String {
        // crude UTC render without a chrono dep: days since epoch + hh:mm:ss
        let secs = ms / 1000;
        let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
        let days = secs / 86400;
        format!("d{days} {h:02}:{m:02}:{s:02}Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The shared TTL predicate — the engine's read paths AND the cluster
    /// coordinator's LWW-winner filtering both use this exact rule, so a
    /// row expires identically whether served locally or via quorum merge.
    #[test]
    fn row_expired_rule() {
        let now = now_wall_ms();
        assert!(!row_expired(None, Hlc::new(0, 0)), "no TTL: never expires");
        assert!(row_expired(Some(1_000), Hlc::new(now - 5_000, 0)));
        assert!(!row_expired(Some(1_000), Hlc::new(now, 0)));
        // A stamp AHEAD of the wall clock (HLC ratchet) never underflows.
        assert!(!row_expired(Some(1_000), Hlc::new(now + 60_000, 0)));
    }

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

    /// End-to-end at-rest: an encrypted engine flushes + compacts encrypted
    /// SSTables and an encrypted WAL, on-disk bytes are ciphertext, and a
    /// reopen with the key recovers everything. Wrong/no key fails the reopen.
    #[test]
    fn at_rest_encrypts_wal_and_sstables_end_to_end() {
        let dir = tempdir();
        let kek = Kek::from_bytes(&[3u8; 32]).unwrap();
        let enc_opts = || EngineOptions {
            flush_threshold_bytes: 256,
            l0_compaction_trigger: 3,
            level1_capacity: 8,
            kek: Some(kek.clone()),
            at_rest_enabled: true,
            ..Default::default()
        };
        {
            let mut e = Engine::open_with_options(&dir, enc_opts()).unwrap();
            for i in 0..300 {
                e.put(format!("secretkey{i:04}").as_bytes(),
                      format!("secretval{i:04}").into_bytes()).unwrap();
            }
            e.maybe_flush().unwrap();
            e.maybe_compact().unwrap();
            // Value still readable live.
            assert_eq!(e.get(b"secretkey0100").unwrap(), Some(b"secretval0100".to_vec()));
        }
        // On disk: no plaintext key/value anywhere under the dir.
        fn walk(d: &Path, needle: &str) -> bool {
            for e in std::fs::read_dir(d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    if walk(&p, needle) { return true; }
                } else if String::from_utf8_lossy(&std::fs::read(&p).unwrap()).contains(needle) {
                    return true;
                }
            }
            false
        }
        assert!(!walk(&dir, "secretkey0100"), "key must not be on disk in the clear");
        assert!(!walk(&dir, "secretval0100"), "value must not be on disk in the clear");
        // Reopen WITH the key: data recovers.
        {
            let e = Engine::open_with_options(&dir, enc_opts()).unwrap();
            assert_eq!(e.get(b"secretkey0299").unwrap(), Some(b"secretval0299".to_vec()));
        }
        // Reopen with the WRONG key, or none, fails (can't open the files).
        let wrong = EngineOptions {
            kek: Some(Kek::from_bytes(&[9u8; 32]).unwrap()),
            at_rest_enabled: true,
            ..enc_opts()
        };
        assert!(Engine::open_with_options(&dir, wrong).is_err(), "wrong KEK must fail");
        let nokey = EngineOptions { kek: None, at_rest_enabled: false, ..enc_opts() };
        assert!(Engine::open_with_options(&dir, nokey).is_err(), "encrypted store needs a key");
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
    fn deepest_level_over_capacity_does_not_re_compact_forever() {
        // A deepest level larger than its per-level entry budget must NOT be
        // re-selected for compaction: there is nowhere deeper to move it, so
        // rewriting it in place is pure churn. The background compactor
        // re-checked capacity every tick and rewrote a 900 MB+ deepest level
        // forever (~40 MB/s, prod 2026-07-15).
        let opts = EngineOptions {
            flush_threshold_bytes: 1 << 20,
            l0_compaction_trigger: 2,
            level1_capacity: 8, // tiny: the merged L1 will exceed it
            ..Default::default()
        };
        let mut e = Engine::open_with_options(tempdir(), opts).unwrap();
        // Flush enough L0 tables to trigger an L0->L1 merge; the resulting
        // L1 holds ~40 keys, far over the 8-entry "capacity".
        for batch in 0..4u32 {
            for i in 0..10u32 {
                let k = batch * 10 + i;
                e.put(format!("k{k:03}").as_bytes(), vec![k as u8; 32]).unwrap();
            }
            e.freeze().unwrap();
            let j = e.take_flush_job().unwrap();
            let sst = Engine::build_flush(&j).unwrap();
            e.install_flush(j, sst).unwrap();
        }
        // Drain the L0->L1 merge.
        while let Some(job) = e.take_compact_job() {
            let built = Engine::build_compact(&job).unwrap();
            assert!(e.install_compact(job, built).unwrap());
        }
        // L1 is now the deepest level and over the 8-entry budget — but there
        // must be NO further compaction work (the loop is broken).
        assert!(
            e.take_compact_job().is_none(),
            "deepest over-capacity level must not re-trigger compaction"
        );
        assert!(!e.has_background_work(), "no phantom background work");
        // Data intact.
        assert_eq!(e.get(b"k000").unwrap(), Some(vec![0u8; 32]));
        assert_eq!(e.get(b"k039").unwrap(), Some(vec![39u8; 32]));
    }

    #[test]
    fn compaction_install_survives_a_concurrent_flush() {
        // A flush landing between take_compact_job and install_compact adds a
        // new L0 table. The compaction must still install (its own inputs are
        // untouched) AND the flushed table must survive — the global-epoch
        // discard re-planned the merge forever, a ~20 MB/s disk loop
        // (2026-07-15).
        // Large level1_capacity so the deepest level never self-triggers —
        // this test is about an L0->L1 merge racing a flush, not cascades.
        let opts = EngineOptions {
            flush_threshold_bytes: 1 << 20,
            l0_compaction_trigger: 3,
            level1_capacity: 1_000_000,
            ..Default::default()
        };
        let mut e = Engine::open_with_options(tempdir(), opts).unwrap();
        // Build up L0 tables (manual freeze/flush) past the trigger.
        for batch in 0..4u32 {
            for i in 0..10u32 {
                let k = batch * 10 + i;
                e.put(format!("k{k:03}").as_bytes(), vec![k as u8; 64]).unwrap();
            }
            e.freeze().unwrap();
            let j = e.take_flush_job().unwrap();
            let sst = Engine::build_flush(&j).unwrap();
            e.install_flush(j, sst).unwrap();
        }
        let job = e.take_compact_job().expect("l0 over trigger");
        let built = Engine::build_compact(&job).unwrap();
        // Concurrent flush: a brand-new key freezes + installs as a fresh L0
        // table WHILE the compaction output is in hand.
        e.put(b"concurrent", b"kept".to_vec()).unwrap();
        e.freeze().unwrap();
        let fj = e.take_flush_job().unwrap();
        let fsst = Engine::build_flush(&fj).unwrap();
        e.install_flush(fj, fsst).unwrap();
        // The compaction installs (not discarded) despite the epoch bump.
        assert!(e.install_compact(job, built).unwrap(), "compaction must install");
        // Both the compacted data AND the concurrently flushed row survive.
        assert_eq!(e.get(b"k000").unwrap(), Some(vec![0u8; 64]));
        assert_eq!(e.get(b"k039").unwrap(), Some(vec![39u8; 64]));
        assert_eq!(e.get(b"concurrent").unwrap(), Some(b"kept".to_vec()));
    }

    #[test]
    fn background_flush_freezes_then_installs_and_replays_on_crash() {
        let dir = tempdir();
        {
            let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
            for i in 0..8u32 {
                e.put(format!("k{i:02}").as_bytes(), vec![i as u8; 64]).unwrap();
            }
            // Freeze whatever is active: reads must keep serving from the
            // frozen layer with nothing yet in SSTables for those rows.
            e.freeze().unwrap();
            assert_eq!(e.get(b"k00").unwrap(), Some(vec![0u8; 64]));
            assert_eq!(e.get(b"k07").unwrap(), Some(vec![7u8; 64]));
            assert_eq!(e.scan_prefix(b"k").unwrap().len(), 8);
            // Background cycle: take → build (no lock needed) → install.
            let job = e.take_flush_job().expect("frozen memtable pending");
            let sst = Engine::build_flush(&job).unwrap();
            e.install_flush(job, sst).unwrap();
            assert_eq!(e.get(b"k03").unwrap(), Some(vec![3u8; 64]));
            // A second freeze that never gets built = the crash window.
            e.put(b"late", b"survives".to_vec()).unwrap();
            e.freeze().unwrap();
            // e dropped here with a sealed, unflushed WAL segment.
        }
        let e = Engine::open_with_options(&dir, small_opts()).unwrap();
        assert_eq!(
            e.get(b"late").unwrap(),
            Some(b"survives".to_vec()),
            "sealed segment replayed at open"
        );
        assert_eq!(e.get(b"k00").unwrap(), Some(vec![0u8; 64]));
        assert_eq!(e.scan_prefix(b"k").unwrap().len(), 8);
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
    fn tombstone_retention_gates_the_deepest_drop() {
        let dir = tempdir();
        let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
        for i in 0..30u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        // Retention 0 (default): the deepest merge purges the marker. The
        // churn after the delete guarantees enough L0 tables that the
        // cascade actually reaches the deepest level (one lone flush
        // leaves the marker in an upper level, correctly retained).
        e.delete(b"k0005").unwrap();
        for i in 30..60u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        e.flush().unwrap();
        let markers = |e: &Engine| -> usize {
            e.scan_versioned_with_tombstones()
                .unwrap()
                .into_iter()
                .filter(|(_, _, value)| value.is_none())
                .count()
        };
        assert_eq!(markers(&e), 0, "default behavior: tombstones drop at deepest");

        // With a retention window, a fresh marker SURVIVES compaction —
        // a witness that hasn't pulled it yet can still learn the delete.
        e.set_tombstone_retention_ms(60 * 60 * 1000);
        e.delete(b"k0007").unwrap();
        for i in 60..90u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        e.flush().unwrap();
        assert_eq!(markers(&e), 1, "retained marker survives the deepest merge");
        assert_eq!(e.get(b"k0007").unwrap(), None, "reads still see the delete");

        // Retention back to 0: the next deepest merge purges it.
        e.set_tombstone_retention_ms(0);
        for i in 90..120u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        e.flush().unwrap();
        assert_eq!(markers(&e), 0, "lapsed retention purges on the next merge");
        assert_eq!(e.scan().unwrap().len(), 118, "118 live keys (120 - 2 deleted)");
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

    #[test]
    fn scan_range_iter_matches_scan_range() {
        // Data spread over memtable + several SSTables with overwrites and
        // deletes, so the streaming merge exercises every source kind.
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        for i in 0..150u32 {
            e.put(format!("r{i:04}").as_bytes(), vec![i as u8; 30]).unwrap();
        }
        e.delete(b"r0010").unwrap();
        e.flush().unwrap();
        for i in 100..160u32 {
            e.put(format!("r{i:04}").as_bytes(), vec![9u8; 30]).unwrap(); // overwrites + fresh
        }
        e.delete(b"r0120").unwrap();

        for (start, end) in [
            (None, None),
            (Some(b"r0050".as_slice()), None),
            (None, Some(b"r0100".as_slice())),
            (Some(b"r0025".as_slice()), Some(b"r0130".as_slice())),
        ] {
            let collected = e.scan_range(start, end).unwrap();
            let streamed: Vec<(Vec<u8>, Vec<u8>)> =
                e.scan_range_iter(start, end).collect::<Result<_>>().unwrap();
            assert_eq!(streamed, collected, "range {start:?}..{end:?}");
        }
    }

    #[test]
    fn stamps_page_matches_versioned_page() {
        // Data spread across memtable + several SSTables (small_opts flushes
        // and compacts aggressively), with deletes in both regions.
        let dir = tempdir();
        let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
        for i in 0..200u32 {
            e.put(format!("s{i:04}").as_bytes(), vec![7u8; 60]).unwrap();
        }
        e.delete(b"s0003").unwrap();
        e.flush().unwrap();
        for i in 200..230u32 {
            e.put(format!("s{i:04}").as_bytes(), vec![7u8; 60]).unwrap();
        }
        e.delete(b"s0210").unwrap(); // memtable tombstone

        let mut after: Option<Vec<u8>> = None;
        let mut stamps = Vec::new();
        loop {
            let page = e.scan_stamps_page(after.as_deref(), 64).unwrap();
            let Some((k, _, _)) = page.last() else { break };
            after = Some(k.clone());
            let n = page.len();
            stamps.extend(page);
            if n < 64 {
                break;
            }
        }
        let expect: Vec<(Vec<u8>, Hlc, bool)> = e
            .scan_versioned_with_tombstones()
            .unwrap()
            .into_iter()
            .map(|(k, hlc, v)| (k, hlc, v.is_some()))
            .collect();
        assert_eq!(stamps, expect);
        assert!(stamps.iter().any(|(_, _, p)| !p), "tombstones included");

        // Compaction removed input files' sidecars along with the files: no
        // orphaned .stamps in the sst dir.
        let sst_dir = dir.join("sst");
        for entry in std::fs::read_dir(&sst_dir).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().is_some_and(|x| x == "stamps") {
                let mut main = p.clone();
                main.set_extension(""); // strips ".stamps", leaving "<n>.sst"
                assert!(main.exists(), "orphaned sidecar {p:?}");
            }
        }
    }

    impl Engine {
        /// Test helper: a snapshot stamp (exposed only in tests).
        fn clock_snapshot(&self) -> Hlc {
            self.clock.now()
        }
    }
}
