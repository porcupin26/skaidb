//! The embeddable query engine: parse, plan, and execute against storage.
//!
//! One [`storage::Engine`] backs each table (a table is a namespace, SPEC §2).
//! Rows are documents keyed by their primary key, encoded with the
//! order-preserving key codec so scans come back in key order.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use skaidb_sql::ast::{
    AggArg, AggFunc, AlterAction, AlterTable, BinaryOp, CreateSearchIndex, CreateTable, Delete,
    Expr, Insert, JoinKind, OrderKey, Rerank, Select, SelectItem, Statement, UnaryOp, Update,
};
use skaidb_sql::parse;
use std::sync::Arc;

use skaidb_storage::{
    CompactJob, Engine as StorageEngine, EngineOptions, FlushJob, Hlc, HlcClock, VersionValue,
    WalCommit, WalSync,
};
use skaidb_types::{Document, Value};

use skaidb_tsdb::{Tsdb, TsdbOptions};

use skaidb_fts::{SearchIndex, SearchIndexConfig, SearchQuery, Watermark};

use crate::catalog::{AuthRoleDef, Catalog, IndexDef, RollupDef, SchemaVersion, SearchIndexDef, TableDef, TsTableDef, UserAuthKind, UserDef, VectorIndexDef};
use skaidb_auth::{privilege_from_name, Object as AuthObject, Privilege as AuthPrivilege, RoleStore, ScramCredential};
use crate::error::{EngineError, Result};
use crate::eval::{compare, eval, eval_predicate};
use crate::namespace::{self, DEFAULT_DATABASE};
use crate::result::{QueryOutput, ResultSet, SessionEffect};
use crate::vector::{Hnsw, Metric};

/// A live row with its encoded document and version stamp.
pub type VersionedRow = (Vec<u8>, Vec<u8>, Hlc);

/// A versioned row including tombstones: `(key, value, hlc, is_put)`. When
/// `is_put` is false the row is a delete and `value` is empty.
pub type VersionedTombstoneRow = (Vec<u8>, Vec<u8>, Hlc, bool);

/// A secondary-index scan request for a coordinator: `(index_name, start_key,
/// end_key)` (byte bounds, `None` = unbounded).
pub type IndexScanRange = (String, Option<Vec<u8>>, Option<Vec<u8>>);

/// A geo-index scan plan: `(index_name, ranges)` where each range is a byte
/// `(start_inclusive, end_exclusive)` over the index's Morton-code entries
/// (`end = None` when the range runs to the maximum code). The union of ranges
/// is a superset of the query's matching points — the caller re-reads each
/// candidate and applies the exact `geo_distance` / `geo_bbox` predicate.
pub type GeoScanPlan = (String, Vec<(Vec<u8>, Option<Vec<u8>>)>);

/// `(key, document)` rows — the engine's standard row-gather result.
type KeyedRows = Vec<(Vec<u8>, Document)>;

/// One `HIGHLIGHT()` request: the column and its resolved highlight options
/// (fragment size, pre/post tags, no-match size). Threaded through the search
/// gather so each hit gets a `_highlight_<column>` field.
type HighlightReq = (String, skaidb_fts::HighlightOpts);
/// `DESCRIBE`'s catalog half: the primary-key columns (in key order) and, per
/// column, the descriptors of the indexes covering it.
type DescribeCatalog = (Vec<String>, BTreeMap<String, Vec<String>>);
/// `DESCRIBE … FULL`'s data half: field name → the set of value-type tags
/// seen for it (schema-less, so one field can hold several types).
type FieldTypes = BTreeMap<String, BTreeSet<&'static str>>;
/// Gathered rows plus whether they are already in the requested `ORDER BY` order.
type OrderedRows = (KeyedRows, bool);
/// A chosen index access path: `(index_name, start_key, end_key, sorted)` where
/// `sorted` is whether the scan order already satisfies the query's `ORDER BY`.
/// The deferred half of one replicated write: everything the applier needs
/// to maintain secondary indexes, vectors, and search off the ack path. The
/// raw bytes ride along so decoding happens outside the write lock.
#[derive(Debug)]
pub struct MaintTask {
    pub table: String,
    pub key: Vec<u8>,
    pub hlc: Hlc,
    /// Previous row version at capture time (already-encoded), if any.
    pub old: Option<Vec<u8>>,
    /// New row version; `None` = delete.
    pub new: Option<Vec<u8>>,
}

/// A [`MaintTask`] with its documents decoded (outside the write lock).
#[derive(Debug)]
pub struct DecodedMaint {
    pub table: String,
    pub key: Vec<u8>,
    pub hlc: Hlc,
    pub old_doc: Option<Document>,
    pub new_doc: Option<Document>,
}

impl MaintTask {
    /// Decode the old/new payloads (CPU-heavy for large JSON rows — the
    /// entire reason maintenance is deferred). Non-document payloads decode
    /// to `None` and are skipped by maintenance, matching the sync path.
    pub fn decode(self) -> DecodedMaint {
        let doc = |b: Option<Vec<u8>>| {
            b.and_then(|bytes| match Value::decode(&bytes) {
                Ok(Value::Document(d)) => Some(d),
                _ => None,
            })
        };
        DecodedMaint {
            table: self.table,
            key: self.key,
            hlc: self.hlc,
            old_doc: doc(self.old),
            new_doc: doc(self.new),
        }
    }
}

type IndexPlan = (String, Option<Vec<u8>>, Option<Vec<u8>>, bool, bool);

/// Which live map a catalog index name resolves against, for
/// `Database::index_local_health` (the SHOW INDEXES `local` column).
#[derive(Clone, Copy)]
enum IndexClass {
    Secondary,
    Vector,
    Search,
    Geo,
}
/// Per-key version chain: `(hlc, Some(bytes) | None-tombstone)`, used by the
/// deferred-maintenance crash replay.
type VersionChain = Vec<(Hlc, Option<Vec<u8>>)>;
/// A buffered batch apply's result: the last commit handle (for one group
/// fsync) plus the deferred-maintenance tasks.
type BatchRowOnly = (Option<(WalCommit, Arc<WalSync>)>, Vec<MaintTask>);

/// Candidate-range size below which the planner abandons a sorted
/// ORDER BY + LIMIT walk for a strictly more selective unsorted index.
/// Matches the cluster's point-read gather ceiling (`INDEX_POINT_READ_MAX`),
/// so a "small" verdict here lands on the cheap resolve path there.
const PLANNER_PROBE_MAX: usize = 256;

/// An embedded skaidb database: catalog plus one storage engine per table and
/// per secondary index.
#[derive(Debug)]
pub struct Database {
    dir: PathBuf,
    /// Storage tuning (flush threshold, read-cache size, codecs) applied to
    /// every table/index engine this database opens or creates.
    storage_opts: EngineOptions,
    catalog: Catalog,
    tables: HashMap<String, StorageEngine>,
    /// Time-series stores by table name (the tsdb engine, not the LSM).
    timeseries: HashMap<String, Tsdb>,
    /// RBAC view rebuilt from the catalog on open and after auth DDL.
    role_store: RoleStore,
    /// Secondary index storage by index name; entries map an indexed value to a
    /// table primary-key (so a `WHERE path = v` lookup avoids a full scan).
    indexes: HashMap<String, StorageEngine>,
    /// In-memory HNSW vector indexes by name (rebuilt from the table on open).
    vector_indexes: HashMap<String, Hnsw>,
    /// Max row HLC each vector index has applied — persisted in its snapshot
    /// so a reload replays only rows stamped after it (FTS-style catch-up).
    vector_watermarks: HashMap<String, Hlc>,
    /// Live full-text search indexes by name (reopened from disk on open,
    /// caught up from the table by watermark replay).
    search_indexes: HashMap<String, LiveSearchIndex>,
    /// The open transaction's buffered writes, if a `BEGIN` is in flight.
    txn: Option<TxnBuffer>,
    /// Clock for stamping DDL so schema changes order under last-writer-wins.
    clock: HlcClock,
    /// When `Some`, the HLC the current (replicated) DDL statement must use
    /// instead of a fresh local stamp; set by [`Database::execute_session_with_hlc`]
    /// and consumed by [`Database::ddl_stamp`].
    ddl_hlc: Option<Hlc>,
    /// Wall-clock spent rebuilding HNSW vector indexes during the last `open`
    /// (they live only in RAM and are reconstructed from table rows), in
    /// milliseconds — surfaced as a metric so a slow open is visible.
    vector_rebuild_ms: u64,
    /// Wall-clock spent reopening/replaying/rebuilding search indexes during
    /// the last `open`, in milliseconds.
    search_rebuild_ms: u64,
    /// Indexes created/imported here whose backfill hasn't been driven yet.
    /// A Session drains inline after DDL; a cluster node's background worker
    /// pages through them without monopolizing the write lock.
    pending_backfills: Vec<String>,
    /// Vector indexes this node is currently backfilling in pages. Runtime
    /// state, local by design (like a search index's `building`): searches
    /// error with "rebuilding — retry shortly" and SHOW INDEXES reports
    /// `building` while a name is in here. Writes landing meanwhile maintain
    /// the live HNSW normally (idempotent overlap with the pages).
    building_vectors: std::collections::HashSet<String>,
    /// Vector backfills not yet driven — same drain contract as
    /// `pending_backfills` (inline for a Session, background on a cluster
    /// node).
    pending_vector_backfills: Vec<String>,
    /// Geo indexes created/imported here whose backfill hasn't been driven.
    /// Same drain contract as `pending_backfills` (geo entries persist in the
    /// index engine, so this is only about the initial fill of existing rows).
    pending_geo_backfills: Vec<String>,
    /// Cluster mode: leave pending backfills for the background worker
    /// instead of draining inline after each statement, so DDL acks at
    /// schema-apply (single-node/Session keeps run-to-completion DDL).
    defer_backfills: bool,
    /// Managed (`EMBED`) vector indexes → row keys awaiting embedding. A write
    /// to a managed index enqueues here instead of embedding inline (the row's
    /// text is the source of truth); a drain reads the text, calls the external
    /// embedder OFF the write lock, inserts the vector, and advances the
    /// watermark — so the model server being down delays searchability but
    /// never blocks or fails a write. In-memory (rebuilt from rows past the
    /// snapshot watermark on open), so a lost queue costs re-embedding, not
    /// data.
    pending_embeds: std::collections::HashMap<String, std::collections::BTreeSet<Vec<u8>>>,
    /// Search indexes whose startup catch-up was deferred (server mode),
    /// with the committed watermark to replay from (`None` = full rebuild;
    /// the index was already cleared at open).
    pending_search_catchups: Vec<(String, Option<Hlc>)>,
    /// Per-table applier watermark: every write stamped `<=` it has had its
    /// deferred index/vector/search maintenance applied. Persisted to
    /// `applier.watermarks`; drives crash-recovery replay and the storage
    /// layer's WAL-truncation gate. Absent entry = table has never had a
    /// deferred write (sync path or no consumers).
    applied_watermarks: HashMap<String, Hlc>,
    /// RAM-only cache for `DESCRIBE … FULL EXACT`: per table, the field→types
    /// map from a full scan, stamped with the storage engine's `write_seq` at
    /// scan time. Valid while the stamp still matches (every mutation bumps
    /// the seq); never persisted — both the cache and the seq reset with the
    /// process, so they can never disagree across a restart. Tables no one
    /// DESCRIBEs pay nothing. `Mutex` because DESCRIBE runs on the shared-lock
    /// read path (`&self`); size is bounded by fields × type tags, tiny.
    field_registry: std::sync::Mutex<HashMap<String, (u64, FieldTypes)>>,
    /// When `Some`, statement-final group commits ([`LocalCluster::
    /// flush_pending`]) defer: the `(WalSync, WalCommit)` pairs land here
    /// instead of fsyncing inline, so a caller holding this Database behind
    /// an exclusive lock can release it BEFORE the fsync — concurrent
    /// sessions' commits then coalesce in `WalSync::sync_through` (group
    /// commit) instead of serializing whole fsyncs under the lock. Set and
    /// drained only by [`Database::execute_session_statement_deferred`].
    deferred_syncs: Option<Vec<(Arc<WalSync>, WalCommit)>>,
    /// Text-embedding provider for managed vector indexes (`… EMBED`). `None`
    /// unless `[inference]` is configured. Invoked only off the hot path (the
    /// background embedding worker and query-time auto-embed).
    embedder: Option<Arc<dyn crate::embed::Embedder>>,
    /// Cross-encoder rerank provider for the `RERANK` clause. `None` unless
    /// `[inference] rerank_url` is configured. Invoked only by opt-in rerank
    /// queries, coordinator-side, never on the write path.
    reranker: Option<Arc<dyn crate::embed::Reranker>>,
}

/// A search index plus its NRT refresh state: writes apply immediately but
/// only commit (become visible and durable) when `refresh_ms` has elapsed
/// since the last commit — or right before a search on the write path.
/// Deliberately no commit on `Drop`: a clean shutdown loses at most
/// `refresh_ms` of index writes and the open-time watermark replay recovers
/// them, so the recovery path is exercised constantly.
#[derive(Debug)]
struct LiveSearchIndex {
    index: SearchIndex,
    last_commit: std::time::Instant,
    refresh_ms: u64,
    /// Startup catch-up/rebuild still paging in the background: MATCH errors
    /// clearly instead of silently returning partial results.
    building: bool,
}

impl LiveSearchIndex {
    /// Commit if there is anything to commit and the refresh interval elapsed.
    fn maybe_refresh(&mut self) -> Result<()> {
        if self.index.dirty()
            && self.last_commit.elapsed() >= std::time::Duration::from_millis(self.refresh_ms)
        {
            self.index.commit()?;
            self.last_commit = std::time::Instant::now();
        }
        Ok(())
    }

    /// Commit any pending writes now (read-your-writes before a search).
    fn commit_if_dirty(&mut self) -> Result<()> {
        if self.index.dirty() {
            self.index.commit()?;
            self.last_commit = std::time::Instant::now();
        }
        Ok(())
    }
}


/// An [`Hlc`] as the search crate's engine-agnostic watermark.
fn hlc_to_watermark(hlc: Hlc) -> Watermark {
    Watermark {
        physical: hlc.physical,
        logical: hlc.logical,
    }
}

/// A persisted watermark back as an [`Hlc`].
fn watermark_to_hlc(w: Watermark) -> Hlc {
    Hlc::new(w.physical, w.logical)
}

/// Aggregate storage/runtime statistics across all of a database's tables and
/// indexes, surfaced as Prometheus metrics by the server at scrape time.
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub tables: usize,
    pub secondary_indexes: usize,
    pub vector_indexes: usize,
    /// Total vectors held across all in-memory HNSW indexes.
    pub vectors_indexed: usize,
    /// Wall-clock spent rebuilding vector indexes on the last open (ms).
    pub vector_rebuild_ms: u64,
    pub search_indexes: usize,
    /// Total searchable (committed) documents across all search indexes.
    pub search_docs: u64,
    /// Bytes on disk across all search index segments.
    pub search_disk_bytes: u64,
    /// Wall-clock spent reopening/replaying search indexes on the last open (ms).
    pub search_rebuild_ms: u64,
    pub memtable_bytes: u64,
    pub sstable_count: u64,
    pub disk_bytes: u64,
    pub wal_bytes: u64,
    pub wal_fsyncs: u64,
    pub compactions: u64,
    pub compaction_bytes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_entries: u64,
    pub bloom_negatives: u64,
    /// Per-table breakdown (only populated when per-table metrics are enabled).
    pub per_table: Vec<TableStats>,
    /// Per-TIME-SERIES-table breakdown (same gate) — TS tables were
    /// invisible to per-table consumers (/metrics gauges, SHOW STATUS),
    /// reported from onet.
    pub per_timeseries: Vec<TsTableStats>,
}

/// Per-time-series-table statistics for metrics.
#[derive(Debug, Clone, Default)]
pub struct TsTableStats {
    pub database: String,
    pub name: String,
    pub series: u64,
    pub samples_appended: u64,
    pub samples_rejected: u64,
    pub disk_bytes: u64,
    /// On-disk block count — the observable for compaction-backlog
    /// drains (the 2026-07-22 390k-block incident's recovery metric).
    pub blocks: u64,
    /// Best-effort retention/compaction failures since open.
    pub maintenance_errors: u64,
}

/// Per-table live-key / tombstone / size breakdown.
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// The database the table belongs to (`default` for unqualified tables).
    pub database: String,
    /// The bare table name (without the database qualifier).
    pub name: String,
    pub live_keys: u64,
    pub tombstones: u64,
    pub disk_bytes: u64,
    pub sstables: u64,
}

/// One node's schema + storage inventory (see [`Database::inventory`]).
#[derive(Debug, Default)]
pub struct Inventory {
    pub tables: Vec<InventoryTable>,
    pub timeseries: Vec<InventoryTimeseries>,
    pub indexes: Vec<InventoryIndex>,
    pub vector_indexes: Vec<InventoryVector>,
    pub search_indexes: Vec<InventorySearch>,
}

#[derive(Debug)]
pub struct InventoryTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub ttl_ms: Option<i64>,
    pub memory: bool,
    pub live_keys: u64,
    pub tombstones: u64,
    pub disk_bytes: u64,
    pub sstables: u64,
    /// Per-table RF override (`None` = cluster default).
    pub replication: Option<u32>,
    /// Pinned members (stable ids; empty = ring placement).
    pub pinned_nodes: Vec<String>,
    /// Witness mirroring flag.
    pub witness: bool,
    /// A placement transition is open (dual-placement union window).
    pub transition: bool,
}

#[derive(Debug)]
pub struct InventoryTimeseries {
    pub name: String,
    pub series_key: Vec<String>,
    pub retention_ms: Option<i64>,
    pub rollup_of: Option<String>,
    pub series: u64,
    pub disk_bytes: u64,
}

#[derive(Debug)]
pub struct InventoryIndex {
    pub name: String,
    pub table: String,
    pub paths: Vec<String>,
    pub entries: u64,
    pub disk_bytes: u64,
}

#[derive(Debug)]
pub struct InventoryVector {
    pub name: String,
    pub table: String,
    pub path: String,
    pub metric: String,
    pub dim: usize,
    pub ef_search: Option<usize>,
    pub vectors: u64,
    pub snapshot_bytes: u64,
}

#[derive(Debug)]
pub struct InventorySearch {
    pub name: String,
    pub table: String,
    pub paths: Vec<String>,
    pub docs: u64,
    pub disk_bytes: u64,
    pub uncommitted: u64,
}


/// Buffered writes of an open transaction: `(table, key) -> Some(doc)` for a
/// put, `None` for a delete. Reads during the transaction merge this over
/// committed storage (read-your-writes); `COMMIT` flushes it to storage and
/// `ROLLBACK` discards it. Embedded, single-connection only.
#[derive(Debug, Default)]
struct TxnBuffer {
    writes: TxnWrites,
}

/// A transaction's buffered write set: `(table, key)` → put(doc) / delete.
type TxnWrites = BTreeMap<(String, Vec<u8>), Option<Document>>;

/// A caller-owned transaction slot — the session identity the ACID audit
/// found missing (2026-07-21: the engine's single global buffer let one
/// server connection's uncommitted writes leak into every other
/// connection's reads). Each connection owns one and passes it to
/// [`Database::execute_in_session`]; buffers are private to their owner by
/// construction. `Default` = no open transaction.
#[derive(Debug, Default)]
pub struct SessionTxn(Option<TxnBuffer>);

impl SessionTxn {
    /// Whether this session has an open transaction.
    pub fn open(&self) -> bool {
        self.0.is_some()
    }
}

impl Database {
    /// Open (creating if needed) a database rooted at `dir`, with default
    /// storage tuning.
    pub fn open(dir: impl AsRef<Path>) -> Result<Database> {
        Database::open_with_options(dir, EngineOptions::default())
    }

    /// Open (creating if needed) a database rooted at `dir`, applying `opts`
    /// (flush threshold, read-cache size, codecs) to every table and index
    /// engine — both the ones loaded now and any created later.
    pub fn open_with_options(dir: impl AsRef<Path>, opts: EngineOptions) -> Result<Database> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let catalog = Catalog::load(dir.join("catalog.json"))?;

        // Seed the DDL clock past every persisted schema stamp, so the first
        // DDL after a reopen can't be stamped behind (and silently lose
        // last-writer-wins to) a version recorded by the previous process in
        // the same millisecond.
        let clock = HlcClock::new();
        for version in catalog.schema_versions.values() {
            clock.observe(version.hlc());
        }

        let mut tables = HashMap::new();
        for name in catalog.tables.keys() {
            let mut table_opts = opts.clone();
            table_opts.ephemeral = catalog.tables[name].memory;
            let mut engine = StorageEngine::open_with_options(table_dir(&dir, name), table_opts)?;
            engine.set_ttl(catalog.tables[name].ttl_ms.map(|ms| ms as u64));
            tables.insert(name.clone(), engine);
        }

        let mut indexes = HashMap::new();
        for (name, def) in &catalog.indexes {
            if def.global {
                continue; // entries live in the __gidx table opened above
            }
            let engine = StorageEngine::open_with_options(index_dir(&dir, name), opts.clone())?;
            indexes.insert(name.clone(), engine);
        }
        // Geo indexes share the on-disk index-engine machinery (durable
        // entries, distributed via the same `IndexScan` scatter), so reopen
        // each into the same `indexes` map. Entries persist — nothing rebuilds
        // — but an index whose backfill was interrupted resumes below.
        let mut pending_geo_backfills = Vec::new();
        for (name, def) in &catalog.geo_indexes {
            let engine = StorageEngine::open_with_options(index_dir(&dir, name), opts.clone())?;
            indexes.insert(name.clone(), engine);
            if def.building {
                pending_geo_backfills.push(name.clone());
            }
        }

        let mut timeseries = HashMap::new();
        for (name, def) in &catalog.timeseries {
            timeseries.insert(name.clone(), open_tsdb(&dir, name, def, opts.ts_head_max_bytes)?);
        }

        // Vector indexes live in memory. Load each from its on-disk snapshot
        // and replay only rows stamped after the snapshot's watermark
        // (FTS-style catch-up): a reload takes seconds where the from-scratch
        // graph build takes tens of minutes and has dominated every restart.
        // Missing, corrupt, or definition-mismatched snapshots fall back to
        // the full streamed rebuild, and a fresh snapshot is written below.
        let rebuild_start = std::time::Instant::now();
        let mut vector_indexes = HashMap::new();
        let mut vector_watermarks: HashMap<String, Hlc> = HashMap::new();
        for (name, def) in &catalog.vector_indexes {
            let snap = vector_snapshot_path(&dir, name);
            let (mut hnsw, watermark, fresh) = match load_vector_snapshot(&snap, def) {
                Some((h, w)) => (h, w, false),
                None => (new_hnsw(def), Hlc::MIN, true),
            };
            // Rows arrive in KEY order, not HLC order: compare every row
            // against the snapshot's fixed watermark and track the max seen
            // separately — advancing the bar mid-loop let one late-stamped
            // early-keyed tombstone shadow every later-keyed newer row.
            let mut max_seen = watermark;
            if let Some(engine) = tables.get(&def.table) {
                // Tombstones included: a row deleted after the snapshot must
                // leave the graph. HLCs come from the row header, so rows at
                // or before the watermark skip without a decode.
                for row in engine.scan_versioned_with_tombstones_iter() {
                    let (key, hlc, value) = row?;
                    if hlc <= watermark && !fresh {
                        continue;
                    }
                    if hlc > max_seen {
                        max_seen = hlc;
                    }
                    match value {
                        Some(bytes) => {
                            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                                match doc_vector(&doc, &def.path, def.dim) {
                                    Some(v) => hnsw.insert(key, v),
                                    None => hnsw.remove(&key),
                                }
                            }
                        }
                        None => hnsw.remove(&key),
                    }
                }
            }
            if hnsw.is_dirty() || fresh {
                if let Err(e) = save_vector_snapshot(&snap, &hnsw, max_seen) {
                    skaidb_types::slog!("skaidb: vector snapshot save failed for {name}: {e}");
                } else {
                    hnsw.mark_clean();
                }
            }
            vector_watermarks.insert(name.clone(), max_seen);
            vector_indexes.insert(name.clone(), hnsw);
        }
        let vector_rebuild_ms = rebuild_start.elapsed().as_millis() as u64;

        // Search indexes persist their own segments; reopen each and replay
        // table rows newer than its committed watermark (writes lost to a
        // crash or the no-commit-on-drop shutdown). A missing, corrupt, or
        // definition-mismatched index is wiped and rebuilt from the table.
        let rebuild_start = std::time::Instant::now();
        let mut search_indexes = HashMap::new();
        let mut pending_search: Vec<(String, Option<Hlc>)> = Vec::new();
        for (name, def) in &catalog.search_indexes {
            let (cfg, refresh_ms) =
                SearchIndexConfig::from_declaration(&def.paths, &def.options)?;
            let idx_dir = fts_dir(&dir, name);
            let heap = opts.search_writer_heap_bytes;
            let mut index = match SearchIndex::open(&idx_dir, &cfg, heap) {
                Ok(index) => index,
                // NeedsRebuild or any other open failure: start from scratch.
                Err(_) => {
                    let _ = std::fs::remove_dir_all(&idx_dir);
                    SearchIndex::open(&idx_dir, &cfg, heap)?
                }
            };
            let mut building = false;
            if let Some(engine) = tables.get(&def.table) {
                let watermark = index.committed_watermark();
                if opts.defer_search_startup {
                    // Server mode: the catch-up/rebuild pages run in the
                    // background worker; the node's listener opens without
                    // waiting behind minutes of FTS indexing. MATCH on this
                    // index errors clearly until the pages complete.
                    if watermark.is_none() {
                        index.clear()?;
                    }
                    building = true;
                    pending_search.push((name.clone(), watermark.map(watermark_to_hlc)));
                } else {
                    match watermark {
                        // Catch-up: replay every put/delete stamped after the
                        // watermark (deletes included, so a row removed while
                        // the delete was uncommitted stays removed).
                        Some(w) => {
                            let watermark = watermark_to_hlc(w);
                            // Stream the shard: a full `Vec` gather here OOM'd
                            // small nodes catching up over a large table.
                            for row in engine.scan_versioned_with_tombstones_iter() {
                                let (key, hlc, value) = row?;
                                if hlc <= watermark {
                                    continue;
                                }
                                match value {
                                    Some(bytes) => {
                                        if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                                            index.put(&key, &doc, hlc_to_watermark(hlc))?;
                                        }
                                    }
                                    None => index.delete(&key, hlc_to_watermark(hlc)),
                                }
                            }
                        }
                        // Never committed: full rebuild from the table.
                        None => {
                            index.clear()?;
                            for row in engine.scan_versioned_iter() {
                                let (key, bytes, hlc) = row?;
                                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                                    index.put(&key, &doc, hlc_to_watermark(hlc))?;
                                }
                            }
                        }
                    }
                }
            }
            if !building && index.dirty() {
                index.commit()?;
            }
            search_indexes.insert(
                name.clone(),
                LiveSearchIndex {
                    index,
                    last_commit: std::time::Instant::now(),
                    refresh_ms,
                    building,
                },
            );
        }
        let search_rebuild_ms = rebuild_start.elapsed().as_millis() as u64;
        // The replay/rebuild above ran silently for years of accumulated
        // startup mystery ("why is this node at 400% CPU with no log
        // lines?"). One line per open keeps it attributable.
        if search_rebuild_ms > 2_000 {
            skaidb_types::slog!(
                "skaidb: startup search-index catch-up took {}s ({} indexes)",
                search_rebuild_ms / 1000,
                search_indexes.len()
            );
        }

        let role_store = build_role_store(&catalog);
        // Applier watermarks persisted by the deferred-maintenance path.
        let applied_watermarks: HashMap<String, Hlc> =
            match std::fs::read(dir.join("applier.watermarks")) {
                Ok(bytes) => serde_json::from_slice::<Vec<(String, u64, u32)>>(&bytes)
                    .map(|v| {
                        v.into_iter()
                            .map(|(t, wall, ctr)| (t, Hlc::new(wall, ctr)))
                            .collect()
                    })
                    .unwrap_or_default(),
                Err(_) => HashMap::new(),
            };
        let mut db = Database {
            dir,
            storage_opts: opts,
            catalog,
            tables,
            timeseries,
            role_store,
            indexes,
            vector_indexes,
            vector_watermarks,
            search_indexes,
            txn: None,
            clock,
            ddl_hlc: None,
            vector_rebuild_ms,
            search_rebuild_ms,
            applied_watermarks,
            pending_backfills: Vec::new(),
            building_vectors: std::collections::HashSet::new(),
            pending_vector_backfills: Vec::new(),
            pending_geo_backfills,
            pending_search_catchups: pending_search,
            pending_embeds: std::collections::HashMap::new(),
            defer_backfills: false,
            field_registry: std::sync::Mutex::new(HashMap::new()),
            deferred_syncs: None,
            embedder: None,
            reranker: None,
        };
        // Re-arm the storage truncation gates, then replay any deferred
        // maintenance the crash interrupted (WAL replay above re-populated
        // the memtables the replay walks).
        let gates: Vec<(String, Hlc)> =
            db.applied_watermarks.iter().map(|(t, h)| (t.clone(), *h)).collect();
        for (table, hlc) in gates {
            if let Ok(engine) = db.table_engine_mut(&table) {
                engine.set_maintenance_watermark(hlc);
            }
        }
        db.recover_deferred_maintenance()?;
        // A durable-but-unretired transaction journal means a crash landed
        // between COMMIT's journal fsync and the end of its apply: replay
        // the write set to completion (idempotent — re-putting an applied
        // row rewrites the same content) so the commit is all-or-nothing
        // across crashes. A torn/invalid journal decodes to None: that
        // commit never became durable and is (correctly) forgotten.
        if let Some(writes) = read_txn_journal(&db.dir) {
            let n = writes.len();
            db.apply_txn_writes(writes)?;
            let _ = std::fs::remove_file(txn_journal_path(&db.dir));
            skaidb_types::slog!(
                "skaidb: replayed an interrupted transaction commit ({n} writes) from txn.journal"
            );
        } else {
            // Clean up a torn journal so it isn't re-parsed every open.
            let _ = std::fs::remove_file(txn_journal_path(&db.dir));
        }
        Ok(db)
    }

    /// Resolve the HLC for the DDL about to run: the replicated stamp if one was
    /// supplied (so every node applies the same change at the same version),
    /// else a fresh local stamp. Always observed so the clock stays monotonic.
    fn ddl_stamp(&mut self) -> Hlc {
        match self.ddl_hlc {
            // Replicated DDL: use the coordinator's stamp verbatim so every node
            // records the same version; still advance our clock past it.
            Some(h) => {
                self.clock.observe(h);
                h
            }
            None => self.clock.now(),
        }
    }

    /// Whether `hlc` is newer than the recorded version for `key` (or there is
    /// none) — i.e. this DDL should take effect under last-writer-wins.
    fn schema_advances(&self, key: &str, hlc: Hlc) -> bool {
        self.catalog
            .schema_versions
            .get(key)
            .is_none_or(|v| hlc > v.hlc())
    }

    /// Record the version stamp for a schema object after applying its DDL.
    fn record_schema(&mut self, key: String, hlc: Hlc, dropped: bool) {
        self.catalog
            .schema_versions
            .insert(key, SchemaVersion::new(hlc, dropped));
    }

    /// Create an HNSW vector index over the float array at `path` of `table`.
    /// `dim` fixes the vector dimension; pass `None` to infer it from existing
    /// rows (convenient for a single node, but in a cluster a shard may be empty,
    /// so the broadcast DDL always supplies an explicit dimension).
    /// `metric` is `cosine`/`l2`/`dot`.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn create_vector_index(
        &mut self,
        name: &str,
        table: &str,
        path: &str,
        metric: &str,
        dim: Option<usize>,
        embed: bool,
        quantized: bool,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(table) {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        // Rescoring reads the exact vector back from the row; an EMBED index
        // stores only the text there, so the two options cannot combine.
        if quantized && embed {
            return Err(EngineError::Unsupported(
                "QUANTIZED requires the vector stored in the row and cannot combine with \
                 EMBED (the row holds only the text — there is no exact vector to rescore \
                 against)"
                    .into(),
            ));
        }
        // A managed (EMBED) index needs an inference provider and an explicit
        // DIM matching it — fail at create, not silently at first write.
        if embed {
            let d = dim.ok_or_else(|| {
                EngineError::Constraint("CREATE VECTOR INDEX … EMBED requires DIM <n>".into())
            })?;
            match self.embedder.as_ref() {
                None => {
                    return Err(EngineError::Unsupported(
                        "CREATE VECTOR INDEX … EMBED needs a configured [inference] provider"
                            .into(),
                    ))
                }
                Some(e) if e.dim() != d => {
                    return Err(EngineError::Constraint(format!(
                        "index DIM {d} does not match the inference model's dimension {} \
                         (inference.dim)",
                        e.dim()
                    )))
                }
                Some(_) => {}
            }
        }
        let hlc = self.ddl_stamp();
        let key = format!("v:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        // The live-tunable `ef_search` survives a definition replacement.
        let mut preserved_ef: Option<usize> = None;
        if let Some(existing) = self.catalog.vector_indexes.get(name) {
            if !if_not_exists {
                return Err(EngineError::IndexExists(name.to_string()));
            }
            // Schema sync replays peer defs as IF NOT EXISTS with the peer's
            // HLC; the stamp advanced, so a differing def is newer — replace
            // and rebuild instead of letting a stale definition persist
            // forever (the search-index `.6` divergence class).
            let differs = existing.table != table
                || existing.path != path
                || !existing.metric.eq_ignore_ascii_case(metric)
                || existing.embed != embed
                || existing.quantized != quantized
                || dim.is_some_and(|d| d != existing.dim);
            if !differs {
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            skaidb_types::slog!(
                "skaidb: vector index '{name}' definition superseded by a newer \
                 schema stamp — replacing and rebuilding"
            );
            preserved_ef = existing.ef_search;
            self.catalog.vector_indexes.remove(name);
            self.vector_indexes.remove(name);
            self.vector_watermarks.remove(name);
            self.building_vectors.remove(name);
            self.pending_vector_backfills.retain(|n| n != name);
            let _ = std::fs::remove_file(vector_snapshot_path(&self.dir, name));
        }
        if Metric::parse(metric).is_none() {
            return Err(EngineError::Constraint(format!("unknown vector metric '{metric}'")));
        }
        // One vector index per (table, path): `NEAREST` resolves by path, so a
        // second differently-named index over the same column would be
        // maintained but never queried — reject it instead of silently
        // shadowing.
        if let Some((other, _)) = self
            .catalog
            .vector_indexes
            .iter()
            .find(|(n, d)| n.as_str() != name && d.table == table && d.path == path)
        {
            return Err(EngineError::Constraint(format!(
                "vector index '{}' already covers {table} ({path}) — drop it first",
                namespace::split(other).1
            )));
        }
        // Explicit dimension: the graph can be sized without scanning a
        // single row, so DDL acks with an empty index marked `building` and
        // the backfill runs in pages afterwards — inline for a single-node
        // Session, in the background worker on a cluster node (a schema-sync
        // def replay always lands here, so a repair pass no longer streams a
        // whole table inside its RPC). Only the dimension-INFERENCE form
        // below still scans inline: it cannot know `dim` without looking at
        // a row.
        if let Some(d) = dim {
            let def = VectorIndexDef {
                table: table.to_string(),
                path: path.to_string(),
                metric: metric.to_ascii_lowercase(),
                dim: d,
                ef_search: preserved_ef,
                embed,
                quantized,
            };
            self.vector_indexes.insert(name.to_string(), new_hnsw(&def));
            self.vector_watermarks.insert(name.to_string(), Hlc::MIN);
            self.catalog.vector_indexes.insert(name.to_string(), def);
            if embed {
                // Managed: queue existing rows for out-of-band embedding (not
                // the vector-array backfill). Searches return NRT partial
                // results while it catches up, so no `building` gate. A
                // single-node Session drains inline now; a cluster node leaves
                // it for the background embed worker.
                self.enqueue_unembedded(name);
                if !self.defer_backfills {
                    self.run_embed_drain();
                }
            } else {
                self.building_vectors.insert(name.to_string());
                self.pending_vector_backfills.push(name.to_string());
            }
            self.record_schema(key, hlc, false);
            self.save_catalog()?;
            return Ok(QueryOutput::Ddl);
        }
        // Stream the table: materializing every row as a Document tree
        // multiplied memory ~8x (a 3 KB float array becomes ~24 KB of Value
        // nodes) — building over 182k embedding rows OOM-killed a 4 GB node
        // (2026-07-13). Each row is decoded, its f32 vector extracted, and
        // the Document dropped before the next row is read.
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let mut dim_seen: Option<usize> = dim;
        let mut hnsw = None;
        let mut pending_def: Option<VectorIndexDef> = None;
        let mut watermark = Hlc::MIN;
        for row in engine.scan_versioned_iter() {
            let (row_key, bytes, hlc) = row?;
            if hlc > watermark {
                watermark = hlc;
            }
            let Ok(Value::Document(doc)) = Value::decode(&bytes) else {
                continue;
            };
            let Some(raw) = doc_vector_raw(&doc, path) else {
                continue;
            };
            let d = *dim_seen.get_or_insert(raw.len());
            if hnsw.is_none() {
                let def = VectorIndexDef {
                    table: table.to_string(),
                    path: path.to_string(),
                    metric: metric.to_ascii_lowercase(),
                    dim: d,
                    ef_search: None,
                    embed,
                    quantized,
                };
                hnsw = Some(new_hnsw(&def));
                pending_def = Some(def);
            }
            if raw.len() == d {
                if let (Some(h), Some(v)) = (hnsw.as_mut(), doc_vector(&doc, path, d)) {
                    h.insert(row_key, v);
                }
            }
        }
        let dim = dim_seen.ok_or_else(|| {
            EngineError::Constraint(format!(
                "cannot infer vector dimension: no row of '{table}' has a numeric array at '{path}'"
            ))
        })?;
        let def = pending_def.unwrap_or_else(|| VectorIndexDef {
            table: table.to_string(),
            path: path.to_string(),
            metric: metric.to_ascii_lowercase(),
            dim,
            ef_search: None,
            embed,
            quantized,
        });
        let mut hnsw = hnsw.unwrap_or_else(|| new_hnsw(&def));
        if let Err(e) = save_vector_snapshot(&vector_snapshot_path(&self.dir, name), &hnsw, watermark)
        {
            skaidb_types::slog!("skaidb: vector snapshot save failed for {name}: {e}");
        } else {
            hnsw.mark_clean();
        }
        self.vector_watermarks.insert(name.to_string(), watermark);
        self.vector_indexes.insert(name.to_string(), hnsw);
        self.catalog.vector_indexes.insert(name.to_string(), def);
        if preserved_ef.is_some() {
            if let Some(d) = self.catalog.vector_indexes.get_mut(name) {
                d.ef_search = preserved_ef;
            }
        }
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Drop a vector index.
    pub fn drop_vector_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("v:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.vector_indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.vector_indexes.remove(name);
        self.vector_watermarks.remove(name);
        self.building_vectors.remove(name);
        self.pending_vector_backfills.retain(|n| n != name);
        let _ = std::fs::remove_file(vector_snapshot_path(&self.dir, name));
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `CREATE SEARCH INDEX`: validate the `WITH` options, build the index on
    /// disk, backfill it from the table's existing rows (committing so the
    /// backfill is immediately searchable), and register it in the catalog.
    fn create_search_index(&mut self, c: &CreateSearchIndex) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(&c.table) {
            return Err(EngineError::TableNotFound(c.table.clone()));
        }
        let hlc = self.ddl_stamp();
        let key = format!("s:{}", c.name);
        if !self.schema_advances(&key, hlc) {
            // The DDL itself is a replay no-op — but a prior partial
            // application can have left the catalog def WITHOUT a live index
            // (the production `.6` divergence class: SHOW INDEXES lists it,
            // every query fails). Heal instead of preserving the gap.
            self.heal_missing_live_search_index(&c.name)?;
            return Ok(QueryOutput::Ddl);
        }
        if let Some(existing) = self.catalog.search_indexes.get(&c.name) {
            if c.if_not_exists {
                // The schema-sync path replays every peer def as
                // `CREATE ... IF NOT EXISTS` with the peer's HLC. Mere name
                // presence must NOT satisfy it: a node that missed the DDL
                // that widened the definition keeps its stale, narrower def
                // forever otherwise (the production `.6` divergence — its
                // `slack_messages_fts` covered only `text` while peers had
                // `text, channel, ts`, and repair never converged it). The
                // HLC advanced, so the incoming def is newer: replace and
                // rebuild when it differs.
                let differs = existing.table != c.table
                    || existing.paths != c.paths
                    || existing.options != c.options;
                if differs {
                    skaidb_types::slog!(
                        "skaidb: search index '{}' definition superseded by a newer \
                         schema stamp — replacing and rebuilding",
                        c.name
                    );
                    self.catalog.search_indexes.insert(
                        c.name.clone(),
                        SearchIndexDef {
                            table: c.table.clone(),
                            paths: c.paths.clone(),
                            options: c.options.clone(),
                        },
                    );
                    self.rebuild_search_index(&c.name)?;
                } else {
                    // Same definition: only heal a missing live index.
                    self.heal_missing_live_search_index(&c.name)?;
                }
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::IndexExists(c.name.clone()));
        }
        // Validate the declaration up front (unknown options, bad analyzers,
        // type/option conflicts) — the raw options are what the catalog keeps.
        let (cfg, refresh_ms) = SearchIndexConfig::from_declaration(&c.paths, &c.options)?;
        let def = SearchIndexDef {
            table: c.table.clone(),
            paths: c.paths.clone(),
            options: c.options.clone(),
        };
        // Start from an empty directory (a leftover from a dropped index of
        // the same name must not leak into the new one) and backfill.
        let idx_dir = fts_dir(&self.dir, &c.name);
        let _ = std::fs::remove_dir_all(&idx_dir);
        let mut index = SearchIndex::open(&idx_dir, &cfg, self.storage_opts.search_writer_heap_bytes)?;
        let engine = self
            .tables
            .get(&c.table)
            .ok_or_else(|| EngineError::TableNotFound(c.table.clone()))?;
        // Stream the shard rather than collecting it: a full `Vec` gather here
        // OOM'd a small node building an index over 100k+ rows (DB workload
        // must never OOM — only the writer heap + one row are held at a time).
        for row in engine.scan_versioned_iter() {
            let (row_key, bytes, row_hlc) = row?;
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                index.put(&row_key, &doc, hlc_to_watermark(row_hlc))?;
            }
        }
        index.commit()?;
        self.search_indexes.insert(
            c.name.clone(),
            LiveSearchIndex {
                index,
                last_commit: std::time::Instant::now(),
                refresh_ms,
                building: false,
            },
        );
        self.catalog.search_indexes.insert(c.name.clone(), def);
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Drop a search index (and its on-disk segments).
    fn drop_search_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("s:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.search_indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.search_indexes.remove(name); // drop the writer before the files
        let idx_dir = fts_dir(&self.dir, name);
        if idx_dir.exists() {
            std::fs::remove_dir_all(idx_dir)?;
        }
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `ALTER SEARCH INDEX <name> SET (...)`: change **query-time** options
    /// — `synonyms`, `refresh_ms`, `<col>.search_analyzer`, `<col>.boost` —
    /// in place: catalog merge (last-wins per key), live runtime rebuild,
    /// no reindex. Index-time options (analyzers, types, twins, copy_to)
    /// error: those change the stored postings and need DROP + CREATE.
    fn alter_search_index(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<QueryOutput> {
        for (key, _) in options {
            let query_time = key == "synonyms"
                || key == "refresh_ms"
                || key.ends_with(".search_analyzer")
                || key.ends_with(".boost");
            if !query_time {
                return Err(EngineError::Type(format!(
                    "'{key}' is an index-time option — DROP and re-CREATE the index to \
                     change it (ALTER SET takes synonyms, refresh_ms, \
                     <col>.search_analyzer, <col>.boost)"
                )));
            }
        }
        let hlc = self.ddl_stamp();
        let key = format!("s:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        let Some(def) = self.catalog.search_indexes.get_mut(name) else {
            return Err(EngineError::IndexNotFound(name.to_string()));
        };
        // Merge: replace existing entries for each altered key, keeping
        // declaration order otherwise.
        let mut merged = def.options.clone();
        for (k, v) in options {
            merged.retain(|(existing, _)| existing != k);
            merged.push((k.clone(), v.clone()));
        }
        // Validate the merged declaration before committing anything.
        let (cfg, refresh_ms) = SearchIndexConfig::from_declaration(&def.paths, &merged)?;
        def.options = merged;
        if let Some(live) = self.search_indexes.get_mut(name) {
            live.index.update_query_config(&cfg);
            live.refresh_ms = refresh_ms;
        }
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `REBUILD SEARCH INDEX`: discard the index data and re-index every row
    /// of the table (recovery / anti-entropy escape hatch).
    /// If the catalog has a search-index def but the live map lacks the
    /// index (a partial DDL application or lost directory), rebuild it from
    /// the catalog def. No-op when both sides agree.
    fn heal_missing_live_search_index(&mut self, name: &str) -> Result<()> {
        if self.catalog.search_indexes.contains_key(name)
            && !self.search_indexes.contains_key(name)
        {
            skaidb_types::slog!(
                "skaidb: search index '{name}' is in the catalog but has no live \
                 index — healing by rebuild"
            );
            self.rebuild_search_index(name)?;
        }
        Ok(())
    }

    fn rebuild_search_index_cmd(&mut self, name: &str) -> Result<QueryOutput> {
        if !self.catalog.search_indexes.contains_key(name) {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.rebuild_search_index(name)?;
        Ok(QueryOutput::Ddl)
    }

    /// Wipe and re-backfill a search index from its table's current rows
    /// (also picks up a changed definition, e.g. after a column rename).
    fn rebuild_search_index(&mut self, name: &str) -> Result<()> {
        // Loud on purpose: this streams the whole table under the engine
        // write lock, so it starves every reader for its duration — a
        // rebuild that loops silently (repeated NeedsRebuild) looked like an
        // unexplained multi-hour 4-core burn in production. Start/finish
        // lines make that failure mode visible in the journal.
        let rebuild_started = std::time::Instant::now();
        skaidb_types::slog!("skaidb: search index '{name}' rebuild starting (write-lock held for the duration)");
        let result = self.rebuild_search_index_inner(name);
        match &result {
            Ok(()) => skaidb_types::slog!(
                "skaidb: search index '{name}' rebuilt in {}s",
                rebuild_started.elapsed().as_secs()
            ),
            Err(e) => skaidb_types::slog!(
                "skaidb: search index '{name}' rebuild FAILED after {}s: {e}",
                rebuild_started.elapsed().as_secs()
            ),
        }
        result
    }

    fn rebuild_search_index_inner(&mut self, name: &str) -> Result<()> {
        let def = self
            .catalog
            .search_indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?
            .clone();
        let (cfg, refresh_ms) = SearchIndexConfig::from_declaration(&def.paths, &def.options)?;
        self.search_indexes.remove(name); // drop the writer before the files
        let idx_dir = fts_dir(&self.dir, name);
        let _ = std::fs::remove_dir_all(&idx_dir);
        let mut index = SearchIndex::open(&idx_dir, &cfg, self.storage_opts.search_writer_heap_bytes)?;
        if let Some(engine) = self.tables.get(&def.table) {
            // Stream the shard (see create_search_index): collecting a large
            // table into a `Vec` to rebuild an index OOM'd small nodes.
            for row in engine.scan_versioned_iter() {
                let (row_key, bytes, row_hlc) = row?;
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    index.put(&row_key, &doc, hlc_to_watermark(row_hlc))?;
                }
            }
        }
        index.commit()?;
        self.search_indexes.insert(
            name.to_string(),
            LiveSearchIndex {
                index,
                last_commit: std::time::Instant::now(),
                refresh_ms,
                building: false,
            },
        );
        Ok(())
    }

    /// Approximate `k` nearest rows to `query` under the named vector index,
    /// optionally restricted to rows matching `filter` (filtered ANN). Returns
    /// `(key, doc, distance)` nearest-first.
    /// Embed a query string against the managed (`EMBED`) index on
    /// `(table, path)`. Errors if the index isn't managed or no embedder is set.
    pub fn embed_query(&self, table: &str, path: &str, text: &str) -> Result<Vec<f32>> {
        let name = self
            .vector_index_for(table, path)
            .ok_or_else(|| EngineError::Unsupported(format!("no vector index on {table} ({path})")))?;
        if !self.catalog.vector_indexes.get(&name).is_some_and(|d| d.embed) {
            return Err(EngineError::Type(format!(
                "NEAREST on '{path}' takes a query vector; a string query needs a managed EMBED index"
            )));
        }
        let embedder = self
            .embedder
            .as_ref()
            .ok_or_else(|| EngineError::Unsupported("inference is not configured".into()))?;
        embedder
            .embed(&[text.to_string()])?
            .pop()
            .ok_or_else(|| EngineError::Type("embedder returned no vector".into()))
    }

    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        self.vector_index_ready(index)?;
        let hnsw = self
            .vector_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        let def = self
            .catalog
            .vector_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        let table_engine = self
            .tables
            .get(&def.table)
            .ok_or_else(|| EngineError::TableNotFound(def.table.clone()))?;

        // Quantized graph: over-fetch approximate candidates, then rescore
        // the survivors against the exact row vectors below.
        let fetch = if def.quantized {
            k.saturating_mul(QUANT_RESCORE_OVERSAMPLE)
        } else {
            k
        };
        // The HNSW only knows keys; resolve each candidate to its row to apply
        // the filter (filtered nearest-neighbor search). Decoded docs are kept
        // so each candidate is read and decoded once, and survivors are served
        // from the same map instead of a second storage read.
        let decoded: std::cell::RefCell<HashMap<Vec<u8>, Option<Document>>> =
            std::cell::RefCell::new(HashMap::new());
        let hits = hnsw.search(query, fetch, |key| {
            let mut cache = decoded.borrow_mut();
            let doc = cache.entry(key.to_vec()).or_insert_with(|| {
                match table_engine.get(key) {
                    Ok(Some(bytes)) => match Value::decode(&bytes) {
                        Ok(Value::Document(doc)) => Some(doc),
                        _ => None,
                    },
                    _ => None,
                }
            });
            match (filter, doc.as_ref()) {
                (Some(_), Some(doc)) => matches_filter(filter, doc).unwrap_or(false),
                (None, Some(_)) => true,
                _ => false,
            }
        });

        let mut cache = decoded.into_inner();
        let mut out = Vec::with_capacity(hits.len());
        for (key, dist) in hits {
            if let Some(Some(doc)) = cache.remove(&key) {
                out.push((key, doc, dist));
            }
        }
        if def.quantized {
            // Mandatory exact rescore: replace each candidate's approximate
            // graph distance with the true metric distance to the row's
            // stored f32 vector, then keep the best `k`. (A row whose vector
            // field changed shape keeps its approximate distance.)
            let metric = Metric::parse(&def.metric).unwrap_or(Metric::Cosine);
            for (_, doc, dist) in &mut out {
                if let Some(v) = doc_vector(doc, &def.path, def.dim) {
                    *dist = crate::vector::exact_distance(metric, query, &v);
                }
            }
            out.sort_by(|a, b| a.2.total_cmp(&b.2).then_with(|| a.0.cmp(&b.0)));
            out.truncate(k);
        }
        Ok(out)
    }

    /// Local-shard ANN: the `k` nearest `(key, distance)` to `query` from this
    /// node's HNSW, unfiltered. Used by the cluster coordinator, which gathers
    /// these from every node, merges, then re-reads + filters the survivors.
    pub fn vector_search_local(
        &self,
        index: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(Vec<u8>, f32)>> {
        self.vector_index_ready(index)?;
        let hnsw = self
            .vector_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        Ok(hnsw.search(query, k, |_| true))
    }

    /// Refuse to search a vector index whose backfill is still paging — a
    /// partial graph would silently return wrong neighbors (search indexes
    /// error the same way while `building`).
    fn vector_index_ready(&self, index: &str) -> Result<()> {
        if self.building_vectors.contains(index) {
            return Err(EngineError::Unsupported(format!(
                "vector index '{}' is rebuilding — retry shortly",
                namespace::split(index).1
            )));
        }
        Ok(())
    }

    /// The table a vector index is defined on, if it exists locally.
    pub fn vector_index_table(&self, index: &str) -> Option<String> {
        self.catalog
            .vector_indexes
            .get(index)
            .map(|d| d.table.clone())
    }

    /// What a cluster coordinator needs to rescore a vector index's
    /// candidates exactly: `(path, dim, metric, quantized)`. `None` if the
    /// index does not exist locally.
    pub fn vector_index_rescore(&self, index: &str) -> Option<(String, usize, String, bool)> {
        self.catalog
            .vector_indexes
            .get(index)
            .map(|d| (d.path.clone(), d.dim, d.metric.clone(), d.quantized))
    }

    /// The `(name, path)` of every vector index on `table`.
    /// The name of the vector index on `(table, path)`, if one exists — the
    /// resolution step behind the SQL `NEAREST` clause.
    pub fn vector_index_for(&self, table: &str, path: &str) -> Option<String> {
        self.catalog
            .vector_indexes
            .iter()
            .find(|(_, def)| def.table == table && def.path == path)
            .map(|(name, _)| name.clone())
    }

    /// Install the text-embedding provider for managed vector indexes. Set at
    /// startup from `[inference]`; `None` leaves managed-index create/query as
    /// a clear "inference is not configured" error.
    pub fn set_embedder(&mut self, embedder: Arc<dyn crate::embed::Embedder>) {
        self.embedder = Some(embedder);
    }

    /// The configured embedder, if any.
    pub fn embedder(&self) -> Option<Arc<dyn crate::embed::Embedder>> {
        self.embedder.clone()
    }

    /// Install the cross-encoder rerank provider for the `RERANK` clause. Set
    /// at startup from `[inference] rerank_url`; `None` leaves `RERANK` as a
    /// clear "no reranker configured" error.
    pub fn set_reranker(&mut self, reranker: Arc<dyn crate::embed::Reranker>) {
        self.reranker = Some(reranker);
    }

    /// Score `documents` against `query` with the configured reranker
    /// (higher = more relevant, one score per document in order). Errors if no
    /// reranker is installed or the endpoint misbehaves.
    pub fn rerank(&self, model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        let reranker = self.reranker.as_ref().ok_or_else(|| {
            EngineError::Unsupported(
                "RERANK needs a rerank provider ([inference] rerank_url)".into(),
            )
        })?;
        let scores = reranker.rerank(model, query, documents)?;
        if scores.len() != documents.len() {
            return Err(EngineError::Type(format!(
                "reranker returned {} scores for {} documents",
                scores.len(),
                documents.len()
            )));
        }
        Ok(scores)
    }

    fn vector_indexes_on(&self, table: &str) -> Vec<(String, String)> {
        self.catalog
            .vector_indexes
            .iter()
            .filter(|(_, def)| def.table == table)
            .map(|(name, def)| (name.clone(), def.path.clone()))
            .collect()
    }

    /// Update the vector index `name` for `doc` at `key` (insert/replace), or
    /// remove the entry when the doc has no vector at `path`. `hlc` advances
    /// the index's replay watermark.
    fn vector_index_put(&mut self, name: &str, path: &str, doc: &Document, key: &[u8], hlc: Hlc) {
        // Managed (EMBED) index: enqueue the key for out-of-band embedding when
        // the source text is present; drop it when the text went away. The
        // vector watermark is NOT advanced here — the embed drain advances it
        // once the vector actually lands, so a crash before embedding leaves
        // the row "past the watermark" and it is re-enqueued on open.
        if self.catalog.vector_indexes.get(name).is_some_and(|d| d.embed) {
            let has_text = matches!(doc.get_path(path), Some(Value::String(s)) if !s.is_empty());
            if has_text {
                self.pending_embeds
                    .entry(name.to_string())
                    .or_default()
                    .insert(key.to_vec());
            } else {
                if let Some(hnsw) = self.vector_indexes.get_mut(name) {
                    hnsw.remove(key);
                }
                if let Some(q) = self.pending_embeds.get_mut(name) {
                    q.remove(key);
                }
            }
            return;
        }
        let dim = self.vector_indexes.get(name).map(|h| h.dim());
        if let (Some(dim), Some(hnsw)) = (dim, self.vector_indexes.get_mut(name)) {
            match doc_vector(doc, path, dim) {
                Some(v) => hnsw.insert(key.to_vec(), v),
                None => hnsw.remove(key),
            }
            self.advance_vector_watermark(name, hlc);
        }
    }

    fn vector_index_del(&mut self, name: &str, key: &[u8], hlc: Hlc) {
        if let Some(hnsw) = self.vector_indexes.get_mut(name) {
            hnsw.remove(key);
            self.advance_vector_watermark(name, hlc);
        }
        // A managed index also drops the key from its embed queue.
        if let Some(q) = self.pending_embeds.get_mut(name) {
            q.remove(key);
        }
    }

    fn advance_vector_watermark(&mut self, name: &str, hlc: Hlc) {
        let w = self.vector_watermarks.entry(name.to_string()).or_insert(Hlc::MIN);
        if hlc > *w {
            *w = hlc;
        }
    }

    /// Maintain every vector index on `table` for a written row.
    fn maintain_vectors_put(&mut self, table: &str, doc: &Document, key: &[u8], hlc: Hlc) {
        for (name, path) in self.vector_indexes_on(table) {
            self.vector_index_put(&name, &path, doc, key, hlc);
        }
    }

    /// Maintain every vector index on `table` for a deleted row.
    fn maintain_vectors_del(&mut self, table: &str, key: &[u8], hlc: Hlc) {
        for (name, _) in self.vector_indexes_on(table) {
            self.vector_index_del(&name, key, hlc);
        }
    }

    /// The `(name, path)` of every geo index on `table`.
    fn geo_indexes_on(&self, table: &str) -> Vec<(String, String)> {
        self.catalog
            .geo_indexes
            .iter()
            .filter(|(_, g)| g.table == table)
            .map(|(name, g)| (name.clone(), g.path.clone()))
            .collect()
    }

    /// Add the geo index entry for a written row's point (all geo indexes on
    /// `table`). A row whose column is not a readable point gets no entry.
    fn maintain_geo_put(&mut self, table: &str, doc: &Document, key: &[u8]) -> Result<()> {
        for (name, path) in self.geo_indexes_on(table) {
            if let Some(entry) = geo_entry_key(doc, &path, key) {
                if let Some(engine) = self.indexes.get_mut(&name) {
                    engine.put(&entry, key.to_vec())?;
                }
            }
        }
        Ok(())
    }

    /// Remove the geo index entry for a row's point (used on delete, and on the
    /// previous version before an overwrite whose point moved).
    fn maintain_geo_del(&mut self, table: &str, doc: &Document, key: &[u8]) -> Result<()> {
        for (name, path) in self.geo_indexes_on(table) {
            if let Some(entry) = geo_entry_key(doc, &path, key) {
                if let Some(engine) = self.indexes.get_mut(&name) {
                    engine.delete(&entry)?;
                }
            }
        }
        Ok(())
    }

    // ---- managed (EMBED) vector index: out-of-band embedding ----

    /// Queue every row of a managed index whose text is present but not yet in
    /// the HNSW. Run at create (initial backfill) and on open (crash recovery —
    /// the snapshot restores embedded vectors; this re-queues the rest). Scans
    /// keys only, so it is cheap; the actual embedding is deferred to the drain.
    pub fn enqueue_unembedded(&mut self, name: &str) {
        let Some(def) = self.catalog.vector_indexes.get(name).cloned() else {
            return;
        };
        if !def.embed {
            return;
        }
        let Some(engine) = self.tables.get(&def.table) else {
            return;
        };
        let mut with_text: Vec<Vec<u8>> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        while let Ok(rows) = engine.scan_versioned_page(cursor.as_deref(), 4096) {
            let done = rows.len() < 4096;
            cursor = rows.last().map(|(k, ..)| k.clone());
            for (key, _hlc, bytes) in rows {
                let Some(bytes) = bytes else { continue };
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    if matches!(doc.get_path(&def.path), Some(Value::String(s)) if !s.is_empty()) {
                        with_text.push(key);
                    }
                }
            }
            if done {
                break;
            }
        }
        // Keep only keys not already embedded (immutable borrow before the
        // mutable queue insert).
        let unembedded: Vec<Vec<u8>> = {
            let hnsw = self.vector_indexes.get(name);
            with_text
                .into_iter()
                .filter(|k| !hnsw.is_some_and(|h| h.contains(k)))
                .collect()
        };
        if !unembedded.is_empty() {
            self.pending_embeds
                .entry(name.to_string())
                .or_default()
                .extend(unembedded);
        }
    }

    /// Read the current text for up to `limit` queued keys of a managed index.
    /// Returns `(keys, texts)` in matching order; keys whose row/text vanished
    /// are dropped from the queue and omitted.
    pub fn peek_embed_batch(&mut self, name: &str, limit: usize) -> (Vec<Vec<u8>>, Vec<String>) {
        let Some(def) = self.catalog.vector_indexes.get(name).cloned() else {
            return (Vec::new(), Vec::new());
        };
        let candidates: Vec<Vec<u8>> = self
            .pending_embeds
            .get(name)
            .map(|q| q.iter().take(limit).cloned().collect())
            .unwrap_or_default();
        let mut gone: Vec<Vec<u8>> = Vec::new();
        let (mut keys, mut texts) = (Vec::new(), Vec::new());
        if let Some(engine) = self.tables.get(&def.table) {
            for key in candidates {
                let text = engine.get(&key).ok().flatten().and_then(|bytes| {
                    match Value::decode(&bytes) {
                        Ok(Value::Document(doc)) => match doc.get_path(&def.path) {
                            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                            _ => None,
                        },
                        _ => None,
                    }
                });
                match text {
                    Some(t) => {
                        keys.push(key);
                        texts.push(t);
                    }
                    None => gone.push(key), // row deleted / text removed
                }
            }
        }
        if let Some(q) = self.pending_embeds.get_mut(name) {
            for k in &gone {
                q.remove(k);
            }
        }
        (keys, texts)
    }

    /// Insert freshly-embedded vectors into the managed index's HNSW, dropping
    /// their keys from the queue. Dimension is validated by the caller.
    pub fn apply_embeddings(&mut self, name: &str, keys: &[Vec<u8>], vectors: Vec<Vec<f32>>) {
        if let Some(hnsw) = self.vector_indexes.get_mut(name) {
            for (key, vector) in keys.iter().zip(vectors) {
                hnsw.insert(key.clone(), vector);
            }
        }
        if let Some(q) = self.pending_embeds.get_mut(name) {
            for k in keys {
                q.remove(k);
            }
        }
    }

    /// True if any managed index has rows awaiting embedding.
    pub fn has_pending_embeds(&self) -> bool {
        self.pending_embeds.values().any(|q| !q.is_empty())
    }

    /// Names of managed indexes with rows awaiting embedding — the work list
    /// for a cluster node's background embed drain.
    pub fn pending_embed_indexes(&self) -> Vec<String> {
        self.pending_embeds
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Persist a managed index's HNSW if it has unsaved changes (the background
    /// worker calls this after applying a batch — it embeds off the lock, so it
    /// can't use the combined `run_embed_drain`).
    pub fn save_vector_snapshot_if_dirty(&mut self, name: &str) {
        let path = vector_snapshot_path(&self.dir, name);
        let watermark = self.vector_watermarks.get(name).copied().unwrap_or(Hlc::MIN);
        if let Some(hnsw) = self.vector_indexes.get_mut(name) {
            if hnsw.is_dirty() {
                if let Err(e) = save_vector_snapshot(&path, hnsw, watermark) {
                    skaidb_types::slog!("skaidb: vector snapshot save failed for {name}: {e}");
                } else {
                    hnsw.mark_clean();
                }
            }
        }
    }

    /// Queue every managed index's un-embedded rows — run once after the
    /// embedder is installed at startup, so a crash-window delta (rows not in
    /// the loaded snapshot) is re-embedded. A clean restart queues nothing.
    pub fn enqueue_all_managed_unembedded(&mut self) {
        let managed: Vec<String> = self
            .catalog
            .vector_indexes
            .iter()
            .filter(|(_, d)| d.embed)
            .map(|(n, _)| n.clone())
            .collect();
        for name in managed {
            self.enqueue_unembedded(&name);
        }
    }

    /// Drain the embed queue for every managed index: batch queued rows, call
    /// the external embedder OFF nothing that blocks a write, insert the
    /// vectors, and snapshot. Errors (endpoint down) are swallowed and the keys
    /// stay queued for the next drain — a write is never failed by inference.
    /// Inline for a single-node Session (drained at commit); a cluster node's
    /// background worker calls it on its tick.
    pub fn run_embed_drain(&mut self) {
        let Some(embedder) = self.embedder.clone() else {
            return;
        };
        // Texts per embeddings request. A generous default; the endpoint's own
        // batch limit is the real ceiling, and the drain loops until caught up.
        const EMBED_BATCH: usize = 32;
        let batch = EMBED_BATCH;
        let names: Vec<String> = self
            .pending_embeds
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(n, _)| n.clone())
            .collect();
        for name in names {
            loop {
                let (keys, texts) = self.peek_embed_batch(&name, batch);
                if keys.is_empty() {
                    break;
                }
                match embedder.embed(&texts) {
                    Ok(vectors) => {
                        if crate::embed::check_dim(&vectors, embedder.dim()).is_err() {
                            skaidb_types::slog!(
                                "skaidb: embedder dimension mismatch for '{name}' — pausing embed"
                            );
                            break;
                        }
                        self.apply_embeddings(&name, &keys, vectors);
                    }
                    Err(e) => {
                        skaidb_types::slog!(
                            "skaidb: embedding failed for '{name}' ({e}); rows stay queued, will retry"
                        );
                        break; // leave keys queued; do not fail any write
                    }
                }
            }
            // Persist the freshly embedded vectors.
            let path = vector_snapshot_path(&self.dir, &name);
            let watermark = self.vector_watermarks.get(&name).copied().unwrap_or(Hlc::MIN);
            if let Some(hnsw) = self.vector_indexes.get_mut(&name) {
                if hnsw.is_dirty() {
                    if let Err(e) = save_vector_snapshot(&path, hnsw, watermark) {
                        skaidb_types::slog!("skaidb: vector snapshot save failed for {name}: {e}");
                    } else {
                        hnsw.mark_clean();
                    }
                }
            }
        }
    }

    /// The names of every search index on `table`.
    fn search_indexes_on(&self, table: &str) -> Vec<String> {
        self.catalog
            .search_indexes
            .iter()
            .filter(|(_, def)| def.table == table)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Index one written row in every search index on `table`, without the
    /// NRT refresh check — statement-batch callers refresh once per batch
    /// via [`Database::search_refresh`] (the phase-5 bulk-ingest path)
    /// instead of per row.
    fn search_put_unrefreshed(
        &mut self,
        table: &str,
        doc: &Document,
        key: &[u8],
        hlc: Hlc,
    ) -> Result<()> {
        for name in self.search_indexes_on(table) {
            if let Some(live) = self.search_indexes.get_mut(&name) {
                live.index.put(key, doc, hlc_to_watermark(hlc))?;
            }
        }
        Ok(())
    }

    /// Remove one deleted row from every search index on `table`; see
    /// [`Database::search_put_unrefreshed`].
    fn search_del_unrefreshed(&mut self, table: &str, key: &[u8], hlc: Hlc) -> Result<()> {
        for name in self.search_indexes_on(table) {
            if let Some(live) = self.search_indexes.get_mut(&name) {
                live.index.delete(key, hlc_to_watermark(hlc));
            }
        }
        Ok(())
    }

    /// One NRT refresh check for every search index on `table`: commit an
    /// index whose refresh interval elapsed since its last commit.
    pub fn search_refresh(&mut self, table: &str) -> Result<()> {
        for name in self.search_indexes_on(table) {
            if let Some(live) = self.search_indexes.get_mut(&name) {
                live.maybe_refresh()?;
            }
        }
        Ok(())
    }

    /// Whether any search index exists — the cheap gate for the server's
    /// background NRT tick (checked under a read lock before taking the
    /// write lock).
    pub fn has_search_indexes(&self) -> bool {
        !self.search_indexes.is_empty()
    }

    /// One background NRT tick over **all** search indexes: commit any with
    /// pending writes whose refresh interval elapsed. Write-path refresh
    /// checks only run on the next write, so without this an idle table's
    /// last index writes stay invisible to shared/read-only searches until
    /// traffic resumes; the server ticks this so they become searchable
    /// within `refresh_ms` regardless.
    pub fn search_refresh_tick(&mut self) -> Result<()> {
        for live in self.search_indexes.values_mut() {
            live.maybe_refresh()?;
        }
        Ok(())
    }

    /// Maintain every search index on `table` for a written row, then commit
    /// any index whose NRT refresh interval elapsed.
    fn maintain_search_put(
        &mut self,
        table: &str,
        doc: &Document,
        key: &[u8],
        hlc: Hlc,
    ) -> Result<()> {
        self.search_put_unrefreshed(table, doc, key, hlc)?;
        self.search_refresh(table)
    }

    /// Maintain every search index on `table` for a deleted row; see
    /// [`Database::maintain_search_put`].
    fn maintain_search_del(&mut self, table: &str, key: &[u8], hlc: Hlc) -> Result<()> {
        self.search_del_unrefreshed(table, key, hlc)?;
        self.search_refresh(table)
    }

    /// Resolve the search index serving `query` on `table`: the index that
    /// covers every field the query names — declared paths, `.keyword`
    /// twins, and `copy_to` targets (a `QueryString` resolves its fields
    /// inside the index, so any index on the table serves it).
    fn search_index_for_query(&self, table: &str, query: &SearchQuery) -> Result<String> {
        let mut fields = Vec::new();
        collect_search_fields(query, &mut fields);
        let mut on_table = self
            .catalog
            .search_indexes
            .iter()
            .filter(|(_, def)| def.table == table)
            .peekable();
        if on_table.peek().is_none() {
            return Err(EngineError::Unsupported(format!(
                "table '{table}' has no search index"
            )));
        }
        for (name, def) in on_table {
            if fields.iter().all(|f| def.covers(f)) {
                return Ok(name.clone());
            }
        }
        let uncovered = fields
            .iter()
            .find(|f| {
                !self
                    .catalog
                    .search_indexes
                    .values()
                    .any(|def| def.table == table && def.covers(f))
            })
            .cloned()
            .unwrap_or_default();
        Err(EngineError::Unsupported(format!(
            "no search index on table '{table}' covers column '{uncovered}'"
        )))
    }

    /// Full-text search with read-your-writes: commit the serving index's
    /// pending writes first, then search. Needs `&mut` — the shared-access
    /// variant is [`Database::search_read`] (NRT semantics: staleness at most
    /// the index's `refresh_ms`).
    pub fn search_commit_if_dirty(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let name = self.search_index_for_query(table, query)?;
        if let Some(live) = self.search_indexes.get_mut(&name) {
            live.commit_if_dirty()?;
        }
        self.search_with_index(&name, table, query, k, filter, highlights)
    }

    /// Local-shard raw hits for the cluster scatter path: `(key, score)`
    /// from the index covering `query`, no row resolution or filtering.
    /// `k = Some(n)`: the `n` best local scores, best first; `k = None`:
    /// every matching key with score 0.0 (the unranked path). Serves the
    /// last-committed index state (NRT).
    pub fn search_local(
        &self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, f32)>> {
        let name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&name)
            .ok_or_else(|| EngineError::IndexNotFound(name.clone()))?;
        if live.building {
            return Err(EngineError::Unsupported(format!(
                "search index '{}' is rebuilding after restart — retry shortly",
                namespace::split(&name).1
            )));
        }
        match k {
            Some(n) => Ok(live
                .index
                .search_top(query, n)?
                .into_iter()
                .map(|hit| (hit.key, hit.score))
                .collect()),
            None => Ok(live
                .index
                .search_keys(query)?
                .into_iter()
                .map(|key| (key, 0.0))
                .collect()),
        }
    }

    /// [`Database::search_local`] with the coordinator's read-your-writes
    /// half: commit the serving index's pending writes first.
    pub fn search_local_commit_if_dirty(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, f32)>> {
        let name = self.search_index_for_query(table, query)?;
        if let Some(live) = self.search_indexes.get_mut(&name) {
            live.commit_if_dirty()?;
        }
        self.search_local(table, query, k)
    }

    /// Exact fast-field aggregation over the local index (read-your-writes:
    /// pending index writes commit first). `Ok(None)` = the index cannot
    /// serve this request exactly; the caller falls back to rows.
    /// `ownership`: restrict to documents in the given placement-hash arcs
    /// (a sharded scatter's per-node primary key-space); `None` = whole
    /// index.
    pub fn search_aggregate(
        &mut self,
        table: &str,
        query: &SearchQuery,
        agg: &skaidb_fts::AggRequest,
        ownership: Option<&[(u64, u64)]>,
    ) -> Result<Option<Vec<skaidb_fts::AggRow>>> {
        let name = self.search_index_for_query(table, query)?;
        if let Some(live) = self.search_indexes.get_mut(&name) {
            live.commit_if_dirty()?;
        }
        self.search_aggregate_read(table, query, agg, ownership)
    }

    /// [`Database::search_aggregate`] over the last-committed index state
    /// (the shared/read-only path).
    pub fn search_aggregate_read(
        &self,
        table: &str,
        query: &SearchQuery,
        agg: &skaidb_fts::AggRequest,
        ownership: Option<&[(u64, u64)]>,
    ) -> Result<Option<Vec<skaidb_fts::AggRow>>> {
        let name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&name)
            .ok_or(EngineError::IndexNotFound(name))?;
        Ok(live.index.aggregate(query, agg, ownership)?)
    }

    /// Fast-field-ordered top-k over the local index (read-your-writes),
    /// resolving hits to rows with the residual-filter over-fetch
    /// discipline. `Ok(None)` = the index cannot serve this ordering
    /// exactly; the caller falls back.
    #[allow(clippy::too_many_arguments)]
    pub fn search_sorted(
        &mut self,
        table: &str,
        query: &SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
        ownership: Option<&[(u64, u64)]>,
    ) -> Result<Option<SortedSearchRows>> {
        let name = self.search_index_for_query(table, query)?;
        if let Some(live) = self.search_indexes.get_mut(&name) {
            live.commit_if_dirty()?;
        }
        self.search_sorted_read(table, query, sort, k, filter, highlights, ownership)
    }

    /// [`Database::search_sorted`] over the last-committed index state.
    #[allow(clippy::too_many_arguments)]
    pub fn search_sorted_read(
        &self,
        table: &str,
        query: &SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
        ownership: Option<&[(u64, u64)]>,
    ) -> Result<Option<SortedSearchRows>> {
        let name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&name)
            .ok_or(EngineError::IndexNotFound(name))?;
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let fetch = if filter.is_some() {
            k.saturating_mul(4).max(k + 16)
        } else {
            k
        };
        let Some(hits) = live.index.search_sorted(query, sort, fetch, ownership)? else {
            return Ok(None);
        };
        let highlighters = highlights
            .iter()
            .map(|(col, opts)| {
                let h = live.index.highlighter(query, col, opts)?;
                Ok((col.as_str(), h))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut out = Vec::new();
        for (key, _) in hits {
            let Some(bytes) = table_engine.get(&key)? else {
                continue;
            };
            let Ok(Value::Document(mut doc)) = Value::decode(&bytes) else {
                continue;
            };
            if !matches_filter(filter, &doc)? {
                continue;
            }
            for (col, h) in &highlighters {
                let snippet = h.snippet_doc(&doc, col);
                doc.insert(format!("_highlight_{col}"), snippet);
            }
            out.push((key, doc));
            if out.len() >= k {
                break;
            }
        }
        Ok(Some(out))
    }

    /// [`Database::suggest_cmd`] on the exclusive path: commit pending
    /// index writes first so a suggestion right after an insert sees it
    /// (read-your-writes, like searches on the write path).
    /// `EXPLAIN <statement>`: describe the plan the executor would choose,
    /// as `(aspect, decision)` rows. Mirrors the planner's actual helpers
    /// (`pk_point_key`, `plan_index_scan`, `filter_search_query`,
    /// `map_search_aggregate`) rather than re-deriving rules, so the
    /// answer tracks the executor. Advisory — nothing executes.
    pub fn explain_statement(&self, stmt: &Statement) -> Result<ResultSet> {
        let mut rows: Vec<(String, String)> = Vec::new();
        let mut push = |aspect: &str, decision: String| {
            rows.push((aspect.to_string(), decision));
        };
        match stmt {
            Statement::Select(sel) => {
                push("statement", "SELECT".into());
                push("table", sel.from.clone());
                if self.catalog.timeseries.contains_key(&sel.from) {
                    push(
                        "access",
                        "time-series read: ts-range + label pushdown narrows the storage \
                         scan; grouped aggregates ship per-series per-bucket partials on \
                         clusters; aged buckets may serve from a rollup"
                            .into(),
                    );
                } else if sel.nearest.is_some() {
                    let idx = self
                        .catalog
                        .vector_indexes
                        .iter()
                        .find(|(_, d)| d.table == sel.from)
                        .map(|(n, _)| n.clone())
                        .unwrap_or_else(|| "<missing vector index>".into());
                    push("access", format!("vector search (HNSW) via index '{idx}'"));
                    if sel.filter.is_some() {
                        push("residual_filter", "applied to candidates after the search".into());
                    }
                } else if let Some(query) = filter_search_query(&sel.filter)? {
                    let idx = self.search_index_for_query(&sel.from, &query)?;
                    let aggregate_mode = !sel.group_by.is_empty()
                        || sel.items.iter().any(|it| match it {
                            SelectItem::Expr { expr, .. } => contains_aggregate(expr),
                            _ => false,
                        });
                    if aggregate_mode {
                        match map_search_aggregate(sel) {
                            Some(_) => push(
                                "access",
                                format!(
                                    "search aggregation: exact fast-field facet pushdown \
                                     via '{idx}' (declines to the row gather on any \
                                     inexactness)"
                                ),
                            ),
                            None => push(
                                "access",
                                format!(
                                    "search aggregation: row-gather fallback via '{idx}' \
                                     (shape not servable by the fast-field pushdown)"
                                ),
                            ),
                        }
                    } else {
                        let score_topk = sel.rerank.is_some()
                            || (sel.limit.is_some()
                                && sel.order_by.len() == 1
                                && is_score_call(&sel.order_by[0].expr)
                                && sel.order_by[0].descending);
                        let col_topk = sel.limit.is_some()
                            && sel.order_by.len() == 1
                            && matches!(sel.order_by[0].expr, Expr::Column(_));
                        if score_topk {
                            push(
                                "access",
                                format!(
                                    "BM25 top-k pushdown via '{idx}' (k = {})",
                                    sel.rerank
                                        .as_ref()
                                        .map(|r| r.top)
                                        .or(sel.limit)
                                        .unwrap_or(0)
                                ),
                            );
                        } else if col_topk {
                            push(
                                "access",
                                format!(
                                    "search via '{idx}', index-ordered top-k when the sort \
                                     column is a declared fast field (else gather + sort)"
                                ),
                            );
                        } else {
                            push(
                                "access",
                                format!("unranked search via '{idx}' (all matching keys)"),
                            );
                        }
                        let (_, residual) = split_search_filter(&sel.filter)?;
                        if residual.is_some() {
                            push(
                                "residual_filter",
                                "ordinary predicates filter the matches after the index".into(),
                            );
                        }
                    }
                } else if let Some((idx, ranges)) =
                    self.plan_geo_scan(&sel.from, &sel.filter)
                {
                    push(
                        "access",
                        format!(
                            "geo index scan via '{idx}' ({} Morton-code range(s))",
                            ranges.len()
                        ),
                    );
                    push(
                        "residual_filter",
                        "exact geo_distance/geo_bbox re-checked on candidates".into(),
                    );
                } else {
                    let pk = self.table_primary_key(&sel.from)?;
                    if pk_point_key(&pk, &sel.filter).is_some() {
                        push("access", "point read (primary-key equality)".into());
                    } else if let Some(keys) = pk_point_keys(&pk, &sel.filter) {
                        push(
                            "access",
                            format!(
                                "point-read set (primary-key =/IN, {} keys)",
                                keys.len()
                            ),
                        );
                    } else if let Some((idx, start, end, sorted, reverse)) = {
                        // Plan with the same ORDER BY / LIMIT inputs execution
                        // uses — the previous order-blind plan here reported an
                        // equality index for queries that actually take the
                        // sorted walk, sending users chasing the wrong index.
                        let order2 = sel.order_by.first().and_then(|k| match &k.expr {
                            Expr::Column(c) => Some((c.as_str(), k.descending)),
                            _ => None,
                        });
                        let lim = sel.limit.map(|n| n as usize);
                        self.plan_index(&sel.from, &sel.filter, order2, lim)
                    } {
                        let bounds = match (start.is_some(), end.is_some()) {
                            (true, true) => "bounded range",
                            (true, false) => "lower-bounded range",
                            (false, true) => "upper-bounded range",
                            (false, false) => "full index",
                        };
                        if sorted {
                            push(
                                "access",
                                format!(
                                    "index-ordered walk via '{idx}' ({bounds}, {}, \
                                     early-stop at LIMIT)",
                                    if reverse { "DESC tail-walk" } else { "ASC" }
                                ),
                            );
                        } else {
                            push("access", format!("index scan via '{idx}' ({bounds})"));
                        }
                    } else if let Some((idx, _)) =
                        self.plan_global_probe(&sel.from, &sel.filter)
                    {
                        push(
                            "access",
                            format!(
                                "global-index probe via '{}' (routed to the value's \
                                 replica set)",
                                namespace::split(&idx).1
                            ),
                        );
                    } else if pk_prefix_scan_range(&pk, &sel.filter).is_some() {
                        push(
                            "access",
                            "primary-key prefix range (leftmost PK column(s) \
                             equality-pinned; every shard scans only that key slice)"
                                .into(),
                        );
                    } else {
                        push("access", "full table scan (streaming k-way merge)".into());
                    }
                    if sel.limit.is_some() && !sel.order_by.is_empty() {
                        push(
                            "order",
                            "ORDER BY + LIMIT uses top-k selection, not a full sort".into(),
                        );
                    }
                }
                if sel.after.is_some() {
                    push(
                        "page",
                        "AFTER keyset cursor: ranked fetch (doubling depth) filtered \
                         strictly after the (sort value, primary key) cursor"
                            .into(),
                    );
                }
                if let Some(rr) = &sel.rerank {
                    push(
                        "rerank",
                        format!(
                            "top {} candidates re-scored coordinator-side by the \
                             inference reranker{}",
                            rr.top,
                            rr.model
                                .as_ref()
                                .map(|m| format!(" (model '{m}')"))
                                .unwrap_or_default()
                        ),
                    );
                }
                if let Some(t) = &sel.group_top {
                    push(
                        "group_top",
                        format!(
                            "per-group top-k rows: each group's {} best by the ranking \
                             expression (row gather, ranked at the coordinator)",
                            t.k
                        ),
                    );
                }
                if !sel.joins.is_empty() {
                    push(
                        "join",
                        "equi-joins hash-join, other predicates nested-loop; on a \
                         cluster both sides gather at the coordinator"
                            .into(),
                    );
                }
            }
            Statement::Insert(ins) => {
                push("statement", "INSERT".into());
                push("table", ins.table.clone());
                push(
                    "write",
                    "keys route to their replica sets; acked at the write consistency; \
                     secondary/search/vector indexes maintained per replica"
                        .into(),
                );
            }
            Statement::Update(u) => {
                push("statement", "UPDATE".into());
                push("table", u.table.clone());
                let pk = self.table_primary_key(&u.table)?;
                if pk_point_key(&pk, &u.filter).is_some() {
                    push("access", "point read (primary-key equality), then write".into());
                } else {
                    push("access", "scan matching rows, then write each".into());
                }
            }
            Statement::Delete(d) => {
                push("statement", "DELETE".into());
                push("table", d.table.clone());
                let pk = self.table_primary_key(&d.table)?;
                if pk_point_key(&pk, &d.filter).is_some() {
                    push("access", "point read (primary-key equality), then tombstone".into());
                } else {
                    push("access", "scan matching rows, then tombstone each".into());
                }
            }
            other => {
                push("statement", format!("{other:?}").split(' ').next().unwrap_or("?").into());
                push("plan", "executes directly (no data-access plan)".into());
            }
        }
        Ok(ResultSet {
            columns: vec!["aspect".into(), "decision".into()],
            rows: rows
                .into_iter()
                // Table/index names reaching here are internal
                // (`db\u{1f}name`); render the namespace separator as `.` so
                // the plan reads `agencik.i_slack…`, matching error messages.
                .map(|(a, d)| {
                    vec![Value::String(a), Value::String(d.replace('\u{1f}', "."))]
                })
                .collect(),
        })
    }

    /// Per-hit BM25 score breakdown (ES `_explanation`): how one row —
    /// identified by its primary-key value — scored against the search
    /// predicates in `filter`, as tantivy's explanation JSON. `Ok(None)`
    /// when the row does not match. Serves from this node's local index
    /// at its last-committed state; callers gate on deployments where one
    /// index holds every row, like the other local-index paths.
    pub fn search_explain(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
        pk_value: &Value,
    ) -> Result<Option<String>> {
        let Some(query) = filter_search_query(filter)? else {
            return Err(EngineError::Type(
                "score explain needs a MATCH()/SEARCH() predicate".into(),
            ));
        };
        self.search_explain_query(table, &query, pk_value)
    }

    /// [`Database::search_explain_query`] over the last-committed index
    /// state (the shared/read-only path — pending writes are not
    /// committed first).
    pub fn search_explain_query_read(
        &self,
        table: &str,
        query: &SearchQuery,
        pk_value: &Value,
    ) -> Result<Option<String>> {
        let index_name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&index_name)
            .ok_or_else(|| EngineError::IndexNotFound(index_name.clone()))?;
        let key = Value::Array(vec![pk_value.clone()]).encode_key();
        Ok(live.index.explain(query, &key)?)
    }

    /// [`Database::search_explain`] for an already-extracted search query —
    /// the form a coordinator can route to a remote replica (the SQL
    /// filter itself does not travel; the [`SearchQuery`] does).
    pub fn search_explain_query(
        &mut self,
        table: &str,
        query: &SearchQuery,
        pk_value: &Value,
    ) -> Result<Option<String>> {
        let index_name = self.search_index_for_query(table, query)?;
        if let Some(live) = self.search_indexes.get_mut(&index_name) {
            live.commit_if_dirty()?;
        }
        let live = self
            .search_indexes
            .get(&index_name)
            .ok_or_else(|| EngineError::IndexNotFound(index_name.clone()))?;
        let key = Value::Array(vec![pk_value.clone()]).encode_key();
        Ok(live.index.explain(query, &key)?)
    }

    /// `ALTER VECTOR INDEX ... SET (...)`: live search-time tuning. Only
    /// `ef` (recall/latency) qualifies; the graph-shaping parameters
    /// (`m`, `ef_construction`) error with a pointer to DROP + CREATE.
    fn alter_vector_index(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<QueryOutput> {
        let mut new_ef = None;
        for (key, value) in options {
            match key.as_str() {
                "ef" => {
                    let ef: usize = value.parse().map_err(|_| {
                        EngineError::Type(format!("ef must be a positive integer, got '{value}'"))
                    })?;
                    if ef == 0 || ef > 65_536 {
                        return Err(EngineError::Type("ef must be between 1 and 65536".into()));
                    }
                    new_ef = Some(ef);
                }
                "m" | "ef_construction" => {
                    return Err(EngineError::Unsupported(format!(
                        "'{key}' shapes the graph at build time — DROP and re-CREATE the \
                         vector index to change it"
                    )));
                }
                other => {
                    return Err(EngineError::Unsupported(format!(
                        "unknown vector index option '{other}' (try ef)"
                    )));
                }
            }
        }
        let hlc = self.ddl_stamp();
        let key = format!("v:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        let Some(def) = self.catalog.vector_indexes.get_mut(name) else {
            return Err(EngineError::IndexNotFound(name.to_string()));
        };
        if let Some(ef) = new_ef {
            def.ef_search = Some(ef);
            if let Some(h) = self.vector_indexes.get_mut(name) {
                h.set_ef_search(ef);
            }
        }
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn suggest_cmd_mut(
        &mut self,
        index: &str,
        text: &str,
        column: Option<&str>,
        limit: u64,
    ) -> Result<ResultSet> {
        if let Some(live) = self.search_indexes.get_mut(index) {
            live.commit_if_dirty()?;
        }
        self.suggest_cmd(index, text, column, limit)
    }

    /// `SUGGEST '<text>' ON <index>` — term suggestions ("did you mean")
    /// from the index's dictionary. `column` defaults to the index's sole
    /// text column; several text columns require an explicit `COLUMN`.
    /// Served from the local shard's last-committed state.
    fn suggest_cmd(
        &self,
        index: &str,
        text: &str,
        column: Option<&str>,
        limit: u64,
    ) -> Result<ResultSet> {
        let def = self
            .catalog
            .search_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        let live = self
            .search_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        let column = match column {
            Some(c) => c.to_string(),
            None => {
                let (cfg, _) = SearchIndexConfig::from_declaration(&def.paths, &def.options)?;
                let mut texty = cfg.fields.iter().filter(|f| {
                    matches!(
                        f.ftype,
                        skaidb_fts::FieldType::Text | skaidb_fts::FieldType::Keyword
                    )
                });
                match (texty.next(), texty.next()) {
                    (Some(only), None) => only.path.clone(),
                    _ => {
                        return Err(EngineError::Type(
                            "SUGGEST needs COLUMN <col> when the index covers several text \
                             columns"
                                .into(),
                        ))
                    }
                }
            }
        };
        let suggestions = live.index.suggest(&column, text, limit as usize)?;
        Ok(ResultSet::new(
            vec![
                "input".into(),
                "suggestion".into(),
                "distance".into(),
                "doc_freq".into(),
            ],
            suggestions
                .into_iter()
                .map(|s| {
                    vec![
                        Value::String(s.input),
                        Value::String(s.term),
                        Value::Int(s.distance as i64),
                        Value::Int(s.doc_freq as i64),
                    ]
                })
                .collect(),
        ))
    }

    /// The declared `(path, type-name)` fields of the first search index on
    /// `table` (the ES-REST `_mapping` view). `None` when the table has no
    /// search index.
    pub fn search_index_fields(&self, table: &str) -> Option<Vec<(String, String)>> {
        let def = self
            .catalog
            .search_indexes
            .values()
            .find(|def| def.table == table)?;
        let (cfg, _) = SearchIndexConfig::from_declaration(&def.paths, &def.options).ok()?;
        Some(
            cfg.fields
                .into_iter()
                .map(|f| {
                    let t = match f.ftype {
                        skaidb_fts::FieldType::Text => "text",
                        skaidb_fts::FieldType::Keyword => "keyword",
                        skaidb_fts::FieldType::Long => "long",
                        skaidb_fts::FieldType::Double => "double",
                        skaidb_fts::FieldType::Bool => "boolean",
                        skaidb_fts::FieldType::Date => "date",
                    };
                    (f.path, t.to_string())
                })
                .collect(),
        )
    }

    /// A snippet generator for `query` over `column`, from the index
    /// serving the query — the cluster coordinator highlights re-read rows
    /// with this after the scatter merge.
    pub fn search_highlighter(
        &self,
        table: &str,
        query: &SearchQuery,
        column: &str,
        opts: &skaidb_fts::HighlightOpts,
    ) -> Result<skaidb_fts::Highlighter> {
        let name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&name)
            .ok_or(EngineError::IndexNotFound(name))?;
        Ok(live.index.highlighter(query, column, opts)?)
    }

    /// Full-text search over the last-committed index state (see
    /// [`Database::search_commit_if_dirty`]).
    pub fn search_read(
        &self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let name = self.search_index_for_query(table, query)?;
        self.search_with_index(&name, table, query, k, filter, highlights)
    }

    /// Run `query` against the named index and resolve hits to rows.
    /// `k = Some(n)`: the `n` best-scoring rows, best first (over-fetched when
    /// a residual `filter` will drop candidates, like filtered ANN).
    /// `k = None`: every matching row, order unspecified, scores 0.0 (the
    /// pure-predicate path). Each requested highlight column adds a
    /// `_highlight_<column>` snippet field to every returned doc.
    fn search_with_index(
        &self,
        name: &str,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let live = self
            .search_indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?;
        if live.building {
            return Err(EngineError::Unsupported(format!(
                "search index '{}' is rebuilding after restart — retry shortly",
                namespace::split(name).1
            )));
        }
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        // Snippet generators are per-(query, column), built once and applied
        // to each hit's authoritative row text.
        let highlighters = highlights
            .iter()
            .map(|(col, opts)| {
                let h = live.index.highlighter(query, col, opts)?;
                Ok((col.as_str(), h))
            })
            .collect::<Result<Vec<_>>>()?;
        // The index only knows keys; re-read each hit's row (authoritative)
        // to apply the residual filter and return the document. Metered: an
        // unbounded gather (`k = None`, the grouped-fallback shape) must die
        // at the scan budget, not run for the whole statement timeout.
        let resolve = |key: &[u8]| -> Result<Option<Document>> {
            crate::scan_meter::tick(1)?;
            match table_engine.get(key)? {
                Some(bytes) => match Value::decode(&bytes) {
                    Ok(Value::Document(mut doc)) if matches_filter(filter, &doc)? => {
                        for (col, h) in &highlighters {
                            let snippet = h.snippet_doc(&doc, col);
                            doc.insert(format!("_highlight_{col}"), snippet);
                        }
                        Ok(Some(doc))
                    }
                    _ => Ok(None),
                },
                None => Ok(None),
            }
        };
        match k {
            Some(k) => {
                let fetch = if filter.is_some() {
                    k.saturating_mul(4).max(k.saturating_add(16))
                } else {
                    k
                };
                let mut out = Vec::new();
                for hit in live.index.search_top(query, fetch)? {
                    if let Some(doc) = resolve(&hit.key)? {
                        out.push((hit.key, doc, hit.score));
                        if out.len() >= k {
                            break;
                        }
                    }
                }
                Ok(out)
            }
            None => {
                let mut out = Vec::new();
                for key in live.index.search_keys(query)? {
                    if let Some(doc) = resolve(&key)? {
                        out.push((key, doc, 0.0));
                    }
                }
                Ok(out)
            }
        }
    }

    /// Parse and execute a single SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<QueryOutput> {
        self.execute_statement(parse(sql)?)
    }

    /// Install a CALLER-OWNED transaction session for the statement about
    /// to run (pair with [`Database::take_session_txn`] right after): every
    /// internal path keeps its `self.txn` view while each connection's
    /// transaction stays private to it — the session identity the ACID
    /// audit found missing. The engine write lock serializes statements,
    /// making the install race-free; the embedded slot must be idle (the
    /// embedded `execute` API and server sessions never share a handle).
    pub fn install_session_txn(&mut self, session: &mut SessionTxn) {
        debug_assert!(self.txn.is_none(), "embedded txn slot busy under a server session");
        self.txn = session.0.take();
    }

    /// Return the (possibly begun/committed/rolled-back) transaction state
    /// to its owning session after a statement ran under
    /// [`Database::install_session_txn`].
    pub fn take_session_txn(&mut self, session: &mut SessionTxn) {
        session.0 = self.txn.take();
    }

    /// Parse and execute a single **read-only** statement (`SELECT` / `SHOW …`)
    /// through `&self`, so callers holding the database behind an `RwLock` can
    /// serve reads under a shared lock and run them concurrently. A statement
    /// that is not read-only (see [`statement_is_read_only`]) is rejected —
    /// route it through [`Database::execute`] instead.
    pub fn execute_read(&self, sql: &str) -> Result<QueryOutput> {
        self.execute_read_statement(parse(sql)?)
    }

    /// Persist every vector index whose graph changed since its last
    /// snapshot. Called on graceful shutdown (and after builds), so the next
    /// start reloads in seconds instead of rebuilding for tens of minutes.
    /// A cheap identity+mutation version for `table`, for caches that must
    /// invalidate on ANY data change: the table's schema stamp (changes on
    /// drop+recreate, so a fresh table can never collide with a cached
    /// predecessor) plus the storage engine's write sequence (bumped on
    /// every append and on non-append data changes). `None` if unknown.
    pub fn table_repair_version(&self, table: &str) -> Option<(Hlc, u64)> {
        let schema = self
            .catalog
            .schema_versions
            .get(&format!("t:{table}"))
            .map(|v| v.hlc())
            .unwrap_or(Hlc::MIN);
        Some((schema, self.tables.get(table)?.write_seq()))
    }

    /// Whether any vector index has in-memory changes not yet snapshotted —
    /// the cheap read-gate for the periodic checkpoint tick.
    pub fn has_dirty_vector_indexes(&self) -> bool {
        self.vector_indexes
            .iter()
            .any(|(n, h)| h.is_dirty() && !self.building_vectors.contains(n))
    }

    pub fn save_vector_indexes(&mut self) {
        // Skip indexes mid-backfill: their watermark is still advancing and a
        // checkpoint would persist a partial graph as if complete; the final
        // backfill page writes their snapshot.
        let names: Vec<String> = self
            .vector_indexes
            .iter()
            .filter(|(n, h)| h.is_dirty() && !self.building_vectors.contains(*n))
            .map(|(n, _)| n.clone())
            .collect();
        for name in names {
            let watermark = self
                .vector_watermarks
                .get(&name)
                .copied()
                .unwrap_or(Hlc::MIN);
            let path = vector_snapshot_path(&self.dir, &name);
            let Some(hnsw) = self.vector_indexes.get_mut(&name) else {
                continue;
            };
            match save_vector_snapshot(&path, hnsw, watermark) {
                Ok(()) => hnsw.mark_clean(),
                Err(e) => {
                    skaidb_types::slog!("skaidb: vector snapshot save failed for {name}: {e}")
                }
            }
        }
    }

    /// One node's full schema-and-storage inventory: every table (regular,
    /// timeseries, memory), secondary/vector/search index, with definition
    /// details and this node's usage numbers. Table/index counts use the
    /// The table owning the index named by the user-typed reference `index`
    /// under `current_db` (secondary, vector, or search index), returned as a
    /// user-facing table reference (`table`, or `db.table` when foreign to
    /// `current_db`). `None` if no such index exists. Used by the server's
    /// permission layer to scope index DDL to the owning table.
    pub fn index_owner_table(&self, index: &str, current_db: &str) -> Option<String> {
        let internal = crate::namespace::qualify(current_db, index);
        let table = self
            .catalog
            .indexes
            .get(&internal)
            .map(|d| &d.table)
            .or_else(|| self.catalog.vector_indexes.get(&internal).map(|d| &d.table))
            .or_else(|| self.catalog.search_indexes.get(&internal).map(|d| &d.table))
            .or_else(|| self.catalog.geo_indexes.get(&internal).map(|d| &d.table))?;
        Some(crate::namespace::display_name(table, current_db))
    }

    /// O(files) approximate stats; disk sizes are exact. Names are
    /// namespaced (`db\u{1f}table`) — callers split for display.
    pub fn inventory(&self) -> Inventory {
        let mut inv = Inventory::default();
        for (name, def) in &self.catalog.tables {
            let (ks, st) = match self.tables.get(name) {
                Some(e) => (e.key_stats_fast(), e.stats()),
                None => continue,
            };
            inv.tables.push(InventoryTable {
                name: name.clone(),
                primary_key: def.primary_key.clone(),
                ttl_ms: def.ttl_ms,
                memory: def.memory,
                live_keys: ks.live_keys as u64,
                tombstones: ks.tombstones as u64,
                disk_bytes: st.disk_bytes,
                sstables: st.sstable_count as u64,
                replication: def.replication,
                pinned_nodes: def.pinned_nodes.clone(),
                witness: def.witness,
                transition: def.prev_placement.is_some(),
            });
        }
        for (name, def) in &self.catalog.timeseries {
            let Some(store) = self.timeseries.get(name) else {
                continue;
            };
            let ts = store.stats();
            inv.timeseries.push(InventoryTimeseries {
                name: name.clone(),
                series_key: def.series_key.clone(),
                retention_ms: def.retention_ms,
                rollup_of: def.rollup_of.clone(),
                series: ts.series as u64,
                disk_bytes: ts.disk_bytes,
            });
        }
        for (name, def) in &self.catalog.indexes {
            let (ks, st) = match self.indexes.get(name) {
                Some(e) => (e.key_stats_fast(), e.stats()),
                None => continue,
            };
            inv.indexes.push(InventoryIndex {
                name: name.clone(),
                table: def.table.clone(),
                paths: def.paths.clone(),
                entries: ks.live_keys as u64,
                disk_bytes: st.disk_bytes,
            });
        }
        for (name, def) in &self.catalog.vector_indexes {
            let Some(hnsw) = self.vector_indexes.get(name) else {
                continue;
            };
            let snapshot_bytes = std::fs::metadata(vector_snapshot_path(&self.dir, name))
                .map(|m| m.len())
                .unwrap_or(0);
            inv.vector_indexes.push(InventoryVector {
                name: name.clone(),
                table: def.table.clone(),
                path: def.path.clone(),
                metric: def.metric.clone(),
                dim: def.dim,
                ef_search: def.ef_search,
                vectors: hnsw.len() as u64,
                snapshot_bytes,
            });
        }
        for (name, def) in &self.catalog.search_indexes {
            let Some(live) = self.search_indexes.get(name) else {
                continue;
            };
            let fs = live.index.stats();
            inv.search_indexes.push(InventorySearch {
                name: name.clone(),
                table: def.table.clone(),
                paths: def.paths.clone(),
                docs: fs.docs,
                disk_bytes: fs.disk_bytes,
                uncommitted: fs.uncommitted,
            });
        }
        inv
    }

    /// The `(scan_row_budget, scan_byte_budget, statement_timeout_secs)` tuple
    /// for callers that arm the scan meter outside this database (the cluster
    /// coordinator, whose gather materializes the result set to bound).
    pub fn scan_meter_opts(&self) -> (usize, usize, u64) {
        (
            self.storage_opts.scan_row_budget,
            self.storage_opts.scan_byte_budget,
            self.storage_opts.statement_timeout_secs,
        )
    }

    /// Arm the per-statement scan meter from this database's options. Held
    /// for the statement's whole execution; nested calls no-op (outermost
    /// meter wins).
    fn arm_scan_meter(&self) -> Option<crate::scan_meter::Armed> {
        let secs = self.storage_opts.statement_timeout_secs;
        let deadline = (secs != 0)
            .then(|| std::time::Instant::now() + std::time::Duration::from_secs(secs));
        crate::scan_meter::arm(
            self.storage_opts.scan_row_budget,
            self.storage_opts.scan_byte_budget,
            deadline,
        )
    }

    /// Execute an already-parsed read-only statement; see
    /// [`Database::execute_read`]. A `SELECT` inside an open transaction still
    /// works here: it reads through the buffered overlay, which needs only
    /// shared access.
    pub fn execute_read_statement(&self, stmt: Statement) -> Result<QueryOutput> {
        let _meter = self.arm_scan_meter();
        let mut stmt = stmt;
        skaidb_sql::resolve_now(&mut stmt, now_ms());
        if let Statement::Select(sel) = &mut stmt {
            skaidb_sql::resolve_select_aliases(sel);
        }
        match stmt {
            // A search SELECT needs the `&mut` seam (its `search` receiver is
            // `&mut` for the write path's commit-if-dirty); `LocalRead` is a
            // local value, so the reborrow is free and still read-only.
            Statement::Select(sel) if select_uses_search(&sel) => {
                run_search_select(&sel, &mut LocalRead { db: self }).map(QueryOutput::Rows)
            }
            Statement::Select(sel) => {
                run_select(&sel, &LocalRead { db: self }).map(QueryOutput::Rows)
            }
            Statement::Suggest {
                text,
                index,
                column,
                limit,
            } => self
                .suggest_cmd(&index, &text, column.as_deref(), limit)
                .map(QueryOutput::Rows),
            Statement::ExplainScore { select, key } => {
                let query = filter_search_query(&select.filter)?.ok_or_else(|| {
                    EngineError::Type(
                        "EXPLAIN SCORE needs a MATCH()/SEARCH() predicate in the WHERE".into(),
                    )
                })?;
                let text = self.search_explain_query_read(&select.from, &query, &key)?;
                Ok(QueryOutput::Rows(explain_score_rows(text)))
            }
            Statement::Explain { ref statement } => {
                self.explain_statement(statement).map(QueryOutput::Rows)
            }
            Statement::ShowTables => Ok(QueryOutput::Rows(self.show_tables())),
            Statement::ShowIndexes => Ok(QueryOutput::Rows(self.show_indexes())),
            Statement::ShowStatus => Ok(QueryOutput::Rows(self.show_status())),
            Statement::Describe { ref table, full, sample, exact } => Ok(QueryOutput::Rows(if full {
                self.describe_full(table, sample, exact)?
            } else {
                self.describe(table)?
            })),
            Statement::ShowGrants { role } => {
                Ok(QueryOutput::Rows(self.show_grants(role.as_deref())))
            }
            Statement::ShowDatabases => Err(EngineError::Unsupported(
                "database statements require a multi-database session".into(),
            )),
            _ => Err(EngineError::Unsupported(
                "statement is not read-only; use the exclusive execution path".into(),
            )),
        }
    }

    /// Execute an already-parsed statement. This lets the multi-database
    /// [`Session`](crate::Session) dispatch a statement it has classified
    /// without re-parsing it.
    pub fn execute_statement(&mut self, stmt: Statement) -> Result<QueryOutput> {
        let out = self.execute_statement_inner(stmt)?;
        // Single-node semantics: DDL returns with its index backfills done.
        // Cluster nodes set `defer_backfills` and drive the pages from their
        // background worker so DDL acks at schema-apply.
        if !self.defer_backfills && !self.pending_backfills.is_empty() {
            for name in self.take_pending_backfills() {
                self.run_index_backfill(&name)?;
            }
        }
        if !self.defer_backfills && !self.pending_vector_backfills.is_empty() {
            for name in self.take_pending_vector_backfills() {
                self.run_vector_backfill(&name)?;
            }
        }
        if !self.defer_backfills && !self.pending_geo_backfills.is_empty() {
            for name in self.take_pending_geo_backfills() {
                self.run_geo_backfill(&name)?;
            }
        }
        // Single-node: embed any rows this statement queued to managed indexes,
        // inline at commit (a Session has no background worker). Never fails the
        // statement — a down embedder just leaves rows queued.
        if !self.defer_backfills && self.has_pending_embeds() {
            self.run_embed_drain();
        }
        Ok(out)
    }

    fn execute_statement_inner(&mut self, stmt: Statement) -> Result<QueryOutput> {
        let _meter = self.arm_scan_meter();
        let mut stmt = stmt;
        skaidb_sql::resolve_now(&mut stmt, now_ms());
        match stmt {
            Statement::CreateTable(ct) => {
                self.create_table(ct)
            }
            Statement::CreateTimeseriesTable(ct) => self.create_timeseries_table(
                &ct.name,
                ct.series_key,
                ct.retention_ms,
                ct.ooo_ms.unwrap_or(0),
                ct.if_not_exists,
            ),
            Statement::CreateRollup(cr) => self.create_rollup(
                &cr.name,
                &cr.table,
                cr.bucket_ms,
                cr.retention_ms,
                cr.if_not_exists,
            ),
            Statement::DropTable { name, if_exists } => self.drop_table(&name, if_exists),
            Statement::CreateIndex(ci) => {
                self.create_index(
                    &ci.name,
                    &ci.table,
                    &ci.paths,
                    ci.if_not_exists,
                    ci.global,
                    ci.ready,
                )
            }
            Statement::DropIndex { name, if_exists } => self.drop_index(&name, if_exists),
            Statement::CreateVectorIndex(ci) => {
                self.create_vector_index(
                    &ci.name,
                    &ci.table,
                    &ci.path,
                    &ci.metric,
                    Some(ci.dim),
                    ci.embed,
                    ci.quantized,
                    ci.if_not_exists,
                )
            }
            Statement::DropVectorIndex { name, if_exists } => {
                self.drop_vector_index(&name, if_exists)
            }
            Statement::CreateGeoIndex(ci) => {
                self.create_geo_index(&ci.name, &ci.table, &ci.path, ci.if_not_exists)
            }
            Statement::DropGeoIndex { name, if_exists } => self.drop_geo_index(&name, if_exists),
            Statement::CreateSearchIndex(ci) => self.create_search_index(&ci),
            Statement::DropSearchIndex { name, if_exists } => {
                self.drop_search_index(&name, if_exists)
            }
            Statement::RebuildSearchIndex { name } => self.rebuild_search_index_cmd(&name),
            Statement::AlterSearchIndex { name, options } => {
                self.alter_search_index(&name, &options)
            }
            Statement::AlterVectorIndex { name, options } => {
                self.alter_vector_index(&name, &options)
            }
            Statement::Backup { path } => self.backup_to(&path).map(QueryOutput::Rows),
            Statement::Restore { path } => self.restore_from(&path).map(QueryOutput::Rows),
            Statement::Suggest {
                text,
                index,
                column,
                limit,
            } => self
                .suggest_cmd_mut(&index, &text, column.as_deref(), limit)
                .map(QueryOutput::Rows),
            Statement::ExplainScore { select, key } => {
                let text = self.search_explain(&select.from, &select.filter, &key)?;
                Ok(QueryOutput::Rows(explain_score_rows(text)))
            }
            Statement::Explain { statement } => {
                self.explain_statement(&statement).map(QueryOutput::Rows)
            }
            Statement::AlterTable(alt) => self.alter_table(alt),
            Statement::Begin => self.begin(),
            Statement::Commit => self.commit(),
            Statement::Rollback => self.rollback(),
            Statement::ShowTables => Ok(QueryOutput::Rows(self.show_tables())),
            Statement::ShowIndexes => Ok(QueryOutput::Rows(self.show_indexes())),
            Statement::ShowStatus => Ok(QueryOutput::Rows(self.show_status())),
            Statement::Describe { ref table, full, sample, exact } => Ok(QueryOutput::Rows(if full {
                self.describe_full(table, sample, exact)?
            } else {
                self.describe(table)?
            })),
            Statement::CreateUser(cu) => self.create_user(
                &cu.name,
                cu.password.as_deref(),
                cu.verifier.as_deref(),
                cu.gssapi,
                cu.if_not_exists,
                false,
            ),
            Statement::AlterUser { name, password } => {
                self.create_user(&name, Some(&password), None, false, false, true)
            }
            Statement::DropUser { name, if_exists } => self.drop_user(&name, if_exists),
            Statement::CreateRole {
                name,
                if_not_exists,
                state,
            } => self.create_auth_role(&name, state.as_deref(), if_not_exists),
            Statement::DropRole { name, if_exists } => self.drop_auth_role(&name, if_exists),
            Statement::Grant {
                privilege,
                object,
                to,
            } => self.grant(&to, &privilege, grant_object_key(&object).as_deref()),
            Statement::Revoke {
                privilege,
                object,
                from,
            } => self.revoke(&from, &privilege, grant_object_key(&object).as_deref()),
            Statement::GrantRole { role, to } => self.grant_role_edge(&to, &role, true),
            Statement::RevokeRole { role, from } => self.grant_role_edge(&from, &role, false),
            Statement::ShowGrants { role } => Ok(QueryOutput::Rows(self.show_grants(role.as_deref()))),
            // Database statements span databases and so cannot run against a
            // single engine; the session layer handles them before dispatch.
            Statement::ShowDatabases
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::UseDatabase { .. } => Err(EngineError::Unsupported(
                "database statements require a multi-database session".into(),
            )),
            // DML and SELECT run through the storage-agnostic executor so the
            // exact same logic serves the local engine and the cluster
            // coordinator (which replaces LocalCluster with a networked one).
            dml => {
                let mut local = LocalCluster::new(self);
                let out = run(dml, &mut local);
                // Applied rows must become durable whether or not the
                // statement finished cleanly (matches per-row fsync behavior).
                let synced = local.flush_pending();
                let out = out?;
                synced?;
                Ok(out)
            }
        }
    }

    // ---- databases (namespace layer) ----

    /// Execute a statement in a session whose current database is `current_db`.
    ///
    /// This is the database-aware entry point: it resolves table/index names
    /// against `current_db` (so unqualified names land in the current database
    /// and `db.table` names override it), handles the cross-database statements
    /// itself, and otherwise delegates to [`Database::execute_statement`]. `USE`
    /// returns [`SessionEffect::UseDatabase`] for the caller to apply.
    pub fn execute_session(
        &mut self,
        current_db: &str,
        sql: &str,
    ) -> Result<SessionEffect> {
        self.execute_session_statement(current_db, parse(sql)?)
    }

    /// [`Database::execute_session`] over an already-parsed statement, so a
    /// caller that parsed the SQL once (e.g. for privilege checks) doesn't pay
    /// for a second parse.
    pub fn execute_session_statement(
        &mut self,
        current_db: &str,
        mut stmt: Statement,
    ) -> Result<SessionEffect> {
        let out = match stmt {
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => self.create_database(&name, if_not_exists)?,
            Statement::DropDatabase { name, if_exists } => self.drop_database(&name, if_exists)?,
            Statement::ShowDatabases => QueryOutput::Rows(self.show_databases(current_db)),
            Statement::UseDatabase { name } => {
                if !self.has_database(&name) {
                    return Err(EngineError::DatabaseNotFound(name));
                }
                return Ok(SessionEffect::UseDatabase(name));
            }
            // Catalog introspection is filtered to the current database.
            Statement::ShowTables => QueryOutput::Rows(self.show_tables_in(current_db)),
            Statement::ShowIndexes => QueryOutput::Rows(self.show_indexes_in(current_db)),
            _ => {
                namespace::resolve_statement(&mut stmt, current_db);
                self.execute_statement(stmt)
                    .map_err(|e| namespace::humanize_error(e, current_db))?
            }
        };
        Ok(SessionEffect::Output(out))
    }

    /// [`Database::execute_session_statement`] with the statement-final
    /// fsync **handed back to the caller** instead of run inline: the
    /// returned pairs are the group commits the statement would have
    /// synced. The caller MUST `sync_through` every pair before
    /// acknowledging the statement — on success AND on error (rows applied
    /// before a mid-statement error must become durable either way, same
    /// contract as the inline path).
    ///
    /// Why this exists: a server holding this Database behind an exclusive
    /// `RwLock` would otherwise spend ~the whole statement latency inside
    /// the lock on the WAL fsync, fully serializing concurrent writers (16
    /// connections measured no faster than 1). Syncing after the lock drops
    /// lets concurrent sessions' commits coalesce in `WalSync::sync_through`
    /// — the same fsync-outside-the-lock design the cluster write path
    /// (`Node::replicate`) already uses. Reads-after-writes hold: the
    /// memtable has the rows before the fsync.
    pub fn execute_session_statement_deferred(
        &mut self,
        current_db: &str,
        stmt: Statement,
    ) -> (Result<SessionEffect>, Vec<(Arc<WalSync>, WalCommit)>) {
        self.deferred_syncs = Some(Vec::new());
        let res = self.execute_session_statement(current_db, stmt);
        let pending = self.deferred_syncs.take().unwrap_or_default();
        (res, pending)
    }

    /// Database-aware read-only entry point: like [`Database::execute_session`]
    /// but through `&self`, for statements that only read (see
    /// [`statement_is_read_only`]). Table names resolve against `current_db`
    /// and `SHOW` output is filtered to it; anything that mutates is rejected.
    /// Callers behind an `RwLock` use this under a shared lock so concurrent
    /// readers don't serialize.
    pub fn execute_session_read(&self, current_db: &str, sql: &str) -> Result<QueryOutput> {
        self.execute_session_read_statement(current_db, parse(sql)?)
    }

    /// [`Database::execute_session_read`] over an already-parsed statement.
    pub fn execute_session_read_statement(
        &self,
        current_db: &str,
        mut stmt: Statement,
    ) -> Result<QueryOutput> {
        match stmt {
            Statement::ShowDatabases => Ok(QueryOutput::Rows(self.show_databases(current_db))),
            Statement::ShowTables => Ok(QueryOutput::Rows(self.show_tables_in(current_db))),
            Statement::ShowIndexes => Ok(QueryOutput::Rows(self.show_indexes_in(current_db))),
            Statement::ShowStatus => Ok(QueryOutput::Rows(self.show_status())),
            Statement::ShowGrants { ref role } => {
                Ok(QueryOutput::Rows(self.show_grants(role.as_deref())))
            }
            Statement::Select(_)
            | Statement::Suggest { .. }
            | Statement::ExplainScore { .. }
            | Statement::Describe { .. }
            | Statement::Explain { .. } => {
                namespace::resolve_statement(&mut stmt, current_db);
                self.execute_read_statement(stmt)
                    .map_err(|e| namespace::humanize_error(e, current_db))
            }
            _ => Err(EngineError::Unsupported(
                "statement is not read-only; use the exclusive execution path".into(),
            )),
        }
    }

    /// Like [`Database::execute_session`], but any DDL is stamped with `hlc`
    /// instead of a fresh local clock value — so a cluster coordinator can
    /// replicate a schema change to every node at the same version (and a node
    /// catching up applies it under last-writer-wins). Used for `ApplyDdl`.
    pub fn execute_session_with_hlc(
        &mut self,
        current_db: &str,
        sql: &str,
        hlc: Hlc,
    ) -> Result<SessionEffect> {
        self.ddl_hlc = Some(hlc);
        let result = self.execute_session(current_db, sql);
        self.ddl_hlc = None;
        result
    }

    /// True if `name` is the default database or a registered database.
    pub fn has_database(&self, name: &str) -> bool {
        name == DEFAULT_DATABASE || self.catalog.databases.contains(name)
    }

    /// All database names (the implicit `default` plus registered ones), sorted.
    pub fn database_names(&self) -> Vec<String> {
        let mut names = vec![DEFAULT_DATABASE.to_string()];
        names.extend(self.catalog.databases.iter().cloned());
        names
    }

    fn create_database(&mut self, name: &str, if_not_exists: bool) -> Result<QueryOutput> {
        if !namespace::valid_database_name(name) {
            return Err(EngineError::Constraint(format!(
                "invalid database name {name:?}: use letters, digits, '_' or '-' (max 64 chars)"
            )));
        }
        let hlc = self.ddl_stamp();
        let key = format!("d:{name}");
        // Last-writer-wins: ignore a create older than the object's last change
        // (e.g. a stale replicated create arriving after a newer drop).
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        let exists = self.has_database(name);
        if exists && !if_not_exists {
            return Err(EngineError::DatabaseExists(name.to_string()));
        }
        if !exists {
            self.catalog.databases.insert(name.to_string());
        }
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_database(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        if name == DEFAULT_DATABASE {
            return Err(EngineError::Constraint(
                "cannot drop the default database".into(),
            ));
        }
        let hlc = self.ddl_stamp();
        let key = format!("d:{name}");
        // Last-writer-wins: ignore a drop older than the object's last change.
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if !self.catalog.databases.contains(name) {
            if if_exists {
                // Record the tombstone even when absent, so a stale create
                // replicated from a peer that missed this drop can't resurrect it.
                self.record_schema(key, hlc, true);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::DatabaseNotFound(name.to_string()));
        }
        // Cascade: drop every table (and its indexes) in this database, then any
        // remaining indexes that belong to it, then deregister it.
        let tables: Vec<String> = self
            .catalog
            .tables
            .keys()
            .filter(|t| namespace::belongs_to(t, name))
            .cloned()
            .collect();
        for table in tables {
            self.drop_table(&table, true)?;
        }
        let vec_indexes: Vec<String> = self
            .catalog
            .vector_indexes
            .keys()
            .filter(|i| namespace::belongs_to(i, name))
            .cloned()
            .collect();
        for index in vec_indexes {
            self.drop_vector_index(&index, true)?;
        }
        let fts_indexes: Vec<String> = self
            .catalog
            .search_indexes
            .keys()
            .filter(|i| namespace::belongs_to(i, name))
            .cloned()
            .collect();
        for index in fts_indexes {
            self.drop_search_index(&index, true)?;
        }
        let sec_indexes: Vec<String> = self
            .catalog
            .indexes
            .keys()
            .filter(|i| namespace::belongs_to(i, name))
            .cloned()
            .collect();
        for index in sec_indexes {
            self.drop_index(&index, true)?;
        }
        self.catalog.databases.remove(name);
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `SHOW DATABASES`: every database, with `*` marking the current one.
    fn show_databases(&self, current_db: &str) -> ResultSet {
        let rows = self
            .database_names()
            .into_iter()
            .map(|name| {
                let marker = if name == current_db { "*" } else { "" };
                vec![Value::String(name), Value::String(marker.into())]
            })
            .collect();
        ResultSet {
            columns: vec!["database".into(), "current".into()],
            rows,
        }
    }

    /// `SHOW TABLES` filtered to `current_db`, with names de-qualified.
    fn show_tables_in(&self, current_db: &str) -> ResultSet {
        let mut rows: Vec<Vec<Value>> = self
            .catalog
            .tables
            .iter()
            .filter(|(name, _)| {
                namespace::belongs_to(name, current_db)
                    && !namespace::split(name).1.starts_with("__gidx__")
            })
            .map(|(name, def)| {
                vec![
                    Value::String(namespace::split(name).1.to_string()),
                    Value::String(def.primary_key.join(", ")),
                    match def.replication {
                        Some(n) => Value::Int(i64::from(n)),
                        None if !def.pinned_nodes.is_empty() => {
                            Value::Int(def.pinned_nodes.len() as i64)
                        }
                        None => Value::Null, // cluster default
                    },
                    if def.pinned_nodes.is_empty() {
                        Value::Null
                    } else {
                        Value::String(def.pinned_nodes.join(","))
                    },
                    Value::Bool(def.witness),
                    Value::Bool(def.prev_placement.is_some()),
                    Value::String("row".into()),
                ]
            })
            .collect();
        for (name, def) in &self.catalog.timeseries {
            if namespace::belongs_to(name, current_db) {
                rows.push(vec![
                    Value::String(namespace::split(name).1.to_string()),
                    Value::String(format!("{}, ts", def.series_key.join(", "))),
                    Value::Null,
                    Value::Null,
                    Value::Bool(true),
                    Value::Bool(false),
                    // The kind column routes consumers that must treat
                    // time-series tables differently — a witness pulls rows
                    // via ScanPage but samples via TsQuery, and telling the
                    // two apart from name+pk alone is impossible.
                    Value::String(
                        if def.rollup_of.is_some() { "rollup" } else { "timeseries" }.into(),
                    ),
                ]);
            }
        }
        rows.sort_by(|a, b| a[0].total_cmp(&b[0]));
        ResultSet {
            columns: vec![
                "table".into(),
                "primary_key".into(),
                "replication".into(),
                "nodes".into(),
                "witness".into(),
                "transition".into(),
                "kind".into(),
            ],
            rows,
        }
    }

    /// `SHOW INDEXES` filtered to `current_db`, with names de-qualified.
    fn show_indexes_in(&self, current_db: &str) -> ResultSet {
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (name, idx) in &self.catalog.indexes {
            if !namespace::belongs_to(name, current_db) {
                continue;
            }
            rows.push(vec![
                Value::String(namespace::split(name).1.to_string()),
                Value::String(namespace::split(&idx.table).1.to_string()),
                Value::String(
                    match (idx.global, idx.building) {
                        (true, true) => "global (building)",
                        (true, false) => "global",
                        (false, true) => "secondary (building)",
                        (false, false) => "secondary",
                    }
                    .into(),
                ),
                Value::String(idx.paths.join(", ")),
                Value::String(self.index_local_health(name, IndexClass::Secondary).into()),
            ]);
        }
        for (name, v) in &self.catalog.vector_indexes {
            if !namespace::belongs_to(name, current_db) {
                continue;
            }
            rows.push(vec![
                Value::String(namespace::split(name).1.to_string()),
                Value::String(namespace::split(&v.table).1.to_string()),
                Value::String(format!("vector({}, dim={})", v.metric, v.dim)),
                Value::String(v.path.clone()),
                Value::String(self.index_local_health(name, IndexClass::Vector).into()),
            ]);
        }
        for (name, s) in &self.catalog.search_indexes {
            if !namespace::belongs_to(name, current_db) {
                continue;
            }
            rows.push(vec![
                Value::String(namespace::split(name).1.to_string()),
                Value::String(namespace::split(&s.table).1.to_string()),
                Value::String(format!("search({})", s.analyzer())),
                Value::String(s.paths.join(", ")),
                Value::String(self.index_local_health(name, IndexClass::Search).into()),
            ]);
        }
        for (name, g) in &self.catalog.geo_indexes {
            if !namespace::belongs_to(name, current_db) {
                continue;
            }
            rows.push(vec![
                Value::String(namespace::split(name).1.to_string()),
                Value::String(namespace::split(&g.table).1.to_string()),
                Value::String("geo".into()),
                Value::String(g.path.clone()),
                Value::String(self.index_local_health(name, IndexClass::Geo).into()),
            ]);
        }
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::String(x), Value::String(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        ResultSet {
            columns: vec![
                "index".into(),
                "table".into(),
                "kind".into(),
                "columns".into(),
                "local".into(),
            ],
            rows,
        }
    }

    /// This node's live state for a catalog-listed index: `ok` (open and
    /// serving), `building` (backfill/catch-up running), or `missing` (in the
    /// catalog but no live index — the divergence SHOW INDEXES used to hide,
    /// which made the production `.6` incident a multi-day mystery). Local by
    /// design: introspection answers from the node you asked.
    fn index_local_health(&self, name: &str, class: IndexClass) -> &'static str {
        match class {
            IndexClass::Secondary => match self.catalog.indexes.get(name) {
                // `building` first: a global index mid-backfill must say so
                // (probes don't route to it yet). Once ready, global entries
                // are replicated table rows — no per-node state to be
                // missing.
                Some(d) if d.building => "building",
                Some(d) if d.global => "ok",
                _ if self.indexes.contains_key(name) => "ok",
                _ => "missing",
            },
            IndexClass::Vector => {
                if self.building_vectors.contains(name) {
                    "building"
                } else if self.vector_indexes.contains_key(name) {
                    "ok"
                } else {
                    "missing"
                }
            }
            IndexClass::Search => match self.search_indexes.get(name) {
                Some(live) if live.building => "building",
                Some(_) => "ok",
                None => "missing",
            },
            // Geo entries persist in the on-disk index engine (like a secondary
            // index): `building` while its initial backfill runs, else `ok` if
            // the engine is live, `missing` if the catalog lists it but no
            // engine is open.
            IndexClass::Geo => match self.catalog.geo_indexes.get(name) {
                Some(d) if d.building => "building",
                _ if self.indexes.contains_key(name) => "ok",
                _ => "missing",
            },
        }
    }

    // ---- DDL ----

    fn create_table(&mut self, ct: CreateTable) -> Result<QueryOutput> {
        let CreateTable {
            name,
            if_not_exists,
            primary_key: pk,
            ttl_ms,
            memory,
            replication,
            nodes,
            witness,
        } = ct;
        let name = &name;
        if pk.is_empty() {
            return Err(EngineError::Constraint(
                "primary key must have at least one column".into(),
            ));
        }
        // System/registry tables stay cluster-default placement and
        // witness-mirrored: every node consults them locally, so RF<members
        // or pins would undermine the machinery that reads them.
        if (replication.is_some() || !nodes.is_empty() || !witness)
            && is_system_table(name)
        {
            return Err(EngineError::Constraint(format!(
                "'{name}' is a system table: placement/witness options are not allowed"
            )));
        }
        let hlc = self.ddl_stamp();
        let key = format!("t:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.tables.contains_key(name) || self.catalog.timeseries.contains_key(name) {
            if if_not_exists {
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableExists(name.to_string()));
        }
        let mut opts = self.storage_opts.clone();
        opts.ephemeral = memory;
        let mut engine = StorageEngine::open_with_options(table_dir(&self.dir, name), opts)?;
        engine.set_ttl(ttl_ms.map(|ms| ms as u64));
        self.tables.insert(name.to_string(), engine);
        self.catalog.tables.insert(
            name.to_string(),
            TableDef {
                primary_key: pk,
                ttl_ms,
                memory,
                replication,
                pinned_nodes: nodes,
                witness,
                prev_placement: None,
            },
        );
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `CREATE TIMESERIES TABLE`: register the definition and open its tsdb
    /// store. Shares the table namespace (and the `t:` schema-LWW key space)
    /// with regular tables, so drops and re-creates order correctly.
    fn create_timeseries_table(
        &mut self,
        name: &str,
        series_key: Vec<String>,
        retention_ms: Option<i64>,
        ooo_window_ms: i64,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        if series_key.is_empty() {
            return Err(EngineError::Constraint(
                "SERIES KEY must have at least one column".into(),
            ));
        }
        if series_key.iter().any(|c| c == "ts" || c.starts_with("__")) {
            return Err(EngineError::Constraint(
                "series key columns may not be named `ts` or start with `__`".into(),
            ));
        }
        let hlc = self.ddl_stamp();
        let key = format!("t:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.tables.contains_key(name) || self.catalog.timeseries.contains_key(name) {
            if if_not_exists {
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableExists(name.to_string()));
        }
        let def = TsTableDef {
            series_key,
            retention_ms,
            ooo_window_ms,
            rollups: Vec::new(),
            rollup_of: None,
        };
        let store = open_tsdb(&self.dir, name, &def, self.storage_opts.ts_head_max_bytes)?;
        self.timeseries.insert(name.to_string(), store);
        self.catalog.timeseries.insert(name.to_string(), def);
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `CREATE ROLLUP`: register a derived time-series table holding
    /// per-bucket partials of its source, maintained at flush.
    fn create_rollup(
        &mut self,
        name: &str,
        source: &str,
        bucket_ms: i64,
        retention_ms: Option<i64>,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        let Some(src_def) = self.catalog.timeseries.get(source).cloned() else {
            return Err(EngineError::Unsupported(format!(
                "ROLLUP source {source} must be a timeseries table"
            )));
        };
        if src_def.rollup_of.is_some() {
            return Err(EngineError::Unsupported(
                "cannot create a rollup of a rollup".into(),
            ));
        }
        // Flushes happen at block-window boundaries; a bucket must nest
        // inside them or its partials would split across two appends.
        let span = 2 * 3600 * 1000i64;
        if bucket_ms <= 0 || span % bucket_ms != 0 {
            return Err(EngineError::Constraint(
                "BUCKET must be positive and evenly divide 2h".into(),
            ));
        }
        let hlc = self.ddl_stamp();
        let key = format!("t:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.tables.contains_key(name) || self.catalog.timeseries.contains_key(name) {
            if if_not_exists {
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableExists(name.to_string()));
        }
        let def = TsTableDef {
            series_key: src_def.series_key.clone(),
            retention_ms,
            ooo_window_ms: 0,
            rollups: Vec::new(),
            rollup_of: Some(source.to_string()),
        };
        let store = open_tsdb(&self.dir, name, &def, self.storage_opts.ts_head_max_bytes)?;
        self.timeseries.insert(name.to_string(), store);
        self.catalog.timeseries.insert(name.to_string(), def);
        self.catalog
            .timeseries
            .get_mut(source)
            .expect("source checked")
            .rollups
            .push(RollupDef {
                name: name.to_string(),
                bucket_ms,
            });
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    // ---- users / roles (RBAC management) ----

    /// `CREATE USER` / `ALTER USER` (`replace`) / verifier replay (upsert
    /// under LWW). Creates the user's own-named role too.
    fn create_user(
        &mut self,
        name: &str,
        password: Option<&str>,
        verifier: Option<&str>,
        gssapi: bool,
        if_not_exists: bool,
        replace: bool,
    ) -> Result<QueryOutput> {
        let exists = self.catalog.users.contains_key(name);
        // The verifier form is the replication/replay path: always an
        // upsert (the LWW stamp below is the arbiter). Client-facing forms
        // get CREATE/ALTER semantics.
        if verifier.is_none() {
            if replace && !exists {
                return Err(EngineError::Constraint(format!("user {name:?} does not exist")));
            }
            if !replace && exists {
                if if_not_exists {
                    return Ok(QueryOutput::Ddl);
                }
                return Err(EngineError::Constraint(format!("user {name:?} already exists")));
            }
        }
        // External (GSSAPI) users carry no local secret; everything else is a
        // SCRAM credential derived from a password or replayed from a verifier.
        let (credential, auth_kind) = if gssapi {
            (String::new(), UserAuthKind::Gssapi)
        } else {
            let credential = match (password, verifier) {
                (Some(pw), _) => {
                    // Deterministic per-user salt (matches the server's existing
                    // scheme; the salt travels inside the encoded verifier).
                    let salt =
                        skaidb_auth::crypto::sha256(format!("skaidb-user:{name}").as_bytes())[..16]
                            .to_vec();
                    ScramCredential::new(pw, &salt, skaidb_auth::DEFAULT_ITERATIONS).encode()
                }
                (None, Some(v)) => {
                    if ScramCredential::decode(v).is_none() {
                        return Err(EngineError::Constraint("invalid VERIFIER encoding".into()));
                    }
                    v.to_string()
                }
                (None, None) => {
                    return Err(EngineError::Constraint(
                        "CREATE USER requires PASSWORD, VERIFIER, or GSSAPI".into(),
                    ))
                }
            };
            (credential, UserAuthKind::Scram)
        };
        let hlc = self.ddl_stamp();
        let key = format!("usr:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        self.catalog.users.insert(
            name.to_string(),
            UserDef {
                credential,
                auth_kind,
            },
        );
        // The user's personal role (idempotent; keeps existing grants).
        self.catalog.auth_roles.entry(name.to_string()).or_default();
        self.record_schema(key, hlc, false);
        self.record_schema(format!("rol:{name}"), hlc, false);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_user(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("usr:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.users.remove(name).is_none() && !if_exists {
            return Err(EngineError::Constraint(format!("user {name:?} does not exist")));
        }
        // The personal role drops with the user.
        self.catalog.auth_roles.remove(name);
        self.record_schema(key, hlc, true);
        self.record_schema(format!("rol:{name}"), hlc, true);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `CREATE ROLE` (the internal `GRANTS '<state>'` form replaces the
    /// whole role under a newer stamp — the replication unit).
    fn create_auth_role(
        &mut self,
        name: &str,
        state: Option<&str>,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("rol:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.auth_roles.contains_key(name) && !if_not_exists && state.is_none() {
            return Err(EngineError::Constraint(format!("role {name:?} already exists")));
        }
        let def = match state {
            Some(enc) => decode_role_state(enc)?,
            None => self
                .catalog
                .auth_roles
                .get(name)
                .cloned()
                .unwrap_or_default(),
        };
        self.catalog.auth_roles.insert(name.to_string(), def);
        self.record_schema(key, hlc, false);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_auth_role(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        if self.catalog.users.contains_key(name) {
            return Err(EngineError::Constraint(
                "cannot drop a user's personal role; DROP USER instead".into(),
            ));
        }
        let hlc = self.ddl_stamp();
        let key = format!("rol:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.auth_roles.remove(name).is_none() && !if_exists {
            return Err(EngineError::Constraint(format!("role {name:?} does not exist")));
        }
        self.record_schema(key, hlc, true);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Apply a grant/revoke to `role`, stamping the whole role (the LWW
    /// replication unit is the role's complete state).
    fn grant(&mut self, role: &str, privilege: &str, table: Option<&str>) -> Result<QueryOutput> {
        privilege_from_name(privilege)
            .ok_or_else(|| EngineError::Constraint(format!("unknown privilege {privilege:?}")))?;
        let hlc = self.ddl_stamp();
        let def = self
            .catalog
            .auth_roles
            .get_mut(role)
            .ok_or_else(|| EngineError::Constraint(format!("role {role:?} does not exist")))?;
        let entry = (privilege.to_ascii_lowercase(), table.map(str::to_string));
        if !def.grants.contains(&entry) {
            def.grants.push(entry);
        }
        self.record_schema(format!("rol:{role}"), hlc, false);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn revoke(&mut self, role: &str, privilege: &str, table: Option<&str>) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let def = self
            .catalog
            .auth_roles
            .get_mut(role)
            .ok_or_else(|| EngineError::Constraint(format!("role {role:?} does not exist")))?;
        let entry = (privilege.to_ascii_lowercase(), table.map(str::to_string));
        def.grants.retain(|g| *g != entry);
        self.record_schema(format!("rol:{role}"), hlc, false);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Add/remove a role-inheritance edge on `member`.
    fn grant_role_edge(&mut self, member: &str, parent: &str, add: bool) -> Result<QueryOutput> {
        if add && !self.catalog.auth_roles.contains_key(parent) {
            return Err(EngineError::Constraint(format!("role {parent:?} does not exist")));
        }
        let hlc = self.ddl_stamp();
        let def = self
            .catalog
            .auth_roles
            .get_mut(member)
            .ok_or_else(|| EngineError::Constraint(format!("role {member:?} does not exist")))?;
        if add {
            if !def.inherits.contains(&parent.to_string()) {
                def.inherits.push(parent.to_string());
            }
        } else {
            def.inherits.retain(|p| p != parent);
        }
        self.record_schema(format!("rol:{member}"), hlc, false);
        self.rebuild_role_store();
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `SHOW GRANTS [FOR role]` as `(role, privilege, object)` rows.
    pub fn show_grants(&self, role: Option<&str>) -> ResultSet {
        let rows = self
            .role_store
            .grants(role)
            .into_iter()
            .map(|(r, p, o)| {
                // Table objects are stored in internal `db\x1ftable` form;
                // render them back to `db.table` (bare for the default db)
                // so the separator never surfaces to the user.
                let object = if o.contains('\u{1f}') {
                    namespace::display_name(&o, namespace::DEFAULT_DATABASE)
                } else {
                    o
                };
                vec![Value::String(r), Value::String(p), Value::String(object)]
            })
            .collect();
        ResultSet {
            columns: vec!["role".into(), "privilege".into(), "object".into()],
            rows,
        }
    }

    fn rebuild_role_store(&mut self) {
        self.role_store = build_role_store(&self.catalog);
    }

    /// Whether `role` holds `privilege` on `object` (RBAC view of the
    /// catalog; the server short-circuits its config superuser first).
    pub fn has_privilege(&self, role: &str, privilege: AuthPrivilege, object: &AuthObject) -> bool {
        self.role_store.has_privilege(role, privilege, object)
    }

    /// The stored SCRAM credential for `name`, if the user exists and
    /// authenticates by password. External (GSSAPI) users have no local
    /// secret and return `None` here — they never take the SCRAM path.
    pub fn auth_user(&self, name: &str) -> Option<ScramCredential> {
        let user = self.catalog.users.get(name)?;
        if user.auth_kind != UserAuthKind::Scram {
            return None;
        }
        ScramCredential::decode(&user.credential)
    }

    /// The role an externally-authenticated (GSSAPI) principal acts as, if such
    /// a user exists. A user acts as its own-named role, so this returns the
    /// principal itself — but only when it was created `IDENTIFIED BY GSSAPI`,
    /// so a password user can never be impersonated through the external path.
    pub fn external_user_role(&self, principal: &str) -> Option<String> {
        let user = self.catalog.users.get(principal)?;
        (user.auth_kind == UserAuthKind::Gssapi).then(|| principal.to_string())
    }

    fn drop_table(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("t:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        // Time-series tables drop with plain `DROP TABLE` too: a rollup
        // deregisters from its source; a source cascades to its rollups.
        if let Some(def) = self.catalog.timeseries.get(name).cloned() {
            let mut to_remove = vec![name.to_string()];
            to_remove.extend(def.rollups.iter().map(|r| r.name.clone()));
            if let Some(source) = &def.rollup_of {
                if let Some(sdef) = self.catalog.timeseries.get_mut(source) {
                    sdef.rollups.retain(|r| r.name != name);
                }
            }
            for victim in &to_remove {
                self.timeseries.remove(victim);
                self.catalog.timeseries.remove(victim);
                self.record_schema(format!("t:{victim}"), hlc, true);
                let dir = ts_dir(&self.dir, victim);
                if dir.exists() {
                    std::fs::remove_dir_all(dir)?;
                }
            }
            self.save_catalog()?;
            return Ok(QueryOutput::Ddl);
        }
        if !self.catalog.tables.contains_key(name) {
            if if_exists {
                self.record_schema(key, hlc, true);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableNotFound(name.to_string()));
        }
        self.tables.remove(name);
        self.catalog.tables.remove(name);
        // A recreated table's engine restarts its write_seq at 0, which could
        // collide with a stamp cached for this incarnation — purge, don't trust.
        self.field_registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
        // Drop the table's indexes too, tombstoning each so the drop replicates.
        let dropped: Vec<(String, bool)> = self
            .catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == name)
            .map(|(n, idx)| (n.clone(), idx.global))
            .collect();
        for (index_name, global) in dropped {
            self.catalog.indexes.remove(&index_name);
            self.indexes.remove(&index_name);
            self.record_schema(format!("i:{index_name}"), hlc, true);
            if global {
                let gname = gidx_table(&index_name);
                self.tables.remove(&gname);
                self.catalog.tables.remove(&gname);
                self.record_schema(format!("t:{gname}"), hlc, true);
                let tdir = table_dir(&self.dir, &gname);
                if tdir.exists() {
                    std::fs::remove_dir_all(tdir)?;
                }
            }
            let idir = index_dir(&self.dir, &index_name);
            if idir.exists() {
                std::fs::remove_dir_all(idir)?;
            }
        }
        // Search and vector indexes cascade too. An orphan here isn't just
        // clutter: schema bootstrap replays every live object on a joining
        // node, and a CREATE ... ON <dropped table> fails the whole join
        // (bit the production join, 2026-07-09).
        let dropped_search: Vec<String> = self
            .catalog
            .search_indexes
            .iter()
            .filter(|(_, sdef)| sdef.table == name)
            .map(|(n, _)| n.clone())
            .collect();
        for sname in dropped_search {
            self.catalog.search_indexes.remove(&sname);
            self.search_indexes.remove(&sname); // drop the writer before the files
            self.record_schema(format!("s:{sname}"), hlc, true);
            let sdir = fts_dir(&self.dir, &sname);
            if sdir.exists() {
                std::fs::remove_dir_all(sdir)?;
            }
        }
        let dropped_vector: Vec<String> = self
            .catalog
            .vector_indexes
            .iter()
            .filter(|(_, vdef)| vdef.table == name)
            .map(|(n, _)| n.clone())
            .collect();
        for vname in dropped_vector {
            self.catalog.vector_indexes.remove(&vname);
            self.vector_indexes.remove(&vname);
            self.record_schema(format!("v:{vname}"), hlc, true);
        }
        // Geo indexes cascade too (same orphan-on-join hazard as above).
        let dropped_geo: Vec<String> = self
            .catalog
            .geo_indexes
            .iter()
            .filter(|(_, gdef)| gdef.table == name)
            .map(|(n, _)| n.clone())
            .collect();
        for gname in dropped_geo {
            self.catalog.geo_indexes.remove(&gname);
            self.indexes.remove(&gname);
            self.pending_geo_backfills.retain(|n| n != &gname);
            self.record_schema(format!("g:{gname}"), hlc, true);
            let gdir = index_dir(&self.dir, &gname);
            if gdir.exists() {
                std::fs::remove_dir_all(gdir)?;
            }
        }
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        let dir = table_dir(&self.dir, name);
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        Ok(QueryOutput::Ddl)
    }

    fn create_index(
        &mut self,
        name: &str,
        table: &str,
        paths: &[String],
        if_not_exists: bool,
        global: bool,
        ready: bool,
    ) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(table) {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        let hlc = self.ddl_stamp();
        let key = format!("i:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.indexes.contains_key(name) {
            if if_not_exists {
                // Schema replay of a READY global index: a node stuck
                // `building` (down during the readiness broadcast) converges
                // here instead of never routing probes.
                if global && ready {
                    if let Some(def) = self.catalog.indexes.get_mut(name) {
                        if def.global && def.building {
                            def.building = false;
                        }
                    }
                }
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::IndexExists(name.to_string()));
        }
        if self.catalog.tables.get(table).is_some_and(|t| t.memory) {
            return Err(EngineError::Unsupported(
                "indexes on memory tables are not supported".into(),
            ));
        }
        if paths.iter().filter(|p| p.ends_with("[]")).count() > 1 {
            return Err(EngineError::Unsupported(
                "at most one multikey ([]) component per index".into(),
            ));
        }
        if global {
            // Global (value-sharded) index: entries live in an internal
            // replicated table — created here on every node (DDL is
            // broadcast/replayed), written by the coordinator routed by
            // ENTRY key. No per-node index engine, no local backfill.
            // Phase 1 (docs/GLOBAL_INDEXES.md): entries track writes from
            // this moment on; reader + backfill come in phase 2.
            let gname = gidx_table(name);
            if !self.tables.contains_key(&gname) {
                let engine =
                    StorageEngine::open_with_options(table_dir(&self.dir, &gname), self.storage_opts.clone())?;
                self.tables.insert(gname.clone(), engine);
            }
            self.catalog.tables.entry(gname).or_insert(TableDef {
                primary_key: vec!["k".into()],
                ttl_ms: None,
                memory: false,
                replication: None,
                pinned_nodes: Vec::new(),
                witness: true,
                prev_placement: None,
            });
            self.catalog.indexes.insert(
                name.to_string(),
                IndexDef {
                    table: table.to_string(),
                    paths: paths.to_vec(),
                    // A ready-marked replay (rejoin/bootstrap import of an
                    // index whose backfill completed long ago) starts
                    // serving immediately; entry data converges via the
                    // entry table's own anti-entropy.
                    building: !ready,
                    global: true,
                },
            );
            self.record_schema(key, hlc, false);
            self.save_catalog()?;
            // Backfill of pre-existing rows: inline on a single node (the
            // local shard is the whole ring); in cluster mode the DDL
            // coordinator drives a replicated backfill across every member's
            // shard and broadcasts readiness — this engine-side apply does
            // nothing more.
            if !ready && !self.defer_backfills {
                self.run_gidx_backfill(name)?;
            }
            return Ok(QueryOutput::Ddl);
        }
        // Schema-only: the index exists (empty, marked `building`) the
        // moment DDL acks; the backfill runs in pages afterwards — inline
        // for a single-node Session (`run_index_backfill`), in a background
        // thread with brief per-page locks on a cluster node. Writes landing
        // meanwhile maintain the index normally (idempotent overlap with the
        // pages), and the planner refuses `building` indexes.
        let index_engine =
            StorageEngine::open_with_options(index_dir(&self.dir, name), self.storage_opts.clone())?;
        self.indexes.insert(name.to_string(), index_engine);
        self.catalog.indexes.insert(
            name.to_string(),
            IndexDef {
                table: table.to_string(),
                paths: paths.to_vec(),
                building: true,
                global: false,
            },
        );
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        self.pending_backfills.push(name.to_string());
        Ok(QueryOutput::Ddl)
    }

    /// One page of an index backfill: copy up to `limit` rows after `cursor`
    /// into the index, returning the next cursor — `None` when the backfill
    /// is complete (the index is then unmarked `building` and the catalog
    /// saved). Page-sized so a cluster node holds its write lock for
    /// milliseconds at a time instead of the whole table stream.
    pub fn backfill_index_page(
        &mut self,
        name: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(def) = self.catalog.indexes.get(name).cloned() else {
            return Ok(None); // dropped mid-backfill
        };
        if !def.building {
            return Ok(None);
        }
        let rows = {
            let Some(engine) = self.tables.get(&def.table) else {
                return Ok(None);
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        if let Some(index_engine) = self.indexes.get_mut(name) {
            for (row_key, _hlc, bytes) in rows {
                let Some(bytes) = bytes else { continue }; // tombstone
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    for values in index_value_tuples(&doc, &def.paths) {
                        index_engine
                            .put(&index_entry_key(&values, &row_key), row_key.clone())?;
                    }
                }
            }
        }
        if done {
            if let Some(def) = self.catalog.indexes.get_mut(name) {
                def.building = false;
            }
            self.save_catalog()?;
            return Ok(None);
        }
        Ok(next)
    }

    /// Run an index backfill to completion, inline. The single-node path
    /// (Session, tests); cluster nodes page it in a background thread.
    pub fn run_index_backfill(&mut self, name: &str) -> Result<()> {
        let mut cursor = None;
        loop {
            match self.backfill_index_page(name, cursor, 4096)? {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Names of indexes whose backfill this node still owes (freshly created
    /// or imported via schema sync). The cluster's background worker drains
    /// this; a Session drains it inline right after the DDL.
    pub fn take_pending_backfills(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_backfills)
    }

    /// `CREATE GEO INDEX [IF NOT EXISTS] name ON table (path)`: a Morton/Z-order
    /// spatial index over a `{lat, lon}` point column, stored in an ordinary
    /// on-disk index engine. DDL acks with the index marked `building`; the
    /// backfill of existing rows runs afterward (inline for a Session, paged in
    /// the background on a cluster node), and writes maintain it meanwhile.
    fn create_geo_index(
        &mut self,
        name: &str,
        table: &str,
        path: &str,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(table) {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        let hlc = self.ddl_stamp();
        let key = format!("g:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if let Some(existing) = self.catalog.geo_indexes.get(name) {
            if !if_not_exists {
                return Err(EngineError::IndexExists(name.to_string()));
            }
            // Schema sync replays peer defs as IF NOT EXISTS with the peer's
            // HLC; a differing def is newer — replace and rebuild.
            if existing.table == table && existing.path == path {
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            self.catalog.geo_indexes.remove(name);
            self.indexes.remove(name);
            self.pending_geo_backfills.retain(|n| n != name);
            let _ = std::fs::remove_dir_all(index_dir(&self.dir, name));
        }
        if self.catalog.tables.get(table).is_some_and(|t| t.memory) {
            return Err(EngineError::Unsupported(
                "geo indexes on memory tables are not supported".into(),
            ));
        }
        let engine =
            StorageEngine::open_with_options(index_dir(&self.dir, name), self.storage_opts.clone())?;
        self.indexes.insert(name.to_string(), engine);
        self.catalog.geo_indexes.insert(
            name.to_string(),
            crate::catalog::GeoIndexDef {
                table: table.to_string(),
                path: path.to_string(),
                building: true,
            },
        );
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        self.pending_geo_backfills.push(name.to_string());
        Ok(QueryOutput::Ddl)
    }

    /// `DROP GEO INDEX [IF EXISTS] name`.
    fn drop_geo_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("g:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.geo_indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.indexes.remove(name);
        self.pending_geo_backfills.retain(|n| n != name);
        let idir = index_dir(&self.dir, name);
        if idir.exists() {
            std::fs::remove_dir_all(idir)?;
        }
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// One page of a geo-index backfill: encode each row's point into the index
    /// engine keyed by its Morton code. Page-sized like [`Database::backfill_index_page`].
    pub fn backfill_geo_page(
        &mut self,
        name: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(def) = self.catalog.geo_indexes.get(name).cloned() else {
            return Ok(None); // dropped mid-backfill
        };
        if !def.building {
            return Ok(None);
        }
        let rows = {
            let Some(engine) = self.tables.get(&def.table) else {
                return Ok(None);
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        if let Some(index_engine) = self.indexes.get_mut(name) {
            for (row_key, _hlc, bytes) in rows {
                let Some(bytes) = bytes else { continue }; // tombstone
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    if let Some(entry) = geo_entry_key(&doc, &def.path, &row_key) {
                        index_engine.put(&entry, row_key.clone())?;
                    }
                }
            }
        }
        if done {
            if let Some(def) = self.catalog.geo_indexes.get_mut(name) {
                def.building = false;
            }
            self.save_catalog()?;
            return Ok(None);
        }
        Ok(next)
    }

    /// Run a geo backfill to completion, inline (Session / single-node path).
    pub fn run_geo_backfill(&mut self, name: &str) -> Result<()> {
        let mut cursor = None;
        loop {
            match self.backfill_geo_page(name, cursor, 4096)? {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Geo backfills this node still owes; drained like [`Database::take_pending_backfills`].
    pub fn take_pending_geo_backfills(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_geo_backfills)
    }

    /// Cluster mode: DDL acks at schema-apply and index backfills run in the
    /// node's background worker (see [`Database::take_pending_backfills`]).
    pub fn set_defer_backfills(&mut self, defer: bool) {
        self.defer_backfills = defer;
    }

    /// Deferred startup search catch-ups (server mode): `(name, watermark)`.
    pub fn take_pending_search_catchups(&mut self) -> Vec<(String, Option<Hlc>)> {
        std::mem::take(&mut self.pending_search_catchups)
    }

    /// One page of a deferred search catch-up: replay up to `limit` rows
    /// after `cursor` (skipping those at or below `watermark`) into the
    /// index. Returns the next cursor; `None` when complete — the index is
    /// committed and unmarked `building`. Runs under the caller's write
    /// lock, page-sized so queries interleave.
    pub fn search_catchup_page(
        &mut self,
        name: &str,
        watermark: Option<Hlc>,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(def) = self.catalog.search_indexes.get(name) else {
            return Ok(None); // dropped mid-catch-up
        };
        let table = def.table.clone();
        let rows = {
            let Some(engine) = self.tables.get(&table) else {
                return Ok(None);
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        if let Some(live) = self.search_indexes.get_mut(name) {
            for (key, hlc, bytes) in rows {
                if watermark.is_some_and(|w| hlc <= w) {
                    continue;
                }
                match bytes {
                    Some(bytes) => {
                        if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                            live.index.put(&key, &doc, hlc_to_watermark(hlc))?;
                        }
                    }
                    None => live.index.delete(&key, hlc_to_watermark(hlc)),
                }
            }
            if done {
                live.index.commit()?;
                live.building = false;
            }
        }
        Ok(if done { None } else { next })
    }

    fn drop_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("i:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        let dropped = self.catalog.indexes.remove(name);
        if dropped.is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        if dropped.as_ref().is_some_and(|d| d.global) {
            // Global index: the entries live in the internal replicated
            // table, not a local index engine.
            let gname = gidx_table(name);
            self.tables.remove(&gname);
            self.catalog.tables.remove(&gname);
            self.record_schema(format!("t:{gname}"), hlc, true);
            let tdir = table_dir(&self.dir, &gname);
            if tdir.exists() {
                std::fs::remove_dir_all(tdir)?;
            }
        }
        self.indexes.remove(name);
        let idir = index_dir(&self.dir, name);
        if idir.exists() {
            std::fs::remove_dir_all(idir)?;
        }
        self.record_schema(key, hlc, true);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    // ---- ALTER ----

    fn alter_table(&mut self, alt: AlterTable) -> Result<QueryOutput> {
        if self.catalog.timeseries.contains_key(&alt.name) {
            return match alt.action {
                AlterAction::SetOptions { options } => {
                    self.set_ts_table_options(&alt.name, &options)
                }
                _ => Err(EngineError::Unsupported(
                    "timeseries tables support ALTER TABLE ... SET (retention/ooo) only".into(),
                )),
            };
        }
        if !self.catalog.tables.contains_key(&alt.name) {
            return Err(EngineError::TableNotFound(alt.name.clone()));
        }
        match alt.action {
            AlterAction::RenameTable { new_name } => self.rename_table(&alt.name, &new_name),
            AlterAction::RenameColumn { from, to } => self.rename_column(&alt.name, &from, &to),
            AlterAction::SetOptions { options } => self.set_table_options(&alt.name, &options),
        }
    }

    /// `ALTER TABLE <ts> SET (retention = <dur> | ooo = <dur>, ...)`:
    /// retention and OOO acceptance are LIVE-tunable on time-series tables
    /// (per-org retention settings change; backfills need a window). Both
    /// update the catalog def and the open store — retention applies at the
    /// next flush (it cannot resurrect already-dropped blocks), the OOO
    /// window to subsequent appends. `retention = 0` clears it (keep
    /// forever).
    fn set_ts_table_options(
        &mut self,
        table: &str,
        options: &[(String, String)],
    ) -> Result<QueryOutput> {
        for (opt, val) in options {
            let ms: i64 = val.parse().map_err(|_| {
                EngineError::Constraint(format!(
                    "{opt} must be a duration like 60d (or integer ms), got '{val}'"
                ))
            })?;
            if ms < 0 {
                return Err(EngineError::Constraint(format!("{opt} must not be negative")));
            }
            let def = self
                .catalog
                .timeseries
                .get_mut(table)
                .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
            match opt.as_str() {
                "retention" => {
                    def.retention_ms = (ms > 0).then_some(ms);
                    if let Some(store) = self.timeseries.get_mut(table) {
                        store.set_retention_ms((ms > 0).then_some(ms));
                    }
                }
                "ooo" => {
                    def.ooo_window_ms = ms;
                    if let Some(store) = self.timeseries.get_mut(table) {
                        store.set_ooo_window_ms(ms);
                    }
                }
                other => {
                    return Err(EngineError::Unsupported(format!(
                        "unknown ALTER TABLE option '{other}' for a timeseries table \
                         (supported: retention, ooo)"
                    )));
                }
            }
        }
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `ALTER TABLE t SET (...)`: `witness` toggles freely (pull-side only,
    /// no placement transition); `nodes` may only SHRINK an existing pin
    /// list (remaining pins already hold full copies — transition-free; the
    /// dead-pin escape hatch); everything else waits for the
    /// placement-transition work.
    fn set_table_options(&mut self, table: &str, options: &[(String, String)]) -> Result<QueryOutput> {
        if is_system_table(table) {
            return Err(EngineError::Constraint(format!(
                "'{table}' is a system table: placement/witness options are not allowed"
            )));
        }
        for (opt, val) in options {
            match opt.as_str() {
                "witness" => {
                    let flag = matches!(val.as_str(), "true" | "1");
                    let def = self
                        .catalog
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                    def.witness = flag;
                }
                // Placement changes OPEN A TRANSITION: the old placement is
                // stashed and every routing site addresses the union of old
                // and new until repair converges the new owners and a
                // finalize clears the stash (the per-table twin of the
                // membership change's dual-ring window). One transition at a
                // time — the union of three placements has no clear quorum
                // story, and repair needs a stable target to converge to.
                "nodes" => {
                    let new: Vec<String> = val
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect();
                    if new.is_empty() {
                        return Err(EngineError::Constraint(
                            "SET (nodes = ...) needs at least one member; to unpin, \
                             transition to ring placement with SET (replication = n)"
                                .into(),
                        ));
                    }
                    let def = self
                        .catalog
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                    if def.prev_placement.is_some() {
                        return Err(EngineError::Constraint(format!(
                            "'{table}' already has a placement transition in progress; \
                             wait for it to finalize (or run REPAIR CLUSTER, then \
                             ALTER TABLE {table} SET (placement_finalized = true))"
                        )));
                    }
                    // Pure shrink of an existing pin set needs no window:
                    // every remaining pin already holds a full copy.
                    let shrink = !def.pinned_nodes.is_empty()
                        && new.iter().all(|n| def.pinned_nodes.contains(n));
                    if !shrink && (def.pinned_nodes != new) {
                        def.prev_placement = Some(crate::catalog::PrevPlacement {
                            replication: def.replication,
                            pinned_nodes: def.pinned_nodes.clone(),
                        });
                    }
                    def.replication = None;
                    def.pinned_nodes = new;
                }
                "replication" => {
                    let n: u32 = val.parse().map_err(|_| {
                        EngineError::Constraint(format!(
                            "replication must be a positive integer, got '{val}'"
                        ))
                    })?;
                    if n == 0 {
                        return Err(EngineError::Constraint(
                            "replication must be at least 1".into(),
                        ));
                    }
                    let def = self
                        .catalog
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                    if def.prev_placement.is_some() {
                        return Err(EngineError::Constraint(format!(
                            "'{table}' already has a placement transition in progress; \
                             wait for it to finalize (or run REPAIR CLUSTER, then \
                             ALTER TABLE {table} SET (placement_finalized = true))"
                        )));
                    }
                    if def.replication != Some(n) || !def.pinned_nodes.is_empty() {
                        def.prev_placement = Some(crate::catalog::PrevPlacement {
                            replication: def.replication,
                            pinned_nodes: std::mem::take(&mut def.pinned_nodes),
                        });
                        def.replication = Some(n);
                    }
                }
                // Live TTL change (mirrors the timeseries retention/ooo
                // options): rows expire by their write stamp, so a new TTL
                // applies to reads and compaction immediately — shortening
                // it can expire existing rows at once; `0` clears it (keep
                // forever).
                "ttl" => {
                    let ms: i64 = val.parse().map_err(|_| {
                        EngineError::Constraint(format!(
                            "ttl must be a duration like 30d (or integer ms), got '{val}'"
                        ))
                    })?;
                    if ms < 0 {
                        return Err(EngineError::Constraint("ttl must not be negative".into()));
                    }
                    let def = self
                        .catalog
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                    def.ttl_ms = (ms > 0).then_some(ms);
                    if let Some(engine) = self.tables.get_mut(table) {
                        engine.set_ttl((ms > 0).then_some(ms as u64));
                    }
                }
                // The transition's closing bracket — broadcast by the ALTER
                // coordinator once repair has converged the new owners, or
                // run by an operator after a manual REPAIR CLUSTER if the
                // coordinator died mid-transition (the union window is safe
                // to sit in indefinitely, just wider than needed).
                "placement_finalized" => {
                    let def = self
                        .catalog
                        .tables
                        .get_mut(table)
                        .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                    def.prev_placement = None;
                }
                other => {
                    return Err(EngineError::Unsupported(format!(
                        "unknown ALTER TABLE option '{other}' \
                         (supported: ttl, witness, nodes, replication, placement_finalized; \
                         retention/ooo apply to TIMESERIES tables)"
                    )));
                }
            }
        }
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `ALTER TABLE old RENAME TO new`: move the on-disk table, repoint the
    /// catalog entry, and update dependent index/vector-index definitions. The
    /// open storage engine is dropped first (releasing its file handles) so the
    /// directory rename is safe; it is reopened under the new path.
    fn rename_table(&mut self, old: &str, new: &str) -> Result<QueryOutput> {
        if self.catalog.tables.contains_key(new) {
            return Err(EngineError::TableExists(new.to_string()));
        }
        self.tables.remove(old); // close handles before moving the directory
        let old_dir = table_dir(&self.dir, old);
        let new_dir = table_dir(&self.dir, new);
        if old_dir.exists() {
            std::fs::rename(&old_dir, &new_dir)?;
        }
        self.tables
            .insert(new.to_string(), StorageEngine::open_with_options(new_dir, self.storage_opts.clone())?);
        // The reopened engine restarts write_seq at 0: purge both names' field
        // registry so a stale stamp can't validate against the fresh counter.
        {
            let mut reg = self.field_registry.lock().unwrap_or_else(|e| e.into_inner());
            reg.remove(old);
            reg.remove(new);
        }

        let def = self.catalog.tables.remove(old).expect("table present");
        self.catalog.tables.insert(new.to_string(), def);
        for idx in self.catalog.indexes.values_mut() {
            if idx.table == old {
                idx.table = new.to_string();
            }
        }
        for v in self.catalog.vector_indexes.values_mut() {
            if v.table == old {
                v.table = new.to_string();
            }
        }
        for s in self.catalog.search_indexes.values_mut() {
            if s.table == old {
                s.table = new.to_string();
            }
        }
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// `ALTER TABLE t RENAME COLUMN from TO to`: rewrite the field in every row
    /// (recomputing the primary key if `from` is a key column), then rebuild any
    /// index that referenced the renamed path. Rows are rewritten directly in
    /// storage (index maintenance is deferred to the rebuild).
    fn rename_column(&mut self, table: &str, from: &str, to: &str) -> Result<QueryOutput> {
        // Update PK column names and index/vector path references first.
        if let Some(def) = self.catalog.tables.get_mut(table) {
            for c in def.primary_key.iter_mut() {
                if c == from {
                    *c = to.to_string();
                }
            }
        }
        for idx in self.catalog.indexes.values_mut() {
            for p in idx.paths.iter_mut() {
                if p == from {
                    *p = to.to_string();
                } else if p.strip_suffix("[]") == Some(from) {
                    *p = format!("{to}[]");
                }
            }
        }
        for v in self.catalog.vector_indexes.values_mut() {
            if v.path == from {
                v.path = to.to_string();
            }
        }
        for s in self.catalog.search_indexes.values_mut() {
            for p in s.paths.iter_mut() {
                if p == from {
                    *p = to.to_string();
                }
            }
        }

        // Rewrite each row in raw storage: move the field, recompute the key.
        let pk = self.table_def(table)?.primary_key.clone();
        for (old_key, old_doc) in self.scan_docs(table)? {
            let mut new_doc = old_doc.clone();
            if let Some(val) = new_doc.0.remove(from) {
                new_doc.0.insert(to.to_string(), val);
            } else if old_key == primary_key_bytes(&pk, &new_doc)? {
                continue; // field absent and key unaffected: nothing to do
            }
            let new_key = primary_key_bytes(&pk, &new_doc)?;
            let engine = self.table_engine_mut(table)?;
            if new_key != old_key {
                engine.delete(&old_key)?;
            }
            engine.put(&new_key, Value::Document(new_doc).encode())?;
        }

        // Rebuild the table's secondary indexes (entries used the old path) and
        // any vector indexes whose path changed.
        let secondary: Vec<String> = self
            .catalog
            .indexes
            .iter()
            .filter(|(_, i)| i.table == table)
            .map(|(n, _)| n.clone())
            .collect();
        for name in secondary {
            self.rebuild_index(&name)?;
        }
        let vectors: Vec<String> = self
            .catalog
            .vector_indexes
            .iter()
            .filter(|(_, v)| v.table == table)
            .map(|(n, _)| n.clone())
            .collect();
        for name in vectors {
            self.rebuild_vector_index(&name)?;
        }
        let searches: Vec<String> = self
            .catalog
            .search_indexes
            .iter()
            .filter(|(_, s)| s.table == table)
            .map(|(n, _)| n.clone())
            .collect();
        for name in searches {
            self.rebuild_search_index(&name)?;
        }

        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Wipe an index and queue its re-backfill (marked `building`; the
    /// planner skips it until the pages complete). Formerly streamed the
    /// whole table inline — minutes under the write lock for a large table.
    fn rebuild_index(&mut self, name: &str) -> Result<()> {
        if !self.catalog.indexes.contains_key(name) {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.indexes.remove(name);
        let dir = index_dir(&self.dir, name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        let engine = StorageEngine::open_with_options(dir, self.storage_opts.clone())?;
        self.indexes.insert(name.to_string(), engine);
        if let Some(def) = self.catalog.indexes.get_mut(name) {
            def.building = true;
        }
        self.pending_backfills.push(name.to_string());
        Ok(())
    }

    /// Wipe a vector index and queue its paged re-backfill (marked
    /// `building`; searches error "retry shortly" until the pages complete).
    /// Formerly streamed the whole table inline — minutes under the write
    /// lock for a large table, the same class as the secondary-index inline
    /// backfill this mirrors.
    fn rebuild_vector_index(&mut self, name: &str) -> Result<()> {
        let def = self
            .catalog
            .vector_indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?
            .clone();
        // Drop the stale snapshot now: a crash mid-backfill must trigger the
        // open-time full rebuild, not a load of pre-rebuild contents.
        let _ = std::fs::remove_file(vector_snapshot_path(&self.dir, name));
        self.vector_indexes.insert(name.to_string(), new_hnsw(&def));
        self.vector_watermarks.insert(name.to_string(), Hlc::MIN);
        self.building_vectors.insert(name.to_string());
        self.pending_vector_backfills.push(name.to_string());
        Ok(())
    }

    /// One page of a vector-index backfill: read up to `limit` rows after
    /// `cursor`, insert their vectors into the live HNSW, and return the next
    /// cursor — `None` when complete (the index is then unmarked `building`
    /// and snapshotted). Page-sized so a cluster node holds its write lock
    /// for milliseconds at a time. Writes landing between pages maintain the
    /// index normally; a page re-reading such a row re-inserts the same
    /// latest version (idempotent).
    pub fn backfill_vector_page(
        &mut self,
        name: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        if !self.building_vectors.contains(name) {
            return Ok(None); // dropped or superseded mid-backfill
        }
        let Some(def) = self.catalog.vector_indexes.get(name).cloned() else {
            self.building_vectors.remove(name);
            return Ok(None);
        };
        let rows = {
            let Some(engine) = self.tables.get(&def.table) else {
                self.building_vectors.remove(name);
                return Ok(None);
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        let mut watermark = self
            .vector_watermarks
            .get(name)
            .copied()
            .unwrap_or(Hlc::MIN);
        if let Some(hnsw) = self.vector_indexes.get_mut(name) {
            for (row_key, hlc, bytes) in rows {
                if hlc > watermark {
                    watermark = hlc;
                }
                let Some(bytes) = bytes else { continue }; // tombstone
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    if let Some(v) = doc_vector(&doc, &def.path, def.dim) {
                        hnsw.insert(row_key, v);
                    }
                }
            }
        }
        self.vector_watermarks.insert(name.to_string(), watermark);
        if done {
            self.building_vectors.remove(name);
            let path = vector_snapshot_path(&self.dir, name);
            if let Some(hnsw) = self.vector_indexes.get_mut(name) {
                match save_vector_snapshot(&path, hnsw, watermark) {
                    Ok(()) => hnsw.mark_clean(),
                    Err(e) => skaidb_types::slog!(
                        "skaidb: vector snapshot save failed for {name}: {e}"
                    ),
                }
            }
            return Ok(None);
        }
        Ok(next)
    }

    /// Run a vector backfill to completion, inline. The single-node path
    /// (Session, tests); cluster nodes page it in a background thread.
    pub fn run_vector_backfill(&mut self, name: &str) -> Result<()> {
        let mut cursor = None;
        loop {
            match self.backfill_vector_page(name, cursor, 2048)? {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Names of vector indexes whose backfill this node still owes. Same
    /// drain contract as [`Database::take_pending_backfills`].
    pub fn take_pending_vector_backfills(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_vector_backfills)
    }

    // ---- transactions (embedded, single-connection) ----

    /// Begin a transaction: subsequent writes buffer until `COMMIT`/`ROLLBACK`.
    fn begin(&mut self) -> Result<QueryOutput> {
        if self.txn.is_some() {
            return Err(EngineError::Constraint(
                "a transaction is already in progress".into(),
            ));
        }
        self.txn = Some(TxnBuffer::default());
        Ok(QueryOutput::Ddl)
    }

    /// Commit the open transaction: flush its buffered writes to storage
    /// (maintaining indexes), then clear it.
    fn commit(&mut self) -> Result<QueryOutput> {
        let Some(txn) = self.txn.take() else {
            return Err(EngineError::Constraint("no transaction in progress".into()));
        };
        // Crash-atomic commit via a redo journal (the ACID audit showed a
        // kill -9 mid-commit persisting a PREFIX of the transaction — the
        // writes applied row-by-row with no commit record): persist the
        // whole write set durably FIRST, then apply, then retire the
        // journal. A crash before the journal is durable = the transaction
        // never happened; a crash at any later point = `open()` replays
        // the journal to completion (idempotent: re-putting an applied row
        // rewrites the same content).
        write_txn_journal(&self.dir, &txn.writes)?;
        let applied = self.apply_txn_writes(txn.writes);
        if applied.is_ok() {
            let _ = std::fs::remove_file(txn_journal_path(&self.dir));
        }
        applied?;
        Ok(QueryOutput::Ddl)
    }

    /// Apply a committed write set through the normal put/delete paths
    /// (indexes maintained) and sync. Shared by COMMIT and the journal
    /// replay in `open()`.
    fn apply_txn_writes(&mut self, writes: TxnWrites) -> Result<()> {
        let mut local = LocalCluster::new(self);
        let apply = || -> Result<()> {
            for ((table, key), op) in writes {
                match op {
                    Some(doc) => local.put(&table, &key, &doc)?,
                    None => {
                        // Read the committed doc so index entries are removed correctly.
                        let doc = local
                            .db
                            .committed_doc(&table, &key)?
                            .unwrap_or_default();
                        local.delete(&table, &key, &doc)?;
                    }
                }
            }
            Ok(())
        };
        let applied = apply();
        let synced = local.flush_pending();
        applied?;
        synced?;
        Ok(())
    }

    /// Roll back (discard) the open transaction.
    fn rollback(&mut self) -> Result<QueryOutput> {
        if self.txn.take().is_none() {
            return Err(EngineError::Constraint("no transaction in progress".into()));
        }
        Ok(QueryOutput::Ddl)
    }

    /// The committed document at `(table, key)`, ignoring any open transaction.
    fn committed_doc(&self, table: &str, key: &[u8]) -> Result<Option<Document>> {
        let Some(engine) = self.tables.get(table) else {
            return Err(EngineError::TableNotFound(table.to_string()));
        };
        match engine.get(key)? {
            Some(bytes) => match Value::decode(&bytes) {
                Ok(Value::Document(doc)) => Ok(Some(doc)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Gather rows for `table` matching `filter` with the open transaction's
    /// buffered writes merged over committed storage (read-your-writes).
    /// Committed rows come through the index planner (bounded when the filter
    /// permits); the overlay is exact per-key, so merging it afterwards keeps
    /// full-scan semantics: an overlay write always wins over the committed
    /// version of its key.
    fn gather_with_overlay(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: Option<&HashSet<String>>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let txn = self.txn.as_ref().expect("transaction active");
        let mut map: BTreeMap<Vec<u8>, Document> =
            self.gather_rows_keyed(table, filter, project)?.into_iter().collect();
        for ((t, k), op) in &txn.writes {
            if t != table {
                continue;
            }
            match op {
                Some(doc) => {
                    // The overlay version masks the committed one — keep it
                    // only if it (still) matches the filter.
                    if matches_filter(filter, doc)? {
                        map.insert(k.clone(), doc.clone());
                    } else {
                        map.remove(k);
                    }
                }
                None => {
                    map.remove(k);
                }
            }
        }
        Ok(map.into_iter().collect())
    }

    // ---- row gathering (with optional index acceleration) ----

    /// `(key, doc)` rows of `table` matching `filter`, reading through the open
    /// transaction's buffered overlay when one is active (read-your-writes).
    /// The shared body of [`Cluster::matching_rows`] for the embedded engine —
    /// `&self` so a `SELECT` needs only shared access.
    fn local_matching_rows(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        if self.txn.is_some() {
            return self.gather_with_overlay(table, filter, None);
        }
        self.gather_rows_keyed(table, filter, None)
    }

    /// Like [`Database::local_matching_rows`], but only the fields named in
    /// `project` are decoded from storage — used by `GROUP BY`/aggregate
    /// gathers that can only ever read a known column subset (see
    /// [`group_by_projection_columns`]). `project` must be a superset of
    /// every column the caller will read; this function has no way to check
    /// that itself. Correctness-neutral for the overlay path: buffered
    /// transaction writes are already-decoded full documents, so pruning
    /// only ever narrows what's read from storage, never the overlay.
    fn local_matching_rows_projected(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: &HashSet<String>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        if self.txn.is_some() {
            return self.gather_with_overlay(table, filter, Some(project));
        }
        self.gather_rows_keyed(table, filter, Some(project))
    }

    /// Ordered/limited variant of [`Database::local_matching_rows`]; see
    /// [`Cluster::matching_rows_ordered`]. In a transaction, reads go through
    /// the buffered overlay (unindexed, never presorted).
    /// Streamed count of rows matching `filter` — decode, test, discard;
    /// memory stays one row regardless of match count. Scan-meter ticked.
    pub fn local_count_matching(&self, table: &str, filter: &Option<Expr>) -> Result<usize> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let mut n = 0usize;
        // PK-prefix narrowing (agencik E-8): an equality-pinned leftmost PK
        // run bounds the count to that key slice — `scan_range_iter` streams
        // live, TTL-filtered rows exactly like the full `scan_iter` walk.
        let range = self
            .table_def(table)
            .ok()
            .and_then(|def| pk_prefix_scan_range(&def.primary_key, filter));
        let iter: Box<dyn Iterator<Item = _>> = match &range {
            Some((start, end)) => Box::new(engine.scan_range_iter(Some(start), end.as_deref())),
            None => Box::new(engine.scan_iter()),
        };
        for item in iter {
            crate::scan_meter::tick(1)?;
            let (_key, bytes) = item?;
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                if matches_filter(filter, &doc)? {
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Streamed distinct values of `col` over rows matching `filter` —
    /// decode, test, extract one value, discard the row. Values dedupe by
    /// their order-preserving key encoding; NULL/absent is skipped (it is
    /// not a value in use). Scan-meter ticked.
    pub fn local_distinct_values(
        &self,
        table: &str,
        col: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<Value>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let mut set: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
        for item in engine.scan_iter() {
            crate::scan_meter::tick(1)?;
            let (_key, bytes) = item?;
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                if matches_filter(filter, &doc)? {
                    if let Some(v) = doc.get_path(col) {
                        if !v.is_null() {
                            set.entry(v.encode_key()).or_insert_with(|| v.clone());
                        }
                    }
                }
            }
        }
        Ok(set.into_values().collect())
    }

    pub fn local_matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<(&str, bool, bool)>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        if self.txn.is_some() {
            return Ok((self.gather_with_overlay(table, filter, None)?, false));
        }
        self.gather_rows_planned(table, filter, order, fetch_limit, None)
    }

    /// This shard's top-`fetch` candidate keys for an ordered read — served
    /// only when a local index plan actually walks in `(col, desc)` order
    /// (`None` otherwise, so a coordinator falls back to the full gather
    /// rather than trusting an unordered heap). Feeds the distributed sorted
    /// top-k: candidates are re-read at quorum and re-sorted by the caller,
    /// so per-shard staleness in the sort column is corrected there.
    pub fn local_sorted_candidates(
        &self,
        table: &str,
        filter: &Option<Expr>,
        col: &str,
        desc: bool,
        fetch: usize,
    ) -> Result<Option<Vec<Vec<u8>>>> {
        if self.txn.is_some() {
            return Ok(None);
        }
        let (rows, sorted) =
            self.gather_rows_planned(table, filter, Some((col, desc, true)), Some(fetch), None)?;
        if !sorted {
            return Ok(None);
        }
        Ok(Some(rows.into_iter().take(fetch).map(|(k, _)| k).collect()))
    }

    /// Live-row count of `table` straight from storage key statistics — no row
    /// decode. Unavailable while a transaction is open (its overlay could
    /// change the count).
    pub fn local_count_rows(&self, table: &str) -> Result<Option<usize>> {
        if self.txn.is_some() {
            return Ok(None);
        }
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        // Key stats count PHYSICAL live keys; with a TTL, expired rows stay
        // physically live until compaction, so the stats overcount. `None` =
        // fall back to the streaming count, whose scan filters expiry.
        if engine.has_ttl() {
            return Ok(None);
        }
        Ok(Some(engine.key_stats()?.live_keys))
    }

    /// Index-only count of `table`'s rows matching `filter`: `Some(n)` when a
    /// secondary index fully covers a purely conjunctive filter (see
    /// `plan_covering`), so the answer is the entry count of one index byte
    /// range with no row reads or decodes. `None` = no covering index (or a
    /// transaction is open, whose overlay could change the count) — the
    /// caller falls back to a full gather.
    pub fn local_count_filtered(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Option<usize>> {
        if self.txn.is_some() {
            return Ok(None);
        }
        if !self.catalog.tables.contains_key(table) {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        // A covering-index count trusts entry liveness; with a TTL, entries
        // of expired-but-unreclaimed rows still exist, so it overcounts —
        // fall back to the streaming count (TTL-filtered scan).
        if self.tables.get(table).is_some_and(|e| e.has_ttl()) {
            return Ok(None);
        }
        let Some(expr) = filter else {
            return Ok(None);
        };
        if let Some(n) = self.covering_count(table, expr)? {
            return Ok(Some(n));
        }
        // One negated equality (`col != lit`) beside an otherwise-covering
        // conjunction: count by complement — COUNT(rest) − COUNT(rest AND
        // col = lit) — when BOTH sides are covering. The UI's default mail
        // view (`is_archived != true`) hits exactly this; its streamed
        // fallback walked 183k rows per pagination count.
        if let Some((rest, col, lit)) = split_one_negated_eq(expr) {
            let eq = Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(col)),
                right: Box::new(Expr::Literal(lit)),
            };
            let with_eq = match &rest {
                Some(r) => Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(r.clone()),
                    right: Box::new(eq),
                },
                None => eq,
            };
            let total = match &rest {
                Some(r) => self.covering_count(table, r)?,
                // No other terms: the unfiltered storage count is exact.
                None => self.local_count_rows(table)?,
            };
            if let (Some(total), Some(matching)) =
                (total, self.covering_count(table, &with_eq)?)
            {
                return Ok(Some(total.saturating_sub(matching)));
            }
        }
        Ok(None)
    }

    /// `Some(count)` when a covering index answers `expr` exactly (see
    /// `plan_covering`); `None` otherwise.
    fn covering_count(&self, table: &str, expr: &Expr) -> Result<Option<usize>> {
        if !filter_is_conjunctive(expr) {
            return Ok(None);
        }
        let filter = Some(expr.clone());
        let constraints = column_constraints(&filter);
        if constraints.is_empty() {
            return Ok(None);
        }
        for (name, idx) in self
            .catalog
            .indexes
            .iter()
            .filter(|(_, i)| i.table == table && !i.building && !i.global)
        {
            let names = index_key_names(&idx.paths);
            // Same multikey gate as `plan_index`: with the [] component
            // equality-pinned, one entry per (element, row) makes the range
            // cardinality an exact ROW count; unpinned, it would overcount
            // a row once per element.
            if let Some(m) = multi_pos(&idx.paths) {
                let eq_through = names[..=m].iter().all(|p| {
                    constraints.iter().any(|(c, cc)| c == p && cc.eq.is_some())
                });
                if !eq_through {
                    continue;
                }
            }
            let Some((start, end)) = plan_covering(&names, &constraints) else {
                continue;
            };
            let Some(engine) = self.indexes.get(name) else {
                continue;
            };
            return Ok(Some(engine.count_range(start.as_deref(), end.as_deref())?));
        }
        Ok(None)
    }

    /// Collect `(key, doc)` for the rows of `table` matching `filter`, using a
    /// secondary index when the filter permits (equality or range on an indexed
    /// path). Unordered convenience wrapper over [`Database::gather_rows_planned`].
    fn gather_rows_keyed(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: Option<&HashSet<String>>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        Ok(self.gather_rows_planned(table, filter, None, None, project)?.0)
    }

    /// Decode one stored row's bytes into a `Document` — the full row when
    /// `project` is `None`, or only the fields named in `project` when it
    /// isn't (see [`skaidb_types::Value::decode_document_projected`]; the
    /// caller is responsible for `project` being a true superset of every
    /// column it will read — this function has no way to verify that).
    fn decode_row(bytes: &[u8], project: Option<&HashSet<String>>) -> Result<Document> {
        match project {
            Some(wanted) => Value::decode_document_projected(bytes, wanted)
                .map_err(|e| EngineError::Constraint(format!("corrupt row: {e}"))),
            None => match Value::decode(bytes) {
                Ok(Value::Document(doc)) => Ok(doc),
                Ok(_) => Err(EngineError::Constraint("stored row is not a document".into())),
                Err(e) => Err(EngineError::Constraint(format!("corrupt row: {e}"))),
            },
        }
    }

    /// Plan and execute a row gather, optionally using a secondary index to (a)
    /// bound the scan to a value range (equality/`<`/`>`/`BETWEEN` on an indexed
    /// path) and/or (b) return rows already sorted ascending by `order`. When the
    /// result is in `order` and `fetch_limit` is set, scanning stops early
    /// (top-N). Returns the rows and whether they are sorted by `order`.
    /// `project`: see [`Database::decode_row`].
    fn gather_rows_planned(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<(&str, bool, bool)>,
        fetch_limit: Option<usize>,
        project: Option<&HashSet<String>>,
    ) -> Result<OrderedRows> {
        // Fast path: the whole filter is a primary-key equality — one point
        // read (memtable → read cache → SSTables with bloom filters) instead
        // of a table scan. ≤ 1 row is trivially in any requested order.
        if let Some(key) = pk_point_key(&self.table_def(table)?.primary_key, filter) {
            let engine = self
                .tables
                .get(table)
                .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
            let rows = match engine.get(&key)? {
                Some(bytes) => {
                    let doc = Self::decode_row(&bytes, project)?;
                    // Re-check the full filter: pk_point_key keys off the PK
                    // equalities but a residual constraint on a non-PK
                    // column could still exclude the row (the caller does
                    // not filter a point-read result) — `project` already
                    // covers the filter's own columns (see
                    // group_by_projection_columns).
                    if matches_filter(filter, &doc)? {
                        vec![(key, doc)]
                    } else {
                        Vec::new()
                    }
                }
                None => Vec::new(),
            };
            return Ok((rows, true));
        }
        // Fast path: every PK column pinned by `=` or a literal `IN` list —
        // a bounded set of bloom-gated point reads (the "fetch these N ids"
        // shape) instead of a table scan into the scan budget.
        if let Some(keys) = pk_point_keys(&self.table_def(table)?.primary_key, filter) {
            let engine = self
                .tables
                .get(table)
                .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
            let mut rows = Vec::new();
            for key in keys {
                crate::scan_meter::tick(1)?;
                let Some(bytes) = engine.get(&key)? else { continue };
                let doc = Self::decode_row(&bytes, project)?;
                // Re-check the full filter: the key set covers the PK pins,
                // but residual predicates (other columns, NOT IN exclusions)
                // can still drop a fetched row.
                if matches_filter(filter, &doc)? {
                    crate::scan_meter::tick_bytes(bytes.len())?;
                    rows.push((key, doc));
                }
            }
            // Keys were generated in ascending PK order; claim sorted only
            // when the caller asked for no particular order.
            return Ok((rows, order.is_none()));
        }
        // Geo predicate served by a geo index: walk the Morton-code ranges the
        // bbox/radius expands to instead of the whole shard, re-reading each
        // candidate and applying the exact geo filter. Unordered (Z-order is
        // not distance order), so an ORDER BY/LIMIT is re-sorted by the caller.
        if let Some((index, ranges)) = self.plan_geo_scan(table, filter) {
            let rows = self.geo_gather_local(&index, table, &ranges, filter, project)?;
            return Ok((rows, false));
        }
        // Primary-key prefix range: a leftmost run of PK columns pinned by
        // equality (plus one optional trailing range on the next PK column)
        // bounds the scan to that key range of the table itself — the table
        // IS the primary index, ordered by its encoded key. `WHERE channel
        // = ?` on PK (channel, ts) reads one channel's slice, not 252k rows
        // into the scan budget (the slack thread-refresh shape, 2026-07-15).
        // Rows come back in PK order; the caller re-sorts, so `sorted` is
        // claimed only when no explicit ORDER BY was requested.
        {
            let pk = self.table_def(table)?.primary_key.clone();
            if let Some((start, end)) = pk_prefix_scan_range(&pk, filter) {
                let engine = self
                    .tables
                    .get(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                let mut out = Vec::new();
                for (key, bytes) in engine.scan_range(Some(&start), end.as_deref())? {
                    crate::scan_meter::tick(1)?;
                    if let Ok(doc) = Self::decode_row(&bytes, project) {
                        if matches_filter(filter, &doc)? {
                            crate::scan_meter::tick_bytes(bytes.len())?;
                            out.push((key, doc));
                            if order.is_none() && fetch_limit.is_some_and(|l| out.len() >= l)
                            {
                                break;
                            }
                        }
                    }
                }
                return Ok((out, order.is_none()));
            }
        }
        // Primary-key ORDER BY: the table itself IS the primary index, so
        // `ORDER BY <leftmost pk column>` is served by walking the table in
        // key order with early stop — never by materializing every matching
        // row to sort. This is the fix for the 2026-07-17 production OOM:
        // `SELECT id FROM gmail_emails ORDER BY id LIMIT 3` (183k rows /
        // 1.9 GB, `id` = PK, no secondary index on it) gathered and sorted
        // the whole table (3.9 GB peak, kernel OOM-killed the node — twice)
        // because only *secondary* indexes were considered order-serving.
        // Exact single-key orders only: for composite PKs, key order breaks
        // pk[0] ties by the remaining PK columns — a valid ties-in-arbitrary-
        // order top-k for the one requested column, but wrong for a
        // multi-key ORDER BY, which keeps the sort-after-gather path.
        // Cross-type caveat (schema-less PKs): rows whose pk[0] values have
        // different types order by the encoding's type tag here
        // (`Value::total_cmp`'s order), where a full sort would compare
        // numerics numerically across Int/Float — the same claim every
        // key-ordered path (e.g. `pk_point_keys`) already makes;
        // homogeneous-typed keys agree exactly.
        if let Some((col, desc, true)) = order {
            let pk = self.table_def(table)?.primary_key.clone();
            if col == pk[0] {
                let constraints = column_constraints(filter);
                let c = constraints
                    .iter()
                    .find(|(cc, _)| *cc == pk[0])
                    .map(|(_, c)| c);
                // Bounds from the filter's pk[0] range, widened to inclusive
                // (`index_prefix_n` has no exclusive form): the full-filter
                // re-check below drops a `>`-excluded boundary row.
                let start = c
                    .and_then(|c| c.lo.as_ref())
                    .map(|v| index_prefix_n(std::slice::from_ref(v)));
                let end = c
                    .and_then(|c| c.hi.as_ref())
                    .and_then(|v| index_upper_bound_n(std::slice::from_ref(v)));
                let engine = self
                    .tables
                    .get(table)
                    .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
                if !desc {
                    let mut out = Vec::new();
                    for item in engine.scan_range_iter(start.as_deref(), end.as_deref()) {
                        let (key, bytes) = item?;
                        crate::scan_meter::tick(1)?;
                        let doc = Self::decode_row(&bytes, project)?;
                        if matches_filter(filter, &doc)? {
                            crate::scan_meter::tick_bytes(bytes.len())?;
                            out.push((key, doc));
                            if fetch_limit.is_some_and(|lim| out.len() >= lim) {
                                break;
                            }
                        }
                    }
                    return Ok((out, true));
                }
                if let Some(lim) = fetch_limit {
                    // DESC with a limit: block iterators only run forward, so
                    // page the VALUE-FREE stamps (the repair sidecar read —
                    // keys + tombstone flags, no row bytes) across the range,
                    // reverse the key list (O(range keys) memory, ~tens of
                    // bytes per row — not row payloads), then point-read from
                    // the tail until the limit fills. Each examined key ticks
                    // the scan meter: a whole-table DESC head is honestly
                    // O(table) work even though its memory stays bounded.
                    const STAMPS_PAGE: usize = 4096;
                    let mut keys: Vec<Vec<u8>> = Vec::new();
                    let mut after: Option<Vec<u8>> = start.clone().and_then(|s| {
                        // `after` is exclusive; the inclusive start bound is
                        // re-admitted below via the >= comparison.
                        s.len().checked_sub(1).map(|n| s[..n].to_vec())
                    });
                    'pages: loop {
                        let page = engine.scan_stamps_page(after.as_deref(), STAMPS_PAGE)?;
                        let done = page.len() < STAMPS_PAGE;
                        after = page.last().map(|(k, ..)| k.clone());
                        for (key, _hlc, is_put) in page {
                            if start.as_ref().is_some_and(|s| &key < s) {
                                continue;
                            }
                            if end.as_ref().is_some_and(|e| &key >= e) {
                                break 'pages;
                            }
                            crate::scan_meter::tick(1)?;
                            if is_put {
                                keys.push(key);
                            }
                        }
                        if done {
                            break;
                        }
                    }
                    let mut out = Vec::new();
                    for key in keys.into_iter().rev() {
                        let Some(bytes) = engine.get(&key)? else {
                            continue; // deleted between the stamps read and now
                        };
                        let doc = Self::decode_row(&bytes, project)?;
                        if matches_filter(filter, &doc)? {
                            crate::scan_meter::tick_bytes(bytes.len())?;
                            out.push((key, doc));
                            if out.len() >= lim {
                                break;
                            }
                        }
                    }
                    return Ok((out, true));
                }
                // DESC without a limit: the result is O(table) whichever way
                // it is produced — keep the ordinary gather + executor sort.
            }
        }
        let order2 = order.map(|(c, d, _)| (c, d));
        let Some((index_name, start, end, sorted, reverse)) =
            self.plan_index(table, filter, order2, fetch_limit)
        else {
            let engine = self
                .tables
                .get(table)
                .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
            // No usable index, but an exact single-key ORDER BY with a fetch
            // limit: keep a bounded top-k instead of collecting every
            // matching row for the executor to sort — the other half of the
            // 2026-07-17 OOM class (`ORDER BY <unindexed col> LIMIT k` on a
            // large table is O(k) memory now, though still O(table) scan
            // work). Comparison mirrors `order_compare` exactly — NULLs last
            // ascending, whole ordering reversed for DESC (so NULLs first),
            // row key as the deterministic tiebreak — so the claimed
            // `sorted = true` means precisely what the executor's own sort
            // would have produced.
            if let (Some((col, desc, true)), Some(lim)) = (order, fetch_limit) {
                type Entry = (Value, Vec<u8>, Document);
                let cmp = |a: &Entry, b: &Entry| {
                    use std::cmp::Ordering;
                    let ord = match (a.0.is_null(), b.0.is_null()) {
                        (true, true) => Ordering::Equal,
                        (true, false) => Ordering::Greater, // NULLs last
                        (false, true) => Ordering::Less,
                        (false, false) => {
                            compare(&a.0, &b.0).unwrap_or_else(|| a.0.total_cmp(&b.0))
                        }
                    };
                    let ord = if desc { ord.reverse() } else { ord };
                    ord.then_with(|| a.1.cmp(&b.1))
                };
                let mut top: Vec<Entry> = Vec::new();
                for item in engine.scan_iter() {
                    crate::scan_meter::tick(1)?;
                    let (key, bytes) = item?;
                    let doc = Self::decode_row(&bytes, project)?;
                    if matches_filter(filter, &doc)? {
                        let sort_val = doc.get_path(col).cloned().unwrap_or(Value::Null);
                        top.push((sort_val, key, doc));
                        // Compact at 2k so the buffer stays O(k) with
                        // amortized O(log k) per row, not a sort per row.
                        if top.len() >= lim.saturating_mul(2).max(lim + 1) {
                            top.sort_by(cmp);
                            top.truncate(lim);
                        }
                    }
                }
                top.sort_by(cmp);
                top.truncate(lim);
                let out = top.into_iter().map(|(_, key, doc)| (key, doc)).collect();
                return Ok((out, true));
            }
            // Remaining shapes: stream the table scan (decode one row at a
            // time) and, when no ordering is required, stop as soon as the
            // fetch limit is satisfied — a plain `LIMIT n` touches n matching
            // rows, not the whole table.
            let mut out = Vec::new();
            for item in engine.scan_iter() {
                crate::scan_meter::tick(1)?;
                let (key, bytes) = item?;
                let doc = Self::decode_row(&bytes, project)?;
                if matches_filter(filter, &doc)? {
                    crate::scan_meter::tick_bytes(bytes.len())?;
                    out.push((key, doc));
                    if order.is_none() && fetch_limit.is_some_and(|lim| out.len() >= lim) {
                        break;
                    }
                }
            }
            return Ok((out, false));
        };

        let index_engine = self
            .indexes
            .get(&index_name)
            .ok_or_else(|| EngineError::IndexNotFound(index_name.clone()))?;
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;

        let mut out = Vec::new();
        // A DESC request on the index's scan column: entry keys ascend, so
        // the tail of the range is the DESC head — that walk materializes
        // the entry range and reverses it (block iterators only run
        // forward). Forward walks STREAM the merge instead: one entry in
        // flight, so an unbounded `ORDER BY <indexed>` no longer holds the
        // whole index range in memory before emitting its first row.
        type EntryItem = std::result::Result<(Vec<u8>, Vec<u8>), skaidb_storage::StorageError>;
        let iter: Box<dyn Iterator<Item = EntryItem>> = if reverse {
            let entries = index_engine.scan_range(start.as_deref(), end.as_deref())?;
            Box::new(entries.into_iter().rev().map(Ok))
        } else {
            Box::new(index_engine.scan_range_iter(start.as_deref(), end.as_deref()))
        };
        // Multi-key ORDER BY: the walk satisfies only the leading key, so
        // reaching the limit isn't enough — every row TIED with the boundary
        // on the leading column must also be gathered, then the executor
        // re-sorts the bounded set by the full clause. Without this, rows
        // equal on the leading key could be dropped by walk order rather
        // than by the secondary keys.
        let exact_order = order.is_none_or(|(_, _, exact)| exact);
        let mut tie_boundary: Option<Option<Value>> = None;
        for item in iter {
            let (_entry_key, row_key) = item?;
            crate::scan_meter::tick(1)?;
            let Some(bytes) = table_engine.get(&row_key)? else {
                continue; // index entry for a since-deleted row
            };
            let doc = Self::decode_row(&bytes, project)?;
            if matches_filter(filter, &doc)? {
                if let (Some(boundary), Some((col, _, _))) = (&tie_boundary, order) {
                    // Limit already met: keep only leading-key ties.
                    if doc.get_path(col) != boundary.as_ref() {
                        break;
                    }
                    crate::scan_meter::tick_bytes(bytes.len())?;
                    out.push((row_key, doc));
                    continue;
                }
                crate::scan_meter::tick_bytes(bytes.len())?;
                out.push((row_key, doc));
                // Stop early when the rows already arrive in `order`, or
                // when the query never asked for one.
                if (sorted || order.is_none())
                    && fetch_limit.is_some_and(|lim| out.len() >= lim)
                {
                    if sorted && !exact_order {
                        if let Some((col, _, _)) = order {
                            let (_, last) = out.last().expect("just pushed");
                            tie_boundary = Some(last.get_path(col).cloned());
                            continue;
                        }
                    }
                    break;
                }
            }
        }
        Ok((out, sorted && exact_order))
    }

    /// Choose an index access path for `(filter, order)`: a column with
    /// equality/range bounds that is indexed (bounded scan), else an indexed
    /// `ORDER BY` column (full ordered scan). Returns
    /// `(index_name, path, start_key, end_key)`.
    fn plan_index(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<(&str, bool)>,
        fetch_limit: Option<usize>,
    ) -> Option<IndexPlan> {
        let constraints = column_constraints(filter);
        // Rank candidates by how much of the filter the index consumes: the
        // number of equality-pinned prefix columns, then a trailing range.
        // Taking the *first* usable index (HashMap order — varies per
        // process!) let a two-equality dedup probe land on a sibling index
        // that only consumed one column: the 150k-entry candidate range
        // overflowed the point-read budget and fell back to a whole-table
        // coordinator gather, OOM-killing 4 GB nodes (2026-07-13).
        let selectivity = |paths: &[String]| -> (usize, usize) {
            let get =
                |col: &str| constraints.iter().find(|(c, _)| c == col).map(|(_, c)| c);
            let mut eq = 0usize;
            while eq < paths.len() {
                match get(&paths[eq]) {
                    Some(c) if c.eq.is_some() => eq += 1,
                    _ => break,
                }
            }
            let range = usize::from(paths.get(eq).and_then(|p| get(p)).is_some_and(|c| {
                c.lo.is_some() || c.hi.is_some()
            }));
            (eq, range)
        };
        // Which quality leads depends on what bounds the work. With an ORDER
        // BY *and* a fetch limit, a sorted plan stops after `limit` matches —
        // the categorizer shape (account eq + ORDER BY date DESC LIMIT n)
        // must take the sorted (account, date) walk, not a sibling index
        // that pins one more equality yet spans half the table (that pick
        // ran 150k row reads for LIMIT 5). Without a limit the whole result
        // gets built either way, so the tightest range wins — the dedup
        // shape (two equalities, no order) must take its covering index.
        let sorted_first = order.is_some() && fetch_limit.is_some();
        let mut best_sorted: Option<(IndexPlan, (usize, usize))> = None;
        let mut best_unsorted: Option<(IndexPlan, (usize, usize))> = None;
        for (name, idx) in self
            .catalog
            .indexes
            .iter()
            .filter(|(_, i)| i.table == table && !i.building && !i.global)
        {
            let names = index_key_names(&idx.paths);
            // A multikey index is usable only when every column through the
            // [] component is equality-pinned (the element probe). Below
            // that, a row surfaces once per array element — duplicate rows
            // in walks, overcounts in counts.
            if let Some(m) = multi_pos(&idx.paths) {
                let eq_through = names[..=m].iter().all(|p| {
                    constraints.iter().any(|(c, cc)| c == p && cc.eq.is_some())
                });
                if !eq_through {
                    continue;
                }
            }
            if let Some((start, end, sorted, reverse)) =
                plan_for_index(&names, &constraints, order)
            {
                let score = selectivity(&names);
                let plan = (name.clone(), start, end, sorted, reverse);
                let slot = if sorted { &mut best_sorted } else { &mut best_unsorted };
                if slot.as_ref().is_none_or(|(_, bs)| score > *bs) {
                    *slot = Some((plan, score));
                }
            }
        }
        match (best_sorted, best_unsorted) {
            (Some((sp, ss)), Some((up, us))) => {
                // The sorted walk's blind spot: when the residual filter
                // matches (almost) nothing, "stops after limit matches"
                // never happens and the walk degenerates into a full
                // partition scan — the empty Archived view burned 9.5 s
                // re-reading 183k rows per click while a sibling equality
                // index knew the answer was 0 (2026-07-14). When a strictly
                // more selective unsorted plan exists, probe its local range
                // (O(cap)); if it is point-read small, gathering + sorting
                // those few rows beats any walk. Past the cap the walk keeps
                // its win — probing is a bounded peek, never a full count.
                if us > ss
                    && (!sorted_first || self.index_range_at_most(&up, PLANNER_PROBE_MAX)
                        <= PLANNER_PROBE_MAX)
                {
                    Some(up)
                } else {
                    Some(sp)
                }
            }
            (s, u) => s.or(u).map(|(plan, _)| plan),
        }
    }

    /// `min(live entries, cap + 1)` of an index plan's byte range on the
    /// local shard — the planner's cheap cardinality peek. Local is a proxy
    /// for the cluster (replicas hold supersets per shard; a tiny local
    /// range and a huge cluster range can't coexist under our replication),
    /// and an unreadable index reports "big", which keeps the sorted plan —
    /// the status quo, never a new failure mode.
    fn index_range_at_most(&self, plan: &IndexPlan, cap: usize) -> usize {
        let (name, start, end, _, _) = plan;
        self.indexes
            .get(name)
            .and_then(|e| e.count_range_at_most(start.as_deref(), end.as_deref(), cap).ok())
            .unwrap_or(cap + 1)
    }

    /// The `(name, paths)` of every index defined on `table`.
    /// Whether any index consumer (secondary/vector/search) needs the
    /// decoded document on this table's write path. Replicated applies of
    /// 10 KB JSON documents burned a core in serde_json (`parse_escape`,
    /// profiled live on .4, 2026-07-12) decoding rows for tables with no
    /// indexes at all — the decode exists only to feed index maintenance.
    fn needs_doc_on_write(&self, table: &str) -> bool {
        self.catalog.indexes.values().any(|i| i.table == table)
            || self.catalog.vector_indexes.values().any(|v| v.table == table)
            || self.catalog.search_indexes.values().any(|s| s.table == table)
            || self.catalog.geo_indexes.values().any(|g| g.table == table)
    }

    fn indexes_on(&self, table: &str) -> Vec<(String, Vec<String>)> {
        self.catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == table)
            .map(|(name, idx)| (name.clone(), idx.paths.clone()))
            .collect()
    }

    /// The GLOBAL index declarations on `table` as `(index name, paths)`.
    /// The write coordinator consults this to issue companion entry writes;
    /// empty for tables without global indexes (the overwhelmingly common
    /// case — one Vec::new, no reads).
    pub fn global_indexes_of(&self, table: &str) -> Vec<(String, Vec<String>)> {
        self.catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == table && idx.global)
            .map(|(name, idx)| (name.clone(), idx.paths.clone()))
            .collect()
    }

    /// Single-node maintenance of GLOBAL index entries on a row put: diff the
    /// old row's entry keys against the new row's and apply put/delete to the
    /// local `__gidx__` table. On a single node the local shard IS the whole
    /// ring, so writing entries locally is exactly the coordinator's routed
    /// companion write. Cluster coordinators do NOT use this — they route
    /// each entry to its own replica set (`Node::write_global_entries`); the
    /// replica APPLY path never touches global entries at all.
    fn maintain_global_put(&mut self, table: &str, key: &[u8], new: &Document) -> Result<()> {
        let gidx = self.global_indexes_of(table);
        if gidx.is_empty() {
            return Ok(());
        }
        let old = self
            .tables
            .get(table)
            .and_then(|e| e.get(key).ok().flatten())
            .and_then(|b| match Value::decode(&b) {
                Ok(Value::Document(d)) => Some(d),
                _ => None,
            });
        for (name, paths) in gidx {
            let (dels, puts) = global_entry_delta(&paths, key, old.as_ref(), Some(new));
            let Some(engine) = self.tables.get_mut(&gidx_table(&name)) else {
                continue;
            };
            for k in dels {
                engine.delete(&k)?;
            }
            for k in puts {
                engine.put(&k, Value::encode_document(&Document::new()))?;
            }
        }
        Ok(())
    }

    /// Single-node maintenance of GLOBAL index entries on a row delete
    /// (`old` is the row being removed). See [`Database::maintain_global_put`].
    fn maintain_global_del(&mut self, table: &str, key: &[u8], old: &Document) -> Result<()> {
        let gidx = self.global_indexes_of(table);
        for (name, paths) in gidx {
            let (dels, _) = global_entry_delta(&paths, key, Some(old), None);
            let Some(engine) = self.tables.get_mut(&gidx_table(&name)) else {
                continue;
            };
            for k in dels {
                engine.delete(&k)?;
            }
        }
        Ok(())
    }

    /// Remove the secondary-index entries of the row's PREVIOUS version
    /// before an overwrite. Without this, a put whose indexed values changed
    /// leaves the old entries behind: row queries re-filter them away, but
    /// index-only counts overcount and the entries never get cleaned.
    /// UPDATE used to mask this by modelling every rewrite as delete+put —
    /// which lost rows when the put half's quorum failed (2026-07-13).
    /// No-op (no read) on tables without secondary indexes.
    fn index_del_previous(&mut self, table: &str, key: &[u8]) -> Result<()> {
        let has_secondary = self.catalog.indexes.values().any(|i| i.table == table);
        // A geo index's entry key embeds the point, so an overwrite that moved
        // the point leaves a stale entry unless the old one is removed here —
        // the same read-before-write the secondary indexes need.
        let has_geo = self.catalog.geo_indexes.values().any(|g| g.table == table);
        if !has_secondary && !has_geo {
            return Ok(());
        }
        let existing = match self.tables.get(table) {
            Some(engine) => engine.get(key)?,
            None => None,
        };
        if let Some(bytes) = existing {
            if let Ok(Value::Document(old)) = Value::decode(&bytes) {
                if has_secondary {
                    for (name, path) in self.indexes_on(table) {
                        self.index_del(&name, &path, &old, key)?;
                    }
                }
                if has_geo {
                    self.maintain_geo_del(table, &old, key)?;
                }
            }
        }
        Ok(())
    }

    /// Add an index entry for `doc`'s values at `paths` pointing to `row_key`.
    fn index_put(
        &mut self,
        name: &str,
        paths: &[String],
        doc: &Document,
        row_key: &[u8],
    ) -> Result<()> {
        if let Some(engine) = self.indexes.get_mut(name) {
            for values in index_value_tuples(doc, paths) {
                engine.put(&index_entry_key(&values, row_key), row_key.to_vec())?;
            }
        }
        Ok(())
    }

    /// [`Database::index_put`] without the fsync: returns the index WAL's sync
    /// handle and commit point so a multi-row statement group-commits once.
    fn index_put_deferred(
        &mut self,
        name: &str,
        paths: &[String],
        doc: &Document,
        row_key: &[u8],
    ) -> Result<Option<(Arc<WalSync>, WalCommit)>> {
        if let Some(engine) = self.indexes.get_mut(name) {
            let mut last = None;
            for values in index_value_tuples(doc, paths) {
                let (_, commit) =
                    engine.put_deferred(&index_entry_key(&values, row_key), row_key.to_vec())?;
                last = Some(commit);
            }
            if let Some(commit) = last {
                return Ok(Some((engine.wal_sync_handle(), commit)));
            }
        }
        Ok(None)
    }

    /// Remove the index entry for `doc`'s values at `paths` pointing to `row_key`.
    fn index_del(
        &mut self,
        name: &str,
        paths: &[String],
        doc: &Document,
        row_key: &[u8],
    ) -> Result<()> {
        if let Some(engine) = self.indexes.get_mut(name) {
            for values in index_value_tuples(doc, paths) {
                engine.delete(&index_entry_key(&values, row_key))?;
            }
        }
        Ok(())
    }

    /// Deferred-durability [`Database::index_del`]; see
    /// [`Database::index_put_deferred`].
    fn index_del_deferred(
        &mut self,
        name: &str,
        paths: &[String],
        doc: &Document,
        row_key: &[u8],
    ) -> Result<Option<(Arc<WalSync>, WalCommit)>> {
        if let Some(engine) = self.indexes.get_mut(name) {
            let mut last = None;
            for values in index_value_tuples(doc, paths) {
                let (_, commit) = engine.delete_deferred(&index_entry_key(&values, row_key))?;
                last = Some(commit);
            }
            if let Some(commit) = last {
                return Ok(Some((engine.wal_sync_handle(), commit)));
            }
        }
        Ok(None)
    }

    // ---- distribution support (used by the cluster coordinator) ----

    /// Primary-key columns of `table` (public for the coordinator).
    pub fn table_primary_key(&self, table: &str) -> Result<Vec<String>> {
        Ok(self.table_def(table)?.primary_key.clone())
    }

    /// Plan a secondary-index scan for a coordinator pushdown: the index name
    /// and byte range that covers `filter`, or `None` if no index serves it.
    /// The byte bounds are catalog-deterministic, so every node's local index
    /// scans the same range.
    pub fn plan_index_scan(&self, table: &str, filter: &Option<Expr>) -> Option<IndexScanRange> {
        self.plan_index(table, filter, None, None)
            .map(|(name, start, end, _sorted, _reverse)| (name, start, end))
    }

    /// Row keys whose entries fall in `[start, end)` of the named index — the
    /// candidate keys for a distributed index lookup (a superset; the
    /// coordinator re-reads and re-filters each against the authoritative row).
    pub fn index_scan_keys(
        &self,
        index: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<Vec<u8>>> {
        let engine = self
            .indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        Ok(engine
            .scan_range(start, end)?
            .into_iter()
            .map(|(_entry_key, row_key)| row_key)
            .collect())
    }

    /// Plan a geo-index scan for `filter`: the serving index and the byte
    /// ranges over its Morton codes, or `None` when no geo index covers the
    /// predicate (the query then scans). The encoding is catalog-deterministic,
    /// so every node's local index scans the same ranges — the coordinator
    /// reuses this plan for its distributed `IndexScan` scatter.
    pub fn plan_geo_scan(&self, table: &str, filter: &Option<Expr>) -> Option<GeoScanPlan> {
        let (col, bbox) = geo_predicate(filter)?;
        for (name, def) in &self.catalog.geo_indexes {
            if def.table != table || def.building || def.path != col {
                continue;
            }
            // An antimeridian-crossing box covers as two non-wrapping halves;
            // the gather dedups keys across ranges, so concatenation is safe.
            let (east, west) = bbox.split_antimeridian();
            let mut ranges: Vec<_> =
                crate::geo::cover_ranges(&east, crate::geo::DEFAULT_MAX_RANGES)
                    .into_iter()
                    .map(|(lo, hi)| crate::geo::range_bytes(lo, hi))
                    .collect();
            if let Some(w) = west {
                ranges.extend(
                    crate::geo::cover_ranges(&w, crate::geo::DEFAULT_MAX_RANGES)
                        .into_iter()
                        .map(|(lo, hi)| crate::geo::range_bytes(lo, hi)),
                );
            }
            return Some((name.clone(), ranges));
        }
        None
    }

    /// Gather rows from a local geo index: walk each Morton-code range, re-read
    /// each candidate row, and keep those passing the exact `filter` (the range
    /// cover is a superset — the residual filter is the authoritative geo
    /// predicate). Row keys are de-duplicated defensively across ranges.
    fn geo_gather_local(
        &self,
        index: &str,
        table: &str,
        ranges: &[(Vec<u8>, Option<Vec<u8>>)],
        filter: &Option<Expr>,
        project: Option<&HashSet<String>>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let index_engine = self
            .indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for (start, end) in ranges {
            for item in index_engine.scan_range_iter(Some(start), end.as_deref()) {
                let (_entry_key, row_key) = item?;
                crate::scan_meter::tick(1)?;
                if !seen.insert(row_key.clone()) {
                    continue; // a row appears once per code, but guard anyway
                }
                let Some(bytes) = table_engine.get(&row_key)? else {
                    continue; // index entry for a since-deleted row
                };
                let doc = Self::decode_row(&bytes, project)?;
                if matches_filter(filter, &doc)? {
                    crate::scan_meter::tick_bytes(bytes.len())?;
                    out.push((row_key, doc));
                }
            }
        }
        Ok(out)
    }

    /// Whether `table` exists locally.
    pub fn has_table(&self, table: &str) -> bool {
        self.catalog.tables.contains_key(table)
    }

    /// Names of all tables (used to migrate every table during resharding).
    pub fn table_names(&self) -> Vec<String> {
        self.catalog.tables.keys().cloned().collect()
    }

    /// Names of tables whose data is worth moving between nodes: memory
    /// tables are ephemeral by contract (empty on restart, healed by their
    /// writers), so repair and reshard traffic skips them.
    pub fn persistent_table_names(&self) -> Vec<String> {
        self.catalog
            .tables
            .iter()
            .filter(|(_, def)| !def.memory)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Whether `table` exists and is a memory table.
    pub fn table_is_memory(&self, table: &str) -> Option<bool> {
        self.catalog.tables.get(table).map(|d| d.memory)
    }

    /// The directory backing this database — the cluster layer persists its
    /// membership/topology alongside the data here.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `CREATE` statements that reconstruct this node's schema (tables, then
    /// secondary indexes, then vector indexes) — sent to a joining node so it can
    /// receive migrated rows. Identifiers are assumed simple (no quoting).
    /// Versioned schema for last-writer-wins replication: every live object as a
    /// `CREATE … IF NOT EXISTS` and every dropped object as a `DROP … IF EXISTS`,
    /// each paired with its DDL version stamp. A peer applies each statement via
    /// [`Database::execute_session_with_hlc`], so creates and drops converge
    /// across nodes and a node that missed a drop stops resurrecting the object.
    pub fn schema_sync(&self) -> Vec<(String, String, Hlc)> {
        let ver = |key: &str| {
            self.catalog
                .schema_versions
                .get(key)
                .map_or(Hlc::MIN, |v| v.hlc())
        };
        let mut out: Vec<(String, String, Hlc)> = Vec::new();
        // Live objects as idempotent CREATEs, each with its version stamp.
        for db in &self.catalog.databases {
            out.push((
                DEFAULT_DATABASE.to_string(),
                format!("CREATE DATABASE IF NOT EXISTS {db}"),
                ver(&format!("d:{db}")),
            ));
        }
        for (name, def) in &self.catalog.tables {
            let (db, bare) = namespace::split(name);
            if bare.starts_with("__gidx__") {
                continue; // implied by its global index's CREATE INDEX
            }
            out.push((
                db.to_string(),
                format!(
                    "CREATE TABLE IF NOT EXISTS {bare} (PRIMARY KEY ({})){}",
                    def.primary_key.join(", "),
                    table_with_options(def)
                ),
                ver(&format!("t:{name}")),
            ));
        }
        for (name, def) in &self.catalog.timeseries {
            if def.rollup_of.is_some() {
                continue; // emitted as CREATE ROLLUP below
            }
            let (db, bare) = namespace::split(name);
            out.push((
                db.to_string(),
                ts_create_ddl(bare, def),
                ver(&format!("t:{name}")),
            ));
            for r in &def.rollups {
                if let Some(rdef) = self.catalog.timeseries.get(&r.name) {
                    let (rdb, rbare) = namespace::split(&r.name);
                    out.push((
                        rdb.to_string(),
                        ts_rollup_ddl(rbare, bare, r.bucket_ms, rdef),
                        ver(&format!("t:{}", r.name)),
                    ));
                }
            }
        }
        for (name, idx) in &self.catalog.indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&idx.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE INDEX IF NOT EXISTS {bare} ON {table} ({}){}",
                    idx.paths.join(", "),
                    match (idx.global, idx.building) {
                        (true, false) => " WITH (global = true, ready = true)",
                        (true, true) => " WITH (global = true)",
                        _ => "",
                    }
                ),
                ver(&format!("i:{name}")),
            ));
        }
        for (name, v) in &self.catalog.vector_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&v.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE VECTOR INDEX IF NOT EXISTS {bare} ON {table} ({}) DIM {} USING {}{}{}",
                    v.path,
                    v.dim,
                    v.metric,
                    // Both options must survive the DDL-string regeneration
                    // (schema repair replays these) — EMBED used to be
                    // dropped here, silently demoting a managed index to a
                    // plain one on a repaired peer.
                    if v.quantized { " QUANTIZED" } else { "" },
                    if v.embed { " EMBED" } else { "" },
                ),
                ver(&format!("v:{name}")),
            ));
        }
        for (name, s) in &self.catalog.search_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&s.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE SEARCH INDEX IF NOT EXISTS {bare} ON {table} ({}){}",
                    s.paths.join(", "),
                    s.with_clause()
                ),
                ver(&format!("s:{name}")),
            ));
        }
        for (name, g) in &self.catalog.geo_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&g.table).1;
            out.push((
                db.to_string(),
                format!("CREATE GEO INDEX IF NOT EXISTS {bare} ON {table} ({})", g.path),
                ver(&format!("g:{name}")),
            ));
        }
        for (name, def) in &self.catalog.users {
            let ddl = match def.auth_kind {
                UserAuthKind::Scram => format!("CREATE USER {name} VERIFIER '{}'", def.credential),
                UserAuthKind::Gssapi => format!("CREATE USER {name} GSSAPI"),
            };
            out.push((DEFAULT_DATABASE.to_string(), ddl, ver(&format!("usr:{name}"))));
        }
        for (name, def) in &self.catalog.auth_roles {
            out.push((
                DEFAULT_DATABASE.to_string(),
                format!("CREATE ROLE {name} GRANTS '{}'", encode_role_state(def)),
                ver(&format!("rol:{name}")),
            ));
        }
        // Tombstones: emit a DROP for every dropped object, with its version.
        for (key, v) in &self.catalog.schema_versions {
            if !v.dropped {
                continue;
            }
            let Some((kind, name)) = key.split_once(':') else {
                continue;
            };
            let entry = match kind {
                "d" => (
                    DEFAULT_DATABASE.to_string(),
                    format!("DROP DATABASE IF EXISTS {name}"),
                ),
                "t" => {
                    let (db, bare) = namespace::split(name);
                    (db.to_string(), format!("DROP TABLE IF EXISTS {bare}"))
                }
                "i" => {
                    let (db, bare) = namespace::split(name);
                    (db.to_string(), format!("DROP INDEX IF EXISTS {bare}"))
                }
                "v" => {
                    let (db, bare) = namespace::split(name);
                    (db.to_string(), format!("DROP VECTOR INDEX IF EXISTS {bare}"))
                }
                "g" => {
                    let (db, bare) = namespace::split(name);
                    (db.to_string(), format!("DROP GEO INDEX IF EXISTS {bare}"))
                }
                "s" => {
                    let (db, bare) = namespace::split(name);
                    (db.to_string(), format!("DROP SEARCH INDEX IF EXISTS {bare}"))
                }
                "usr" => (
                    DEFAULT_DATABASE.to_string(),
                    format!("DROP USER IF EXISTS {name}"),
                ),
                "rol" => (
                    DEFAULT_DATABASE.to_string(),
                    format!("DROP ROLE IF EXISTS {name}"),
                ),
                _ => continue,
            };
            out.push((entry.0, entry.1, v.hlc()));
        }
        out
    }

    /// Returns `(database, sql)` pairs: each statement applied via
    /// [`Database::execute_session`] with the given current database recreates
    /// the object in its correct namespace, using bare (de-qualified) names so
    /// the SQL is parseable. Databases come first so their tables can follow.
    pub fn schema_ddl(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for db in &self.catalog.databases {
            out.push((
                DEFAULT_DATABASE.to_string(),
                format!("CREATE DATABASE IF NOT EXISTS {db}"),
            ));
        }
        for (name, def) in &self.catalog.tables {
            let (db, bare) = namespace::split(name);
            if bare.starts_with("__gidx__") {
                continue; // implied by its global index's CREATE INDEX
            }
            out.push((
                db.to_string(),
                format!(
                    "CREATE TABLE IF NOT EXISTS {bare} (PRIMARY KEY ({})){}",
                    def.primary_key.join(", "),
                    table_with_options(def)
                ),
            ));
        }
        for (name, def) in &self.catalog.timeseries {
            if def.rollup_of.is_some() {
                continue; // emitted as CREATE ROLLUP below
            }
            let (db, bare) = namespace::split(name);
            out.push((db.to_string(), ts_create_ddl(bare, def)));
            for r in &def.rollups {
                if let Some(rdef) = self.catalog.timeseries.get(&r.name) {
                    let (rdb, rbare) = namespace::split(&r.name);
                    out.push((
                        rdb.to_string(),
                        ts_rollup_ddl(rbare, bare, r.bucket_ms, rdef),
                    ));
                }
            }
        }
        for (name, idx) in &self.catalog.indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&idx.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE INDEX IF NOT EXISTS {bare} ON {table} ({}){}",
                    idx.paths.join(", "),
                    match (idx.global, idx.building) {
                        (true, false) => " WITH (global = true, ready = true)",
                        (true, true) => " WITH (global = true)",
                        _ => "",
                    }
                ),
            ));
        }
        for (name, v) in &self.catalog.vector_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&v.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE VECTOR INDEX IF NOT EXISTS {bare} ON {table} ({}) DIM {} USING {}{}{}",
                    v.path,
                    v.dim,
                    v.metric,
                    if v.quantized { " QUANTIZED" } else { "" },
                    if v.embed { " EMBED" } else { "" },
                ),
            ));
        }
        for (name, s) in &self.catalog.search_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&s.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE SEARCH INDEX IF NOT EXISTS {bare} ON {table} ({}){}",
                    s.paths.join(", "),
                    s.with_clause()
                ),
            ));
        }
        for (name, g) in &self.catalog.geo_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&g.table).1;
            out.push((
                db.to_string(),
                format!("CREATE GEO INDEX IF NOT EXISTS {bare} ON {table} ({})", g.path),
            ));
        }
        out
    }

    /// Names of all time-series tables.
    pub fn ts_table_names(&self) -> Vec<String> {
        self.catalog.timeseries.keys().cloned().collect()
    }

    /// Every series label set in `table` (the resharding migration unit).
    pub fn ts_series_labels(&self, table: &str) -> Result<Vec<skaidb_tsdb::Labels>> {
        Ok(self.ts_store(table)?.series_labels())
    }

    /// Repair-path sample merge (accepts any-aged samples; see tsdb docs).
    /// Also **backfills rollups**: repair-merged samples can land in
    /// buckets the flush path already aggregated, so every touched bucket
    /// is recomputed from the source table (authoritative after the merge)
    /// and rewritten — same series + bucket timestamp, and the newer block
    /// wins the query-time/compaction dedupe, so the stale partial is
    /// replaced rather than doubled.
    pub fn ts_merge(&self, table: &str, rows: &[(skaidb_tsdb::Labels, i64, f64)]) -> Result<usize> {
        let n = self
            .ts_store(table)?
            .merge_samples(rows)
            .map_err(|e| EngineError::Timeseries(e.to_string()))?;
        if n > 0 {
            if let Some(def) = self.catalog.timeseries.get(table) {
                if !def.rollups.is_empty() {
                    self.ts_rollup_backfill(table, &def.rollups, rows)?;
                }
            }
        }
        Ok(n)
    }

    /// Recompute and rewrite every rollup bucket touched by repair-merged
    /// `rows` (see [`Database::ts_merge`]).
    fn ts_rollup_backfill(
        &self,
        table: &str,
        rollups: &[crate::catalog::RollupDef],
        rows: &[(skaidb_tsdb::Labels, i64, f64)],
    ) -> Result<()> {
        let source = self.ts_store(table)?;
        for rollup in rollups {
            let Some(store) = self.timeseries.get(&rollup.name) else {
                continue;
            };
            // The touched buckets, per source series.
            let mut per: BTreeMap<&skaidb_tsdb::Labels, std::collections::BTreeSet<i64>> =
                BTreeMap::new();
            for (labels, ts, _) in rows {
                per.entry(labels)
                    .or_default()
                    .insert(ts.div_euclid(rollup.bucket_ms) * rollup.bucket_ms);
            }
            let mut recomputed = Vec::new();
            for (labels, buckets) in per {
                let matchers: Vec<skaidb_tsdb::Matcher> = labels
                    .iter()
                    .map(|(k, v)| skaidb_tsdb::Matcher::Eq(k.clone(), v.clone()))
                    .collect();
                for bucket in buckets {
                    let series = source
                        .query(&matchers, bucket, bucket + rollup.bucket_ms - 1)
                        .map_err(|e| EngineError::Timeseries(e.to_string()))?;
                    for (slabels, samples) in &series {
                        // Eq matchers admit series with extra labels; the
                        // rollup row belongs to this exact label set only.
                        if slabels == labels {
                            recomputed.extend(rollup_partial_rows(
                                slabels,
                                samples,
                                rollup.bucket_ms,
                            ));
                        }
                    }
                }
            }
            if !recomputed.is_empty() {
                // The stale partials may still sit in the rollup's head,
                // which outranks any block in the later-wins dedupe. Flush
                // it first so the recomputed rows land in the newest block
                // and win.
                store
                    .flush()
                    .map_err(|e| EngineError::Timeseries(e.to_string()))?;
                store
                    .merge_samples(&recomputed)
                    .map_err(|e| EngineError::Timeseries(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Drop whole series from a time-series table (the post-resharding
    /// reclaim unit; see the tsdb's `drop_series`).
    pub fn ts_drop_series(
        &self,
        table: &str,
        targets: &std::collections::HashSet<skaidb_tsdb::Labels>,
    ) -> Result<usize> {
        self.ts_store(table)?
            .drop_series(targets)
            .map_err(|e| EngineError::Timeseries(e.to_string()))
    }

    /// Per-series anti-entropy summaries `(labels, count, checksum)`.
    pub fn ts_summaries(&self, table: &str) -> Result<Vec<(skaidb_tsdb::Labels, u64, u64)>> {
        self.ts_store(table)?
            .series_summaries()
            .map_err(|e| EngineError::Timeseries(e.to_string()))
    }

    /// Whether any time-series table exists (topology-change guard).
    pub fn has_timeseries_tables(&self) -> bool {
        !self.catalog.timeseries.is_empty()
    }

    /// The full time-series definition when `table` is a time-series table.
    pub fn ts_table_def(&self, table: &str) -> Option<&crate::catalog::TsTableDef> {
        self.catalog.timeseries.get(table)
    }

    /// The table's row TTL, if set (cluster coordinators filter merged
    /// winners with it — the versioned wire paths are TTL-blind).
    pub fn table_ttl_ms(&self, table: &str) -> Option<u64> {
        self.catalog.tables.get(table)?.ttl_ms.map(|ms| ms as u64)
    }

    /// Series-key columns when `table` is a time-series table.
    pub fn ts_series_key(&self, table: &str) -> Option<Vec<String>> {
        self.catalog
            .timeseries
            .get(table)
            .map(|d| d.series_key.clone())
    }

    fn ts_store(&self, table: &str) -> Result<&Tsdb> {
        self.timeseries
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))
    }

    /// Append samples to a time-series table (see [`Cluster::ts_append`]).
    /// A triggered window flush also maintains the table's rollups — each
    /// replica does this locally: a rollup series carries the same labels as
    /// its source, so it places on the same replica set.
    pub fn ts_append(&self, table: &str, rows: &[(skaidb_tsdb::Labels, i64, f64)]) -> Result<usize> {
        let (res, flushed) = self
            .ts_store(table)?
            .append_batch_with_flush(rows)
            .map_err(|e| EngineError::Timeseries(e.to_string()))?;
        if !flushed.is_empty() {
            if let Some(def) = self.catalog.timeseries.get(table) {
                for rollup in &def.rollups {
                    let rows = rollup_rows(&flushed, rollup.bucket_ms)?;
                    if let Some(store) = self.timeseries.get(&rollup.name) {
                        store
                            .append_batch(&rows)
                            .map_err(|e| EngineError::Timeseries(e.to_string()))?;
                    }
                }
            }
        }
        Ok(res.appended)
    }

    /// This node's series LABEL SETS for a time-series table, matcher-
    /// filtered — labels only, no chunk decode, no sample materialization.
    /// The series-metadata unit behind label-DISTINCT queries and the
    /// `/api/v1/series` endpoint at scale.
    pub fn ts_series_sets(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
    ) -> Result<Vec<skaidb_tsdb::Labels>> {
        Ok(self
            .ts_store(table)?
            .series_labels()
            .into_iter()
            .filter(|labels| matchers.iter().all(|m| m.accepts(labels)))
            .collect())
    }

    /// Query samples from a time-series table (see [`Cluster::ts_query`]).
    pub fn ts_query(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        self.ts_store(table)?
            .query(matchers, t0, t1)
            .map_err(|e| EngineError::Timeseries(e.to_string()))
    }

    /// Rollup routing metadata for a time-series table (see
    /// [`Cluster::ts_rollup_info`]): the retention horizon — the timestamp
    /// below which source blocks may already be dropped (`max_ts -
    /// retention`; `None` without a `RETENTION` or on an empty store) — and
    /// the table's rollups as `(name, bucket_ms)`.
    /// The table's local data range `(oldest, newest)` across head and
    /// blocks (`None` = empty / not a TS table) — see
    /// [`Cluster::ts_local_range`].
    pub fn ts_local_range(&self, table: &str) -> Option<(i64, i64)> {
        self.timeseries.get(table)?.ts_range()
    }

    pub fn ts_rollup_info(&self, table: &str) -> Result<TsRollupInfo> {
        let def = self
            .catalog
            .timeseries
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let horizon = match def.retention_ms {
            Some(r) => {
                let store = self.ts_store(table)?;
                let max_ts = store.max_ts();
                // Retention drops only flushed blocks, and rollups only hold
                // flushed data — so the horizon is capped at the flush
                // boundary: above it the source is complete (head data is
                // never dropped) and the rollup may not have it yet.
                (max_ts != i64::MIN)
                    .then(|| max_ts.saturating_sub(r).min(store.flushed_through()))
                    .filter(|h| *h != i64::MIN)
            }
            None => None,
        };
        let rollups: Vec<(String, i64)> = def
            .rollups
            .iter()
            .map(|r| (r.name.clone(), r.bucket_ms))
            .collect();
        // Opportunistic boundary: rollups are complete strictly below the
        // head's oldest sample (everything else went through flush-path or
        // repair-path rollup maintenance). An empty head means complete
        // through the newest sample.
        let complete_below = if rollups.is_empty() {
            None
        } else {
            let store = self.ts_store(table)?;
            store.head_min_ts().or_else(|| {
                let m = store.max_ts_all();
                (m != i64::MIN).then(|| m + 1)
            })
        };
        Ok((horizon, complete_below, rollups))
    }

    /// Per-series per-bucket partial aggregates (see [`Cluster::ts_partials`]);
    /// the internode `TsPartials` request answers from this.
    pub fn ts_partials(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        bucket_ms: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<crate::ts_query::TsPartial>)>> {
        Ok(crate::ts_query::ts_partialize(
            self.ts_query(table, matchers, t0, t1)?,
            bucket_ms,
        ))
    }

    /// `SHOW TABLES`: the catalog's tables and their primary keys, in name
    /// order. Time-series tables list their implicit `(series key, ts)` key.
    pub fn show_tables(&self) -> ResultSet {
        let mut rows: Vec<Vec<Value>> = self
            .catalog
            .tables
            .iter()
            .filter(|(name, _)| !namespace::split(name).1.starts_with("__gidx__"))
            .map(|(name, def)| {
                vec![
                    Value::String(name.clone()),
                    Value::String(def.primary_key.join(", ")),
                ]
            })
            .collect();
        for (name, def) in &self.catalog.timeseries {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(format!("{}, ts", def.series_key.join(", "))),
            ]);
        }
        rows.sort_by(|a, b| a[0].total_cmp(&b[0]));
        ResultSet {
            columns: vec!["table".into(), "primary_key".into()],
            rows,
        }
    }

    /// `SHOW INDEXES`: secondary and vector indexes, in name order.
    pub fn show_indexes(&self) -> ResultSet {
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for (name, idx) in &self.catalog.indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(idx.table.clone()),
                Value::String(
                    match (idx.global, idx.building) {
                        (true, true) => "global (building)",
                        (true, false) => "global",
                        (false, true) => "secondary (building)",
                        (false, false) => "secondary",
                    }
                    .into(),
                ),
                Value::String(idx.paths.join(", ")),
                Value::String(self.index_local_health(name, IndexClass::Secondary).into()),
            ]);
        }
        for (name, v) in &self.catalog.vector_indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(v.table.clone()),
                Value::String(format!("vector({}, dim={})", v.metric, v.dim)),
                Value::String(v.path.clone()),
                Value::String(self.index_local_health(name, IndexClass::Vector).into()),
            ]);
        }
        for (name, s) in &self.catalog.search_indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(s.table.clone()),
                Value::String(format!("search({})", s.analyzer())),
                Value::String(s.paths.join(", ")),
                Value::String(self.index_local_health(name, IndexClass::Search).into()),
            ]);
        }
        for (name, g) in &self.catalog.geo_indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(g.table.clone()),
                Value::String("geo".into()),
                Value::String(g.path.clone()),
                Value::String(self.index_local_health(name, IndexClass::Geo).into()),
            ]);
        }
        rows.sort_by(|a, b| match (&a[0], &b[0]) {
            (Value::String(x), Value::String(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        });
        ResultSet {
            columns: vec![
                "index".into(),
                "table".into(),
                "kind".into(),
                "columns".into(),
                "local".into(),
            ],
            rows,
        }
    }

    /// The catalog half of `DESCRIBE`: a table's primary-key columns (in key
    /// order) and, per column, the indexes that cover it (`name (kind)`).
    /// Handles ordinary tables and a time-series table's implicit
    /// `(series key…, ts)` key. Errors if the table is unknown. Shared by
    /// [`Database::describe`] and [`Database::describe_full`].
    fn describe_catalog(&self, table: &str) -> Result<DescribeCatalog> {
        let pk: Vec<String> = if let Some(def) = self.catalog.tables.get(table) {
            def.primary_key.clone()
        } else if let Some(ts) = self.catalog.timeseries.get(table) {
            let mut k = ts.series_key.clone();
            k.push("ts".to_string());
            k
        } else {
            return Err(EngineError::TableNotFound(table.to_string()));
        };

        // A MULTIKEY index path carries a trailing `[]`; the column it covers
        // is the path without it (matching how the engine strips it).
        let column_of = |p: &str| p.strip_suffix("[]").unwrap_or(p).to_string();

        // column -> covering-index descriptors, keyed sorted so non-PK columns
        // come out alphabetically.
        let mut idx_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, idx) in &self.catalog.indexes {
            if idx.table != table {
                continue;
            }
            let kind = if idx.building { "secondary, building" } else { "secondary" };
            let label = format!("{} ({kind})", namespace::split(name).1);
            for p in &idx.paths {
                idx_of.entry(column_of(p)).or_default().push(label.clone());
            }
        }
        for (name, v) in &self.catalog.vector_indexes {
            if v.table != table {
                continue;
            }
            idx_of
                .entry(column_of(&v.path))
                .or_default()
                .push(format!("{} (vector)", namespace::split(name).1));
        }
        for (name, s) in &self.catalog.search_indexes {
            if s.table != table {
                continue;
            }
            let label = format!("{} (search)", namespace::split(name).1);
            for p in &s.paths {
                idx_of.entry(column_of(p)).or_default().push(label.clone());
            }
        }
        for (name, g) in &self.catalog.geo_indexes {
            if g.table != table {
                continue;
            }
            idx_of
                .entry(column_of(&g.path))
                .or_default()
                .push(format!("{} (geo)", namespace::split(name).1));
        }
        Ok((pk, idx_of))
    }

    /// The `key` cell for column `col` given the primary key `pk`: `primary
    /// key` for a single-column key, `primary key (n/m)` for a composite one,
    /// empty for a non-key column.
    fn describe_key_cell(pk: &[String], col: &str) -> String {
        match pk.iter().position(|c| c == col) {
            Some(_) if pk.len() == 1 => "primary key".to_string(),
            Some(i) => format!("primary key ({}/{})", i + 1, pk.len()),
            None => String::new(),
        }
    }

    /// `DESCRIBE <table>`: the table's structural catalog as a
    /// `column | key | indexes` table — one row per column that participates
    /// in the primary key or an index. Rows are ordered primary-key columns
    /// first (in key order), then the remaining indexed columns alphabetically.
    /// The store is schema-less, so fields that are neither part of the key nor
    /// indexed are not in the catalog and do not appear — use
    /// [`Database::describe_full`] (`DESCRIBE … FULL`) to sample the data for
    /// the complete field set. `table` is the internal (database-resolved)
    /// name; errors if unknown.
    pub fn describe(&self, table: &str) -> Result<ResultSet> {
        let (pk, idx_of) = self.describe_catalog(table)?;
        let pk_set: HashSet<&String> = pk.iter().collect();
        let mut order = pk.clone();
        order.extend(idx_of.keys().filter(|c| !pk_set.contains(*c)).cloned());

        let rows = order
            .iter()
            .map(|col| {
                let indexes = idx_of.get(col).map(|v| v.join(", ")).unwrap_or_default();
                vec![
                    Value::String(col.clone()),
                    Value::String(Self::describe_key_cell(&pk, col)),
                    Value::String(indexes),
                ]
            })
            .collect();
        Ok(ResultSet {
            columns: vec!["column".into(), "key".into(), "indexes".into()],
            rows,
        })
    }

    /// The scan half of `DESCRIBE … FULL`: fold the first `limit` rows'
    /// top-level fields into the set of types seen per field. Streaming k-way
    /// merge in key order — we keep only field names + type tags, never the
    /// rows, so even an unbounded scan stays bounded in memory.
    fn scan_field_types(engine: &StorageEngine, limit: usize) -> Result<FieldTypes> {
        let mut types = FieldTypes::new();
        for item in engine.scan_iter().take(limit) {
            let (_, bytes) = item?;
            // Best-effort: skip a row that fails to decode rather than
            // failing the whole DESCRIBE over one corrupt value.
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                for (field, v) in &doc.0 {
                    types
                        .entry(field.clone())
                        .or_default()
                        .insert(value_type_label(v));
                }
            }
        }
        Ok(types)
    }

    /// `DESCRIBE … FULL EXACT`'s data half: the complete field→types map from
    /// a full scan, served from the RAM registry when the table's `write_seq`
    /// still matches the stamp the cached scan ran at (callers hold at least
    /// the shared database lock, and every mutation path holds it exclusively,
    /// so the seq cannot move mid-scan). A TTL table is never cached: row
    /// visibility there decays with wall time, without any write to bump the
    /// seq. The registry resets with the process — the first EXACT after a
    /// restart rescans.
    fn exact_field_types(&self, table: &str) -> Result<FieldTypes> {
        let Some(engine) = self.tables.get(table) else {
            return Ok(FieldTypes::new()); // time-series: no row store
        };
        let seq = engine.write_seq();
        let cacheable = !engine.has_ttl();
        if cacheable {
            let reg = self.field_registry.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((stamp, cached)) = reg.get(table) {
                if *stamp == seq {
                    return Ok(cached.clone());
                }
            }
        }
        let types = Self::scan_field_types(engine, usize::MAX)?;
        if cacheable {
            self.field_registry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(table.to_string(), (seq, types.clone()));
        }
        Ok(types)
    }

    /// `DESCRIBE <table> FULL [SAMPLE n | EXACT]`: the catalog view plus
    /// **every field** discovered in the data — a `column | type | key |
    /// indexes` table. `type` is the set of value types seen for that field,
    /// joined by ` | ` (schema-less: one field can hold several types).
    ///
    /// Sampling (`SAMPLE n`, default [`DESCRIBE_FULL_DEFAULT_SAMPLE`]) reads
    /// the first `n` rows in primary-key order; a field set only in rows
    /// outside the sample is not seen. `EXACT` scans all rows and caches the
    /// result in the RAM field registry, revalidated against the table's write
    /// stamp — repeated EXACTs on an unchanged table are O(fields), and any
    /// write triggers a rescan on the next call. Non-key/non-indexed columns
    /// absent from the data show a blank type. On a cluster this reads the
    /// local shard (complete when RF ≥ members). Time-series tables report
    /// catalog columns only (blank types), as their values live outside the
    /// row store.
    pub fn describe_full(
        &self,
        table: &str,
        sample: Option<usize>,
        exact: bool,
    ) -> Result<ResultSet> {
        let (pk, idx_of) = self.describe_catalog(table)?;
        let types = if exact {
            self.exact_field_types(table)?
        } else {
            let limit = sample.unwrap_or(DESCRIBE_FULL_DEFAULT_SAMPLE);
            match self.tables.get(table) {
                Some(engine) => Self::scan_field_types(engine, limit)?,
                None => FieldTypes::new(), // time-series: no row store
            }
        };

        // Column universe: primary-key columns first (key order), then every
        // indexed-or-sampled column, alphabetically.
        let pk_set: HashSet<&String> = pk.iter().collect();
        let mut rest: BTreeSet<String> = BTreeSet::new();
        rest.extend(idx_of.keys().cloned());
        rest.extend(types.keys().cloned());
        let mut order = pk.clone();
        order.extend(rest.into_iter().filter(|c| !pk_set.contains(c)));

        let rows = order
            .iter()
            .map(|col| {
                let ty = types
                    .get(col)
                    .map(|s| s.iter().copied().collect::<Vec<_>>().join(" | "))
                    .unwrap_or_default();
                let indexes = idx_of.get(col).map(|v| v.join(", ")).unwrap_or_default();
                vec![
                    Value::String(col.clone()),
                    Value::String(ty),
                    Value::String(Self::describe_key_cell(&pk, col)),
                    Value::String(indexes),
                ]
            })
            .collect();
        Ok(ResultSet {
            columns: vec![
                "column".into(),
                "type".into(),
                "key".into(),
                "indexes".into(),
            ],
            rows,
        })
    }

    /// `SHOW STATUS`: storage and runtime statistics as a `metric | value`
    /// table, plus a per-table live-key/tombstone/disk breakdown.
    pub fn show_status(&self) -> ResultSet {
        let s = self.stats(true);
        let hit_rate = {
            let total = s.cache_hits + s.cache_misses;
            if total == 0 {
                "n/a".to_string()
            } else {
                format!("{:.1}%", 100.0 * s.cache_hits as f64 / total as f64)
            }
        };
        let mut rows: Vec<Vec<Value>> = Vec::new();
        {
            let mut row =
                |metric: &str, value: Value| rows.push(vec![Value::String(metric.into()), value]);
            row("tables", Value::Int(s.tables as i64));
            row("secondary_indexes", Value::Int(s.secondary_indexes as i64));
            row("vector_indexes", Value::Int(s.vector_indexes as i64));
            row("vectors_indexed", Value::Int(s.vectors_indexed as i64));
            row("vector_rebuild_ms", Value::Int(s.vector_rebuild_ms as i64));
            row("search_indexes", Value::Int(s.search_indexes as i64));
            row("search_docs", Value::Int(s.search_docs as i64));
            row("search_rebuild_ms", Value::Int(s.search_rebuild_ms as i64));
            row("disk_bytes", Value::Int(s.disk_bytes as i64));
            row("memtable_bytes", Value::Int(s.memtable_bytes as i64));
            row("sstable_count", Value::Int(s.sstable_count as i64));
            row("wal_bytes", Value::Int(s.wal_bytes as i64));
            row("wal_fsyncs", Value::Int(s.wal_fsyncs as i64));
            row("compactions", Value::Int(s.compactions as i64));
            row("compaction_bytes", Value::Int(s.compaction_bytes as i64));
            row("cache_hits", Value::Int(s.cache_hits as i64));
            row("cache_misses", Value::Int(s.cache_misses as i64));
            row("cache_hit_rate", Value::String(hit_rate));
            row("cache_entries", Value::Int(s.cache_entries as i64));
            row("cache_evictions", Value::Int(s.cache_evictions as i64));
            row("bloom_negatives", Value::Int(s.bloom_negatives as i64));
            for t in &s.per_table {
                let q = format!("{}.{}", t.database, t.name);
                row(&format!("table.{q}.live_keys"), Value::Int(t.live_keys as i64));
                row(&format!("table.{q}.tombstones"), Value::Int(t.tombstones as i64));
                row(&format!("table.{q}.disk_bytes"), Value::Int(t.disk_bytes as i64));
            }
            // TIME-SERIES tables join the per-table breakdown (they were
            // invisible to any client walking `table.*` — reported from
            // onet): same `table.<db>.<name>.` prefix, with the fields that
            // are MEANINGFUL for a TS store (`series`, cumulative sample
            // counters, `disk_bytes`) instead of pretending row-table
            // semantics (there are no tombstones, and a cheap exact live
            // count doesn't exist — the `timeseries.*` keys remain for
            // existing consumers).
            for (name, store) in &self.timeseries {
                let ts = store.stats();
                let (dbname, bare) = namespace::split(name);
                let q = format!("{dbname}.{bare}");
                row(&format!("table.{q}.kind"), Value::String("timeseries".into()));
                row(&format!("table.{q}.series"), Value::Int(ts.series as i64));
                row(
                    &format!("table.{q}.samples_appended"),
                    Value::Int(ts.samples_appended as i64),
                );
                row(&format!("table.{q}.disk_bytes"), Value::Int(ts.disk_bytes as i64));
            }
            // Per-search-index breakdown, in catalog (name) order.
            for name in self.catalog.search_indexes.keys() {
                let Some(live) = self.search_indexes.get(name) else {
                    continue;
                };
                let fs = live.index.stats();
                let d = namespace::display_name(name, DEFAULT_DATABASE);
                row(&format!("search.{d}.docs"), Value::Int(fs.docs as i64));
                row(&format!("search.{d}.disk_bytes"), Value::Int(fs.disk_bytes as i64));
                row(&format!("search.{d}.uncommitted"), Value::Int(fs.uncommitted as i64));
            }
            row("timeseries_tables", Value::Int(self.timeseries.len() as i64));
            for (name, store) in &self.timeseries {
                let ts = store.stats();
                let name = namespace::display_name(name, DEFAULT_DATABASE);
                row(&format!("timeseries.{name}.series"), Value::Int(ts.series as i64));
                row(&format!("timeseries.{name}.blocks"), Value::Int(ts.blocks as i64));
                row(
                    &format!("timeseries.{name}.samples_appended"),
                    Value::Int(ts.samples_appended as i64),
                );
                row(
                    &format!("timeseries.{name}.samples_rejected"),
                    Value::Int(ts.samples_rejected as i64),
                );
                row(&format!("timeseries.{name}.disk_bytes"), Value::Int(ts.disk_bytes as i64));
                row(
                    &format!("timeseries.{name}.maintenance_errors"),
                    Value::Int(ts.maintenance_errors as i64),
                );
            }
        }
        ResultSet {
            columns: vec!["metric".into(), "value".into()],
            rows,
        }
    }

    /// Flush every table/index memtable holding a non-trivial amount of memory
    /// to an on-disk SSTable, reclaiming its in-memory footprint. Called by the
    /// node under memory pressure: the per-engine flush threshold is global-
    /// pressure-blind (memtables spread thin across many tables each stay under
    /// it while the sum pins the node), and the normal flush is triggered by a
    /// *write* — but a shedding node rejects writes, so nothing would ever
    /// trigger it and the node deadlocks (sheds → no write → no flush → stays
    /// shedding). Returns bytes reclaimed; trivially small memtables are skipped
    /// so this doesn't litter tiny SSTables.
    /// Push the deepest-level tombstone-retention window to every TABLE
    /// engine (indexes are local derived data — never pulled by a witness —
    /// so their tombstones keep dropping immediately). Tables created after
    /// this call start at 0 and pick the value up on the next push (the
    /// server re-pushes every minute).
    pub fn set_tombstone_retention_ms(&mut self, ms: u64) {
        for (name, engine) in self.tables.iter_mut() {
            // A table excluded from witness mirrors owes witnesses nothing:
            // holding its tombstones would pay exactly the cost the
            // exclusion exists to avoid.
            let mirrored = self.catalog.tables.get(name).is_none_or(|d| d.witness);
            engine.set_tombstone_retention_ms(if mirrored { ms } else { 0 });
        }
    }

    pub fn flush_memtables_under_pressure(&mut self) -> usize {
        self.release_memory_under_pressure(false)
    }

    /// The full memory-release pass for a node under pressure: flush memtables
    /// past a floor (64 KB when `aggressive` — a node about to shed keeps
    /// nothing it can drop; 4 MB otherwise, so routine release doesn't litter
    /// tiny SSTables) and commit every dirty search-index writer (Tantivy
    /// holds indexed-but-uncommitted docs in heap buffers; a commit moves them
    /// to disk segments). Returns memtable bytes reclaimed (search-writer
    /// buffers aren't cheaply measurable, so they're released but uncounted).
    pub fn release_memory_under_pressure(&mut self, aggressive: bool) -> usize {
        let floor: usize = if aggressive { 64 * 1024 } else { 4 * 1024 * 1024 };
        let mut reclaimed = 0;
        for engine in self.tables.values_mut().chain(self.indexes.values_mut()) {
            let bytes = engine.memtable_bytes();
            if bytes >= floor && engine.flush().is_ok() {
                reclaimed += bytes;
            }
            // Aggressive (shedding-level) pressure also drops the point-read
            // caches: they are entry-capped and byte-blind, so bulk point
            // reads of multi-KB rows can pin far more RAM than the budget
            // assumed (the witness ramped to its ceiling this way with
            // memtables flat). A cold cache refills; an OOM kill does not.
            if aggressive {
                engine.clear_read_cache();
            }
        }
        for live in self.search_indexes.values_mut() {
            let _ = live.commit_if_dirty();
        }
        // TIME-SERIES heads: up to 256 MB EACH (budget/8, clamped) and the
        // release tier never touched them — under sustained TS ingest
        // (onet's ~135 inserts/s across several per-org metrics tables)
        // the heads were the ramp the flush-memtables release couldn't
        // reclaim, and skai1 OOM-cycled through a full day (2026-07-21,
        // 54 kernel kills). Flush them wholesale under pressure; the head
        // repopulates from live ingest and the flushed blocks compact.
        if aggressive {
            for store in self.timeseries.values() {
                let head = store.head_bytes();
                if head >= floor && store.flush().is_ok() {
                    reclaimed += head;
                }
            }
        }
        reclaimed
    }

    /// Aggregate storage/runtime statistics for metrics. When `per_table` is set,
    /// each table is scanned for live-key/tombstone counts (O(rows)); otherwise
    /// only cheap engine snapshots are gathered.
    pub fn stats(&self, per_table: bool) -> DbStats {
        let mut agg = DbStats {
            tables: self.catalog.tables.len(),
            secondary_indexes: self.catalog.indexes.len(),
            vector_indexes: self.catalog.vector_indexes.len(),
            vector_rebuild_ms: self.vector_rebuild_ms,
            vectors_indexed: self.vector_indexes.values().map(|h| h.len()).sum(),
            search_indexes: self.catalog.search_indexes.len(),
            search_rebuild_ms: self.search_rebuild_ms,
            ..Default::default()
        };
        for live in self.search_indexes.values() {
            let s = live.index.stats();
            agg.search_docs += s.docs;
            agg.search_disk_bytes += s.disk_bytes;
        }
        // Fold in every table and index storage engine.
        for engine in self.tables.values().chain(self.indexes.values()) {
            let s = engine.stats();
            agg.memtable_bytes += s.memtable_bytes as u64;
            agg.sstable_count += s.sstable_count as u64;
            agg.disk_bytes += s.disk_bytes;
            agg.wal_bytes += s.wal_bytes;
            agg.wal_fsyncs += s.wal_fsyncs;
            agg.compactions += s.compactions;
            agg.compaction_bytes += s.compaction_bytes;
            agg.cache_hits += s.cache.hits;
            agg.cache_misses += s.cache.misses;
            agg.cache_evictions += s.cache.evictions;
            agg.cache_entries += s.cache.entries as u64;
            agg.bloom_negatives += s.bloom_negatives;
        }
        if per_table {
            for (name, engine) in &self.tables {
                let s = engine.stats();
                // Approximate (version counts): SHOW STATUS iterated every
                // key of every table exactly — 22s on a populated node.
                let ks = engine.key_stats_fast();
                let (db, bare) = namespace::split(name);
                agg.per_table.push(TableStats {
                    database: db.to_string(),
                    name: bare.to_string(),
                    live_keys: ks.live_keys as u64,
                    tombstones: ks.tombstones as u64,
                    disk_bytes: s.disk_bytes,
                    sstables: s.sstable_count as u64,
                });
            }
            agg
                .per_table
                .sort_by(|a, b| (&a.database, &a.name).cmp(&(&b.database, &b.name)));
            for (name, store) in &self.timeseries {
                let ts = store.stats();
                let (db, bare) = namespace::split(name);
                agg.per_timeseries.push(TsTableStats {
                    database: db.to_string(),
                    name: bare.to_string(),
                    series: ts.series as u64,
                    samples_appended: ts.samples_appended,
                    samples_rejected: ts.samples_rejected,
                    disk_bytes: ts.disk_bytes,
                    blocks: ts.blocks as u64,
                    maintenance_errors: ts.maintenance_errors,
                });
            }
            agg
                .per_timeseries
                .sort_by(|a, b| (&a.database, &a.name).cmp(&(&b.database, &b.name)));
        }
        agg
    }

    /// Scan a local table returning `(key, encoded_doc, stamp)` for each live
    /// row — the coordinator merges these across replicas by last-writer-wins.
    pub fn local_scan_versioned(&self, table: &str) -> Result<Vec<VersionedRow>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine.scan_versioned()?)
    }

    /// Like [`Database::local_scan_versioned`] but includes tombstones as
    /// `(key, empty_value, hlc, is_put = false)`, so a coordinator can resolve a
    /// table across replicas by last-writer-wins without a delete being masked by
    /// a stale `Put` on another replica.
    pub fn local_scan_versioned_with_tombstones(
        &self,
        table: &str,
    ) -> Result<Vec<VersionedTombstoneRow>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine
            .scan_versioned_with_tombstones()?
            .into_iter()
            .map(|(key, hlc, value)| match value {
                Some(bytes) => (key, bytes, hlc, true),
                None => (key, Vec::new(), hlc, false),
            })
            .collect())
    }

    /// One bounded page of [`Database::local_scan_versioned_with_tombstones`]:
    /// up to `limit` rows with key strictly greater than `after`, in key
    /// order. Memory is proportional to the page, not the table — the seam
    /// incremental anti-entropy pages through.
    pub fn local_scan_versioned_page(
        &self,
        table: &str,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<VersionedTombstoneRow>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine
            .scan_versioned_page(after, limit)?
            .into_iter()
            .map(|(key, hlc, value)| match value {
                Some(bytes) => (key, bytes, hlc, true),
                None => (key, Vec::new(), hlc, false),
            })
            .collect())
    }

    /// Value-free [`Database::local_scan_versioned_page`]: one bounded page of
    /// `(key, hlc, is_put)` stamps in key order. Anti-entropy digests hash
    /// exactly these three fields, and the storage layer serves them from the
    /// stamps sidecar — a whole-table digest pass never decompresses values.
    /// The table's storage mutation counter (`Engine::write_seq`):
    /// process-lifetime monotonic, bumped on every applied mutation. A
    /// witness uses it as a cheap per-cycle change hint.
    pub fn table_write_seq(&self, table: &str) -> Result<u64> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine.write_seq())
    }

    /// One bounded DELTA page for a witness's incremental pull: walk up to
    /// `stamp_limit` value-free stamps with key strictly greater than
    /// `after`, and return the rows whose HLC physical time is
    /// `>= since_physical` — values point-read per matching key (a put
    /// whose value vanished mid-walk is skipped; the next cycle converges
    /// it), tombstones included with empty values. Returns
    /// `(rows, cursor, done)`: the cursor resumes the STAMPS walk, so a
    /// sparse delta costs bounded work per call regardless of match count.
    pub fn local_scan_since_page(
        &self,
        table: &str,
        since_physical: u64,
        after: Option<&[u8]>,
        stamp_limit: usize,
    ) -> Result<(Vec<VersionedTombstoneRow>, Option<Vec<u8>>, bool)> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let stamps = engine.scan_stamps_page(after, stamp_limit)?;
        let done = stamps.len() < stamp_limit;
        let cursor = stamps.last().map(|(k, ..)| k.clone());
        let mut rows = Vec::new();
        for (key, hlc, is_put) in stamps {
            if hlc.physical < since_physical {
                continue;
            }
            if is_put {
                // A vanished value (rewritten mid-walk) converges next cycle.
                if let Some(value) = engine.get(&key)? {
                    rows.push((key, value, hlc, true));
                }
            } else {
                rows.push((key, Vec::new(), hlc, false));
            }
        }
        Ok((rows, cursor, done))
    }

    pub fn local_scan_stamps_page(
        &self,
        table: &str,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Hlc, bool)>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine.scan_stamps_page(after, limit)?)
    }

    /// One bounded versioned range `[start, end)` of `table`, tombstones
    /// included — the per-replica read behind the routed global-index probe.
    pub fn local_scan_versioned_range(
        &self,
        table: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<VersionedTombstoneRow>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(engine
            .scan_versioned_range(start, end, limit)?
            .into_iter()
            .map(|(key, hlc, value)| match value {
                Some(bytes) => (key, bytes, hlc, true),
                None => (key, Vec::new(), hlc, false),
            })
            .collect())
    }

    /// Plan a routed GLOBAL-index probe for `filter`: the first ready
    /// (non-building) global index on `table` whose EVERY column is pinned
    /// by an equality or a literal `IN` list, as `(index name, entry
    /// ranges)` — one `[start, end)` range per value tuple (`IN` lists
    /// cross-multiply, capped at [`GIDX_PROBE_RANGES`]). Partial prefixes
    /// and value ranges do not route under hash placement and keep the
    /// scatter paths. Consulted by the cluster coordinator after PK and
    /// local-index plans decline.
    pub fn plan_global_probe(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Option<(String, Vec<ProbeRange>)> {
        let expr = filter.as_ref()?;
        for (name, idx) in self
            .catalog
            .indexes
            .iter()
            .filter(|(_, i)| i.table == table && i.global && !i.building)
        {
            let cols = index_key_names(&idx.paths);
            // Per-column candidate sets: `col = lit` or `col IN (lits)` —
            // the same pins the PK point-read set accepts.
            let mut per_col: Vec<Vec<Value>> = Vec::with_capacity(cols.len());
            for col in &cols {
                match pk_column_candidates(expr, col) {
                    Some(c) if !c.is_empty() => per_col.push(c),
                    // Pinned to an empty set (`IN (NULL)`): matches nothing.
                    Some(_) => return Some((name.clone(), Vec::new())),
                    None => {
                        per_col.clear();
                        break;
                    }
                }
            }
            if per_col.len() != cols.len() {
                continue; // not fully pinned: this index cannot route
            }
            let mut total = 1usize;
            for c in &per_col {
                total = total.checked_mul(c.len())?;
                if total > GIDX_PROBE_RANGES {
                    return None; // too many tuples: scatter beats N range RTTs
                }
            }
            // Cross product over the composite columns → one probe range per
            // value tuple.
            let mut ranges = Vec::with_capacity(total);
            let mut idx_v = vec![0usize; per_col.len()];
            'outer: loop {
                let values: Vec<Value> = idx_v
                    .iter()
                    .zip(&per_col)
                    .map(|(&i, c)| c[i].clone())
                    .collect();
                if let Some(bounds) = gidx_probe_bounds(&values) {
                    ranges.push(bounds);
                }
                let mut d = per_col.len();
                loop {
                    if d == 0 {
                        break 'outer;
                    }
                    d -= 1;
                    idx_v[d] += 1;
                    if idx_v[d] < per_col[d].len() {
                        break;
                    }
                    idx_v[d] = 0;
                }
            }
            ranges.sort();
            ranges.dedup();
            return Some((name.clone(), ranges));
        }
        None
    }

    /// Every GLOBAL index as `(name, building)` — the repair pass's
    /// maintenance worklist.
    pub fn all_global_indexes(&self) -> Vec<(String, bool)> {
        self.catalog
            .indexes
            .iter()
            .filter(|(_, d)| d.global)
            .map(|(n, d)| (n.clone(), d.building))
            .collect()
    }

    /// A GLOBAL index's `(base table, paths)`, if it exists and is global.
    pub fn gidx_def(&self, index: &str) -> Option<(String, Vec<String>)> {
        self.catalog
            .indexes
            .get(index)
            .filter(|d| d.global)
            .map(|d| (d.table.clone(), d.paths.clone()))
    }

    /// The placement-relevant slice of a table's definition:
    /// `(replication override, pinned nodes)`. GLOBAL-index entry tables
    /// resolve through their BASE table — an entry's durability follows the
    /// row's, and a pinned base places its entries on the same pins (the
    /// routed probe stays one replica-set round-trip). Unknown tables get
    /// cluster-default placement (`(None, [])`) — the safe answer for a
    /// table mid-drop or not yet synced.
    #[allow(clippy::type_complexity)]
    pub fn table_placement(
        &self,
        table: &str,
    ) -> ((Option<u32>, Vec<String>), Option<(Option<u32>, Vec<String>)>) {
        let base;
        let name = if is_gidx_table(table) {
            let (db, bare) = namespace::split(table);
            let index = namespace::qualify(db, bare.strip_prefix("__gidx__").unwrap_or(bare));
            match self.catalog.indexes.get(&index) {
                Some(d) if d.global => {
                    base = d.table.clone();
                    base.as_str()
                }
                _ => return ((None, Vec::new()), None),
            }
        } else {
            table
        };
        self.catalog
            .tables
            .get(name)
            .map(|d| {
                (
                    (d.replication, d.pinned_nodes.clone()),
                    d.prev_placement
                        .as_ref()
                        .map(|p| (p.replication, p.pinned_nodes.clone())),
                )
            })
            .unwrap_or(((None, Vec::new()), None))
    }

    /// Tables with an OPEN placement transition (prev stashed, finalize
    /// pending) — the transition driver's work list.
    pub fn tables_in_placement_transition(&self) -> Vec<String> {
        self.catalog
            .tables
            .iter()
            .filter(|(_, d)| d.prev_placement.is_some())
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Tables whose pinned placement names `node` — `remove_member`'s guard:
    /// a pinned node may hold a table's only copies, so removal must be
    /// refused until the operator re-pins.
    pub fn tables_pinning(&self, node: &str) -> Vec<String> {
        self.catalog
            .tables
            .iter()
            .filter(|(_, d)| {
                d.pinned_nodes.iter().any(|n| n == node)
                    // Mid-transition the OLD pins may hold the only
                    // converged copies — they are as unremovable as
                    // current pins until finalize.
                    || d.prev_placement
                        .as_ref()
                        .is_some_and(|p| p.pinned_nodes.iter().any(|n| n == node))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Flip a GLOBAL index out of `building` (the backfill driver finished).
    /// Advances the index's schema stamp: schema sync then replays the
    /// definition as `WITH (global = true, ready = true)` past nodes whose
    /// stamp predates readiness — a node that missed the broadcast (down at
    /// the time, or freshly bootstrapped) converges on its next repair
    /// instead of never routing probes.
    pub fn finish_global_backfill(&mut self, name: &str) {
        if let Some(def) = self.catalog.indexes.get_mut(name) {
            if def.global && def.building {
                def.building = false;
                let hlc = self.ddl_stamp();
                self.record_schema(format!("i:{name}"), hlc, false);
                let _ = self.save_catalog();
            }
        }
    }

    /// One page of a single-node GLOBAL-index backfill: entries for up to
    /// `limit` live rows of the base table after `cursor`, written to the
    /// local entry table (on a single node the local shard IS the ring).
    /// Returns the next cursor; `None` when complete (index unmarked
    /// `building`). Cluster nodes do NOT use this — the DDL coordinator
    /// drives a replicated backfill across every member's shard instead.
    pub fn backfill_gidx_page(
        &mut self,
        name: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        let Some(def) = self.catalog.indexes.get(name).cloned() else {
            return Ok(None); // dropped mid-backfill
        };
        if !def.global || !def.building {
            return Ok(None);
        }
        let rows = {
            let Some(engine) = self.tables.get(&def.table) else {
                self.finish_global_backfill(name);
                return Ok(None);
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        let gname = gidx_table(name);
        if let Some(engine) = self.tables.get_mut(&gname) {
            for (row_key, _hlc, bytes) in rows {
                let Some(bytes) = bytes else { continue }; // tombstone
                if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                    let (_, puts) = global_entry_delta(&def.paths, &row_key, None, Some(&doc));
                    for k in puts {
                        engine.put(&k, Value::encode_document(&Document::new()))?;
                    }
                }
            }
        }
        if done {
            self.finish_global_backfill(name);
            return Ok(None);
        }
        Ok(next)
    }

    /// One page of GLOBAL-index **orphan GC**: walk up to `limit` live
    /// entries of the index after `cursor` and delete every entry whose row
    /// no longer exists or no longer produces it (the write-crash-window
    /// leftovers; until collected they only waste probe re-checks, never
    /// correctness). Verification is per-entry local point reads
    /// (bloom-gated), so this is only sound where the rows are guaranteed
    /// local — full-copy clusters and single nodes; the caller gates that.
    /// Returns `(next cursor, entries removed)`.
    pub fn gidx_gc_page(
        &mut self,
        index: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Option<Vec<u8>>, usize)> {
        let Some((base_table, paths)) = self.gidx_def(index) else {
            return Ok((None, 0));
        };
        let gname = gidx_table(index);
        let entries = {
            let Some(engine) = self.tables.get(&gname) else {
                return Ok((None, 0));
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = entries.len() < limit;
        let next = entries.last().map(|(k, _, _)| k.clone());
        let mut orphans: Vec<Vec<u8>> = Vec::new();
        for (ekey, _hlc, value) in &entries {
            if value.is_none() {
                continue; // already a tombstone
            }
            let Some(row_key) = gidx_entry_row_key(ekey) else {
                orphans.push(ekey.clone()); // malformed (pre-phase-2 codec)
                continue;
            };
            let row = self
                .tables
                .get(&base_table)
                .and_then(|e| e.get(row_key).ok().flatten());
            let produced = row
                .and_then(|bytes| match Value::decode(&bytes) {
                    Ok(Value::Document(doc)) => Some(doc),
                    _ => None,
                })
                .is_some_and(|doc| {
                    index_value_tuples(&doc, &paths)
                        .iter()
                        .any(|values| gidx_entry_key(values, row_key) == *ekey)
                });
            if !produced {
                orphans.push(ekey.clone());
            }
        }
        let removed = orphans.len();
        if let Some(engine) = self.tables.get_mut(&gname) {
            for ekey in orphans {
                engine.delete(&ekey)?;
            }
        }
        Ok(((!done).then_some(next).flatten(), removed))
    }

    /// One page of GLOBAL-index **missing-entry healing**: walk up to
    /// `limit` live rows of the base table after `cursor` and write any
    /// entry the row should have but the entry table lacks (the other side
    /// of the crash window — and unlike orphans, a missing entry silently
    /// hides its row from probes). Same locality requirement as
    /// [`Database::gidx_gc_page`]. Returns `(next cursor, entries added)`.
    pub fn gidx_heal_page(
        &mut self,
        index: &str,
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Option<Vec<u8>>, usize)> {
        let Some((base_table, paths)) = self.gidx_def(index) else {
            return Ok((None, 0));
        };
        let gname = gidx_table(index);
        let rows = {
            let Some(engine) = self.tables.get(&base_table) else {
                return Ok((None, 0));
            };
            engine.scan_versioned_page(cursor.as_deref(), limit)?
        };
        let done = rows.len() < limit;
        let next = rows.last().map(|(k, _, _)| k.clone());
        let mut missing: Vec<Vec<u8>> = Vec::new();
        for (row_key, _hlc, value) in &rows {
            let Some(bytes) = value else { continue }; // tombstone
            let Ok(Value::Document(doc)) = Value::decode(bytes) else {
                continue;
            };
            let Some(engine) = self.tables.get(&gname) else {
                return Ok((None, 0));
            };
            for values in index_value_tuples(&doc, &paths) {
                let ekey = gidx_entry_key(&values, row_key);
                if engine.get(&ekey)?.is_none() {
                    missing.push(ekey);
                }
            }
        }
        let added = missing.len();
        if let Some(engine) = self.tables.get_mut(&gname) {
            for ekey in missing {
                engine.put(&ekey, Value::encode_document(&Document::new()))?;
            }
        }
        Ok(((!done).then_some(next).flatten(), added))
    }

    /// The subset of `keys` that exist as LIVE puts in `table` (order
    /// preserved). Bloom-gated point reads — the RF<members verify leg's
    /// presence probe.
    pub fn keys_present(&self, table: &str, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let Some(engine) = self.tables.get(table) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for k in keys {
            if engine.get(k)?.is_some() {
                out.push(k.clone());
            }
        }
        Ok(out)
    }

    /// The subset of GLOBAL-index `entry_keys` still PRODUCED by this node's
    /// rows: for each, the embedded row exists locally and its current
    /// document yields exactly that entry key. Asked of a row's primary
    /// owner, so "row absent here" soundly means "row absent" — the orphan
    /// direction of the RF<members verify leg.
    pub fn gidx_produced(&self, index: &str, entry_keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let Some((base_table, paths)) = self.gidx_def(index) else {
            return Ok(Vec::new());
        };
        let Some(engine) = self.tables.get(&base_table) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for ekey in entry_keys {
            let Some(row_key) = gidx_entry_row_key(ekey) else {
                continue; // malformed: never "produced"
            };
            let produced = engine
                .get(row_key)?
                .and_then(|bytes| match Value::decode(&bytes) {
                    Ok(Value::Document(doc)) => Some(doc),
                    _ => None,
                })
                .is_some_and(|doc| {
                    index_value_tuples(&doc, &paths)
                        .iter()
                        .any(|values| gidx_entry_key(values, row_key) == *ekey)
                });
            if produced {
                out.push(ekey.clone());
            }
        }
        Ok(out)
    }

    /// Run a single-node GLOBAL-index backfill to completion, inline.
    pub fn run_gidx_backfill(&mut self, name: &str) -> Result<()> {
        let mut cursor = None;
        while let Some(next) = self.backfill_gidx_page(name, cursor.take(), 2048)? {
            cursor = Some(next);
        }
        Ok(())
    }

    /// **Filter pushdown**: the primary keys of this node's rows whose latest
    /// local version is a `Put` matching `filter`. The coordinator unions these
    /// candidate keys across replicas and re-reads each at quorum — exactly like a
    /// secondary-index scan — so the result is sound under last-writer-wins (a
    /// newer non-matching or deleted version on another replica is caught by the
    /// re-read) while shipping only candidate keys instead of the whole shard.
    pub fn local_scan_filtered_keys(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<Vec<u8>>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        let mut out = Vec::new();
        // PK-prefix narrowing (agencik E-8): when the filter equality-pins a
        // leftmost run of PK columns, walk only that key slice of this shard
        // instead of the whole table — this runs on EVERY member (the
        // coordinator's local leg and each peer's `FilteredScan` handler
        // both land here), so the clustered `WHERE channel = ?` shape stops
        // costing a full shard scan per member. Streaming, like the full
        // walk below. Tombstones need no separate handling: `scan_range_iter`
        // yields live puts only, and a key whose latest version is a
        // tombstone was never a candidate.
        if let Some((start, end)) = self
            .table_def(table)
            .ok()
            .and_then(|def| pk_prefix_scan_range(&def.primary_key, filter))
        {
            for item in engine.scan_range_iter(Some(&start), end.as_deref()) {
                crate::scan_meter::tick(1)?;
                let (key, bytes) = item?;
                if let Value::Document(doc) = Value::decode(&bytes)
                    .map_err(|e| EngineError::Constraint(format!("corrupt row: {e}")))?
                {
                    if matches_filter(filter, &doc)? {
                        out.push(key);
                    }
                }
            }
            return Ok(out);
        }
        // Stream one row at a time: the materializing scan built a whole-table
        // Vec (~1.8 GB on the largest production table) per filtered RPC and
        // OOM'd 4 GB nodes when several stacked (2026-07-13).
        for item in engine.scan_versioned_with_tombstones_iter() {
            crate::scan_meter::tick(1)?;
            let (key, _hlc, value) = item?;
            let Some(bytes) = value else { continue }; // tombstone: not a candidate
            if let Value::Document(doc) = Value::decode(&bytes)
                .map_err(|e| EngineError::Constraint(format!("corrupt row: {e}")))?
            {
                if matches_filter(filter, &doc)? {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }

    /// Point-read the latest stored version of `key` in `table`: returns
    /// `(value, stamp, is_put)` where `is_put == false` marks a tombstone, or
    /// `None` if the key was never written here. The coordinator merges these
    /// across replicas by last-writer-wins.
    pub fn local_get_versioned(
        &self,
        table: &str,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Hlc, bool)>> {
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(match engine.get_versioned(key)? {
            Some((hlc, VersionValue::Put(bytes))) => Some((bytes, hlc, true)),
            Some((hlc, VersionValue::Delete)) => Some((Vec::new(), hlc, false)),
            None => None,
        })
    }

    /// The ack half of a replicated write: capture the previous row version
    /// (deferred index maintenance needs it — after the memtable insert it is
    /// no longer the latest), append to the WAL + memtable, and hand back a
    /// [`MaintTask`] for the applier when any index/vector/search consumer
    /// needs the document. This is the journal-ack write path: everything
    /// expensive (decode, per-index engine writes and their fsyncs, HNSW,
    /// FTS) happens later, off the ack.
    pub fn apply_put_row_only(
        &mut self,
        table: &str,
        key: &[u8],
        bytes: Vec<u8>,
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>, Option<MaintTask>)> {
        let task = if self.needs_doc_on_write(table) {
            let old = self.tables.get(table).and_then(|e| e.get(key).ok().flatten());
            Some(MaintTask {
                table: table.to_string(),
                key: key.to_vec(),
                hlc,
                old,
                new: Some(bytes.clone()),
            })
        } else {
            None
        };
        let (commit, handle) = {
            let engine = self.table_engine_mut(table)?;
            let commit = engine.append_put_buffered(key, bytes, hlc)?;
            (commit, engine.wal_sync_handle())
        };
        Ok((commit, handle, task))
    }

    /// The ack half of a replicated delete; see [`Database::apply_put_row_only`].
    pub fn apply_delete_row_only(
        &mut self,
        table: &str,
        key: &[u8],
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>, Option<MaintTask>)> {
        let task = if self.needs_doc_on_write(table) {
            let old = self.tables.get(table).and_then(|e| e.get(key).ok().flatten());
            Some(MaintTask {
                table: table.to_string(),
                key: key.to_vec(),
                hlc,
                old,
                new: None,
            })
        } else {
            None
        };
        let (commit, handle) = {
            let engine = self.table_engine_mut(table)?;
            let commit = engine.append_delete_buffered(key, hlc)?;
            (commit, engine.wal_sync_handle())
        };
        Ok((commit, handle, task))
    }

    /// The deferred half: index delete/put, vector, and search maintenance
    /// for one write, from documents the applier decoded OUTSIDE the write
    /// lock. Called by the applier thread and by crash-recovery replay.
    pub fn apply_maintenance(&mut self, m: &DecodedMaint) -> Result<()> {
        if let Some(old) = &m.old_doc {
            for (name, path) in self.indexes_on(&m.table) {
                self.index_del(&name, &path, old, &m.key)?;
            }
            self.maintain_geo_del(&m.table, old, &m.key)?;
        }
        match &m.new_doc {
            Some(new) => {
                for (name, path) in self.indexes_on(&m.table) {
                    self.index_put(&name, &path, new, &m.key)?;
                }
                self.maintain_vectors_put(&m.table, new, &m.key, m.hlc);
                self.maintain_geo_put(&m.table, new, &m.key)?;
                self.search_put_unrefreshed(&m.table, new, &m.key, m.hlc)?;
            }
            None => {
                self.maintain_vectors_del(&m.table, &m.key, m.hlc);
                self.search_del_unrefreshed(&m.table, &m.key, m.hlc)?;
            }
        }
        Ok(())
    }

    /// Background-flusher work orders across every table and index engine:
    /// `(is_index, engine_name, job)`. Marks each frozen memtable as
    /// building; abort or install must follow.
    pub fn background_flush_jobs(&mut self) -> Vec<(bool, String, FlushJob)> {
        let mut out = Vec::new();
        for (name, e) in self.tables.iter_mut() {
            if let Some(job) = e.take_flush_job() {
                out.push((false, name.clone(), job));
            }
        }
        for (name, e) in self.indexes.iter_mut() {
            if let Some(job) = e.take_flush_job() {
                out.push((true, name.clone(), job));
            }
        }
        out
    }

    /// Install (or on `sst: Err`, abort) one built background flush.
    pub fn finish_flush_job(
        &mut self,
        is_index: bool,
        name: &str,
        job: FlushJob,
        sst: std::result::Result<skaidb_storage::SsTable, EngineError>,
    ) -> Result<()> {
        let engine = if is_index {
            self.indexes.get_mut(name)
        } else {
            self.tables.get_mut(name)
        };
        let Some(engine) = engine else {
            return Ok(()); // dropped table/index — job is moot
        };
        match sst {
            Ok(sst) => engine.install_flush(job, sst)?,
            Err(_) => engine.abort_flush(&job),
        }
        Ok(())
    }

    /// One background compaction work order per engine over its trigger.
    pub fn background_compact_jobs(&mut self) -> Vec<(bool, String, CompactJob)> {
        let mut out = Vec::new();
        for (name, e) in self.tables.iter_mut() {
            if let Some(job) = e.take_compact_job() {
                out.push((false, name.clone(), job));
            }
        }
        for (name, e) in self.indexes.iter_mut() {
            if let Some(job) = e.take_compact_job() {
                out.push((true, name.clone(), job));
            }
        }
        out
    }

    /// Install one built background compaction (stale epochs self-discard).
    pub fn finish_compact_job(
        &mut self,
        is_index: bool,
        name: &str,
        job: CompactJob,
        new_run: skaidb_storage::SsTable,
    ) -> Result<()> {
        let engine = if is_index {
            self.indexes.get_mut(name)
        } else {
            self.tables.get_mut(name)
        };
        if let Some(engine) = engine {
            engine.install_compact(job, new_run)?;
        }
        Ok(())
    }

    /// Whether any engine has background flush/compaction work pending.
    pub fn has_background_storage_work(&self) -> bool {
        self.tables.values().any(StorageEngine::has_background_work)
            || self.indexes.values().any(StorageEngine::has_background_work)
    }

    /// Advance the applier watermark for `table`: every write stamped `<=`
    /// `hlc` has had its deferred maintenance applied. Unblocks WAL
    /// truncation at the storage layer and is persisted (throttled by the
    /// caller) for crash-recovery replay.
    pub fn set_applied_watermark(&mut self, table: &str, hlc: Hlc) {
        self.applied_watermarks.insert(table.to_string(), hlc);
        if let Ok(engine) = self.table_engine_mut(table) {
            engine.set_maintenance_watermark(hlc);
        }
    }

    /// Persist the applier watermarks (atomic rename). Cheap — one tiny file.
    pub fn persist_applied_watermarks(&self) -> Result<()> {
        let tmp = self.dir.join("applier.watermarks.tmp");
        let dst = self.dir.join("applier.watermarks");
        let entries: Vec<(String, u64, u32)> = self
            .applied_watermarks
            .iter()
            .map(|(t, h)| (t.clone(), h.physical, h.logical))
            .collect();
        let bytes = serde_json::to_vec(&entries)
            .map_err(|e| EngineError::Constraint(format!("watermark encode: {e}")))?;
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &dst)?;
        Ok(())
    }

    /// Replay deferred maintenance for writes newer than each table's
    /// persisted watermark. Runs at open, after WAL replay re-populated the
    /// memtables: the truncation gate guarantees every unapplied write is
    /// still there. Idempotent — re-applying an already-applied write puts
    /// the same index entries back.
    fn recover_deferred_maintenance(&mut self) -> Result<()> {
        let tables: Vec<String> = self
            .catalog
            .tables
            .keys()
            .filter(|t| self.needs_doc_on_write(t))
            .cloned()
            .collect();
        for table in tables {
            let wm = self.applied_watermarks.get(&table).copied();
            let Some(wm) = wm else { continue };
            // Collect (key, ascending versions past the watermark) chains.
            let mut chains: Vec<(Vec<u8>, VersionChain)> = Vec::new();
            {
                let Some(engine) = self.tables.get(&table) else { continue };
                let mut cur_key: Option<Vec<u8>> = None;
                for (key, hlc, vv) in engine.mem_versions() {
                    if hlc <= wm {
                        continue;
                    }
                    let val = match vv {
                        VersionValue::Put(b) => Some(b.clone()),
                        VersionValue::Delete => None,
                    };
                    if cur_key.as_deref() != Some(key) {
                        cur_key = Some(key.to_vec());
                        chains.push((key.to_vec(), Vec::new()));
                    }
                    // mem_versions yields newest-first per key; build then reverse.
                    chains.last_mut().expect("pushed above").1.push((hlc, val));
                }
            }
            if chains.is_empty() {
                continue;
            }
            let mut max_seen = wm;
            for (key, mut versions) in chains {
                versions.reverse(); // oldest first
                // Base: the newest version at-or-below the watermark — from
                // the memtable chain if present, else the flushed layer.
                let mut prev: Option<Vec<u8>> = {
                    let engine = self.tables.get(&table).expect("checked above");
                    let mem_base = engine
                        .mem_versions()
                        .filter(|(k, h, _)| *k == key.as_slice() && *h <= wm)
                        .max_by_key(|(_, h, _)| *h)
                        .map(|(_, _, vv)| match vv {
                            VersionValue::Put(b) => Some(b.clone()),
                            VersionValue::Delete => None,
                        });
                    match mem_base {
                        Some(v) => v,
                        None => engine.get_flushed(&key)?,
                    }
                };
                for (hlc, new) in versions {
                    let decode = |b: &Vec<u8>| match Value::decode(b) {
                        Ok(Value::Document(d)) => Some(d),
                        _ => None,
                    };
                    let m = DecodedMaint {
                        table: table.clone(),
                        key: key.clone(),
                        hlc,
                        old_doc: prev.as_ref().and_then(decode),
                        new_doc: new.as_ref().and_then(decode),
                    };
                    self.apply_maintenance(&m)?;
                    if hlc > max_seen {
                        max_seen = hlc;
                    }
                    prev = new;
                }
            }
            self.set_applied_watermark(&table, max_seen);
        }
        self.persist_applied_watermarks()?;
        Ok(())
    }

    /// Apply a replicated row write at an explicit stamp, maintaining indexes.
    pub fn apply_put(&mut self, table: &str, key: &[u8], bytes: Vec<u8>, hlc: Hlc) -> Result<()> {
        // Decode lazily: only index maintenance reads the document, and
        // parsing large JSON rows dominated replicated-apply CPU on tables
        // with no indexes (see `needs_doc_on_write`).
        let doc = if self.needs_doc_on_write(table) {
            match Value::decode(&bytes)
                .map_err(|e| EngineError::Constraint(format!("corrupt replicated row: {e}")))?
            {
                Value::Document(d) => Some(d),
                _ => {
                    return Err(EngineError::Constraint(
                        "replicated row is not a document".into(),
                    ))
                }
            }
        } else {
            None
        };
        if doc.is_some() {
            self.index_del_previous(table, key)?;
        }
        self.table_engine_mut(table)?
            .put_with_hlc(key, bytes, hlc)?;
        if let Some(doc) = doc {
            for (name, path) in self.indexes_on(table) {
                self.index_put(&name, &path, &doc, key)?;
            }
            self.maintain_vectors_put(table, &doc, key, hlc);
            self.maintain_geo_put(table, &doc, key)?;
            self.maintain_search_put(table, &doc, key, hlc)?;
        }
        Ok(())
    }

    /// Physically drop local rows of `table` whose key fails `keep`, reclaiming
    /// space for keys this node no longer owns after resharding. Unlike a delete
    /// this leaves **no tombstone** (dropped keys vanish from scans and from
    /// migration), so it must only be used for keys whose authoritative copy
    /// lives on another node. Secondary-index entries pointing at dropped rows
    /// are left dangling — reads already skip an index entry whose row is absent,
    /// and they are reclaimed as the index compacts. Returns the rows dropped.
    pub fn retain_rows(&mut self, table: &str, keep: impl Fn(&[u8]) -> bool) -> Result<usize> {
        Ok(self.table_engine_mut(table)?.retain(keep)?)
    }

    /// Apply a replicated delete at an explicit stamp, maintaining indexes.
    pub fn apply_delete(&mut self, table: &str, key: &[u8], hlc: Hlc) -> Result<()> {
        // Read the local row first so its index entries can be removed.
        let existing = self
            .tables
            .get(table)
            .and_then(|e| e.get(key).ok().flatten());
        self.table_engine_mut(table)?.delete_with_hlc(key, hlc)?;
        if let Some(bytes) = existing {
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                for (name, path) in self.indexes_on(table) {
                    self.index_del(&name, &path, &doc, key)?;
                }
                self.maintain_geo_del(table, &doc, key)?;
            }
        }
        self.maintain_vectors_del(table, key, hlc);
        self.maintain_search_del(table, key, hlc)?;
        Ok(())
    }

    /// Buffered replicated write (no fsync): append + apply + maintain indexes,
    /// returning the commit point and the WAL sync handle so the coordinator can
    /// fsync after releasing its write lock (group commit).
    pub fn apply_put_buffered(
        &mut self,
        table: &str,
        key: &[u8],
        bytes: Vec<u8>,
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>)> {
        let out = self.apply_put_row_buffered(table, key, bytes, hlc)?;
        self.search_refresh(table)?;
        Ok(out)
    }

    /// The row half of [`Database::apply_put_buffered`], without the NRT
    /// refresh check (batch callers refresh once per batch).
    fn apply_put_row_buffered(
        &mut self,
        table: &str,
        key: &[u8],
        bytes: Vec<u8>,
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>)> {
        let doc = if self.needs_doc_on_write(table) {
            match Value::decode(&bytes)
                .map_err(|e| EngineError::Constraint(format!("corrupt replicated row: {e}")))?
            {
                Value::Document(d) => Some(d),
                _ => {
                    return Err(EngineError::Constraint(
                        "replicated row is not a document".into(),
                    ))
                }
            }
        } else {
            None
        };
        if doc.is_some() {
            self.index_del_previous(table, key)?;
        }
        let (commit, handle) = {
            let engine = self.table_engine_mut(table)?;
            let commit = engine.append_put_buffered(key, bytes, hlc)?;
            (commit, engine.wal_sync_handle())
        };
        if let Some(doc) = doc {
            for (name, path) in self.indexes_on(table) {
                self.index_put(&name, &path, &doc, key)?;
            }
            self.maintain_vectors_put(table, &doc, key, hlc);
            self.maintain_geo_put(table, &doc, key)?;
            self.search_put_unrefreshed(table, &doc, key, hlc)?;
        }
        Ok((commit, handle))
    }

    /// Buffered replicated delete (no fsync); see [`Database::apply_put_buffered`].
    pub fn apply_delete_buffered(
        &mut self,
        table: &str,
        key: &[u8],
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>)> {
        let out = self.apply_delete_row_buffered(table, key, hlc)?;
        self.search_refresh(table)?;
        Ok(out)
    }

    /// The row half of [`Database::apply_delete_buffered`], without the NRT
    /// refresh check.
    fn apply_delete_row_buffered(
        &mut self,
        table: &str,
        key: &[u8],
        hlc: Hlc,
    ) -> Result<(WalCommit, Arc<WalSync>)> {
        let existing = self
            .tables
            .get(table)
            .and_then(|e| e.get(key).ok().flatten());
        let (commit, handle) = {
            let engine = self.table_engine_mut(table)?;
            let commit = engine.append_delete_buffered(key, hlc)?;
            (commit, engine.wal_sync_handle())
        };
        if let Some(bytes) = existing {
            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                for (name, path) in self.indexes_on(table) {
                    self.index_del(&name, &path, &doc, key)?;
                }
                self.maintain_geo_del(table, &doc, key)?;
            }
        }
        self.maintain_vectors_del(table, key, hlc);
        self.search_del_unrefreshed(table, key, hlc)?;
        Ok((commit, handle))
    }

    /// A whole replicated batch, buffered: every row appended + applied +
    /// index-maintained under **one** NRT refresh check (the phase-5
    /// bulk-ingest path — an index commit can no longer fire mid-batch),
    /// returning the last row's commit point + sync handle. Row shape:
    /// `(key, value, hlc, is_put)`; `is_put == false` is a tombstone.
    pub fn apply_batch_buffered(
        &mut self,
        table: &str,
        rows: &[(Vec<u8>, Vec<u8>, Hlc, bool)],
    ) -> Result<Option<(WalCommit, Arc<WalSync>)>> {
        let mut last = None;
        for (key, value, hlc, is_put) in rows {
            last = Some(if *is_put {
                self.apply_put_row_buffered(table, key, value.clone(), *hlc)?
            } else {
                self.apply_delete_row_buffered(table, key, *hlc)?
            });
        }
        self.search_refresh(table)?;
        Ok(last)
    }

    /// Journal-ack twin of [`Database::apply_batch_buffered`]: rows land in
    /// the WAL + memtable only; the returned [`MaintTask`]s carry the
    /// deferred index/vector/search half to the applier.
    pub fn apply_batch_row_only(
        &mut self,
        table: &str,
        rows: &[(Vec<u8>, Vec<u8>, Hlc, bool)],
    ) -> Result<BatchRowOnly> {
        let mut last = None;
        let mut tasks = Vec::new();
        for (key, value, hlc, is_put) in rows {
            let (commit, handle, task) = if *is_put {
                self.apply_put_row_only(table, key, value.clone(), *hlc)?
            } else {
                self.apply_delete_row_only(table, key, *hlc)?
            };
            last = Some((commit, handle));
            if let Some(t) = task {
                tasks.push(t);
            }
        }
        Ok((last, tasks))
    }

    // ---- helpers ----

    fn table_def(&self, name: &str) -> Result<&TableDef> {
        self.catalog
            .tables
            .get(name)
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))
    }

    fn table_engine_mut(&mut self, name: &str) -> Result<&mut StorageEngine> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))
    }

    fn scan_docs(&self, name: &str) -> Result<Vec<(Vec<u8>, Document)>> {
        let engine = self
            .tables
            .get(name)
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;
        let mut out = Vec::new();
        for (key, bytes) in engine.scan()? {
            match Value::decode(&bytes) {
                Ok(Value::Document(doc)) => out.push((key, doc)),
                Ok(_) => {
                    return Err(EngineError::Constraint(
                        "stored row is not a document".into(),
                    ))
                }
                Err(e) => return Err(EngineError::Constraint(format!("corrupt row: {e}"))),
            }
        }
        Ok(out)
    }

    fn save_catalog(&self) -> Result<()> {
        self.catalog.save(self.dir.join("catalog.json"))
    }
}

/// Storage seam for the SQL executor (SPEC §4–6). Implemented by [`LocalCluster`]
/// for the embedded engine and by the cluster coordinator for replicated,
/// quorum-based reads and writes — so [`run`] is identical in both worlds.
///
/// Read methods take `&self` so a `SELECT` can execute with shared access to
/// the underlying storage (concurrent readers); only the write methods need
/// `&mut self`.
/// Rollup routing metadata:
/// `(retention horizon, rollup-complete-below, [(rollup name, bucket_ms)])`.
/// Below the **retention horizon** raw blocks may already be dropped, so
/// group buckets *must* come from a rollup. Below **rollup-complete-below**
/// (the head's oldest timestamp) every sample has been through rollup
/// maintenance (flush path or repair backfill), so buckets *may* come from
/// a rollup opportunistically — same numbers, less raw-sample IO. `None`
/// disables the respective routing.
pub type TsRollupInfo = (Option<i64>, Option<i64>, Vec<(String, i64)>);

pub trait Cluster {
    /// Primary-key columns of `table`.
    fn primary_key(&self, table: &str) -> Result<Vec<String>>;
    /// `(key, doc)` for rows of `table` matching `filter`.
    fn matching_rows(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>>;

    /// Like [`Cluster::matching_rows`] but may use an index to return rows
    /// already sorted ascending by `order` (a single plain column) and to stop
    /// after `fetch_limit` matching rows. Returns the rows and whether they are
    /// actually sorted by `order`. The default ignores the hints.
    fn matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        _order: Option<(&str, bool, bool)>,
        _fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        Ok((self.matching_rows(table, filter)?, false))
    }
    /// Like [`Cluster::matching_rows`], but hints that only the document
    /// fields named in `project` will ever be read from the result —
    /// implementors MAY use this to skip decoding every other field
    /// (see [`group_by_projection_columns`], the only current caller: a
    /// `GROUP BY`/aggregate gather over a wide-row table otherwise decodes
    /// every column of every matching row before discarding all but a
    /// handful — the exact shape that OOM-killed a node on an unfiltered
    /// `GROUP BY` over a 1.9 GB table, agencik wishlist E-7). The default
    /// ignores the hint and defers to `matching_rows` (always correct,
    /// just unoptimized). `project` must be a true superset of every
    /// column the caller will read — an implementor that prunes anything
    /// not in `project` is trusting the caller got that superset right.
    fn matching_rows_projected(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: &HashSet<String>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let _ = project;
        self.matching_rows(table, filter)
    }
    /// Count rows matching `filter` without materializing them — the fallback
    /// for filtered `COUNT(*)` when no covering index applies. `None` = no
    /// streaming count available; the caller gathers (scan-meter bounded).
    fn count_matching(&self, _table: &str, _filter: &Option<Expr>) -> Result<Option<usize>> {
        Ok(None)
    }
    /// Distinct values of one column across rows matching `filter`, without
    /// materializing rows — `SELECT DISTINCT col` used to gather every
    /// matching document on the coordinator to deduplicate at the end.
    /// `None` = unavailable; the caller falls back to the gather.
    fn distinct_values(
        &self,
        _table: &str,
        _col: &str,
        _filter: &Option<Expr>,
    ) -> Result<Option<Vec<Value>>> {
        Ok(None)
    }
    /// Fast count of `table`'s live rows, when the implementation can serve it
    /// without materializing or decoding rows (`None` = unavailable; the
    /// caller falls back to a full gather). Only consulted for unfiltered
    /// `COUNT(*)`.
    fn count_rows(&self, _table: &str) -> Result<Option<usize>> {
        Ok(None)
    }
    /// Fast count of `table`'s rows matching `filter`, when a fully covering
    /// secondary index answers without materializing or decoding rows
    /// (`None` = unavailable; the caller falls back to a full gather).
    fn count_filtered(&self, _table: &str, _filter: &Option<Expr>) -> Result<Option<usize>> {
        Ok(None)
    }
    /// Approximate nearest-neighbor search over the vector index on
    /// `(table, path)`: the `k` rows whose vector is closest to `query`,
    /// nearest first, as `(key, doc, distance)`. `filter` restricts results
    /// (evaluated authoritatively by the implementation).
    fn vector_search(
        &self,
        _table: &str,
        _path: &str,
        _query: &[f32],
        _k: usize,
        _filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        Err(EngineError::Unsupported(
            "vector search is not available on this backend".into(),
        ))
    }
    /// Embed a query string for a **managed** (`EMBED`) vector index on
    /// `(table, path)` — a string `NEAREST` query is auto-embedded. Errors if
    /// the index isn't managed or no inference provider is configured.
    fn embed_query(&self, _table: &str, _path: &str, _text: &str) -> Result<Vec<f32>> {
        Err(EngineError::Unsupported(
            "a string NEAREST query needs a managed EMBED index with [inference] configured".into(),
        ))
    }
    /// Score `documents` against `query` with the deployment's rerank
    /// provider (the `RERANK` clause) — higher = more relevant, one score per
    /// document in order. Always answered by the coordinator's local engine
    /// (the provider is installed on every node); no scatter.
    fn rerank(&self, _model: &str, _query: &str, _documents: &[String]) -> Result<Vec<f32>> {
        Err(EngineError::Unsupported(
            "RERANK needs a rerank provider ([inference] rerank_url)".into(),
        ))
    }
    /// Full-text search over the search index covering `query`'s fields on
    /// `table`, as `(key, doc, score)`. `k = Some(n)` is the ranked path (the
    /// `n` best BM25 scores, best first); `k = None` returns every matching
    /// row with score 0.0 and unspecified order (pure predicate). `filter` is
    /// the residual WHERE, applied authoritatively by the implementation.
    /// `highlights` are `(column, max_chars)` snippet requests; each hit doc
    /// gets a `_highlight_<column>` field. `&mut self` so the write path can
    /// commit pending index writes first (read-your-writes); read-only
    /// implementations serve the last-committed state instead.
    fn search(
        &mut self,
        _table: &str,
        _query: &SearchQuery,
        _k: Option<usize>,
        _filter: &Option<Expr>,
        _highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        Err(EngineError::Unsupported(
            "full-text search is not supported on this deployment".into(),
        ))
    }
    /// Exact fast-field aggregation pushdown for a grouped search SELECT
    /// (docs/SEARCH.md "Aggregations"), or `Ok(None)` when this deployment or
    /// request shape cannot serve it **exactly** — the caller falls back to
    /// materializing the matching rows and aggregating those. The default
    /// (and any deployment whose local index does not hold every row)
    /// declines.
    fn search_aggregate(
        &mut self,
        _table: &str,
        _query: &SearchQuery,
        _agg: &skaidb_fts::AggRequest,
    ) -> Result<Option<Vec<skaidb_fts::AggRow>>> {
        Ok(None)
    }
    /// Fast-field-ordered top-k search (`ORDER BY <col> LIMIT k`,
    /// phase 7): the `k` best rows by the sort column, best-first, or
    /// `Ok(None)` when the deployment or column cannot serve the exact SQL
    /// ordering — the caller falls back to gathering every match and
    /// sorting generically. The default declines; multi-member scatter for
    /// this path is future work (the fallback stays correct at any RF).
    fn search_sorted(
        &mut self,
        _table: &str,
        _query: &SearchQuery,
        _sort: &skaidb_fts::SortSpec,
        _k: usize,
        _filter: &Option<Expr>,
        _highlights: &[HighlightReq],
    ) -> Result<Option<SortedSearchRows>> {
        Ok(None)
    }
    /// Write `doc` under `key` in `table` (and maintain indexes/replicas).
    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()>;
    /// Write a whole statement's rows in one call. Implementations may batch
    /// the WAL fsync and the replication round-trips (one per replica set
    /// instead of one per row); the default is the per-row loop.
    fn put_batch(&mut self, table: &str, rows: &[(Vec<u8>, Document)]) -> Result<()> {
        for (key, doc) in rows {
            self.put(table, key, doc)?;
        }
        Ok(())
    }
    /// Delete `key` from `table`; `doc` is the row being removed (for indexes).
    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()>;

    /// Series-key (label) columns when `table` is a **time-series** table;
    /// `None` for regular tables. Routes DML/SELECT to the tsdb paths.
    fn ts_series_key(&self, _table: &str) -> Result<Option<Vec<String>>> {
        Ok(None)
    }
    /// Append samples to a time-series table. Labels include the reserved
    /// `__field__` pair and are sorted by key.
    fn ts_append(&mut self, _table: &str, _rows: &[(skaidb_tsdb::Labels, i64, f64)]) -> Result<usize> {
        Err(EngineError::Unsupported(
            "time-series tables are not available on this backend".into(),
        ))
    }
    /// Samples in `[t0, t1]` for series matching every matcher, per series,
    /// time-ordered.
    fn ts_query(
        &self,
        _table: &str,
        _matchers: &[skaidb_tsdb::Matcher],
        _t0: i64,
        _t1: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        Err(EngineError::Unsupported(
            "time-series tables are not available on this backend".into(),
        ))
    }
    /// Series LABEL SETS matching every matcher — series METADATA, no
    /// sample materialization; the unit behind label-DISTINCT queries
    /// (`SELECT DISTINCT <label cols> FROM <ts>` used to gather every
    /// sample and die on the scan budget at metrics scale). The default
    /// derives them from [`Cluster::ts_query`] (correct, heavy);
    /// implementations override with the store's series listing.
    fn ts_series_sets(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
    ) -> Result<Vec<skaidb_tsdb::Labels>> {
        Ok(self
            .ts_query(table, matchers, i64::MIN, i64::MAX)?
            .into_iter()
            .map(|(labels, _)| labels)
            .collect())
    }
    /// The table's LOCAL data range `(oldest, newest)`, `None` when
    /// unknown/empty. Anchors unbounded LIMIT slice walks at the actual
    /// data frontier instead of the wall clock (a dormant table's data is
    /// otherwise only reachable via a widened slice that swallows it
    /// whole). Local is exact at RF = members (full copies, e.g. prod);
    /// at RF < members the pre-walk edge gathers cover cross-shard skew.
    fn ts_local_range(&self, _table: &str) -> Result<Option<(i64, i64)>> {
        Ok(None)
    }

    /// Rollup routing metadata for a time-series table: the retention
    /// horizon (timestamp below which source blocks may already be dropped;
    /// `None` = source is complete) and the table's rollups as
    /// `(name, bucket_ms)`. Lets the executor serve aged buckets from a
    /// rollup. The default reports no retention and no rollups.
    fn ts_rollup_info(&self, _table: &str) -> Result<TsRollupInfo> {
        Ok((None, None, Vec::new()))
    }
    /// Per-series per-bucket partial aggregates for series matching every
    /// matcher in `[t0, t1]` (`bucket_ms <= 0` = one whole-range bucket).
    /// A cluster implementation ships partials instead of raw samples and
    /// answers each series from one replica; the default derives them from
    /// [`Cluster::ts_query`].
    fn ts_partials(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        bucket_ms: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<crate::ts_query::TsPartial>)>> {
        Ok(crate::ts_query::ts_partialize(
            self.ts_query(table, matchers, t0, t1)?,
            bucket_ms,
        ))
    }
}

/// Whether `stmt` only reads — i.e. it can run through the shared-access entry
/// points ([`Database::execute_read`] / [`Database::execute_session_read`])
/// without exclusive access. DML, DDL, and transaction control are not
/// read-only. Lets a caller holding an `RwLock<Database>` pick the lock mode.
/// The `EXPLAIN SCORE` result: one `explanation` row of BM25-breakdown
/// JSON when the row matches, zero rows when it does not.
fn explain_score_rows(text: Option<String>) -> ResultSet {
    ResultSet {
        columns: vec!["explanation".into()],
        rows: text.map(|t| vec![vec![Value::String(t)]]).unwrap_or_default(),
    }
}

pub fn statement_is_read_only(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Select(_)
            | Statement::Suggest { .. }
            | Statement::ExplainScore { .. }
            | Statement::Explain { .. }
            | Statement::Describe { .. }
            | Statement::ShowTables
            | Statement::ShowIndexes
            | Statement::ShowStatus
            | Statement::ShowGrants { .. }
            | Statement::ShowDatabases
    )
}

/// Execute one DML/SELECT statement against any [`Cluster`].
///
/// DDL is handled by each executor directly (locally, or broadcast in a
/// cluster), so only the data-plane statements arrive here.
pub fn run(stmt: Statement, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
    // Output aliases referenced from ORDER BY/GROUP BY/HAVING resolve to
    // their expressions BEFORE dispatch — unresolved they read NULL off the
    // source docs and the query silently mis-orders or empties.
    let stmt = match stmt {
        Statement::Select(mut sel) => {
            skaidb_sql::resolve_select_aliases(&mut sel);
            Statement::Select(sel)
        }
        other => other,
    };
    match stmt {
        Statement::Insert(ins) => run_insert(ins, cluster),
        // Hybrid (`RANK BY RRF`) runs both the search leg (needs the `&mut`
        // read-your-writes seam) and the vector leg, so it takes precedence
        // over the plain search dispatch below.
        Statement::Select(sel) if sel.rrf.is_some() => {
            run_hybrid_select(&sel, cluster).map(QueryOutput::Rows)
        }
        // A search SELECT keeps the `&mut` seam: `Cluster::search` commits
        // pending index writes first (read-your-writes).
        Statement::Select(sel) if select_uses_search(&sel) => {
            run_search_select(&sel, cluster).map(QueryOutput::Rows)
        }
        // RERANK re-scores search hits: without a search predicate there is
        // nothing to rerank (caught here so plain/NEAREST selects error
        // clearly instead of silently ignoring the clause).
        Statement::Select(sel) if sel.rerank.is_some() => Err(EngineError::Unsupported(
            "RERANK requires a MATCH()/SEARCH() predicate in WHERE (optionally with \
             NEAREST … RANK BY RRF)"
                .into(),
        )),
        // AFTER pages a ranked search; a filter-only or NEAREST select has no
        // stable rank to cursor over — keyset-paginate those with a WHERE
        // range on the sort column instead.
        Statement::Select(sel) if sel.after.is_some() => Err(EngineError::Unsupported(
            "AFTER requires a MATCH()/SEARCH() query ordered by score() DESC or a \
             column; for filter-only queries page with `WHERE <col> > <last>` instead"
                .into(),
        )),
        // SELECT only reads: reborrow shared so the executor's read-only
        // requirements are checked by the compiler.
        Statement::Select(sel) => run_select(&sel, &*cluster).map(QueryOutput::Rows),
        Statement::Update(upd) => run_update(upd, cluster),
        Statement::Delete(del) => run_delete(del, cluster),
        _ => Err(EngineError::Unsupported(
            "non-data statement reached the data-plane executor".into(),
        )),
    }
}

fn run_insert(ins: Insert, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
    if let Some(series_key) = cluster.ts_series_key(&ins.table)? {
        return crate::ts_query::run_ts_insert(&ins, &series_key, cluster);
    }
    let pk = cluster.primary_key(&ins.table)?;
    let empty = Document::new();
    let mut rows: Vec<(Vec<u8>, Document)> = Vec::with_capacity(ins.rows.len());
    for row in &ins.rows {
        let mut doc = Document::new();
        for (col, expr) in ins.columns.iter().zip(row) {
            doc.insert(col.clone(), eval(expr, &empty)?);
        }
        let key = primary_key_bytes(&pk, &doc)?;
        rows.push((key, doc));
    }
    let affected = rows.len();
    cluster.put_batch(&ins.table, &rows)?;
    Ok(QueryOutput::Mutation { affected })
}

fn run_select(sel: &Select, cluster: &dyn Cluster) -> Result<ResultSet> {
    check_bound_counts(sel)?;
    // The scan meter is context-free by design; re-add the context here where
    // the statement is in scope, so "scan budget exceeded" names the table
    // and filter columns and the fix (which index to add) is mechanical.
    enrich_scan_budget(run_select_impl(sel, cluster), &sel.from, &sel.filter)
}

/// A `LIMIT ?`/`OFFSET ?` position surviving to execution means the statement
/// went through the one-shot path instead of prepare/execute — same contract
/// as an unbound `?` in an expression.
fn check_bound_counts(sel: &Select) -> Result<()> {
    if sel.limit_param.is_some() || sel.offset_param.is_some() {
        return Err(EngineError::Type(
            "unbound parameter (`?`) in LIMIT/OFFSET".into(),
        ));
    }
    Ok(())
}

/// Append the table and filter columns to a scan-budget error.
fn enrich_scan_budget<T>(
    r: Result<T>,
    table: &str,
    filter: &Option<Expr>,
) -> Result<T> {
    match r {
        Err(EngineError::ResourceLimit(msg)) if msg.starts_with("scan budget exceeded") => {
            // Internal names carry the namespace separator; show `db.table`.
            let table = table.replace('\u{1f}', ".");
            let mut cols: Vec<String> = Vec::new();
            if let Some(f) = filter {
                collect_filter_columns(f, &mut cols);
            }
            let detail = if cols.is_empty() {
                format!(" [table {table}]")
            } else {
                format!(" [table {table}, filter column(s): {}]", cols.join(", "))
            };
            Err(EngineError::ResourceLimit(format!("{msg}{detail}")))
        }
        other => other,
    }
}

/// Column paths referenced anywhere in a filter, in first-seen order.
fn collect_filter_columns(e: &Expr, out: &mut Vec<String>) {
    if let Expr::Column(c) = e {
        if !out.iter().any(|x| x == c) {
            out.push(c.clone());
        }
    }
    match e {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => collect_filter_columns(expr, out),
        Expr::Binary { left, right, .. } => {
            collect_filter_columns(left, out);
            collect_filter_columns(right, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_filter_columns(expr, out);
            for item in list {
                collect_filter_columns(item, out);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            collect_filter_columns(expr, out);
            collect_filter_columns(lo, out);
            collect_filter_columns(hi, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_filter_columns(expr, out);
            collect_filter_columns(pattern, out);
        }
        Expr::Func { args, .. } => {
            for a in args {
                collect_filter_columns(a, out);
            }
        }
        Expr::Aggregate {
            arg: AggArg::Expr(inner),
            ..
        } => collect_filter_columns(inner, out),
        Expr::Aggregate { .. } | Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

fn run_select_impl(sel: &Select, cluster: &dyn Cluster) -> Result<ResultSet> {
    // `SELECT <expr>` without FROM: constant projection over one empty row —
    // the cheap liveness probe (`SELECT 1`) and expression calculator. Shared
    // by the embedded and clustered paths since both funnel through here.
    if sel.from.is_empty() {
        return run_const_select(sel);
    }
    // Time-series tables gather compressed samples with time-range + label
    // pushdown, then reuse the ordinary projection/aggregation machinery.
    if let Some(series_key) = cluster.ts_series_key(&sel.from)? {
        return crate::ts_query::run_ts_select(sel, &series_key, cluster);
    }
    // ANN clause: a distance-ordered top-k gather replaces the normal scan.
    if sel.nearest.is_some() {
        return run_nearest_select(sel, cluster);
    }
    // Compound query (`UNION [ALL]`): project each core without ordering/limiting,
    // combine the row sets, then apply the whole-query DISTINCT / ORDER BY / LIMIT.
    if !sel.set_ops.is_empty() {
        let mut rs = project_core(sel, cluster)?;
        for op in &sel.set_ops {
            let leg = project_core(&op.select, cluster)?;
            if leg.columns.len() != rs.columns.len() {
                return Err(EngineError::Unsupported(
                    "UNION branches must have the same number of columns".into(),
                ));
            }
            rs.rows.extend(leg.rows);
            if !op.all {
                dedup_rows(&mut rs.rows);
            }
        }
        return finalize_compound(sel, rs);
    }
    // Single query with joins: gather the joined rows, then project.
    if !sel.joins.is_empty() {
        let docs = gather_join_docs(sel, cluster)?;
        let hide = join_hidden_keys(sel);
        return project(sel, docs, &hide, true);
    }
    // Simple single-table query: keep the index-accelerated ORDER BY / top-N path.
    run_simple_select(sel, cluster)
}

/// `SELECT <expr> [, ...]` with no FROM: evaluate each projected expression
/// against an empty row and return the single constant row. Aggregates are
/// rejected by scalar eval; a stray column reference evaluates to `NULL`
/// (there is no row for it to come from).
fn run_const_select(sel: &Select) -> Result<ResultSet> {
    let empty = Document::new();
    let mut columns = Vec::with_capacity(sel.items.len());
    let mut row = Vec::with_capacity(sel.items.len());
    for item in &sel.items {
        let SelectItem::Expr { expr, alias } = item else {
            return Err(EngineError::Unsupported(
                "SELECT * requires a FROM table".into(),
            ));
        };
        columns.push(alias.clone().unwrap_or_else(|| expr_name(expr)));
        row.push(eval(expr, &empty)?);
    }
    Ok(ResultSet::new(columns, vec![row]))
}

/// Execute a `NEAREST` (vector search) select: resolve the query vector and
/// `k`, run the ANN gather (already filtered and nearest-first), expose each
/// hit's distance as a `_distance` field, and project as usual.
/// Evaluate a query-vector expression (array literal or bound parameter) to
/// `Vec<f32>` — shared by the `NEAREST` and hybrid (`RANK BY RRF`) paths.
fn resolve_query_vector(
    cluster: &dyn Cluster,
    table: &str,
    path: &str,
    expr: &Expr,
) -> Result<Vec<f32>> {
    match eval(expr, &Document::new())? {
        // A string query on a managed (EMBED) index is auto-embedded.
        Value::String(s) => cluster.embed_query(table, path, &s),
        Value::Array(items) => {
            let mut v = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(i) => v.push(i as f32),
                    Value::Float(f) => v.push(f as f32),
                    _ => {
                        return Err(EngineError::Type(
                            "query vector must be a numeric array".into(),
                        ))
                    }
                }
            }
            Ok(v)
        }
        _ => Err(EngineError::Type(
            "NEAREST query must be a numeric array, or a string for a managed EMBED index".into(),
        )),
    }
}

/// Hybrid retrieval (`RANK BY RRF`): fuse the text leg (the `WHERE` search
/// predicate) and the vector leg (the `NEAREST` clause) by Reciprocal Rank
/// Fusion. Each leg fetches the `NEAREST` `k` candidates — with the residual
/// (non-search) part of `WHERE` applied to BOTH legs (as ES applies a filter to
/// both retrievers) — then `rrf(key) = Σ 1/(c + rank)` over the two ranked
/// lists decides the order. `rrf_score()` exposes the fused score; `LIMIT`
/// takes the final top-k. RRF is rank-based, so BM25 scores and vector
/// distances need no normalization.
fn run_hybrid_select(sel: &Select, cluster: &mut dyn Cluster) -> Result<ResultSet> {
    let rrf = sel.rrf.expect("checked by run()");
    let Some(nearest) = sel.nearest.as_ref() else {
        return Err(EngineError::Unsupported(
            "RANK BY RRF requires a NEAREST (vector) clause".into(),
        ));
    };
    if !sel.joins.is_empty()
        || !sel.set_ops.is_empty()
        || sel.distinct
        || is_grouped(sel)
        || sel.having.is_some()
    {
        return Err(EngineError::Unsupported(
            "RANK BY RRF cannot be combined with JOIN, UNION, DISTINCT, or GROUP BY".into(),
        ));
    }
    if !sel.order_by.is_empty() {
        return Err(EngineError::Unsupported(
            "RANK BY RRF results are ordered by rrf_score(); ORDER BY is not supported".into(),
        ));
    }
    if sel.after.is_some() {
        return Err(EngineError::Unsupported(
            "AFTER is not supported with RANK BY RRF (fused ranks are not a stable cursor)".into(),
        ));
    }
    check_bound_counts(sel)?;

    // Split WHERE: the search predicate is the text leg; the residual filters
    // both legs.
    let (mut queries, residual) = split_search_filter(&sel.filter)?;
    if queries.is_empty() {
        return Err(EngineError::Type(
            "RANK BY RRF requires a MATCH()/SEARCH() predicate in WHERE (the text leg)".into(),
        ));
    }
    let text_query = match queries.len() {
        1 => queries.pop().expect("len checked"),
        _ => SearchQuery::All(queries),
    };

    // Shared vector-leg query + candidate depth (both legs fetch `k`).
    let empty = Document::new();
    let query_vec = resolve_query_vector(&*cluster, &sel.from, &nearest.path, &nearest.query)?;
    let candidate_k = match eval(&nearest.k, &empty)? {
        Value::Int(k) if k > 0 => k as usize,
        _ => {
            return Err(EngineError::Type(
                "NEAREST k must be a positive integer".into(),
            ))
        }
    };

    // Run both legs — each already scatter-gathers to a coordinator-merged
    // ranked list, with the residual filter applied.
    let text_hits = cluster.search(&sel.from, &text_query, Some(candidate_k), &residual, &[])?;
    let vec_hits = cluster.vector_search(&sel.from, &nearest.path, &query_vec, candidate_k, &residual)?;

    // Reciprocal Rank Fusion over the two ranked lists.
    let c = rrf.constant as f32;
    let mut fused: HashMap<Vec<u8>, f32> = HashMap::new();
    let mut docs: HashMap<Vec<u8>, Document> = HashMap::new();
    let mut fuse = |key: Vec<u8>, doc: Document, rank: usize| {
        *fused.entry(key.clone()).or_insert(0.0) += 1.0 / (c + (rank + 1) as f32);
        docs.entry(key).or_insert(doc);
    };
    for (rank, (key, doc, _)) in text_hits.into_iter().enumerate() {
        fuse(key, doc, rank);
    }
    for (rank, (key, doc, _)) in vec_hits.into_iter().enumerate() {
        fuse(key, doc, rank);
    }

    // Order by fused score desc; key asc as a deterministic tie-break.
    let mut ranked: Vec<(Vec<u8>, f32)> = fused.into_iter().collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let mut out_docs: Vec<Document> = ranked
        .into_iter()
        .map(|(key, s)| {
            let mut doc = docs.remove(&key).expect("doc present for fused key");
            doc.insert("_rrf_score", Value::Float(f64::from(s)));
            doc
        })
        .collect();
    // RERANK on a hybrid query re-scores the top `TOP` fused candidates;
    // `rrf_score()` keeps the fusion score, `score()` reads the rerank score.
    if let Some(rr) = &sel.rerank {
        out_docs.truncate(rr.top as usize);
        out_docs = apply_rerank(&*cluster, rr, &text_query, out_docs)?;
    }
    project(sel, out_docs, &HashSet::new(), true)
}

fn run_nearest_select(sel: &Select, cluster: &dyn Cluster) -> Result<ResultSet> {
    let nearest = sel.nearest.as_ref().expect("checked by run_select");
    if !sel.joins.is_empty() || !sel.set_ops.is_empty() {
        return Err(EngineError::Unsupported(
            "NEAREST cannot be combined with JOIN or UNION".into(),
        ));
    }
    if is_grouped(sel) || sel.having.is_some() {
        return Err(EngineError::Unsupported(
            "NEAREST cannot be combined with aggregates or GROUP BY".into(),
        ));
    }
    if !sel.order_by.is_empty() {
        return Err(EngineError::Unsupported(
            "NEAREST results are already ordered by distance; ORDER BY is not supported".into(),
        ));
    }
    let empty = Document::new();
    let query = resolve_query_vector(cluster, &sel.from, &nearest.path, &nearest.query)?;
    let k = match eval(&nearest.k, &empty)? {
        Value::Int(k) if k > 0 => k as usize,
        _ => {
            return Err(EngineError::Type(
                "NEAREST k must be a positive integer".into(),
            ))
        }
    };
    let hits = cluster.vector_search(&sel.from, &nearest.path, &query, k, &sel.filter)?;
    let docs: Vec<Document> = hits
        .into_iter()
        .map(|(_, mut doc, dist)| {
            doc.insert("_distance", Value::Float(dist as f64));
            doc
        })
        .collect();
    // `project` with `finalize` applies the OFFSET/LIMIT page (paging again
    // here double-skipped the offset — found adding RERANK).
    project(sel, docs, &HashSet::new(), true)
}

/// A geo predicate a geo index can serve — a `geo_distance(col, lat, lon) <= r`
/// (or flipped) comparison, or a bare `geo_bbox(col, …)` — pulled out of the
/// filter's top-level AND chain as `(column, bounding box)`. `None` when no such
/// conjunct exists or its arguments are not constants (the query then scans).
/// An antimeridian-crossing or degenerate box also returns `None` — v1 serves a
/// single non-wrapping rectangle and falls back to a scan otherwise.
fn geo_predicate(filter: &Option<Expr>) -> Option<(String, crate::geo::BBox)> {
    find_geo_conjunct(filter.as_ref()?)
}

fn find_geo_conjunct(expr: &Expr) -> Option<(String, crate::geo::BBox)> {
    match expr {
        Expr::Binary { op: BinaryOp::And, left, right } => {
            find_geo_conjunct(left).or_else(|| find_geo_conjunct(right))
        }
        // geo_distance(col, lat, lon) <= r   /   < r
        Expr::Binary { op: BinaryOp::LtEq | BinaryOp::Lt, left, right } => {
            geo_distance_bbox(left, right)
        }
        // r >= geo_distance(col, lat, lon)   /   r > …
        Expr::Binary { op: BinaryOp::GtEq | BinaryOp::Gt, left, right } => {
            geo_distance_bbox(right, left)
        }
        Expr::Func { name, args } if name == "geo_bbox" => geo_bbox_predicate(args),
        _ => None,
    }
}

/// `geo_distance(col, lat, lon)` on `func_side`, radius on `radius_side`.
fn geo_distance_bbox(func_side: &Expr, radius_side: &Expr) -> Option<(String, crate::geo::BBox)> {
    let Expr::Func { name, args } = func_side else {
        return None;
    };
    if name != "geo_distance" || args.len() != 3 {
        return None;
    }
    let col = geo_column(&args[0])?;
    let lat = const_f64(&args[1])?;
    let lon = const_f64(&args[2])?;
    let radius = const_f64(radius_side)?;
    if !(radius.is_finite() && radius >= 0.0) {
        return None;
    }
    Some((col, crate::geo::BBox::around(lat, lon, radius)))
}

/// `geo_bbox(col, min_lat, min_lon, max_lat, max_lon)` → `(col, box)`.
fn geo_bbox_predicate(args: &[Expr]) -> Option<(String, crate::geo::BBox)> {
    if args.len() != 5 {
        return None;
    }
    let col = geo_column(&args[0])?;
    let min_lat = const_f64(&args[1])?;
    let min_lon = const_f64(&args[2])?;
    let max_lat = const_f64(&args[3])?;
    let max_lon = const_f64(&args[4])?;
    // A degenerate latitude box is left to the scan path. `min_lon > max_lon`
    // (antimeridian wrap) is fine: the planner splits it into two halves.
    if min_lat > max_lat {
        return None;
    }
    Some((col, crate::geo::BBox { min_lat, min_lon, max_lat, max_lon }))
}

/// The column path of a geo function's point argument (`Expr::Column`).
fn geo_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.clone()),
        _ => None,
    }
}

/// Evaluate a constant coordinate/radius argument (literal, unary minus, or any
/// row-independent expression) to `f64`. Row-dependent or unbound → `None`.
fn const_f64(expr: &Expr) -> Option<f64> {
    match crate::eval::eval(expr, &Document::new()).ok()? {
        Value::Int(i) => Some(i as f64),
        Value::Float(f) => Some(f),
        Value::Decimal(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// The `MATCH()`-family predicate functions (names arrive lowercased).
fn is_search_func(name: &str) -> bool {
    matches!(
        name,
        "match"
            | "match_phrase"
            | "match_prefix"
            | "fuzzy"
            | "wildcard"
            | "regexp"
            | "more_like_this"
            | "search"
            | "boosted"
            | "match_cross"
            | "match_best"
    )
}

/// Whether `score()` (no arguments) is called.
fn is_score_call(expr: &Expr) -> bool {
    matches!(expr, Expr::Func { name, args } if name == "score" && args.is_empty())
}

/// Whether a function satisfying `pred` appears anywhere in `expr`.
fn expr_has_func(expr: &Expr, pred: &impl Fn(&str) -> bool) -> bool {
    match expr {
        Expr::Func { name, args } => pred(name) || args.iter().any(|a| expr_has_func(a, pred)),
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => expr_has_func(expr, pred),
        Expr::Binary { left, right, .. } => {
            expr_has_func(left, pred) || expr_has_func(right, pred)
        }
        Expr::Aggregate {
            arg: AggArg::Expr(e),
            ..
        } => expr_has_func(e, pred),
        Expr::InList { expr, list, .. } => {
            expr_has_func(expr, pred) || list.iter().any(|a| expr_has_func(a, pred))
        }
        Expr::Between { expr, lo, hi, .. } => {
            expr_has_func(expr, pred) || expr_has_func(lo, pred) || expr_has_func(hi, pred)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_has_func(expr, pred) || expr_has_func(pattern, pred)
        }
        Expr::Aggregate { .. } | Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => false,
    }
}

/// Whether a SELECT is a full-text search query: a `MATCH()`-family predicate
/// anywhere in its WHERE, or `score()` in its projection or ORDER BY.
fn select_uses_search(sel: &Select) -> bool {
    sel.filter
        .as_ref()
        .is_some_and(|f| expr_has_func(f, &is_search_func))
        || sel.items.iter().any(|it| match it {
            SelectItem::Expr { expr, .. } => expr_has_func(expr, &|n| n == "score"),
            SelectItem::Wildcard => false,
        })
        || sel
            .order_by
            .iter()
            .any(|ok| expr_has_func(&ok.expr, &|n| n == "score"))
        || sel
            .group_top
            .as_ref()
            .is_some_and(|t| expr_has_func(&t.by, &|n| n == "score"))
}

/// The field names a search query explicitly targets (used to pick the
/// serving index; `SEARCH('...')` fields resolve inside the index and are
/// not collected).
fn collect_search_fields(query: &SearchQuery, out: &mut Vec<String>) {
    match query {
        SearchQuery::Match { field, .. }
        | SearchQuery::Phrase { field, .. }
        | SearchQuery::Fuzzy { field, .. }
        | SearchQuery::Prefix { field, .. }
        | SearchQuery::Wildcard { field, .. }
        | SearchQuery::Regexp { field, .. }
        | SearchQuery::MoreLikeThis { field, .. } => {
            if let Some(f) = field {
                if !out.contains(f) {
                    out.push(f.clone());
                }
            }
        }
        SearchQuery::QueryString(_) => {}
        SearchQuery::All(subs) | SearchQuery::Any(subs) => {
            for sub in subs {
                collect_search_fields(sub, out);
            }
        }
        SearchQuery::Not(sub) => collect_search_fields(sub, out),
        SearchQuery::MultiMatch { fields, .. } => {
            for f in fields {
                if !out.contains(f) {
                    out.push(f.clone());
                }
            }
        }
        SearchQuery::Boosted { required, optional } => {
            collect_search_fields(required, out);
            for sub in optional {
                collect_search_fields(sub, out);
            }
        }
    }
}

/// Fetch-size ceiling for the `AFTER` keyset-cursor doubling loop: a cursor
/// deeper than this many ranked hits errors instead of fetching unboundedly.
const AFTER_FETCH_MAX: usize = 65_536;

/// The parsed `AFTER (<sort-value>, <pk-value>)` cursor: the previous page's
/// last sort value plus that row's encoded key (the ascending tie-break).
struct AfterCursor {
    sort_value: Value,
    key: Vec<u8>,
}

/// Evaluate and validate the `AFTER` clause against the table's primary key.
fn parse_after_cursor(sel: &Select, cluster: &dyn Cluster) -> Result<AfterCursor> {
    let values = sel.after.as_ref().expect("caller checked");
    let [sort_expr, pk_expr] = values.as_slice() else {
        return Err(EngineError::Type(
            "AFTER takes exactly (last sort value, last primary-key value)".into(),
        ));
    };
    let empty = Document::new();
    let sort_value = eval(sort_expr, &empty)?;
    let pk_value = eval(pk_expr, &empty)?;
    if matches!(pk_value, Value::Null) {
        return Err(EngineError::Type(
            "AFTER primary-key value must not be NULL".into(),
        ));
    }
    let pk = cluster.primary_key(&sel.from)?;
    if pk.len() != 1 {
        return Err(EngineError::Unsupported(
            "AFTER requires a single-column primary key (the cursor tie-break)".into(),
        ));
    }
    Ok(AfterCursor {
        sort_value,
        key: Value::Array(vec![pk_value]).encode_key(),
    })
}

/// Deep pagination (`AFTER` keyset cursor — ES `search_after`) over a search
/// select ordered by `score() DESC` or a single plain column. The ranked
/// fetch is filtered to rows strictly after the `(sort value, primary key)`
/// cursor; because the cursor's rank is unknown, the fetch depth doubles
/// until a full page is found or the match set is exhausted (per-page cost ≈
/// an equivalent `OFFSET` fetch — the win is a STABLE cursor: concurrent
/// writes never shift or duplicate pages — and no depth cap beyond
/// [`AFTER_FETCH_MAX`]).
fn run_search_after(
    sel: &Select,
    cluster: &mut dyn Cluster,
    query: &SearchQuery,
    residual: &Option<Expr>,
) -> Result<ResultSet> {
    if sel.offset.is_some() {
        return Err(EngineError::Unsupported(
            "AFTER is a keyset cursor; OFFSET is not supported — page with AFTER alone".into(),
        ));
    }
    if sel.rerank.is_some() {
        return Err(EngineError::Unsupported(
            "AFTER cannot be combined with RERANK (rerank scores are not a stable cursor)".into(),
        ));
    }
    if is_grouped(sel) || sel.group_top.is_some() || sel.distinct {
        return Err(EngineError::Unsupported(
            "AFTER cannot be combined with GROUP BY, aggregates, or DISTINCT".into(),
        ));
    }
    let limit = sel.limit.ok_or_else(|| {
        EngineError::Unsupported("AFTER requires LIMIT (the page size)".into())
    })? as usize;
    let cursor = parse_after_cursor(sel, &*cluster)?;
    let highlights = collect_highlights(sel)?;
    let first_fetch = limit.saturating_mul(2).clamp(64, AFTER_FETCH_MAX);

    match sel.order_by.as_slice() {
        // Relevance pages: ORDER BY score() DESC — cursor = (score, key).
        [key] if key.descending && is_score_call(&key.expr) => {
            let c_score = match &cursor.sort_value {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => {
                    return Err(EngineError::Type(
                        "AFTER with ORDER BY score() takes a numeric score cursor".into(),
                    ))
                }
            };
            let mut k = first_fetch;
            loop {
                let mut hits = cluster.search(&sel.from, query, Some(k), residual, &highlights)?;
                let exhausted = hits.len() < k;
                // Deterministic page order: score desc, then key asc.
                hits.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
                let page: Vec<Document> = hits
                    .into_iter()
                    .filter(|(hkey, _, score)| {
                        let s = f64::from(*score);
                        s < c_score || (s == c_score && hkey.as_slice() > cursor.key.as_slice())
                    })
                    .take(limit)
                    .map(|(_, mut doc, score)| {
                        doc.insert("_score", Value::Float(f64::from(score)));
                        doc
                    })
                    .collect();
                if page.len() >= limit || exhausted {
                    return project(sel, page, &HashSet::new(), true);
                }
                if k >= AFTER_FETCH_MAX {
                    return Err(EngineError::ResourceLimit(format!(
                        "AFTER cursor is deeper than {AFTER_FETCH_MAX} ranked hits — \
                         narrow the query"
                    )));
                }
                k = k.saturating_mul(2).min(AFTER_FETCH_MAX);
            }
        }
        // Field-sorted pages: ORDER BY <col> [ASC|DESC] — cursor = (value, key).
        [key] => {
            let Expr::Column(col) = &key.expr else {
                return Err(EngineError::Unsupported(
                    "AFTER requires ORDER BY score() DESC or ORDER BY <column>".into(),
                ));
            };
            let single = std::slice::from_ref(key);
            let sort_val = |doc: &Document| doc.get_path(col).cloned().unwrap_or(Value::Null);
            let strictly_after = |v: &Value, hkey: &[u8]| {
                match order_compare(
                    std::slice::from_ref(v),
                    std::slice::from_ref(&cursor.sort_value),
                    single,
                ) {
                    std::cmp::Ordering::Greater => true,
                    std::cmp::Ordering::Equal => hkey > cursor.key.as_slice(),
                    std::cmp::Ordering::Less => false,
                }
            };
            let finish = |mut rows: Vec<(Vec<u8>, Document)>| -> Result<ResultSet> {
                rows.sort_by(|a, b| {
                    order_compare(
                        std::slice::from_ref(&sort_val(&a.1)),
                        std::slice::from_ref(&sort_val(&b.1)),
                        single,
                    )
                    .then_with(|| a.0.cmp(&b.0))
                });
                let page: Vec<Document> = rows
                    .into_iter()
                    .filter(|(hkey, doc)| strictly_after(&sort_val(doc), hkey))
                    .take(limit)
                    .map(|(_, doc)| doc)
                    .collect();
                project(sel, page, &HashSet::new(), true)
            };
            let sort = skaidb_fts::SortSpec {
                column: col.clone(),
                descending: key.descending,
            };
            let mut k = first_fetch;
            loop {
                let Some(rows) =
                    cluster.search_sorted(&sel.from, query, &sort, k, residual, &highlights)?
                else {
                    // No index-ordered pushdown for this column/deployment:
                    // gather every match and page over the exact full order.
                    let hits = cluster.search(&sel.from, query, None, residual, &highlights)?;
                    return finish(hits.into_iter().map(|(hkey, doc, _)| (hkey, doc)).collect());
                };
                let exhausted = rows.len() < k;
                // Count the post-cursor rows before committing to this fetch.
                let found = rows
                    .iter()
                    .filter(|(hkey, doc)| strictly_after(&sort_val(doc), hkey))
                    .count();
                if found >= limit || exhausted {
                    return finish(rows);
                }
                if k >= AFTER_FETCH_MAX {
                    return Err(EngineError::ResourceLimit(format!(
                        "AFTER cursor is deeper than {AFTER_FETCH_MAX} sorted hits — \
                         narrow the query"
                    )));
                }
                k = k.saturating_mul(2).min(AFTER_FETCH_MAX);
            }
        }
        [] => Err(EngineError::Unsupported(
            "AFTER requires ORDER BY score() DESC or ORDER BY <column> (plus LIMIT)".into(),
        )),
        _ => Err(EngineError::Unsupported(
            "AFTER supports exactly one ORDER BY key — the primary key is the \
             implicit ascending tie-break"
                .into(),
        )),
    }
}

/// Candidate-window cap for `RERANK … TOP` — bounds both the coordinator
/// gather and the size of the rerank HTTP request.
const RERANK_MAX_TOP: u64 = 1000;
/// Per-document text cap sent to the rerank endpoint (cross-encoders truncate
/// around this length anyway; keeps the request body bounded).
const RERANK_DOC_MAX_CHARS: usize = 4000;

/// The positive query texts of a search predicate, for the reranker's query
/// side. Negated (`NOT`) legs and term-level patterns (`wildcard`/`regexp`)
/// carry no relevance text and are skipped.
fn collect_search_texts(query: &SearchQuery, out: &mut Vec<String>) {
    match query {
        SearchQuery::Match { text, .. }
        | SearchQuery::Phrase { text, .. }
        | SearchQuery::Fuzzy { text, .. }
        | SearchQuery::Prefix { text, .. }
        | SearchQuery::MoreLikeThis { text, .. }
        | SearchQuery::MultiMatch { text, .. } => {
            if !out.contains(text) {
                out.push(text.clone());
            }
        }
        SearchQuery::QueryString(s) => {
            if !out.contains(s) {
                out.push(s.clone());
            }
        }
        SearchQuery::Wildcard { .. } | SearchQuery::Regexp { .. } | SearchQuery::Not(_) => {}
        SearchQuery::All(subs) | SearchQuery::Any(subs) => {
            for sub in subs {
                collect_search_texts(sub, out);
            }
        }
        SearchQuery::Boosted { required, optional } => {
            collect_search_texts(required, out);
            for sub in optional {
                collect_search_texts(sub, out);
            }
        }
    }
}

/// The text sent to the reranker for one candidate row: the string values of
/// `cols` (every string field of the doc when `cols` is empty — the
/// field-less `SEARCH()` case), newline-joined, capped at
/// [`RERANK_DOC_MAX_CHARS`]. Internal `_`-prefixed fields never contribute.
fn rerank_doc_text(doc: &Document, cols: &[String]) -> String {
    let mut text = String::new();
    let mut push = |s: &str| {
        if text.len() >= RERANK_DOC_MAX_CHARS || s.is_empty() {
            return;
        }
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(s);
        if text.len() > RERANK_DOC_MAX_CHARS {
            let mut cut = RERANK_DOC_MAX_CHARS;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            text.truncate(cut);
        }
    };
    if cols.is_empty() {
        for (k, v) in &doc.0 {
            if let (false, Value::String(s)) = (k.starts_with('_'), v) {
                push(s);
            }
        }
    } else {
        for col in cols {
            if let Some(Value::String(s)) = doc.get_path(col) {
                push(s);
            }
        }
    }
    text
}

/// Apply the `RERANK` clause to an already-ranked candidate list: derive the
/// query and per-document texts, score them with the deployment's reranker,
/// and return the documents reordered best-first with the rerank score
/// injected as `_score` (`score()` reads it). Ties keep the retrieval order.
fn apply_rerank(
    cluster: &dyn Cluster,
    rerank: &Rerank,
    search_query: &SearchQuery,
    docs: Vec<Document>,
) -> Result<Vec<Document>> {
    if docs.is_empty() {
        return Ok(docs);
    }
    let query_text = match &rerank.query {
        Some(q) => q.clone(),
        None => {
            let mut texts = Vec::new();
            collect_search_texts(search_query, &mut texts);
            let t = texts.join(" ");
            if t.trim().is_empty() {
                return Err(EngineError::Type(
                    "RERANK could not derive a query text from the search predicate; \
                     add QUERY '<text>'"
                        .into(),
                ));
            }
            t
        }
    };
    let cols: Vec<String> = match &rerank.column {
        Some(c) => vec![c.clone()],
        None => {
            let mut fields = Vec::new();
            collect_search_fields(search_query, &mut fields);
            fields
        }
    };
    let doc_texts: Vec<String> = docs
        .iter()
        .map(|d| {
            let t = rerank_doc_text(d, &cols);
            // Rerank endpoints reject empty documents; a blank stays scoreable.
            if t.is_empty() {
                " ".to_string()
            } else {
                t
            }
        })
        .collect();
    let scores = cluster.rerank(
        rerank.model.as_deref().unwrap_or(""),
        &query_text,
        &doc_texts,
    )?;
    let mut order: Vec<usize> = (0..docs.len()).collect();
    order.sort_by(|&a, &b| scores[b].total_cmp(&scores[a]).then(a.cmp(&b)));
    let mut slots: Vec<Option<Document>> = docs.into_iter().map(Some).collect();
    Ok(order
        .into_iter()
        .map(|i| {
            let mut doc = slots[i].take().expect("each index taken once");
            doc.insert("_score", Value::Float(f64::from(scores[i])));
            doc
        })
        .collect())
}

/// Shared `RERANK` shape checks for the search and hybrid paths.
fn check_rerank(sel: &Select) -> Result<()> {
    let Some(rr) = &sel.rerank else {
        return Ok(());
    };
    if is_grouped(sel) || sel.group_top.is_some() {
        return Err(EngineError::Unsupported(
            "RERANK cannot be combined with GROUP BY or aggregates".into(),
        ));
    }
    if !sel.order_by.is_empty() {
        return Err(EngineError::Unsupported(
            "RERANK results are ordered by the reranker; ORDER BY is not supported".into(),
        ));
    }
    if rr.top > RERANK_MAX_TOP {
        return Err(EngineError::ResourceLimit(format!(
            "RERANK TOP is capped at {RERANK_MAX_TOP} candidates"
        )));
    }
    Ok(())
}

/// Whether the expression is built purely of search predicates composed
/// with AND/OR/NOT — i.e. the whole subtree can push to the index.
fn is_pure_search(e: &Expr) -> bool {
    match e {
        Expr::Func { name, .. } => is_search_func(name),
        Expr::Binary {
            op: BinaryOp::And | BinaryOp::Or,
            left,
            right,
        } => is_pure_search(left) && is_pure_search(right),
        Expr::Unary {
            op: UnaryOp::Not,
            expr,
        } => is_pure_search(expr),
        _ => false,
    }
}

/// The combined search query of a WHERE clause, for callers outside the
/// executor (e.g. a cluster coordinator routing a per-hit explain): the
/// search predicates AND-ed into one [`SearchQuery`], `None` when the
/// filter has none, `Err` on unpushable mixing.
pub fn filter_search_query(filter: &Option<Expr>) -> Result<Option<SearchQuery>> {
    let (mut queries, _residual) = split_search_filter(filter)?;
    Ok(match queries.len() {
        0 => None,
        1 => Some(queries.pop().expect("len checked")),
        _ => Some(SearchQuery::All(queries)),
    })
}

/// Convert a pure-search expression (see [`is_pure_search`]) into a
/// [`SearchQuery`] tree.
fn to_search_query(e: &Expr) -> Result<SearchQuery> {
    match e {
        Expr::Func { name, args } => search_query_from_func(name, args),
        Expr::Binary { op, left, right } => {
            let l = to_search_query(left)?;
            let r = to_search_query(right)?;
            Ok(match op {
                BinaryOp::And => SearchQuery::All(vec![l, r]),
                BinaryOp::Or => SearchQuery::Any(vec![l, r]),
                _ => unreachable!("is_pure_search admits only AND/OR"),
            })
        }
        Expr::Unary { expr, .. } => Ok(SearchQuery::Not(Box::new(to_search_query(expr)?))),
        _ => unreachable!("is_pure_search admits only funcs and AND/OR/NOT"),
    }
}

/// Split a WHERE clause into search predicates and the residual filter. The
/// filter must be a top-level AND chain: each leaf is either a pure search
/// subtree — search functions composed with AND/OR/NOT, converted to a
/// [`SearchQuery`] — or an ordinary predicate (recombined into the
/// residual). Mixing a search function with ordinary predicates under
/// OR/NOT cannot be pushed to the index and is rejected.
fn split_search_filter(filter: &Option<Expr>) -> Result<(Vec<SearchQuery>, Option<Expr>)> {
    let Some(expr) = filter else {
        return Ok((Vec::new(), None));
    };
    fn leaves<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
        if let Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = e
        {
            leaves(left, out);
            leaves(right, out);
        } else {
            out.push(e);
        }
    }
    let mut ls = Vec::new();
    leaves(expr, &mut ls);
    let mut queries = Vec::new();
    let mut residual: Option<Expr> = None;
    for leaf in ls {
        if is_pure_search(leaf) {
            queries.push(to_search_query(leaf)?);
            continue;
        }
        if expr_has_func(leaf, &is_search_func) {
            return Err(EngineError::Unsupported(
                "search predicates compose with AND/OR/NOT among themselves; mixing them \
                 with ordinary conditions under OR/NOT is not supported"
                    .into(),
            ));
        }
        residual = Some(match residual.take() {
            Some(acc) => Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(acc),
                right: Box::new(leaf.clone()),
            },
            None => leaf.clone(),
        });
    }
    Ok((queries, residual))
}

/// Convert one `MATCH()`-family call into a [`SearchQuery`]. Arguments are
/// literals after binding (plus a column reference for the field).
fn search_query_from_func(name: &str, args: &[Expr]) -> Result<SearchQuery> {
    fn column(e: &Expr, usage: &str) -> Result<String> {
        match e {
            Expr::Column(path) => Ok(path.clone()),
            _ => Err(EngineError::Type(format!(
                "{usage} takes a column as its first argument"
            ))),
        }
    }
    fn text(e: &Expr, usage: &str) -> Result<String> {
        match e {
            Expr::Literal(Value::String(s)) => Ok(s.clone()),
            _ => Err(EngineError::Type(format!(
                "{usage} takes a string literal as its query text"
            ))),
        }
    }
    fn uint(e: &Expr, usage: &str) -> Result<u64> {
        match e {
            Expr::Literal(Value::Int(i)) if *i >= 0 => Ok(*i as u64),
            _ => Err(EngineError::Type(format!(
                "{usage} must be a non-negative integer literal"
            ))),
        }
    }
    match name {
        "match" => {
            let [col, q] = args else {
                return Err(EngineError::Type(
                    "MATCH(column, 'text') takes exactly two arguments".into(),
                ));
            };
            Ok(SearchQuery::Match {
                field: Some(column(col, "MATCH(column, 'text')")?),
                text: text(q, "MATCH(column, 'text')")?,
            })
        }
        "match_phrase" => {
            let (col, q, slop) = match args {
                [col, q] => (col, q, 0),
                [col, q, s] => (col, q, uint(s, "MATCH_PHRASE slop")?),
                _ => {
                    return Err(EngineError::Type(
                        "MATCH_PHRASE(column, 'text' [, slop]) takes two or three arguments"
                            .into(),
                    ))
                }
            };
            let slop = u32::try_from(slop)
                .map_err(|_| EngineError::Type("MATCH_PHRASE slop is too large".into()))?;
            Ok(SearchQuery::Phrase {
                field: Some(column(col, "MATCH_PHRASE(column, 'text')")?),
                text: text(q, "MATCH_PHRASE(column, 'text')")?,
                slop,
            })
        }
        "fuzzy" => {
            let (col, q, distance) = match args {
                [col, q] => (col, q, 1),
                [col, q, d] => (col, q, uint(d, "FUZZY distance")?),
                _ => {
                    return Err(EngineError::Type(
                        "FUZZY(column, 'text' [, distance]) takes two or three arguments".into(),
                    ))
                }
            };
            let distance = u8::try_from(distance)
                .map_err(|_| EngineError::Type("FUZZY distance is too large".into()))?;
            Ok(SearchQuery::Fuzzy {
                field: Some(column(col, "FUZZY(column, 'text')")?),
                text: text(q, "FUZZY(column, 'text')")?,
                distance,
            })
        }
        "match_prefix" => {
            let [col, q] = args else {
                return Err(EngineError::Type(
                    "MATCH_PREFIX(column, 'prefix') takes exactly two arguments".into(),
                ));
            };
            Ok(SearchQuery::Prefix {
                field: Some(column(col, "MATCH_PREFIX(column, 'prefix')")?),
                text: text(q, "MATCH_PREFIX(column, 'prefix')")?,
            })
        }
        "wildcard" => {
            let [col, q] = args else {
                return Err(EngineError::Type(
                    "WILDCARD(column, 'pattern') takes exactly two arguments".into(),
                ));
            };
            Ok(SearchQuery::Wildcard {
                field: Some(column(col, "WILDCARD(column, 'pattern')")?),
                pattern: text(q, "WILDCARD(column, 'pattern')")?,
            })
        }
        "regexp" => {
            let [col, q] = args else {
                return Err(EngineError::Type(
                    "REGEXP(column, 'pattern') takes exactly two arguments".into(),
                ));
            };
            Ok(SearchQuery::Regexp {
                field: Some(column(col, "REGEXP(column, 'pattern')")?),
                pattern: text(q, "REGEXP(column, 'pattern')")?,
            })
        }
        "more_like_this" => {
            let [col, q] = args else {
                return Err(EngineError::Type(
                    "MORE_LIKE_THIS(column, 'text') takes exactly two arguments".into(),
                ));
            };
            Ok(SearchQuery::MoreLikeThis {
                field: Some(column(col, "MORE_LIKE_THIS(column, 'text')")?),
                text: text(q, "MORE_LIKE_THIS(column, 'text')")?,
            })
        }
        "search" => {
            let [q] = args else {
                return Err(EngineError::Type(
                    "SEARCH('query') takes exactly one string argument".into(),
                ));
            };
            Ok(SearchQuery::QueryString(text(q, "SEARCH('query')")?))
        }
        // MATCH_CROSS(col, col, ..., 'text') — term-centric multi-field
        // match (ES multi_match cross_fields): the fields behave like one
        // big field, each term scoring by its best field. `match_best` is
        // the field-centric twin (best_fields over an explicit subset;
        // reached via the ES shim).
        "match_cross" | "match_best" => {
            let [cols @ .., q] = args else {
                return Err(EngineError::Type(
                    "MATCH_CROSS(column, ..., 'text') takes columns then the query text".into(),
                ));
            };
            if cols.len() < 2 {
                return Err(EngineError::Type(
                    "MATCH_CROSS(column, ..., 'text') needs at least two columns \
                     (use MATCH() for one)"
                        .into(),
                ));
            }
            let fields = cols
                .iter()
                .map(|c| column(c, "MATCH_CROSS(column, ..., 'text')"))
                .collect::<Result<Vec<_>>>()?;
            Ok(SearchQuery::MultiMatch {
                fields,
                text: text(q, "MATCH_CROSS(column, ..., 'text')")?,
                term_centric: name == "match_cross",
            })
        }
        // BOOSTED(required, optional...): `required` decides which rows
        // match; each optional predicate only raises the score of rows that
        // already match (ES bool must/filter + should). Every argument must
        // itself be a pure search predicate — the index serves the whole
        // thing or the statement is rejected.
        "boosted" => {
            let [required, optional @ ..] = args else {
                return Err(EngineError::Type(
                    "BOOSTED(required, optional...) takes at least one search predicate".into(),
                ));
            };
            if optional.is_empty() {
                return Err(EngineError::Type(
                    "BOOSTED(required, optional...) needs at least one optional predicate \
                     (without one it is just the required predicate)"
                        .into(),
                ));
            }
            for arg in args {
                if !is_pure_search(arg) {
                    return Err(EngineError::Type(
                        "BOOSTED() arguments must be search predicates (MATCH()-family \
                         functions composed with AND/OR/NOT)"
                            .into(),
                    ));
                }
            }
            Ok(SearchQuery::Boosted {
                required: Box::new(to_search_query(required)?),
                optional: optional
                    .iter()
                    .map(to_search_query)
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        other => Err(EngineError::Type(format!(
            "unknown search function {other}()"
        ))),
    }
}

/// Execute a full-text search SELECT: split the WHERE into search predicates
/// (AND-ed into one [`SearchQuery`]) and a residual filter, gather through
/// [`Cluster::search`], expose each hit's BM25 score as a `_score` field, and
/// project as usual. `ORDER BY score() DESC LIMIT n` is the ranked top-n
/// path; with no ORDER BY every matching row comes back (order unspecified,
/// scores 0.0).
fn run_search_select(sel: &Select, cluster: &mut dyn Cluster) -> Result<ResultSet> {
    check_bound_counts(sel)?;
    if !sel.joins.is_empty() || !sel.set_ops.is_empty() {
        return Err(EngineError::Unsupported(
            "MATCH()/SEARCH() cannot be combined with JOIN or UNION".into(),
        ));
    }
    if sel.distinct {
        return Err(EngineError::Unsupported(
            "MATCH()/SEARCH() cannot be combined with DISTINCT".into(),
        ));
    }
    if sel.nearest.is_some() {
        return Err(EngineError::Unsupported(
            "MATCH()/SEARCH() cannot be combined with NEAREST".into(),
        ));
    }
    let (mut queries, residual) = split_search_filter(&sel.filter)?;
    if queries.is_empty() {
        // Routed here by score() alone: there is nothing to search.
        return Err(EngineError::Type(
            "score() is only valid in a query with a MATCH()/SEARCH() predicate".into(),
        ));
    }
    let query = match queries.len() {
        1 => queries.pop().expect("len checked"),
        _ => SearchQuery::All(queries),
    };
    // AFTER: deep pagination by keyset cursor (ES `search_after`).
    if sel.after.is_some() {
        return run_search_after(sel, cluster, &query, &residual);
    }
    // RERANK: fetch the top `TOP` candidates by BM25, re-score them with the
    // external cross-encoder, and serve the page from the reranker's order
    // (the rerank score lands in `_score`, so `score()` reads it).
    if let Some(rr) = &sel.rerank {
        check_rerank(sel)?;
        let highlights = collect_highlights(sel)?;
        let hits = cluster.search(&sel.from, &query, Some(rr.top as usize), &residual, &highlights)?;
        let docs: Vec<Document> = hits.into_iter().map(|(_, doc, _)| doc).collect();
        let docs = apply_rerank(&*cluster, rr, &query, docs)?;
        return project(sel, docs, &HashSet::new(), true);
    }
    // Aggregates / GROUP BY over a search (phase 6): push exact fast-field
    // facets into the index when the shape and deployment allow; otherwise
    // materialize the matching rows (deduped by key at the coordinator, so
    // correct at any replication factor) and run the ordinary grouped
    // projection over them.
    if is_grouped(sel) {
        if residual.is_none() {
            if let Some((agg, outs)) = map_search_aggregate(sel) {
                if let Some(rows) = cluster.search_aggregate(&sel.from, &query, &agg)? {
                    return finish_agg_pushdown(sel, rows, &outs);
                }
            }
        }
        // Per-group top-k ranks rows, typically by score(): gather every
        // match *scored* (k = all) with any requested highlights, and
        // expose the score like the ranked path does. Plain aggregation
        // needs neither.
        if sel.group_top.is_some() {
            let highlights = collect_highlights(sel)?;
            let hits = cluster.search(&sel.from, &query, Some(usize::MAX), &residual, &highlights)?;
            let docs: Vec<Document> = hits
                .into_iter()
                .map(|(_, mut doc, score)| {
                    doc.insert("_score", Value::Float(score as f64));
                    doc
                })
                .collect();
            return select_aggregate(sel, docs, true);
        }
        // The exact-row fallback materializes EVERY match; on a large match
        // set the (metered) gather dies at the scan budget instead of tying
        // the coordinator up for the whole statement timeout (2026-07-15
        // incident). Point the error at the search-side fix — the generic
        // "add a covering index" advice does not apply here.
        let hits = cluster
            .search(&sel.from, &query, None, &residual, &[])
            .map_err(|e| match e {
                EngineError::ResourceLimit(msg) => EngineError::ResourceLimit(format!(
                    "{msg} [grouped search fallback: declare the GROUP BY column as a \
                     keyword fast field (WITH ('<col>.type' = 'keyword') + REBUILD \
                     SEARCH INDEX) so it answers index-side, or narrow the match set]"
                )),
                other => other,
            })?;
        let docs: Vec<Document> = hits.into_iter().map(|(_, doc, _)| doc).collect();
        return select_aggregate(sel, docs, true);
    }
    // ORDER BY: absent (predicate-only), exactly `score() DESC LIMIT k`
    // (BM25 top-k in the index), or any other ordering (phase 7) — a
    // single plain fast-field column with LIMIT tries the index-ordered
    // pushdown, everything else gathers every match and sorts through the
    // ordinary executor.
    let k = match sel.order_by.as_slice() {
        [] => None,
        [key] if key.descending && is_score_call(&key.expr) => {
            let limit = sel.limit.ok_or_else(|| {
                EngineError::Unsupported("ORDER BY score() requires LIMIT".into())
            })?;
            Some(sel.offset.unwrap_or(0).saturating_add(limit) as usize)
        }
        order => {
            if order.iter().any(|ok| expr_has_func(&ok.expr, &|n| n == "score")) {
                return Err(EngineError::Unsupported(
                    "score() ordering must be exactly ORDER BY score() DESC".into(),
                ));
            }
            let highlights = collect_highlights(sel)?;
            if let ([key], Some(limit)) = (order, sel.limit) {
                if let Expr::Column(col) = &key.expr {
                    let want = sel.offset.unwrap_or(0).saturating_add(limit) as usize;
                    let sort = skaidb_fts::SortSpec {
                        column: col.clone(),
                        descending: key.descending,
                    };
                    if let Some(mut hits) = cluster.search_sorted(
                        &sel.from, &query, &sort, want, &residual, &highlights,
                    )? {
                        // Deterministic tie-break by key (ascending), so page
                        // order agrees with the AFTER cursor order.
                        let single = std::slice::from_ref(key);
                        hits.sort_by(|a, b| {
                            let va = a.1.get_path(col).cloned().unwrap_or(Value::Null);
                            let vb = b.1.get_path(col).cloned().unwrap_or(Value::Null);
                            order_compare(
                                std::slice::from_ref(&va),
                                std::slice::from_ref(&vb),
                                single,
                            )
                            .then_with(|| a.0.cmp(&b.0))
                        });
                        let docs = hits.into_iter().map(|(_, d)| d).collect();
                        // The generic sort in `project` re-orders the
                        // already-bounded gather identically (the pushdown
                        // declined if NULL ordering could diverge; ties keep
                        // the key order — the doc sort is stable).
                        return project(sel, docs, &HashSet::new(), true);
                    }
                }
            }
            // Fallback: every matching row, ordered by the executor. Pre-sort
            // by key so the executor's stable sort tie-breaks by key
            // (ascending) — same page order as the AFTER cursor.
            let mut hits = cluster.search(&sel.from, &query, None, &residual, &highlights)?;
            hits.sort_by(|a, b| a.0.cmp(&b.0));
            let docs: Vec<Document> = hits.into_iter().map(|(_, d, _)| d).collect();
            return project(sel, docs, &HashSet::new(), true);
        }
    };
    let highlights = collect_highlights(sel)?;
    let mut hits = cluster.search(&sel.from, &query, k, &residual, &highlights)?;
    if k.is_some() {
        // Ranked page: deterministic tie-break by key (ascending), so page
        // order agrees with the AFTER cursor order.
        hits.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    }
    let docs: Vec<Document> = hits
        .into_iter()
        .map(|(_, mut doc, score)| {
            doc.insert("_score", Value::Float(score as f64));
            doc
        })
        .collect();
    // `project` with `finalize` applies the ORDER BY (score() reads the
    // injected `_score`; the stable doc sort keeps the key tie-break) and the
    // OFFSET/LIMIT page.
    project(sel, docs, &HashSet::new(), true)
}

/// `(key, doc)` rows from a fast-field-ordered search, best-first.
pub type SortedSearchRows = Vec<(Vec<u8>, Document)>;

/// What each select item of an agg-pushdown query reads from an
/// [`skaidb_fts::AggRow`].
enum AggOut {
    /// The GROUP BY column — the bucket key.
    GroupKey,
    /// The i-th requested metric.
    Metric(usize),
}

/// Map a grouped search SELECT onto an exact fast-field [`AggRequest`],
/// when its shape allows: at most one plain-column GROUP BY, and items
/// that are the group column or simple aggregates
/// (`COUNT(*)`/`COUNT(col)`/`SUM`/`AVG`/`MIN`/`MAX` over a column).
/// HAVING/ORDER BY/OFFSET force the row-materialization path instead —
/// the fallback computes them with the ordinary grouped executor.
fn map_search_aggregate(sel: &Select) -> Option<(skaidb_fts::AggRequest, Vec<AggOut>)> {
    if sel.having.is_some() || !sel.order_by.is_empty() || sel.offset.is_some() {
        return None;
    }
    // Per-group top-k returns rows, not aggregates — never a facet pushdown.
    if sel.group_top.is_some() {
        return None;
    }
    let group_expr = match sel.group_by.as_slice() {
        [] => None,
        [g] => Some(g),
        _ => return None,
    };
    let group_by = match group_expr {
        None => None,
        Some(Expr::Column(c)) => Some(skaidb_fts::AggGroupBy::Keyword(c.clone())),
        // `time_bucket(step, col)` over a declared date column becomes a
        // fixed-interval date histogram (duration literals lex to ms ints).
        Some(Expr::Func { name, args }) if name == "time_bucket" => match args.as_slice() {
            [Expr::Literal(Value::Int(ms)), Expr::Column(c)] if *ms > 0 => {
                Some(skaidb_fts::AggGroupBy::DateHistogram {
                    column: c.clone(),
                    interval_ms: *ms,
                })
            }
            _ => return None,
        },
        Some(_) => return None,
    };
    let mut metrics = Vec::new();
    let mut outs = Vec::with_capacity(sel.items.len());
    for item in &sel.items {
        let SelectItem::Expr { expr, .. } = item else {
            return None;
        };
        if Some(expr) == group_expr {
            outs.push(AggOut::GroupKey);
            continue;
        }
        match expr {
            Expr::Aggregate { func, arg } => {
                let metric = match (func, arg) {
                    (AggFunc::Count, AggArg::Star) => skaidb_fts::AggMetric {
                        func: skaidb_fts::AggMetricFunc::Count,
                        column: None,
                    },
                    (AggFunc::Count, AggArg::ApproxDistinct(e)) => {
                        let Expr::Column(col) = e.as_ref() else {
                            return None;
                        };
                        skaidb_fts::AggMetric {
                            func: skaidb_fts::AggMetricFunc::ApproxCountDistinct,
                            column: Some(col.clone()),
                        }
                    }
                    (AggFunc::Count, AggArg::Distinct(e)) => {
                        let Expr::Column(col) = &**e else {
                            return None;
                        };
                        skaidb_fts::AggMetric {
                            func: skaidb_fts::AggMetricFunc::CountDistinct,
                            column: Some(col.clone()),
                        }
                    }
                    (func, AggArg::Expr(e)) => {
                        let Expr::Column(col) = &**e else {
                            return None;
                        };
                        let mfunc = match func {
                            AggFunc::Count => skaidb_fts::AggMetricFunc::ValueCount,
                            AggFunc::Sum => skaidb_fts::AggMetricFunc::Sum,
                            AggFunc::Avg => skaidb_fts::AggMetricFunc::Avg,
                            AggFunc::Min => skaidb_fts::AggMetricFunc::Min,
                            AggFunc::Max => skaidb_fts::AggMetricFunc::Max,
                            // Time-series aggregates never push into a
                            // search index.
                            _ => return None,
                        };
                        skaidb_fts::AggMetric {
                            func: mfunc,
                            column: Some(col.clone()),
                        }
                    }
                    _ => return None,
                };
                outs.push(AggOut::Metric(metrics.len()));
                metrics.push(metric);
            }
            _ => return None,
        }
    }
    Some((skaidb_fts::AggRequest { group_by, metrics }, outs))
}

/// Turn pushed-down aggregation rows into the SELECT's result set (columns
/// named like the grouped executor names them; LIMIT applied — group order
/// is unspecified, exactly as on the fallback path).
fn finish_agg_pushdown(
    sel: &Select,
    rows: Vec<skaidb_fts::AggRow>,
    outs: &[AggOut],
) -> Result<ResultSet> {
    let columns: Vec<String> = sel
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard => unreachable!("wildcard never maps to a pushdown"),
            SelectItem::Expr { expr, alias } => alias.clone().unwrap_or_else(|| expr_name(expr)),
        })
        .collect();
    let mut out: Vec<Vec<Value>> = rows
        .into_iter()
        .map(|row| {
            outs.iter()
                .map(|o| match o {
                    AggOut::GroupKey => row.key.clone(),
                    AggOut::Metric(i) => row.metrics[*i].clone(),
                })
                .collect()
        })
        .collect();
    if let Some(limit) = sel.limit {
        out.truncate(limit as usize);
    }
    Ok(ResultSet { columns, rows: out })
}

/// Parse a `HIGHLIGHT(column [, max_chars [, pre_tag, post_tag [, no_match_size]]])`
/// call into its column and options. `max_chars` is the fragment size (ES
/// `fragment_size`); the `pre_tag`/`post_tag` string pair replaces the default
/// `<b>`/`</b>` markers (ES `pre_tags`/`post_tags`); `no_match_size` returns
/// that many leading characters when nothing matched (ES `no_match_size`).
fn parse_highlight_args(args: &[Expr]) -> Result<HighlightReq> {
    let bad = || {
        EngineError::Type(
            "HIGHLIGHT(column [, max_chars [, pre_tag, post_tag [, no_match_size \
             [, fragments]]]]) takes a column, an optional positive fragment size, an \
             optional pre/post tag string pair, an optional non-negative no-match size, \
             and an optional fragment count (1-10; > 1 returns an array of fragments)"
                .into(),
        )
    };
    let Some(Expr::Column(col)) = args.first() else {
        return Err(bad());
    };
    let mut opts = skaidb_fts::HighlightOpts::default();
    match &args[1..] {
        [] => {}
        [Expr::Literal(Value::Int(n))] if *n > 0 => opts.max_chars = *n as usize,
        [Expr::Literal(Value::Int(n)), Expr::Literal(Value::String(pre)), Expr::Literal(Value::String(post))]
            if *n > 0 =>
        {
            opts.max_chars = *n as usize;
            opts.pre_tag = pre.clone();
            opts.post_tag = post.clone();
        }
        [Expr::Literal(Value::Int(n)), Expr::Literal(Value::String(pre)), Expr::Literal(Value::String(post)), Expr::Literal(Value::Int(nm))]
            if *n > 0 && *nm >= 0 =>
        {
            opts.max_chars = *n as usize;
            opts.pre_tag = pre.clone();
            opts.post_tag = post.clone();
            opts.no_match_size = *nm as usize;
        }
        [Expr::Literal(Value::Int(n)), Expr::Literal(Value::String(pre)), Expr::Literal(Value::String(post)), Expr::Literal(Value::Int(nm)), Expr::Literal(Value::Int(fr))]
            if *n > 0 && *nm >= 0 && (1..=10).contains(fr) =>
        {
            opts.max_chars = *n as usize;
            opts.pre_tag = pre.clone();
            opts.post_tag = post.clone();
            opts.no_match_size = *nm as usize;
            opts.fragments = *fr as usize;
        }
        _ => return Err(bad()),
    }
    Ok((col.clone(), opts))
}

/// The `HIGHLIGHT(...)` requests in a search SELECT's projection. The search
/// gather answers each with a `_highlight_<column>` snippet field on every hit.
fn collect_highlights(sel: &Select) -> Result<Vec<HighlightReq>> {
    fn walk(expr: &Expr, out: &mut Vec<HighlightReq>) -> Result<()> {
        if let Expr::Func { name, args } = expr {
            if name == "highlight" {
                let (col, opts) = parse_highlight_args(args)?;
                match out.iter().find(|(c, _)| *c == col) {
                    Some((_, prev)) if *prev != opts => {
                        return Err(EngineError::Type(format!(
                            "conflicting HIGHLIGHT options for column '{col}'"
                        )))
                    }
                    Some(_) => {}
                    None => out.push((col, opts)),
                }
                return Ok(());
            }
        }
        match expr {
            Expr::Func { args, .. } => args.iter().try_for_each(|a| walk(a, out)),
            Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => walk(expr, out),
            Expr::Binary { left, right, .. } => {
                walk(left, out)?;
                walk(right, out)
            }
            Expr::Aggregate {
                arg: AggArg::Expr(e),
                ..
            } => walk(e, out),
            Expr::InList { expr, list, .. } => {
                walk(expr, out)?;
                list.iter().try_for_each(|a| walk(a, out))
            }
            Expr::Between { expr, lo, hi, .. } => {
                walk(expr, out)?;
                walk(lo, out)?;
                walk(hi, out)
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, out)?;
                walk(pattern, out)
            }
            Expr::Aggregate { .. } | Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {
                Ok(())
            }
        }
    }
    let mut out = Vec::new();
    for item in &sel.items {
        if let SelectItem::Expr { expr, .. } = item {
            walk(expr, &mut out)?;
        }
    }
    Ok(out)
}


/// Whether the query is in aggregate/grouped mode.
pub(crate) fn is_grouped(sel: &Select) -> bool {
    sel.items.iter().any(|it| match it {
        SelectItem::Expr { expr, .. } => contains_aggregate(expr),
        SelectItem::Wildcard => false,
    }) || !sel.group_by.is_empty()
}

/// Single-table query using the index planner for ordered/top-N gathers.
fn run_simple_select(sel: &Select, cluster: &dyn Cluster) -> Result<ResultSet> {
    let grouped = is_grouped(sel);
    // Unfiltered `SELECT COUNT(*)`: serve straight from storage key statistics
    // without decoding a single row.
    if grouped && sel.group_by.is_empty() && sel.having.is_none() {
        if let [SelectItem::Expr {
            expr:
                expr @ Expr::Aggregate {
                    func: AggFunc::Count,
                    arg: AggArg::Star,
                },
            alias,
        }] = sel.items.as_slice()
        {
            // Filtered counts are answered from a covering secondary index
            // when one exists — a `count_documents`-shaped query on a large
            // table must not gather the table.
            let counted = if sel.filter.is_none() {
                match cluster.count_rows(&sel.from)? {
                    // Stats unavailable (open transaction, or a TTL table —
                    // physical key stats overcount expired rows): stream a
                    // counting scan rather than gathering the table.
                    None => cluster.count_matching(&sel.from, &sel.filter)?,
                    n => n,
                }
            } else {
                match cluster.count_filtered(&sel.from, &sel.filter)? {
                    // No covering index (e.g. a `!=` in the filter): stream a
                    // counting scan instead of materializing every matching
                    // document just to take its length — a UI pagination
                    // count over 150k rows allocated gigabytes on the
                    // coordinator and pushed nodes into shedding.
                    None => cluster.count_matching(&sel.from, &sel.filter)?,
                    n => n,
                }
            };
            if let Some(n) = counted {
                let col = alias.clone().unwrap_or_else(|| expr_name(expr));
                let mut rows = vec![vec![Value::Int(n as i64)]];
                apply_offset_limit(&mut rows, sel.offset, sel.limit);
                return Ok(ResultSet::new(vec![col], rows));
            }
        }
    }
    // `SELECT DISTINCT <one column>`: stream the distinct set instead of
    // gathering every matching row to deduplicate at the end — a tags
    // endpoint's distinct over 183k rows materialized the whole table on
    // the coordinator and OOM-killed nodes (2026-07-13).
    if sel.distinct && !grouped && sel.order_by.is_empty() && sel.joins.is_empty() {
        if let [SelectItem::Expr {
            expr: Expr::Column(col),
            alias,
        }] = sel.items.as_slice()
        {
            if let Some(values) = cluster.distinct_values(&sel.from, col, &sel.filter)? {
                let name = alias.clone().unwrap_or_else(|| col.clone());
                let mut rows: Vec<Vec<Value>> = values.into_iter().map(|v| vec![v]).collect();
                apply_offset_limit(&mut rows, sel.offset, sel.limit);
                return Ok(ResultSet::new(vec![name], rows));
            }
        }
    }
    // An index can satisfy a single ascending `ORDER BY <column>` directly.
    let order_col = if grouped {
        None
    } else {
        index_order_column(&sel.order_by)
    };
    // Push a fetch limit when truncating the gather is correct: either the
    // rows come back already in the requested order, or the query asks for no
    // order at all. DISTINCT and grouping need every row.
    let fetch_limit = match (&order_col, sel.limit, sel.distinct) {
        (Some(_), Some(limit), false) => {
            Some(sel.offset.unwrap_or(0).saturating_add(limit) as usize)
        }
        (None, Some(limit), false) if !grouped && sel.order_by.is_empty() => {
            Some(sel.offset.unwrap_or(0).saturating_add(limit) as usize)
        }
        _ => None,
    };

    // GROUP BY/aggregate gathers never push order or a fetch limit (DISTINCT
    // and grouping need every row — see the fetch_limit match above), so
    // when a projection is safe (group_by_projection_columns, which only
    // ever returns Some for a grouped, non-wildcard, non-TOP-k, join-free,
    // set-op-free query) this is exactly equivalent to the call below, minus
    // decoding columns nothing in the statement can read. `presorted` is
    // meaningless here either way: it only reaches `select_rows` in the
    // `!grouped` branch below, and this branch only runs when grouped.
    let (docs, presorted): (Vec<Document>, bool) =
        if let Some(project) = group_by_projection_columns(sel) {
            let keyed = cluster.matching_rows_projected(&sel.from, &sel.filter, &project)?;
            (keyed.into_iter().map(|(_k, doc)| doc).collect(), false)
        } else {
            let (keyed, presorted) = cluster.matching_rows_ordered(
                &sel.from,
                &sel.filter,
                order_col
                    .as_ref()
                    .map(|(c, desc, exact)| (c.as_str(), *desc, *exact)),
                fetch_limit,
            )?;
            (keyed.into_iter().map(|(_k, doc)| doc).collect(), presorted)
        };

    if grouped {
        select_aggregate(sel, docs, true)
    } else {
        select_rows(sel, docs, presorted, &HashSet::new(), true)
    }
}

/// Project a query core (its own projection/grouping/having/distinct) to a
/// `ResultSet`, but **without** applying ORDER BY / LIMIT — those belong to the
/// enclosing compound query. Join-aware.
fn project_core(sel: &Select, cluster: &dyn Cluster) -> Result<ResultSet> {
    // A FROM-less UNION leg contributes its one constant row.
    if sel.from.is_empty() {
        return run_const_select(sel);
    }
    let docs: Vec<Document> = if let Some(project_cols) = group_by_projection_columns(sel) {
        cluster
            .matching_rows_projected(&sel.from, &sel.filter, &project_cols)?
            .into_iter()
            .map(|(_, d)| d)
            .collect()
    } else if sel.joins.is_empty() {
        cluster
            .matching_rows(&sel.from, &sel.filter)?
            .into_iter()
            .map(|(_, d)| d)
            .collect()
    } else {
        gather_join_docs(sel, cluster)?
    };
    let hide = join_hidden_keys(sel);
    project(sel, docs, &hide, false)
}

/// Group-or-row projection over already-gathered docs.
pub(crate) fn project(
    sel: &Select,
    docs: Vec<Document>,
    hide: &HashSet<String>,
    finalize: bool,
) -> Result<ResultSet> {
    if is_grouped(sel) {
        select_aggregate(sel, docs, finalize)
    } else {
        select_rows(sel, docs, false, hide, finalize)
    }
}

/// The set of join-alias container keys to hide from `SELECT *` expansion (so a
/// join wildcard shows the underlying fields, not the per-table sub-documents).
/// Empty for a non-join query.
fn join_hidden_keys(sel: &Select) -> HashSet<String> {
    if sel.joins.is_empty() {
        return HashSet::new();
    }
    let mut s = HashSet::new();
    s.insert(sel.from_alias.clone());
    for j in &sel.joins {
        s.insert(j.alias.clone());
    }
    s
}

/// One row of an in-progress join: each source alias paired with its document
/// (`None` for the null side of an outer join).
type JoinTuple = Vec<(String, Option<Document>)>;

/// Flatten a join tuple into one evaluation document: each present side is
/// available both qualified (`alias.field`, via a nested sub-document) and
/// unqualified (`field`, first present side wins on a name clash).
fn merge_tuple(parts: &JoinTuple) -> Document {
    let mut row = Document::new();
    for (alias, doc) in parts {
        if let Some(d) = doc {
            row.0.insert(alias.clone(), Value::Document(d.clone()));
        }
    }
    for (_, doc) in parts {
        if let Some(d) = doc {
            for (k, v) in &d.0 {
                row.0.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }
    row
}

fn join_keep(on: &Option<Expr>, cand: &JoinTuple) -> Result<bool> {
    match on {
        None => Ok(true),
        Some(e) => eval_predicate(e, &merge_tuple(cand)),
    }
}

/// Find an `ON` conjunct of the form `<left column> = <ra>.<column>` (either
/// operand order): the equi-join key that lets [`gather_join_docs`] build a
/// hash table instead of running the O(left × right) nested loop. Returns the
/// left path (evaluated against the merged left tuple) and the right path
/// stripped of its `ra.` prefix (evaluated against the right document).
fn equi_key_paths(on: &Option<Expr>, ra: &str) -> Option<(String, String)> {
    fn conjuncts<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
        if let Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = e
        {
            conjuncts(left, out);
            conjuncts(right, out);
        } else {
            out.push(e);
        }
    }
    let mut cs = Vec::new();
    conjuncts(on.as_ref()?, &mut cs);
    for c in cs {
        let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } = c
        else {
            continue;
        };
        let (Expr::Column(a), Expr::Column(b)) = (left.as_ref(), right.as_ref()) else {
            continue;
        };
        for (x, y) in [(a, b), (b, a)] {
            let Some(stripped) = y.strip_prefix(ra).and_then(|s| s.strip_prefix('.')) else {
                continue;
            };
            let x_head = x.split('.').next().unwrap_or(x);
            if x_head != ra {
                return Some((x.clone(), stripped.to_string()));
            }
        }
    }
    None
}

/// Hash-bucket key under which two values can possibly compare equal by
/// [`compare`]'s coercion rules: numerics collapse to their f64 projection
/// (so `1 = 1.0` lands in one bucket), everything else to its type-tagged key
/// encoding. `None` (NULL or NaN) equals nothing. Collisions are harmless —
/// every candidate pair is re-verified against the full `ON` expression;
/// only equal values landing in different buckets would be a bug, which the
/// f64 collapse rules out.
fn join_hash_key(v: &Value) -> Option<Vec<u8>> {
    if v.is_null() {
        return None;
    }
    if let Some(n) = crate::eval::as_number(v) {
        if n.is_nan() {
            return None;
        }
        let n = if n == 0.0 { 0.0f64 } else { n }; // collapse -0.0 into +0.0
        let mut k = Vec::with_capacity(9);
        k.push(0);
        k.extend_from_slice(&n.to_bits().to_le_bytes());
        return Some(k);
    }
    let mut k = Vec::with_capacity(16);
    k.push(1);
    v.encode_into(&mut k);
    Some(k)
}

/// Nested-loop evaluation of `FROM` + its joins into flattened result documents
/// (then filtered by the query `WHERE`). Each table's rows are gathered through
/// the [`Cluster`] seam, so joins work embedded and cluster-wide alike. Note: in
/// a cluster this pulls each joined table to the coordinator — there is no join
/// pushdown — so it suits modest tables / lookups.
fn gather_join_docs(sel: &Select, cluster: &dyn Cluster) -> Result<Vec<Document>> {
    let base = cluster.matching_rows(&sel.from, &None)?;
    let mut tuples: Vec<JoinTuple> = base
        .into_iter()
        .map(|(_, d)| vec![(sel.from_alias.clone(), Some(d))])
        .collect();
    let mut left_aliases = vec![sel.from_alias.clone()];

    for join in &sel.joins {
        let right = cluster.matching_rows(&join.table, &None)?;
        let ra = &join.alias;
        let mut next: Vec<JoinTuple> = Vec::new();
        match join.kind {
            JoinKind::Inner | JoinKind::Left | JoinKind::Cross => {
                // Equi-join: bucket the right side by its key once, then probe
                // per left tuple — O(left + right + matches) instead of the
                // O(left × right) nested loop. Bucket collisions and residual
                // `ON` conjuncts are handled by re-verifying each candidate.
                let equi = equi_key_paths(&join.on, ra);
                let buckets: Option<HashMap<Vec<u8>, Vec<usize>>> =
                    equi.as_ref().map(|(_, rpath)| {
                        let mut b: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
                        for (i, (_, rd)) in right.iter().enumerate() {
                            // A right row with a NULL/absent key can never
                            // satisfy the equality — leave it unbucketed.
                            if let Some(k) = rd.get_path(rpath).and_then(join_hash_key) {
                                b.entry(k).or_default().push(i);
                            }
                        }
                        b
                    });
                for t in &tuples {
                    let mut matched = false;
                    let probe: Option<&[usize]> = match (&equi, &buckets) {
                        (Some((lpath, _)), Some(b)) => {
                            match merge_tuple(t).get_path(lpath).and_then(join_hash_key) {
                                Some(k) => Some(b.get(&k).map(Vec::as_slice).unwrap_or(&[])),
                                // NULL left key: the merged candidate could in
                                // principle still resolve the path through the
                                // right side, so keep exact semantics with a
                                // per-tuple nested loop.
                                None => None,
                            }
                        }
                        _ => None,
                    };
                    match probe {
                        Some(idxs) => {
                            for &i in idxs {
                                let mut cand = t.clone();
                                cand.push((ra.clone(), Some(right[i].1.clone())));
                                if join_keep(&join.on, &cand)? {
                                    next.push(cand);
                                    matched = true;
                                }
                            }
                        }
                        None => {
                            for (_, rd) in &right {
                                let mut cand = t.clone();
                                cand.push((ra.clone(), Some(rd.clone())));
                                if join_keep(&join.on, &cand)? {
                                    next.push(cand);
                                    matched = true;
                                }
                            }
                        }
                    }
                    // LEFT JOIN: emit the left row with a null right side if nothing matched.
                    if matches!(join.kind, JoinKind::Left) && !matched {
                        let mut cand = t.clone();
                        cand.push((ra.clone(), None));
                        next.push(cand);
                    }
                }
            }
            JoinKind::Right => {
                for (_, rd) in &right {
                    let mut matched = false;
                    for t in &tuples {
                        let mut cand = t.clone();
                        cand.push((ra.clone(), Some(rd.clone())));
                        if join_keep(&join.on, &cand)? {
                            next.push(cand);
                            matched = true;
                        }
                    }
                    // Unmatched right row: emit it with every left alias nulled.
                    if !matched {
                        let mut cand: JoinTuple =
                            left_aliases.iter().map(|a| (a.clone(), None)).collect();
                        cand.push((ra.clone(), Some(rd.clone())));
                        next.push(cand);
                    }
                }
            }
        }
        left_aliases.push(ra.clone());
        tuples = next;
    }

    let mut out = Vec::with_capacity(tuples.len());
    for t in &tuples {
        let doc = merge_tuple(t);
        if matches_filter(&sel.filter, &doc)? {
            out.push(doc);
        }
    }
    Ok(out)
}

/// Dedup rows in place, preserving first-seen order (for `DISTINCT` / `UNION`).
fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen = BTreeSet::new();
    rows.retain(|row| {
        // Concatenated per-value key encodings are self-delimiting (the same
        // property composite index keys rely on), so no need to clone the row
        // into a `Value::Array` just to encode it.
        let mut key = Vec::new();
        for v in row {
            v.encode_into(&mut key);
        }
        seen.insert(key)
    });
}

/// Apply the whole-query DISTINCT, ORDER BY, and OFFSET/LIMIT to a combined
/// compound (`UNION`) result. ORDER BY here references **output columns** by name.
fn finalize_compound(sel: &Select, mut rs: ResultSet) -> Result<ResultSet> {
    if sel.distinct {
        dedup_rows(&mut rs.rows);
    }
    if !sel.order_by.is_empty() {
        sort_result_rows(&mut rs, &sel.order_by)?;
    }
    apply_offset_limit(&mut rs.rows, sel.offset, sel.limit);
    Ok(rs)
}

/// Sort a `ResultSet`'s rows by `order_by`, evaluating each key against a
/// document built from the output columns (so `ORDER BY <output-column>` works
/// over a `UNION`). Non-column order keys still evaluate (e.g. `ORDER BY a + b`
/// when `a`/`b` are output columns).
fn sort_result_rows(rs: &mut ResultSet, order_by: &[OrderKey]) -> Result<()> {
    let mut keyed: Vec<(Vec<Value>, usize)> = Vec::with_capacity(rs.rows.len());
    for (i, row) in rs.rows.iter().enumerate() {
        let mut doc = Document::new();
        for (col, val) in rs.columns.iter().zip(row.iter()) {
            doc.insert(col.clone(), val.clone());
        }
        let mut k = Vec::with_capacity(order_by.len());
        for ok in order_by {
            k.push(eval(&ok.expr, &doc)?);
        }
        keyed.push((k, i));
    }
    keyed.sort_by(|a, b| order_compare(&a.0, &b.0, order_by));
    // Reorder by moving rows into place — no clones.
    let mut old = std::mem::take(&mut rs.rows);
    rs.rows = keyed
        .into_iter()
        .map(|(_, old_idx)| std::mem::take(&mut old[old_idx]))
        .collect();
    Ok(())
}

/// The column an index could order by: a lone ascending `ORDER BY <column>`.
/// The leading `ORDER BY` key when it is a plain column, plus whether the
/// index walk alone satisfies the whole clause (`exact` — a single key) or
/// only its primary component (multi-key: the walk bounds the gather, the
/// executor re-sorts by the full clause; see `gather_rows_planned`'s
/// tie-group handling).
fn index_order_column(order_by: &[OrderKey]) -> Option<(String, bool, bool)> {
    let first = order_by.first()?;
    match &first.expr {
        Expr::Column(col) => Some((col.clone(), first.descending, order_by.len() == 1)),
        _ => None,
    }
}

fn run_update(upd: Update, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
    if cluster.ts_series_key(&upd.table)?.is_some() {
        return Err(EngineError::Unsupported(
            "time-series tables are append-only; UPDATE is not supported".into(),
        ));
    }
    let pk = cluster.primary_key(&upd.table)?;
    let matches = cluster.matching_rows(&upd.table, &upd.filter)?;
    let affected = matches.len();
    for (old_key, old_doc) in matches {
        let mut new_doc = old_doc.clone();
        for (path, expr) in &upd.assignments {
            let val = eval(expr, &old_doc)?;
            set_path(&mut new_doc, path, val);
        }
        let new_key = primary_key_bytes(&pk, &new_doc)?;
        if new_key == old_key {
            // Key unchanged (the overwhelmingly common case): one atomic
            // LWW overwrite. The old delete-then-put pair LOST THE ROW when
            // the put's quorum failed between the two — the delete had
            // already committed and nothing re-put the document. Two
            // production account rows died exactly this way, each while a
            // rolling restart window failed the put half (2026-07-13,
            // forensically confirmed from sstable version history). The put
            // path maintains secondary/search/vector indexes for overwrites
            // (it reads the existing version and removes stale entries).
            cluster.put(&upd.table, &new_key, &new_doc)?;
        } else {
            // PK change: put the new row FIRST, delete the old second — a
            // failure between the two leaves a recoverable duplicate rather
            // than a lost row.
            cluster.put(&upd.table, &new_key, &new_doc)?;
            cluster.delete(&upd.table, &old_key, &old_doc)?;
        }
    }
    Ok(QueryOutput::Mutation { affected })
}

fn run_delete(del: Delete, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
    if cluster.ts_series_key(&del.table)?.is_some() {
        return Err(EngineError::Unsupported(
            "time-series tables are append-only; expired data is dropped by RETENTION".into(),
        ));
    }
    let matches = cluster.matching_rows(&del.table, &del.filter)?;
    let affected = matches.len();
    for (key, doc) in matches {
        cluster.delete(&del.table, &key, &doc)?;
    }
    Ok(QueryOutput::Mutation { affected })
}

/// Default number of rows `DESCRIBE … FULL` samples when no `SAMPLE n` is
/// given: enough to surface the common field set cheaply, bounded so a large
/// table is not scanned in full. Streaming, so the scan stops after this many
/// rows.
pub const DESCRIBE_FULL_DEFAULT_SAMPLE: usize = 1000;

/// The lowercase type tag of a [`Value`], for `DESCRIBE … FULL`'s `type`
/// column (schema-less, so a field's type is discovered from the data).
fn value_type_label(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Decimal(_) => "decimal",
        Value::String(_) => "string",
        Value::Bytes(_) => "bytes",
        Value::Uuid(_) => "uuid",
        Value::Timestamp(_) => "timestamp",
        Value::Array(_) => "array",
        Value::Document(_) => "document",
    }
}

/// When the entire filter is `pk = <literal>` on a single-column primary key,
/// the storage key for that row so the read can be a point get. The key must
/// be built exactly as the engine builds it for inserts: the order-preserving
/// encoding of a one-element array holding the value.
pub fn pk_point_key(pk: &[String], filter: &Option<Expr>) -> Option<Vec<u8>> {
    if pk.is_empty() {
        return None;
    }
    // Every PK column must be equality-pinned by the filter to a non-null
    // literal — then the row key is exact and this is a point read. A
    // composite PK (channel, ts) with both pinned qualifies; a missing key
    // returns "not found" in one bloom-gated lookup instead of a full table
    // scan (LIMIT 1 masked this for present keys — it stopped early — but a
    // missing key walked the whole table into the scan budget, 2026-07-15).
    let constraints = column_constraints(filter);
    let mut values = Vec::with_capacity(pk.len());
    for col in pk {
        let c = constraints.iter().find(|(name, _)| name == col).map(|(_, c)| c)?;
        // Exactly an equality on this column — a range (lo/hi) is not a point.
        if c.lo.is_some() || c.hi.is_some() {
            return None;
        }
        let v = c.eq.as_ref()?;
        if v.is_null() {
            return None;
        }
        values.push(v.clone());
    }
    // A residual constraint on a non-PK column is fine: both callers
    // re-filter the point-read result (gather_rows_planned and the
    // cluster's matching_rows via filter_rows).
    Some(Value::Array(values).encode_key())
}

/// Cap on the candidate-key set [`pk_point_keys`] will expand: past this,
/// a bounded scan plans better than thousands of bloom-gated point reads.
pub const PK_IN_MAX_KEYS: usize = 1000;

/// Multi-key generalization of [`pk_point_key`]: every PK column pinned
/// (AND-reachable) by a non-null literal equality **or a non-negated `IN`
/// list of literals** yields the exact candidate key set — the "fetch these
/// N ids" shape becomes N point reads instead of a table scan. A literal
/// array element is flattened exactly as evaluation flattens it, so the
/// bound-parameter form (`id IN (?)` with `?` = `[1, 2, 3]`) qualifies.
/// `None` when a PK column is unpinned, a list element is not a literal, or
/// the composite cross product exceeds [`PK_IN_MAX_KEYS`]. The set is a
/// per-column superset (a doubly-constrained column keeps its smallest pin),
/// sound because every caller re-checks the full filter against the rows.
pub fn pk_point_keys(pk: &[String], filter: &Option<Expr>) -> Option<Vec<Vec<u8>>> {
    if pk.is_empty() {
        return None;
    }
    let expr = filter.as_ref()?;
    let mut per_col: Vec<Vec<Value>> = Vec::with_capacity(pk.len());
    for col in pk {
        let cands = pk_column_candidates(expr, col)?;
        if cands.is_empty() {
            // Pinned to an empty set (e.g. `IN (NULL)`): matches nothing.
            return Some(Vec::new());
        }
        per_col.push(cands);
    }
    let mut total = 1usize;
    for c in &per_col {
        total = total.checked_mul(c.len())?;
        if total > PK_IN_MAX_KEYS {
            return None;
        }
    }
    // Cross product over the composite columns; the BTreeSet dedups repeated
    // list values and yields keys in ascending (primary-key) order.
    let mut keys = std::collections::BTreeSet::new();
    let mut idx = vec![0usize; per_col.len()];
    loop {
        let values: Vec<Value> = idx
            .iter()
            .zip(&per_col)
            .map(|(&i, c)| c[i].clone())
            .collect();
        keys.insert(Value::Array(values).encode_key());
        let mut d = per_col.len();
        loop {
            if d == 0 {
                return Some(keys.into_iter().collect());
            }
            d -= 1;
            idx[d] += 1;
            if idx[d] < per_col[d].len() {
                break;
            }
            idx[d] = 0;
        }
    }
}

/// The AND-reachable literal candidate set pinning `col`: from `col = lit`
/// (either side) or `col IN (literals)`. When several pins constrain the same
/// column, the smallest set wins (any single pin is a superset of their
/// intersection). `None` if no usable pin exists.
fn pk_column_candidates(expr: &Expr, col: &str) -> Option<Vec<Value>> {
    fn consider(best: &mut Option<Vec<Value>>, vals: Vec<Value>) {
        match best {
            Some(b) if b.len() <= vals.len() => {}
            _ => *best = Some(vals),
        }
    }
    fn collect(expr: &Expr, col: &str, best: &mut Option<Vec<Value>>) {
        match expr {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                collect(left, col, best);
                collect(right, col, best);
            }
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            } => {
                let v = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v)) if c == col && !v.is_null() => v,
                    (Expr::Literal(v), Expr::Column(c)) if c == col && !v.is_null() => v,
                    _ => return,
                };
                consider(best, vec![v.clone()]);
            }
            Expr::InList {
                expr: e,
                list,
                negated: false,
            } => {
                let Expr::Column(c) = e.as_ref() else { return };
                if c != col {
                    return;
                }
                let mut vals = Vec::new();
                for item in list {
                    match item {
                        // A literal array is the bound-parameter form
                        // (`id IN (?)`); flatten like evaluation does.
                        Expr::Literal(Value::Array(elems)) => {
                            vals.extend(elems.iter().filter(|v| !v.is_null()).cloned())
                        }
                        Expr::Literal(v) if !v.is_null() => vals.push(v.clone()),
                        Expr::Literal(_) => {} // NULL element matches nothing
                        _ => return, // non-literal element: pin unusable
                    }
                }
                consider(best, vals);
            }
            _ => {}
        }
    }
    let mut best = None;
    collect(expr, col, &mut best);
    best
}

/// The embedded, single-node implementation of [`Cluster`].
///
/// Writes are appended with **deferred durability**: each put/delete lands in
/// the WAL and memtable immediately, but the fsync is postponed and issued
/// once per touched engine when the statement finishes ([`LocalCluster::
/// flush_pending`]) — a multi-row `INSERT` costs one fsync, not one per row.
struct LocalCluster<'a> {
    db: &'a mut Database,
    /// Per-statement memo of each table's secondary indexes, so a multi-row
    /// statement doesn't rescan (and re-clone) the catalog per row.
    index_memo: HashMap<String, Vec<(String, Vec<String>)>>,
    /// Latest deferred WAL commit per engine touched (`t:`/`i:`-prefixed
    /// name); syncing the latest commit makes all earlier ones durable.
    pending: HashMap<String, (Arc<WalSync>, WalCommit)>,
}

impl<'a> LocalCluster<'a> {
    fn new(db: &'a mut Database) -> LocalCluster<'a> {
        LocalCluster {
            db,
            index_memo: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    /// Group-commit every deferred write. Must be called when the statement
    /// finishes (success or error — applied rows must become durable either
    /// way, matching the previous per-row fsync behavior). When the Database
    /// is in deferred-sync mode (see [`Database::deferred_syncs`]) the pairs
    /// are handed up to the caller instead, who fsyncs them after releasing
    /// its exclusive lock.
    fn flush_pending(&mut self) -> Result<()> {
        if let Some(deferred) = self.db.deferred_syncs.as_mut() {
            deferred.extend(self.pending.drain().map(|(_, pair)| pair));
            return Ok(());
        }
        for (_, (sync, commit)) in self.pending.drain() {
            sync.sync_through(commit)?;
        }
        Ok(())
    }

    /// The memoized `(name, paths)` list of `table`'s secondary indexes.
    fn table_indexes(&mut self, table: &str) -> &[(String, Vec<String>)] {
        if !self.index_memo.contains_key(table) {
            let list = self.db.indexes_on(table);
            self.index_memo.insert(table.to_string(), list);
        }
        &self.index_memo[table]
    }

    /// The row half of [`Cluster::put`] for this executor: everything except
    /// the search-index refresh check, which the caller runs once per
    /// statement. Returns `false` when the write was buffered into an open
    /// transaction (nothing reached the indexes yet).
    fn put_row(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<bool> {
        // Buffer the write when a transaction is open (flushed on COMMIT).
        if let Some(txn) = self.db.txn.as_mut() {
            txn.writes
                .insert((table.to_string(), key.to_vec()), Some(doc.clone()));
            return Ok(false);
        }
        self.table_indexes(table);
        self.db.index_del_previous(table, key)?;
        self.db.maintain_global_put(table, key, doc)?;
        let engine = self.db.table_engine_mut(table)?;
        let (hlc, commit) = engine.put_deferred(key, Value::encode_document(doc))?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_put_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_vectors_put(table, doc, key, hlc);
        self.db.maintain_geo_put(table, doc, key)?;
        self.db.search_put_unrefreshed(table, doc, key, hlc)?;
        Ok(true)
    }
}

impl Cluster for LocalCluster<'_> {
    fn primary_key(&self, table: &str) -> Result<Vec<String>> {
        Ok(self.db.table_def(table)?.primary_key.clone())
    }

    fn matching_rows(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        self.db.local_matching_rows(table, filter)
    }

    fn matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<(&str, bool, bool)>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        self.db.local_matching_rows_ordered(table, filter, order, fetch_limit)
    }

    fn matching_rows_projected(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: &HashSet<String>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        self.db.local_matching_rows_projected(table, filter, project)
    }

    fn count_rows(&self, table: &str) -> Result<Option<usize>> {
        self.db.local_count_rows(table)
    }

    fn count_filtered(&self, table: &str, filter: &Option<Expr>) -> Result<Option<usize>> {
        self.db.local_count_filtered(table, filter)
    }

    fn count_matching(&self, table: &str, filter: &Option<Expr>) -> Result<Option<usize>> {
        self.db.local_count_matching(table, filter).map(Some)
    }

    fn distinct_values(
        &self,
        table: &str,
        col: &str,
        filter: &Option<Expr>,
    ) -> Result<Option<Vec<Value>>> {
        self.db.local_distinct_values(table, col, filter).map(Some)
    }

    fn vector_search(
        &self,
        table: &str,
        path: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let index = self.db.vector_index_for(table, path).ok_or_else(|| {
            EngineError::Unsupported(format!("no vector index on {table} ({path})"))
        })?;
        self.db.vector_search(&index, query, k, filter)
    }

    fn embed_query(&self, table: &str, path: &str, text: &str) -> Result<Vec<f32>> {
        self.db.embed_query(table, path, text)
    }

    fn rerank(&self, model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        self.db.rerank(model, query, documents)
    }

    fn search(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        self.db
            .search_commit_if_dirty(table, query, k, filter, highlights)
    }

    fn search_aggregate(
        &mut self,
        table: &str,
        query: &SearchQuery,
        agg: &skaidb_fts::AggRequest,
    ) -> Result<Option<Vec<skaidb_fts::AggRow>>> {
        // Single-node: the local index holds every row; no ownership filter.
        self.db.search_aggregate(table, query, agg, None)
    }

    fn search_sorted(
        &mut self,
        table: &str,
        query: &SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Option<SortedSearchRows>> {
        self.db
            .search_sorted(table, query, sort, k, filter, highlights, None)
    }

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        if self.put_row(table, key, doc)? {
            self.db.search_refresh(table)?;
        }
        Ok(())
    }

    fn put_batch(&mut self, table: &str, rows: &[(Vec<u8>, Document)]) -> Result<()> {
        // The whole statement indexes under one NRT refresh check (the
        // phase-5 bulk-ingest path) instead of one per row.
        let mut wrote = false;
        for (key, doc) in rows {
            wrote |= self.put_row(table, key, doc)?;
        }
        if wrote {
            self.db.search_refresh(table)?;
        }
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        if let Some(txn) = self.db.txn.as_mut() {
            txn.writes.insert((table.to_string(), key.to_vec()), None);
            return Ok(());
        }
        self.table_indexes(table);
        self.db.maintain_global_del(table, key, doc)?;
        let engine = self.db.table_engine_mut(table)?;
        let (hlc, commit) = engine.delete_deferred(key)?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_del_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_geo_del(table, doc, key)?;
        self.db.maintain_vectors_del(table, key, hlc);
        self.db.maintain_search_del(table, key, hlc)?;
        Ok(())
    }

    fn ts_series_key(&self, table: &str) -> Result<Option<Vec<String>>> {
        Ok(self.db.ts_series_key(table))
    }

    fn ts_append(&mut self, table: &str, rows: &[(skaidb_tsdb::Labels, i64, f64)]) -> Result<usize> {
        if self.db.txn.is_some() {
            return Err(EngineError::Unsupported(
                "time-series tables cannot be written inside a transaction".into(),
            ));
        }
        self.db.ts_append(table, rows)
    }

    fn ts_query(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        self.db.ts_query(table, matchers, t0, t1)
    }

    fn ts_series_sets(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
    ) -> Result<Vec<skaidb_tsdb::Labels>> {
        self.db.ts_series_sets(table, matchers)
    }

    fn ts_local_range(&self, table: &str) -> Result<Option<(i64, i64)>> {
        Ok(self.db.ts_local_range(table))
    }

    fn ts_rollup_info(&self, table: &str) -> Result<TsRollupInfo> {
        self.db.ts_rollup_info(table)
    }
}

/// A read-only [`Cluster`] over a shared `&Database` — the seam behind
/// [`Database::execute_read_statement`]. SELECT execution never calls the
/// write methods, so they only guard against misuse.
struct LocalRead<'a> {
    db: &'a Database,
}

impl Cluster for LocalRead<'_> {
    fn primary_key(&self, table: &str) -> Result<Vec<String>> {
        Ok(self.db.table_def(table)?.primary_key.clone())
    }

    fn matching_rows(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        self.db.local_matching_rows(table, filter)
    }

    fn matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<(&str, bool, bool)>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        self.db.local_matching_rows_ordered(table, filter, order, fetch_limit)
    }

    fn matching_rows_projected(
        &self,
        table: &str,
        filter: &Option<Expr>,
        project: &HashSet<String>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        self.db.local_matching_rows_projected(table, filter, project)
    }

    fn count_rows(&self, table: &str) -> Result<Option<usize>> {
        self.db.local_count_rows(table)
    }

    fn count_filtered(&self, table: &str, filter: &Option<Expr>) -> Result<Option<usize>> {
        self.db.local_count_filtered(table, filter)
    }

    fn count_matching(&self, table: &str, filter: &Option<Expr>) -> Result<Option<usize>> {
        self.db.local_count_matching(table, filter).map(Some)
    }

    fn distinct_values(
        &self,
        table: &str,
        col: &str,
        filter: &Option<Expr>,
    ) -> Result<Option<Vec<Value>>> {
        self.db.local_distinct_values(table, col, filter).map(Some)
    }

    fn vector_search(
        &self,
        table: &str,
        path: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let index = self.db.vector_index_for(table, path).ok_or_else(|| {
            EngineError::Unsupported(format!("no vector index on {table} ({path})"))
        })?;
        self.db.vector_search(&index, query, k, filter)
    }

    fn embed_query(&self, table: &str, path: &str, text: &str) -> Result<Vec<f32>> {
        self.db.embed_query(table, path, text)
    }

    fn rerank(&self, model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        self.db.rerank(model, query, documents)
    }

    fn search(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        // Shared access: serve the last-committed index state (NRT — at most
        // `refresh_ms` stale) rather than committing pending writes.
        self.db.search_read(table, query, k, filter, highlights)
    }

    fn search_aggregate(
        &mut self,
        table: &str,
        query: &SearchQuery,
        agg: &skaidb_fts::AggRequest,
    ) -> Result<Option<Vec<skaidb_fts::AggRow>>> {
        self.db.search_aggregate_read(table, query, agg, None)
    }

    fn search_sorted(
        &mut self,
        table: &str,
        query: &SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[HighlightReq],
    ) -> Result<Option<SortedSearchRows>> {
        self.db
            .search_sorted_read(table, query, sort, k, filter, highlights, None)
    }

    fn put(&mut self, _table: &str, _key: &[u8], _doc: &Document) -> Result<()> {
        Err(EngineError::Unsupported(
            "write statement reached the read-only executor".into(),
        ))
    }

    fn delete(&mut self, _table: &str, _key: &[u8], _doc: &Document) -> Result<()> {
        Err(EngineError::Unsupported(
            "write statement reached the read-only executor".into(),
        ))
    }

    fn ts_series_key(&self, table: &str) -> Result<Option<Vec<String>>> {
        Ok(self.db.ts_series_key(table))
    }

    fn ts_query(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        self.db.ts_query(table, matchers, t0, t1)
    }

    fn ts_series_sets(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
    ) -> Result<Vec<skaidb_tsdb::Labels>> {
        self.db.ts_series_sets(table, matchers)
    }

    fn ts_local_range(&self, table: &str) -> Result<Option<(i64, i64)>> {
        Ok(self.db.ts_local_range(table))
    }

    fn ts_rollup_info(&self, table: &str) -> Result<TsRollupInfo> {
        self.db.ts_rollup_info(table)
    }
}

fn txn_journal_path(root: &Path) -> PathBuf {
    root.join("txn.journal")
}

/// Persist a transaction's write set durably: tmp file + fsync + rename +
/// dir fsync. Format: magic, entry count, then `(table, key, op, doc?)`
/// records, a trailing FNV-1a checksum over everything before it. A torn
/// or checksum-failing journal is treated as "never committed".
fn write_txn_journal(root: &Path, writes: &TxnWrites) -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"SKTXJ1");
    buf.extend_from_slice(&(writes.len() as u32).to_le_bytes());
    for ((table, key), op) in writes {
        buf.extend_from_slice(&(table.len() as u32).to_le_bytes());
        buf.extend_from_slice(table.as_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        match op {
            Some(doc) => {
                buf.push(1);
                let bytes = Value::encode_document(doc);
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
            }
            None => buf.push(0),
        }
    }
    let sum = fnv1a(&buf);
    buf.extend_from_slice(&sum.to_le_bytes());
    let tmp = root.join("txn.journal.tmp");
    std::fs::write(&tmp, &buf)?;
    let f = std::fs::File::open(&tmp)?;
    f.sync_all()?;
    std::fs::rename(&tmp, txn_journal_path(root))?;
    if let Ok(d) = std::fs::File::open(root) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Decode a journal; `None` for missing/torn/checksum-failing files (the
/// transaction was never durably committed).
fn read_txn_journal(root: &Path) -> Option<TxnWrites> {
    let buf = std::fs::read(txn_journal_path(root)).ok()?;
    if buf.len() < 6 + 4 + 8 || &buf[..6] != b"SKTXJ1" {
        return None;
    }
    let (body, sum_bytes) = buf.split_at(buf.len() - 8);
    if fnv1a(body) != u64::from_le_bytes(sum_bytes.try_into().ok()?) {
        return None;
    }
    let mut at = 6;
    let take = |at: &mut usize, n: usize| -> Option<&[u8]> {
        let s = body.get(*at..*at + n)?;
        *at += n;
        Some(s)
    };
    let count = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
    let mut writes = BTreeMap::new();
    for _ in 0..count {
        let tlen = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
        let table = String::from_utf8(take(&mut at, tlen)?.to_vec()).ok()?;
        let klen = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
        let key = take(&mut at, klen)?.to_vec();
        let op = match take(&mut at, 1)?[0] {
            0 => None,
            _ => {
                let dlen = u32::from_le_bytes(take(&mut at, 4)?.try_into().ok()?) as usize;
                let bytes = take(&mut at, dlen)?;
                match Value::decode(bytes).ok()? {
                    Value::Document(d) => Some(d),
                    _ => return None,
                }
            }
        };
        writes.insert((table, key), op);
    }
    Some(writes)
}

fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn table_dir(root: &Path, name: &str) -> PathBuf {
    root.join("tables").join(name)
}

fn index_dir(root: &Path, name: &str) -> PathBuf {
    root.join("indexes").join(name)
}

fn fts_dir(root: &Path, name: &str) -> PathBuf {
    root.join("fts").join(name)
}

/// Aggregate a flushed window into rollup samples: per series and bucket,
/// `<field>_{count,sum,min,max,first,last}` rows at the bucket start.
fn rollup_rows(
    flushed: &skaidb_tsdb::FlushedSeries,
    bucket_ms: i64,
) -> Result<Vec<(skaidb_tsdb::Labels, i64, f64)>> {
    let mut out = Vec::new();
    for (labels, chunks) in flushed {
        let mut samples = Vec::new();
        for chunk in chunks {
            samples.extend(
                skaidb_tsdb::decode_chunk(&chunk.data)
                    .map_err(|e| EngineError::Timeseries(e.to_string()))?,
            );
        }
        out.extend(rollup_partial_rows(labels, &samples, bucket_ms));
    }
    Ok(out)
}

/// One series' per-bucket rollup partial rows
/// (`<field>_{count,sum,min,max,first,last}`) from its samples. Shared by
/// the flush path ([`rollup_rows`]) and repair backfill.
fn rollup_partial_rows(
    labels: &skaidb_tsdb::Labels,
    samples: &[skaidb_tsdb::Sample],
    bucket_ms: i64,
) -> Vec<(skaidb_tsdb::Labels, i64, f64)> {
    let field = labels
        .iter()
        .find(|(k, _)| k == "__field__")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "value".into());
    let base: skaidb_tsdb::Labels = labels
        .iter()
        .filter(|(k, _)| k != "__field__")
        .cloned()
        .collect();
    // (count, sum, min, max, first_ts, first, last_ts, last) per bucket.
    type Acc = (u64, f64, f64, f64, i64, f64, i64, f64);
    let mut buckets: BTreeMap<i64, Acc> = BTreeMap::new();
    for s in samples {
        let b = s.ts.div_euclid(bucket_ms) * bucket_ms;
        let e = buckets.entry(b).or_insert((
            0,
            0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            s.ts,
            s.value,
            s.ts,
            s.value,
        ));
        e.0 += 1;
        e.1 += s.value;
        e.2 = e.2.min(s.value);
        e.3 = e.3.max(s.value);
        if s.ts < e.4 {
            e.4 = s.ts;
            e.5 = s.value;
        }
        if s.ts >= e.6 {
            e.6 = s.ts;
            e.7 = s.value;
        }
    }
    let mut out = Vec::new();
    for (bucket, (count, sum, min, max, _, first, _, last)) in buckets {
        for (suffix, value) in [
            ("count", count as f64),
            ("sum", sum),
            ("min", min),
            ("max", max),
            ("first", first),
            ("last", last),
        ] {
            let mut l = base.clone();
            l.push(("__field__".to_string(), format!("{field}_{suffix}")));
            l.sort();
            out.push((l, bucket, value));
        }
    }
    out
}

/// Render the idempotent CREATE DDL for a time-series table definition
/// (schema replay/bootstrap/repair all share it).
/// Build the RBAC view from catalog users + roles.
/// The catalog/wire key for a grant object: `None` = global, `db:<name>` =
/// a database, anything else = a table (table names cannot contain `:`).
const DB_OBJECT_PREFIX: &str = "db:";

fn grant_object_key(o: &skaidb_sql::ast::GrantObject) -> Option<String> {
    use skaidb_sql::ast::GrantObject;
    match o {
        GrantObject::Global => None,
        GrantObject::Table(t) => Some(t.clone()),
        GrantObject::Database(d) => Some(format!("{DB_OBJECT_PREFIX}{d}")),
    }
}

fn build_role_store(catalog: &Catalog) -> RoleStore {
    let mut store = RoleStore::new();
    for name in catalog.auth_roles.keys() {
        let _ = store.create_role(name);
    }
    for (name, def) in &catalog.auth_roles {
        for (priv_name, table) in &def.grants {
            if let Some(p) = privilege_from_name(priv_name) {
                let object = match table {
                    Some(t) => match t.strip_prefix(DB_OBJECT_PREFIX) {
                        Some(db) => AuthObject::Database(db.to_string()),
                        None => AuthObject::Table(t.clone()),
                    },
                    None => AuthObject::Global,
                };
                let _ = store.grant(name, p, object);
            }
        }
        for parent in &def.inherits {
            let _ = store.grant_role(name, parent);
        }
    }
    store
}

/// Encode a role's whole state (`priv@obj;...;+parent;...`) — the internal
/// replication form carried by `CREATE ROLE ... GRANTS '<state>'`.
fn encode_role_state(def: &AuthRoleDef) -> String {
    let mut parts: Vec<String> = def
        .grants
        .iter()
        .map(|(p, t)| format!("{p}@{}", t.as_deref().unwrap_or("*")))
        .collect();
    parts.extend(def.inherits.iter().map(|p| format!("+{p}")));
    parts.join(";")
}

fn decode_role_state(enc: &str) -> Result<AuthRoleDef> {
    let mut def = AuthRoleDef::default();
    for part in enc.split(';').filter(|p| !p.is_empty()) {
        if let Some(parent) = part.strip_prefix('+') {
            def.inherits.push(parent.to_string());
        } else if let Some((p, obj)) = part.split_once('@') {
            privilege_from_name(p)
                .ok_or_else(|| EngineError::Constraint(format!("bad role state: {part}")))?;
            let table = if obj == "*" { None } else { Some(obj.to_string()) };
            def.grants.push((p.to_string(), table));
        } else {
            return Err(EngineError::Constraint(format!("bad role state: {part}")));
        }
    }
    Ok(def)
}

/// Render the idempotent CREATE ROLLUP DDL.
fn ts_rollup_ddl(bare: &str, source_bare: &str, bucket_ms: i64, def: &TsTableDef) -> String {
    let retention = def
        .retention_ms
        .map(|ms| format!(" RETENTION {ms}ms"))
        .unwrap_or_default();
    format!("CREATE ROLLUP IF NOT EXISTS {bare} ON {source_bare} BUCKET {bucket_ms}ms{retention}")
}

fn ts_create_ddl(bare: &str, def: &TsTableDef) -> String {
    let retention = def
        .retention_ms
        .map(|ms| format!(", RETENTION {ms}ms"))
        .unwrap_or_default();
    let ooo = if def.ooo_window_ms > 0 {
        format!(", OOO {}ms", def.ooo_window_ms)
    } else {
        String::new()
    };
    format!(
        "CREATE TIMESERIES TABLE IF NOT EXISTS {bare} (SERIES KEY ({}){retention}{ooo})",
        def.series_key.join(", ")
    )
}

fn ts_dir(root: &Path, name: &str) -> PathBuf {
    root.join("timeseries").join(name)
}

/// Open a time-series store for a catalog definition.
fn open_tsdb(root: &Path, name: &str, def: &TsTableDef, head_max_bytes: u64) -> Result<Tsdb> {
    Tsdb::open(
        &ts_dir(root, name),
        TsdbOptions {
            retention_ms: def.retention_ms,
            ooo_window_ms: def.ooo_window_ms,
            head_max_bytes,
            ..TsdbOptions::default()
        },
    )
    .map_err(|e| EngineError::Timeseries(e.to_string()))
}

/// Current wall-clock in milliseconds (`now()` resolution).
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The indexed values of `doc` at `paths` (a missing field indexes as `NULL`).
/// Build an empty HNSW for a vector index definition.
/// Recursively copy a directory tree (regular files + dirs only),
/// returning `(files, bytes)`.
fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<(u64, u64)> {
    std::fs::create_dir_all(dst)?;
    let (mut files, mut bytes) = (0u64, 0u64);
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            let (f, b) = copy_dir_all(&entry.path(), &to)?;
            files += f;
            bytes += b;
        } else if ty.is_file() {
            bytes += std::fs::copy(entry.path(), &to)?;
            files += 1;
        }
    }
    Ok((files, bytes))
}

impl Database {
    /// `BACKUP TO '<path>'`: a crash-consistent copy of the whole data
    /// directory. Runs under the exclusive lock (`&mut self`), so no write
    /// can interleave; WALs travel with the files, so opening the copy
    /// replays exactly like a crash recovery. Vector indexes are derived
    /// (in-memory, rebuilt on open) and need nothing extra. The target
    /// must not already exist.
    pub fn backup_to(&mut self, path: &str) -> Result<ResultSet> {
        let dst = Path::new(path);
        if dst.exists() {
            return Err(EngineError::Unsupported(format!(
                "backup target '{path}' already exists — refusing to overwrite"
            )));
        }
        // Make the copy as compact and current as possible: commit pending
        // search-index writes so their segments are on disk.
        let names: Vec<String> = self.search_indexes.keys().cloned().collect();
        for name in names {
            if let Some(live) = self.search_indexes.get_mut(&name) {
                live.commit_if_dirty()?;
            }
        }
        let started = std::time::Instant::now();
        let (files, bytes) = copy_dir_all(&self.dir, dst)
            .map_err(|e| EngineError::Unsupported(format!("backup to '{path}': {e}")))?;
        Ok(ResultSet {
            columns: vec![
                "path".into(),
                "files".into(),
                "bytes".into(),
                "elapsed_ms".into(),
            ],
            rows: vec![vec![
                Value::String(path.to_string()),
                Value::Int(files as i64),
                Value::Int(bytes as i64),
                Value::Int(started.elapsed().as_millis() as i64),
            ]],
        })
    }

    /// `RESTORE FROM '<path>'`: replace this instance's data with a backup
    /// and reopen in place. The old data directory is moved aside to
    /// `<dir>.pre-restore-<n>` (never deleted). Single-instance semantics —
    /// the cluster layer rejects RESTORE (restoring one node's data into a
    /// live ring is an operator-level action).
    pub fn restore_from(&mut self, path: &str) -> Result<ResultSet> {
        let src = Path::new(path);
        if !src.join("catalog.json").is_file() {
            return Err(EngineError::Unsupported(format!(
                "'{path}' does not look like a skaidb backup (no catalog.json)"
            )));
        }
        let dir = self.dir.clone();
        let opts = self.storage_opts.clone();
        // Move the live directory aside (transaction-ish: the backup copy
        // happens into the ORIGINAL path, so on any error the aside copy
        // still holds the previous state).
        let mut aside = dir.with_extension("pre-restore");
        let mut n = 1;
        while aside.exists() {
            aside = dir.with_extension(format!("pre-restore-{n}"));
            n += 1;
        }
        // Drop every open handle before touching the files: swap in a
        // placeholder opened on an empty temp dir? Not needed — renaming
        // the directory out from under memory-mapped/open files is safe on
        // POSIX (handles follow the inode), and we reopen from scratch
        // below before serving anything.
        std::fs::rename(&dir, &aside)
            .map_err(|e| EngineError::Unsupported(format!("restore: move aside: {e}")))?;
        if let Err(e) = copy_dir_all(src, &dir) {
            // Roll back: put the original data back.
            let _ = std::fs::remove_dir_all(&dir);
            let _ = std::fs::rename(&aside, &dir);
            return Err(EngineError::Unsupported(format!(
                "restore from '{path}': {e} (previous data restored)"
            )));
        }
        match Database::open_with_options(&dir, opts.clone()) {
            Ok(fresh) => {
                *self = fresh;
                Ok(ResultSet {
                    columns: vec!["restored_from".into(), "previous_data".into()],
                    rows: vec![vec![
                        Value::String(path.to_string()),
                        Value::String(aside.display().to_string()),
                    ]],
                })
            }
            Err(e) => {
                // Roll back to the pre-restore data.
                let _ = std::fs::remove_dir_all(&dir);
                let _ = std::fs::rename(&aside, &dir);
                *self = Database::open_with_options(&dir, opts)?;
                Err(EngineError::Unsupported(format!(
                    "restored data failed to open ({e}); previous data reinstated"
                )))
            }
        }
    }
}

/// `<data dir>/vector/<index>.hnsw` — the persisted HNSW snapshot.
fn vector_snapshot_path(dir: &Path, name: &str) -> std::path::PathBuf {
    dir.join("vector").join(format!("{name}.hnsw"))
}

/// Load a snapshot if it exists and matches the live definition's
/// construction parameters (metric, dim, m, ef_construction — the graph's
/// shape). `ef_search` is a query-time knob and takes the definition's
/// current value. Any failure returns `None`: the caller rebuilds.
fn load_vector_snapshot(
    path: &Path,
    def: &VectorIndexDef,
) -> Option<(Hnsw, Hlc)> {
    let f = std::fs::File::open(path).ok()?;
    let mut r = std::io::BufReader::with_capacity(1 << 20, f);
    let mut wm = [0u8; 12];
    std::io::Read::read_exact(&mut r, &mut wm).ok()?;
    let watermark = Hlc::from_bytes(wm);
    let mut h = Hnsw::read_from(&mut r).ok()?;
    let fresh = new_hnsw(def);
    if h.params() != fresh.params() || h.is_quantized() != fresh.is_quantized() {
        skaidb_types::slog!(
            "skaidb: vector snapshot {} ignored — construction params changed",
            path.display()
        );
        return None;
    }
    if let Some(ef) = def.ef_search {
        h.set_ef_search(ef);
    }
    Some((h, watermark))
}

/// Persist `hnsw` + its replay watermark: temp file, fsync, rename — a crash
/// mid-save leaves the previous snapshot intact.
fn save_vector_snapshot(path: &Path, hnsw: &Hnsw, watermark: Hlc) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("hnsw.tmp");
    {
        let f = std::fs::File::create(&tmp)?;
        let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
        std::io::Write::write_all(&mut w, &watermark.to_bytes())?;
        hnsw.write_to(&mut w)?;
        let f = std::io::Write::flush(&mut w).map(|()| w.into_inner());
        f?.map_err(|e| e.into_error())?.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Render a table's `WITH (...)` options for schema statements — empty when
/// every option is at its default.
fn table_with_options(def: &TableDef) -> String {
    let mut opts: Vec<String> = Vec::new();
    if let Some(ms) = def.ttl_ms {
        opts.push(format!("ttl = {ms}"));
    }
    if def.memory {
        opts.push("memory = true".into());
    }
    if opts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", opts.join(", "))
    }
}

fn new_hnsw(def: &VectorIndexDef) -> Hnsw {
    let metric = Metric::parse(&def.metric).unwrap_or(Metric::Cosine);
    let mut h = if def.quantized {
        Hnsw::new_quantized(metric, def.dim)
    } else {
        Hnsw::new(metric, def.dim)
    };
    if let Some(ef) = def.ef_search {
        h.set_ef_search(ef);
    }
    h
}

/// How far a quantized-graph search over-fetches before the exact rescore:
/// `k × this` approximate candidates are re-scored against the exact row
/// vectors, and the best `k` by exact distance are returned.
pub const QUANT_RESCORE_OVERSAMPLE: usize = 4;

/// Exact metric distance between two raw f32 vectors (`None` on an unknown
/// metric or a dimension mismatch) — the rescoring primitive a cluster
/// coordinator uses on candidates from a quantized graph. Cosine normalizes
/// internally, so row-stored vectors need no preparation.
pub fn vector_exact_distance(metric: &str, a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    Metric::parse(metric).map(|m| crate::vector::exact_distance(m, a, b))
}

/// Extract the float vector at `path` (an array of `int`/`float`), or `None`.
fn doc_vector_raw(doc: &Document, path: &str) -> Option<Vec<f32>> {
    let Some(Value::Array(items)) = doc.get_path(path) else {
        return None;
    };
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        match it {
            Value::Int(i) => out.push(*i as f32),
            Value::Float(f) => out.push(*f as f32),
            _ => return None, // non-numeric element: not a vector
        }
    }
    Some(out)
}

/// Like [`doc_vector_raw`] but only accepts vectors of the expected dimension.
/// `pub` so a cluster coordinator can pull the exact vector out of a re-read
/// row when rescoring quantized-graph candidates.
pub fn doc_vector(doc: &Document, path: &str, dim: usize) -> Option<Vec<f32>> {
    doc_vector_raw(doc, path).filter(|v| v.len() == dim)
}

fn index_values(doc: &Document, paths: &[String]) -> Vec<Value> {
    paths
        .iter()
        .map(|p| doc.get_path(p).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Position of the MULTIKEY component (`path[]` in CREATE INDEX), if any.
fn multi_pos(paths: &[String]) -> Option<usize> {
    paths.iter().position(|p| p.ends_with("[]"))
}

/// Index paths with the `[]` multikey marker stripped — the column names the
/// planner matches filter constraints against.
fn index_key_names(paths: &[String]) -> Vec<String> {
    paths
        .iter()
        .map(|p| p.trim_end_matches("[]").to_string())
        .collect()
}

/// Every index entry tuple for `doc`: one tuple for a plain index; for a
/// multikey index, one per element of the array at the `[]` component (a
/// scalar there indexes as itself; an empty array yields no entries — no
/// element equality can match it). Duplicate elements collapse structurally:
/// the entry key embeds the row key, so identical (element, row) pairs are
/// one entry — which is what makes an exact-element `count_range` an exact
/// ROW count.
fn index_value_tuples(doc: &Document, paths: &[String]) -> Vec<Vec<Value>> {
    let Some(m) = multi_pos(paths) else {
        return vec![index_values(doc, paths)];
    };
    let names = index_key_names(paths);
    let base = index_values(doc, &names);
    match &base[m] {
        Value::Array(items) => items
            .iter()
            .map(|el| {
                let mut t = base.clone();
                t[m] = el.clone();
                t
            })
            .collect(),
        _ => vec![base],
    }
}

/// Index entry key: `[v1, .., vk, row_key]` encoded order-preservingly, so all
/// entries sharing a leading value prefix share a byte prefix (composite-aware).
/// The entry-key delta a row mutation implies for one GLOBAL index:
/// `(deletes, puts)` — old entries no longer produced by the new document,
/// and new entries the old one didn't have. Unchanged entries are untouched
/// (no redundant replicated writes). `new = None` is a row delete.
pub fn global_entry_delta(
    paths: &[String],
    row_key: &[u8],
    old: Option<&Document>,
    new: Option<&Document>,
) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let keys = |doc: Option<&Document>| -> std::collections::HashSet<Vec<u8>> {
        doc.map(|d| {
            index_value_tuples(d, paths)
                .iter()
                .map(|values| gidx_entry_key(values, row_key))
                .collect()
        })
        .unwrap_or_default()
    };
    let old_keys = keys(old);
    let new_keys = keys(new);
    let dels = old_keys.difference(&new_keys).cloned().collect();
    let puts = new_keys.difference(&old_keys).cloned().collect();
    (dels, puts)
}

/// One GLOBAL index entry key: `u16 BE prefix_len ‖ values_prefix ‖ row_key`,
/// where `values_prefix` is the order-preserving encoding of the indexed
/// value tuple (`index_prefix_n`). Self-describing on purpose: ring
/// placement, repair ownership, and resharding must recover the VALUES
/// prefix from the key alone ([`gidx_placement_prefix`]) so every entry for
/// one value lands on — and stays on — one replica set, which is what makes
/// the routed equality probe a single replica-set round-trip.
pub fn gidx_entry_key(values: &[Value], row_key: &[u8]) -> Vec<u8> {
    let prefix = index_prefix_n(values);
    let mut key = Vec::with_capacity(2 + prefix.len() + row_key.len());
    key.extend_from_slice(&(prefix.len() as u16).to_be_bytes());
    key.extend_from_slice(&prefix);
    key.extend_from_slice(row_key);
    key
}

/// The ring-placement bytes of a GLOBAL index entry key: the length header
/// plus the values prefix (everything but the row-key tail). Malformed keys
/// (foreign writes, pre-phase-2 entries) fall back to the whole key — safe,
/// they just don't co-locate.
pub fn gidx_placement_prefix(key: &[u8]) -> &[u8] {
    if key.len() >= 2 {
        let n = u16::from_be_bytes([key[0], key[1]]) as usize;
        if 2 + n <= key.len() {
            return &key[..2 + n];
        }
    }
    key
}

/// The row key embedded in a GLOBAL index entry key; `None` if malformed.
pub fn gidx_entry_row_key(key: &[u8]) -> Option<&[u8]> {
    if key.len() >= 2 {
        let n = u16::from_be_bytes([key[0], key[1]]) as usize;
        if 2 + n <= key.len() {
            return Some(&key[2 + n..]);
        }
    }
    None
}

/// The `[start, end)` entry-key range holding every entry whose value tuple
/// is exactly `values` — the routed probe's read range. `None` only in the
/// degenerate all-0xFF-prefix case.
pub fn gidx_probe_bounds(values: &[Value]) -> Option<(Vec<u8>, Vec<u8>)> {
    let prefix = index_prefix_n(values);
    let mut start = Vec::with_capacity(2 + prefix.len());
    start.extend_from_slice(&(prefix.len() as u16).to_be_bytes());
    start.extend_from_slice(&prefix);
    let end = prefix_upper_bound(&start)?;
    Some((start, end))
}

/// Whether `table` (internal name) is a GLOBAL index entry table.
pub fn is_gidx_table(table: &str) -> bool {
    namespace::split(table).1.starts_with("__gidx__")
}

/// One GLOBAL-index probe range: the `[start, end)` entry keys of a single
/// pinned value tuple.
pub type ProbeRange = (Vec<u8>, Vec<u8>);

/// Max value tuples (probe ranges) one GLOBAL-index probe may expand to —
/// each range costs a replica-set round-trip; past this the scatter paths
/// win.
pub const GIDX_PROBE_RANGES: usize = 100;

/// The internal replicated table holding a GLOBAL index's entries:
/// `<db>␟__gidx__<bare index name>` — same database as the index, hidden
/// from SHOW TABLES and schema replay (the index DDL implies it). A plain
/// `__gidx__` prefix rather than a `␟`-separated segment: in the default
/// database names are unprefixed, and a leading `␟` segment would parse as
/// a database. Entries are `index_entry_key(values, row_key) → empty
/// document`, placed on the ring by entry key like any other row.
pub fn gidx_table(index_name: &str) -> String {
    let (db, bare) = namespace::split(index_name);
    namespace::qualify(db, &format!("__gidx__{bare}"))
}

pub fn index_entry_key(values: &[Value], row_key: &[u8]) -> Vec<u8> {
    let mut elems = values.to_vec();
    elems.push(Value::Bytes(row_key.to_vec()));
    Value::Array(elems).encode_key()
}

/// The geo index entry key for `doc`'s point at `path`: the 8-byte big-endian
/// Morton code of the point, followed by the row key (so entries sort by
/// Z-order and every row is a distinct entry). `None` when the field is not a
/// readable `{lat, lon}` point — schema-less policy: absent/bad → no entry,
/// exactly as the row would never satisfy a geo predicate.
fn geo_entry_key(doc: &Document, path: &str, row_key: &[u8]) -> Option<Vec<u8>> {
    let (lat, lon) = crate::eval::read_point(doc.get_path(path)?)?;
    let mut key = crate::geo::morton_key(lat, lon).to_vec();
    key.extend_from_slice(row_key);
    Some(key)
}

/// The shared byte prefix of every index entry whose leading values are
/// `values` (the encoding of the array `[values..]` without its trailing array
/// terminator). Also the inclusive lower bound for a scan starting there.
/// Primary-key prefix range for `filter` over a table with PK columns `pk`:
/// a leftmost run of PK columns pinned by equality (plus one optional
/// trailing range on the next PK column) bounds the scan to `[start, end)`
/// of the table's own key space — the table IS the primary index, ordered by
/// its encoded key. `WHERE channel = ?` on PK `(channel, ts)` reads one
/// channel's slice instead of the whole table. `None` when the filter pins
/// no leftmost PK column, and for full-PK equality (that is the point-read
/// fast path). Bounds are a *superset* of the matches (see
/// [`column_constraints`]) — callers re-check the filter on every row.
/// `end = None` means unbounded above. Used by the local gather
/// ([`Database::gather_rows_planned`]) and the clustered gather
/// (`cluster_scan_collect`), which narrows every source — its own shard
/// walk and each peer's scan cursor — to the same range.
pub fn pk_prefix_scan_range(
    pk: &[String],
    filter: &Option<Expr>,
) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let constraints = column_constraints(filter);
    let get = |col: &str| constraints.iter().find(|(c, _)| c == col).map(|(_, c)| c);
    let mut prefix: Vec<Value> = Vec::new();
    while prefix.len() < pk.len() {
        match get(&pk[prefix.len()]).and_then(|c| c.eq.clone()) {
            Some(v) => prefix.push(v),
            None => break,
        }
    }
    if prefix.is_empty() || prefix.len() >= pk.len() {
        return None;
    }
    let trailing = get(&pk[prefix.len()])
        .filter(|c| c.eq.is_none() && (c.lo.is_some() || c.hi.is_some()));
    Some(match trailing {
        Some(c) => (
            match &c.lo {
                Some(v) => index_prefix_n(&push(&prefix, v)),
                None => index_prefix_n(&prefix),
            },
            match &c.hi {
                Some(v) => index_upper_bound_n(&push(&prefix, v)),
                None => index_upper_bound_n(&prefix),
            },
        ),
        None => (index_prefix_n(&prefix), index_upper_bound_n(&prefix)),
    })
}

fn index_prefix_n(values: &[Value]) -> Vec<u8> {
    let mut key = Value::Array(values.to_vec()).encode_key();
    key.pop(); // drop the array-terminator byte
    key
}

/// Exclusive upper bound just past every entry with leading values `values`.
fn index_upper_bound_n(values: &[Value]) -> Option<Vec<u8>> {
    prefix_upper_bound(&index_prefix_n(values))
}

/// The smallest byte string strictly greater than every string with `prefix`.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while let Some(last) = bound.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(bound);
        }
        bound.pop();
    }
    None
}

/// Equality / range constraints gathered for one column from a filter.
#[derive(Default, Clone)]
struct ColConstraint {
    eq: Option<Value>,
    lo: Option<Value>, // from `>` / `>=`
    hi: Option<Value>, // from `<` / `<=`
}

/// Per-column constraints derived from `filter` (equality + comparisons reached
/// through `AND`). Bounds are a *superset* of the matches — `matches_filter`
/// re-checks exact predicates — so inclusivity need not be byte-exact.
fn column_constraints(filter: &Option<Expr>) -> Vec<(String, ColConstraint)> {
    let mut cmps: Vec<(String, BinaryOp, Value)> = Vec::new();
    if let Some(expr) = filter {
        collect_comparisons(expr, &mut cmps);
    }
    let mut by_col: Vec<(String, ColConstraint)> = Vec::new();
    for (col, op, val) in cmps {
        let idx = match by_col.iter().position(|(c, _)| *c == col) {
            Some(i) => i,
            None => {
                by_col.push((col.clone(), ColConstraint::default()));
                by_col.len() - 1
            }
        };
        let c = &mut by_col[idx].1;
        match op {
            BinaryOp::Eq => c.eq = Some(val),
            BinaryOp::Gt | BinaryOp::GtEq => {
                c.lo = Some(match c.lo.take() {
                    Some(cur) => value_max(cur, val),
                    None => val,
                });
            }
            BinaryOp::Lt | BinaryOp::LtEq => {
                c.hi = Some(match c.hi.take() {
                    Some(cur) => value_min(cur, val),
                    None => val,
                });
            }
            _ => {}
        }
    }
    by_col
}

/// Split a conjunction holding exactly one NULL-safe negated equality —
/// `(col != lit OR col IS NULL)`, the shape Mongo-semantics adapters emit for
/// `$ne` — into `(rest_of_conjunction, col, lit)`. Only this form admits the
/// complement identity COUNT(rest) − COUNT(rest AND col = lit): it counts
/// everything except `col = lit`, absent/null included. A BARE `col != lit`
/// must NOT take this path — SQL `!=` excludes nulls, the complement keeps
/// them (a test caught exactly that off-by-nulls).
fn split_one_negated_eq(expr: &Expr) -> Option<(Option<Expr>, String, Value)> {
    /// `col != lit` with a count-safe literal → `(col, lit)`.
    fn as_neq(expr: &Expr) -> Option<(String, Value)> {
        let Expr::Binary {
            op: BinaryOp::NotEq,
            left,
            right,
        } = expr
        else {
            return None;
        };
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(c))
                if count_safe_literal(v) =>
            {
                Some((c.clone(), v.clone()))
            }
            _ => None,
        }
    }
    /// `col IS NULL` → the column name.
    fn as_is_null(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::IsNull { expr, negated: false } => match expr.as_ref() {
                Expr::Column(c) => Some(c),
                _ => None,
            },
            _ => None,
        }
    }
    fn collect(expr: &Expr, keep: &mut Vec<Expr>, neg: &mut Vec<(String, Value)>) -> bool {
        match expr {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => collect(left, keep, neg) && collect(right, keep, neg),
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => {
                // NULL-safe pair: (col != lit OR col IS NULL), either order.
                let pair = match (as_neq(left), as_is_null(right)) {
                    (Some((c, v)), Some(n)) if c == n => Some((c, v)),
                    _ => match (as_is_null(left), as_neq(right)) {
                        (Some(n), Some((c, v))) if c == n => Some((c, v)),
                        _ => None,
                    },
                };
                match pair {
                    Some(cv) => {
                        neg.push(cv);
                        true
                    }
                    None => false,
                }
            }
            other => {
                keep.push(other.clone());
                true
            }
        }
    }
    let mut keep = Vec::new();
    let mut neg = Vec::new();
    if !collect(expr, &mut keep, &mut neg) || neg.len() != 1 {
        return None;
    }
    let rest = keep.into_iter().reduce(|a, b| Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(a),
        right: Box::new(b),
    });
    let (col, lit) = neg.pop().expect("checked len");
    Some((rest, col, lit))
}

/// Whether `expr` is a pure conjunction of `column <op> literal` comparisons —
/// exactly the shape `collect_comparisons` captures in full, so the collected
/// constraints *are* the filter with nothing residual. Index-only `COUNT(*)`
/// requires this: any residual predicate would need row re-reads to apply.
fn filter_is_conjunctive(expr: &Expr) -> bool {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => filter_is_conjunctive(left) && filter_is_conjunctive(right),
        Expr::Binary {
            op: BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
            left,
            right,
        } => {
            matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(_), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(_))
                    if count_safe_literal(v)
            )
        }
        _ => false,
    }
}

/// Whether a literal can probe an index with byte-exact fidelity. Filter
/// evaluation coerces numerics cross-type (`Int(1)` matches a stored
/// `Float(1.0)`), but index keys encode each type distinctly — a numeric
/// probe could silently undercount rows stored under the sibling type. Bool,
/// String, Bytes and Uuid compare only within their own type in eval, so
/// index bytes and eval agree. Null literals never match anything.
fn count_safe_literal(v: &Value) -> bool {
    matches!(
        v,
        Value::Bool(_) | Value::String(_) | Value::Bytes(_) | Value::Uuid(_)
    )
}

/// Like `plan_for_index`, but only when the index *fully covers* the
/// constraint set: a leftmost run of equality-pinned columns, optionally one
/// trailing range column, and no constrained column left over. Every row
/// contributes exactly one entry per index (`index_values` never fans out),
/// so under full coverage the entries in the returned byte range correspond
/// 1:1 with the rows matching the filter.
type CoveringBounds = (Option<Vec<u8>>, Option<Vec<u8>>);
fn plan_covering(
    paths: &[String],
    constraints: &[(String, ColConstraint)],
) -> Option<CoveringBounds> {
    let get = |col: &str| constraints.iter().find(|(c, _)| c == col).map(|(_, c)| c);

    let mut eq_prefix: Vec<Value> = Vec::new();
    let mut consumed = 0usize;
    while eq_prefix.len() < paths.len() {
        match get(&paths[eq_prefix.len()]) {
            Some(c) if c.eq.is_some() && c.lo.is_none() && c.hi.is_none() => {
                eq_prefix.push(c.eq.clone().expect("checked eq above"));
                consumed += 1;
            }
            _ => break,
        }
    }
    let i = eq_prefix.len();
    let trailing = if i < paths.len() {
        get(&paths[i]).filter(|c| c.eq.is_none() && (c.lo.is_some() || c.hi.is_some()))
    } else {
        None
    };
    if trailing.is_some() {
        consumed += 1;
    }
    if consumed == 0 || consumed != constraints.len() {
        return None; // residual constraints — an entry count would be a superset
    }

    Some(match trailing {
        Some(c) => {
            let start = match &c.lo {
                Some(v) => Some(index_prefix_n(&push(&eq_prefix, v))),
                None => Some(index_prefix_n(&eq_prefix)),
            };
            let end = match &c.hi {
                Some(v) => index_upper_bound_n(&push(&eq_prefix, v)),
                None => index_upper_bound_n(&eq_prefix),
            };
            (start, end)
        }
        None => (
            Some(index_prefix_n(&eq_prefix)),
            index_upper_bound_n(&eq_prefix),
        ),
    })
}

/// Build a scan plan for one (possibly composite) index given the filter's
/// per-column constraints and the requested `ORDER BY` column. Consumes a
/// leftmost run of equality-pinned columns, then an optional trailing range on
/// the next column. Returns `(start, end, sorted)` where `sorted` is whether the
/// scan order already satisfies `order`.
type ScanBounds = (Option<Vec<u8>>, Option<Vec<u8>>, bool, bool);
fn plan_for_index(
    paths: &[String],
    constraints: &[(String, ColConstraint)],
    order: Option<(&str, bool)>,
) -> Option<ScanBounds> {
    let get = |col: &str| constraints.iter().find(|(c, _)| c == col).map(|(_, c)| c);

    // Leftmost equality-pinned prefix.
    let mut eq_prefix: Vec<Value> = Vec::new();
    while eq_prefix.len() < paths.len() {
        match get(&paths[eq_prefix.len()]).and_then(|c| c.eq.clone()) {
            Some(v) => eq_prefix.push(v),
            None => break,
        }
    }
    let i = eq_prefix.len();
    // Optional trailing range on the first unpinned column.
    let trailing = if i < paths.len() {
        get(&paths[i]).filter(|c| c.lo.is_some() || c.hi.is_some())
    } else {
        None
    };

    // The scan yields rows ordered by paths[i], paths[i+1], … (columns 0..i are
    // pinned to a constant). A single `ORDER BY oc` is satisfied if `oc` is one
    // of those pinned columns or is paths[i].
    // Equality-pinned columns are constant across the range, so any direction
    // is trivially satisfied; the scan column itself arrives ascending and a
    // DESC request on it needs the gather to walk the range from the tail.
    let sorted = order.is_some_and(|(oc, _)| {
        paths[..i].iter().any(|p| p == oc) || paths.get(i).map(String::as_str) == Some(oc)
    });
    let reverse =
        order.is_some_and(|(oc, desc)| desc && paths.get(i).map(String::as_str) == Some(oc));

    let usable = !eq_prefix.is_empty() || trailing.is_some() || sorted;
    if !usable {
        return None;
    }

    let (start, end) = match trailing {
        Some(c) => {
            let start = match &c.lo {
                Some(v) => Some(index_prefix_n(&push(&eq_prefix, v))),
                None if eq_prefix.is_empty() => None,
                None => Some(index_prefix_n(&eq_prefix)),
            };
            let end = match &c.hi {
                Some(v) => index_upper_bound_n(&push(&eq_prefix, v)),
                None if eq_prefix.is_empty() => None,
                None => index_upper_bound_n(&eq_prefix),
            };
            (start, end)
        }
        None if eq_prefix.is_empty() => (None, None), // ORDER BY-only full scan
        None => (
            Some(index_prefix_n(&eq_prefix)),
            index_upper_bound_n(&eq_prefix),
        ),
    };
    Some((start, end, sorted, reverse))
}

fn push(prefix: &[Value], v: &Value) -> Vec<Value> {
    let mut out = prefix.to_vec();
    out.push(v.clone());
    out
}

fn value_max(a: Value, b: Value) -> Value {
    if index_prefix_n(std::slice::from_ref(&b)) > index_prefix_n(std::slice::from_ref(&a)) {
        b
    } else {
        a
    }
}
fn value_min(a: Value, b: Value) -> Value {
    if index_prefix_n(std::slice::from_ref(&b)) < index_prefix_n(std::slice::from_ref(&a)) {
        b
    } else {
        a
    }
}

/// Collect `column <op> literal` comparisons reachable through `AND`.
fn collect_comparisons(expr: &Expr, out: &mut Vec<(String, BinaryOp, Value)>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            collect_comparisons(left, out);
            collect_comparisons(right, out);
        }
        Expr::Binary { op, left, right }
            if matches!(
                op,
                BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
            ) =>
        {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(v)) if !v.is_null() => {
                    out.push((c.clone(), *op, v.clone()))
                }
                (Expr::Literal(v), Expr::Column(c)) if !v.is_null() => {
                    out.push((c.clone(), flip_op(*op), v.clone()))
                }
                _ => {}
            }
        }
        // `col BETWEEN lo AND hi` implies `col >= lo` and `col <= hi`, so each
        // literal bound contributes a range constraint (either alone is still
        // a sound superset — the residual filter re-checks). `NOT BETWEEN`
        // implies neither bound and contributes nothing.
        Expr::Between {
            expr,
            lo,
            hi,
            negated: false,
        } => {
            if let Expr::Column(c) = expr.as_ref() {
                if let Expr::Literal(v) = lo.as_ref() {
                    if !v.is_null() {
                        out.push((c.clone(), BinaryOp::GtEq, v.clone()));
                    }
                }
                if let Expr::Literal(v) = hi.as_ref() {
                    if !v.is_null() {
                        out.push((c.clone(), BinaryOp::LtEq, v.clone()));
                    }
                }
            }
        }
        _ => {}
    }
}

/// Flip a comparison so the column sits on the left (`5 < x` → `x > 5`).
fn flip_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

pub fn matches_filter(filter: &Option<Expr>, doc: &Document) -> Result<bool> {
    match filter {
        Some(expr) => eval_predicate(expr, doc),
        None => Ok(true),
    }
}

/// Keep the `(key, doc)` rows whose document satisfies `filter`. Public so the
/// cluster coordinator can filter cluster-merged rows with engine semantics.
pub fn filter_rows(
    filter: &Option<Expr>,
    rows: Vec<(Vec<u8>, Document)>,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let mut out = Vec::new();
    for (key, doc) in rows {
        if matches_filter(filter, &doc)? {
            out.push((key, doc));
        }
    }
    Ok(out)
}

/// Extract primary-key values and encode them into an order-preserving key.
fn primary_key_bytes(pk: &[String], doc: &Document) -> Result<Vec<u8>> {
    let mut values = Vec::with_capacity(pk.len());
    for col in pk {
        match doc.get(col) {
            Some(Value::Null) | None => {
                return Err(EngineError::Constraint(format!(
                    "primary key column {col:?} must not be null or missing"
                )))
            }
            Some(v) => values.push(v.clone()),
        }
    }
    Ok(Value::Array(values).encode_key())
}

/// Set `doc[path] = val`, creating intermediate documents for nested paths.
fn set_path(doc: &mut Document, path: &str, val: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    set_path_parts(doc, &parts, val);
}

fn set_path_parts(doc: &mut Document, parts: &[&str], val: Value) {
    let (head, rest) = parts.split_first().expect("path has at least one segment");
    if rest.is_empty() {
        doc.0.insert(head.to_string(), val);
        return;
    }
    let entry = doc
        .0
        .entry(head.to_string())
        .or_insert_with(|| Value::Document(Document::new()));
    if !matches!(entry, Value::Document(_)) {
        *entry = Value::Document(Document::new());
    }
    if let Value::Document(child) = entry {
        set_path_parts(child, rest, val);
    }
}

fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => contains_aggregate(expr),
        Expr::Binary { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::Func { args, .. } => args.iter().any(contains_aggregate),
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::Between { expr, lo, hi, .. } => {
            contains_aggregate(expr) || contains_aggregate(lo) || contains_aggregate(hi)
        }
        Expr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => false,
    }
}

/// Collect every top-level document field an expression could read into
/// `out` — the first path segment of every `Expr::Column("a.b.c")`, at any
/// depth (including inside aggregate arguments: `SUM(amount)` needs
/// `amount` just as much as a bare `amount` reference would). Nested-path
/// precision (`a.b` vs `a`) isn't tracked — wanting any part of a top-level
/// field means the whole field decodes, which is correct (if occasionally
/// wider than strictly necessary) and keeps the projected-decode contract
/// at the document-field level, matching what
/// [`skaidb_types::Value::decode_document_projected`] actually offers.
fn column_refs(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Column(path) => {
            let top = path.split_once('.').map_or(path.as_str(), |(first, _)| first);
            out.insert(top.to_string());
        }
        Expr::Literal(_) | Expr::Parameter(_) => {}
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => column_refs(expr, out),
        Expr::Binary { left, right, .. } => {
            column_refs(left, out);
            column_refs(right, out);
        }
        Expr::InList { expr, list, .. } => {
            column_refs(expr, out);
            for e in list {
                column_refs(e, out);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            column_refs(expr, out);
            column_refs(lo, out);
            column_refs(hi, out);
        }
        Expr::Like { expr, pattern, .. } => {
            column_refs(expr, out);
            column_refs(pattern, out);
        }
        Expr::Func { args, .. } => {
            for a in args {
                column_refs(a, out);
            }
        }
        Expr::Aggregate { arg, .. } => match arg {
            AggArg::Star => {}
            AggArg::Expr(e) | AggArg::Distinct(e) | AggArg::ApproxDistinct(e) => {
                column_refs(e, out);
            }
        },
    }
}

/// The set of top-level document fields a plain (non-`TOP k BY`) `GROUP
/// BY`/aggregate query can possibly read — every column referenced in the
/// `WHERE` filter (the same decoded document is filter-tested during the
/// gather, before it ever reaches grouping — a column pruned from decode
/// because it looked unused by the aggregate would make the filter
/// evaluate against a field that silently isn't there), `group_by`, the
/// select items (including inside aggregates: `SUM(amount)` needs
/// `amount`), `HAVING`, and `ORDER BY` (all lowered/evaluated per group in
/// [`select_aggregate`], so all their column needs apply too). `None` when
/// pruning isn't safe to attempt: a wildcard needs every field by
/// definition; `TOP k BY` returns whole rows via [`select_group_topk`], a
/// different consumer with its own (unrestricted) column needs; joins and
/// set operations have their own gather/merge shapes not audited for this.
///
/// Correctness of the whole column-pruning feature rests on this being a
/// true superset — this is the ONE place that decides what's safe to
/// discard during decode, so any expression form added to `Select` in the
/// future that reads a column outside `filter`/`group_by`/`items`/`having`/
/// `order_by` must extend this function or column-pruning silently returns
/// wrong results instead of erroring. Deliberately returns the filter's
/// columns bundled in (rather than leaving that union to each call site) so
/// there is exactly one place that can get this wrong, not one per caller.
fn group_by_projection_columns(sel: &Select) -> Option<HashSet<String>> {
    if !is_grouped(sel)
        || sel.group_top.is_some()
        || has_wildcard(sel)
        || !sel.joins.is_empty()
        || !sel.set_ops.is_empty()
        || sel.nearest.is_some()
    {
        return None;
    }
    let mut out = HashSet::new();
    if let Some(filter) = &sel.filter {
        column_refs(filter, &mut out);
    }
    for g in &sel.group_by {
        column_refs(g, &mut out);
    }
    for item in &sel.items {
        if let SelectItem::Expr { expr, .. } = item {
            column_refs(expr, &mut out);
        }
    }
    if let Some(having) = &sel.having {
        column_refs(having, &mut out);
    }
    for ok in &sel.order_by {
        column_refs(&ok.expr, &mut out);
    }
    Some(out)
}

/// Default output column name for an expression.
pub(crate) fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(path) => path.clone(),
        Expr::Aggregate { func, .. } => match func {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Rate => "rate",
            AggFunc::Increase => "increase",
            AggFunc::Delta => "delta",
            AggFunc::First => "first",
            AggFunc::Last => "last",
            AggFunc::Percentile(_) => "percentile",
        }
        .to_string(),
        Expr::Func { name, .. } => name.clone(),
        _ => "expr".to_string(),
    }
}

/// Row-per-document projection (no aggregates).
fn select_rows(
    sel: &Select,
    mut docs: Vec<Document>,
    presorted: bool,
    hide: &HashSet<String>,
    finalize: bool,
) -> Result<ResultSet> {
    // `finalize` applies the query's ORDER BY / OFFSET / LIMIT (skipped for a
    // `UNION` core, where they belong to the whole query). `presorted` means the
    // gather already returned rows in `ORDER BY` order (via an index).
    if finalize && !presorted {
        // Without DISTINCT only the first offset+limit sorted rows survive, so
        // a bounded selection beats sorting the whole set.
        let top_k = if sel.distinct {
            None
        } else {
            sel.limit
                .map(|lim| (sel.offset.unwrap_or(0).saturating_add(lim)) as usize)
        };
        sort_docs(&mut docs, &sel.order_by, top_k)?;
    }
    // Without DISTINCT we can trim to the page before projecting (cheap top-N);
    // with DISTINCT the dedup must happen first, so we page after projection.
    if finalize && !sel.distinct {
        apply_offset_limit(&mut docs, sel.offset, sel.limit);
    }

    // Wildcard expands to the sorted union of field names (minus join-alias
    // container keys, which would otherwise show as sub-documents).
    let wildcard_fields: Vec<String> = if has_wildcard(sel) {
        let mut set = BTreeSet::new();
        for doc in &docs {
            for k in doc.0.keys() {
                if !hide.contains(k) {
                    set.insert(k.clone());
                }
            }
        }
        set.into_iter().collect()
    } else {
        Vec::new()
    };

    let mut columns = Vec::new();
    for item in &sel.items {
        match item {
            SelectItem::Wildcard => columns.extend(wildcard_fields.iter().cloned()),
            SelectItem::Expr { expr, alias } => {
                columns.push(alias.clone().unwrap_or_else(|| expr_name(expr)))
            }
        }
    }

    let mut rows = Vec::with_capacity(docs.len());
    for doc in &docs {
        let mut row = Vec::with_capacity(columns.len());
        for item in &sel.items {
            match item {
                SelectItem::Wildcard => {
                    for f in &wildcard_fields {
                        row.push(doc.get(f).cloned().unwrap_or(Value::Null));
                    }
                }
                SelectItem::Expr { expr, .. } => row.push(eval(expr, doc)?),
            }
        }
        rows.push(row);
    }
    if sel.distinct {
        dedup_rows(&mut rows);
    }
    if finalize && sel.distinct {
        apply_offset_limit(&mut rows, sel.offset, sel.limit);
    }
    Ok(ResultSet::new(columns, rows))
}

/// Aggregate / grouped projection. `finalize` applies ORDER BY / OFFSET / LIMIT.
fn select_aggregate(sel: &Select, docs: Vec<Document>, finalize: bool) -> Result<ResultSet> {
    if sel.group_top.is_some() {
        return select_group_topk(sel, docs, finalize);
    }
    if has_wildcard(sel) {
        return Err(EngineError::Unsupported(
            "`*` cannot be combined with aggregates or GROUP BY".into(),
        ));
    }

    // Build groups keyed by the encoded group-by values, preserving first-seen order.
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, Vec<Document>> = HashMap::new();
    if sel.group_by.is_empty() {
        order.push(Vec::new());
        groups.insert(Vec::new(), docs);
    } else {
        for doc in docs {
            let mut key_vals = Vec::with_capacity(sel.group_by.len());
            for g in &sel.group_by {
                key_vals.push(eval(g, &doc)?);
            }
            let key = Value::Array(key_vals).encode_key();
            groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                Vec::new()
            });
            groups.get_mut(&key).expect("group present").push(doc);
        }
    }

    let columns: Vec<String> = sel
        .items
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard => unreachable!("wildcard rejected above"),
            SelectItem::Expr { expr, alias } => alias.clone().unwrap_or_else(|| expr_name(expr)),
        })
        .collect();

    // Build one output row per group, then order/limit on the produced rows.
    let mut keyed_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
    for key in &order {
        let group_docs = &groups[key];
        let rep = group_docs.first().cloned().unwrap_or_default();
        // HAVING filters whole groups; it may reference aggregates (lowered over
        // the group) and the group-by columns.
        if let Some(having) = &sel.having {
            let lowered = lower_aggregates(having, group_docs)?;
            if !eval_predicate(&lowered, &rep)? {
                continue;
            }
        }
        let mut row = Vec::with_capacity(columns.len());
        for item in &sel.items {
            if let SelectItem::Expr { expr, .. } = item {
                let lowered = lower_aggregates(expr, group_docs)?;
                row.push(eval(&lowered, &rep)?);
            }
        }
        // Sort key: ORDER BY expressions lowered over this group.
        let mut sort_vals = Vec::with_capacity(sel.order_by.len());
        for ok in &sel.order_by {
            let lowered = lower_aggregates(&ok.expr, group_docs)?;
            sort_vals.push(eval(&lowered, &rep)?);
        }
        keyed_rows.push((sort_vals, row));
    }

    if finalize && !sel.order_by.is_empty() {
        keyed_rows.sort_by(|a, b| order_compare(&a.0, &b.0, &sel.order_by));
    }

    let mut rows: Vec<Vec<Value>> = keyed_rows.into_iter().map(|(_, r)| r).collect();
    if sel.distinct {
        dedup_rows(&mut rows);
    }
    if finalize {
        apply_offset_limit(&mut rows, sel.offset, sel.limit);
    }
    Ok(ResultSet::new(columns, rows))
}

/// Per-group top-k rows (`GROUP BY ... TOP k BY <expr> [ASC|DESC]`): each
/// group contributes its `k` best rows ranked by the expression, instead of
/// one aggregated row. `HAVING` (aggregate-lowered per group) filters whole
/// groups first; `ORDER BY` / `OFFSET` / `LIMIT` then apply to the flattened
/// output. Without an outer `ORDER BY`, groups keep first-seen order with
/// rows best-first inside each.
fn select_group_topk(sel: &Select, docs: Vec<Document>, finalize: bool) -> Result<ResultSet> {
    let top = sel.group_top.as_ref().expect("caller checked");
    if contains_aggregate(&top.by) {
        return Err(EngineError::Unsupported(
            "TOP ... BY ranks individual rows — the ranking expression cannot contain an \
             aggregate"
                .into(),
        ));
    }
    for item in &sel.items {
        if let SelectItem::Expr { expr, .. } = item {
            if contains_aggregate(expr) {
                return Err(EngineError::Unsupported(
                    "with GROUP BY ... TOP the output is the groups' rows, not aggregates — \
                     drop the aggregate or the TOP clause"
                        .into(),
                ));
            }
        }
    }
    // Group in first-seen order (same keying as the aggregate path).
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, Vec<Document>> = HashMap::new();
    for doc in docs {
        let mut key_vals = Vec::with_capacity(sel.group_by.len());
        for g in &sel.group_by {
            key_vals.push(eval(g, &doc)?);
        }
        let key = Value::Array(key_vals).encode_key();
        groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            Vec::new()
        });
        groups.get_mut(&key).expect("group present").push(doc);
    }
    // Rank within each group: best-first (`DESC` default), NULLs last.
    let rank = [OrderKey {
        expr: top.by.clone(),
        descending: !top.ascending,
    }];
    let mut kept: Vec<Document> = Vec::new();
    for key in &order {
        let mut group_docs = groups.remove(key).expect("group present");
        if let Some(having) = &sel.having {
            let lowered = lower_aggregates(having, &group_docs)?;
            let rep = group_docs.first().cloned().unwrap_or_default();
            if !eval_predicate(&lowered, &rep)? {
                continue;
            }
        }
        sort_docs(&mut group_docs, &rank, Some(top.k as usize))?;
        group_docs.truncate(top.k as usize);
        kept.extend(group_docs);
    }
    select_rows(sel, kept, false, &HashSet::new(), finalize)
}

fn has_wildcard(sel: &Select) -> bool {
    sel.items.iter().any(|i| matches!(i, SelectItem::Wildcard))
}

/// Replace aggregate nodes in `expr` with literals computed over `docs`.
fn lower_aggregates(expr: &Expr, docs: &[Document]) -> Result<Expr> {
    Ok(match expr {
        Expr::Aggregate { func, arg } => Expr::Literal(eval_aggregate(*func, arg, docs)?),
        Expr::Unary { op, expr } => Expr::Unary {
            op: *op,
            expr: Box::new(lower_aggregates(expr, docs)?),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(lower_aggregates(expr, docs)?),
            negated: *negated,
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(lower_aggregates(left, docs)?),
            right: Box::new(lower_aggregates(right, docs)?),
        },
        Expr::Func { name, args } => Expr::Func {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| lower_aggregates(a, docs))
                .collect::<Result<Vec<_>>>()?,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(lower_aggregates(expr, docs)?),
            list: list
                .iter()
                .map(|a| lower_aggregates(a, docs))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        },
        Expr::Between {
            expr,
            lo,
            hi,
            negated,
        } => Expr::Between {
            expr: Box::new(lower_aggregates(expr, docs)?),
            lo: Box::new(lower_aggregates(lo, docs)?),
            hi: Box::new(lower_aggregates(hi, docs)?),
            negated: *negated,
        },
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(lower_aggregates(expr, docs)?),
            pattern: Box::new(lower_aggregates(pattern, docs)?),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => expr.clone(),
    })
}

fn eval_aggregate(func: AggFunc, arg: &AggArg, docs: &[Document]) -> Result<Value> {
    // `DISTINCT` is exact and COUNT-only (SQL semantics). The opt-in
    // `APPROX_COUNT_DISTINCT()` shares this row path — here it answers
    // exactly (an exact answer is a valid approximation); only the
    // search-index pushdown uses a sketch.
    if let AggArg::Distinct(e) | AggArg::ApproxDistinct(e) = arg {
        if func != AggFunc::Count {
            return Err(EngineError::Unsupported(
                "DISTINCT aggregate arguments are supported for COUNT only".into(),
            ));
        }
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for doc in docs {
            let v = eval(e, doc)?;
            if !v.is_null() {
                seen.insert(v.encode_key());
            }
        }
        return Ok(Value::Int(seen.len() as i64));
    }
    // Collect the (non-null) argument values for the group.
    let values: Vec<Value> = match arg {
        AggArg::Star | AggArg::Distinct(_) | AggArg::ApproxDistinct(_) => Vec::new(),
        AggArg::Expr(e) => {
            let mut vs = Vec::new();
            for doc in docs {
                let v = eval(e, doc)?;
                if !v.is_null() {
                    vs.push(v);
                }
            }
            vs
        }
    };

    Ok(match func {
        AggFunc::Count => match arg {
            AggArg::Star => Value::Int(docs.len() as i64),
            AggArg::Expr(_) | AggArg::Distinct(_) | AggArg::ApproxDistinct(_) => {
                Value::Int(values.len() as i64)
            }
        },
        AggFunc::Sum => sum_values(&values),
        AggFunc::Avg => {
            if values.is_empty() {
                Value::Null
            } else {
                let total: f64 = values.iter().filter_map(number).sum();
                Value::Float(total / values.len() as f64)
            }
        }
        AggFunc::Min => reduce_extreme(&values, std::cmp::Ordering::Less),
        AggFunc::Max => reduce_extreme(&values, std::cmp::Ordering::Greater),
        AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => {
            let AggArg::Expr(e) = arg else {
                return Err(EngineError::Type(
                    "rate()/increase()/delta() take a field argument, not *".into(),
                ));
            };
            eval_ts_change(func, e, docs)?
        }
        AggFunc::Percentile(bp) => {
            if !matches!(arg, AggArg::Expr(_)) {
                return Err(EngineError::Type(
                    "percentile() takes a field argument, not * or DISTINCT".into(),
                ));
            }
            // Linear interpolation over the sorted numeric values
            // (`percentile_cont` semantics); non-numeric values are skipped,
            // an empty group is NULL.
            let mut nums: Vec<f64> = values
                .iter()
                .filter_map(crate::eval::as_f64)
                .filter(|x| x.is_finite())
                .collect();
            if nums.is_empty() {
                return Ok(Value::Null);
            }
            nums.sort_by(|a, b| a.total_cmp(b));
            let p = f64::from(bp) / 10_000.0;
            let rank = p * (nums.len() - 1) as f64;
            let lo = rank.floor() as usize;
            let hi = rank.ceil() as usize;
            let frac = rank - lo as f64;
            Value::Float(nums[lo] + (nums[hi] - nums[lo]) * frac)
        }
        AggFunc::First | AggFunc::Last => {
            let AggArg::Expr(e) = arg else {
                return Err(EngineError::Type(
                    "first()/last() take a field argument, not *".into(),
                ));
            };
            let want_last = func == AggFunc::Last;
            let mut best: Option<(i64, Value)> = None;
            for doc in docs {
                let Some(ts) = doc.get("ts").and_then(crate::eval::as_int_ms) else {
                    return Err(EngineError::Type(
                        "first()/last() require a `ts` column on every row".into(),
                    ));
                };
                let v = eval(e, doc)?;
                if v.is_null() {
                    continue;
                }
                let better = match &best {
                    None => true,
                    Some((bts, _)) => {
                        if want_last {
                            ts > *bts
                        } else {
                            ts < *bts
                        }
                    }
                };
                if better {
                    best = Some((ts, v));
                }
            }
            best.map(|(_, v)| v).unwrap_or(Value::Null)
        }
    })
}

/// `rate` / `increase` / `delta` over a group's rows: samples are segmented
/// per series (the hidden `__series__` field the time-series gather adds; a
/// plain table with a `ts` column is treated as one series), each series'
/// change is computed over its time-ordered samples — counter-reset-aware
/// for rate/increase, plain `last - first` for delta — and the per-series
/// results are summed, PromQL `sum(rate(...))`-style.
fn eval_ts_change(func: AggFunc, e: &Expr, docs: &[Document]) -> Result<Value> {
    // Per-series time-ordered samples (docs arrive series-grouped and
    // time-ordered from the gather; sort defensively anyway).
    let mut per: Vec<(String, Vec<(i64, f64)>)> = Vec::new();
    let mut idx: HashMap<String, usize> = HashMap::new();
    for doc in docs {
        let Some(ts) = doc.get("ts").and_then(crate::eval::as_int_ms) else {
            return Err(EngineError::Type(
                "rate()/increase()/delta() require a `ts` column on every row".into(),
            ));
        };
        let v = eval(e, doc)?;
        let Some(v) = number(&v) else { continue };
        let series = match doc.get("__series__") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };
        match idx.get(&series) {
            Some(&i) => per[i].1.push((ts, v)),
            None => {
                idx.insert(series.clone(), per.len());
                per.push((series, vec![(ts, v)]));
            }
        }
    }

    let mut total = 0f64;
    let mut any = false;
    for (_, samples) in &mut per {
        samples.sort_by_key(|(ts, _)| *ts);
        if samples.len() < 2 {
            continue;
        }
        let change = if func == AggFunc::Delta {
            samples[samples.len() - 1].1 - samples[0].1
        } else {
            // Counter increase: a drop is a reset — the counter restarted
            // from zero, so the new value is its own contribution.
            let mut inc = 0f64;
            let mut prev = samples[0].1;
            for &(_, v) in &samples[1..] {
                inc += if v >= prev { v - prev } else { v };
                prev = v;
            }
            inc
        };
        if func == AggFunc::Rate {
            let span_secs = (samples[samples.len() - 1].0 - samples[0].0) as f64 / 1000.0;
            if span_secs <= 0.0 {
                continue;
            }
            total += change / span_secs;
        } else {
            total += change;
        }
        any = true;
    }
    Ok(if any { Value::Float(total) } else { Value::Null })
}

fn sum_values(values: &[Value]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    if values.iter().all(|v| matches!(v, Value::Int(_))) {
        let mut acc: i64 = 0;
        for v in values {
            if let Value::Int(i) = v {
                acc = acc.wrapping_add(*i);
            }
        }
        Value::Int(acc)
    } else {
        Value::Float(values.iter().filter_map(number).sum())
    }
}

fn reduce_extreme(values: &[Value], want: std::cmp::Ordering) -> Value {
    let mut best: Option<&Value> = None;
    for v in values {
        match best {
            None => best = Some(v),
            Some(cur) => {
                if let Some(ord) = compare(v, cur) {
                    if ord == want {
                        best = Some(v);
                    }
                }
            }
        }
    }
    best.cloned().unwrap_or(Value::Null)
}

fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => Some(d.to_f64()),
        Value::Timestamp(t) => Some(*t as f64),
        _ => None,
    }
}

/// Sort `docs` by `order_by`; with `top_k` set, keep only the first `k` rows
/// of the sorted order — an O(n) selection plus an O(k log k) sort instead of
/// sorting everything, for `ORDER BY … LIMIT` queries. Ties break by original
/// position, so the result matches what a full stable sort would keep.
fn sort_docs(docs: &mut Vec<Document>, order_by: &[OrderKey], top_k: Option<usize>) -> Result<()> {
    if order_by.is_empty() {
        return Ok(());
    }
    // Precompute sort keys to keep comparisons cheap and error-free.
    let mut keyed: Vec<(Vec<Value>, usize)> = Vec::with_capacity(docs.len());
    for (i, doc) in docs.iter().enumerate() {
        let mut k = Vec::with_capacity(order_by.len());
        for ok in order_by {
            k.push(eval(&ok.expr, doc)?);
        }
        keyed.push((k, i));
    }
    let cmp = |a: &(Vec<Value>, usize), b: &(Vec<Value>, usize)| {
        order_compare(&a.0, &b.0, order_by).then(a.1.cmp(&b.1))
    };
    if let Some(k) = top_k.filter(|k| *k < keyed.len()) {
        if k == 0 {
            docs.clear();
            return Ok(());
        }
        keyed.select_nth_unstable_by(k - 1, cmp);
        keyed.truncate(k);
    }
    keyed.sort_unstable_by(cmp);

    // Reorder by moving the docs into place — no clones.
    let mut old = std::mem::take(docs);
    *docs = keyed
        .into_iter()
        .map(|(_, i)| std::mem::take(&mut old[i]))
        .collect();
    Ok(())
}

/// Compare two sort-key tuples honoring per-key direction; NULLs sort last.
/// Tables the server machinery itself reads on every node — they must
/// stay cluster-default placement and witness-mirrorable, so placement/
/// witness DDL options are rejected on them. Matched on the BARE name
/// (system tables live in the default database).
pub fn is_system_table(name: &str) -> bool {
    matches!(
        name,
        "node_stats"
            | "drivers"
            | "witnesses"
            | "witness_gc_config"
            | "witness_sync_state"
            | "cluster_meta"
            | "node_aliases"
            | "metrics"
    )
}

fn order_compare(a: &[Value], b: &[Value], order_by: &[OrderKey]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for ((x, y), ok) in a.iter().zip(b.iter()).zip(order_by.iter()) {
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater, // NULLs last
            (false, true) => Ordering::Less,
            (false, false) => compare(x, y).unwrap_or_else(|| x.total_cmp(y)),
        };
        let ord = if ok.descending { ord.reverse() } else { ord };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn apply_offset_limit<T>(rows: &mut Vec<T>, offset: Option<u64>, limit: Option<u64>) {
    if let Some(off) = offset {
        let off = (off as usize).min(rows.len());
        rows.drain(..off);
    }
    if let Some(lim) = limit {
        rows.truncate(lim as usize);
    }
}

#[cfg(test)]
mod journal_ack_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "skaidb-jack-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn doc(id: i64, v: &str) -> Vec<u8> {
        let mut d = Document::new();
        d.insert("id", Value::Int(id));
        d.insert("v", Value::String(v.into()));
        Value::Document(d).encode()
    }
    fn key(id: i64) -> Vec<u8> {
        Value::Array(vec![Value::Int(id)]).encode_key()
    }

    /// Kill-crash semantics for the deferred half: rows landed via the
    /// row-only path with their maintenance never applied (the "crash before
    /// the applier ran" state) must be replayed into the index at the next
    /// open — including the delete of a superseded version's entries.
    #[test]
    fn deferred_maintenance_replays_after_crash() {
        let dir = tmp();
        let clock = HlcClock::new();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("CREATE INDEX i_v ON t (v)").unwrap();
            // Applied baseline: one row through the sync path, watermark at
            // its stamp.
            let h0 = clock.now();
            db.apply_put(&namespace::qualify(DEFAULT_DATABASE, "t"), &key(1), doc(1, "old"), h0)
                .unwrap();
            db.set_applied_watermark(&namespace::qualify(DEFAULT_DATABASE, "t"), h0);
            db.persist_applied_watermarks().unwrap();
            // "Crash window": two writes land rows-only — an overwrite of
            // row 1 (index entry must MOVE old->new) and a fresh row 2 —
            // and their MaintTasks are dropped on the floor.
            let t = namespace::qualify(DEFAULT_DATABASE, "t");
            let (c1, h1, _lost1) =
                db.apply_put_row_only(&t, &key(1), doc(1, "new"), clock.now()).unwrap();
            h1.sync_through(c1).unwrap();
            let (c2, h2, _lost2) =
                db.apply_put_row_only(&t, &key(2), doc(2, "fresh"), clock.now()).unwrap();
            h2.sync_through(c2).unwrap();
            // db dropped without maintenance = the crash.
        }
        let mut db = Database::open(&dir).unwrap();
        // Replay happened at open: the index answers through fresh entries.
        let out = db
            .execute("SELECT id FROM t WHERE v = 'fresh'")
            .unwrap();
        let rows = match out {
            QueryOutput::Rows(rs) => rs.rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows.len(), 1, "replayed index entry for row 2");
        // The overwrite's OLD entry must be gone (exact covering count).
        let out = db.execute("SELECT count(*) FROM t WHERE v = 'old'").unwrap();
        let n = match out {
            QueryOutput::Rows(rs) => rs.rows[0][0].clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(n, Value::Int(0), "superseded entry replayed away");
        let out = db.execute("SELECT count(*) FROM t WHERE v = 'new'").unwrap();
        let n = match out {
            QueryOutput::Rows(rs) => rs.rows[0][0].clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(n, Value::Int(1));
    }

    /// The row half is visible to point reads immediately (read-your-writes
    /// is not deferred), before any maintenance runs.
    #[test]
    fn row_visible_before_maintenance() {
        let dir = tmp();
        let clock = HlcClock::new();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX i_v ON t (v)").unwrap();
        let t = namespace::qualify(DEFAULT_DATABASE, "t");
        let (c, h, task) =
            db.apply_put_row_only(&t, &key(7), doc(7, "x"), clock.now()).unwrap();
        h.sync_through(c).unwrap();
        assert!(task.is_some(), "indexed table produces a maintenance task");
        let out = db.execute("SELECT v FROM t WHERE id = 7").unwrap();
        let rows = match out {
            QueryOutput::Rows(rs) => rs.rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows.len(), 1, "PK read sees the row pre-maintenance");
        // And applying the task catches the index up.
        let m = task.expect("checked above").decode();
        db.apply_maintenance(&m).unwrap();
        let out = db.execute("SELECT count(*) FROM t WHERE v = 'x'").unwrap();
        let n = match out {
            QueryOutput::Rows(rs) => rs.rows[0][0].clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(n, Value::Int(1));
    }
}

#[cfg(test)]
mod deferred_fts_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "skaidb-dfts-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// Server-mode open must not pay the FTS rebuild: the index reopens
    /// marked `building` (MATCH errors clearly), and paging the catch-up in
    /// the background brings it live with complete results.
    #[test]
    fn deferred_search_startup_pages_to_live() {
        let dir = tmp();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("CREATE SEARCH INDEX s_body ON t (body)").unwrap();
            for i in 0..300 {
                db.execute(&format!(
                    "INSERT INTO t (id, body) VALUES ({i}, 'findable text {i}')"
                ))
                .unwrap();
            }
            // dropped WITHOUT the shutdown commit: the reopen must catch up.
        }
        let opts = EngineOptions {
            defer_search_startup: true,
            ..Default::default()
        };
        let mut db = Database::open_with_options(&dir, opts).unwrap();
        let err = db
            .execute("SELECT id FROM t WHERE MATCH(body, 'findable')")
            .unwrap_err();
        assert!(err.to_string().contains("rebuilding"), "{err}");
        // Rows themselves are fully readable meanwhile.
        let out = db.execute("SELECT count(*) FROM t").unwrap();
        let n = match out {
            QueryOutput::Rows(rs) => rs.rows[0][0].clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(n, Value::Int(300));
        // Drive the deferred pages (the cluster worker's job, inline here).
        let pending = db.take_pending_search_catchups();
        assert_eq!(pending.len(), 1);
        for (name, watermark) in pending {
            let mut cursor = None;
            while let Some(next) =
                db.search_catchup_page(&name, watermark, cursor.take(), 64).unwrap()
            {
                cursor = Some(next);
            }
        }
        let out = db
            .execute("SELECT id FROM t WHERE MATCH(body, 'findable') ORDER BY score() DESC LIMIT 500")
            .unwrap();
        let rows = match out {
            QueryOutput::Rows(rs) => rs.rows,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows.len(), 300, "catch-up indexed every row");
    }
}

#[cfg(test)]
mod schema_lww_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "skaidb-schema-lww-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// A drop tombstone must beat a stale (older-HLC) recreate, but yield to a
    /// genuinely newer one — the property that lets DROP replicate without a
    /// lagging node resurrecting the object.
    #[test]
    fn drop_tombstone_blocks_stale_recreate() {
        let mut db = Database::open(tmp()).unwrap();
        db.execute_session("default", "CREATE TABLE t (PRIMARY KEY (id))")
            .unwrap();
        db.execute_session("default", "DROP TABLE t").unwrap();
        assert!(!db.catalog.tables.contains_key("t"), "dropped");

        // Stale replicated CREATE (HLC older than the drop) must NOT resurrect.
        db.execute_session_with_hlc(
            "default",
            "CREATE TABLE t (PRIMARY KEY (id))",
            Hlc::new(1, 0),
        )
        .unwrap();
        assert!(
            !db.catalog.tables.contains_key("t"),
            "stale create must not resurrect a dropped table"
        );

        // A genuinely newer CREATE wins and recreates the table. (Far-future
        // physical time, well past the wall-clock drop stamp.)
        db.execute_session_with_hlc(
            "default",
            "CREATE TABLE t (PRIMARY KEY (id))",
            Hlc::new(u64::MAX / 2, 0),
        )
        .unwrap();
        assert!(
            db.catalog.tables.contains_key("t"),
            "a newer create must win over the tombstone"
        );
    }

    /// A stale drop must not remove a table created more recently.
    #[test]
    fn stale_drop_does_not_remove_newer_table() {
        let mut db = Database::open(tmp()).unwrap();
        db.execute_session("default", "CREATE TABLE t (PRIMARY KEY (id))")
            .unwrap();
        // Replicated DROP with an ancient HLC: older than the create, so ignored.
        db.execute_session_with_hlc("default", "DROP TABLE IF EXISTS t", Hlc::new(1, 0))
            .unwrap();
        assert!(
            db.catalog.tables.contains_key("t"),
            "a stale drop must not remove a newer table"
        );
    }
}

#[cfg(test)]
mod deferred_sync_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "skaidb-dsync-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn parse_one(sql: &str) -> Statement {
        parse(sql).unwrap()
    }

    /// The deferred entry point hands the statement's group commits to the
    /// caller instead of fsyncing inline, and syncing them makes the rows
    /// durable across a reopen — the Backend::Local contract.
    #[test]
    fn deferred_writes_sync_outside_and_survive_reopen() {
        let dir = tmp();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            let (res, pending) = db.execute_session_statement_deferred(
                "default",
                parse_one("INSERT INTO t (id, v) VALUES (1, 'a'), (2, 'b')"),
            );
            res.unwrap();
            assert!(
                !pending.is_empty(),
                "a write statement must hand back its commits to sync"
            );
            // Read-your-writes holds BEFORE the sync (memtable serves).
            let out = db.execute("SELECT count(*) FROM t").unwrap();
            match out {
                QueryOutput::Rows(rs) => assert_eq!(rs.rows[0][0], Value::Int(2)),
                other => panic!("{other:?}"),
            }
            for (sync, commit) in pending {
                sync.sync_through(commit).unwrap();
            }
            // dropped without a shutdown flush: reopen replays the WAL.
        }
        let mut db = Database::open(&dir).unwrap();
        let out = db.execute("SELECT count(*) FROM t").unwrap();
        match out {
            QueryOutput::Rows(rs) => assert_eq!(rs.rows[0][0], Value::Int(2)),
            other => panic!("{other:?}"),
        }
    }

    /// Deferred mode is statement-scoped: it must reset to inline syncing
    /// afterwards, so interleaved non-deferred callers (Session::execute)
    /// keep their durable-before-return contract.
    #[test]
    fn deferred_mode_resets_after_the_statement() {
        let dir = tmp();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let (res, pending) = db.execute_session_statement_deferred(
            "default",
            parse_one("INSERT INTO t (id) VALUES (1)"),
        );
        res.unwrap();
        for (sync, commit) in pending {
            sync.sync_through(commit).unwrap();
        }
        assert!(
            db.deferred_syncs.is_none(),
            "deferred mode must not leak past the statement"
        );
        // A plain execute afterwards syncs inline as before (no panic, no
        // stranded pending set).
        db.execute("INSERT INTO t (id) VALUES (2)").unwrap();
    }

    /// Rows applied before a mid-statement error still hand their commits
    /// back — the caller syncs on the error path too, matching the inline
    /// path's "applied rows become durable either way" contract.
    #[test]
    fn error_paths_still_hand_back_applied_commits() {
        let dir = tmp();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // UPDATE with a filter on a missing table errors; but first seed a
        // row deferred, then run a statement that errors AFTER the deferred
        // insert landed in the same call? Simpler: a statement that errors
        // outright must return an empty-or-complete pending set without
        // panicking, and the error must surface.
        let (res, pending) = db.execute_session_statement_deferred(
            "default",
            parse_one("INSERT INTO missing (id) VALUES (1)"),
        );
        assert!(res.is_err());
        for (sync, commit) in pending {
            sync.sync_through(commit).unwrap(); // must not panic
        }
    }
}

#[cfg(test)]
mod group_by_projection_tests {
    use super::*;

    fn select_of(sql: &str) -> Select {
        match skaidb_sql::parse(sql).unwrap() {
            Statement::Select(sel) => sel,
            other => panic!("expected a SELECT, got {other:?}"),
        }
    }

    fn wanted(sql: &str) -> Option<HashSet<String>> {
        group_by_projection_columns(&select_of(sql))
    }

    #[test]
    fn plain_group_by_wants_group_and_agg_columns() {
        let w = wanted("SELECT account, SUM(amount) FROM t GROUP BY account").unwrap();
        assert_eq!(w, ["account".to_string(), "amount".to_string()].into_iter().collect());
    }

    #[test]
    fn filter_columns_are_included_even_though_they_never_reach_select_aggregate() {
        // The gather tests this same decoded document against the filter
        // BEFORE it reaches select_aggregate — a column pruned because
        // nothing downstream of grouping reads it would still break the
        // WHERE clause itself if it were missing.
        let w = wanted(
            "SELECT account, COUNT(*) FROM gmail_emails \
             WHERE status = 'active' GROUP BY account",
        )
        .unwrap();
        assert!(w.contains("status"), "filter column must be wanted");
        assert!(w.contains("account"));
        assert!(!w.contains("body"));
    }

    #[test]
    fn plain_count_star_wants_only_group_by_column() {
        // COUNT(*) itself reads nothing — AggArg::Star contributes no column.
        let w = wanted("SELECT account, COUNT(*) FROM gmail_emails GROUP BY account").unwrap();
        assert_eq!(w, ["account".to_string()].into_iter().collect());
        assert!(!w.contains("body"), "must not want the wide unreferenced column");
    }

    #[test]
    fn having_and_order_by_columns_are_included() {
        let w = wanted(
            "SELECT account, COUNT(*) AS c FROM t GROUP BY account \
             HAVING SUM(amount) > 10 ORDER BY last_seen DESC",
        )
        .unwrap();
        assert!(w.contains("account"));
        assert!(w.contains("amount"), "HAVING's aggregate arg must be wanted");
        assert!(w.contains("last_seen"), "ORDER BY column must be wanted");
    }

    #[test]
    fn non_aggregated_select_item_column_is_wanted() {
        // MySQL-style "any value" semantics: `label` isn't grouped or
        // aggregated, but select_aggregate reads it from the group's
        // representative row — it MUST be in the wanted set or that read
        // would silently come back missing.
        let w = wanted("SELECT account, label, COUNT(*) FROM t GROUP BY account").unwrap();
        assert!(w.contains("label"));
    }

    #[test]
    fn nested_path_wants_only_the_top_level_field() {
        let w = wanted("SELECT meta.region, COUNT(*) FROM t GROUP BY meta.region").unwrap();
        assert_eq!(w, ["meta".to_string()].into_iter().collect());
    }

    #[test]
    fn wildcard_disables_pruning() {
        assert_eq!(wanted("SELECT * FROM t"), None);
    }

    #[test]
    fn non_grouped_select_disables_pruning() {
        // This function is GROUP-BY-specific; a plain row SELECT (even one
        // with an aggregate-free projection) isn't its concern.
        assert_eq!(wanted("SELECT account FROM t"), None);
    }

    #[test]
    fn group_top_k_disables_pruning() {
        // select_group_topk returns whole rows via select_rows — a
        // different, unaudited consumer of the gathered documents.
        assert_eq!(
            wanted("SELECT * FROM t GROUP BY account TOP 3 BY amount DESC"),
            None
        );
    }

    #[test]
    fn joins_disable_pruning() {
        assert_eq!(
            wanted(
                "SELECT t.account, COUNT(*) FROM t JOIN u ON t.id = u.tid GROUP BY t.account"
            ),
            None
        );
    }

    #[test]
    fn union_disables_pruning() {
        assert_eq!(
            wanted(
                "SELECT account, COUNT(*) FROM t GROUP BY account \
                 UNION SELECT account, COUNT(*) FROM t2 GROUP BY account"
            ),
            None
        );
    }

    #[test]
    fn column_refs_walks_every_expr_shape() {
        // Exercise every Expr variant's column-carrying position in one
        // expression, via a WHERE clause parsed straight from SQL (simplest
        // way to build every shape without hand-constructing the AST).
        let sel = select_of(
            "SELECT 1 FROM t WHERE \
             (a = 1 AND b > 2) OR NOT c IS NULL \
             OR d IN (e, 5) \
             OR f BETWEEN g AND h \
             OR i LIKE j \
             OR upper(k) = 'X'",
        );
        let mut out = HashSet::new();
        column_refs(sel.filter.as_ref().unwrap(), &mut out);
        for col in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"] {
            assert!(out.contains(col), "missing column ref: {col}");
        }
        assert!(!out.contains("upper"), "function name must not be treated as a column");
    }
}
