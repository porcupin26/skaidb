//! The embeddable query engine: parse, plan, and execute against storage.
//!
//! One [`storage::Engine`] backs each table (a table is a namespace, SPEC §2).
//! Rows are documents keyed by their primary key, encoded with the
//! order-preserving key codec so scans come back in key order.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use skaidb_sql::ast::{
    AggArg, AggFunc, AlterAction, AlterTable, BinaryOp, CreateSearchIndex, Delete, Expr, Insert,
    JoinKind, OrderKey, Select, SelectItem, Statement, UnaryOp, Update,
};
use skaidb_sql::parse;
use std::sync::Arc;

use skaidb_storage::{
    Engine as StorageEngine, EngineOptions, Hlc, HlcClock, VersionValue, WalCommit, WalSync,
};
use skaidb_types::{Document, Value};

use skaidb_tsdb::{Tsdb, TsdbOptions};

use skaidb_fts::{SearchIndex, SearchIndexConfig, SearchQuery, Watermark};

use crate::catalog::{AuthRoleDef, Catalog, IndexDef, RollupDef, SchemaVersion, SearchIndexDef, TableDef, TsTableDef, UserDef, VectorIndexDef};
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

/// `(key, document)` rows — the engine's standard row-gather result.
type KeyedRows = Vec<(Vec<u8>, Document)>;
/// Gathered rows plus whether they are already in the requested `ORDER BY` order.
type OrderedRows = (KeyedRows, bool);
/// A chosen index access path: `(index_name, start_key, end_key, sorted)` where
/// `sorted` is whether the scan order already satisfies the query's `ORDER BY`.
type IndexPlan = (String, Option<Vec<u8>>, Option<Vec<u8>>, bool);

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
}

/// Per-table live-key / tombstone / size breakdown.
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    pub name: String,
    pub live_keys: u64,
    pub tombstones: u64,
    pub disk_bytes: u64,
    pub sstables: u64,
}

/// Buffered writes of an open transaction: `(table, key) -> Some(doc)` for a
/// put, `None` for a delete. Reads during the transaction merge this over
/// committed storage (read-your-writes); `COMMIT` flushes it to storage and
/// `ROLLBACK` discards it. Embedded, single-connection only.
#[derive(Debug, Default)]
struct TxnBuffer {
    writes: BTreeMap<(String, Vec<u8>), Option<Document>>,
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
            let engine = StorageEngine::open_with_options(table_dir(&dir, name), opts)?;
            tables.insert(name.clone(), engine);
        }

        let mut indexes = HashMap::new();
        for name in catalog.indexes.keys() {
            let engine = StorageEngine::open_with_options(index_dir(&dir, name), opts)?;
            indexes.insert(name.clone(), engine);
        }

        let mut timeseries = HashMap::new();
        for (name, def) in &catalog.timeseries {
            timeseries.insert(name.clone(), open_tsdb(&dir, name, def, opts.ts_head_max_bytes)?);
        }

        // Vector indexes live in memory; rebuild each from its table's rows.
        let rebuild_start = std::time::Instant::now();
        let mut vector_indexes = HashMap::new();
        for (name, def) in &catalog.vector_indexes {
            let mut hnsw = new_hnsw(def);
            if let Some(engine) = tables.get(&def.table) {
                for (key, bytes) in engine.scan()? {
                    if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                        if let Some(v) = doc_vector(&doc, &def.path, def.dim) {
                            hnsw.insert(key, v);
                        }
                    }
                }
            }
            vector_indexes.insert(name.clone(), hnsw);
        }
        let vector_rebuild_ms = rebuild_start.elapsed().as_millis() as u64;

        // Search indexes persist their own segments; reopen each and replay
        // table rows newer than its committed watermark (writes lost to a
        // crash or the no-commit-on-drop shutdown). A missing, corrupt, or
        // definition-mismatched index is wiped and rebuilt from the table.
        let rebuild_start = std::time::Instant::now();
        let mut search_indexes = HashMap::new();
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
            if let Some(engine) = tables.get(&def.table) {
                match index.committed_watermark() {
                    // Catch-up: replay every put/delete stamped after the
                    // watermark (deletes included, so a row removed while the
                    // delete was uncommitted stays removed).
                    Some(w) => {
                        let watermark = watermark_to_hlc(w);
                        for (key, hlc, value) in engine.scan_versioned_with_tombstones()? {
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
                        for (key, bytes, hlc) in engine.scan_versioned()? {
                            if let Ok(Value::Document(doc)) = Value::decode(&bytes) {
                                index.put(&key, &doc, hlc_to_watermark(hlc))?;
                            }
                        }
                    }
                }
            }
            if index.dirty() {
                index.commit()?;
            }
            search_indexes.insert(
                name.clone(),
                LiveSearchIndex {
                    index,
                    last_commit: std::time::Instant::now(),
                    refresh_ms,
                },
            );
        }
        let search_rebuild_ms = rebuild_start.elapsed().as_millis() as u64;

        let role_store = build_role_store(&catalog);
        Ok(Database {
            dir,
            storage_opts: opts,
            catalog,
            tables,
            timeseries,
            role_store,
            indexes,
            vector_indexes,
            search_indexes,
            txn: None,
            clock,
            ddl_hlc: None,
            vector_rebuild_ms,
            search_rebuild_ms,
        })
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
    pub fn create_vector_index(
        &mut self,
        name: &str,
        table: &str,
        path: &str,
        metric: &str,
        dim: Option<usize>,
    ) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(table) {
            return Err(EngineError::TableNotFound(table.to_string()));
        }
        let hlc = self.ddl_stamp();
        let key = format!("v:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.vector_indexes.contains_key(name) {
            return Err(EngineError::IndexExists(name.to_string()));
        }
        if Metric::parse(metric).is_none() {
            return Err(EngineError::Constraint(format!("unknown vector metric '{metric}'")));
        }
        let rows = self.scan_docs(table)?;
        let dim = match dim {
            Some(d) => d,
            None => rows
                .iter()
                .find_map(|(_, doc)| doc_vector_raw(doc, path).map(|v| v.len()))
                .ok_or_else(|| {
                    EngineError::Constraint(format!(
                        "cannot infer vector dimension: no row of '{table}' has a numeric array at '{path}'"
                    ))
                })?,
        };

        let def = VectorIndexDef {
            table: table.to_string(),
            path: path.to_string(),
            metric: metric.to_ascii_lowercase(),
            dim,
            ef_search: None,
        };
        let mut hnsw = new_hnsw(&def);
        for (key, doc) in &rows {
            if let Some(v) = doc_vector(doc, path, dim) {
                hnsw.insert(key.clone(), v);
            }
        }
        self.vector_indexes.insert(name.to_string(), hnsw);
        self.catalog.vector_indexes.insert(name.to_string(), def);
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
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.search_indexes.contains_key(&c.name) {
            if c.if_not_exists {
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
        for (row_key, bytes, row_hlc) in engine.scan_versioned()? {
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
            for (row_key, bytes, row_hlc) in engine.scan_versioned()? {
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
            },
        );
        Ok(())
    }

    /// Approximate `k` nearest rows to `query` under the named vector index,
    /// optionally restricted to rows matching `filter` (filtered ANN). Returns
    /// `(key, doc, distance)` nearest-first.
    pub fn vector_search(
        &self,
        index: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
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

        // The HNSW only knows keys; resolve each candidate to its row to apply
        // the filter (filtered nearest-neighbor search). Decoded docs are kept
        // so each candidate is read and decoded once, and survivors are served
        // from the same map instead of a second storage read.
        let decoded: std::cell::RefCell<HashMap<Vec<u8>, Option<Document>>> =
            std::cell::RefCell::new(HashMap::new());
        let hits = hnsw.search(query, k, |key| {
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
        let hnsw = self
            .vector_indexes
            .get(index)
            .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?;
        Ok(hnsw.search(query, k, |_| true))
    }

    /// The table a vector index is defined on, if it exists locally.
    pub fn vector_index_table(&self, index: &str) -> Option<String> {
        self.catalog
            .vector_indexes
            .get(index)
            .map(|d| d.table.clone())
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

    fn vector_indexes_on(&self, table: &str) -> Vec<(String, String)> {
        self.catalog
            .vector_indexes
            .iter()
            .filter(|(_, def)| def.table == table)
            .map(|(name, def)| (name.clone(), def.path.clone()))
            .collect()
    }

    /// Update the vector index `name` for `doc` at `key` (insert/replace), or
    /// remove the entry when the doc has no vector at `path`.
    fn vector_index_put(&mut self, name: &str, path: &str, doc: &Document, key: &[u8]) {
        let dim = self.vector_indexes.get(name).map(|h| h.dim());
        if let (Some(dim), Some(hnsw)) = (dim, self.vector_indexes.get_mut(name)) {
            match doc_vector(doc, path, dim) {
                Some(v) => hnsw.insert(key.to_vec(), v),
                None => hnsw.remove(key),
            }
        }
    }

    fn vector_index_del(&mut self, name: &str, key: &[u8]) {
        if let Some(hnsw) = self.vector_indexes.get_mut(name) {
            hnsw.remove(key);
        }
    }

    /// Maintain every vector index on `table` for a written row.
    fn maintain_vectors_put(&mut self, table: &str, doc: &Document, key: &[u8]) {
        for (name, path) in self.vector_indexes_on(table) {
            self.vector_index_put(&name, &path, doc, key);
        }
    }

    /// Maintain every vector index on `table` for a deleted row.
    fn maintain_vectors_del(&mut self, table: &str, key: &[u8]) {
        for (name, _) in self.vector_indexes_on(table) {
            self.vector_index_del(&name, key);
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
    pub(crate) fn search_refresh(&mut self, table: &str) -> Result<()> {
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
        highlights: &[(String, usize)],
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
            .ok_or(EngineError::IndexNotFound(name))?;
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
        highlights: &[(String, usize)],
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
        highlights: &[(String, usize)],
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
            .map(|(col, max_chars)| {
                let h = live.index.highlighter(query, col, *max_chars)?;
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
                doc.insert(format!("_highlight_{col}"), Value::String(snippet));
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
        max_chars: usize,
    ) -> Result<skaidb_fts::Highlighter> {
        let name = self.search_index_for_query(table, query)?;
        let live = self
            .search_indexes
            .get(&name)
            .ok_or(EngineError::IndexNotFound(name))?;
        Ok(live.index.highlighter(query, column, max_chars)?)
    }

    /// Full-text search over the last-committed index state (see
    /// [`Database::search_commit_if_dirty`]).
    pub fn search_read(
        &self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
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
        highlights: &[(String, usize)],
    ) -> Result<Vec<(Vec<u8>, Document, f32)>> {
        let live = self
            .search_indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?;
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        // Snippet generators are per-(query, column), built once and applied
        // to each hit's authoritative row text.
        let highlighters = highlights
            .iter()
            .map(|(col, max_chars)| {
                let h = live.index.highlighter(query, col, *max_chars)?;
                Ok((col.as_str(), h))
            })
            .collect::<Result<Vec<_>>>()?;
        // The index only knows keys; re-read each hit's row (authoritative)
        // to apply the residual filter and return the document.
        let resolve = |key: &[u8]| -> Result<Option<Document>> {
            match table_engine.get(key)? {
                Some(bytes) => match Value::decode(&bytes) {
                    Ok(Value::Document(mut doc)) if matches_filter(filter, &doc)? => {
                        for (col, h) in &highlighters {
                            let snippet = h.snippet_doc(&doc, col);
                            doc.insert(format!("_highlight_{col}"), Value::String(snippet));
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
                    k.saturating_mul(4).max(k + 16)
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

    /// Parse and execute a single **read-only** statement (`SELECT` / `SHOW …`)
    /// through `&self`, so callers holding the database behind an `RwLock` can
    /// serve reads under a shared lock and run them concurrently. A statement
    /// that is not read-only (see [`statement_is_read_only`]) is rejected —
    /// route it through [`Database::execute`] instead.
    pub fn execute_read(&self, sql: &str) -> Result<QueryOutput> {
        self.execute_read_statement(parse(sql)?)
    }

    /// Execute an already-parsed read-only statement; see
    /// [`Database::execute_read`]. A `SELECT` inside an open transaction still
    /// works here: it reads through the buffered overlay, which needs only
    /// shared access.
    pub fn execute_read_statement(&self, stmt: Statement) -> Result<QueryOutput> {
        let mut stmt = stmt;
        skaidb_sql::resolve_now(&mut stmt, now_ms());
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
            Statement::ShowTables => Ok(QueryOutput::Rows(self.show_tables())),
            Statement::ShowIndexes => Ok(QueryOutput::Rows(self.show_indexes())),
            Statement::ShowStatus => Ok(QueryOutput::Rows(self.show_status())),
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
        let mut stmt = stmt;
        skaidb_sql::resolve_now(&mut stmt, now_ms());
        match stmt {
            Statement::CreateTable(ct) => {
                self.create_table(&ct.name, ct.primary_key, ct.if_not_exists)
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
                self.create_index(&ci.name, &ci.table, &ci.paths, ci.if_not_exists)
            }
            Statement::DropIndex { name, if_exists } => self.drop_index(&name, if_exists),
            Statement::CreateVectorIndex(ci) => {
                self.create_vector_index(&ci.name, &ci.table, &ci.path, &ci.metric, Some(ci.dim))
            }
            Statement::DropVectorIndex { name, if_exists } => {
                self.drop_vector_index(&name, if_exists)
            }
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
            Statement::AlterTable(alt) => self.alter_table(alt),
            Statement::Begin => self.begin(),
            Statement::Commit => self.commit(),
            Statement::Rollback => self.rollback(),
            Statement::ShowTables => Ok(QueryOutput::Rows(self.show_tables())),
            Statement::ShowIndexes => Ok(QueryOutput::Rows(self.show_indexes())),
            Statement::ShowStatus => Ok(QueryOutput::Rows(self.show_status())),
            Statement::CreateUser(cu) => self.create_user(
                &cu.name,
                cu.password.as_deref(),
                cu.verifier.as_deref(),
                cu.if_not_exists,
                false,
            ),
            Statement::AlterUser { name, password } => {
                self.create_user(&name, Some(&password), None, false, true)
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
            | Statement::ExplainScore { .. } => {
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
            .filter(|(name, _)| namespace::belongs_to(name, current_db))
            .map(|(name, def)| {
                vec![
                    Value::String(namespace::split(name).1.to_string()),
                    Value::String(def.primary_key.join(", ")),
                ]
            })
            .collect();
        for (name, def) in &self.catalog.timeseries {
            if namespace::belongs_to(name, current_db) {
                rows.push(vec![
                    Value::String(namespace::split(name).1.to_string()),
                    Value::String(format!("{}, ts", def.series_key.join(", "))),
                ]);
            }
        }
        rows.sort_by(|a, b| a[0].total_cmp(&b[0]));
        ResultSet {
            columns: vec!["table".into(), "primary_key".into()],
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
                Value::String("secondary".into()),
                Value::String(idx.paths.join(", ")),
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
            ],
            rows,
        }
    }

    // ---- DDL ----

    fn create_table(
        &mut self,
        name: &str,
        pk: Vec<String>,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        if pk.is_empty() {
            return Err(EngineError::Constraint(
                "primary key must have at least one column".into(),
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
        let engine = StorageEngine::open_with_options(table_dir(&self.dir, name), self.storage_opts)?;
        self.tables.insert(name.to_string(), engine);
        self.catalog
            .tables
            .insert(name.to_string(), TableDef { primary_key: pk });
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
        let credential = match (password, verifier) {
            (Some(pw), _) => {
                // Deterministic per-user salt (matches the server's existing
                // scheme; the salt travels inside the encoded verifier).
                let salt = skaidb_auth::crypto::sha256(format!("skaidb-user:{name}").as_bytes())
                    [..16]
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
                    "CREATE USER requires PASSWORD or VERIFIER".into(),
                ))
            }
        };
        let hlc = self.ddl_stamp();
        let key = format!("usr:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        self.catalog
            .users
            .insert(name.to_string(), UserDef { credential });
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
                vec![Value::String(r), Value::String(p), Value::String(o)]
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

    /// The stored SCRAM credential for `name`, if the user exists.
    pub fn auth_user(&self, name: &str) -> Option<ScramCredential> {
        ScramCredential::decode(&self.catalog.users.get(name)?.credential)
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
        // Drop the table's indexes too, tombstoning each so the drop replicates.
        let dropped: Vec<String> = self
            .catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == name)
            .map(|(n, _)| n.clone())
            .collect();
        for index_name in dropped {
            self.catalog.indexes.remove(&index_name);
            self.indexes.remove(&index_name);
            self.record_schema(format!("i:{index_name}"), hlc, true);
            let idir = index_dir(&self.dir, &index_name);
            if idir.exists() {
                std::fs::remove_dir_all(idir)?;
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
                self.record_schema(key, hlc, false);
                self.save_catalog()?;
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::IndexExists(name.to_string()));
        }
        // Create the index store and backfill it from the existing rows.
        let mut index_engine = StorageEngine::open_with_options(index_dir(&self.dir, name), self.storage_opts)?;
        for (row_key, doc) in self.scan_docs(table)? {
            let values = index_values(&doc, paths);
            index_engine.put(&index_entry_key(&values, &row_key), row_key.clone())?;
        }
        self.indexes.insert(name.to_string(), index_engine);
        self.catalog.indexes.insert(
            name.to_string(),
            IndexDef {
                table: table.to_string(),
                paths: paths.to_vec(),
            },
        );
        self.record_schema(key, hlc, false);
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        let hlc = self.ddl_stamp();
        let key = format!("i:{name}");
        if !self.schema_advances(&key, hlc) {
            return Ok(QueryOutput::Ddl);
        }
        if self.catalog.indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
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
        if !self.catalog.tables.contains_key(&alt.name) {
            return Err(EngineError::TableNotFound(alt.name.clone()));
        }
        match alt.action {
            AlterAction::RenameTable { new_name } => self.rename_table(&alt.name, &new_name),
            AlterAction::RenameColumn { from, to } => self.rename_column(&alt.name, &from, &to),
        }
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
            .insert(new.to_string(), StorageEngine::open_with_options(new_dir, self.storage_opts)?);

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

    /// Wipe and re-backfill a secondary index from its table's current rows.
    fn rebuild_index(&mut self, name: &str) -> Result<()> {
        let def = self
            .catalog
            .indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?;
        let (idx_table, idx_paths) = (def.table.clone(), def.paths.clone());
        self.indexes.remove(name);
        let dir = index_dir(&self.dir, name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        let mut engine = StorageEngine::open_with_options(dir, self.storage_opts)?;
        for (row_key, doc) in self.scan_docs(&idx_table)? {
            let values = index_values(&doc, &idx_paths);
            engine.put(&index_entry_key(&values, &row_key), row_key.clone())?;
        }
        self.indexes.insert(name.to_string(), engine);
        Ok(())
    }

    /// Rebuild an in-memory HNSW vector index from its table's current rows.
    fn rebuild_vector_index(&mut self, name: &str) -> Result<()> {
        let def = self
            .catalog
            .vector_indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?
            .clone();
        let mut hnsw = new_hnsw(&def);
        for (row_key, doc) in self.scan_docs(&def.table)? {
            if let Some(v) = doc_vector(&doc, &def.path, def.dim) {
                hnsw.insert(row_key, v);
            }
        }
        self.vector_indexes.insert(name.to_string(), hnsw);
        Ok(())
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
        let mut local = LocalCluster::new(self);
        let apply = || -> Result<()> {
            for ((table, key), op) in txn.writes {
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
        Ok(QueryOutput::Ddl)
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
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let txn = self.txn.as_ref().expect("transaction active");
        let mut map: BTreeMap<Vec<u8>, Document> =
            self.gather_rows_keyed(table, filter)?.into_iter().collect();
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
            return self.gather_with_overlay(table, filter);
        }
        self.gather_rows_keyed(table, filter)
    }

    /// Ordered/limited variant of [`Database::local_matching_rows`]; see
    /// [`Cluster::matching_rows_ordered`]. In a transaction, reads go through
    /// the buffered overlay (unindexed, never presorted).
    fn local_matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<&str>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        if self.txn.is_some() {
            return Ok((self.gather_with_overlay(table, filter)?, false));
        }
        self.gather_rows_planned(table, filter, order, fetch_limit)
    }

    /// Live-row count of `table` straight from storage key statistics — no row
    /// decode. Unavailable while a transaction is open (its overlay could
    /// change the count).
    fn local_count_rows(&self, table: &str) -> Result<Option<usize>> {
        if self.txn.is_some() {
            return Ok(None);
        }
        let engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
        Ok(Some(engine.key_stats()?.live_keys))
    }

    /// Collect `(key, doc)` for the rows of `table` matching `filter`, using a
    /// secondary index when the filter permits (equality or range on an indexed
    /// path). Unordered convenience wrapper over [`Database::gather_rows_planned`].
    fn gather_rows_keyed(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        Ok(self.gather_rows_planned(table, filter, None, None)?.0)
    }

    /// Plan and execute a row gather, optionally using a secondary index to (a)
    /// bound the scan to a value range (equality/`<`/`>`/`BETWEEN` on an indexed
    /// path) and/or (b) return rows already sorted ascending by `order`. When the
    /// result is in `order` and `fetch_limit` is set, scanning stops early
    /// (top-N). Returns the rows and whether they are sorted by `order`.
    fn gather_rows_planned(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<&str>,
        fetch_limit: Option<usize>,
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
                Some(bytes) => match Value::decode(&bytes) {
                    Ok(Value::Document(doc)) => vec![(key, doc)],
                    Ok(_) => {
                        return Err(EngineError::Constraint(
                            "stored row is not a document".into(),
                        ))
                    }
                    Err(e) => return Err(EngineError::Constraint(format!("corrupt row: {e}"))),
                },
                None => Vec::new(),
            };
            return Ok((rows, true));
        }
        let Some((index_name, start, end, sorted)) = self.plan_index(table, filter, order) else {
            // No usable index: stream the table scan (decode one row at a
            // time) and, when no ordering is required, stop as soon as the
            // fetch limit is satisfied — a plain `LIMIT n` touches n matching
            // rows, not the whole table.
            let engine = self
                .tables
                .get(table)
                .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;
            let mut out = Vec::new();
            for item in engine.scan_iter() {
                let (key, bytes) = item?;
                let doc = match Value::decode(&bytes) {
                    Ok(Value::Document(doc)) => doc,
                    Ok(_) => {
                        return Err(EngineError::Constraint(
                            "stored row is not a document".into(),
                        ))
                    }
                    Err(e) => return Err(EngineError::Constraint(format!("corrupt row: {e}"))),
                };
                if matches_filter(filter, &doc)? {
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
        for (_entry_key, row_key) in index_engine.scan_range(start.as_deref(), end.as_deref())? {
            let Some(bytes) = table_engine.get(&row_key)? else {
                continue; // index entry for a since-deleted row
            };
            if let Value::Document(doc) =
                Value::decode(&bytes).map_err(|e| EngineError::Constraint(format!("corrupt row: {e}")))?
            {
                if matches_filter(filter, &doc)? {
                    out.push((row_key, doc));
                    // Stop early when the rows already arrive in `order`, or
                    // when the query never asked for one.
                    if (sorted || order.is_none())
                        && fetch_limit.is_some_and(|lim| out.len() >= lim)
                    {
                        break;
                    }
                }
            }
        }
        Ok((out, sorted))
    }

    /// Choose an index access path for `(filter, order)`: a column with
    /// equality/range bounds that is indexed (bounded scan), else an indexed
    /// `ORDER BY` column (full ordered scan). Returns
    /// `(index_name, path, start_key, end_key)`.
    fn plan_index(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<&str>,
    ) -> Option<IndexPlan> {
        let constraints = column_constraints(filter);
        let mut fallback: Option<IndexPlan> = None;
        for (name, idx) in self.catalog.indexes.iter().filter(|(_, i)| i.table == table) {
            if let Some((start, end, sorted)) = plan_for_index(&idx.paths, &constraints, order) {
                let plan = (name.clone(), start, end, sorted);
                if sorted {
                    return Some(plan); // prefer a plan that also satisfies ORDER BY
                }
                fallback.get_or_insert(plan);
            }
        }
        fallback
    }

    /// The `(name, paths)` of every index defined on `table`.
    fn indexes_on(&self, table: &str) -> Vec<(String, Vec<String>)> {
        self.catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == table)
            .map(|(name, idx)| (name.clone(), idx.paths.clone()))
            .collect()
    }

    /// Add an index entry for `doc`'s values at `paths` pointing to `row_key`.
    fn index_put(
        &mut self,
        name: &str,
        paths: &[String],
        doc: &Document,
        row_key: &[u8],
    ) -> Result<()> {
        let values = index_values(doc, paths);
        if let Some(engine) = self.indexes.get_mut(name) {
            engine.put(&index_entry_key(&values, row_key), row_key.to_vec())?;
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
        let values = index_values(doc, paths);
        if let Some(engine) = self.indexes.get_mut(name) {
            let (_, commit) = engine.put_deferred(&index_entry_key(&values, row_key), row_key.to_vec())?;
            return Ok(Some((engine.wal_sync_handle(), commit)));
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
        let values = index_values(doc, paths);
        if let Some(engine) = self.indexes.get_mut(name) {
            engine.delete(&index_entry_key(&values, row_key))?;
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
        let values = index_values(doc, paths);
        if let Some(engine) = self.indexes.get_mut(name) {
            let (_, commit) = engine.delete_deferred(&index_entry_key(&values, row_key))?;
            return Ok(Some((engine.wal_sync_handle(), commit)));
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
        self.plan_index(table, filter, None)
            .map(|(name, start, end, _sorted)| (name, start, end))
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

    /// Whether `table` exists locally.
    pub fn has_table(&self, table: &str) -> bool {
        self.catalog.tables.contains_key(table)
    }

    /// Names of all tables (used to migrate every table during resharding).
    pub fn table_names(&self) -> Vec<String> {
        self.catalog.tables.keys().cloned().collect()
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
            out.push((
                db.to_string(),
                format!(
                    "CREATE TABLE IF NOT EXISTS {bare} (PRIMARY KEY ({}))",
                    def.primary_key.join(", ")
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
                    "CREATE INDEX IF NOT EXISTS {bare} ON {table} ({})",
                    idx.paths.join(", ")
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
                    "CREATE VECTOR INDEX IF NOT EXISTS {bare} ON {table} ({}) DIM {} USING {}",
                    v.path, v.dim, v.metric
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
        for (name, def) in &self.catalog.users {
            out.push((
                DEFAULT_DATABASE.to_string(),
                format!("CREATE USER {name} VERIFIER '{}'", def.credential),
                ver(&format!("usr:{name}")),
            ));
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
            out.push((
                db.to_string(),
                format!(
                    "CREATE TABLE IF NOT EXISTS {bare} (PRIMARY KEY ({}))",
                    def.primary_key.join(", ")
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
                    "CREATE INDEX IF NOT EXISTS {bare} ON {table} ({})",
                    idx.paths.join(", ")
                ),
            ));
        }
        for (name, v) in &self.catalog.vector_indexes {
            let (db, bare) = namespace::split(name);
            let table = namespace::split(&v.table).1;
            out.push((
                db.to_string(),
                format!(
                    "CREATE VECTOR INDEX IF NOT EXISTS {bare} ON {table} ({}) DIM {} USING {}",
                    v.path, v.dim, v.metric
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
                Value::String("secondary".into()),
                Value::String(idx.paths.join(", ")),
            ]);
        }
        for (name, v) in &self.catalog.vector_indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(v.table.clone()),
                Value::String(format!("vector({}, dim={})", v.metric, v.dim)),
                Value::String(v.path.clone()),
            ]);
        }
        for (name, s) in &self.catalog.search_indexes {
            rows.push(vec![
                Value::String(name.clone()),
                Value::String(s.table.clone()),
                Value::String(format!("search({})", s.analyzer())),
                Value::String(s.paths.join(", ")),
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
            ],
            rows,
        }
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
                row(&format!("table.{}.live_keys", t.name), Value::Int(t.live_keys as i64));
                row(&format!("table.{}.tombstones", t.name), Value::Int(t.tombstones as i64));
                row(&format!("table.{}.disk_bytes", t.name), Value::Int(t.disk_bytes as i64));
            }
            // Per-search-index breakdown, in catalog (name) order.
            for name in self.catalog.search_indexes.keys() {
                let Some(live) = self.search_indexes.get(name) else {
                    continue;
                };
                let fs = live.index.stats();
                row(&format!("search.{name}.docs"), Value::Int(fs.docs as i64));
                row(&format!("search.{name}.disk_bytes"), Value::Int(fs.disk_bytes as i64));
                row(&format!("search.{name}.uncommitted"), Value::Int(fs.uncommitted as i64));
            }
            row("timeseries_tables", Value::Int(self.timeseries.len() as i64));
            for (name, store) in &self.timeseries {
                let ts = store.stats();
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
            }
        }
        ResultSet {
            columns: vec!["metric".into(), "value".into()],
            rows,
        }
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
                let ks = engine.key_stats().unwrap_or_default();
                agg.per_table.push(TableStats {
                    name: name.clone(),
                    live_keys: ks.live_keys as u64,
                    tombstones: ks.tombstones as u64,
                    disk_bytes: s.disk_bytes,
                    sstables: s.sstable_count as u64,
                });
            }
            agg.per_table.sort_by(|a, b| a.name.cmp(&b.name));
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
        for (key, _hlc, value) in engine.scan_versioned_with_tombstones()? {
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

    /// Apply a replicated row write at an explicit stamp, maintaining indexes.
    pub fn apply_put(&mut self, table: &str, key: &[u8], bytes: Vec<u8>, hlc: Hlc) -> Result<()> {
        let doc = match Value::decode(&bytes)
            .map_err(|e| EngineError::Constraint(format!("corrupt replicated row: {e}")))?
        {
            Value::Document(d) => d,
            _ => {
                return Err(EngineError::Constraint(
                    "replicated row is not a document".into(),
                ))
            }
        };
        self.table_engine_mut(table)?
            .put_with_hlc(key, bytes, hlc)?;
        for (name, path) in self.indexes_on(table) {
            self.index_put(&name, &path, &doc, key)?;
        }
        self.maintain_vectors_put(table, &doc, key);
        self.maintain_search_put(table, &doc, key, hlc)?;
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
            }
        }
        self.maintain_vectors_del(table, key);
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
        let doc = match Value::decode(&bytes)
            .map_err(|e| EngineError::Constraint(format!("corrupt replicated row: {e}")))?
        {
            Value::Document(d) => d,
            _ => {
                return Err(EngineError::Constraint(
                    "replicated row is not a document".into(),
                ))
            }
        };
        let (commit, handle) = {
            let engine = self.table_engine_mut(table)?;
            let commit = engine.append_put_buffered(key, bytes, hlc)?;
            (commit, engine.wal_sync_handle())
        };
        for (name, path) in self.indexes_on(table) {
            self.index_put(&name, &path, &doc, key)?;
        }
        self.maintain_vectors_put(table, &doc, key);
        self.search_put_unrefreshed(table, &doc, key, hlc)?;
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
            }
        }
        self.maintain_vectors_del(table, key);
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
        _order: Option<&str>,
        _fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        Ok((self.matching_rows(table, filter)?, false))
    }
    /// Fast count of `table`'s live rows, when the implementation can serve it
    /// without materializing or decoding rows (`None` = unavailable; the
    /// caller falls back to a full gather). Only consulted for unfiltered
    /// `COUNT(*)`.
    fn count_rows(&self, _table: &str) -> Result<Option<usize>> {
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
        _highlights: &[(String, usize)],
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
        _highlights: &[(String, usize)],
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
    match stmt {
        Statement::Insert(ins) => run_insert(ins, cluster),
        // A search SELECT keeps the `&mut` seam: `Cluster::search` commits
        // pending index writes first (read-your-writes).
        Statement::Select(sel) if select_uses_search(&sel) => {
            run_search_select(&sel, cluster).map(QueryOutput::Rows)
        }
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

/// Execute a `NEAREST` (vector search) select: resolve the query vector and
/// `k`, run the ANN gather (already filtered and nearest-first), expose each
/// hit's distance as a `_distance` field, and project as usual.
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
    let query = match eval(&nearest.query, &empty)? {
        Value::Array(items) => {
            let mut v = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(i) => v.push(i as f32),
                    Value::Float(f) => v.push(f as f32),
                    _ => {
                        return Err(EngineError::Type(
                            "NEAREST query vector must be a numeric array".into(),
                        ))
                    }
                }
            }
            v
        }
        _ => {
            return Err(EngineError::Type(
                "NEAREST query vector must be a numeric array".into(),
            ))
        }
    };
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
    let mut rs = project(sel, docs, &HashSet::new(), true)?;
    apply_offset_limit(&mut rs.rows, sel.offset, sel.limit);
    Ok(rs)
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
        let hits = cluster.search(&sel.from, &query, None, &residual, &[])?;
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
                    if let Some(hits) = cluster.search_sorted(
                        &sel.from, &query, &sort, want, &residual, &highlights,
                    )? {
                        let docs = hits.into_iter().map(|(_, d)| d).collect();
                        // The generic sort in `project` re-orders the
                        // already-bounded gather identically (the pushdown
                        // declined if NULL ordering could diverge).
                        return project(sel, docs, &HashSet::new(), true);
                    }
                }
            }
            // Fallback: every matching row, ordered by the executor.
            let hits = cluster.search(&sel.from, &query, None, &residual, &highlights)?;
            let docs: Vec<Document> = hits.into_iter().map(|(_, d, _)| d).collect();
            return project(sel, docs, &HashSet::new(), true);
        }
    };
    let highlights = collect_highlights(sel)?;
    let hits = cluster.search(&sel.from, &query, k, &residual, &highlights)?;
    let docs: Vec<Document> = hits
        .into_iter()
        .map(|(_, mut doc, score)| {
            doc.insert("_score", Value::Float(score as f64));
            doc
        })
        .collect();
    // `project` with `finalize` applies the ORDER BY (score() reads the
    // injected `_score`) and the OFFSET/LIMIT page.
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

/// The `HIGHLIGHT(column [, max_chars])` requests in a search SELECT's
/// projection, as `(column, max_chars)` (default 150 chars, the ES-ish
/// fragment size). The search gather answers each with a
/// `_highlight_<column>` snippet field on every hit.
fn collect_highlights(sel: &Select) -> Result<Vec<(String, usize)>> {
    fn walk(expr: &Expr, out: &mut Vec<(String, usize)>) -> Result<()> {
        if let Expr::Func { name, args } = expr {
            if name == "highlight" {
                let (col, max_chars) = match args.as_slice() {
                    [Expr::Column(col)] => (col.clone(), 150),
                    [Expr::Column(col), Expr::Literal(Value::Int(n))] if *n > 0 => {
                        (col.clone(), *n as usize)
                    }
                    _ => {
                        return Err(EngineError::Type(
                            "HIGHLIGHT(column [, max_chars]) takes a column and an optional \
                             positive integer"
                                .into(),
                        ))
                    }
                };
                match out.iter().find(|(c, _)| *c == col) {
                    Some((_, prev)) if *prev != max_chars => {
                        return Err(EngineError::Type(format!(
                            "conflicting HIGHLIGHT lengths for column '{col}'"
                        )))
                    }
                    Some(_) => {}
                    None => out.push((col, max_chars)),
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
    if grouped && sel.filter.is_none() && sel.group_by.is_empty() && sel.having.is_none() {
        if let [SelectItem::Expr {
            expr:
                expr @ Expr::Aggregate {
                    func: AggFunc::Count,
                    arg: AggArg::Star,
                },
            alias,
        }] = sel.items.as_slice()
        {
            if let Some(n) = cluster.count_rows(&sel.from)? {
                let col = alias.clone().unwrap_or_else(|| expr_name(expr));
                let mut rows = vec![vec![Value::Int(n as i64)]];
                apply_offset_limit(&mut rows, sel.offset, sel.limit);
                return Ok(ResultSet::new(vec![col], rows));
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

    let (keyed, presorted) =
        cluster.matching_rows_ordered(&sel.from, &sel.filter, order_col.as_deref(), fetch_limit)?;
    let docs: Vec<Document> = keyed.into_iter().map(|(_k, doc)| doc).collect();

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
    let docs: Vec<Document> = if sel.joins.is_empty() {
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
fn index_order_column(order_by: &[OrderKey]) -> Option<String> {
    match order_by {
        [key] if !key.descending => match &key.expr {
            Expr::Column(col) => Some(col.clone()),
            _ => None,
        },
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
        // Model the rewrite as delete-old then put-new (covers PK changes and
        // keeps index/replica maintenance uniform).
        cluster.delete(&upd.table, &old_key, &old_doc)?;
        cluster.put(&upd.table, &new_key, &new_doc)?;
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

/// When the entire filter is `pk = <literal>` on a single-column primary key,
/// the storage key for that row so the read can be a point get. The key must
/// be built exactly as the engine builds it for inserts: the order-preserving
/// encoding of a one-element array holding the value.
pub fn pk_point_key(pk: &[String], filter: &Option<Expr>) -> Option<Vec<u8>> {
    if pk.len() != 1 {
        return None;
    }
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }) = filter
    else {
        return None;
    };
    let col = &pk[0];
    let value = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(c)) if c == col => v,
        _ => return None,
    };
    if value.is_null() {
        return None;
    }
    Some(Value::Array(vec![value.clone()]).encode_key())
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
    /// way, matching the previous per-row fsync behavior).
    fn flush_pending(&mut self) -> Result<()> {
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
        let engine = self.db.table_engine_mut(table)?;
        let (hlc, commit) = engine.put_deferred(key, Value::encode_document(doc))?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_put_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_vectors_put(table, doc, key);
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
        order: Option<&str>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        self.db.local_matching_rows_ordered(table, filter, order, fetch_limit)
    }

    fn count_rows(&self, table: &str) -> Result<Option<usize>> {
        self.db.local_count_rows(table)
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

    fn search(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
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
        highlights: &[(String, usize)],
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
        let engine = self.db.table_engine_mut(table)?;
        let (hlc, commit) = engine.delete_deferred(key)?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_del_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_vectors_del(table, key);
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
        order: Option<&str>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        self.db.local_matching_rows_ordered(table, filter, order, fetch_limit)
    }

    fn count_rows(&self, table: &str) -> Result<Option<usize>> {
        self.db.local_count_rows(table)
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

    fn search(
        &mut self,
        table: &str,
        query: &SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
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
        highlights: &[(String, usize)],
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

    fn ts_rollup_info(&self, table: &str) -> Result<TsRollupInfo> {
        self.db.ts_rollup_info(table)
    }
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
        let opts = self.storage_opts;
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
        match Database::open_with_options(&dir, opts) {
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

fn new_hnsw(def: &VectorIndexDef) -> Hnsw {
    let mut h = Hnsw::new(Metric::parse(&def.metric).unwrap_or(Metric::Cosine), def.dim);
    if let Some(ef) = def.ef_search {
        h.set_ef_search(ef);
    }
    h
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
fn doc_vector(doc: &Document, path: &str, dim: usize) -> Option<Vec<f32>> {
    doc_vector_raw(doc, path).filter(|v| v.len() == dim)
}

fn index_values(doc: &Document, paths: &[String]) -> Vec<Value> {
    paths
        .iter()
        .map(|p| doc.get_path(p).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Index entry key: `[v1, .., vk, row_key]` encoded order-preservingly, so all
/// entries sharing a leading value prefix share a byte prefix (composite-aware).
fn index_entry_key(values: &[Value], row_key: &[u8]) -> Vec<u8> {
    let mut elems = values.to_vec();
    elems.push(Value::Bytes(row_key.to_vec()));
    Value::Array(elems).encode_key()
}

/// The shared byte prefix of every index entry whose leading values are
/// `values` (the encoding of the array `[values..]` without its trailing array
/// terminator). Also the inclusive lower bound for a scan starting there.
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

/// Build a scan plan for one (possibly composite) index given the filter's
/// per-column constraints and the requested `ORDER BY` column. Consumes a
/// leftmost run of equality-pinned columns, then an optional trailing range on
/// the next column. Returns `(start, end, sorted)` where `sorted` is whether the
/// scan order already satisfies `order`.
type ScanBounds = (Option<Vec<u8>>, Option<Vec<u8>>, bool);
fn plan_for_index(
    paths: &[String],
    constraints: &[(String, ColConstraint)],
    order: Option<&str>,
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
    let sorted = order.is_some_and(|oc| {
        paths[..i].iter().any(|p| p == oc) || paths.get(i).map(String::as_str) == Some(oc)
    });

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
    Some((start, end, sorted))
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

fn matches_filter(filter: &Option<Expr>, doc: &Document) -> Result<bool> {
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
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => false,
    }
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
