//! The embeddable query engine: parse, plan, and execute against storage.
//!
//! One [`storage::Engine`] backs each table (a table is a namespace, SPEC §2).
//! Rows are documents keyed by their primary key, encoded with the
//! order-preserving key codec so scans come back in key order.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use skaidb_sql::ast::{
    AggArg, AggFunc, AlterAction, AlterTable, BinaryOp, Delete, Expr, Insert, JoinKind, OrderKey,
    Select, SelectItem, Statement, Update,
};
use skaidb_sql::parse;
use std::sync::Arc;

use skaidb_storage::{
    Engine as StorageEngine, EngineOptions, Hlc, HlcClock, VersionValue, WalCommit, WalSync,
};
use skaidb_types::{Document, Value};

use skaidb_tsdb::{Tsdb, TsdbOptions};

use crate::catalog::{AuthRoleDef, Catalog, IndexDef, RollupDef, SchemaVersion, TableDef, TsTableDef, UserDef, VectorIndexDef};
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
            timeseries.insert(name.clone(), open_tsdb(&dir, name, def)?);
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
            txn: None,
            clock,
            ddl_hlc: None,
            vector_rebuild_ms,
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
            Statement::Select(sel) => {
                run_select(&sel, &LocalRead { db: self }).map(QueryOutput::Rows)
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
                table,
                to,
            } => self.grant(&to, &privilege, table.as_deref()),
            Statement::Revoke {
                privilege,
                table,
                from,
            } => self.revoke(&from, &privilege, table.as_deref()),
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
            Statement::Select(_) => {
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
        let store = open_tsdb(&self.dir, name, &def)?;
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
        let store = open_tsdb(&self.dir, name, &def)?;
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
    pub fn ts_merge(&self, table: &str, rows: &[(skaidb_tsdb::Labels, i64, f64)]) -> Result<usize> {
        self.ts_store(table)?
            .merge_samples(rows)
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
            ..Default::default()
        };
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
        Ok((commit, handle))
    }

    /// Buffered replicated delete (no fsync); see [`Database::apply_put_buffered`].
    pub fn apply_delete_buffered(
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
        Ok((commit, handle))
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
}

/// Whether `stmt` only reads — i.e. it can run through the shared-access entry
/// points ([`Database::execute_read`] / [`Database::execute_session_read`])
/// without exclusive access. DML, DDL, and transaction control are not
/// read-only. Lets a caller holding an `RwLock<Database>` pick the lock mode.
pub fn statement_is_read_only(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Select(_)
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

/// Whether the query is in aggregate/grouped mode.
fn is_grouped(sel: &Select) -> bool {
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

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        // Buffer the write when a transaction is open (flushed on COMMIT).
        if let Some(txn) = self.db.txn.as_mut() {
            txn.writes
                .insert((table.to_string(), key.to_vec()), Some(doc.clone()));
            return Ok(());
        }
        self.table_indexes(table);
        let engine = self.db.table_engine_mut(table)?;
        let (_, commit) = engine.put_deferred(key, Value::encode_document(doc))?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_put_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_vectors_put(table, doc, key);
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        if let Some(txn) = self.db.txn.as_mut() {
            txn.writes.insert((table.to_string(), key.to_vec()), None);
            return Ok(());
        }
        self.table_indexes(table);
        let engine = self.db.table_engine_mut(table)?;
        let (_, commit) = engine.delete_deferred(key)?;
        self.pending
            .insert(format!("t:{table}"), (engine.wal_sync_handle(), commit));
        for (name, paths) in &self.index_memo[table] {
            if let Some(sync) = self.db.index_del_deferred(name, paths, doc, key)? {
                self.pending.insert(format!("i:{name}"), sync);
            }
        }
        self.db.maintain_vectors_del(table, key);
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
}

fn table_dir(root: &Path, name: &str) -> PathBuf {
    root.join("tables").join(name)
}

fn index_dir(root: &Path, name: &str) -> PathBuf {
    root.join("indexes").join(name)
}

/// Aggregate a flushed window into rollup samples: per series and bucket,
/// `<field>_{count,sum,min,max,first,last}` rows at the bucket start.
fn rollup_rows(
    flushed: &skaidb_tsdb::FlushedSeries,
    bucket_ms: i64,
) -> Result<Vec<(skaidb_tsdb::Labels, i64, f64)>> {
    let mut out = Vec::new();
    for (labels, chunks) in flushed {
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
        // (count, sum, min, max, first_ts, first, last_ts, last)
        type Acc = (u64, f64, f64, f64, i64, f64, i64, f64);
        let mut buckets: BTreeMap<i64, Acc> = BTreeMap::new();
        for chunk in chunks {
            for s in skaidb_tsdb::decode_chunk(&chunk.data)
                .map_err(|e| EngineError::Timeseries(e.to_string()))?
            {
                let b = s.ts.div_euclid(bucket_ms) * bucket_ms;
                let e = buckets
                    .entry(b)
                    .or_insert((0, 0.0, f64::INFINITY, f64::NEG_INFINITY, s.ts, s.value, s.ts, s.value));
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
        }
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
    }
    Ok(out)
}

/// Render the idempotent CREATE DDL for a time-series table definition
/// (schema replay/bootstrap/repair all share it).
/// Build the RBAC view from catalog users + roles.
fn build_role_store(catalog: &Catalog) -> RoleStore {
    let mut store = RoleStore::new();
    for name in catalog.auth_roles.keys() {
        let _ = store.create_role(name);
    }
    for (name, def) in &catalog.auth_roles {
        for (priv_name, table) in &def.grants {
            if let Some(p) = privilege_from_name(priv_name) {
                let object = match table {
                    Some(t) => AuthObject::Table(t.clone()),
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
fn open_tsdb(root: &Path, name: &str, def: &TsTableDef) -> Result<Tsdb> {
    Tsdb::open(
        &ts_dir(root, name),
        TsdbOptions {
            retention_ms: def.retention_ms,
            ooo_window_ms: def.ooo_window_ms,
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
fn new_hnsw(def: &VectorIndexDef) -> Hnsw {
    Hnsw::new(Metric::parse(&def.metric).unwrap_or(Metric::Cosine), def.dim)
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
fn expr_name(expr: &Expr) -> String {
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
    // Collect the (non-null) argument values for the group.
    let values: Vec<Value> = match arg {
        AggArg::Star => Vec::new(),
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
            AggArg::Expr(_) => Value::Int(values.len() as i64),
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
