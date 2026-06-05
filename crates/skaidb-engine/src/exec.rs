//! The embeddable query engine: parse, plan, and execute against storage.
//!
//! One [`storage::Engine`] backs each table (a table is a namespace, SPEC §2).
//! Rows are documents keyed by their primary key, encoded with the
//! order-preserving key codec so scans come back in key order.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use skaidb_sql::ast::{
    AggArg, AggFunc, BinaryOp, Delete, Expr, Insert, OrderKey, Select, SelectItem, Statement,
    Update,
};
use skaidb_sql::parse;
use std::sync::Arc;

use skaidb_storage::{Engine as StorageEngine, Hlc, VersionValue, WalCommit, WalSync};
use skaidb_types::{Document, Value};

use crate::catalog::{Catalog, IndexDef, TableDef, VectorIndexDef};
use crate::error::{EngineError, Result};
use crate::eval::{compare, eval, eval_predicate};
use crate::result::{QueryOutput, ResultSet};
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
    catalog: Catalog,
    tables: HashMap<String, StorageEngine>,
    /// Secondary index storage by index name; entries map an indexed value to a
    /// table primary-key (so a `WHERE path = v` lookup avoids a full scan).
    indexes: HashMap<String, StorageEngine>,
    /// In-memory HNSW vector indexes by name (rebuilt from the table on open).
    vector_indexes: HashMap<String, Hnsw>,
}

impl Database {
    /// Open (creating if needed) a database rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Database> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let catalog = Catalog::load(dir.join("catalog.json"))?;

        let mut tables = HashMap::new();
        for name in catalog.tables.keys() {
            let engine = StorageEngine::open(table_dir(&dir, name))?;
            tables.insert(name.clone(), engine);
        }

        let mut indexes = HashMap::new();
        for name in catalog.indexes.keys() {
            let engine = StorageEngine::open(index_dir(&dir, name))?;
            indexes.insert(name.clone(), engine);
        }

        // Vector indexes live in memory; rebuild each from its table's rows.
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

        Ok(Database {
            dir,
            catalog,
            tables,
            indexes,
            vector_indexes,
        })
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
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    /// Drop a vector index.
    pub fn drop_vector_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        if self.catalog.vector_indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.vector_indexes.remove(name);
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
        // the filter (filtered nearest-neighbor search).
        let hits = hnsw.search(query, k, |key| match table_engine.get(key) {
            Ok(Some(bytes)) => match (filter, Value::decode(&bytes)) {
                (Some(_), Ok(Value::Document(doc))) => matches_filter(filter, &doc).unwrap_or(false),
                (None, Ok(Value::Document(_))) => true,
                _ => false,
            },
            _ => false,
        });

        let mut out = Vec::with_capacity(hits.len());
        for (key, dist) in hits {
            if let Some(bytes) = table_engine.get(&key)? {
                if let Value::Document(doc) = Value::decode(&bytes)
                    .map_err(|e| EngineError::Constraint(format!("corrupt row: {e}")))?
                {
                    out.push((key, doc, dist));
                }
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
        match parse(sql)? {
            Statement::CreateTable(ct) => {
                self.create_table(&ct.name, ct.primary_key, ct.if_not_exists)
            }
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
            // DML and SELECT run through the storage-agnostic executor so the
            // exact same logic serves the local engine and the cluster
            // coordinator (which replaces LocalCluster with a networked one).
            dml => {
                let mut local = LocalCluster { db: self };
                run(dml, &mut local)
            }
        }
    }

    // ---- DDL ----

    fn create_table(
        &mut self,
        name: &str,
        pk: Vec<String>,
        if_not_exists: bool,
    ) -> Result<QueryOutput> {
        if self.catalog.tables.contains_key(name) {
            if if_not_exists {
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableExists(name.to_string()));
        }
        if pk.is_empty() {
            return Err(EngineError::Constraint(
                "primary key must have at least one column".into(),
            ));
        }
        let engine = StorageEngine::open(table_dir(&self.dir, name))?;
        self.tables.insert(name.to_string(), engine);
        self.catalog
            .tables
            .insert(name.to_string(), TableDef { primary_key: pk });
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_table(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        if !self.catalog.tables.contains_key(name) {
            if if_exists {
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::TableNotFound(name.to_string()));
        }
        self.tables.remove(name);
        self.catalog.tables.remove(name);
        // Drop the table's indexes too.
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
            let idir = index_dir(&self.dir, &index_name);
            if idir.exists() {
                std::fs::remove_dir_all(idir)?;
            }
        }
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
        if self.catalog.indexes.contains_key(name) {
            if if_not_exists {
                return Ok(QueryOutput::Ddl);
            }
            return Err(EngineError::IndexExists(name.to_string()));
        }
        // Create the index store and backfill it from the existing rows.
        let mut index_engine = StorageEngine::open(index_dir(&self.dir, name))?;
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
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    fn drop_index(&mut self, name: &str, if_exists: bool) -> Result<QueryOutput> {
        if self.catalog.indexes.remove(name).is_none() && !if_exists {
            return Err(EngineError::IndexNotFound(name.to_string()));
        }
        self.indexes.remove(name);
        let idir = index_dir(&self.dir, name);
        if idir.exists() {
            std::fs::remove_dir_all(idir)?;
        }
        self.save_catalog()?;
        Ok(QueryOutput::Ddl)
    }

    // ---- row gathering (with optional index acceleration) ----

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
        let Some((index_name, start, end, sorted)) = self.plan_index(table, filter, order) else {
            // No usable index: full table scan + filter, unordered.
            let mut out = Vec::new();
            for (key, doc) in self.scan_docs(table)? {
                if matches_filter(filter, &doc)? {
                    out.push((key, doc));
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
                    // Rows already arrive in `order`, so a fetch limit lets us stop.
                    if sorted && fetch_limit.is_some_and(|lim| out.len() >= lim) {
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
pub trait Cluster {
    /// Primary-key columns of `table`.
    fn primary_key(&self, table: &str) -> Result<Vec<String>>;
    /// `(key, doc)` for rows of `table` matching `filter`.
    fn matching_rows(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>>;

    /// Like [`Cluster::matching_rows`] but may use an index to return rows
    /// already sorted ascending by `order` (a single plain column) and to stop
    /// after `fetch_limit` matching rows. Returns the rows and whether they are
    /// actually sorted by `order`. The default ignores the hints.
    fn matching_rows_ordered(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
        _order: Option<&str>,
        _fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        Ok((self.matching_rows(table, filter)?, false))
    }
    /// Write `doc` under `key` in `table` (and maintain indexes/replicas).
    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()>;
    /// Delete `key` from `table`; `doc` is the row being removed (for indexes).
    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()>;
}

/// Execute one DML/SELECT statement against any [`Cluster`].
///
/// DDL is handled by each executor directly (locally, or broadcast in a
/// cluster), so only the data-plane statements arrive here.
pub fn run(stmt: Statement, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
    match stmt {
        Statement::Insert(ins) => run_insert(ins, cluster),
        Statement::Select(sel) => run_select(sel, cluster).map(QueryOutput::Rows),
        Statement::Update(upd) => run_update(upd, cluster),
        Statement::Delete(del) => run_delete(del, cluster),
        _ => Err(EngineError::Unsupported(
            "non-data statement reached the data-plane executor".into(),
        )),
    }
}

fn run_insert(ins: Insert, cluster: &mut dyn Cluster) -> Result<QueryOutput> {
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
    for (key, doc) in &rows {
        cluster.put(&ins.table, key, doc)?;
    }
    Ok(QueryOutput::Mutation { affected })
}

fn run_select(sel: Select, cluster: &mut dyn Cluster) -> Result<ResultSet> {
    let has_aggregate = sel.items.iter().any(|it| match it {
        SelectItem::Expr { expr, .. } => contains_aggregate(expr),
        SelectItem::Wildcard => false,
    });
    let grouped = has_aggregate || !sel.group_by.is_empty();

    // An index can satisfy a single ascending `ORDER BY <column>` directly.
    let order_col = if grouped {
        None
    } else {
        index_order_column(&sel.order_by)
    };
    // Push a fetch limit only when the rows will come back in the requested order
    // (so truncating the prefix is correct).
    let fetch_limit = match (&order_col, sel.limit) {
        (Some(_), Some(limit)) => Some(sel.offset.unwrap_or(0).saturating_add(limit) as usize),
        _ => None,
    };

    let (keyed, presorted) =
        cluster.matching_rows_ordered(&sel.from, &sel.filter, order_col.as_deref(), fetch_limit)?;
    let docs: Vec<Document> = keyed.into_iter().map(|(_k, doc)| doc).collect();

    if grouped {
        select_aggregate(&sel, docs)
    } else {
        select_rows(&sel, docs, presorted)
    }
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
    let matches = cluster.matching_rows(&del.table, &del.filter)?;
    let affected = matches.len();
    for (key, doc) in matches {
        cluster.delete(&del.table, &key, &doc)?;
    }
    Ok(QueryOutput::Mutation { affected })
}

/// The embedded, single-node implementation of [`Cluster`].
struct LocalCluster<'a> {
    db: &'a mut Database,
}

impl Cluster for LocalCluster<'_> {
    fn primary_key(&self, table: &str) -> Result<Vec<String>> {
        Ok(self.db.table_def(table)?.primary_key.clone())
    }

    fn matching_rows(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        self.db.gather_rows_keyed(table, filter)
    }

    fn matching_rows_ordered(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<&str>,
        fetch_limit: Option<usize>,
    ) -> Result<OrderedRows> {
        self.db.gather_rows_planned(table, filter, order, fetch_limit)
    }

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        self.db
            .table_engine_mut(table)?
            .put(key, Value::Document(doc.clone()).encode())?;
        for (name, path) in self.db.indexes_on(table) {
            self.db.index_put(&name, &path, doc, key)?;
        }
        self.db.maintain_vectors_put(table, doc, key);
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        self.db.table_engine_mut(table)?.delete(key)?;
        for (name, path) in self.db.indexes_on(table) {
            self.db.index_del(&name, &path, doc, key)?;
        }
        self.db.maintain_vectors_del(table, key);
        Ok(())
    }
}

fn table_dir(root: &Path, name: &str) -> PathBuf {
    root.join("tables").join(name)
}

fn index_dir(root: &Path, name: &str) -> PathBuf {
    root.join("indexes").join(name)
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
        Expr::Literal(_) | Expr::Column(_) => false,
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
        }
        .to_string(),
        _ => "expr".to_string(),
    }
}

/// Row-per-document projection (no aggregates).
fn select_rows(sel: &Select, mut docs: Vec<Document>, presorted: bool) -> Result<ResultSet> {
    // `presorted` means the gather already returned rows in `ORDER BY` order
    // (via an index), so the sort pass is skipped.
    if !presorted {
        sort_docs(&mut docs, &sel.order_by)?;
    }
    apply_offset_limit(&mut docs, sel.offset, sel.limit);

    // Wildcard expands to the sorted union of all field names in the output set.
    let wildcard_fields: Vec<String> = if has_wildcard(sel) {
        let mut set = BTreeSet::new();
        for doc in &docs {
            for k in doc.0.keys() {
                set.insert(k.clone());
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
    Ok(ResultSet::new(columns, rows))
}

/// Aggregate / grouped projection.
fn select_aggregate(sel: &Select, docs: Vec<Document>) -> Result<ResultSet> {
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

    if !sel.order_by.is_empty() {
        keyed_rows.sort_by(|a, b| order_compare(&a.0, &b.0, &sel.order_by));
    }

    let mut rows: Vec<Vec<Value>> = keyed_rows.into_iter().map(|(_, r)| r).collect();
    apply_offset_limit(&mut rows, sel.offset, sel.limit);
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
        Expr::Literal(_) | Expr::Column(_) => expr.clone(),
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
    })
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

fn sort_docs(docs: &mut [Document], order_by: &[OrderKey]) -> Result<()> {
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
    keyed.sort_by(|a, b| order_compare(&a.0, &b.0, order_by));

    let original: Vec<Document> = docs.to_vec();
    for (new_pos, (_, old_idx)) in keyed.into_iter().enumerate() {
        docs[new_pos] = original[old_idx].clone();
    }
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
