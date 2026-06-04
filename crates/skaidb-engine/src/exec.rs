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
use skaidb_storage::{Engine as StorageEngine, Hlc};
use skaidb_types::{Document, Value};

use crate::catalog::{Catalog, IndexDef, TableDef};
use crate::error::{EngineError, Result};
use crate::eval::{compare, eval, eval_predicate};
use crate::result::{QueryOutput, ResultSet};

/// A live row with its encoded document and version stamp.
pub type VersionedRow = (Vec<u8>, Vec<u8>, Hlc);

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

        Ok(Database {
            dir,
            catalog,
            tables,
            indexes,
        })
    }

    /// Parse and execute a single SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<QueryOutput> {
        match parse(sql)? {
            Statement::CreateTable(ct) => {
                self.create_table(&ct.name, ct.primary_key, ct.if_not_exists)
            }
            Statement::DropTable { name, if_exists } => self.drop_table(&name, if_exists),
            Statement::CreateIndex(ci) => {
                self.create_index(&ci.name, &ci.table, &ci.path, ci.if_not_exists)
            }
            Statement::DropIndex { name, if_exists } => self.drop_index(&name, if_exists),
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
        path: &str,
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
            let value = index_value(&doc, path);
            index_engine.put(&index_entry_key(&value, &row_key), row_key.clone())?;
        }
        self.indexes.insert(name.to_string(), index_engine);
        self.catalog.indexes.insert(
            name.to_string(),
            IndexDef {
                table: table.to_string(),
                path: path.to_string(),
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

    /// Collect `(key, doc)` for the rows of `table` matching `filter`. When
    /// `filter` is a simple equality on an indexed path, matching primary keys
    /// are read from the secondary index and point-fetched, avoiding a scan.
    fn gather_rows_keyed(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let candidates = if let Some((path, value)) = index_equality(filter) {
            match self.find_index(table, &path) {
                Some(name) => self.lookup_by_index_keyed(table, &name, &value)?,
                None => self.scan_docs(table)?,
            }
        } else {
            self.scan_docs(table)?
        };
        let mut out = Vec::new();
        for (key, doc) in candidates {
            if matches_filter(filter, &doc)? {
                out.push((key, doc));
            }
        }
        Ok(out)
    }

    /// The (name, path) of every index defined on `table`.
    fn indexes_on(&self, table: &str) -> Vec<(String, String)> {
        self.catalog
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table == table)
            .map(|(name, idx)| (name.clone(), idx.path.clone()))
            .collect()
    }

    /// Find an index on `table` over exactly `path`, if one exists.
    fn find_index(&self, table: &str, path: &str) -> Option<String> {
        self.catalog
            .indexes
            .iter()
            .find(|(_, idx)| idx.table == table && idx.path == path)
            .map(|(name, _)| name.clone())
    }

    /// Add an index entry for `doc`'s value at `path` pointing to `row_key`.
    fn index_put(&mut self, name: &str, path: &str, doc: &Document, row_key: &[u8]) -> Result<()> {
        let value = index_value(doc, path);
        if let Some(engine) = self.indexes.get_mut(name) {
            engine.put(&index_entry_key(&value, row_key), row_key.to_vec())?;
        }
        Ok(())
    }

    /// Remove the index entry for `doc`'s value at `path` pointing to `row_key`.
    fn index_del(&mut self, name: &str, path: &str, doc: &Document, row_key: &[u8]) -> Result<()> {
        let value = index_value(doc, path);
        if let Some(engine) = self.indexes.get_mut(name) {
            engine.delete(&index_entry_key(&value, row_key))?;
        }
        Ok(())
    }

    /// Fetch `(key, doc)` for rows whose indexed value equals `value`.
    fn lookup_by_index_keyed(
        &self,
        table: &str,
        name: &str,
        value: &Value,
    ) -> Result<Vec<(Vec<u8>, Document)>> {
        let index_engine = self
            .indexes
            .get(name)
            .ok_or_else(|| EngineError::IndexNotFound(name.to_string()))?;
        let table_engine = self
            .tables
            .get(table)
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))?;

        let mut rows = Vec::new();
        for (_entry_key, row_key) in index_engine.scan_prefix(&index_prefix(value))? {
            if let Some(bytes) = table_engine.get(&row_key)? {
                if let Value::Document(doc) = Value::decode(&bytes)
                    .map_err(|e| EngineError::Constraint(format!("corrupt row: {e}")))?
                {
                    rows.push((row_key, doc));
                }
            }
        }
        Ok(rows)
    }

    // ---- distribution support (used by the cluster coordinator) ----

    /// Primary-key columns of `table` (public for the coordinator).
    pub fn table_primary_key(&self, table: &str) -> Result<Vec<String>> {
        Ok(self.table_def(table)?.primary_key.clone())
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
        Ok(())
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
    let docs: Vec<Document> = cluster
        .matching_rows(&sel.from, &sel.filter)?
        .into_iter()
        .map(|(_k, doc)| doc)
        .collect();
    let has_aggregate = sel.items.iter().any(|it| match it {
        SelectItem::Expr { expr, .. } => contains_aggregate(expr),
        SelectItem::Wildcard => false,
    });
    if has_aggregate || !sel.group_by.is_empty() {
        select_aggregate(&sel, docs)
    } else {
        select_rows(&sel, docs)
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

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        self.db
            .table_engine_mut(table)?
            .put(key, Value::Document(doc.clone()).encode())?;
        for (name, path) in self.db.indexes_on(table) {
            self.db.index_put(&name, &path, doc, key)?;
        }
        Ok(())
    }

    fn delete(&mut self, table: &str, key: &[u8], doc: &Document) -> Result<()> {
        self.db.table_engine_mut(table)?.delete(key)?;
        for (name, path) in self.db.indexes_on(table) {
            self.db.index_del(&name, &path, doc, key)?;
        }
        Ok(())
    }
}

fn table_dir(root: &Path, name: &str) -> PathBuf {
    root.join("tables").join(name)
}

fn index_dir(root: &Path, name: &str) -> PathBuf {
    root.join("indexes").join(name)
}

/// The indexed value of `doc` at `path` (a missing field indexes as `NULL`).
fn index_value(doc: &Document, path: &str) -> Value {
    doc.get_path(path).cloned().unwrap_or(Value::Null)
}

/// Index entry key: `[indexed_value, row_key]` encoded order-preservingly, so
/// all entries for one value share a common byte prefix.
fn index_entry_key(value: &Value, row_key: &[u8]) -> Vec<u8> {
    Value::Array(vec![value.clone(), Value::Bytes(row_key.to_vec())]).encode_key()
}

/// The shared byte prefix of every index entry for `value` (the encoding of the
/// single-element array `[value]` without its trailing array terminator).
fn index_prefix(value: &Value) -> Vec<u8> {
    let mut key = Value::Array(vec![value.clone()]).encode_key();
    key.pop(); // drop the array-terminator byte
    key
}

/// Recognize a `path = literal` (or `literal = path`) predicate for index use.
fn index_equality(filter: &Option<Expr>) -> Option<(String, Value)> {
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }) = filter
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(p), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(p))
            if !v.is_null() =>
        {
            Some((p.clone(), v.clone()))
        }
        _ => None,
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
fn select_rows(sel: &Select, mut docs: Vec<Document>) -> Result<ResultSet> {
    sort_docs(&mut docs, &sel.order_by)?;
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
