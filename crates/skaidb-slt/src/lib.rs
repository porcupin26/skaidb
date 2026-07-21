//! sqllogictest adapter for skaidb (dev tooling, never shipped).
//!
//! Wraps the EMBEDDED engine as a [`sqllogictest::DB`], so `.slt` files
//! under `tests/corpus/` run through the standard sqllogictest runner in
//! `cargo test -p skaidb-slt` — the pg_regress idea (golden files, one
//! appended per bug) in the engine-neutral format the rest of the industry
//! settled on. The `slt-diff` binary reuses the same rendering to
//! cross-check generated queries against SQLite.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use skaidb_engine::{Database, QueryOutput};
use skaidb_types::Value;
use sqllogictest::{DBOutput, DefaultColumnType};

/// A fresh embedded database in a unique temp directory.
pub struct SkaiDb {
    db: Database,
    dir: PathBuf,
}

fn temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut dir = std::env::temp_dir();
    dir.push(format!("skaidb-slt-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create slt temp dir");
    dir
}

impl SkaiDb {
    pub fn new() -> Self {
        let dir = temp_dir();
        let db = Database::open(&dir).expect("open embedded db");
        SkaiDb { db, dir }
    }

    pub fn execute(&mut self, sql: &str) -> Result<QueryOutput, skaidb_engine::EngineError> {
        self.db.execute(sql)
    }
}

impl Default for SkaiDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SkaiDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Render one value the way the corpus expects it: `NULL` for null/absent,
/// integers bare, floats with three decimals (the sqllogictest convention),
/// booleans as `true`/`false`, strings raw.
pub fn render(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_nan() {
                "NaN".to_string()
            } else {
                format!("{f:.3}")
            }
        }
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
        Value::Timestamp(t) => t.to_string(),
        other => format!("{other:?}"),
    }
}

fn column_type(v: &Value) -> DefaultColumnType {
    match v {
        Value::Int(_) | Value::Timestamp(_) => DefaultColumnType::Integer,
        Value::Float(_) => DefaultColumnType::FloatingPoint,
        Value::String(_) => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

#[derive(Debug)]
pub struct SltError(pub String);

impl std::fmt::Display for SltError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SltError {}

impl sqllogictest::DB for SkaiDb {
    type Error = SltError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, SltError> {
        match self.db.execute(sql).map_err(|e| SltError(e.to_string()))? {
            QueryOutput::Rows(rs) => {
                let types: Vec<DefaultColumnType> = match rs.rows.first() {
                    Some(row) => row.iter().map(column_type).collect(),
                    None => rs.columns.iter().map(|_| DefaultColumnType::Any).collect(),
                };
                let rows = rs
                    .rows
                    .iter()
                    .map(|row| row.iter().map(render).collect())
                    .collect();
                Ok(DBOutput::Rows { types, rows })
            }
            QueryOutput::Mutation { affected } => {
                Ok(DBOutput::StatementComplete(affected as u64))
            }
            QueryOutput::Ddl => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn engine_name(&self) -> &str {
        "skaidb"
    }
}
