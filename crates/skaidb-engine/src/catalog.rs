//! Table/index catalog, persisted as JSON alongside the data (SPEC §2).
//!
//! Schema-less means a table records only its primary key and any declared
//! secondary indexes — never a column list.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use skaidb_storage::Hlc;

use crate::error::{EngineError, Result};

/// Last-writer-wins version stamp for a schema object, so DDL (including drops)
/// can replicate across a cluster: the change with the higher HLC wins, and a
/// `dropped` stamp is a tombstone that prevents a stale peer from resurrecting
/// an object it still holds. Stored as plain ints so the catalog JSON needs no
/// dependency on `Hlc`'s wire format.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub physical: u64,
    pub logical: u32,
    pub dropped: bool,
}

impl SchemaVersion {
    pub fn new(hlc: Hlc, dropped: bool) -> Self {
        SchemaVersion {
            physical: hlc.physical,
            logical: hlc.logical,
            dropped,
        }
    }

    pub fn hlc(&self) -> Hlc {
        Hlc::new(self.physical, self.logical)
    }
}

/// A secondary index declaration (`CREATE INDEX ... ON t(path1, path2, ...)`).
/// `paths` is ordered: a composite index sorts by the first path, then the
/// second, and so on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    pub table: String,
    pub paths: Vec<String>,
}

/// A vector (HNSW) index declaration for approximate nearest-neighbor search
/// over the float array at `path`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexDef {
    pub table: String,
    pub path: String,
    /// `"cosine"`, `"l2"`, or `"dot"`.
    pub metric: String,
    /// Vector dimension (all indexed vectors must match).
    pub dim: usize,
}

/// A full-text search index declaration (`CREATE SEARCH INDEX ... ON t(paths)
/// WITH (...)`), backing `MATCH()`/`SEARCH()` predicates over the text at
/// `paths`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "SearchIndexDefWire")]
pub struct SearchIndexDef {
    pub table: String,
    /// Dotted paths into the row document to index.
    pub paths: Vec<String>,
    /// Raw `WITH (...)` options as declared — the declaration is the mapping;
    /// parsed and validated by `SearchIndexConfig::from_declaration`.
    pub options: Vec<(String, String)>,
}

impl SearchIndexDef {
    /// The index's default analyzer name, for display (`SHOW INDEXES`).
    /// Options apply last-wins, hence the reverse scan.
    pub fn analyzer(&self) -> &str {
        self.options
            .iter()
            .rev()
            .find(|(k, _)| k == "analyzer")
            .map_or("standard", |(_, v)| v.as_str())
    }

    /// Whether the index serves queries naming `field`: a declared path, a
    /// declared `<path>.keyword` exact-match twin, or a `copy_to` composite
    /// target.
    pub fn covers(&self, field: &str) -> bool {
        if self.paths.iter().any(|p| p == field) {
            return true;
        }
        // The twin's option key is literally the queryable field name
        // (`<path>.keyword = true`); options apply last-wins.
        if field
            .strip_suffix(".keyword")
            .is_some_and(|base| self.paths.iter().any(|p| p == base))
            && self
                .options
                .iter()
                .rev()
                .find(|(k, _)| k == field)
                .is_some_and(|(_, v)| v == "true")
        {
            return true;
        }
        self.options
            .iter()
            .any(|(k, v)| k.ends_with(".copy_to") && v == field)
    }

    /// Render the `WITH (...)` clause for DDL regeneration (schema
    /// replication/repair). Every value is quoted as a string literal; the
    /// option parser accepts strings for all value types.
    pub fn with_clause(&self) -> String {
        if self.options.is_empty() {
            return String::new();
        }
        let opts: Vec<String> = self
            .options
            .iter()
            .map(|(k, v)| format!("{k} = '{}'", v.replace('\'', "''")))
            .collect();
        format!(" WITH ({})", opts.join(", "))
    }
}

/// Loader shim: phase-1 catalogs stored `analyzer`/`refresh_ms` as dedicated
/// fields; fold them into `options` so old defs keep their behavior.
#[derive(Deserialize)]
struct SearchIndexDefWire {
    table: String,
    paths: Vec<String>,
    #[serde(default)]
    options: Vec<(String, String)>,
    analyzer: Option<String>,
    refresh_ms: Option<u64>,
}

impl From<SearchIndexDefWire> for SearchIndexDef {
    fn from(w: SearchIndexDefWire) -> SearchIndexDef {
        let mut options = w.options;
        if let Some(analyzer) = w.analyzer {
            options.push(("analyzer".to_string(), analyzer));
        }
        if let Some(refresh_ms) = w.refresh_ms {
            options.push(("refresh_ms".to_string(), refresh_ms.to_string()));
        }
        SearchIndexDef {
            table: w.table,
            paths: w.paths,
            options,
        }
    }
}

/// A table definition: just its primary key columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub primary_key: Vec<String>,
}

/// A time-series table definition: label columns and optional retention.
/// The implicit sample key is `(series key, ts)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TsTableDef {
    pub series_key: Vec<String>,
    pub retention_ms: Option<i64>,
    /// Out-of-order acceptance window (ms); default 0 = strict monotonic.
    #[serde(default)]
    pub ooo_window_ms: i64,
    /// Rollups derived from this table, maintained at flush.
    #[serde(default)]
    pub rollups: Vec<RollupDef>,
    /// When this table IS a rollup: its source table.
    #[serde(default)]
    pub rollup_of: Option<String>,
}

/// One rollup registration on its source table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollupDef {
    pub name: String,
    pub bucket_ms: i64,
}

/// A user principal: its SCRAM verifier (encoded; never plaintext). A user
/// acts as its own-named role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserDef {
    pub credential: String,
}

/// A role's grants and inherited roles. `grants` pairs are
/// `(privilege name, table)` with `None` = global.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AuthRoleDef {
    pub grants: Vec<(String, Option<String>)>,
    pub inherits: Vec<String>,
}

/// The set of all tables and indexes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub tables: BTreeMap<String, TableDef>,
    /// Time-series tables (stored in the tsdb engine, not the LSM). Shares
    /// the table namespace with `tables`. `default` so older catalogs load.
    #[serde(default)]
    pub timeseries: BTreeMap<String, TsTableDef>,
    pub indexes: BTreeMap<String, IndexDef>,
    /// Vector indexes (rebuilt in memory on open). `default` so older catalogs load.
    #[serde(default)]
    pub vector_indexes: BTreeMap<String, VectorIndexDef>,
    /// Full-text search indexes (Tantivy-backed, reopened/replayed on open).
    /// `default` so older catalogs load.
    #[serde(default)]
    pub search_indexes: BTreeMap<String, SearchIndexDef>,
    /// Named databases other than the implicit `default`. A database is a
    /// namespace prefix on table/index names; this set lets an empty database
    /// (created but holding no tables yet) persist. `default` so older catalogs
    /// load with no databases. See [`crate::namespace`].
    #[serde(default)]
    pub databases: std::collections::BTreeSet<String>,
    /// User principals by name. `default` so older catalogs load.
    #[serde(default)]
    pub users: BTreeMap<String, UserDef>,
    /// RBAC roles by name. `default` so older catalogs load.
    #[serde(default)]
    pub auth_roles: BTreeMap<String, AuthRoleDef>,
    /// Per-object DDL version stamps for last-writer-wins schema replication,
    /// keyed by a kind-prefixed name: `d:<db>`, `t:<table>`, `i:<index>`,
    /// `v:<vector index>`, `s:<search index>`. A `dropped` stamp is a
    /// tombstone. `default` so older
    /// catalogs load with no versions and converge on the next DDL/repair.
    #[serde(default)]
    pub schema_versions: BTreeMap<String, SchemaVersion>,
}

impl Catalog {
    /// Load the catalog from `path`, or return an empty catalog if absent.
    pub fn load(path: impl AsRef<Path>) -> Result<Catalog> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Catalog::default());
        }
        let text = std::fs::read_to_string(path)?;
        let catalog = serde_json::from_str(&text)
            .map_err(|e| EngineError::Constraint(format!("corrupt catalog: {e}")))?;
        Ok(catalog)
    }

    /// Persist the catalog to `path` atomically (write-temp-then-rename).
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(self)
            .map_err(|e| EngineError::Constraint(format!("cannot serialize catalog: {e}")))?;
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}
