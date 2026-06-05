//! Table/index catalog, persisted as JSON alongside the data (SPEC §2).
//!
//! Schema-less means a table records only its primary key and any declared
//! secondary indexes — never a column list.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, Result};

/// A secondary index declaration (`CREATE INDEX ... ON t(path1, path2, ...)`).
/// `paths` is ordered: a composite index sorts by the first path, then the
/// second, and so on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDef {
    pub table: String,
    pub paths: Vec<String>,
}

/// A table definition: just its primary key columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub primary_key: Vec<String>,
}

/// The set of all tables and indexes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub tables: BTreeMap<String, TableDef>,
    pub indexes: BTreeMap<String, IndexDef>,
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
