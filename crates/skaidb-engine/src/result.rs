//! Result types returned by statement execution.

use skaidb_types::Value;

/// A tabular result set (from `SELECT`).
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl ResultSet {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        ResultSet { columns, rows }
    }
}

/// The outcome of executing one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutput {
    /// A `SELECT` produced rows.
    Rows(ResultSet),
    /// A DML statement changed `affected` rows.
    Mutation { affected: usize },
    /// A DDL statement succeeded.
    Ddl,
}

/// The effect of executing one statement in a *session* that has a current
/// database. Most statements produce a [`QueryOutput`]; `USE <db>` instead asks
/// the stateful caller (the embedded session or a server connection) to switch
/// its current database, which the engine cannot hold itself.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionEffect {
    /// A normal statement result.
    Output(QueryOutput),
    /// `USE <name>` — set the caller's current database to `name` (already
    /// validated to exist).
    UseDatabase(String),
}
