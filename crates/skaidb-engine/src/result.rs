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
