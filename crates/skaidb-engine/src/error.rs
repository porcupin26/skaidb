//! Query engine error types.

use skaidb_sql::ParseError;
use skaidb_storage::StorageError;

/// Errors raised while planning or executing a statement.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("catalog I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("table {0:?} does not exist")]
    TableNotFound(String),

    #[error("table {0:?} already exists")]
    TableExists(String),

    #[error("index {0:?} does not exist")]
    IndexNotFound(String),

    #[error("index {0:?} already exists")]
    IndexExists(String),

    #[error("constraint violation: {0}")]
    Constraint(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("cluster error: {0}")]
    Cluster(String),
}

/// Convenience result alias for engine operations.
pub type Result<T> = std::result::Result<T, EngineError>;
