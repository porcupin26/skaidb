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

    #[error("database {0:?} does not exist")]
    DatabaseNotFound(String),

    #[error("database {0:?} already exists")]
    DatabaseExists(String),

    #[error("constraint violation: {0}")]
    Constraint(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("cluster error: {0}")]
    Cluster(String),

    #[error("time-series error: {0}")]
    Timeseries(String),
}

impl From<skaidb_fts::FtsError> for EngineError {
    fn from(e: skaidb_fts::FtsError) -> Self {
        match e {
            // Bad configuration or query text: the user's mistake.
            skaidb_fts::FtsError::Config(msg) => EngineError::Type(msg),
            // Internal search-engine failures carry their own prefix; reuse
            // the generic string carrier (the `corrupt row:` precedent).
            other => EngineError::Constraint(other.to_string()),
        }
    }
}

/// Convenience result alias for engine operations.
pub type Result<T> = std::result::Result<T, EngineError>;
