//! Shared execution path used by both the binary and REST endpoints.

use std::sync::{Arc, Mutex};

use skaidb_engine::{Database, QueryOutput};
use skaidb_proto::Response;

/// A database shared across connection-handling threads.
pub type SharedDb = Arc<Mutex<Database>>;

/// Execute one SQL statement and map the engine outcome to a protocol response.
/// All errors (including a poisoned lock) become [`Response::Error`].
pub fn execute(db: &SharedDb, sql: &str) -> Response {
    let mut guard = match db.lock() {
        Ok(g) => g,
        Err(_) => return Response::Error("server lock poisoned".into()),
    };
    match guard.execute(sql) {
        Ok(QueryOutput::Rows(rs)) => Response::Rows {
            columns: rs.columns,
            rows: rs.rows,
        },
        Ok(QueryOutput::Mutation { affected }) => Response::Mutation {
            affected: affected as u64,
        },
        Ok(QueryOutput::Ddl) => Response::Ddl,
        Err(e) => Response::Error(e.to_string()),
    }
}
