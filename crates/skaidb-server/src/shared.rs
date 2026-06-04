//! Shared server context and the instrumented execution path used by both the
//! binary and REST endpoints.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use skaidb_auth::RoleStore;
use skaidb_engine::{Database, QueryOutput};
use skaidb_proto::Response;

use crate::audit::AuditSettings;
use crate::metrics::Metrics;

/// State shared across connection-handling threads.
#[derive(Debug)]
pub struct Context {
    pub db: Mutex<Database>,
    pub metrics: Metrics,
    pub audit: AuditSettings,
    /// Roles/grants (SPEC §8.2). The configured superuser is bootstrapped here;
    /// per-connection privilege enforcement is wired in once the protocol
    /// carries an authenticated identity.
    pub roles: RoleStore,
}

/// A reference-counted [`Context`] shared by all handlers.
pub type Shared = Arc<Context>;

/// Execute one SQL statement, recording metrics and audit logs, and map the
/// engine outcome to a protocol response. All errors become [`Response::Error`].
pub fn execute(ctx: &Shared, sql: &str) -> Response {
    let start = Instant::now();

    let response = match ctx.db.lock() {
        Ok(mut db) => match db.execute(sql) {
            Ok(QueryOutput::Rows(rs)) => Response::Rows {
                columns: rs.columns,
                rows: rs.rows,
            },
            Ok(QueryOutput::Mutation { affected }) => Response::Mutation {
                affected: affected as u64,
            },
            Ok(QueryOutput::Ddl) => Response::Ddl,
            Err(e) => Response::Error(e.to_string()),
        },
        Err(_) => Response::Error("server lock poisoned".into()),
    };

    let elapsed_ms = start.elapsed().as_millis() as u64;
    record_metrics(ctx, sql, elapsed_ms, &response);

    let err_msg = match &response {
        Response::Error(m) => Some(m.as_str()),
        _ => None,
    };
    ctx.audit.record(sql, elapsed_ms, err_msg);

    response
}

fn record_metrics(ctx: &Shared, sql: &str, elapsed_ms: u64, response: &Response) {
    ctx.metrics.incr(&format!(
        "skaidb_queries_total{{type=\"{}\"}}",
        statement_type(sql)
    ));
    if matches!(response, Response::Error(_)) {
        ctx.metrics.incr("skaidb_query_errors_total");
    }
    if ctx.audit.slow_query_ms > 0 && elapsed_ms >= ctx.audit.slow_query_ms {
        ctx.metrics.incr("skaidb_slow_queries_total");
    }
}

/// Classify a statement by its leading keyword (for the `type` metric label).
fn statement_type(sql: &str) -> &'static str {
    let word = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match word.as_str() {
        "SELECT" => "select",
        "INSERT" => "insert",
        "UPDATE" => "update",
        "DELETE" => "delete",
        "CREATE" | "DROP" => "ddl",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_statements() {
        assert_eq!(statement_type("  select * from t"), "select");
        assert_eq!(statement_type("INSERT INTO t ..."), "insert");
        assert_eq!(statement_type("CREATE TABLE t ..."), "ddl");
        assert_eq!(statement_type("EXPLAIN ..."), "other");
    }
}
