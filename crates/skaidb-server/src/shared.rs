//! Shared server context and the instrumented execution path used by both the
//! binary and REST endpoints.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use skaidb_auth::{Object, Privilege, RoleStore};
use skaidb_cluster::Node;
use skaidb_engine::{Database, EngineError, QueryOutput};
use skaidb_proto::Response;
use skaidb_sql::ast::Statement;

use crate::audit::AuditSettings;
use crate::authn::AuthState;
use crate::metrics::Metrics;

/// Where statements actually execute: a local single-node engine, or the
/// cluster coordinator that replicates across nodes.
#[derive(Debug)]
pub enum Backend {
    // Boxed so the enum isn't dominated by the large embedded `Database`.
    Local(Box<Mutex<Database>>),
    Cluster(Arc<Node>),
}

impl Backend {
    fn execute(&self, sql: &str) -> Result<QueryOutput, EngineError> {
        match self {
            Backend::Local(db) => db
                .lock()
                .map_err(|_| EngineError::Cluster("server lock poisoned".into()))?
                .execute(sql),
            Backend::Cluster(node) => node.execute(sql),
        }
    }
}

/// State shared across connection-handling threads.
#[derive(Debug)]
pub struct Context {
    pub backend: Backend,
    pub metrics: Metrics,
    pub audit: AuditSettings,
    /// Roles/grants (SPEC §8.2).
    pub roles: RoleStore,
    /// Connection authentication (SPEC §8.1).
    pub authn: AuthState,
    /// Role used for the REST gateway and anonymous connections.
    pub superuser_role: String,
    /// Serializes cluster membership changes (add/remove node) so only one runs
    /// at a time — concurrent ring changes aren't linearizable yet.
    pub admin_lock: Mutex<()>,
}

/// A reference-counted [`Context`] shared by all handlers.
pub type Shared = Arc<Context>;

/// Execute as the superuser role (used by the REST gateway).
pub fn execute(ctx: &Shared, sql: &str) -> Response {
    let role = ctx.superuser_role.clone();
    execute_as(ctx, &role, sql)
}

/// Execute one SQL statement on behalf of `role`: enforce RBAC, then run it,
/// recording metrics and audit logs. All errors become [`Response::Error`].
pub fn execute_as(ctx: &Shared, role: &str, sql: &str) -> Response {
    // Authorization: check the role may perform the statement before executing.
    if let Some((privilege, object)) = required_privilege(sql) {
        if !ctx.roles.has_privilege(role, privilege, &object) {
            ctx.metrics.incr("skaidb_authz_denied_total");
            return Response::Error(format!("permission denied: {privilege:?} on {object:?}"));
        }
    }

    let start = Instant::now();

    let response = match ctx.backend.execute(sql) {
        Ok(QueryOutput::Rows(rs)) => Response::Rows {
            columns: rs.columns,
            rows: rs.rows,
        },
        Ok(QueryOutput::Mutation { affected }) => Response::Mutation {
            affected: affected as u64,
        },
        Ok(QueryOutput::Ddl) => Response::Ddl,
        Err(e) => Response::Error(e.to_string()),
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

/// The privilege and object a statement requires (SPEC §8.2). Returns `None`
/// when the SQL does not parse — the engine then reports the parse error.
fn required_privilege(sql: &str) -> Option<(Privilege, Object)> {
    Some(match skaidb_sql::parse(sql).ok()? {
        Statement::Select(s) => (Privilege::Select, Object::Table(s.from)),
        Statement::Insert(i) => (Privilege::Insert, Object::Table(i.table)),
        Statement::Update(u) => (Privilege::Update, Object::Table(u.table)),
        Statement::Delete(d) => (Privilege::Delete, Object::Table(d.table)),
        Statement::CreateTable(_) => (Privilege::Create, Object::Global),
        Statement::CreateIndex(ci) => (Privilege::Create, Object::Table(ci.table)),
        Statement::CreateVectorIndex(ci) => (Privilege::Create, Object::Table(ci.table)),
        Statement::DropTable { name, .. } => (Privilege::Drop, Object::Table(name)),
        Statement::DropIndex { .. } => (Privilege::Drop, Object::Global),
        Statement::DropVectorIndex { .. } => (Privilege::Drop, Object::Global),
        Statement::AlterTable(a) => (Privilege::Create, Object::Table(a.name)),
        // Transaction control affects writes; gate it like a global write.
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            (Privilege::Insert, Object::Global)
        }
    })
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
        "CREATE" | "DROP" | "ALTER" => "ddl",
        "BEGIN" | "COMMIT" | "ROLLBACK" => "tx",
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
