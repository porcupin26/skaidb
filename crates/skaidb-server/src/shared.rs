//! Shared server context and the instrumented execution path used by both the
//! binary and REST endpoints.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use serde_json::{json, Value as Json};

use crate::slowlog::SlowLog;

use skaidb_auth::{Object, Privilege, RoleStore};
use skaidb_cluster::{ClusterStats, Consistency as ClusterConsistency, Node};
use skaidb_config::Config;
use skaidb_engine::{Database, DbStats, EngineError, QueryOutput, SessionEffect, DEFAULT_DATABASE};
use skaidb_proto::{Consistency as ProtoConsistency, Response};
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
    /// Execute `sql` in a session whose current database is `current_db`,
    /// resolving names against it and replicating database/table DDL across the
    /// cluster. `USE` returns [`SessionEffect::UseDatabase`] for the caller to
    /// apply to the connection's current-database state.
    fn execute_session(
        &self,
        current_db: &str,
        sql: &str,
        consistency: Option<ClusterConsistency>,
    ) -> Result<SessionEffect, EngineError> {
        match self {
            // The embedded engine is single-node; consistency does not apply.
            Backend::Local(db) => db
                .lock()
                .map_err(|_| EngineError::Cluster("server lock poisoned".into()))?
                .execute_session(current_db, sql),
            Backend::Cluster(node) => node.execute_session_with(current_db, sql, consistency),
        }
    }

    /// A storage/runtime statistics snapshot for metrics, or `None` if the
    /// storage lock is currently unavailable (poisoned).
    pub fn db_stats(&self, per_table: bool) -> Option<DbStats> {
        match self {
            Backend::Local(db) => db.lock().ok().map(|d| d.stats(per_table)),
            Backend::Cluster(node) => node.db_stats(per_table),
        }
    }

    /// Cluster statistics when running clustered, else `None`.
    pub fn cluster_stats(&self) -> Option<ClusterStats> {
        match self {
            Backend::Local(_) => None,
            Backend::Cluster(node) => Some(node.stats()),
        }
    }

    /// Whether the backend is ready to serve: the storage engine is open and
    /// lockable (not poisoned). Distinct from process liveness.
    pub fn is_ready(&self) -> bool {
        match self {
            Backend::Local(db) => db.lock().is_ok(),
            Backend::Cluster(node) => node.db_stats(false).is_some(),
        }
    }

    /// Whether this backend is a cluster coordinator.
    pub fn is_clustered(&self) -> bool {
        matches!(self, Backend::Cluster(_))
    }

    /// Client (SQL) endpoints of all cluster members, as `host:quic_port`. Lets
    /// a client that connected to one seed discover its peers for failover.
    /// Members are tracked by internode address (`host:internode_port`); we keep
    /// the host and apply `quic_port`, assuming a homogeneous client port across
    /// the cluster. Empty when standalone.
    pub fn member_client_endpoints(&self, quic_port: u16) -> Vec<String> {
        match self {
            Backend::Local(_) => Vec::new(),
            Backend::Cluster(node) => {
                let mut ids = node.member_ids();
                ids.sort();
                ids.into_iter()
                    .map(|id| {
                        let host = id.rsplit_once(':').map(|(h, _)| h).unwrap_or(&id);
                        format!("{host}:{quic_port}")
                    })
                    .collect()
            }
        }
    }
}

/// State shared across connection-handling threads.
#[derive(Debug)]
pub struct Context {
    pub backend: Backend,
    pub metrics: Metrics,
    /// Live-tunable audit/observability settings. Behind a lock so `config set`
    /// on an `observability.*` key takes effect without a restart.
    pub audit: RwLock<AuditSettings>,
    /// Roles/grants (SPEC §8.2).
    pub roles: RoleStore,
    /// Connection authentication (SPEC §8.1).
    pub authn: AuthState,
    /// Role used for the REST gateway and anonymous connections.
    pub superuser_role: String,
    /// Serializes cluster membership changes (add/remove node) so only one runs
    /// at a time — concurrent ring changes aren't linearizable yet.
    pub admin_lock: Mutex<()>,
    /// When the process started — drives `skaidb_uptime_seconds`.
    pub start: Instant,
    /// A bounded ring of recent slow queries, for drill-down via `/admin/slow`.
    pub slow_log: SlowLog,
    /// Authoritative current configuration, for `/admin/config` show/get/set.
    pub config: RwLock<Config>,
    /// Path the config was loaded from, used to persist `config set`. `None`
    /// when the server was started from built-in defaults (no file to write).
    pub config_path: Option<String>,
}

impl Context {
    /// Read the live audit settings, tolerating a poisoned lock.
    pub fn audit(&self) -> std::sync::RwLockReadGuard<'_, AuditSettings> {
        self.audit.read().unwrap_or_else(|e| e.into_inner())
    }

    /// A snapshot of the current configuration.
    pub fn config_snapshot(&self) -> Config {
        self.config.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// `config show`: the whole config with secrets masked.
    pub fn config_show_json(&self) -> Json {
        self.config_snapshot().to_redacted_json()
    }

    /// `config get <key>`: one dotted key, masked.
    pub fn config_get_json(&self, key: &str) -> (u16, Json) {
        match self.config_snapshot().get_key_redacted(key) {
            Some(value) => (200, json!({ "key": key, "value": value })),
            None => (404, json!({ "error": format!("unknown config key: {key}") })),
        }
    }

    /// `config set <key> <value>`: validate, apply live if mutable, and persist
    /// to the config file if one is known. Reports what actually happened.
    pub fn config_set(&self, key: &str, value: &str) -> (u16, Json) {
        let updated = match self.config_snapshot().with_key_set(key, value) {
            Ok(u) => u,
            Err(e) => return (400, json!({ "error": e })),
        };
        let applied = skaidb_config::is_runtime_mutable(key);
        // Apply the observability subset live so it takes effect immediately.
        if key.starts_with("observability.") {
            *self.audit.write().unwrap_or_else(|e| e.into_inner()) =
                AuditSettings::from(&updated.observability);
        }
        // Persist to disk when we know where the config lives.
        let persisted = match &self.config_path {
            Some(path) => {
                if let Err(e) = std::fs::write(path, updated.to_toml_string()) {
                    return (500, json!({ "error": format!("could not write {path}: {e}") }));
                }
                true
            }
            None => false,
        };
        *self.config.write().unwrap_or_else(|e| e.into_inner()) = updated;
        (
            200,
            json!({
                "ok": true,
                "key": key,
                "applied": applied,
                "restart_required": !applied,
                "persisted": persisted,
            }),
        )
    }
}

/// A reference-counted [`Context`] shared by all handlers.
pub type Shared = Arc<Context>;

/// Pull current storage/cluster statistics from the backend and write them into
/// the metrics registry as gauges/counters. Called at scrape time so the
/// sub-crates need not hold a registry handle (SPEC §10 pull-on-scrape model).
pub fn collect_runtime_metrics(ctx: &Shared) {
    ctx.metrics
        .set("skaidb_uptime_seconds", ctx.start.elapsed().as_secs());

    let per_table = ctx
        .config
        .read()
        .map(|c| c.observability.per_table_metrics)
        .unwrap_or(false);
    if let Some(s) = ctx.backend.db_stats(per_table) {
        let m = &ctx.metrics;
        m.set("skaidb_storage_tables", s.tables as u64);
        m.set(
            "skaidb_storage_indexes",
            (s.secondary_indexes + s.vector_indexes) as u64,
        );
        m.set("skaidb_storage_memtable_bytes", s.memtable_bytes);
        m.set("skaidb_storage_sstables", s.sstable_count);
        m.set("skaidb_storage_disk_bytes", s.disk_bytes);
        m.set("skaidb_storage_compactions_total", s.compactions);
        m.set("skaidb_storage_compaction_bytes_total", s.compaction_bytes);
        m.set("skaidb_wal_bytes", s.wal_bytes);
        m.set("skaidb_wal_fsyncs_total", s.wal_fsyncs);
        m.set("skaidb_cache_hits_total", s.cache_hits);
        m.set("skaidb_cache_misses_total", s.cache_misses);
        m.set("skaidb_cache_evictions_total", s.cache_evictions);
        m.set("skaidb_cache_entries", s.cache_entries);
        m.set("skaidb_bloom_negative_lookups_total", s.bloom_negatives);
        m.set("skaidb_vector_indexes", s.vector_indexes as u64);
        m.set("skaidb_vector_indexed_total", s.vectors_indexed as u64);
        m.set(
            "skaidb_vector_rebuild_seconds",
            s.vector_rebuild_ms / 1000,
        );
        for t in &s.per_table {
            let label = escape_label(&t.name);
            m.set(
                &format!("skaidb_table_live_keys{{table=\"{label}\"}}"),
                t.live_keys,
            );
            m.set(
                &format!("skaidb_table_tombstones{{table=\"{label}\"}}"),
                t.tombstones,
            );
            m.set(
                &format!("skaidb_table_disk_bytes{{table=\"{label}\"}}"),
                t.disk_bytes,
            );
        }
    }

    if let Some(c) = ctx.backend.cluster_stats() {
        let m = &ctx.metrics;
        m.set("skaidb_membership_epoch", c.epoch);
        m.set("skaidb_cluster_members", c.members as u64);
        m.set("skaidb_cluster_resharding", u64::from(c.resharding_active));
        m.set("skaidb_cluster_hints_pending", c.hints_pending as u64);
        m.set(
            &format!("skaidb_cluster_writes_total{{consistency=\"{}\"}}", c.write_consistency),
            c.writes_total,
        );
        m.set(
            &format!("skaidb_cluster_reads_total{{consistency=\"{}\"}}", c.read_consistency),
            c.reads_total,
        );
        m.set(
            "skaidb_cluster_quorum_failures_total{kind=\"write\"}",
            c.write_quorum_failures,
        );
        m.set(
            "skaidb_cluster_quorum_failures_total{kind=\"read\"}",
            c.read_quorum_failures,
        );
        m.set("skaidb_cluster_read_repairs_total", c.read_repairs);
        m.set("skaidb_cluster_hints_stored_total", c.hints_stored);
        m.set("skaidb_cluster_hints_replayed_total", c.hints_replayed);
        m.set("skaidb_cluster_peer_requests_total", c.peer_requests);
        m.set("skaidb_cluster_peer_errors_total", c.peer_errors);
    }
}

/// Sanitize a value used inside a Prometheus label so it can't break the line
/// (table names are user-controlled). Backslashes and quotes are escaped.
fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Execute as the superuser role against the `default` database (used by the
/// stateless REST gateway). Cross-database access there is via `db.table`.
pub fn execute(ctx: &Shared, sql: &str) -> Response {
    let role = ctx.superuser_role.clone();
    execute_as(ctx, &role, sql)
}

/// Execute one statement on behalf of `role` against the `default` database
/// (stateless: any `USE` applies only for this single call).
pub fn execute_as(ctx: &Shared, role: &str, sql: &str) -> Response {
    let mut current_db = DEFAULT_DATABASE.to_string();
    // The stateless REST gateway carries no per-request consistency; use the
    // server defaults.
    execute_session_as(ctx, role, &mut current_db, sql, None)
}

/// Map a wire-protocol consistency level to the cluster's internal one.
fn map_consistency(c: ProtoConsistency) -> ClusterConsistency {
    match c {
        ProtoConsistency::One => ClusterConsistency::One,
        ProtoConsistency::Quorum => ClusterConsistency::Quorum,
        ProtoConsistency::All => ClusterConsistency::All,
    }
}

/// Execute one SQL statement on behalf of `role` within a session whose current
/// database is `current_db`: enforce RBAC, run it, and record metrics/audit. A
/// successful `USE` updates `current_db` in place (the connection's state).
/// `consistency` overrides the cluster default for this statement when `Some`.
/// All errors become [`Response::Error`].
pub fn execute_session_as(
    ctx: &Shared,
    role: &str,
    current_db: &mut String,
    sql: &str,
    consistency: Option<ProtoConsistency>,
) -> Response {
    // Authorization: check the role may perform the statement before executing.
    if let Some((privilege, object)) = required_privilege(sql) {
        if !ctx.roles.has_privilege(role, privilege, &object) {
            ctx.metrics.incr("skaidb_authz_denied_total");
            return Response::Error(format!("permission denied: {privilege:?} on {object:?}"));
        }
    }

    let start = Instant::now();
    ctx.metrics.gauge_inc("skaidb_queries_in_flight");

    let response = match ctx
        .backend
        .execute_session(current_db, sql, consistency.map(map_consistency))
    {
        Ok(SessionEffect::Output(QueryOutput::Rows(rs))) => Response::Rows {
            columns: rs.columns,
            rows: rs.rows,
        },
        Ok(SessionEffect::Output(QueryOutput::Mutation { affected })) => Response::Mutation {
            affected: affected as u64,
        },
        Ok(SessionEffect::Output(QueryOutput::Ddl)) => Response::Ddl,
        // `USE <db>` switched the connection's current database.
        Ok(SessionEffect::UseDatabase(name)) => {
            *current_db = name;
            Response::Ddl
        }
        Err(e) => Response::Error(e.to_string()),
    };

    ctx.metrics.gauge_dec("skaidb_queries_in_flight");
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis() as u64;
    record_metrics(ctx, sql, elapsed.as_secs_f64(), elapsed_ms, &response);

    let err_msg = match &response {
        Response::Error(m) => Some(m.as_str()),
        _ => None,
    };
    ctx.audit().record(sql, elapsed_ms, err_msg);

    response
}

fn record_metrics(
    ctx: &Shared,
    sql: &str,
    elapsed_secs: f64,
    elapsed_ms: u64,
    response: &Response,
) {
    let kind = statement_type(sql);
    ctx.metrics
        .incr(&format!("skaidb_queries_total{{type=\"{kind}\"}}"));
    ctx.metrics.observe(
        &format!("skaidb_query_duration_seconds{{type=\"{kind}\"}}"),
        elapsed_secs,
    );

    match response {
        Response::Rows { columns, rows } => {
            ctx.metrics
                .add("skaidb_rows_returned_total", rows.len() as u64);
            // Cells returned ≈ result width; a cheap proxy for result volume that
            // avoids re-serializing every row here (exact bytes are accounted at
            // the wire writer).
            let cells = (rows.len() * columns.len().max(1)) as u64;
            ctx.metrics.add("skaidb_rows_scanned_total", cells);
        }
        Response::Error(msg) => {
            ctx.metrics.incr(&format!(
                "skaidb_query_errors_total{{class=\"{}\"}}",
                error_class(msg)
            ));
        }
        _ => {}
    }

    // Transaction control statements, split by kind.
    if kind == "tx" {
        if let Some(tx) = tx_kind(sql) {
            ctx.metrics
                .incr(&format!("skaidb_transactions_total{{kind=\"{tx}\"}}"));
        }
    }

    let slow_query_ms = ctx.audit().slow_query_ms;
    if slow_query_ms > 0 && elapsed_ms >= slow_query_ms {
        ctx.metrics.incr("skaidb_slow_queries_total");
        ctx.slow_log
            .record(&crate::audit::mask_sql(sql), elapsed_ms);
    }
}

/// Bucket an error message into a small, bounded set of classes so
/// `skaidb_query_errors_total` is actionable without unbounded label values.
fn error_class(msg: &str) -> &'static str {
    let m = msg.to_ascii_lowercase();
    if m.contains("permission denied") {
        "permission"
    } else if m.contains("quorum") || m.contains("timeout") || m.contains("timed out") {
        "timeout"
    } else if m.contains("parse")
        || m.contains("expected")
        || m.contains("syntax")
        || m.contains("unexpected")
    {
        "parse"
    } else if m.contains("constraint")
        || m.contains("primary key")
        || m.contains("already exists")
        || m.contains("does not exist")
        || m.contains("not found")
    {
        "constraint"
    } else if m.contains("corrupt") || m.contains("io ") || m.contains("storage") {
        "storage"
    } else {
        "other"
    }
}

/// The transaction-control kind for `skaidb_transactions_total`.
fn tx_kind(sql: &str) -> Option<&'static str> {
    let word = sql.split_whitespace().next()?.to_ascii_uppercase();
    match word.as_str() {
        "BEGIN" => Some("begin"),
        "COMMIT" => Some("commit"),
        "ROLLBACK" => Some("rollback"),
        _ => None,
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
        // Read-only catalog introspection needs no special privilege — it exposes
        // only table/index names, letting a monitoring agent enumerate the schema
        // without `/query` data access.
        Statement::ShowTables
        | Statement::ShowIndexes
        | Statement::ShowStatus
        | Statement::ShowDatabases => return None,
        // Multi-database statements are an embedded-CLI concept; the clustered
        // engine rejects them, but gate the mutating ones as global writes.
        Statement::CreateDatabase { .. } => (Privilege::Create, Object::Global),
        Statement::DropDatabase { .. } => (Privilege::Drop, Object::Global),
        Statement::UseDatabase { .. } => return None,
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
