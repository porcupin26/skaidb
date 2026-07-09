//! Replicated node statistics (`observability.node_stats`).
//!
//! Every node INSERTs its own host statistics — CPU, memory, disk, uptime,
//! restarts, OOM kills — into the `node_stats` table: one row per node,
//! keyed on the node id, stamped with the sample time. The row replicates
//! like any other write, so any member serves the whole cluster's picture
//! from one local table read. The dashboard shows each node's data plus its
//! **age** instead of flapping to "unreachable" whenever a live probe misses
//! (a busy node or a network blip), a silent node is simply one whose row
//! stops advancing, and it is all plain SQL:
//! `SELECT node, ts, mem_used_bytes, restarts, oom_kills FROM node_stats`.
//!
//! Runs as the superuser (a node writing its own telemetry, not a client
//! call), every `observability.node_stats_interval_secs` (default 1 s).

use skaidb_proto::Response;

use crate::shared::Shared;

/// The stats table, in the default database (like the `metrics` table).
pub const TABLE: &str = "node_stats";

/// Data older than this is treated as "node gone" rather than merely stale.
pub const STALE_HORIZON_SECS: u64 = 120;

fn exec(ctx: &Shared, sql: &str) -> Result<Response, String> {
    let role = ctx.superuser_role.clone();
    let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(ctx, &role, &mut db, sql, None) {
        Response::Error(e) => Err(e),
        resp => Ok(resp),
    }
}

/// Idempotent DDL for the stats table (broadcasts across the cluster).
pub fn ensure_table(ctx: &Shared) -> Result<(), String> {
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {TABLE} (PRIMARY KEY (node))"),
    )
    .map(|_| ())
}

/// Publish one stats row for this node (INSERT overwrites on the PK).
pub fn publish_tick(ctx: &Shared) -> Result<(), String> {
    let node = ctx
        .backend
        .cluster_stats()
        .map_or_else(|| "local".to_string(), |c| c.node_id);
    let h = ctx.backend.local_host_stats();
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let sql = format!(
        "INSERT INTO {TABLE} (node, ts, cpu_percent, cpus, load1, mem_total_bytes, \
         mem_used_bytes, rss_bytes, disk_read_bps, disk_write_bps, disk_total_bytes, \
         disk_available_bytes, uptime_secs, restarts, oom_kills, cpu_pressure_pct) VALUES \
         ('{}', {ts_ms}, {:.2}, {}, {:.2}, {}, {}, {}, {:.0}, {:.0}, {}, {}, {}, {}, {}, {:.2})",
        node.replace('\'', "''"),
        h.cpu_percent,
        h.cpus,
        h.load1,
        h.mem_total_bytes,
        h.mem_used_bytes,
        h.rss_bytes,
        h.disk_read_bps,
        h.disk_write_bps,
        h.disk_total_bytes,
        h.disk_available_bytes,
        h.uptime_secs,
        h.restarts,
        h.oom_kills,
        h.cpu_pressure_pct,
    );
    exec(ctx, &sql).map(|_| ())
}

/// Read every node's latest stats row: `(node, stats, age_secs)`, newest
/// first sample wins per node. Empty on any error (callers fall back to
/// live probing).
pub fn read_all(ctx: &Shared) -> Vec<(String, skaidb_cluster::host::HostStats, u64)> {
    let resp = match exec(ctx, &format!("SELECT * FROM {TABLE}")) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let Response::Rows { columns, rows } = resp else {
        return Vec::new();
    };
    let idx = |name: &str| columns.iter().position(|c| c == name);
    let (Some(node_i), Some(ts_i)) = (idx("node"), idx("ts")) else {
        return Vec::new();
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let as_f = |v: &skaidb_types::Value| -> f64 {
        match v {
            skaidb_types::Value::Int(i) => *i as f64,
            skaidb_types::Value::Float(f) => *f,
            _ => 0.0,
        }
    };
    let mut out = Vec::new();
    for row in &rows {
        let Some(skaidb_types::Value::String(node)) = row.get(node_i) else {
            continue;
        };
        let ts = row.get(ts_i).map(&as_f).unwrap_or(0.0) as i64;
        let age_secs = (now_ms.saturating_sub(ts).max(0) / 1000) as u64;
        let get = |name: &str| idx(name).and_then(|i| row.get(i)).map(&as_f).unwrap_or(0.0);
        let h = skaidb_cluster::host::HostStats {
            cpu_percent: get("cpu_percent"),
            cpus: get("cpus") as u32,
            load1: get("load1"),
            mem_total_bytes: get("mem_total_bytes") as u64,
            mem_used_bytes: get("mem_used_bytes") as u64,
            rss_bytes: get("rss_bytes") as u64,
            disk_read_bps: get("disk_read_bps"),
            disk_write_bps: get("disk_write_bps"),
            disk_total_bytes: get("disk_total_bytes") as u64,
            disk_available_bytes: get("disk_available_bytes") as u64,
            uptime_secs: get("uptime_secs") as u64,
            restarts: get("restarts") as u64,
            oom_kills: get("oom_kills") as u64,
            cpu_pressure_pct: get("cpu_pressure_pct"),
            stale_secs: age_secs,
            ..Default::default()
        };
        out.push((node.clone(), h, age_secs));
    }
    out
}
