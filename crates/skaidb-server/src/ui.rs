//! The built-in web UI (docs/UI.md) — a pure API client embedded in the
//! binary at compile time. It adds no privileged surface: the shell and its
//! assets are static and secret-free, and every data call the page makes goes
//! through the ordinary authenticated endpoints (`POST /query`, `GET /status`,
//! `POST /admin/*`). `GET /ui/meta` is the one new JSON route; it carries the
//! same trust level as `/health`.
//!
//! `[ui] enabled` is live-mutable: the guard reads the live config on every
//! request, so `config set ui.enabled false` 404s the whole prefix
//! immediately — indistinguishable from a build without a UI.

use serde_json::{json, Value as Json};

use crate::shared::Shared;

const HTML: &str = include_str!("../assets/ui.html");
const CSS: &str = include_str!("../assets/ui.css");
const JS: &str = include_str!("../assets/ui.js");

/// The no-external-assets rule, enforced by the browser too.
pub const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
     img-src 'self' data:; connect-src 'self'";

/// A response ready for the wire: status, content type, body.
pub struct Asset {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

/// Route a `GET /ui[...]` request, or `None` if the path isn't ours.
/// Returns 404 for every `/ui` path when the UI is disabled (live config).
pub fn try_route(ctx: &Shared, path: &str) -> Option<Asset> {
    if path != "/ui" && !path.starts_with("/ui/") {
        return None;
    }
    let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
    if !enabled {
        return Some(Asset {
            status: 404,
            content_type: "application/json",
            body: "{\"error\": \"not found\"}".to_string(),
        });
    }
    let asset = match path {
        "/ui" | "/ui/" => Asset {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: HTML.to_string(),
        },
        "/ui/app.css" => Asset {
            status: 200,
            content_type: "text/css; charset=utf-8",
            body: CSS.to_string(),
        },
        "/ui/app.js" => Asset {
            status: 200,
            content_type: "text/javascript; charset=utf-8",
            body: JS.to_string(),
        },
        "/ui/meta" => Asset {
            status: 200,
            content_type: "application/json",
            body: meta_json(ctx),
        },
        _ => Asset {
            status: 404,
            content_type: "application/json",
            body: "{\"error\": \"not found\"}".to_string(),
        },
    };
    Some(asset)
}

/// The schema visible to `role` — databases and their tables, filtered by
/// the same RBAC check `/query` enforces (`Select` on the table, satisfied
/// by a table, database, or global grant, following role inheritance). A
/// database with no visible tables still appears if the role holds a
/// database-level (or global) grant on it. Serves `GET /ui/schema`
/// (authenticated; the route in rest.rs resolves `role` first).
pub fn schema_json(ctx: &Shared, role: &str) -> (u16, String) {
    use skaidb_proto::Response;

    let run = |sql: &str, db: &str| -> Result<Vec<Vec<skaidb_types::Value>>, String> {
        let mut current_db = db.to_string();
        match crate::shared::execute_session_as(ctx, role, &mut current_db, sql, None) {
            Response::Rows { rows, .. } => Ok(rows),
            Response::Error(e) => Err(e),
            other => Err(format!("unexpected response: {other:?}")),
        }
    };

    let databases = match run("SHOW DATABASES", skaidb_engine::DEFAULT_DATABASE) {
        Ok(rows) => rows,
        Err(e) => return (500, json!({"error": e}).to_string()),
    };
    let mut out = Vec::new();
    for row in databases {
        let Some(skaidb_types::Value::String(db)) = row.first() else {
            continue;
        };
        let tables = match run("SHOW TABLES", db) {
            Ok(rows) => rows,
            Err(_) => continue, // e.g. dropped concurrently
        };
        let visible: Vec<Json> = tables
            .iter()
            .filter_map(|row| match (row.first(), row.get(1)) {
                (Some(skaidb_types::Value::String(table)), pk) => ctx
                    .allowed_on_table(role, skaidb_auth::Privilege::Select, table, db)
                    .then(|| {
                        json!({
                            "name": table,
                            "primary_key": pk.map(|v| v.to_json()).unwrap_or(Json::Null),
                        })
                    }),
                _ => None,
            })
            .collect();
        let db_granted = ctx.allowed(
            role,
            skaidb_auth::Privilege::Select,
            &skaidb_auth::Object::Database(db.clone()),
        ) || ctx.allowed(
            role,
            skaidb_auth::Privilege::Select,
            &skaidb_auth::Object::Global,
        );
        if !visible.is_empty() || db_granted {
            out.push(json!({"name": db, "tables": visible}));
        }
    }
    (200, json!({"databases": out}).to_string())
}

/// Per-node host statistics (CPU, RAM, disk IO, disk space) plus a
/// cluster-level aggregate, for the stats tab's nodes view. Serves
/// `GET /ui/hosts` (authenticated; rest.rs resolves the role first —
/// nothing here is table data, so any authenticated role may read it).
pub fn hosts_json(ctx: &Shared) -> (u16, String) {
    let nodes = ctx.backend.host_stats();
    let mut agg_cpu = 0.0f64;
    let mut agg_cpu_n = 0u32;
    let (mut mem_total, mut mem_used) = (0u64, 0u64);
    let (mut read_bps, mut write_bps) = (0.0f64, 0.0f64);
    let (mut disk_total, mut disk_avail) = (0u64, 0u64);
    let mut reachable = 0usize;
    let rows: Vec<Json> = nodes
        .iter()
        .map(|(id, stat)| match stat {
            Some(h) => {
                reachable += 1;
                agg_cpu += h.cpu_percent;
                agg_cpu_n += 1;
                mem_total += h.mem_total_bytes;
                mem_used += h.mem_used_bytes;
                read_bps += h.disk_read_bps;
                write_bps += h.disk_write_bps;
                disk_total += h.disk_total_bytes;
                disk_avail += h.disk_available_bytes;
                json!({
                    "id": id,
                    "reachable": true,
                    "cpu_percent": h.cpu_percent,
                    "cpus": h.cpus,
                    "load1": h.load1,
                    "mem_total_bytes": h.mem_total_bytes,
                    "mem_used_bytes": h.mem_used_bytes,
                    "rss_bytes": h.rss_bytes,
                    "disk_read_bps": h.disk_read_bps,
                    "disk_write_bps": h.disk_write_bps,
                    "disk_total_bytes": h.disk_total_bytes,
                    "disk_available_bytes": h.disk_available_bytes,
                })
            }
            None => json!({"id": id, "reachable": false}),
        })
        .collect();
    let body = json!({
        "nodes": rows,
        "cluster": {
            "nodes": nodes.len(),
            "reachable": reachable,
            "cpu_percent_avg": if agg_cpu_n > 0 { agg_cpu / agg_cpu_n as f64 } else { 0.0 },
            "mem_total_bytes": mem_total,
            "mem_used_bytes": mem_used,
            "disk_read_bps": read_bps,
            "disk_write_bps": write_bps,
            "disk_total_bytes": disk_total,
            "disk_available_bytes": disk_avail,
        },
    });
    (200, body.to_string())
}

/// What the login screen needs before any authenticated call can succeed.
/// Nothing here is secret (same trust level as `/health` and `/status`).
fn meta_json(ctx: &Shared) -> String {
    let cluster = ctx.backend.cluster_stats();
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "node_id": cluster.as_ref().map(|c| c.node_id.clone()).unwrap_or_default(),
        "clustered": cluster.is_some(),
        "auth_required": ctx.authn.required,
        "uptime_seconds": ctx.start.elapsed().as_secs(),
    })
    .to_string()
}
