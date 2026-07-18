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
use skaidb_proto::Response;
use skaidb_types::Value;

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

/// The inventory tab's data: databases → tables (all kinds) and indexes,
/// definition plus this node's usage. RBAC-filtered like `schema_json`:
/// a table (and its indexes) appears only for roles allowed to SELECT it.
pub fn inventory_json(ctx: &Shared, role: &str) -> (u16, String) {
    let Some(inv) = ctx.backend.inventory() else {
        return (500, json!({"error": "engine unavailable"}).to_string());
    };
    let sep = '\u{1f}';
    let split = |name: &str| -> (String, String) {
        match name.split_once(sep) {
            Some((db, bare)) => (db.to_string(), bare.to_string()),
            None => (skaidb_engine::DEFAULT_DATABASE.to_string(), name.to_string()),
        }
    };
    let visible = |db: &str, bare: &str| {
        ctx.allowed_on_table(role, skaidb_auth::Privilege::Select, bare, db)
    };
    use std::collections::BTreeMap;
    // Pin ids render as aliases where one exists — the UI speaks names,
    // durable state speaks ids.
    let aliases: std::collections::HashMap<String, String> =
        crate::naming::all_aliases(ctx).into_iter().collect();
    let mut dbs: BTreeMap<String, (Vec<Json>, Vec<Json>)> = BTreeMap::new();
    for t in &inv.tables {
        let (db, bare) = split(&t.name);
        if !visible(&db, &bare) {
            continue;
        }
        // One human-readable placement summary: "rf 2", "pinned: skai2",
        // or "" for cluster default; "→ moving" appended mid-transition.
        let mut placement = if !t.pinned_nodes.is_empty() {
            format!(
                "pinned: {}",
                t.pinned_nodes
                    .iter()
                    .map(|id| aliases.get(id).unwrap_or(id).as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else if let Some(n) = t.replication {
            format!("rf {n}")
        } else {
            String::new()
        };
        if t.transition {
            placement = format!("{placement} → moving").trim().to_string();
        }
        dbs.entry(db).or_default().0.push(json!({
            "name": bare, "kind": if t.memory { "memory" } else { "table" },
            "key": t.primary_key, "ttl_ms": t.ttl_ms,
            "live_keys": t.live_keys, "tombstones": t.tombstones,
            "disk_bytes": t.disk_bytes, "files": t.sstables,
            "placement": placement, "witness": t.witness,
        }));
    }
    for t in &inv.timeseries {
        let (db, bare) = split(&t.name);
        if !visible(&db, &bare) {
            continue;
        }
        dbs.entry(db).or_default().0.push(json!({
            "name": bare, "kind": "timeseries", "key": t.series_key,
            "ttl_ms": t.retention_ms, "rollup_of": t.rollup_of,
            "live_keys": t.series, "disk_bytes": t.disk_bytes,
        }));
    }
    let mut push_index = |name: &str, table: &str, detail: Json| {
        let (db, bare) = split(name);
        let (tdb, tbare) = split(table);
        if !visible(&tdb, &tbare) {
            return;
        }
        let mut obj = detail;
        obj["name"] = json!(bare);
        obj["table"] = json!(tbare);
        dbs.entry(db).or_default().1.push(obj);
    };
    for i in &inv.indexes {
        push_index(&i.name, &i.table, json!({
            "kind": "secondary", "paths": i.paths,
            "entries": i.entries, "disk_bytes": i.disk_bytes,
        }));
    }
    for v in &inv.vector_indexes {
        push_index(&v.name, &v.table, json!({
            "kind": "vector", "paths": [v.path], "metric": v.metric,
            "dim": v.dim, "ef_search": v.ef_search, "entries": v.vectors,
            "disk_bytes": v.snapshot_bytes,
        }));
    }
    for x in &inv.search_indexes {
        push_index(&x.name, &x.table, json!({
            "kind": "search", "paths": x.paths, "entries": x.docs,
            "disk_bytes": x.disk_bytes, "uncommitted": x.uncommitted,
        }));
    }
    let out: Vec<Json> = dbs
        .into_iter()
        .map(|(db, (tables, indexes))| json!({"name": db, "tables": tables, "indexes": indexes}))
        .collect();
    (200, json!({"databases": out}).to_string())
}

/// Per-node host statistics (CPU, RAM, disk IO, disk space) plus a
/// cluster-level aggregate, for the stats tab's nodes view. Serves
/// `GET /ui/hosts` (authenticated; rest.rs resolves the role first —
/// nothing here is table data, so any authenticated role may read it).
pub fn hosts_json(ctx: &Shared) -> (u16, String) {
    // Primary source: the replicated `node_stats` table every member writes
    // itself into (data + age — no probe fan-out, so a missed probe can't
    // flap a live node to "unreachable"). Members without a fresh row (older
    // version in a rolling upgrade, publishing disabled, or silent past the
    // horizon) fall back to the live probe path, which itself serves the
    // coordinator's cached snapshot before reporting a peer unreachable.
    let published = crate::nodestats::read_all(ctx);
    let mut nodes: Vec<(String, Option<skaidb_cluster::host::HostStats>)> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (node, stats, age) in published {
        if age <= crate::nodestats::STALE_HORIZON_SECS {
            seen.insert(node.clone());
            nodes.push((node, Some(stats)));
        }
    }
    let member_ids: Vec<String> = ctx
        .backend
        .cluster_stats()
        .map(|c| {
            let mut ids: Vec<String> = c.peers.iter().map(|p| p.id.clone()).collect();
            ids.push(c.node_id);
            ids
        })
        .unwrap_or_else(|| vec!["local".to_string()]);
    if member_ids.iter().any(|id| !seen.contains(id)) {
        for (id, stat) in ctx.backend.host_stats() {
            if !seen.contains(&id) {
                nodes.push((id, stat));
            }
        }
    }
    nodes.sort_by(|a, b| a.0.cmp(&b.0));
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
                    "cpu_pressure_pct": h.cpu_pressure_pct,
                    "uptime_secs": h.uptime_secs,
                    "restarts": h.restarts,
                    "oom_kills": h.oom_kills,
                    // Seconds since this node last reported; the UI shows the
                    // age and dims stale rows instead of dropping them.
                    "stale_secs": h.stale_secs,
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

/// Live binary-protocol connections, for the status tab. Unlike
/// `hosts_json`, this is a single local table read — `drivers` is a normal
/// (if memory-only) replicated table, no live-peer-probing fallback needed.
pub fn drivers_json(ctx: &Shared) -> (u16, String) {
    let resp = crate::shared::execute_as(
        ctx,
        &ctx.superuser_role,
        &format!("SELECT * FROM {}", crate::drivers::TABLE),
    );
    let Response::Rows { columns, rows } = resp else {
        // Most likely: no connection has landed yet, so the table hasn't
        // been lazily created — an empty list, not an error, matches what
        // "nobody's connected right now" should look like.
        return (
            200,
            json!({ "drivers": [], "rest": rest_stats_json(ctx) }).to_string(),
        );
    };
    let idx = |name: &str| columns.iter().position(|c| c == name);
    let as_s = |v: Option<&Value>| match v {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let as_i = |v: Option<&Value>| match v {
        Some(Value::Int(i)) => *i,
        _ => 0,
    };
    let (node_i, endpoint_i, addr_i, user_i, ts_i) = (
        idx("node"),
        idx("endpoint"),
        idx("remote_addr"),
        idx("auth_user"),
        idx("connected_at"),
    );
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let drivers: Vec<Json> = rows
        .iter()
        .map(|row| {
            let connected_at_ms = as_i(ts_i.and_then(|i| row.get(i)));
            json!({
                "node": as_s(node_i.and_then(|i| row.get(i))),
                "endpoint": as_s(endpoint_i.and_then(|i| row.get(i))),
                "remote_addr": as_s(addr_i.and_then(|i| row.get(i))),
                "auth_user": as_s(user_i.and_then(|i| row.get(i))),
                "connected_at_ms": connected_at_ms,
                "connected_secs": ((now_ms - connected_at_ms).max(0) / 1000),
            })
        })
        .collect();
    (200, json!({ "drivers": drivers, "rest": rest_stats_json(ctx) }).to_string())
}

/// REST request activity (`skaidb_rest_requests_total{path=…}` + average
/// response time), shown beside the drivers table: REST connections are
/// one-shot and deliberately not in the `drivers` registry, so this is
/// where their traffic becomes visible.
fn rest_stats_json(ctx: &Shared) -> Vec<Json> {
    ctx.metrics
        .rest_stats()
        .into_iter()
        .map(|(label, count, avg_ms)| {
            json!({ "path": label, "requests": count, "avg_ms": (avg_ms * 100.0).round() / 100.0 })
        })
        .collect()
}

/// Registered witnesses, for the status tab, plus the current GC grace
/// period so an operator can see what's actually in effect.
pub fn witnesses_json(ctx: &Shared) -> (u16, String) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let witnesses: Vec<Json> = crate::witnesses::read_all(ctx)
        .into_iter()
        .map(|w| {
            // Per-table sync detail from the heartbeat's watermarks doc:
            // `db.table` → {rows, synced_at}. Summarized (table count +
            // total rows + oldest sync age) with the full list attached.
            let mut tables: Vec<Json> = Vec::new();
            let (mut total_rows, mut oldest_sync_ms) = (0i64, i64::MAX);
            if let Some(Value::Document(d)) = &w.watermarks {
                for (name, entry) in &d.0 {
                    let Value::Document(e) = entry else { continue };
                    let rows = match e.0.get("rows") {
                        Some(Value::Int(n)) => *n,
                        _ => 0,
                    };
                    let synced = match e.0.get("synced_at") {
                        Some(Value::Int(n)) => *n,
                        _ => 0,
                    };
                    total_rows += rows;
                    oldest_sync_ms = oldest_sync_ms.min(synced);
                    tables.push(json!({
                        "table": name,
                        "rows": rows,
                        "synced_secs_ago": ((now_ms - synced).max(0) / 1000),
                    }));
                }
            }
            json!({
                "witness_id": w.witness_id,
                "alias": w.alias,
                "region": w.region,
                "registered_at_ms": w.registered_at_ms,
                "last_seen_at_ms": w.last_seen_at_ms,
                "registered_secs": ((now_ms - w.registered_at_ms).max(0) / 1000),
                "stale_secs": ((now_ms - w.last_seen_at_ms).max(0) / 1000),
                "tables": tables,
                "synced_tables": tables.len(),
                "synced_rows": total_rows,
                "oldest_sync_secs": if oldest_sync_ms == i64::MAX { Json::Null }
                    else { json!(((now_ms - oldest_sync_ms).max(0) / 1000)) },
            })
        })
        .collect();
    let body = json!({
        "witnesses": witnesses,
        "grace_period_secs": crate::witnesses::grace_period_secs(ctx),
    });
    (200, body.to_string())
}

/// What the login screen needs before any authenticated call can succeed.
/// Nothing here is secret (same trust level as `/health` and `/status`).
fn meta_json(ctx: &Shared) -> String {
    let cluster = ctx.backend.cluster_stats();
    // The badge's dotted display name: cluster.function.alias — witness
    // nodes mirror these from their primary; blank until bootstrapped.
    let self_id = cluster
        .as_ref()
        .map(|c| c.node_id.clone())
        .unwrap_or_else(|| "local".to_string());
    let function = if ctx.config_snapshot().witness.enabled { "witness" } else { "node" };
    let dotted = match (crate::naming::cluster_name(ctx), crate::naming::node_alias(ctx, &self_id)) {
        (Some(c), Some(a)) => format!("{c}.{function}.{a}"),
        _ => String::new(),
    };
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "display_name": dotted,
        "node_id": cluster.as_ref().map(|c| c.node_id.clone()).unwrap_or_default(),
        "clustered": cluster.is_some(),
        "auth_required": ctx.authn.required,
        "uptime_seconds": ctx.start.elapsed().as_secs(),
    })
    .to_string()
}
