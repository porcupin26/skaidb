//! Cluster control plane: admin operations exposed over `POST /admin/*`.
//!
//! These drive the runtime resharding / anti-entropy APIs on the cluster
//! coordinator ([`skaidb_cluster::Node`]) — adding and removing nodes, repair,
//! reclaim — plus a read-only topology `status`. All commands require the
//! `Admin` privilege on the whole cluster, and membership changes are serialized
//! (one at a time) because concurrent ring changes aren't linearizable yet.

use serde_json::{json, Value as Json};
use skaidb_auth::{Object, Privilege};

use crate::shared::{Backend, Shared};

/// A parsed admin command.
#[derive(Debug, PartialEq, Eq)]
pub enum AdminCmd {
    /// Topology / membership status (read-only).
    Status,
    /// Add a node to the cluster and migrate it its share (`host:internode_port`).
    AddNode(String),
    /// Gracefully decommission a node by its id (`host:internode_port`).
    RemoveNode(String),
    /// Run a cluster-wide anti-entropy repair pass.
    Repair,
    /// Reclaim space former owners no longer own (post-resharding cleanup).
    Reclaim,
    /// Return a sample of recent slow queries (masked), for drill-down.
    Slow,
    /// Show the whole configuration (secrets masked).
    ConfigShow,
    /// Read one dotted `section.field` config key.
    ConfigGet(String),
    /// Set one config key; applies live when mutable, else persisted for restart.
    ConfigSet { key: String, value: String },
}

/// Map an admin route + body to a command. `None` for an unknown route.
pub fn parse(path: &str, body: &str) -> Option<AdminCmd> {
    match path {
        "/admin/status" => Some(AdminCmd::Status),
        "/admin/slow" => Some(AdminCmd::Slow),
        "/admin/repair" => Some(AdminCmd::Repair),
        "/admin/reclaim" => Some(AdminCmd::Reclaim),
        "/admin/add-node" => Some(AdminCmd::AddNode(field(body, "addr")?)),
        "/admin/remove-node" => Some(AdminCmd::RemoveNode(field(body, "id")?)),
        "/admin/config" => Some(AdminCmd::ConfigShow),
        "/admin/config/get" => Some(AdminCmd::ConfigGet(field(body, "key")?)),
        "/admin/config/set" => Some(AdminCmd::ConfigSet {
            key: field(body, "key")?,
            // The value may legitimately be empty (clearing a path), so don't
            // require it to be non-empty like `field` does.
            value: opt_field(body, "value").unwrap_or_default(),
        }),
        _ => None,
    }
}

/// Like [`field`], but returns `Some("")` for an empty value rather than `None`.
/// `None` only when the key is absent or the body isn't the expected shape.
fn opt_field(body: &str, key: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        if let Ok(Json::Object(map)) = serde_json::from_str::<Json>(trimmed) {
            return match map.get(key) {
                Some(Json::String(s)) => Some(s.clone()),
                _ => None,
            };
        }
        return None;
    }
    Some(trimmed.to_string())
}

/// Extract a value from the body: JSON `{"<key>": "..."}` or a raw string.
fn field(body: &str, key: &str) -> Option<String> {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        if let Ok(Json::Object(map)) = serde_json::from_str::<Json>(trimmed) {
            return match map.get(key) {
                Some(Json::String(s)) if !s.is_empty() => Some(s.clone()),
                _ => None,
            };
        }
        return None;
    }
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Run `cmd` for `role`. Returns `(http_status, json)`.
pub fn handle(ctx: &Shared, role: &str, cmd: AdminCmd) -> (u16, Json) {
    // Every admin op (including status) requires cluster-wide Admin.
    if !ctx.allowed(role, Privilege::Admin, &Object::Global) {
        ctx.metrics.incr_authz_denied();
        return (403, json!({ "error": "permission denied: Admin on Global" }));
    }

    // Slow-query sample is available in both standalone and cluster mode.
    if let AdminCmd::Slow = cmd {
        ctx.metrics.incr("skaidb_admin_total{op=\"slow\"}");
        return (200, ctx.slow_log.snapshot());
    }

    // Config inspection/control is local to each node (works standalone too).
    match cmd {
        AdminCmd::ConfigShow => {
            ctx.metrics.incr("skaidb_admin_total{op=\"config_show\"}");
            return (200, ctx.config_show_json());
        }
        AdminCmd::ConfigGet(key) => {
            ctx.metrics.incr("skaidb_admin_total{op=\"config_get\"}");
            return ctx.config_get_json(&key);
        }
        AdminCmd::ConfigSet { key, value } => {
            ctx.metrics.incr("skaidb_admin_total{op=\"config_set\"}");
            return ctx.config_set(&key, &value);
        }
        _ => {}
    }

    let node = match &ctx.backend {
        Backend::Cluster(node) => node,
        Backend::Local(_) => {
            // Standalone: status is informative; mutating ops are an error.
            return match cmd {
                AdminCmd::Status => (200, json!({ "clustered": false })),
                _ => (
                    400,
                    json!({ "error": "this node is standalone; set `seeds` to form a cluster" }),
                ),
            };
        }
    };

    ctx.metrics
        .incr(&format!("skaidb_admin_total{{op=\"{}\"}}", op_label(&cmd)));

    match cmd {
        AdminCmd::Status => {
            let stats = node.stats();
            // Probe peers for liveness (this is an explicit operator action, so the
            // extra round-trips are acceptable; the metrics scrape doesn't probe).
            let peers = node.peer_stats_probed();
            let configured_not_in_ring: Vec<&str> = peers
                .iter()
                .filter(|p| p.in_config && !p.in_ring)
                .map(|p| p.id.as_str())
                .collect();
            let ring_not_configured: Vec<&str> = peers
                .iter()
                .filter(|p| p.in_ring && !p.in_config)
                .map(|p| p.id.as_str())
                .collect();
            let peers_json: Vec<Json> = peers
                .iter()
                .map(|p| {
                    json!({
                        "id": p.id,
                        "addr": p.addr,
                        "in_config": p.in_config,
                        "in_ring": p.in_ring,
                        "reachable": p.reachable,
                        "hints_pending": p.hints_pending,
                        "lag_ms": p.lag_ms,
                        "reported_epoch": p.reported_epoch,
                        "reported_members": p.reported_members,
                        "lists_self": p.lists_self,
                        "rows": p.rows,
                    })
                })
                .collect();
            let mut members = node.member_ids();
            members.sort();
            // Cross-node disagreement: a reachable peer that doesn't list us, or
            // whose member count differs from ours — the split-brain that
            // per-node config checks can't see. (Detectable only for peers we
            // route to; a stranger node we've never heard of stays invisible.)
            let disagreeing: Vec<&str> = peers
                .iter()
                .filter(|p| {
                    p.reachable == Some(true)
                        && (p.lists_self == Some(false) || p.reported_members != Some(members.len()))
                })
                .map(|p| p.id.as_str())
                .collect();
            (
                200,
                json!({
                    "clustered": true,
                    "node_id": node.id(),
                    "epoch": node.membership_epoch(),
                    "replication_factor": node.replication_factor(),
                    "resharding": stats.resharding_active,
                    // What membership is configured (seeds) vs. what is live (ring).
                    "configured": stats.configured,
                    "self_in_ring": stats.self_in_ring,
                    "members": members,
                    "peers": peers_json,
                    "discrepancies": {
                        "configured_not_in_ring": configured_not_in_ring,
                        "ring_not_configured": ring_not_configured,
                        "membership_disagreement": disagreeing,
                    },
                }),
            )
        }
        AdminCmd::AddNode(addr) => {
            let _guard = lock(ctx);
            match node.add_member(&addr, &addr) {
                Ok(()) => (
                    200,
                    json!({ "ok": true, "added": addr, "epoch": node.membership_epoch() }),
                ),
                Err(e) => (400, json!({ "error": e.to_string() })),
            }
        }
        AdminCmd::RemoveNode(id) => {
            let _guard = lock(ctx);
            match node.remove_member(&id) {
                Ok(()) => (
                    200,
                    json!({ "ok": true, "removed": id, "epoch": node.membership_epoch() }),
                ),
                Err(e) => (400, json!({ "error": e.to_string() })),
            }
        }
        AdminCmd::Repair => match node.repair_cluster() {
            Ok(n) => (200, json!({ "ok": true, "repaired": n })),
            Err(e) => (400, json!({ "error": e.to_string() })),
        },
        AdminCmd::Reclaim => match node.reclaim_cluster() {
            Ok(n) => (200, json!({ "ok": true, "reclaimed": n })),
            Err(e) => (400, json!({ "error": e.to_string() })),
        },
        // Handled before the cluster-node match (work standalone too).
        AdminCmd::Slow
        | AdminCmd::ConfigShow
        | AdminCmd::ConfigGet(_)
        | AdminCmd::ConfigSet { .. } => {
            unreachable!("handled before the cluster dispatch")
        }
    }
}

/// Take the membership lock, recovering from a poisoned mutex (the data is `()`).
fn lock(ctx: &Shared) -> std::sync::MutexGuard<'_, ()> {
    ctx.admin_lock.lock().unwrap_or_else(|p| p.into_inner())
}

fn op_label(cmd: &AdminCmd) -> &'static str {
    match cmd {
        AdminCmd::Status => "status",
        AdminCmd::AddNode(_) => "add_node",
        AdminCmd::RemoveNode(_) => "remove_node",
        AdminCmd::Repair => "repair",
        AdminCmd::Reclaim => "reclaim",
        AdminCmd::Slow => "slow",
        AdminCmd::ConfigShow => "config_show",
        AdminCmd::ConfigGet(_) => "config_get",
        AdminCmd::ConfigSet { .. } => "config_set",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_routes_and_bodies() {
        assert_eq!(parse("/admin/status", ""), Some(AdminCmd::Status));
        assert_eq!(parse("/admin/repair", ""), Some(AdminCmd::Repair));
        assert_eq!(parse("/admin/reclaim", ""), Some(AdminCmd::Reclaim));
        assert_eq!(
            parse("/admin/add-node", r#"{"addr":"10.0.0.4:7100"}"#),
            Some(AdminCmd::AddNode("10.0.0.4:7100".into()))
        );
        assert_eq!(
            parse("/admin/add-node", "10.0.0.4:7100"),
            Some(AdminCmd::AddNode("10.0.0.4:7100".into()))
        );
        assert_eq!(
            parse("/admin/remove-node", r#"{"id":"10.0.0.3:7100"}"#),
            Some(AdminCmd::RemoveNode("10.0.0.3:7100".into()))
        );
        assert_eq!(parse("/admin/add-node", ""), None); // missing addr
        assert_eq!(parse("/admin/bogus", ""), None);
    }
}
