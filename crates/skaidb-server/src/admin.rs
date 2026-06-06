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
}

/// Map an admin route + body to a command. `None` for an unknown route.
pub fn parse(path: &str, body: &str) -> Option<AdminCmd> {
    match path {
        "/admin/status" => Some(AdminCmd::Status),
        "/admin/repair" => Some(AdminCmd::Repair),
        "/admin/reclaim" => Some(AdminCmd::Reclaim),
        "/admin/add-node" => Some(AdminCmd::AddNode(field(body, "addr")?)),
        "/admin/remove-node" => Some(AdminCmd::RemoveNode(field(body, "id")?)),
        _ => None,
    }
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
    if !ctx.roles.has_privilege(role, Privilege::Admin, &Object::Global) {
        ctx.metrics.incr("skaidb_authz_denied_total");
        return (403, json!({ "error": "permission denied: Admin on Global" }));
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
            let mut members = node.member_ids();
            members.sort();
            (
                200,
                json!({
                    "clustered": true,
                    "node_id": node.id(),
                    "epoch": node.membership_epoch(),
                    "replication_factor": node.replication_factor(),
                    "members": members,
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
