//! Cluster + node naming (`cluster_meta`, `node_aliases`).
//!
//! Every deployment gets a human name without asking: the cluster gets a
//! random `adjective-animal` name at first boot (whichever member first
//! finds none wins a benign LWW race), and every member self-registers a
//! random alias keyed by its STABLE internode id. Names live in ordinary
//! replicated tables — not config, which is per-node and could diverge —
//! so a rename from any member reaches all of them, and a joiner/witness
//! inherits them through the paths that already exist.
//!
//! Dotted notation: `<cluster>.<function>.<alias>` with function `node`
//! or `witness` (a witness's alias lives in the `witnesses` registry, the
//! stable `witness_id` untouched by renames). Aliases are sugar; ids are
//! truth — anything durable (table pins, membership) stores ids, so a
//! rename never moves data.
//!
//! Renames (`ALTER CLUSTER SET NAME`, `ALTER NODE ... SET NAME`) are
//! Admin-gated and REFUSED on witness nodes: a witness mirrors identity,
//! it must not fork it (the same one-way principle as `server.read_only`).

use skaidb_proto::Response;
use skaidb_types::Value;

use crate::shared::Shared;

pub const CLUSTER_META_TABLE: &str = "cluster_meta";
pub const NODE_ALIASES_TABLE: &str = "node_aliases";

const ADJECTIVES: &[&str] = &[
    "amber", "brisk", "calm", "dapper", "eager", "fabled", "gentle", "hazel", "ivory", "jolly",
    "keen", "lucid", "mellow", "nimble", "opal", "placid", "quiet", "rustic", "sable", "tidy",
    "umber", "vivid", "wry", "young", "zesty", "bold", "crisp", "deft", "early", "fleet",
];
const ANIMALS: &[&str] = &[
    "falcon", "otter", "lynx", "heron", "badger", "crane", "dingo", "egret", "ferret", "gecko",
    "ibis", "jackal", "kestrel", "lemur", "marten", "newt", "osprey", "puffin", "quail", "raven",
    "stoat", "tapir", "urchin", "vole", "wren", "yak", "zebra", "bison", "condor", "dormouse",
];

/// A random `adjective-animal` name. Entropy comes from the OS-seeded
/// `RandomState` hasher (std-only, non-cryptographic — these are display
/// names, not secrets).
pub fn random_name() -> String {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    let r = h.finish();
    format!(
        "{}-{}",
        ADJECTIVES[(r % ADJECTIVES.len() as u64) as usize],
        ANIMALS[((r >> 16) % ANIMALS.len() as u64) as usize],
    )
}

fn exec(ctx: &Shared, sql: &str) -> Result<Response, String> {
    let role = ctx.superuser_role.clone();
    let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(ctx, &role, &mut db, sql, None) {
        Response::Error(e) => Err(e),
        resp => Ok(resp),
    }
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn one_string(resp: Response) -> Option<String> {
    match resp {
        Response::Rows { rows, .. } => rows.first().and_then(|r| match r.first() {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        }),
        _ => None,
    }
}

/// Idempotent bootstrap: ensure the tables, seed the cluster name if
/// absent, self-register this node's random alias if absent. Tolerates
/// the cluster not being ready at boot (callers retry).
pub fn bootstrap(ctx: &Shared, self_id: &str) -> Result<(), String> {
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {CLUSTER_META_TABLE} (PRIMARY KEY (id))"),
    )?;
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {NODE_ALIASES_TABLE} (PRIMARY KEY (node_id))"),
    )?;
    if cluster_name(ctx).is_none() {
        exec(
            ctx,
            &format!(
                "INSERT INTO {CLUSTER_META_TABLE} (id, name) VALUES ('cluster', '{}')",
                quote(&random_name())
            ),
        )?;
    }
    let have = exec(
        ctx,
        &format!(
            "SELECT alias FROM {NODE_ALIASES_TABLE} WHERE node_id = '{}'",
            quote(self_id)
        ),
    )?;
    if one_string(have).is_none() {
        exec(
            ctx,
            &format!(
                "INSERT INTO {NODE_ALIASES_TABLE} (node_id, alias, function) \
                 VALUES ('{}', '{}', 'node')",
                quote(self_id),
                quote(&random_name()),
            ),
        )?;
    }
    Ok(())
}

/// The cluster's name, if seeded.
pub fn cluster_name(ctx: &Shared) -> Option<String> {
    one_string(
        exec(
            ctx,
            &format!("SELECT name FROM {CLUSTER_META_TABLE} WHERE id = 'cluster'"),
        )
        .ok()?,
    )
}

/// A member's alias, if registered.
pub fn node_alias(ctx: &Shared, node_id: &str) -> Option<String> {
    one_string(
        exec(
            ctx,
            &format!(
                "SELECT alias FROM {NODE_ALIASES_TABLE} WHERE node_id = '{}'",
                quote(node_id)
            ),
        )
        .ok()?,
    )
}

/// Every registered `(node_id, alias)` pair (function 'node').
pub fn all_aliases(ctx: &Shared) -> Vec<(String, String)> {
    let Ok(Response::Rows { columns, rows }) = exec(
        ctx,
        &format!("SELECT node_id, alias FROM {NODE_ALIASES_TABLE}"),
    ) else {
        return Vec::new();
    };
    let idx = |n: &str| columns.iter().position(|c| c == n);
    let (Some(id_i), Some(a_i)) = (idx("node_id"), idx("alias")) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|r| match (r.get(id_i), r.get(a_i)) {
            (Some(Value::String(id)), Some(Value::String(a))) => Some((id.clone(), a.clone())),
            _ => None,
        })
        .collect()
}

/// What an `ALTER NODE '<ref>'` reference resolved to.
pub enum NodeRef {
    /// A cluster member: the stable internode id.
    Member(String),
    /// A witness: the stable witness_id in the `witnesses` registry.
    Witness(String),
}

/// Resolve a node reference — bare alias, dotted
/// `<cluster>.<function>.<alias>`, or a raw id — against the alias and
/// witness registries. A dotted form's cluster segment must match this
/// cluster's name; the bare form searches members first, then witnesses.
pub fn resolve_node_ref(ctx: &Shared, reference: &str) -> Result<NodeRef, String> {
    let (want_fn, name) = match reference.splitn(3, '.').collect::<Vec<_>>()[..] {
        [cluster, function, alias] => {
            let ours = cluster_name(ctx).unwrap_or_default();
            if cluster != ours {
                return Err(format!("unknown cluster '{cluster}' (this is '{ours}')"));
            }
            (Some(function.to_string()), alias.to_string())
        }
        _ => (None, reference.to_string()),
    };
    if want_fn.as_deref() != Some("witness") {
        for (id, alias) in all_aliases(ctx) {
            if alias == name || id == name {
                return Ok(NodeRef::Member(id));
            }
        }
    }
    if want_fn.as_deref() != Some("node") {
        for w in crate::witnesses::read_all(ctx) {
            if w.alias == name || w.witness_id == name {
                return Ok(NodeRef::Witness(w.witness_id));
            }
        }
    }
    Err(format!("no member or witness named '{reference}'"))
}

/// `ALTER CLUSTER SET NAME` / `ALTER NODE ... SET NAME` execution. The
/// witness refusal is enforced by the CALLER (shared.rs) before RBAC —
/// this function assumes a member context.
pub fn rename_cluster(ctx: &Shared, name: &str) -> Result<(), String> {
    exec(
        ctx,
        &format!(
            "INSERT INTO {CLUSTER_META_TABLE} (id, name) VALUES ('cluster', '{}')",
            quote(name)
        ),
    )
    .map(|_| ())
}

pub fn rename_node(ctx: &Shared, reference: &str, name: &str) -> Result<(), String> {
    // Best-effort uniqueness within (cluster, function) — no transactions,
    // so a racing duplicate is detectable-and-fixable, not impossible.
    match resolve_node_ref(ctx, reference)? {
        NodeRef::Member(id) => {
            if all_aliases(ctx).iter().any(|(i, a)| a == name && *i != id) {
                return Err(format!("a member named '{name}' already exists"));
            }
            exec(
                ctx,
                &format!(
                    "UPDATE {NODE_ALIASES_TABLE} SET alias = '{}' WHERE node_id = '{}'",
                    quote(name),
                    quote(&id)
                ),
            )
            .map(|_| ())
        }
        NodeRef::Witness(id) => {
            if crate::witnesses::read_all(ctx)
                .iter()
                .any(|w| w.alias == name && w.witness_id != id)
            {
                return Err(format!("a witness named '{name}' already exists"));
            }
            exec(
                ctx,
                &format!(
                    "UPDATE {} SET alias = '{}' WHERE witness_id = '{}'",
                    crate::witnesses::WITNESSES_TABLE,
                    quote(name),
                    quote(&id)
                ),
            )
            .map(|_| ())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::temp_ctx;

    #[test]
    fn bootstrap_seeds_and_is_stable() {
        let ctx = temp_ctx();
        bootstrap(&ctx, "n1:7100").unwrap();
        let cname = cluster_name(&ctx).expect("cluster named");
        let alias = node_alias(&ctx, "n1:7100").expect("node named");
        assert!(cname.contains('-') && alias.contains('-'));
        // Re-bootstrap changes nothing (names are stable across restarts).
        bootstrap(&ctx, "n1:7100").unwrap();
        assert_eq!(cluster_name(&ctx).as_deref(), Some(cname.as_str()));
        assert_eq!(node_alias(&ctx, "n1:7100").as_deref(), Some(alias.as_str()));
        // A second node registers its own alias.
        bootstrap(&ctx, "n2:7100").unwrap();
        assert!(node_alias(&ctx, "n2:7100").is_some());
    }

    #[test]
    fn rename_and_resolution_cover_all_forms() {
        let ctx = temp_ctx();
        bootstrap(&ctx, "n1:7100").unwrap();
        rename_cluster(&ctx, "ember-lynx").unwrap();
        assert_eq!(cluster_name(&ctx).as_deref(), Some("ember-lynx"));
        rename_node(&ctx, "n1:7100", "skai1").unwrap();
        assert_eq!(node_alias(&ctx, "n1:7100").as_deref(), Some("skai1"));
        // Bare alias, dotted, and raw id all resolve to the member.
        for r in ["skai1", "ember-lynx.node.skai1", "n1:7100"] {
            assert!(matches!(
                resolve_node_ref(&ctx, r),
                Ok(NodeRef::Member(id)) if id == "n1:7100"
            ), "{r}");
        }
        // Wrong cluster segment is rejected.
        assert!(resolve_node_ref(&ctx, "other.node.skai1").is_err());
        // Duplicate alias rejected (best-effort uniqueness).
        bootstrap(&ctx, "n2:7100").unwrap();
        assert!(rename_node(&ctx, "n2:7100", "skai1").is_err());
        // Witness resolution + rename through the registry.
        crate::witnesses::ensure_tables(&ctx).unwrap();
        crate::witnesses::upsert_for_test(&ctx, "w1", "r", 1);
        assert!(matches!(
            resolve_node_ref(&ctx, "ember-lynx.witness.w1"),
            Ok(NodeRef::Witness(id)) if id == "w1"
        ));
        rename_node(&ctx, "w1", "dr-site").unwrap();
        assert!(matches!(
            resolve_node_ref(&ctx, "dr-site"),
            Ok(NodeRef::Witness(id)) if id == "w1"
        ));
    }
}
