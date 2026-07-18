//! Witness registry (`witnesses`) and GC grace-period config
//! (`witness_gc_config`).
//!
//! A witness is a cross-region backup node that pulls data on its own
//! schedule and is deliberately NOT a cluster member — it is never counted
//! in `Topology`/`Ring` membership, never a vnode owner, never a target of
//! quorum-blocking replication. It registers itself the same way any
//! ordinary client would: a plain `INSERT`/`UPDATE` against this table,
//! using witness-scoped credentials (an operator-created role — see
//! `.priv/witness-node-plan.md` for the full design and why registration
//! deliberately does NOT need a new RPC verb).
//!
//! Unlike `node_stats`/`drivers`, these tables are **persistent, not
//! `memory = true`**: a witness's watermark must survive a node restart —
//! it may be offline for hours or days, and losing its progress on a
//! routine restart would force an unnecessary full resync every time.
//!
//! `witness_gc_config` is a one-row table holding `grace_period_secs`: how
//! long primary-cluster garbage collection should, in principle, wait past
//! a registered witness's watermark before discarding data it hasn't
//! pulled yet. Deliberately a table row and not a `SET CONFIG` key — config
//! is per-node only (no fan-out to other members, see
//! `admin.rs`'s "Config inspection/control is local to each node"), which
//! would let nodes disagree about a value that needs to be cluster-
//! consistent. A table row replicates through the same quorum-write path
//! any other row does, so `UPDATE witness_gc_config SET grace_period_secs =
//! ...` is both SQL-settable and cluster-consistent by construction.
//! Actually consulting this value during GC is out of scope here (Phase 5
//! in the plan) — this module only makes the value exist and be readable/
//! writable.

use skaidb_proto::Response;
use skaidb_types::Value;

use crate::shared::Shared;

pub const WITNESSES_TABLE: &str = "witnesses";
pub const GC_CONFIG_TABLE: &str = "witness_gc_config";

/// Default grace period: 7 days. Sane starting point for a cross-region
/// backup that's expected to be intermittently reachable, not a hard
/// engineering constraint — change it with `UPDATE witness_gc_config SET
/// grace_period_secs = ...` once it's actually consulted by GC (Phase 5).
const DEFAULT_GRACE_PERIOD_SECS: i64 = 7 * 24 * 60 * 60;

/// The one row `witness_gc_config` ever holds.
const GC_CONFIG_SINGLETON_ID: &str = "default";

fn exec(ctx: &Shared, sql: &str) -> Result<Response, String> {
    let role = ctx.superuser_role.clone();
    let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(ctx, &role, &mut db, sql, None) {
        Response::Error(e) => Err(e),
        resp => Ok(resp),
    }
}

/// Only `upsert_for_test` needs this today — real registration SQL is
/// issued by the witness process itself, not this module.
#[cfg(test)]
fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

/// Idempotent DDL for both tables, plus seeding the default grace-period
/// row if `witness_gc_config` is empty. Safe to call repeatedly (every
/// caller sees the same end state); tolerates the cluster not being ready
/// yet at boot, same as `nodestats::ensure_table`.
pub fn ensure_tables(ctx: &Shared) -> Result<(), String> {
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {WITNESSES_TABLE} (PRIMARY KEY (witness_id))"),
    )?;
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {GC_CONFIG_TABLE} (PRIMARY KEY (id))"),
    )?;
    // Seed the default row only if it's missing — an operator's own
    // `UPDATE` must never be clobbered by a later restart re-running this.
    let resp = exec(
        ctx,
        &format!("SELECT id FROM {GC_CONFIG_TABLE} WHERE id = '{GC_CONFIG_SINGLETON_ID}'"),
    )?;
    let exists = matches!(resp, Response::Rows { rows, .. } if !rows.is_empty());
    if !exists {
        exec(
            ctx,
            &format!(
                "INSERT INTO {GC_CONFIG_TABLE} (id, grace_period_secs) VALUES \
                 ('{GC_CONFIG_SINGLETON_ID}', {DEFAULT_GRACE_PERIOD_SECS})"
            ),
        )?;
    }
    Ok(())
}

/// Current grace period, in seconds. Falls back to the default if the row
/// is somehow missing or the read fails — callers (once Phase 5 wires GC
/// consultation) should never hard-fail just because this table had a
/// transient read error.
pub fn grace_period_secs(ctx: &Shared) -> i64 {
    let resp = exec(
        ctx,
        &format!(
            "SELECT grace_period_secs FROM {GC_CONFIG_TABLE} WHERE id = '{GC_CONFIG_SINGLETON_ID}'"
        ),
    );
    let Ok(Response::Rows { rows, .. }) = resp else {
        return DEFAULT_GRACE_PERIOD_SECS;
    };
    match rows.first().and_then(|r| r.first()) {
        Some(Value::Int(n)) => *n,
        _ => DEFAULT_GRACE_PERIOD_SECS,
    }
}

/// One registered witness, for the UI (`ui.rs`) and any future GC
/// consultation.
#[derive(Debug, Clone, Default)]
pub struct WitnessRow {
    pub witness_id: String,
    pub region: String,
    pub registered_at_ms: i64,
    pub last_seen_at_ms: i64,
    /// Per-table sync state the witness heartbeats in: a document mapping
    /// `db.table` → `{rows, synced_at}` (absent until the first cycle).
    pub watermarks: Option<Value>,
}

/// Every registered witness. Empty on any error — callers should treat that
/// as "no witnesses known," not as a hard failure (mirrors
/// `nodestats::read_all`'s tolerance).
pub fn read_all(ctx: &Shared) -> Vec<WitnessRow> {
    let resp = exec(
        ctx,
        &format!(
            "SELECT witness_id, region, registered_at, last_seen_at, watermarks FROM {WITNESSES_TABLE}"
        ),
    );
    let Ok(Response::Rows { columns, rows }) = resp else {
        return Vec::new();
    };
    let idx = |name: &str| columns.iter().position(|c| c == name);
    let as_i = |v: Option<&Value>| match v {
        Some(Value::Int(i)) => *i,
        _ => 0,
    };
    let as_s = |v: Option<&Value>| match v {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let (Some(id_i), Some(region_i), Some(reg_i), Some(seen_i)) = (
        idx("witness_id"),
        idx("region"),
        idx("registered_at"),
        idx("last_seen_at"),
    ) else {
        return Vec::new();
    };
    let wm_i = idx("watermarks");
    rows.iter()
        .map(|row| WitnessRow {
            witness_id: as_s(row.get(id_i)),
            region: as_s(row.get(region_i)),
            registered_at_ms: as_i(row.get(reg_i)),
            last_seen_at_ms: as_i(row.get(seen_i)),
            watermarks: wm_i
                .and_then(|i| row.get(i))
                .filter(|v| !v.is_null())
                .cloned(),
        })
        .collect()
}

/// The tombstone-retention floor (ms) this node should hold: how far back
/// the LEAST-CAUGHT-UP live witness is. For every witness seen within the
/// grace period, its catch-up point is the oldest per-table `synced_at`
/// from its watermarks (or its `registered_at` before the first sync);
/// the floor is `now − min(catch-up points)`, capped at the grace period
/// (a witness that lapses the cap must full-resync — and, for deletes,
/// be rebuilt — rather than hold the primary's garbage collection
/// hostage forever). No live witnesses → 0, tombstones drop immediately,
/// the historical behavior.
pub fn tombstone_retention_floor_ms(ctx: &Shared) -> u64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let grace_ms = grace_period_secs(ctx).saturating_mul(1000);
    let mut floor: i64 = 0;
    for w in read_all(ctx) {
        if now_ms - w.last_seen_at_ms > grace_ms {
            continue; // lapsed: stops holding GC, full-resync territory
        }
        let catch_up = match &w.watermarks {
            Some(Value::Document(d)) => d
                .0
                .values()
                .filter_map(|e| match e {
                    Value::Document(t) => match t.0.get("synced_at") {
                        Some(Value::Int(n)) => Some(*n),
                        _ => None,
                    },
                    _ => None,
                })
                .min()
                .unwrap_or(w.registered_at_ms),
            _ => w.registered_at_ms,
        };
        floor = floor.max(now_ms - catch_up);
    }
    (floor.max(0) as u64).min(grace_ms.max(0) as u64)
}

/// Test/tooling helper: register or heartbeat a witness exactly the way a
/// real witness process would over an ordinary SQL connection (this module
/// doesn't otherwise write to `witnesses` — that's the witness's job, not
/// the server's). Not used by production code paths.
#[cfg(test)]
pub fn upsert_for_test(ctx: &Shared, witness_id: &str, region: &str, now_ms: i64) {
    let sql = format!(
        "INSERT INTO {WITNESSES_TABLE} (witness_id, region, registered_at, last_seen_at) \
         VALUES ('{}', '{}', {now_ms}, {now_ms})",
        quote(witness_id),
        quote(region),
    );
    exec(ctx, &sql).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::temp_ctx;

    #[test]
    fn ensure_tables_seeds_default_grace_period_once() {
        let ctx = temp_ctx();
        ensure_tables(&ctx).unwrap();
        assert_eq!(grace_period_secs(&ctx), DEFAULT_GRACE_PERIOD_SECS);

        // An operator's UPDATE must survive a re-run of ensure_tables
        // (e.g. a restart) — the seed is "if missing," not "always."
        exec(
            &ctx,
            &format!(
                "UPDATE {GC_CONFIG_TABLE} SET grace_period_secs = 3600 WHERE id = '{GC_CONFIG_SINGLETON_ID}'"
            ),
        )
        .unwrap();
        ensure_tables(&ctx).unwrap();
        assert_eq!(grace_period_secs(&ctx), 3600);
    }

    #[test]
    fn witnesses_register_and_read_back() {
        let ctx = temp_ctx();
        ensure_tables(&ctx).unwrap();
        assert!(read_all(&ctx).is_empty());

        upsert_for_test(&ctx, "witness-1", "us-west", 1_000);
        upsert_for_test(&ctx, "witness-2", "eu-central", 2_000);
        let mut rows = read_all(&ctx);
        rows.sort_by(|a, b| a.witness_id.cmp(&b.witness_id));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].witness_id, "witness-1");
        assert_eq!(rows[0].region, "us-west");
        assert_eq!(rows[1].witness_id, "witness-2");

        // PK overwrite = heartbeat, not a duplicate row.
        upsert_for_test(&ctx, "witness-1", "us-west", 5_000);
        assert_eq!(read_all(&ctx).len(), 2);
        let updated = read_all(&ctx)
            .into_iter()
            .find(|w| w.witness_id == "witness-1")
            .unwrap();
        assert_eq!(updated.last_seen_at_ms, 5_000);
    }

    /// The GC floor holds tombstones back to the least-caught-up LIVE
    /// witness, ignores lapsed ones, and caps at the grace period.
    #[test]
    fn tombstone_retention_floor_follows_the_registry() {
        let ctx = temp_ctx();
        ensure_tables(&ctx).unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap();

        // No witnesses: floor 0 — tombstones drop immediately, as always.
        assert_eq!(tombstone_retention_floor_ms(&ctx), 0);

        // A live witness whose oldest table sync was ~2h ago holds ~2h.
        let two_h = 2 * 3600 * 1000;
        exec(&ctx, &format!(
            "INSERT INTO witnesses (witness_id, region, registered_at, last_seen_at, watermarks)              VALUES ('w1', 'r', {reg}, {seen}, {{'db.a': {{rows: 5, synced_at: {old}}},              'db.b': {{rows: 5, synced_at: {newer}}}}})",
            reg = now_ms - 10 * two_h,
            seen = now_ms - 60_000,
            old = now_ms - two_h,
            newer = now_ms - 60_000,
        )).unwrap();
        let floor = tombstone_retention_floor_ms(&ctx);
        assert!(
            (floor as i64 - two_h).abs() < 30_000,
            "floor ≈ oldest synced_at age, got {floor}"
        );

        // A witness lapsed past the grace period stops holding GC.
        exec(&ctx, "UPDATE witness_gc_config SET grace_period_secs = 3600 WHERE id = 'default'")
            .unwrap();
        exec(&ctx, &format!(
            "UPDATE witnesses SET last_seen_at = {} WHERE witness_id = 'w1'",
            now_ms - 2 * 3600 * 1000,
        )).unwrap();
        assert_eq!(tombstone_retention_floor_ms(&ctx), 0, "lapsed witness ignored");

        // A live witness with NO watermarks yet holds back to registration,
        // capped at the grace period.
        exec(&ctx, &format!(
            "INSERT INTO witnesses (witness_id, region, registered_at, last_seen_at)              VALUES ('w2', 'r', {reg}, {seen})",
            reg = now_ms - 100 * 3600 * 1000,
            seen = now_ms - 30_000,
        )).unwrap();
        let floor = tombstone_retention_floor_ms(&ctx);
        assert_eq!(floor, 3600 * 1000, "capped at the 1h grace period");
    }

    #[test]
    fn ensure_tables_is_idempotent() {
        let ctx = temp_ctx();
        ensure_tables(&ctx).unwrap();
        ensure_tables(&ctx).unwrap();
        ensure_tables(&ctx).unwrap();
    }
}
