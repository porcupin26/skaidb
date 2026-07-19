//! Witness pull loop (`[witness]`, see `WitnessConfig`).
//!
//! A witness is a standalone node holding a periodically-refreshed full
//! copy of chosen databases from a primary cluster it is NOT a member of:
//! never in the primary's ring, never counted toward its quorums, pulling
//! on its own schedule. Data moves over the INTERNODE protocol — the
//! witness pages `Request::ScanPage` per table from wherever a complete
//! copy lives: one member with failover for full-copy tables, the pinned
//! members for pinned tables, and a SCATTER over every member for tables
//! whose per-table RF shards them (no single member holds it all — the
//! per-member pulls LWW-merge locally) — applying pages through
//! `Database::apply_batch_buffered`: byte-exact row values with their HLC
//! stamps and tombstones, so re-pulls converge by last-writer-wins and
//! deletes propagate without any diffing. A staleness guard (mirroring the
//! replica appliers' `filter_newer_rows`) skips rows already held at an
//! equal-or-newer stamp, so failing over to a transiently-lagging member
//! can never regress a row.
//!
//! The two non-bulk jobs ride the ordinary SQL protocol (skaidb-driver,
//! witness-scoped credentials on the primary): schema listing (`USE db` +
//! `SHOW TABLES` → local `CREATE TABLE IF NOT EXISTS`) and the
//! registration/heartbeat/watermark row in the primary's `witnesses`
//! table, which the primary's status UI reads.
//!
//! Local applies go through the engine directly — beneath the session
//! layer, so `server.read_only` (which a witness should run with) never
//! blocks the pull while still rejecting every client mutation.

use skaidb_cluster::internode::{Pool, Request, Response};
use skaidb_config::WitnessConfig;
use skaidb_driver::Client;
use skaidb_types::Value;

use crate::shared::{Backend, Shared};

/// Rows per pulled page — matches the primary's own gather/repair paging.
const PULL_PAGE_ROWS: u32 = 2_000;

/// Flush the ingesting table's memtable once this many applied bytes have
/// accumulated since the last flush. This is the drain cadence for the
/// from-empty bulk pull, and it MUST be measured in bytes, not pages: the
/// standalone witness has NO background flusher (the `take_flush_job` pump
/// lives only in the cluster `Node` thread), so the hot write path's
/// freeze-at-`flush_threshold_bytes` (512 MB on the default auto budget)
/// would stack up to 4 frozen + 1 active memtable ≈ 2.5 GB on a from-empty
/// sweep of a large table — the SIXTH OOM shape. A page-COUNT cadence is
/// byte-blind: 32 pages of 2 KB rows is 128 MB but 32 pages of multi-KB
/// email bodies is ~2 GB, which is exactly what freezes and stacks. Flushing
/// by bytes keeps the active memtable an order of magnitude below the freeze
/// threshold, so it never freezes and RSS stays flat regardless of row size
/// or how many tables a cycle touches. 64 MB trades a handful of small
/// SSTables (compaction merges them) for a bounded, predictable footprint.
const WITNESS_FLUSH_BYTES: usize = 64 * 1024 * 1024;

/// Minimum pause between pulled pages. The REAL pacing rule is adaptive —
/// sleep at least as long as the previous page took to fetch and apply —
/// which caps the pull at ≤ 50% of the serving primary's capacity (and of
/// the witness's own apply path) BY CONSTRUCTION: a fixed pause can't
/// promise that (a slow 100 ms page + 25 ms pause is an 80% duty cycle).
/// The unpaced first deployment both OOM-killed the witness (apply
/// outran background flush) and hammered the primary at full duty. This
/// floor keeps even sub-millisecond pages from spinning.
const PULL_PAGE_PAUSE_FLOOR: std::time::Duration = std::time::Duration::from_millis(25);

/// Safety margin subtracted from a table's watermark on a delta pull:
/// re-observing the last minute costs a few idempotently-skipped rows and
/// absorbs modest HLC skew between primary members.
const DELTA_MARGIN_MS: u64 = 60_000;

/// Local (witness-side) persistent per-table sync state: which primary
/// member's `write_seq` we last saw (comparable only against that member),
/// the HLC-physical watermark deltas resume from, and the last FULL sweep
/// time (the anti-entropy backstop clock).
const SYNC_STATE_TABLE: &str = "witness_sync_state";

/// What one cycle did, for the log line and the heartbeat watermarks.
#[derive(Debug, Default)]
pub struct CycleSummary {
    /// `(db.table, rows_pulled, rows_applied, rows_now)` — applied < pulled
    /// means the staleness guard skipped rows already held; `rows_now` is
    /// the LOCAL live-row count (key stats, no scan) so the heartbeat
    /// reports table sizes, not per-cycle traffic (a skipped table pulls 0
    /// but still holds everything).
    pub tables: Vec<(String, usize, usize, i64)>,
}

/// Start the pull loop when witness mode is enabled. Called once at serve().
pub fn spawn_if_enabled(ctx: Shared) {
    let cfg = ctx.config_snapshot().witness;
    if !cfg.enabled {
        return;
    }
    if !matches!(ctx.backend, Backend::Local(_)) {
        skaidb_types::slog!(
            "witness: [witness] enabled on a CLUSTER member — refusing (a witness \
             must be standalone; it mirrors a primary, it doesn't join one)"
        );
        return;
    }
    if cfg.databases.is_empty() || cfg.primary_sql_addrs.is_empty() || cfg.primary_internode_addrs.is_empty()
    {
        skaidb_types::slog!(
            "witness: [witness] enabled but databases / primary_sql_addrs / \
             primary_internode_addrs incomplete — refusing to start"
        );
        return;
    }
    if !ctx.read_only() {
        skaidb_types::slog!(
            "witness: server.read_only is FALSE — the copy can be diverged by \
             local writes; set server.read_only = true (pull continues anyway)"
        );
    }
    // The pull's internode connections go through the SAME authenticator the
    // primary requires (none / token / cert), built from this witness's
    // `[auth]` config — so a cert-mode primary accepts the witness's mTLS pull
    // instead of dropping an unauthenticated plaintext connection. A bad/absent
    // auth config fails loud here (the pull never starts) rather than silently
    // going plaintext against a secured primary.
    let pool = match crate::build_internode_auth(&ctx.config_snapshot()) {
        Ok(auth) => Pool::new(auth),
        Err(e) => {
            skaidb_types::slog!("witness: internode auth config invalid: {e} — pull not started");
            return;
        }
    };
    std::thread::spawn(move || loop {
        let cfg = ctx.config_snapshot().witness; // live re-read each cycle
        let started = std::time::Instant::now();
        match run_cycle(&ctx, &cfg, &pool) {
            Ok(summary) => {
                let (pulled, applied): (usize, usize) = summary
                    .tables
                    .iter()
                    .fold((0, 0), |(p, a), (_, tp, ta, _)| (p + tp, a + ta));
                skaidb_types::slog!(
                    "witness: cycle ok — {} tables, {pulled} rows pulled, {applied} applied \
                     ({} skipped as already-current) in {:.1}s",
                    summary.tables.len(),
                    pulled - applied,
                    started.elapsed().as_secs_f64()
                );
            }
            Err(e) => skaidb_types::slog!("witness: cycle failed: {e}"),
        }
        std::thread::sleep(std::time::Duration::from_secs(cfg.interval_secs.max(60)));
    });
}

/// One full pull cycle. Public-in-crate so tests drive it synchronously.
pub(crate) fn run_cycle(ctx: &Shared, cfg: &WitnessConfig, pool: &Pool) -> Result<CycleSummary, String> {
    // One SQL connection for the whole cycle (failover inside the driver).
    // The SQL control-plane path is a plain client connection, so it must
    // present client TLS when the primary requires it (`client_tls =
    // "required"`) — otherwise the primary resets it and the whole cycle
    // fails before any data moves. The bulk pull rides the [auth]-secured
    // internode port and is unaffected. CA falls back to the internode CA:
    // one cluster CA usually secures both ports.
    let sql_tls = if cfg.primary_tls {
        let ca = if cfg.primary_tls_ca.is_empty() {
            ctx.config_snapshot().auth.internode_tls_ca.clone()
        } else {
            cfg.primary_tls_ca.clone()
        };
        let verify = if ca.is_empty() {
            skaidb_driver::TlsVerify::Insecure
        } else {
            skaidb_driver::TlsVerify::CaFile(ca)
        };
        Some(
            skaidb_driver::TlsConfig::new(verify, &cfg.primary_tls_server_name)
                .map_err(|e| format!("witness SQL TLS config: {e}"))?,
        )
    } else {
        None
    };
    let mut sql =
        Client::connect_many_tls(&cfg.primary_sql_addrs, &cfg.user, &cfg.password, sql_tls)
            .map_err(|e| format!("primary SQL connect: {e}"))?;

    register(&mut sql, cfg)?;
    mirror_names(ctx, &mut sql, cfg);

    ensure_sync_state_table(ctx)?;
    let now = now_ms() as u64;
    let mut summary = CycleSummary::default();
    for db in &cfg.databases {
        let tables = list_tables(&mut sql, db)?;
        for pt in &tables {
            let table = &pt.table;
            ensure_local_table(ctx, db, table, &pt.pk_cols)?;
            let qualified = skaidb_engine::namespace::qualify(db, table);
            // Where a complete copy of this table can come from. A pinned
            // table lives whole on each pin (any one live pin serves the
            // pull); a table whose RF override is below the primary's
            // member count is SHARDED — no single member has it all, so
            // the pull scatters over every member and LWW-merges (the
            // staleness guard makes application idempotent). Everything
            // else keeps the one-member-with-failover path.
            let members = &cfg.primary_internode_addrs;
            // An open placement transition means NO single source is
            // guaranteed complete (a new pin may still be backfilling):
            // scatter over every candidate — configured members plus the
            // current pins — and let the merge sort it out.
            let scatter = pt.transition
                || (pt.pins.is_empty()
                    && pt
                        .replication
                        .is_some_and(|n| (n as usize) < members.len()));
            let sources: Vec<String> = if pt.transition {
                let mut all = members.clone();
                for p in &pt.pins {
                    if !all.contains(p) {
                        all.push(p.clone());
                    }
                }
                all
            } else if pt.pins.is_empty() {
                members.clone()
            } else {
                pt.pins.clone()
            };
            let (mut pulled, mut applied) = (0usize, 0usize);
            if scatter {
                // Per-member sync state (seq spaces are per-member); a
                // down member degrades that member's shard until it
                // returns instead of failing the whole cycle — but say so.
                for addr in &sources {
                    match sync_from_member(ctx, cfg, pool, db, table, &qualified, addr, true, now) {
                        Ok((p, a)) => {
                            pulled += p;
                            applied += a;
                        }
                        Err(e) => skaidb_types::slog!(
                            "witness: {qualified} shard @{addr} unavailable this cycle                              (sharded table, rf={:?}): {e}",
                            pt.replication
                        ),
                    }
                }
            } else {
                // One complete source suffices; try them in order.
                let mut last_err = String::new();
                let mut synced = false;
                for addr in &sources {
                    match sync_from_member(ctx, cfg, pool, db, table, &qualified, addr, false, now) {
                        Ok((p, a)) => {
                            pulled = p;
                            applied = a;
                            synced = true;
                            break;
                        }
                        Err(e) => last_err = e,
                    }
                }
                if !synced {
                    return Err(format!("{qualified}: no source reachable: {last_err}"));
                }
            }
            let rows_now = rows_now(ctx, &qualified);
            summary
                .tables
                .push((format!("{db}.{table}"), pulled, applied, rows_now));
            // A finished table's memtable would otherwise sit resident
            // until organic pressure — across a 30-table sync those piles
            // are exactly what OOM'd the first deployment. Flush eagerly:
            // the witness is idle-by-design between cycles, so trading a
            // few small SSTables for a flat memory profile is free.
            if pulled > 0 {
                if let Backend::Local(local) = &ctx.backend {
                    if let Ok(mut db) = local.write() {
                        db.flush_memtables_under_pressure();
                    }
                }
            }
        }
    }

    heartbeat(&mut sql, cfg, &summary)?;
    Ok(summary)
}

/// Sync `qualified` from ONE primary member: TableSeq change-gate, delta
/// pull when a watermark exists, full sweep on first sync / unknown verb /
/// due backstop — the same ladder the single-source cycle always ran, with
/// the sync-state row keyed per member (`per_member`) when the table is
/// sharded across the primary (seq spaces are only comparable within one
/// member). Returns `(pulled, applied)`.
#[allow(clippy::too_many_arguments)]
fn sync_from_member(
    ctx: &Shared,
    cfg: &WitnessConfig,
    pool: &Pool,
    db: &str,
    table: &str,
    qualified: &str,
    addr: &str,
    per_member: bool,
    now: u64,
) -> Result<(usize, usize), String> {
    let state_key = if per_member {
        format!("{qualified}|{addr}")
    } else {
        qualified.to_string()
    };
    let state = load_sync_state(ctx, &state_key);
    let seq = table_seq_at(pool, addr, qualified);
    let backstop_due = now.saturating_sub(state.last_full_ms)
        >= cfg.full_sweep_interval_secs.saturating_mul(1000);
    match seq {
        Some(seq_v)
            if addr == state.member
                && seq_v == state.seq
                && state.watermark_ms > 0
                && !backstop_due =>
        {
            Ok((0, 0))
        }
        Some(seq_v) if state.watermark_ms > 0 && !backstop_due => {
            let since = state.watermark_ms.saturating_sub(DELTA_MARGIN_MS);
            let (pulled, applied, max_hlc) = pull_table_delta(ctx, cfg, pool, addr, qualified, since)?;
            save_sync_state(
                ctx,
                &state_key,
                addr,
                seq_v,
                state.watermark_ms.max(max_hlc),
                state.last_full_ms,
            )?;
            Ok((pulled, applied))
        }
        _ => {
            let (pulled, applied, max_hlc) = pull_table(ctx, cfg, pool, db, table, addr)?;
            save_sync_state(
                ctx,
                &state_key,
                addr,
                seq.unwrap_or_default(),
                state.watermark_ms.max(max_hlc),
                now,
            )?;
            Ok((pulled, applied))
        }
    }
}

/// The witness's own persistent per-table sync bookkeeping (local only,
/// written as the superuser beneath the read-only gate).
fn ensure_sync_state_table(ctx: &Shared) -> Result<(), String> {
    let role = ctx.superuser_role.clone();
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(
        ctx,
        &role,
        &mut current_db,
        &format!("CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE} (PRIMARY KEY (tbl))"),
        None,
    ) {
        skaidb_proto::Response::Error(e) => Err(format!("sync state ddl: {e}")),
        _ => Ok(()),
    }
}

#[derive(Debug, Default)]
struct SyncState {
    member: String,
    seq: u64,
    watermark_ms: u64,
    last_full_ms: u64,
}

fn load_sync_state(ctx: &Shared, qualified: &str) -> SyncState {
    let role = ctx.superuser_role.clone();
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    let resp = crate::shared::execute_session_as(
        ctx,
        &role,
        &mut current_db,
        &format!(
            "SELECT member, seq, watermark, last_full FROM {SYNC_STATE_TABLE} \
             WHERE tbl = '{}'",
            quote(qualified)
        ),
        None,
    );
    let skaidb_proto::Response::Rows { rows, .. } = resp else {
        return SyncState::default();
    };
    let Some(row) = rows.first() else {
        return SyncState::default();
    };
    let as_u = |v: Option<&Value>| match v {
        Some(Value::Int(n)) => *n as u64,
        _ => 0,
    };
    SyncState {
        member: match row.first() {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        },
        seq: as_u(row.get(1)),
        watermark_ms: as_u(row.get(2)),
        last_full_ms: as_u(row.get(3)),
    }
}

fn save_sync_state(
    ctx: &Shared,
    qualified: &str,
    member: &str,
    seq: u64,
    watermark_ms: u64,
    last_full_ms: u64,
) -> Result<(), String> {
    let role = ctx.superuser_role.clone();
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(
        ctx,
        &role,
        &mut current_db,
        &format!(
            "INSERT INTO {SYNC_STATE_TABLE} (tbl, member, seq, watermark, last_full) \
             VALUES ('{}', '{}', {seq}, {watermark_ms}, {last_full_ms})",
            quote(qualified),
            quote(member),
        ),
        None,
    ) {
        skaidb_proto::Response::Error(e) => Err(format!("sync state save: {e}")),
        _ => Ok(()),
    }
}

/// Local live-row count from key stats (no scan) for the heartbeat.
fn rows_now(ctx: &Shared, qualified: &str) -> i64 {
    let Backend::Local(local) = &ctx.backend else { return 0 };
    local
        .read()
        .ok()
        .and_then(|db| db.local_count_rows(qualified).ok().flatten())
        .map(|n| n as i64)
        .unwrap_or(0)
}

/// One member's per-table write_seq; `None` when unreachable or the verb
/// is unknown (old primary mid-rolling-upgrade) — callers then take the
/// full-sweep path.
fn table_seq_at(pool: &Pool, addr: &str, qualified: &str) -> Option<u64> {
    match pool.call(
        addr,
        &Request::TableSeq {
            table: qualified.to_string(),
        },
    ) {
        Ok(Response::TableSeq { write_seq }) => Some(write_seq),
        _ => None,
    }
}

/// Incremental pull: page the primary's stamps-walked delta since
/// `since_physical` and apply through the same guarded path the full sweep
/// uses. Pinned to ONE member (`addr`) — a mid-delta failover would mix
/// incomparable write_seq spaces; on error the cycle fails and the next
/// one retries. Returns `(pulled, applied, max_hlc_physical_seen)`.
fn pull_table_delta(
    ctx: &Shared,
    cfg: &WitnessConfig,
    pool: &Pool,
    addr: &str,
    qualified: &str,
    since_physical: u64,
) -> Result<(usize, usize, u64), String> {
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let (mut pulled, mut applied, mut max_hlc) = (0usize, 0usize, 0u64);
    let mut after: Option<Vec<u8>> = None;
    let mut bytes_since_flush = 0usize;
    loop {
        let page_started = std::time::Instant::now();
        let (rows, cursor, done) = match pool.call(
            addr,
            &Request::ScanSincePage {
                table: qualified.to_string(),
                since_physical,
                after: after.clone(),
                limit: PULL_PAGE_ROWS,
            },
        ) {
            Ok(Response::DeltaPage { rows, cursor, done }) => (rows, cursor, done),
            Ok(Response::Err(e)) => return Err(format!("delta {qualified} @{addr}: {e}")),
            Ok(other) => return Err(format!("delta {qualified} @{addr}: unexpected {other:?}")),
            Err(e) => return Err(format!("delta {qualified} @{addr}: {e}")),
        };
        after = cursor;
        pulled += rows.len();
        let mut page_bytes = 0usize;
        for (k, v, hlc, _) in &rows {
            max_hlc = max_hlc.max(hlc.physical);
            page_bytes += k.len() + v.len();
        }
        if !rows.is_empty() {
            applied += apply_rows_guarded(local, qualified, rows)?;
        }
        // Same byte-paced drain as the full sweep: a large catch-up delta
        // (witness offline for a while) is a bulk load too, and without this
        // it would freeze-stack exactly like the from-empty sweep.
        bytes_since_flush += page_bytes;
        if bytes_since_flush >= WITNESS_FLUSH_BYTES {
            bytes_since_flush = 0;
            if let Ok(mut dbw) = local.write() {
                dbw.flush_memtables_under_pressure();
            }
        }
        if done {
            if bytes_since_flush > 0 {
                if let Ok(mut dbw) = local.write() {
                    dbw.flush_memtables_under_pressure();
                }
            }
            return Ok((pulled, applied, max_hlc));
        }
        let pct = f64::from(cfg.duty_pct.clamp(1, 90));
        let rest = page_started.elapsed().mul_f64((100.0 - pct) / pct);
        std::thread::sleep(rest.max(PULL_PAGE_PAUSE_FLOOR));
    }
}

/// Ensure the witness's registration row exists on the primary. An
/// existing row is UPDATEd in place (last_seen + region only) — an
/// INSERT would overwrite the whole row and wipe the previous cycle's
/// `watermarks` until this cycle's closing heartbeat rewrites them,
/// which made the primary's GC floor fall back to registration age
/// mid-cycle (safe — more conservative, grace-capped — but needlessly
/// coarse, and it blanked the status tab's sync detail while a cycle
/// ran). INSERT only on first registration.
pub(crate) fn register(sql: &mut Client, cfg: &WitnessConfig) -> Result<(), String> {
    let existing = sql
        .execute(&format!(
            "SELECT registered_at FROM witnesses WHERE witness_id = '{}'",
            quote(&cfg.witness_id)
        ))
        .map_err(|e| format!("witness registry read: {e}"))?;
    let now_ms = now_ms();
    let exists =
        matches!(&existing, skaidb_proto::Response::Rows { rows, .. } if !rows.is_empty());
    let stmt = if exists {
        format!(
            "UPDATE witnesses SET last_seen_at = {now_ms}, region = '{}' \
             WHERE witness_id = '{}'",
            quote(&cfg.region),
            quote(&cfg.witness_id),
        )
    } else {
        format!(
            "INSERT INTO witnesses (witness_id, alias, region, registered_at, last_seen_at) \
             VALUES ('{0}', '{0}', '{1}', {now_ms}, {now_ms})",
            quote(&cfg.witness_id),
            quote(&cfg.region),
        )
    };
    sql.execute(&stmt)
        .map_err(|e| format!("witness registration: {e}"))?;
    Ok(())
}

/// Mirror the PRIMARY's cluster name and this witness's current alias
/// into the LOCAL naming tables, so the witness's own UI/status show the
/// identity the primary assigned — one-way by design (witness nodes
/// refuse ALTER ... SET NAME; renames happen on a member and arrive here
/// on the next cycle). Best-effort: naming is display, not correctness.
fn mirror_names(ctx: &Shared, sql: &mut Client, cfg: &WitnessConfig) {
    let get_one = |sql: &mut Client, q: &str| -> Option<String> {
        match sql.execute(q) {
            Ok(skaidb_proto::Response::Rows { rows, .. }) => {
                rows.first().and_then(|r| match r.first() {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
            }
            _ => None,
        }
    };
    let cname = get_one(sql, "SELECT name FROM cluster_meta WHERE id = 'cluster'");
    let alias = get_one(
        sql,
        &format!(
            "SELECT alias FROM witnesses WHERE witness_id = '{}'",
            quote(&cfg.witness_id)
        ),
    );
    let role = ctx.superuser_role.clone();
    let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
    let mut run = |stmt: String| {
        let _ = crate::shared::execute_session_as(ctx, &role, &mut db, &stmt, None);
    };
    run("CREATE TABLE IF NOT EXISTS cluster_meta (PRIMARY KEY (id))".into());
    run("CREATE TABLE IF NOT EXISTS node_aliases (PRIMARY KEY (node_id))".into());
    if let Some(c) = cname {
        run(format!(
            "INSERT INTO cluster_meta (id, name) VALUES ('cluster', '{}')",
            quote(&c)
        ));
    }
    if let Some(a) = alias {
        run(format!(
            "INSERT INTO node_aliases (node_id, alias, function) VALUES ('local', '{}', 'witness')",
            quote(&a)
        ));
    }
}

/// One mirrored table's schema + placement as the primary lists it.
struct PrimaryTable {
    table: String,
    pk_cols: Vec<String>,
    /// Per-table RF override on the primary (None = cluster default).
    replication: Option<u32>,
    /// Pinned members (internode ids — which ARE their addresses). A pin
    /// holds the whole table, so any one live pin can serve the pull.
    pins: Vec<String>,
    /// A placement transition is open on the primary: NO single source is
    /// guaranteed complete (a new pin may still be backfilling), so the
    /// pull must scatter over every candidate and merge.
    transition: bool,
}

/// Every table in `db` on the primary, with placement. Old primaries
/// without the placement columns list as cluster-default (the previous
/// behavior exactly).
fn list_tables(sql: &mut Client, db: &str) -> Result<Vec<PrimaryTable>, String> {
    sql.execute(&format!("USE {db}"))
        .map_err(|e| format!("USE {db}: {e}"))?;
    let resp = sql
        .execute("SHOW TABLES")
        .map_err(|e| format!("SHOW TABLES in {db}: {e}"))?;
    let skaidb_proto::Response::Rows { columns, rows } = resp else {
        return Err(format!("SHOW TABLES in {db}: unexpected response"));
    };
    let idx = |name: &str| columns.iter().position(|c| c == name);
    let (Some(t_i), Some(pk_i)) = (idx("table"), idx("primary_key")) else {
        return Err(format!("SHOW TABLES in {db}: missing columns"));
    };
    // `witness = false` tables are excluded from mirrors by the primary's
    // schema (absent column = old primary = mirror everything).
    let w_i = idx("witness");
    let (r_i, n_i) = (idx("replication"), idx("nodes"));
    Ok(rows
        .iter()
        .filter_map(|r| {
            if let Some(i) = w_i {
                if matches!(r.get(i), Some(Value::Bool(false))) {
                    return None;
                }
            }
            match (r.get(t_i), r.get(pk_i)) {
                (Some(Value::String(t)), Some(Value::String(pk))) => Some(PrimaryTable {
                    table: t.clone(),
                    pk_cols: pk.split(',').map(|c| c.trim().to_string()).collect(),
                    replication: match r_i.and_then(|i| r.get(i)) {
                        Some(Value::Int(n)) => Some(*n as u32),
                        _ => None,
                    },
                    pins: match n_i.and_then(|i| r.get(i)) {
                        Some(Value::String(list)) => list
                            .split(',')
                            .map(|p| p.trim().to_string())
                            .filter(|p| !p.is_empty())
                            .collect(),
                        _ => Vec::new(),
                    },
                    transition: matches!(
                        idx("transition").and_then(|i| r.get(i)),
                        Some(Value::Bool(true))
                    ),
                }),
                _ => None,
            }
        })
        .collect())
}

/// Create the database + table locally if missing (schema-less: the PK is
/// the whole schema). Runs as the superuser through the session layer —
/// exempt from read_only, and DDL this way keeps the catalog consistent.
fn ensure_local_table(
    ctx: &Shared,
    db: &str,
    table: &str,
    pk_cols: &[String],
) -> Result<(), String> {
    let role = ctx.superuser_role.clone();
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    for sql in [
        format!("CREATE DATABASE IF NOT EXISTS {db}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {db}.{table} (PRIMARY KEY ({}))",
            pk_cols.join(", ")
        ),
    ] {
        if let skaidb_proto::Response::Error(e) =
            crate::shared::execute_session_as(ctx, &role, &mut current_db, &sql, None)
        {
            return Err(format!("{sql}: {e}"));
        }
    }
    Ok(())
}

/// Full-sweep pull of one table. Returns
/// `(rows_pulled, rows_applied, max_hlc_physical_seen)`.
fn pull_table(
    ctx: &Shared,
    cfg: &WitnessConfig,
    pool: &Pool,
    db: &str,
    table: &str,
    addr: &str,
) -> Result<(usize, usize, u64), String> {
    let qualified = skaidb_engine::namespace::qualify(db, table);
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let (mut pulled, mut applied, mut max_hlc) = (0usize, 0usize, 0u64);
    let mut after: Option<Vec<u8>> = None;
    let mut bytes_since_flush = 0usize;
    loop {
        let page_started = std::time::Instant::now();
        let page = scan_page_at(pool, addr, &qualified, after.as_deref())?;
        let done = page.len() < PULL_PAGE_ROWS as usize;
        after = page.last().map(|(k, ..)| k.clone());
        pulled += page.len();
        // Sum applied bytes as we go: this, not a page count, drives the
        // flush cadence, because the memtable's growth is bytes, not rows
        // (see `WITNESS_FLUSH_BYTES`). Key + value only — the Hlc/tombstone
        // flag are fixed-size and negligible.
        let mut page_bytes = 0usize;
        for (k, v, hlc, _) in &page {
            max_hlc = max_hlc.max(hlc.physical);
            page_bytes += k.len() + v.len();
        }
        if !page.is_empty() {
            applied += apply_rows_guarded(local, &qualified, page)?;
        }
        // Byte-paced flush: keep the active memtable well below the hot
        // path's freeze threshold so it never freezes on the witness (which
        // has no background flusher to drain a frozen pile). See
        // `WITNESS_FLUSH_BYTES` for the OOM this bounds.
        bytes_since_flush += page_bytes;
        if bytes_since_flush >= WITNESS_FLUSH_BYTES {
            bytes_since_flush = 0;
            if let Ok(mut dbw) = local.write() {
                dbw.flush_memtables_under_pressure();
            }
        }
        if done {
            // Drain this table's tail before moving on, so a finished table
            // leaves nothing resident for the next table's ingest to stack
            // on top of.
            if bytes_since_flush > 0 {
                if let Ok(mut dbw) = local.write() {
                    dbw.flush_memtables_under_pressure();
                }
            }
            return Ok((pulled, applied, max_hlc));
        }
        // Bounded duty on the primary (`witness.duty_pct`, default 50,
        // live via SET CONFIG): rest `work × (100 − pct) / pct`.
        let pct = f64::from(cfg.duty_pct.clamp(1, 90));
        let rest = page_started.elapsed().mul_f64((100.0 - pct) / pct);
        std::thread::sleep(rest.max(PULL_PAGE_PAUSE_FLOOR));
    }
}

/// Apply one page of pulled rows under the staleness guard, then sync
/// the page's WAL commits. Shared by the full sweep and the delta pull.
///
/// The guard compares against the VALUE-FREE stamps range, not per-row
/// point reads: `local_get_versioned` pulls each full row through the
/// entry-capped read cache, and 2000 multi-KB rows per page ramped RSS to
/// the cgroup ceiling on large-row tables (the fifth OOM shape of the
/// first deployment). One sorted stamps walk over the page's own key span
/// reads zero values and is ~2000× fewer storage reads. The WAL sync
/// mirrors the replica applier — dropping the handles unsynced accumulated
/// buffered WAL data across a whole 1.9 GB table (the third OOM shape).
fn apply_rows_guarded(
    local: &std::sync::RwLock<skaidb_engine::Database>,
    qualified: &str,
    page: Vec<PulledRow>,
) -> Result<usize, String> {
    let mut dbw = local.write().map_err(|_| "local lock poisoned".to_string())?;
    let span_first = page.first().map(|(k, ..)| k.clone()).unwrap_or_default();
    let span_last = page.last().map(|(k, ..)| k.clone()).unwrap_or_default();
    let mut local_stamps: std::collections::HashMap<Vec<u8>, skaidb_engine::Hlc> =
        std::collections::HashMap::new();
    // Start strictly BEFORE the span's first key so it is included
    // (`after` is exclusive); an empty `after` covers it.
    let mut cursor: Option<Vec<u8>> = span_first
        .len()
        .checked_sub(1)
        .map(|n| span_first[..n].to_vec());
    'stamps: loop {
        let stamps = dbw
            .local_scan_stamps_page(qualified, cursor.as_deref(), 4096)
            .map_err(|e| format!("stamps {qualified}: {e}"))?;
        let done = stamps.len() < 4096;
        cursor = stamps.last().map(|(k, ..)| k.clone());
        for (key, hlc, _is_put) in stamps {
            if key > span_last {
                break 'stamps;
            }
            if key >= span_first {
                local_stamps.insert(key, hlc);
            }
        }
        if done {
            break;
        }
    }
    let fresh: Vec<_> = page
        .into_iter()
        .filter(|(key, _, hlc, _)| match local_stamps.get(key) {
            Some(cur) => cur < hlc,
            None => true,
        })
        .collect();
    let applied = fresh.len();
    if !fresh.is_empty() {
        if let Some((commit, sync)) = dbw
            .apply_batch_buffered(qualified, &fresh)
            .map_err(|e| format!("apply {qualified}: {e}"))?
        {
            sync.sync_through(commit)
                .map_err(|e| format!("wal sync {qualified}: {e}"))?;
        }
    }
    Ok(applied)
}

/// One `ScanPage` against the first reachable primary internode endpoint.
type PulledRow = (Vec<u8>, Vec<u8>, skaidb_engine::Hlc, bool);

fn scan_page_at(pool: &Pool, addr: &str, qualified: &str, after: Option<&[u8]>) -> Result<Vec<PulledRow>, String> {
    match pool.call(
        addr,
        &Request::ScanPage {
            table: qualified.to_string(),
            after: after.map(<[u8]>::to_vec),
            limit: PULL_PAGE_ROWS,
        },
    ) {
        Ok(Response::Scan { rows }) => Ok(rows),
        Ok(Response::Err(e)) => Err(format!("ScanPage {qualified} @{addr}: {e}")),
        Ok(other) => Err(format!("ScanPage {qualified} @{addr}: unexpected {other:?}")),
        Err(e) => Err(format!("ScanPage {qualified} @{addr}: {e}")),
    }
}

/// Update the witness's heartbeat + per-table watermarks on the primary.
/// The registry lives in the default database and the session last `USE`d a
/// mirrored one (list_tables) — switch back first, so the reference stays
/// unqualified and the witness role's grant applies exactly as written.
fn heartbeat(sql: &mut Client, cfg: &WitnessConfig, summary: &CycleSummary) -> Result<(), String> {
    sql.execute(&format!("USE {}", skaidb_engine::DEFAULT_DATABASE))
        .map_err(|e| format!("USE default: {e}"))?;
    let now_ms = now_ms();
    let watermarks: Vec<String> = summary
        .tables
        .iter()
        .map(|(name, _pulled, _applied, rows_now)| {
            format!("'{}': {{rows: {rows_now}, synced_at: {now_ms}}}", quote(name))
        })
        .collect();
    sql.execute(&format!(
        "UPDATE witnesses SET last_seen_at = {now_ms}, watermarks = {{{}}} \
         WHERE witness_id = '{}'",
        watermarks.join(", "),
        quote(&cfg.witness_id),
    ))
    .map_err(|e| format!("witness heartbeat: {e}"))?;
    Ok(())
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
