//! Witness pull loop (`[witness]`, see `WitnessConfig`).
//!
//! A witness is a standalone node holding a periodically-refreshed full
//! copy of chosen databases from a primary cluster it is NOT a member of:
//! never in the primary's ring, never counted toward its quorums, pulling
//! on its own schedule. Data moves over the INTERNODE protocol — the
//! witness pages `Request::ScanPage` from one primary member (failover
//! across the configured list; a full-copy primary serves whole tables
//! from any member) and applies the pages locally through
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

use skaidb_cluster::internode::{self, Request, Response};
use skaidb_config::WitnessConfig;
use skaidb_driver::Client;
use skaidb_types::Value;

use crate::shared::{Backend, Shared};

/// Rows per pulled page — matches the primary's own gather/repair paging.
const PULL_PAGE_ROWS: u32 = 2_000;

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
    std::thread::spawn(move || loop {
        let cfg = ctx.config_snapshot().witness; // live re-read each cycle
        let started = std::time::Instant::now();
        match run_cycle(&ctx, &cfg) {
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
pub(crate) fn run_cycle(ctx: &Shared, cfg: &WitnessConfig) -> Result<CycleSummary, String> {
    // One SQL connection for the whole cycle (failover inside the driver).
    let mut sql = Client::connect_many(&cfg.primary_sql_addrs, &cfg.user, &cfg.password)
        .map_err(|e| format!("primary SQL connect: {e}"))?;

    register(&mut sql, cfg)?;

    ensure_sync_state_table(ctx)?;
    let now = now_ms() as u64;
    let mut summary = CycleSummary::default();
    for db in &cfg.databases {
        let tables = list_tables(&mut sql, db)?;
        for (table, pk_cols) in &tables {
            ensure_local_table(ctx, db, table, pk_cols)?;
            let qualified = skaidb_engine::namespace::qualify(db, table);
            let state = load_sync_state(ctx, &qualified);
            // Cheap change hint: the serving member's per-table write_seq.
            // Unknown verb (old primary) → None → full-sweep behavior.
            let seq = table_seq_with_failover(cfg, &qualified);
            let backstop_due = now.saturating_sub(state.last_full_ms)
                >= cfg.full_sweep_interval_secs.saturating_mul(1000);
            let (pulled, applied) = match &seq {
                // Same member, same seq, a watermark exists, backstop not
                // due: nothing changed — skip without moving a byte.
                Some((addr, seq_v))
                    if *addr == state.member
                        && *seq_v == state.seq
                        && state.watermark_ms > 0
                        && !backstop_due =>
                {
                    (0, 0)
                }
                // Changed (or first contact with this member) with a
                // watermark: pull only the delta since it.
                Some((addr, seq_v)) if state.watermark_ms > 0 && !backstop_due => {
                    let since = state.watermark_ms.saturating_sub(DELTA_MARGIN_MS);
                    let (pulled, applied, max_hlc) =
                        pull_table_delta(ctx, cfg, addr, &qualified, since)?;
                    save_sync_state(
                        ctx,
                        &qualified,
                        addr,
                        *seq_v,
                        state.watermark_ms.max(max_hlc),
                        state.last_full_ms,
                    )?;
                    (pulled, applied)
                }
                // First sync, an old primary without the verbs, or the
                // backstop is due: full sweep.
                _ => {
                    let (pulled, applied, max_hlc) = pull_table(ctx, cfg, db, table)?;
                    let (member, seq_v) = seq.unwrap_or_default();
                    save_sync_state(
                        ctx,
                        &qualified,
                        &member,
                        seq_v,
                        state.watermark_ms.max(max_hlc),
                        now,
                    )?;
                    (pulled, applied)
                }
            };
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

/// The serving member's per-table write_seq: `(addr, seq)` from the first
/// primary that answers; `None` if none do or the verb is unknown (old
/// primary mid-rolling-upgrade) — callers then take the full-sweep path.
fn table_seq_with_failover(cfg: &WitnessConfig, qualified: &str) -> Option<(String, u64)> {
    for addr in &cfg.primary_internode_addrs {
        match internode::call(
            addr,
            &Request::TableSeq {
                table: qualified.to_string(),
            },
        ) {
            Ok(Response::TableSeq { write_seq }) => return Some((addr.clone(), write_seq)),
            _ => continue,
        }
    }
    None
}

/// Incremental pull: page the primary's stamps-walked delta since
/// `since_physical` and apply through the same guarded path the full sweep
/// uses. Pinned to ONE member (`addr`) — a mid-delta failover would mix
/// incomparable write_seq spaces; on error the cycle fails and the next
/// one retries. Returns `(pulled, applied, max_hlc_physical_seen)`.
fn pull_table_delta(
    ctx: &Shared,
    cfg: &WitnessConfig,
    addr: &str,
    qualified: &str,
    since_physical: u64,
) -> Result<(usize, usize, u64), String> {
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let (mut pulled, mut applied, mut max_hlc) = (0usize, 0usize, 0u64);
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page_started = std::time::Instant::now();
        let (rows, cursor, done) = match internode::call(
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
        for (_, _, hlc, _) in &rows {
            max_hlc = max_hlc.max(hlc.physical);
        }
        if !rows.is_empty() {
            applied += apply_rows_guarded(local, qualified, rows)?;
        }
        if done {
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
            "INSERT INTO witnesses (witness_id, region, registered_at, last_seen_at) \
             VALUES ('{}', '{}', {now_ms}, {now_ms})",
            quote(&cfg.witness_id),
            quote(&cfg.region),
        )
    };
    sql.execute(&stmt)
        .map_err(|e| format!("witness registration: {e}"))?;
    Ok(())
}

/// `(table, pk columns)` for every table in `db` on the primary.
fn list_tables(sql: &mut Client, db: &str) -> Result<Vec<(String, Vec<String>)>, String> {
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
    Ok(rows
        .iter()
        .filter_map(|r| match (r.get(t_i), r.get(pk_i)) {
            (Some(Value::String(t)), Some(Value::String(pk))) => Some((
                t.clone(),
                pk.split(',').map(|c| c.trim().to_string()).collect(),
            )),
            _ => None,
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
    db: &str,
    table: &str,
) -> Result<(usize, usize, u64), String> {
    let qualified = skaidb_engine::namespace::qualify(db, table);
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let (mut pulled, mut applied, mut max_hlc) = (0usize, 0usize, 0u64);
    let mut after: Option<Vec<u8>> = None;
    let mut pages_since_flush = 0usize;
    loop {
        let page_started = std::time::Instant::now();
        let page = scan_page_with_failover(cfg, &qualified, after.as_deref())?;
        let done = page.len() < PULL_PAGE_ROWS as usize;
        after = page.last().map(|(k, ..)| k.clone());
        pulled += page.len();
        for (_, _, hlc, _) in &page {
            max_hlc = max_hlc.max(hlc.physical);
        }
        if !page.is_empty() {
            applied += apply_rows_guarded(local, &qualified, page)?;
        }
        // Mid-table flush every ~64k rows: a standalone witness has none
        // of the Node-level memory-pressure machinery replicas rely on
        // (the shedding/release tier), and per-TABLE flushing leaves a
        // 1.9 GB table's whole ingest accumulating between flushes — the
        // fourth OOM's shape after WAL sync bounded the third's. A flush
        // every 32 pages is deterministic and cheap at this cadence.
        pages_since_flush += 1;
        if pages_since_flush >= 32 {
            pages_since_flush = 0;
            if let Ok(mut dbw) = local.write() {
                dbw.flush_memtables_under_pressure();
            }
        }
        if done {
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

fn scan_page_with_failover(
    cfg: &WitnessConfig,
    qualified: &str,
    after: Option<&[u8]>,
) -> Result<Vec<PulledRow>, String> {
    let mut last_err = String::new();
    for addr in &cfg.primary_internode_addrs {
        match internode::call(
            addr,
            &Request::ScanPage {
                table: qualified.to_string(),
                after: after.map(<[u8]>::to_vec),
                limit: PULL_PAGE_ROWS,
            },
        ) {
            Ok(Response::Scan { rows }) => return Ok(rows),
            Ok(Response::Err(e)) => last_err = format!("{addr}: {e}"),
            Ok(other) => last_err = format!("{addr}: unexpected {other:?}"),
            Err(e) => last_err = format!("{addr}: {e}"),
        }
    }
    Err(format!("ScanPage {qualified}: no primary reachable ({last_err})"))
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
