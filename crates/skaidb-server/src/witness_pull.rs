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

/// What one cycle did, for the log line and the heartbeat watermarks.
#[derive(Debug, Default)]
pub struct CycleSummary {
    /// `(db.table, rows_pulled, rows_applied)` — applied < pulled means the
    /// staleness guard skipped rows the witness already held.
    pub tables: Vec<(String, usize, usize)>,
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
                    .fold((0, 0), |(p, a), (_, tp, ta)| (p + tp, a + ta));
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

    let mut summary = CycleSummary::default();
    for db in &cfg.databases {
        let tables = list_tables(&mut sql, db)?;
        for (table, pk_cols) in &tables {
            ensure_local_table(ctx, db, table, pk_cols)?;
            let (pulled, applied) = pull_table(ctx, cfg, db, table)?;
            summary.tables.push((format!("{db}.{table}"), pulled, applied));
            // A finished table's memtable would otherwise sit resident
            // until organic pressure — across a 30-table sync those piles
            // are exactly what OOM'd the first deployment. Flush eagerly:
            // the witness is idle-by-design between cycles, so trading a
            // few small SSTables for a flat memory profile is free.
            if let Backend::Local(local) = &ctx.backend {
                if let Ok(mut db) = local.write() {
                    db.flush_memtables_under_pressure();
                }
            }
        }
    }

    heartbeat(&mut sql, cfg, &summary)?;
    Ok(summary)
}

/// Ensure the witness's registration row exists on the primary, preserving
/// `registered_at` across restarts (INSERT overwrites on PK, so read first).
fn register(sql: &mut Client, cfg: &WitnessConfig) -> Result<(), String> {
    let existing = sql
        .execute(&format!(
            "SELECT registered_at FROM witnesses WHERE witness_id = '{}'",
            quote(&cfg.witness_id)
        ))
        .map_err(|e| format!("witness registry read: {e}"))?;
    let now_ms = now_ms();
    let registered_at = match existing {
        skaidb_proto::Response::Rows { rows, .. } => rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                Value::Int(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(now_ms),
        _ => now_ms,
    };
    sql.execute(&format!(
        "INSERT INTO witnesses (witness_id, region, registered_at, last_seen_at) \
         VALUES ('{}', '{}', {registered_at}, {now_ms})",
        quote(&cfg.witness_id),
        quote(&cfg.region),
    ))
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

/// Pull one table's pages from the primary and apply them locally.
/// Returns `(rows_pulled, rows_applied)`.
fn pull_table(
    ctx: &Shared,
    cfg: &WitnessConfig,
    db: &str,
    table: &str,
) -> Result<(usize, usize), String> {
    let qualified = skaidb_engine::namespace::qualify(db, table);
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let (mut pulled, mut applied) = (0usize, 0usize);
    let mut after: Option<Vec<u8>> = None;
    let mut pages_since_flush = 0usize;
    loop {
        let page_started = std::time::Instant::now();
        let page = scan_page_with_failover(cfg, &qualified, after.as_deref())?;
        let done = page.len() < PULL_PAGE_ROWS as usize;
        after = page.last().map(|(k, ..)| k.clone());
        pulled += page.len();
        if !page.is_empty() {
            // Staleness guard + apply, one write-lock acquisition per page
            // (2000 rows — same tenure the replica appliers accept).
            let mut dbw = local.write().map_err(|_| "local lock poisoned".to_string())?;
            let fresh: Vec<_> = page
                .into_iter()
                .filter(|(key, _, hlc, _)| {
                    match dbw.local_get_versioned(&qualified, key) {
                        Ok(Some((_, cur, _))) => cur < *hlc,
                        _ => true,
                    }
                })
                .collect();
            applied += fresh.len();
            if !fresh.is_empty() {
                // Sync the page's WAL commits like the replica applier does
                // (one fsync per page): dropping the handles unsynced lets
                // buffered WAL data accumulate in memory for the whole
                // table — a 1.9 GB table's pull OOM-killed the witness
                // three times before this landed. Durability per page is
                // the bonus; bounded memory is the point.
                if let Some((commit, sync)) = dbw
                    .apply_batch_buffered(&qualified, &fresh)
                    .map_err(|e| format!("apply {qualified}: {e}"))?
                {
                    sync.sync_through(commit)
                        .map_err(|e| format!("wal sync {qualified}: {e}"))?;
                }
            }
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
            return Ok((pulled, applied));
        }
        // Bounded duty on the primary (`witness.duty_pct`, default 50,
        // live via SET CONFIG): rest `work × (100 − pct) / pct`.
        let pct = f64::from(cfg.duty_pct.clamp(1, 90));
        let rest = page_started.elapsed().mul_f64((100.0 - pct) / pct);
        std::thread::sleep(rest.max(PULL_PAGE_PAUSE_FLOOR));
    }
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
        .map(|(name, pulled, _)| {
            format!("'{}': {{rows: {pulled}, synced_at: {now_ms}}}", quote(name))
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
