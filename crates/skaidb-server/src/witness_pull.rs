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

/// Soft per-page sample target for the TIME-SLICED TS pull. A witness
/// catching up a large delta used to issue ONE `TsQuery [t0, MAX]` per
/// member — onetw's ~3-day catch-up of an 11.9M-sample table produced a
/// response every primary aborted mid-send (and would have had to sit
/// fully resident in a 320 MB container). The walk below slices the range
/// into windows sized so no single response should much exceed this.
const TS_PAGE_TARGET: usize = 50_000;
/// First window width; adapted per page from what the members return.
const TS_SLICE_INIT_MS: i64 = 15 * 60_000;
/// Adaptive bounds for windows that produced data. EMPTY windows always
/// grow the slice ×8 uncapped — an empty span of ANY size (the decades
/// before a full sweep's first sample, or a mid-range ingest pause) is
/// crossed in logarithmically many probes instead of thousands of capped
/// pages. The window that then lands on data can arrive far too wide —
/// dense enough that the member's response would blow the 64 MB internode
/// frame and the connection dies mid-send. That case is handled
/// REACTIVELY: a failed fetch narrows the window (`TsWalk::narrow`, ÷8)
/// and retries the same cursor, bisecting down until the response fits —
/// a member is only marked unreachable once the window is already at the
/// floor and still failing.
const TS_SLICE_MIN_MS: i64 = 10_000;
const TS_SLICE_MAX_MS: i64 = 24 * 3_600_000;
/// Runaway backstop: a cycle that somehow needs more pages than this
/// fails (and retries next cycle) instead of looping forever.
const TS_MAX_PAGES_PER_CYCLE: usize = 10_000;

/// The adaptive time-window walk driving a paged TS pull. Pure state
/// machine (no I/O) so the sizing/advance/narrow logic is unit-testable:
/// `window` yields the next bounded `[t0, t1]` to fetch, `advance` moves
/// past a fetched window resizing the slice from the largest per-member
/// sample count it returned, and `narrow` bisects the CURRENT window
/// after a failed fetch (oversized response killed the connection).
/// The walk covers `[start, frontier]`; the caller issues one final
/// `[frontier+1, i64::MAX]` fetch afterward for future-stamped samples
/// (expected ~empty — the old one-shot pull's `t1 = MAX` tail).
struct TsWalk {
    cursor: i64,
    slice: i64,
    frontier: i64,
    /// Hard cap the zoom/grow paths must respect: half the smallest window
    /// width that ever FAILED a fetch this walk. Without it the
    /// empty-window zoom re-inflates straight back to the failing width
    /// after every narrow — an oscillation costing one dead connection per
    /// zoom (observed live on onetw's first canary: the walk kept
    /// re-approaching the exact width the peers kept aborting).
    ceiling: i64,
}

impl TsWalk {
    fn new(start: i64, frontier: i64) -> TsWalk {
        TsWalk {
            cursor: start,
            slice: TS_SLICE_INIT_MS,
            frontier,
            ceiling: i64::MAX,
        }
    }

    /// The next window to fetch, or `None` when the walk has covered the
    /// frontier.
    fn window(&self) -> Option<(i64, i64)> {
        if self.cursor > self.frontier {
            return None;
        }
        let end = self.cursor.saturating_add(self.slice);
        Some((self.cursor, end - 1))
    }

    /// Advance past the window just fetched. `max_member_samples` is the
    /// largest sample count any single member returned for it.
    fn advance(&mut self, max_member_samples: usize) {
        self.cursor = self.cursor.saturating_add(self.slice);
        if max_member_samples == 0 {
            // Empty window: zoom exponentially (up to the failure ceiling)
            // — cross any gap (or the pre-data epoch of a full sweep) in
            // O(log) probes. The ceiling does NOT relax here: the empty
            // zoom is exactly the path that overshoots into a dense era.
            self.slice = self.slice.saturating_mul(8).min(self.ceiling);
            return;
        }
        // A window that RETURNED data proves the current scale is
        // servable — relax the failure ceiling multiplicatively (AIMD:
        // failures halve it, successes double it). Without this, the
        // ceiling set while bisecting a giant gap-crossing window pins
        // every later window to seconds-wide slices and a multi-day data
        // era page-storms past the cycle cap (observed on onetw's second
        // canary: each cycle re-descended the ladder, ground at the floor,
        // died at TS_MAX_PAGES_PER_CYCLE, repeat).
        self.ceiling = self.ceiling.saturating_mul(2);
        // Adapt, clamped into the bounded range (this also snaps a
        // gap-crossing zoomed slice straight back to TS_SLICE_MAX).
        let adapted = if max_member_samples > 2 * TS_PAGE_TARGET {
            self.slice / 4
        } else if max_member_samples > TS_PAGE_TARGET {
            self.slice / 2
        } else if max_member_samples < TS_PAGE_TARGET / 8 {
            self.slice.saturating_mul(2)
        } else {
            self.slice
        };
        self.slice = adapted.clamp(TS_SLICE_MIN_MS, TS_SLICE_MAX_MS).min(self.ceiling);
    }

    /// A fetch of the current window failed (typically: the response was
    /// too large for the internode frame and the peer dropped the
    /// connection). Bisect: shrink the slice ÷8, cap all future growth
    /// below the width that just failed, and let the caller retry the SAME
    /// cursor. Returns false once already at the floor — the failure is
    /// then genuinely the member, not the window.
    fn narrow(&mut self) -> bool {
        if self.slice <= TS_SLICE_MIN_MS {
            return false;
        }
        self.ceiling = self.ceiling.min((self.slice / 2).max(TS_SLICE_MIN_MS));
        self.slice = (self.slice / 8).max(TS_SLICE_MIN_MS);
        true
    }
}

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
            ensure_local_table(ctx, db, table, &pt.pk_cols, pt.kind)?;
            let qualified = skaidb_engine::namespace::qualify(db, table);
            // Time-series tables take the sample-pull path (see
            // sync_ts_table): scatter over every member, union, one merge.
            if pt.kind == TableKind::Timeseries {
                let (pulled, applied) = sync_ts_table(ctx, cfg, pool, &qualified, now)
                    .map_err(|e| format!("{qualified}: {e}"))?;
                summary
                    .tables
                    .push((format!("{db}.{table}"), pulled, applied, 0));
                continue;
            }
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

/// Per-member TS pull state for one table's sync cycle.
struct Member {
    addr: String,
    state_key: String,
    t0: i64,
    watermark: u64,
    full: bool,
    last_full_ms: u64,
    failed: bool,
}

fn ts_members(ctx: &Shared, cfg: &WitnessConfig, qualified: &str, now: u64) -> Vec<Member> {
    cfg.primary_internode_addrs
        .iter()
        .map(|addr| {
            let state_key = format!("{qualified}|{addr}");
            let state = load_sync_state(ctx, &state_key);
            let backstop_due = now.saturating_sub(state.last_full_ms)
                >= cfg.full_sweep_interval_secs.saturating_mul(1000);
            let full = state.watermark_ms == 0 || backstop_due;
            let t0 = if full {
                0
            } else {
                state.watermark_ms.saturating_sub(DELTA_MARGIN_MS) as i64
            };
            Member {
                addr: addr.clone(),
                state_key,
                t0,
                watermark: state.watermark_ms,
                full,
                last_full_ms: state.last_full_ms,
                failed: false,
            }
        })
        .collect()
}

/// Sync one TIME-SERIES table via `TsQueryPaged`: each member serves a
/// SERVER-BOUNDED window (≤ TS_PAGE_TARGET samples — only the server
/// knows how many samples a window holds, so the cap must live there);
/// the members' pages are trimmed to the smallest common window, UNIONed,
/// merged, and the watermarks saved, then the cursor advances to the
/// common resume point. Peers that predate the verb (mixed-version
/// fleets) make every first-page call fail → one legacy client-side
/// windowed walk (`sync_ts_table_walk`) serves the cycle instead.
/// Returns `(samples_pulled, samples_applied)`.
fn sync_ts_table(
    ctx: &Shared,
    cfg: &WitnessConfig,
    pool: &Pool,
    qualified: &str,
    now: u64,
) -> Result<(usize, usize), String> {
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let mut members = ts_members(ctx, cfg, qualified, now);
    let mut cursor = members.iter().map(|m| m.t0).min().unwrap_or(0);
    let mut pulled = 0usize;
    let mut applied = 0usize;
    let mut pages = 0usize;
    let mut last_err = String::new();
    loop {
        pages += 1;
        if pages > TS_MAX_PAGES_PER_CYCLE {
            return Err(format!("ts pull did not converge in {TS_MAX_PAGES_PER_CYCLE} pages"));
        }
        // One page: ask every live member for a server-bounded window
        // starting at the shared cursor.
        type PageSeries = Vec<(Vec<(String, String)>, Vec<(i64, f64)>)>;
        let mut responses: Vec<(usize, PageSeries, Option<i64>)> = Vec::new();
        for (i, m) in members.iter_mut().enumerate() {
            if m.failed {
                continue;
            }
            let query_started = std::time::Instant::now();
            match pool.call(
                &m.addr,
                &Request::TsQueryPaged {
                    table: qualified.to_string(),
                    matchers: Vec::new(),
                    t0: cursor,
                    t1: i64::MAX,
                    max_samples: TS_PAGE_TARGET as u32,
                },
            ) {
                Ok(Response::TsSeriesPage { series, resume_t0 }) => {
                    responses.push((i, series, resume_t0));
                }
                Ok(Response::Err(e)) => {
                    last_err = e;
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{} unavailable this cycle: {}",
                        m.addr,
                        last_err
                    );
                    m.failed = true;
                }
                Ok(other) => {
                    last_err = format!("unexpected {other:?}");
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{}: {last_err}",
                        m.addr
                    );
                    m.failed = true;
                }
                Err(e) => {
                    last_err = e.to_string();
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{} unavailable this cycle: {}",
                        m.addr,
                        last_err
                    );
                    m.failed = true;
                }
            }
            // Duty pacing, same rule as the row pull: rest in proportion
            // to the time this member just spent serving us.
            let pct = f64::from(cfg.duty_pct.clamp(1, 90));
            let rest = query_started.elapsed().mul_f64((100.0 - pct) / pct);
            std::thread::sleep(rest.max(PULL_PAGE_PAUSE_FLOOR));
        }
        if responses.is_empty() {
            if pages == 1 {
                // Possibly a pre-TsQueryPaged fleet: one shot at the legacy
                // client-side windowed walk before failing the cycle.
                return sync_ts_table_walk(ctx, cfg, pool, qualified, now);
            }
            return Err(format!("no source reachable: {last_err}"));
        }
        // Trim every member's page to the smallest common window so the
        // union stays exact (a member that served further ahead simply
        // re-serves the trimmed remainder from the next cursor — bounded,
        // idempotent waste).
        let window_end: i64 = responses
            .iter()
            .map(|(_, _, resume)| resume.map_or(i64::MAX, |r| r.saturating_sub(1)))
            .min()
            .expect("responses non-empty");
        let mut union: std::collections::BTreeMap<(Vec<(String, String)>, i64), f64> =
            std::collections::BTreeMap::new();
        for (i, series, _) in responses {
            let m = &mut members[i];
            for (labels, samples) in series {
                for (ts, value) in samples {
                    if ts > window_end {
                        continue;
                    }
                    if ts > 0 {
                        m.watermark = m.watermark.max(ts as u64);
                    }
                    pulled += 1;
                    union.insert((labels.clone(), ts), value);
                }
            }
        }
        if !union.is_empty() {
            let rows: Vec<_> = union
                .into_iter()
                .map(|((labels, ts), value)| (labels, ts, value))
                .collect();
            let db = local
                .read()
                .map_err(|_| "witness local lock poisoned".to_string())?;
            applied += db
                .ts_merge(qualified, &rows)
                .map_err(|e| format!("ts merge {qualified}: {e}"))?;
        }
        // Watermarks track DATA time (max sample ts) — see the walk
        // variant's comment; saved per landed page for crash-resume.
        for m in members.iter().filter(|m| !m.failed) {
            save_sync_state(ctx, &m.state_key, &m.addr, 0, m.watermark, m.last_full_ms)?;
        }
        if window_end == i64::MAX {
            break; // every live member reported the range complete
        }
        cursor = window_end.saturating_add(1);
    }
    // A full sweep only counts as complete for members that answered every
    // page — stamp their backstop clock.
    for m in &members {
        if m.full && !m.failed {
            save_sync_state(ctx, &m.state_key, &m.addr, 0, m.watermark, now)?;
        }
    }
    Ok((pulled, applied))
}

/// LEGACY fallback for primaries that predate `TsQueryPaged`: client-side
/// adaptive time windows (`TsWalk`) — bounded requests, but the server
/// picks nothing, so a window that lands on dense data can still produce
/// an oversized response (mitigated by `narrow()` bisection on transport
/// failure). Kept for mixed-version fleets only.
fn sync_ts_table_walk(
    ctx: &Shared,
    cfg: &WitnessConfig,
    pool: &Pool,
    qualified: &str,
    now: u64,
) -> Result<(usize, usize), String> {
    let Backend::Local(local) = &ctx.backend else {
        return Err("witness pull requires a standalone backend".into());
    };
    let mut members = ts_members(ctx, cfg, qualified, now);
    let start = members.iter().map(|m| m.t0).min().unwrap_or(0);
    let mut walk = TsWalk::new(start, now as i64);
    let mut pulled = 0usize;
    let mut applied = 0usize;
    let mut pages = 0usize;
    let mut last_err = String::new();
    // The bounded walk over [start, now], then one tail fetch
    // [now+1, MAX] for future-stamped samples (the old one-shot pull's
    // t1 = MAX covered those; the tail is ~always empty).
    let mut tail_done = false;
    loop {
        let (t0, t1) = match walk.window() {
            Some(w) => w,
            None if !tail_done => {
                tail_done = true;
                ((now as i64).saturating_add(1), i64::MAX)
            }
            None => break,
        };
        pages += 1;
        if pages > TS_MAX_PAGES_PER_CYCLE {
            return Err(format!("ts pull did not converge in {TS_MAX_PAGES_PER_CYCLE} pages"));
        }
        let mut union: std::collections::BTreeMap<(Vec<(String, String)>, i64), f64> =
            std::collections::BTreeMap::new();
        let mut page_max = 0usize; // largest single-member sample count
        let mut answered: Vec<usize> = Vec::new();
        let mut retry_narrower = false;
        for (i, m) in members.iter_mut().enumerate() {
            if m.failed || (t1 != i64::MAX && m.t0 > t1) {
                continue; // window entirely behind this member's watermark
            }
            let query_started = std::time::Instant::now();
            let result = pool.call(
                &m.addr,
                &Request::TsQuery {
                    table: qualified.to_string(),
                    matchers: Vec::new(),
                    t0,
                    t1,
                },
            );
            let series = match result {
                Ok(Response::TsSeries { series }) => series,
                Ok(Response::Err(e)) => {
                    last_err = e;
                    // A structured error is the member speaking, not a
                    // window-size problem — no narrower retry.
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{} unavailable this cycle: {}",
                        m.addr,
                        last_err
                    );
                    m.failed = true;
                    continue;
                }
                Ok(other) => {
                    last_err = format!("unexpected {other:?}");
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{}: {last_err}",
                        m.addr
                    );
                    m.failed = true;
                    continue;
                }
                Err(e) => {
                    // Transport death — the classic cause is a response too
                    // large for the internode frame (the peer aborts the
                    // send mid-frame). Bisect the window and retry the same
                    // cursor before giving up on the member.
                    last_err = e.to_string();
                    if t1 != i64::MAX && walk.narrow() {
                        skaidb_types::slog!(
                            "witness: {qualified} (timeseries) @{} failed a \
                             {}ms window ({last_err}); retrying narrower",
                            m.addr,
                            t1.saturating_sub(t0).saturating_add(1)
                        );
                        retry_narrower = true;
                        break;
                    }
                    skaidb_types::slog!(
                        "witness: {qualified} (timeseries) @{} unavailable this cycle: {}",
                        m.addr,
                        last_err
                    );
                    m.failed = true;
                    continue;
                }
            };
            let mut member_samples = 0usize;
            for (labels, samples) in series {
                for (ts, value) in samples {
                    if ts > 0 {
                        m.watermark = m.watermark.max(ts as u64);
                    }
                    member_samples += 1;
                    union.insert((labels.clone(), ts), value);
                }
            }
            pulled += member_samples;
            page_max = page_max.max(member_samples);
            answered.push(i);
            // Duty pacing, same rule as the row pull: rest in proportion
            // to the time this member just spent serving us.
            let pct = f64::from(cfg.duty_pct.clamp(1, 90));
            let rest = query_started.elapsed().mul_f64((100.0 - pct) / pct);
            std::thread::sleep(rest.max(PULL_PAGE_PAUSE_FLOOR));
        }
        if retry_narrower {
            // Abandon this window's partial union (re-pulls are
            // idempotent) and re-issue the same cursor with the bisected
            // slice. Members that already answered are simply asked again.
            // (Never taken for the tail window — narrow() is only tried on
            // bounded windows.)
            continue;
        }
        if answered.is_empty() && members.iter().all(|m| m.failed) {
            // Progress through prior pages is already saved; the next
            // cycle resumes from the advanced watermarks.
            return Err(format!("no source reachable: {last_err}"));
        }
        // Land THIS page, then advance the answering members' watermarks —
        // a crash mid-walk resumes at the last landed window instead of
        // re-pulling (or worse, skipping) the whole range. Samples merge
        // idempotently, so the DELTA_MARGIN overlap and re-pulls are safe.
        if !union.is_empty() {
            let rows: Vec<_> = union
                .into_iter()
                .map(|((labels, ts), value)| (labels, ts, value))
                .collect();
            let db = local
                .read()
                .map_err(|_| "witness local lock poisoned".to_string())?;
            applied += db
                .ts_merge(qualified, &rows)
                .map_err(|e| format!("ts merge {qualified}: {e}"))?;
        }
        // Watermarks track DATA time (max sample ts seen), never wall-clock
        // coverage — a table whose samples lag the wall clock (backfills,
        // epoch-stamped test data) must keep its delta anchor at the data,
        // or the next cycle's `watermark - margin` starts above samples
        // that haven't arrived yet. Saving per landed page makes a
        // mid-walk crash resume from the last data seen; empty-window
        // progress is deliberately not persisted (re-crossing a gap costs
        // O(log) zoom pages).
        for &i in &answered {
            let m = &mut members[i];
            save_sync_state(ctx, &m.state_key, &m.addr, 0, m.watermark, m.last_full_ms)?;
        }
        if !tail_done {
            walk.advance(page_max);
        }
    }
    // A full sweep only counts as complete for members that answered every
    // window they were asked — stamp their backstop clock.
    for m in &members {
        if m.full && !m.failed {
            save_sync_state(ctx, &m.state_key, &m.addr, 0, m.watermark, now)?;
        }
    }
    Ok((pulled, applied))
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
/// How a primary table's data moves: rows page over `ScanPage`/
/// `ScanSincePage`; time-series samples move via `TsQuery` + local merge.
#[derive(Debug, Clone, Copy, PartialEq)]
enum TableKind {
    Row,
    /// A `CREATE TIMESERIES` table — including rollups, which the witness
    /// mirrors as PLAIN time-series tables (their samples are preserved;
    /// the rollup→source link is derived state the backup doesn't need).
    Timeseries,
}

struct PrimaryTable {
    table: String,
    pk_cols: Vec<String>,
    kind: TableKind,
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
                    // Absent column = pre-kind primary: every table it lists
                    // is a row table as far as this witness can pull anyway.
                    kind: match idx("kind").and_then(|i| r.get(i)) {
                        Some(Value::String(k)) if k == "timeseries" || k == "rollup" => {
                            TableKind::Timeseries
                        }
                        _ => TableKind::Row,
                    },
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
/// A time-series table's listed key is `(series key, ts)` — recreate it as
/// a TIMESERIES table with that series key. No RETENTION on the mirror
/// (a backup keeps everything the primary may have already aged out) and
/// no OOO window (samples land via the any-aged merge path, not append).
fn ensure_local_table(
    ctx: &Shared,
    db: &str,
    table: &str,
    pk_cols: &[String],
    kind: TableKind,
) -> Result<(), String> {
    let role = ctx.superuser_role.clone();
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    let create = match kind {
        TableKind::Row => format!(
            "CREATE TABLE IF NOT EXISTS {db}.{table} (PRIMARY KEY ({}))",
            pk_cols.join(", ")
        ),
        TableKind::Timeseries => {
            let series: Vec<&String> = pk_cols.iter().filter(|c| *c != "ts").collect();
            if series.is_empty() {
                return Err(format!("{db}.{table}: empty series key in listing"));
            }
            format!(
                "CREATE TIMESERIES TABLE IF NOT EXISTS {db}.{table} (SERIES KEY ({}))",
                series.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
            )
        }
    };
    for sql in [format!("CREATE DATABASE IF NOT EXISTS {db}"), create] {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The paged TS walk: covers [start, frontier] in bounded windows with
    /// a final `i64::MAX` window, zooms exponentially over an empty
    /// full-sweep prefix, shrinks under dense pages, and never exceeds the
    /// page cap for a realistic span. Regression for the 2026-07-24 onetw
    /// finding: a multi-day catch-up must not ride one unbounded TsQuery.
    #[test]
    fn ts_walk_pages_cover_range_and_adapt() {
        // Delta-style walk: 2h behind, quiet table -> few pages.
        let now = 1_784_000_000_000i64;
        let mut w = TsWalk::new(now - 2 * 3_600_000, now);
        let mut windows = Vec::new();
        while let Some((t0, t1)) = w.window() {
            windows.push((t0, t1));
            w.advance(100); // sparse data
            assert!(windows.len() < 100, "quiet 2h delta must not page-storm");
        }
        // Contiguous coverage from start past the frontier, no gaps.
        for pair in windows.windows(2) {
            assert_eq!(pair[0].1 + 1, pair[1].0, "gap between windows: {pair:?}");
        }
        assert_eq!(windows[0].0, now - 2 * 3_600_000);
        assert!(windows.last().unwrap().1 >= now, "walk must cover the frontier");

        // Full sweep from 0: the empty epoch prefix must zoom, not crawl.
        let mut w = TsWalk::new(0, now);
        let mut pages = 0;
        while let Some((_, t1)) = w.window() {
            pages += 1;
            // Empty until the last ~2 days of the span, then dense.
            let dense = t1 > now - 2 * 86_400_000;
            w.advance(if dense { 3 * TS_PAGE_TARGET } else { 0 });
            assert!(pages < 200, "full sweep from epoch 0 page-stormed");
        }
        assert!(pages < 100, "expected a few dozen pages, got {pages}");

        // The gap-crossing overshoot: a zoomed window landing on dense data
        // fails its fetch (frame too large) — narrow() bisects the SAME
        // cursor down to a servable width, only bottoming out at the floor.
        let mut w = TsWalk::new(0, now);
        for _ in 0..12 {
            w.advance(0); // zoom across the empty epoch
        }
        assert!(w.slice > TS_SLICE_MAX_MS, "zoom should have exceeded the data cap");
        let cursor_before = w.cursor;
        let mut narrows = 0;
        while w.narrow() {
            narrows += 1;
            assert_eq!(w.cursor, cursor_before, "narrow must not move the cursor");
            assert!(narrows < 64, "narrow never reached the floor");
        }
        assert_eq!(w.slice, TS_SLICE_MIN_MS);
        assert!(!w.narrow(), "at the floor narrow() must report failure");

        // After a narrow, EMPTY windows must NOT re-zoom past the failure
        // ceiling (the live onetw oscillation: zoom -> fail -> narrow ->
        // empty -> zoom back to the same failing width, one dead
        // connection per lap).
        let mut w = TsWalk::new(0, now);
        for _ in 0..12 {
            w.advance(0);
        }
        let failed_width = w.slice;
        assert!(w.narrow());
        for _ in 0..20 {
            w.advance(0); // empty windows try to re-zoom
            assert!(
                w.slice < failed_width,
                "zoom re-approached a width that already failed: {} >= {failed_width}",
                w.slice
            );
        }

        // ...but DATA windows relax the ceiling (AIMD) — after a deep
        // bisection the walk must climb back to useful widths within a
        // handful of successful pages instead of grinding a multi-day era
        // at seconds-wide slices into the page cap (onetw canary #2).
        let mut w = TsWalk::new(0, now);
        for _ in 0..12 {
            w.advance(0); // zoom
        }
        while w.narrow() {} // bisect to the floor, ceiling collapsed
        let floor_slice = w.slice;
        for _ in 0..24 {
            // Sparse data pages (< target/8) want to grow the slice; the
            // relaxing ceiling must let them.
            w.advance(TS_PAGE_TARGET / 16);
        }
        assert!(
            w.slice >= floor_slice * 64,
            "ceiling never relaxed: slice stuck at {} after 24 sparse pages",
            w.slice
        );

        // Data at EPOCH-SMALL timestamps followed by a ~55-year empty span
        // (the witness_pull_mirrors_timeseries_tables shape): the walk must
        // re-zoom after data ends, not crawl the gap at the capped slice.
        let mut w = TsWalk::new(0, now);
        let mut pages = 0;
        while let Some((t0, t1)) = w.window() {
            pages += 1;
            let has_data = t0 <= 3_000 && t1 >= 1_000; // samples at ts 1000..3000
            w.advance(if has_data { 3 } else { 0 });
            assert!(pages < 60, "epoch-data walk page-stormed (gap not re-zoomed)");
        }

        // Dense pages shrink the slice toward the floor; sparse re-grow it.
        let mut w = TsWalk::new(0, now);
        let before = w.slice;
        w.advance(3 * TS_PAGE_TARGET);
        assert!(w.slice < before, "dense page must shrink the slice");
        let shrunk = w.slice;
        w.advance(1); // nearly-empty page (data seen already)
        assert!(w.slice > shrunk, "sparse page must re-grow the slice");
        // Floor holds under repeated dense pages.
        for _ in 0..50 {
            w.advance(10 * TS_PAGE_TARGET);
        }
        assert!(w.slice >= TS_SLICE_MIN_MS);
    }
}
