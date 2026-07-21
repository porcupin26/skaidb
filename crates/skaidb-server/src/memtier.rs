//! Standalone memory-pressure release tier (`Backend::Local`).
//!
//! Cluster members have had a Node-level release/shed loop since v0.70.15;
//! standalone deployments (witnesses, single-node servers, the embedded
//! server behind a bulk loader) had NOTHING driving reclaim — the engine's
//! bulk-apply paths leave flushing to their caller, so a standalone node
//! ingesting hard accumulated memtables until the cgroup OOM kill. The
//! first witness deployment burned five OOM iterations working around this
//! by hand; this module is the general fix: the same sampler ladder as the
//! cluster tier (release at 75% of the detected limit, shed client writes
//! at 85% with hysteresis, ramp/distress logging), driving
//! `release_memory_under_pressure` — which flushes memtables, commits
//! search writers, and at shed level drops the byte-blind point-read
//! caches.
//!
//! The shed flag gates CLIENT mutations (INSERT/UPDATE/DELETE through the
//! session layer) with the same retryable error the cluster tier uses.
//! Internal writers (the witness pull, sync bookkeeping) bypass it — they
//! self-pace and byte-flush, and blocking them under pressure would
//! deadlock recovery exactly the way the cluster tier's doc warns about.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use skaidb_cluster::memguard::{self, MemoryGuard, Pressure};

use crate::shared::{Backend, Shared};

static GUARD: OnceLock<MemoryGuard> = OnceLock::new();

/// Whether the standalone tier is currently shedding client writes.
/// `false` until [`spawn_for_local`] has run (cluster backends never set it —
/// the Node tier owns shedding there).
pub fn shedding() -> bool {
    GUARD.get().is_some_and(MemoryGuard::shedding)
}

/// The retryable client-facing refusal, worded like the cluster tier's so
/// drivers treat both identically.
pub const SHED_ERROR: &str = "memory pressure: node is shedding writes, retry";

/// Test hook: force (or clear) the shed flag without a sampler thread.
#[cfg(test)]
pub fn force_shed_for_test(on: bool) {
    GUARD.get_or_init(MemoryGuard::new).force(on);
}

/// Start the sampler when the backend is standalone. No-op (with no thread)
/// for cluster backends and when no memory limit is detectable.
pub fn spawn_for_local(ctx: Shared) {
    if !matches!(ctx.backend, Backend::Local(_)) {
        return;
    }
    let guard = GUARD.get_or_init(MemoryGuard::new);
    if guard.snapshot().1 == 0 {
        skaidb_types::slog!(
            "skaidb: standalone memory tier inert — no cgroup/system memory limit detected"
        );
        return;
    }
    std::thread::spawn(move || {
        const RELEASE_EVERY: Duration = Duration::from_secs(10);
        const DISTRESS_EVERY: Duration = Duration::from_secs(60);
        const RAMP_LOG_EVERY: Duration = Duration::from_secs(900);
        let guard = GUARD.get().expect("initialized above");
        let mut last_release: Option<Instant> = None;
        let mut shed_since: Option<Instant> = None;
        let mut last_distress: Option<Instant> = None;
        let mut last_ramp_log: Option<Instant> = None;
        loop {
            let pressure = guard.sample_pressure();
            if pressure >= Pressure::Release
                && last_release.is_none_or(|t| t.elapsed() >= RELEASE_EVERY)
            {
                last_release = Some(Instant::now());
                let reclaimed = match &ctx.backend {
                    Backend::Local(local) => match local.write() {
                        Ok(mut db) => {
                            db.release_memory_under_pressure(pressure == Pressure::Shed)
                        }
                        Err(_) => 0,
                    },
                    Backend::Cluster(_) => unreachable!("spawned for Local only"),
                };
                if reclaimed > 0 {
                    skaidb_types::slog!(
                        "skaidb: memory pressure — flushed {} MB of memtables, committed \
                         search writers",
                        reclaimed / (1024 * 1024)
                    );
                }
            }
            let (used, limit) = guard.snapshot();
            let breakdown = || {
                let mut s = memguard::anon_file_breakdown().map_or(String::new(), |(a, f)| {
                    format!(" (anon {} MB, file {} MB)", a / (1024 * 1024), f / (1024 * 1024))
                });
                if let Some(alloc) = memguard::alloc_stats() {
                    s.push_str(&format!(" [{alloc}]"));
                }
                s
            };
            match (pressure == Pressure::Shed, shed_since) {
                (true, None) => {
                    shed_since = Some(Instant::now());
                    last_distress = Some(Instant::now());
                    skaidb_types::slog!(
                        "skaidb: memory pressure — SHEDDING client writes at {}/{} MB{}",
                        used / (1024 * 1024),
                        limit / (1024 * 1024),
                        breakdown()
                    );
                }
                (true, Some(since)) => {
                    if last_distress.is_none_or(|t| t.elapsed() >= DISTRESS_EVERY) {
                        last_distress = Some(Instant::now());
                        skaidb_types::slog!(
                            "skaidb: memory pressure — still shedding after {}s at {}/{} MB{} — \
                             releases are not freeing enough; OOM risk",
                            since.elapsed().as_secs(),
                            used / (1024 * 1024),
                            limit / (1024 * 1024),
                            breakdown()
                        );
                    }
                }
                (false, Some(since)) => {
                    shed_since = None;
                    skaidb_types::slog!(
                        "skaidb: memory pressure — recovered after {}s, now {}/{} MB",
                        since.elapsed().as_secs(),
                        used / (1024 * 1024),
                        limit / (1024 * 1024)
                    );
                }
                (false, None) => {}
            }
            if limit > 0 && used > limit / 2 {
                if last_ramp_log.is_none_or(|t: Instant| t.elapsed() >= RAMP_LOG_EVERY) {
                    last_ramp_log = Some(Instant::now());
                    skaidb_types::slog!(
                        "skaidb: memory ramp — {}/{} MB{}",
                        used / (1024 * 1024),
                        limit / (1024 * 1024),
                        breakdown()
                    );
                }
            } else {
                last_ramp_log = None;
            }
            std::thread::sleep(memguard::SAMPLE_INTERVAL);
        }
    });
}
