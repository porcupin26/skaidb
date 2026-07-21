//! Memory-pressure load shedding.
//!
//! A node under memory pressure must not keep allocating until the kernel OOM
//! killer takes the whole container down — a 512 MB node did exactly that
//! during a bulk full-text load, going unresponsive and dropping out of the
//! ring. This watches the node's memory against its limit and, past a
//! high-water mark, raises a shedding flag so the write path rejects new
//! writes with a retryable error. That lets the node drain its in-memory work
//! (flush the memtable, commit/merge search segments), shrink its footprint,
//! and leave the OS its headroom, then resume — instead of dying. A lower
//! clear mark (hysteresis) keeps the flag from flapping.
//!
//! Reads and already-buffered work are never shed: only *new* writes are
//! turned away, which is what actually grows memory. With no detectable limit
//! (no cgroup, no `/proc`) the guard is inert.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Start shedding above this percent of the memory limit…
const HIGH_PCT: u64 = 85;
/// …and stop once back below this percent (hysteresis).
const LOW_PCT: u64 = 70;
/// Above this percent, actively *release* memory (flush memtables, commit
/// search writers, return allocator pages to the OS) before shedding ever
/// starts. Shedding only stops new writes; nothing about it frees what is
/// already held — a node that merely sheds rides its limit until the fault
/// storm or the OOM killer gets it (observed twice in production: anon crept
/// to the cgroup ceiling, file cache went to ~0, and the node thrashed itself
/// unreachable at 500 MB/s of major faults).
const RELEASE_PCT: u64 = 75;
/// How often the sampler re-reads memory usage.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(1000);
/// A cgroup "max" at or above this is "unlimited" — ignore it.
const CGROUP_UNLIMITED: u64 = 1 << 60;

/// Memory pressure, coarsely. Ordered: each level implies the ones below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Pressure {
    /// Comfortable headroom.
    Normal,
    /// Above [`RELEASE_PCT`]: actively free reclaimable memory now.
    Release,
    /// Above [`HIGH_PCT`] (with hysteresis): also reject new writes.
    Shed,
}

/// Allocator introspection numbers (jemalloc), in bytes. `allocated` is live
/// application heap; `resident` is what the allocator's pages actually pin in
/// RAM; `retained` is address space freed by the app but not yet returned to
/// the OS. `resident - allocated` ≈ fragmentation + not-yet-purged dirty
/// pages — the split that decides whether a node ratcheting to its cgroup
/// ceiling is *live memory* (find the holder) or *allocator behavior*
/// (purge/decay tuning), which is exactly what the production OOM
/// post-mortems could not tell.
#[derive(Debug, Clone, Copy)]
pub struct AllocStats {
    pub allocated: u64,
    pub resident: u64,
    pub retained: u64,
}

/// Process-wide allocator introspection hook. Lives here rather than on a
/// `Node` because the allocator is program-global and the hook is registered
/// once by the binary that chose it; library consumers without one simply
/// never set it.
static ALLOC_STATS_HOOK: OnceLock<Box<dyn Fn() -> Option<AllocStats> + Send + Sync>> =
    OnceLock::new();

/// Register the allocator stats hook (first caller wins).
pub fn set_alloc_stats_hook(hook: Box<dyn Fn() -> Option<AllocStats> + Send + Sync>) {
    let _ = ALLOC_STATS_HOOK.set(hook);
}

/// The current allocator numbers; `None` without a hook.
pub fn alloc_numbers() -> Option<AllocStats> {
    ALLOC_STATS_HOOK.get().and_then(|h| h())
}

/// A one-line allocator summary for pressure logs; `None` without a hook.
pub fn alloc_stats() -> Option<String> {
    let a = alloc_numbers()?;
    let mb = |v: u64| v / (1024 * 1024);
    Some(format!(
        "jemalloc: allocated {} MB, resident {} MB, retained {} MB",
        mb(a.allocated),
        mb(a.resident),
        mb(a.retained),
    ))
}

/// The cgroup's anon vs file byte split, for pressure diagnostics ("is this
/// real heap or just cache?"). `None` outside cgroup v2.
pub fn anon_file_breakdown() -> Option<(u64, u64)> {
    let s = std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let read = |key: &str| {
        s.lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|v| v.trim().parse::<u64>().ok())
    };
    Some((read("anon ")?, read("file ").unwrap_or(0)))
}

/// Tracks memory usage against the node's limit and exposes a shedding flag.
#[derive(Debug)]
pub struct MemoryGuard {
    shedding: AtomicBool,
    used: AtomicU64,
    /// Bytes; 0 = no detectable limit. Refreshed on every sample so a live
    /// cgroup resize (`pct set -memory …` on a running container) takes
    /// effect without a restart. `u64::MAX` marks a fixed test limit.
    limit: AtomicU64,
    /// Fixed limit for tests (bypasses re-detection).
    fixed: bool,
}

impl MemoryGuard {
    /// Build a guard from the detected memory limit (cgroup, else system RAM).
    pub fn new() -> Self {
        Self {
            shedding: AtomicBool::new(false),
            used: AtomicU64::new(0),
            limit: AtomicU64::new(detected_limit().unwrap_or(0)),
            fixed: false,
        }
    }

    /// Build a guard with an explicit limit (tests).
    #[cfg(test)]
    pub fn with_limit(limit: u64) -> Self {
        Self {
            shedding: AtomicBool::new(false),
            used: AtomicU64::new(0),
            limit: AtomicU64::new(limit),
            fixed: true,
        }
    }

    /// Whether the node is currently shedding writes.
    pub fn shedding(&self) -> bool {
        self.shedding.load(Ordering::Relaxed)
    }

    /// Last sampled usage (bytes) and the limit (bytes; 0 = no limit).
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.used.load(Ordering::Relaxed),
            self.limit.load(Ordering::Relaxed),
        )
    }

    /// Re-sample usage (and the limit — cgroup limits can be resized live)
    /// and update the flag with hysteresis. Returns the new shedding state.
    /// With no limit the guard never sheds.
    pub fn sample(&self) -> bool {
        if !self.fixed {
            self.limit
                .store(detected_limit().unwrap_or(0), Ordering::Relaxed);
        }
        if self.limit.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let used = current_usage().unwrap_or(0);
        self.update(used)
    }

    /// Re-sample and classify. [`Pressure::Shed`] follows the hysteresis flag;
    /// [`Pressure::Release`] is a plain threshold (no hysteresis — releasing
    /// reclaimable memory twice is harmless, rejecting writes twice is not).
    pub fn sample_pressure(&self) -> Pressure {
        let shedding = self.sample();
        let (used, limit) = self.snapshot();
        classify(used, limit, shedding)
    }

    /// Force the shedding flag directly (tests only — including downstream
    /// crates' tests, so no `cfg(test)`: that gate only applies when THIS
    /// crate compiles in test mode, which a dependent's test build is not).
    pub fn force(&self, on: bool) {
        self.shedding.store(on, Ordering::Relaxed);
    }

    /// The hysteresis transition, factored out so tests can drive it directly.
    fn update(&self, used: u64) -> bool {
        self.used.store(used, Ordering::Relaxed);
        let limit = self.limit.load(Ordering::Relaxed);
        let (high, low) = (limit / 100 * HIGH_PCT, limit / 100 * LOW_PCT);
        let was = self.shedding.load(Ordering::Relaxed);
        let now = if was { used > low } else { used > high };
        if now != was {
            self.shedding.store(now, Ordering::Relaxed);
        }
        now
    }
}

impl Default for MemoryGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Pure pressure classification (the shedding flag carries the hysteresis).
fn classify(used: u64, limit: u64, shedding: bool) -> Pressure {
    if shedding {
        Pressure::Shed
    } else if limit > 0 && used > limit / 100 * RELEASE_PCT {
        Pressure::Release
    } else {
        Pressure::Normal
    }
}

/// The memory limit in bytes: the cgroup limit when one applies (so a
/// container is bounded by its own allowance), else total system memory.
fn detected_limit() -> Option<u64> {
    cgroup_read("memory.max")
        .filter(|v| *v < CGROUP_UNLIMITED)
        .or_else(meminfo_total)
}

/// Current usage in bytes: the cgroup charge **minus reclaimable file-backed
/// page cache**, else this process's RSS.
///
/// `memory.current` counts mmap'd SSTable/WAL/search-segment page cache, which
/// the kernel evicts before it ever OOM-kills. Counting it toward the shed
/// threshold falsely sheds a node whose real (anon + kernel + unreclaimable
/// slab) footprint has ample headroom — e.g. after a search-index build fills
/// the cache, a node at 76% real usage reads as 96% and rejects every write.
/// Subtracting the reclaimable file cache leaves the footprint that actually
/// risks OOM, so shedding tracks real pressure instead of cache.
fn current_usage() -> Option<u64> {
    match cgroup_read("memory.current") {
        Some(current) => {
            let reclaimable = cgroup_stat_sum(&["inactive_file", "active_file"]).unwrap_or(0);
            Some(current.saturating_sub(reclaimable))
        }
        None => self_rss(),
    }
}

/// Sum the named counters from cgroup v2 `memory.stat` (bytes). `None` if the
/// file is unavailable or none of the keys are present.
fn cgroup_stat_sum(keys: &[&str]) -> Option<u64> {
    let s = std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let mut total = 0u64;
    let mut found = false;
    for line in s.lines() {
        let mut it = line.split_whitespace();
        if let (Some(k), Some(v)) = (it.next(), it.next()) {
            if keys.contains(&k) {
                if let Ok(n) = v.parse::<u64>() {
                    total += n;
                    found = true;
                }
            }
        }
    }
    found.then_some(total)
}

fn cgroup_read(file: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/sys/fs/cgroup/{file}"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn meminfo_total() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = s.lines().find(|l| l.starts_with("MemTotal:"))?;
    Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

fn self_rss() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hysteresis_flag() {
        // Limit 1000 → high 850, low 700.
        let g = MemoryGuard::with_limit(1000);
        assert!(!g.shedding());
        assert!(!g.update(800)); // below high — still fine
        assert!(g.update(900)); // above high — start shedding
        assert!(g.update(800)); // above low — stay shedding (hysteresis)
        assert!(!g.update(650)); // below low — stop shedding
        assert_eq!(g.snapshot(), (650, 1000));
    }

    #[test]
    fn no_limit_never_sheds() {
        let g = MemoryGuard::with_limit(0);
        assert!(!g.sample());
        assert!(!g.shedding());
    }

    #[test]
    fn pressure_tiers() {
        // Limit 1000 → release above 750; shed follows the flag.
        assert_eq!(classify(700, 1000, false), Pressure::Normal);
        assert_eq!(classify(760, 1000, false), Pressure::Release);
        assert_eq!(classify(900, 1000, true), Pressure::Shed);
        // Hysteresis: still shedding at 800 stays Shed, not Release.
        assert_eq!(classify(800, 1000, true), Pressure::Shed);
        // No limit: never anything but Normal.
        assert_eq!(classify(900, 0, false), Pressure::Normal);
        assert!(Pressure::Shed > Pressure::Release && Pressure::Release > Pressure::Normal);
    }
}
