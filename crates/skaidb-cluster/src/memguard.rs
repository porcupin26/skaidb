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
use std::time::Duration;

/// Start shedding above this percent of the memory limit…
const HIGH_PCT: u64 = 85;
/// …and stop once back below this percent (hysteresis).
const LOW_PCT: u64 = 70;
/// How often the sampler re-reads memory usage.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(1000);
/// A cgroup "max" at or above this is "unlimited" — ignore it.
const CGROUP_UNLIMITED: u64 = 1 << 60;

/// Tracks memory usage against the node's limit and exposes a shedding flag.
#[derive(Debug)]
pub struct MemoryGuard {
    shedding: AtomicBool,
    used: AtomicU64,
    limit: u64,
    high: u64,
    low: u64,
}

impl MemoryGuard {
    /// Build a guard from the detected memory limit (cgroup, else system RAM).
    pub fn new() -> Self {
        let limit = detected_limit().unwrap_or(0);
        Self {
            shedding: AtomicBool::new(false),
            used: AtomicU64::new(0),
            limit,
            high: limit / 100 * HIGH_PCT,
            low: limit / 100 * LOW_PCT,
        }
    }

    /// Build a guard with an explicit limit (tests).
    #[cfg(test)]
    pub fn with_limit(limit: u64) -> Self {
        Self {
            shedding: AtomicBool::new(false),
            used: AtomicU64::new(0),
            limit,
            high: limit / 100 * HIGH_PCT,
            low: limit / 100 * LOW_PCT,
        }
    }

    /// Whether the node is currently shedding writes.
    pub fn shedding(&self) -> bool {
        self.shedding.load(Ordering::Relaxed)
    }

    /// Last sampled usage (bytes) and the limit (bytes; 0 = no limit).
    pub fn snapshot(&self) -> (u64, u64) {
        (self.used.load(Ordering::Relaxed), self.limit)
    }

    /// Re-sample usage and update the flag with hysteresis. Returns the new
    /// shedding state. With no limit the guard never sheds.
    pub fn sample(&self) -> bool {
        if self.limit == 0 {
            return false;
        }
        let used = current_usage().unwrap_or(0);
        self.update(used)
    }

    /// Force the shedding flag directly (tests only).
    #[cfg(test)]
    pub fn force(&self, on: bool) {
        self.shedding.store(on, Ordering::Relaxed);
    }

    /// The hysteresis transition, factored out so tests can drive it directly.
    fn update(&self, used: u64) -> bool {
        self.used.store(used, Ordering::Relaxed);
        let was = self.shedding.load(Ordering::Relaxed);
        let now = if was { used > self.low } else { used > self.high };
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
}
