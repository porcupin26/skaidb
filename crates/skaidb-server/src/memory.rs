//! Storage memory budgeting (`[storage] memory_target`).
//!
//! A node can dedicate a slice of its RAM to storage instead of running on
//! the conservative fixed defaults: `memory_target = "auto"` discovers the
//! node's memory limit — the **cgroup** limit when one applies, so an LXC/
//! container gets its own cap rather than the host's — and budgets half of
//! it; an explicit `"256MB"`-style value budgets exactly that. The budget is
//! split across the memtable (which also bounds worst-case ingest RSS — the
//! fixed 256 MB default can exceed a small container's entire allowance) and
//! the point-read cache.

/// How a resolved budget is split. The remainder is deliberate headroom for
/// block caches, WAL buffers, connections, and compaction scratch.
const MEMTABLE_SHARE: u64 = 4; // budget / 4
const READ_CACHE_SHARE: u64 = 2; // budget / 2
const SEARCH_WRITER_SHARE: u64 = 8; // budget / 8 per search index

/// Bounds on the derived per-index search writer heap. The floor is
/// Tantivy's practical minimum; the ceiling is the fixed default — a bigger
/// heap mostly buys bulk-build speed, and peak RSS runs ≈ 1.5× the heap
/// (phase-0 spike). Search reads cost no budget: segments are mmap'd and
/// evictable.
const SEARCH_WRITER_MIN: u64 = 16 * 1024 * 1024;
const SEARCH_WRITER_MAX: u64 = 64 * 1024 * 1024;

/// Share and bounds for each time-series table's in-memory head. The head
/// compresses aggressively (Gorilla-style chunks), so a modest cap holds a
/// lot of samples; past the cap the head flushes wholesale.
const TS_HEAD_SHARE: u64 = 8; // budget / 8 per TS table
const TS_HEAD_MIN: u64 = 4 * 1024 * 1024;
const TS_HEAD_MAX: u64 = 256 * 1024 * 1024;

/// Assumed bytes per read-cache entry (key + value + map overhead) when
/// converting the cache's byte share into an entry capacity. Conservative for
/// small rows; a workload with much larger rows should size
/// `read_cache_entries` explicitly instead.
const ASSUMED_ENTRY_BYTES: u64 = 256;

/// Bounds on the derived memtable flush threshold.
const MEMTABLE_MIN: u64 = 16 * 1024 * 1024;
const MEMTABLE_MAX: u64 = 1024 * 1024 * 1024;

/// The storage knobs derived from a memory budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryPlan {
    /// Total storage budget the plan was derived from (bytes).
    pub budget: u64,
    pub memtable_bytes: u64,
    pub read_cache_entries: u64,
    /// Tantivy writer heap per full-text search index (bytes).
    pub search_writer_bytes: u64,
    /// In-memory head cap per time-series table (bytes).
    pub ts_head_bytes: u64,
}

/// Resolve a `memory_target` setting into a plan. Empty/`"off"` → `None`
/// (fixed knobs apply). `"auto"` → half the detected memory limit. Anything
/// else must parse as a size (`"512MB"`, `"1.5GB"`, `"768M"`, plain MB
/// number). Unparseable values are reported as `Err` so a typo fails loudly
/// at startup instead of silently running unbudgeted.
pub fn resolve(target: &str) -> Result<Option<MemoryPlan>, String> {
    let t = target.trim();
    if t.is_empty() || t.eq_ignore_ascii_case("off") {
        return Ok(None);
    }
    let budget = if t.eq_ignore_ascii_case("auto") {
        let limit = detected_memory_limit()
            .ok_or_else(|| "memory_target = \"auto\": no memory limit detectable".to_string())?;
        limit / 2
    } else {
        parse_size(t).ok_or_else(|| format!("memory_target: cannot parse size {t:?}"))?
    };
    Ok(Some(plan(budget)))
}

/// Split a budget into concrete knobs.
fn plan(budget: u64) -> MemoryPlan {
    let memtable_bytes = (budget / MEMTABLE_SHARE).clamp(MEMTABLE_MIN, MEMTABLE_MAX);
    let read_cache_entries = (budget / READ_CACHE_SHARE) / ASSUMED_ENTRY_BYTES;
    let search_writer_bytes =
        (budget / SEARCH_WRITER_SHARE).clamp(SEARCH_WRITER_MIN, SEARCH_WRITER_MAX);
    let ts_head_bytes = (budget / TS_HEAD_SHARE).clamp(TS_HEAD_MIN, TS_HEAD_MAX);
    MemoryPlan {
        budget,
        memtable_bytes,
        read_cache_entries,
        search_writer_bytes,
        ts_head_bytes,
    }
}

/// The node's effective memory limit in bytes: the cgroup limit when one is
/// set (v2 then v1), otherwise total system memory from `/proc/meminfo`.
pub fn detected_memory_limit() -> Option<u64> {
    cgroup_limit().or_else(meminfo_total)
}

fn cgroup_limit() -> Option<u64> {
    // cgroup v2: "max" means unlimited. v1: a huge sentinel (≥ 2^60) means
    // unlimited. Either way fall through to /proc/meminfo.
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(s) = std::fs::read_to_string(path) {
            if let Ok(v) = s.trim().parse::<u64>() {
                if v < 1 << 60 {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn meminfo_total() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = s.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Parse `"512MB"`, `"1.5GB"`, `"768M"`, `"2G"`, or a plain number (MB).
fn parse_size(s: &str) -> Option<u64> {
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(n) = lower.strip_suffix("gb").or_else(|| lower.strip_suffix("g"))
    {
        (n, 1024.0 * 1024.0 * 1024.0)
    } else if let Some(n) = lower.strip_suffix("mb").or_else(|| lower.strip_suffix("m")) {
        (n, 1024.0 * 1024.0)
    } else {
        (lower.as_str(), 1024.0 * 1024.0)
    };
    let v: f64 = num.trim().parse().ok()?;
    if !v.is_finite() || v <= 0.0 {
        return None;
    }
    Some((v * mult) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_and_empty_disable() {
        assert_eq!(resolve("").unwrap(), None);
        assert_eq!(resolve("off").unwrap(), None);
        assert_eq!(resolve("  OFF ").unwrap(), None);
    }

    #[test]
    fn explicit_sizes_parse() {
        let p = resolve("256MB").unwrap().unwrap();
        assert_eq!(p.budget, 256 * 1024 * 1024);
        assert_eq!(p.memtable_bytes, 64 * 1024 * 1024);
        assert_eq!(p.read_cache_entries, (128 * 1024 * 1024) / 256);

        assert_eq!(
            resolve("1GB").unwrap().unwrap().budget,
            1024 * 1024 * 1024
        );
        assert_eq!(
            resolve("1.5g").unwrap().unwrap().budget,
            (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
        // A plain number is MB.
        assert_eq!(resolve("64").unwrap().unwrap().budget, 64 * 1024 * 1024);
    }

    #[test]
    fn small_budgets_clamp_the_memtable() {
        let p = plan(32 * 1024 * 1024);
        assert_eq!(p.memtable_bytes, MEMTABLE_MIN);
    }

    #[test]
    fn garbage_errors() {
        assert!(resolve("lots").is_err());
        assert!(resolve("-5MB").is_err());
    }

    #[test]
    fn auto_detects_something_on_linux() {
        // CI/dev machines always have /proc/meminfo; the budget must be
        // positive and the plan self-consistent.
        let p = resolve("auto").unwrap().unwrap();
        assert!(p.budget > 0);
        assert!(p.memtable_bytes >= MEMTABLE_MIN && p.memtable_bytes <= MEMTABLE_MAX);
    }
}
