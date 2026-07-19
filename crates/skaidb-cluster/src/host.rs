//! Host system statistics (CPU, memory, disk IO, disk space) for the UI's
//! per-node view and the `/metrics` exporter. Read straight from `/proc`
//! plus one `statvfs` on the data directory — no external collectors.
//!
//! CPU utilisation and disk throughput are *rates*, which need two samples:
//! [`sample`] keeps the previous reading in a process-wide cache and reports
//! the rate over the window since the last call. Calls closer together than
//! [`MIN_WINDOW_MS`] return the previously computed rates instead of
//! re-basing on a noisy sub-second window (several UI sessions polling
//! concurrently would otherwise thrash the baseline).

use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// One node's host statistics. All byte values are bytes; rates are per
/// second over the sampling window. Serialized as JSON on the internode
/// wire so fields can grow without a wire change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostStats {
    /// Busy CPU as a percentage of total capacity across all cores
    /// (0–100), over the last sampling window. 0.0 until two samples exist.
    pub cpu_percent: f64,
    /// Logical CPU count.
    pub cpus: u32,
    /// 1-minute load average.
    pub load1: f64,
    /// Total memory: the cgroup limit when one applies (container-correct),
    /// else `MemTotal`.
    pub mem_total_bytes: u64,
    /// Used memory: cgroup `memory.current` when limited, else
    /// `MemTotal - MemAvailable`.
    pub mem_used_bytes: u64,
    /// This process's resident set size.
    pub rss_bytes: u64,
    /// Whole-host disk reads/writes since boot (sectors × 512, summed over
    /// physical block devices — partitions and loop/ram devices excluded).
    pub disk_read_bytes_total: u64,
    pub disk_written_bytes_total: u64,
    /// Disk throughput over the last sampling window.
    pub disk_read_bps: f64,
    pub disk_write_bps: f64,
    /// The filesystem holding the data directory.
    pub disk_total_bytes: u64,
    pub disk_available_bytes: u64,
    /// CPU pressure (PSI `some avg10`, %): share of the last 10s in which at
    /// least one task stalled waiting for CPU — saturation, not just usage.
    #[serde(default)]
    pub cpu_pressure_pct: f64,
    /// Seconds since this node's process started.
    #[serde(default)]
    pub uptime_secs: u64,
    /// Process starts since the data directory was created (persisted
    /// counter), minus the first — i.e. restarts.
    #[serde(default)]
    pub restarts: u64,
    /// Kernel OOM kills observed in this node's cgroup across its lifetime
    /// (why a node restarted, when the cause was memory).
    #[serde(default)]
    pub oom_kills: u64,
    /// Cgroup anon vs file byte split (0 outside cgroup v2) — the ramp
    /// signature of the production memory wedges was anon growing while
    /// file collapsed toward zero.
    #[serde(default)]
    pub mem_anon_bytes: u64,
    #[serde(default)]
    pub mem_file_bytes: u64,
    /// jemalloc live heap / resident pages / OS-unreturned address space
    /// (0 without an allocator hook). `alloc_resident - alloc_allocated` ≈
    /// fragmentation + unpurged dirty pages: the live-vs-allocator split the
    /// OOM post-mortems lacked.
    #[serde(default)]
    pub alloc_allocated_bytes: u64,
    #[serde(default)]
    pub alloc_resident_bytes: u64,
    #[serde(default)]
    pub alloc_retained_bytes: u64,
    /// Whether an anti-entropy repair pass is currently running on this
    /// node. Peers defer their own pass while one runs anywhere — two
    /// concurrent passes (paced or not) were enough to dent write quorum.
    #[serde(default)]
    pub repairing: bool,
    /// This node's on-disk data footprint (SSTable bytes) — the numerator of
    /// filesize-based resync progress, and the denominator peers read to size
    /// their own resync. 0 if unavailable (engine lock contended).
    #[serde(default)]
    pub data_dir_bytes: u64,
    /// Whether this node is backfilling from an empty data directory after a
    /// (re)join — it holds INCOMPLETE data, so it does not serve scans/counts
    /// locally and the UI/drivers should route around it until it converges.
    #[serde(default)]
    pub resyncing: bool,
    /// Resync completion in `0.0..=1.0` (this node's data bytes over the
    /// largest peer's, captured at resync start). 1.0 when not resyncing.
    #[serde(default)]
    pub resync_progress: f64,
    /// Set by a coordinator when this snapshot was served from cache because
    /// the node missed a probe: seconds since it last answered. 0 = fresh.
    #[serde(default)]
    pub stale_secs: u64,
}

/// Baseline for rate computation, plus the rates computed at the last
/// re-basing (returned verbatim for calls inside the minimum window).
struct Prev {
    at_ms: u64,
    cpu_busy: u64,
    cpu_total: u64,
    disk_read: u64,
    disk_written: u64,
    cpu_percent: f64,
    disk_read_bps: f64,
    disk_write_bps: f64,
}

static PREV: Mutex<Option<Prev>> = Mutex::new(None);

/// Shortest window rates are computed over.
const MIN_WINDOW_MS: u64 = 1_000;

/// Take a host-stats sample, computing CPU/disk rates against the previous
/// call. Values that cannot be read (non-Linux, masked `/proc`) stay 0
/// rather than failing the whole sample.
pub fn sample(data_dir: &Path) -> HostStats {
    let mut s = HostStats {
        cpus: std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(0),
        load1: loadavg1().unwrap_or(0.0),
        rss_bytes: self_rss().unwrap_or(0),
        ..HostStats::default()
    };
    let (mem_total, mem_used) = memory().unwrap_or((0, 0));
    s.mem_total_bytes = mem_total;
    s.mem_used_bytes = mem_used;
    s.cpu_pressure_pct = psi_cpu_some_avg10().unwrap_or(0.0);
    s.uptime_secs = process_uptime_secs().unwrap_or(0);
    s.restarts = read_runtime_counter(data_dir, "starts").saturating_sub(1);
    s.oom_kills = cgroup_oom_kills().unwrap_or(0);
    if let Some((anon, file)) = crate::memguard::anon_file_breakdown() {
        s.mem_anon_bytes = anon;
        s.mem_file_bytes = file;
    }
    if let Some(a) = crate::memguard::alloc_numbers() {
        s.alloc_allocated_bytes = a.allocated;
        s.alloc_resident_bytes = a.resident;
        s.alloc_retained_bytes = a.retained;
    }
    if let Some((total, avail)) = fs_space(data_dir) {
        s.disk_total_bytes = total;
        s.disk_available_bytes = avail;
    }
    let (busy, total) = cpu_jiffies().unwrap_or((0, 0));
    let (read, written) = disk_totals().unwrap_or((0, 0));
    s.disk_read_bytes_total = read;
    s.disk_written_bytes_total = written;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut prev = PREV.lock().unwrap_or_else(|p| p.into_inner());
    match prev.as_mut() {
        Some(p) if now_ms.saturating_sub(p.at_ms) < MIN_WINDOW_MS => {
            // Too soon to re-base: reuse the last computed rates.
            s.cpu_percent = p.cpu_percent;
            s.disk_read_bps = p.disk_read_bps;
            s.disk_write_bps = p.disk_write_bps;
        }
        Some(p) => {
            let dt = (now_ms - p.at_ms) as f64 / 1000.0;
            let dtotal = total.saturating_sub(p.cpu_total);
            if dtotal > 0 {
                s.cpu_percent =
                    100.0 * busy.saturating_sub(p.cpu_busy) as f64 / dtotal as f64;
            }
            s.disk_read_bps = read.saturating_sub(p.disk_read) as f64 / dt;
            s.disk_write_bps = written.saturating_sub(p.disk_written) as f64 / dt;
            *p = Prev {
                at_ms: now_ms,
                cpu_busy: busy,
                cpu_total: total,
                disk_read: read,
                disk_written: written,
                cpu_percent: s.cpu_percent,
                disk_read_bps: s.disk_read_bps,
                disk_write_bps: s.disk_write_bps,
            };
        }
        None => {
            *prev = Some(Prev {
                at_ms: now_ms,
                cpu_busy: busy,
                cpu_total: total,
                disk_read: read,
                disk_written: written,
                cpu_percent: 0.0,
                disk_read_bps: 0.0,
                disk_write_bps: 0.0,
            });
        }
    }
    s
}

/// `(busy, total)` jiffies from the aggregate `cpu` line of `/proc/stat`.
/// Busy excludes idle and iowait.
fn cpu_jiffies() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let line = stat.lines().find(|l| l.starts_with("cpu "))?;
    let fields: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|f| f.parse().ok())
        .collect();
    if fields.len() < 5 {
        return None;
    }
    let total: u64 = fields.iter().sum();
    let idle = fields[3] + fields.get(4).copied().unwrap_or(0); // idle + iowait
    Some((total - idle, total))
}

fn loadavg1() -> Option<f64> {
    std::fs::read_to_string("/proc/loadavg")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// `(total, used)` memory. Prefers the cgroup view when a limit applies —
/// a container's own allowance, not the host's — else `/proc/meminfo`.
fn memory() -> Option<(u64, u64)> {
    if let (Some(limit), Some(current)) = (cgroup_read("memory.max"), cgroup_read("memory.current"))
    {
        if limit < 1 << 60 {
            return Some((limit, current));
        }
    }
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    let field = |name: &str| -> Option<u64> {
        let line = s.lines().find(|l| l.starts_with(name))?;
        Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
    };
    let total = field("MemTotal:")?;
    let avail = field("MemAvailable:").unwrap_or(0);
    Some((total, total.saturating_sub(avail)))
}

fn cgroup_read(file: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/sys/fs/cgroup/{file}"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// This process's resident set size from `/proc/self/statm` (pages × page
/// size; the kernel page size is 4096 on every platform skaidb targets).
fn self_rss() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

/// Whole-host `(read, written)` bytes since boot, summed over physical
/// block devices from `/proc/diskstats`. Partitions are skipped (their IO
/// is already counted by the parent device), as are loop/ram/zram/dm
/// virtual devices (dm would double-count its backing device).
fn disk_totals() -> Option<(u64, u64)> {
    let stats = std::fs::read_to_string("/proc/diskstats").ok()?;
    let mut read = 0u64;
    let mut written = 0u64;
    for line in stats.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 10 {
            continue;
        }
        let name = f[2];
        if !is_physical_disk(name) {
            continue;
        }
        read += f[5].parse::<u64>().unwrap_or(0) * 512;
        written += f[9].parse::<u64>().unwrap_or(0) * 512;
    }
    Some((read, written))
}

/// Whether a `/proc/diskstats` device name is a whole physical disk.
fn is_physical_disk(name: &str) -> bool {
    for skip in ["loop", "ram", "zram", "dm-", "sr", "fd", "md"] {
        if name.starts_with(skip) {
            return false;
        }
    }
    // nvme0n1p2 / mmcblk0p1 are partitions; nvme0n1 / mmcblk0 are disks.
    if name.starts_with("nvme") || name.starts_with("mmcblk") {
        return !name.contains('p');
    }
    // sda1 / vdb2 / xvda1 are partitions; sda / vdb are disks.
    !name.ends_with(|c: char| c.is_ascii_digit())
}

/// `(total, available)` bytes of the filesystem holding `dir`, via
/// POSIX `df -P -k` (identical output shape on coreutils and busybox;
/// `statvfs` directly would need `unsafe`, which the workspace forbids).
fn fs_space(dir: &Path) -> Option<(u64, u64)> {
    let out = std::process::Command::new("df")
        .arg("-P")
        .arg("-k")
        .arg(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // "Filesystem 1024-blocks Used Available Capacity Mounted on"
    let fields: Vec<&str> = text.lines().nth(1)?.split_whitespace().collect();
    let total: u64 = fields.get(1)?.parse().ok()?;
    let avail: u64 = fields.get(3)?.parse().ok()?;
    Some((total * 1024, avail * 1024))
}

/// Seconds since this process started (`/proc/self/stat` field 22, jiffies
/// since boot, against `/proc/uptime`). `None` off Linux.
fn process_uptime_secs() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Field 2 (comm) may contain spaces — parse after the closing paren.
    let after = &stat[stat.rfind(')')? + 2..];
    let start_jiffies: u64 = after.split_whitespace().nth(19)?.parse().ok()?;
    let uptime: f64 = std::fs::read_to_string("/proc/uptime")
        .ok()?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    let hz = 100.0; // USER_HZ; universal on the targets we ship
    Some((uptime - start_jiffies as f64 / hz).max(0.0) as u64)
}

/// CPU pressure: PSI `some avg10` from `/proc/pressure/cpu` (Linux 4.20+).
fn psi_cpu_some_avg10() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/pressure/cpu").ok()?;
    let line = s.lines().find(|l| l.starts_with("some"))?;
    line.split_whitespace()
        .find_map(|f| f.strip_prefix("avg10="))
        .and_then(|v| v.parse().ok())
}

/// Kernel OOM kills in this cgroup (cgroup v2 `memory.events`).
fn cgroup_oom_kills() -> Option<u64> {
    let s = std::fs::read_to_string("/sys/fs/cgroup/memory.events").ok()?;
    s.lines()
        .find_map(|l| l.strip_prefix("oom_kill "))
        .and_then(|v| v.trim().parse().ok())
}

/// Read a persisted runtime counter (`<data_dir>/runtime/<name>`), 0 if absent.
pub fn read_runtime_counter(data_dir: &Path, name: &str) -> u64 {
    std::fs::read_to_string(data_dir.join("runtime").join(name))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// Record a process start in `<data_dir>/runtime/`: bump the persistent
/// `starts` counter and diff the cgroup's lifetime OOM-kill count against the
/// value saved at the previous start. Returns `(starts, new_oom_kills)` —
/// a nonzero second value means the previous run (or its container peers)
/// died to the kernel OOM killer since the last start, which is the reason
/// worth logging for an unexplained restart.
pub fn record_start(data_dir: &Path) -> (u64, u64) {
    let dir = data_dir.join("runtime");
    let _ = std::fs::create_dir_all(&dir);
    let starts = read_runtime_counter(data_dir, "starts") + 1;
    let _ = std::fs::write(dir.join("starts"), starts.to_string());
    let ooms_now = cgroup_oom_kills().unwrap_or(0);
    let ooms_prev = read_runtime_counter(data_dir, "oom_kills_at_start");
    let _ = std::fs::write(dir.join("oom_kills_at_start"), ooms_now.to_string());
    (starts, ooms_now.saturating_sub(ooms_prev))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_start_counts_and_detects_oom_delta() {
        let dir = std::env::temp_dir().join(format!("skaidb-runtime-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (starts1, ooms1) = record_start(&dir);
        assert_eq!(starts1, 1);
        let _ = ooms1; // depends on the host's cgroup — just must not panic
        let (starts2, ooms2) = record_start(&dir);
        assert_eq!(starts2, 2);
        // Same boot, no new OOM kills between the two calls.
        assert_eq!(ooms2, 0);
        assert_eq!(read_runtime_counter(&dir, "starts"), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sample_reads_something() {
        let s = sample(Path::new("/tmp"));
        assert!(s.cpus > 0);
        assert!(s.mem_total_bytes > 0, "meminfo/cgroup total");
        assert!(s.rss_bytes > 0, "own RSS");
        assert!(s.disk_total_bytes > 0, "statvfs on /tmp");
        assert!(s.disk_available_bytes <= s.disk_total_bytes);
        // Second sample after the baseline: rates defined (possibly 0.0).
        let s2 = sample(Path::new("/tmp"));
        assert!((0.0..=100.5).contains(&s2.cpu_percent));
    }

    #[test]
    fn partition_names_are_skipped() {
        assert!(is_physical_disk("sda"));
        assert!(is_physical_disk("vdb"));
        assert!(is_physical_disk("nvme0n1"));
        assert!(is_physical_disk("mmcblk0"));
        assert!(!is_physical_disk("sda1"));
        assert!(!is_physical_disk("nvme0n1p2"));
        assert!(!is_physical_disk("mmcblk0p1"));
        assert!(!is_physical_disk("loop3"));
        assert!(!is_physical_disk("dm-0"));
        assert!(!is_physical_disk("md0"));
    }
}
