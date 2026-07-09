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

#[cfg(test)]
mod tests {
    use super::*;

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
