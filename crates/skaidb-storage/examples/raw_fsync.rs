//! Hardware floor for a single-row durable write: open a small file on the
//! target directory, then loop { write a few bytes; fsync }. No skaidb code
//! at all — this isolates "what does this disk/filesystem cost per fsync"
//! from "what does skaidb's write path cost", so a skaidb-vs-other-engine
//! latency gap can be attributed to one or the other.
//!
//! Run: cargo run --release --example raw_fsync -p skaidb-storage -- [dir] [ops]

use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

fn percentiles(mut v: Vec<u128>) -> (f64, f64, f64, f64) {
    v.sort_unstable();
    let n = v.len().max(1);
    let at = |p: f64| v[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0;
    let avg = v.iter().sum::<u128>() as f64 / n as f64 / 1000.0;
    (avg, at(0.50), at(0.95), at(0.99))
}

fn main() {
    let dir = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("./raw-fsync-data"));
    let ops: usize = std::env::args()
        .nth(2)
        .map_or(2_000, |s| s.parse().unwrap());

    let preallocate = std::env::args().nth(3).as_deref() == Some("prealloc");

    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("fsync_probe.dat");
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    let payload = b"payload-0000000000-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    if preallocate {
        // Grow the file to its final size up front (like a fixed-size WAL
        // segment) and fsync that extension once, then overwrite the same
        // already-allocated bytes every iteration — isolates "pure data
        // flush" cost from "extend file + journal the new metadata" cost.
        f.set_len((payload.len() * ops) as u64).unwrap();
        f.sync_all().unwrap();
        use std::io::{Seek, SeekFrom};
        f.seek(SeekFrom::Start(0)).unwrap();
    }

    let mut lat_write_only = Vec::with_capacity(ops);
    let mut lat_write_fsync = Vec::with_capacity(ops);
    for i in 0..ops {
        if preallocate {
            use std::io::{Seek, SeekFrom};
            f.seek(SeekFrom::Start((i * payload.len()) as u64)).unwrap();
        }
        let t0 = Instant::now();
        f.write_all(payload).unwrap();
        lat_write_only.push(t0.elapsed().as_nanos());

        let t1 = Instant::now();
        f.sync_data().unwrap();
        lat_write_fsync.push(t1.elapsed().as_nanos());
    }

    let (wa, wp50, wp95, wp99) = percentiles(lat_write_only);
    let (sa, sp50, sp95, sp99) = percentiles(lat_write_fsync);
    println!("dir: {}", dir.display());
    println!(
        "write() only:        avg {wa:.3}us p50 {wp50:.3}us p95 {wp95:.3}us p99 {wp99:.3}us"
    );
    println!(
        "sync_data() only:    avg {sa:.3}us p50 {sp50:.3}us p95 {sp95:.3}us p99 {sp99:.3}us"
    );
    println!("write+fsync total avg: {:.3}us", wa + sa);

    let _ = std::fs::remove_file(&path);
}
