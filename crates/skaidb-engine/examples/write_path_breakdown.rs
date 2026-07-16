//! Layered per-operation timing breakdown for the single-row durable INSERT
//! path, mirroring read_path_breakdown.rs. Unlike reads, this path's cost is
//! expected to be dominated by a real disk fsync, so **the target directory
//! matters** — pass a path on the same filesystem/device as the production
//! data_dir (default here is a fixed subdir, not std::temp_dir(), since /tmp
//! is tmpfs on some hosts and would silently measure RAM instead of disk).
//!
//!   stage A: SQL parse alone (skaidb_sql::parse)
//!   stage B: parse + bind + dispatch + WAL append + fsync + memtable insert,
//!            in-process, no network (Session::execute) — the real floor,
//!            fsync included.
//!
//! Run: cargo run --release --example write_path_breakdown -p skaidb-engine -- [dir] [ops]

use std::time::Instant;

use skaidb_engine::Session;

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
        .unwrap_or_else(|| std::path::PathBuf::from("./write-path-bd-data"));
    let ops: usize = std::env::args()
        .nth(2)
        .map_or(2_000, |s| s.parse().unwrap());

    let _ = std::fs::remove_dir_all(&dir);
    let mut session = Session::open(&dir).unwrap();
    session.execute("CREATE TABLE bench (PRIMARY KEY (id))").unwrap();

    // Stage A: parse only — one fresh unique-key INSERT per op, matching the
    // C0 write-1c workload's literal-SQL shape.
    let mut a = Vec::with_capacity(ops);
    for i in 0..ops {
        let sql = format!("INSERT INTO bench (id, v) VALUES ({i}, 'payload-{i}')");
        let t0 = Instant::now();
        std::hint::black_box(skaidb_sql::parse(&sql).unwrap());
        a.push(t0.elapsed().as_nanos());
    }

    // Stage B: parse + bind + dispatch + WAL append + fsync + memtable
    // insert, in-process, zero network — real disk fsync included since
    // `dir` is a real path, not tmpfs.
    let mut b = Vec::with_capacity(ops);
    for i in 0..ops {
        let id = ops + i; // fresh keys, no overwrite of stage A's rows
        let sql = format!("INSERT INTO bench (id, v) VALUES ({id}, 'payload-{id}')");
        let t0 = Instant::now();
        std::hint::black_box(session.execute(&sql).unwrap());
        b.push(t0.elapsed().as_nanos());
    }

    let (a_avg, a_p50, a_p95, a_p99) = percentiles(a);
    let (b_avg, b_p50, b_p95, b_p99) = percentiles(b);
    println!("dir: {}", dir.display());
    println!("stage A (parse only):                    avg {a_avg:.3}us p50 {a_p50:.3}us p95 {a_p95:.3}us p99 {a_p99:.3}us");
    println!("stage B (parse+bind+dispatch+WAL+fsync):  avg {b_avg:.3}us p50 {b_p50:.3}us p95 {b_p95:.3}us p99 {b_p99:.3}us");
    println!("B - A (bind+dispatch+WAL+fsync only):     avg {:.3}us", b_avg - a_avg);

    let _ = std::fs::remove_dir_all(&dir);
}
