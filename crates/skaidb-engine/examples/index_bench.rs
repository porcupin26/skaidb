//! Synthetic benchmark for secondary-index acceleration on the embedded engine.
//!
//! Measures read latency for several query shapes **without** an index (full
//! scan) and then **with** the matching index, plus write throughput with and
//! without index maintenance, on a large dataset.
//!
//!   cargo run --release --example index_bench -p skaidb-engine -- <data_dir> [rows] [pad_bytes]
//!
//! Rows look like `{ id, v, g, pad }` where `v` is high-cardinality (range /
//! equality / ORDER BY), `g` is low-cardinality (composite leading column), and
//! `pad` inflates row size so the dataset occupies realistic space on disk.

use std::time::Instant;

use skaidb_engine::{Database, QueryOutput};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| "/tmp/skaidb-index-bench".to_string());
    let rows: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(500_000);
    let pad: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(120);
    let _ = std::fs::remove_dir_all(&dir);

    println!("dataset: {rows} rows, ~{pad}B padding/row, dir={dir}\n");

    let mut db = Database::open(&dir).expect("open");
    db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

    // Phase 1: bulk-load WITHOUT indexes; record write throughput.
    let t = Instant::now();
    load(&mut db, 0, rows, pad);
    let load_secs = t.elapsed().as_secs_f64();
    println!(
        "load {rows} rows, no index:   {:>8.1} rows/s   ({:.1}s)",
        rows as f64 / load_secs,
        load_secs
    );

    // Queries while there is NO index (full scans).
    let lo = rows / 2;
    let hi = lo + rows / 100; // ~1% value window
    let eq = rows / 3;
    let q_eq = format!("SELECT id FROM t WHERE v = {eq}");
    let q_range = format!("SELECT id FROM t WHERE v >= {lo} AND v < {hi}");
    let q_topn = "SELECT id FROM t ORDER BY v LIMIT 10".to_string();
    let q_comp = format!("SELECT id FROM t WHERE g = 3 AND v >= {lo} AND v < {hi}");

    println!("\nquery latencies (best of 3):");
    let base_eq = bench(&mut db, &q_eq);
    let base_range = bench(&mut db, &q_range);
    let base_topn = bench(&mut db, &q_topn);
    let base_comp = bench(&mut db, &q_comp);

    // Build the indexes.
    let t = Instant::now();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    let v_build = t.elapsed().as_secs_f64();
    let t = Instant::now();
    db.execute("CREATE INDEX idx_gv ON t(g, v)").unwrap();
    let gv_build = t.elapsed().as_secs_f64();
    println!("\nindex build: idx_v {v_build:.1}s, idx_gv(g,v) {gv_build:.1}s");

    // Same queries, now index-accelerated.
    let idx_eq = bench(&mut db, &q_eq);
    let idx_range = bench(&mut db, &q_range);
    let idx_topn = bench(&mut db, &q_topn);
    let idx_comp = bench(&mut db, &q_comp);

    // Phase 2: load more rows WITH both indexes present; record throughput.
    let more = rows / 4;
    let t = Instant::now();
    load(&mut db, rows, more, pad);
    let load2_secs = t.elapsed().as_secs_f64();

    // Report.
    println!("\n{:<28} {:>10} {:>10} {:>9} {:>8}", "query", "no-index", "indexed", "speedup", "rows");
    row("equality  v = X", base_eq, idx_eq);
    row("range     v in 1%", base_range, idx_range);
    row("top-N     ORDER BY v LIMIT 10", base_topn, idx_topn);
    row("composite g= AND v in 1%", base_comp, idx_comp);

    println!("\nwrite throughput:");
    println!("  no index:    {:>9.1} rows/s", rows as f64 / load_secs);
    println!(
        "  2 indexes:   {:>9.1} rows/s   ({:.0}% of no-index)",
        more as f64 / load2_secs,
        (more as f64 / load2_secs) / (rows as f64 / load_secs) * 100.0
    );

    if let Some(disk) = disk_usage(&dir) {
        println!("\non-disk size: {disk}");
    }
}

/// Insert `count` rows starting at `start`, batched to keep statement overhead
/// low. `v` is pseudo-random (high cardinality), `g = id % 10`.
fn load(db: &mut Database, start: u64, count: u64, pad: usize) {
    let padding = "x".repeat(pad);
    let batch = 500u64;
    let mut id = start;
    let end = start + count;
    while id < end {
        let mut sql = String::from("INSERT INTO t (id, v, g, pad) VALUES ");
        let n = batch.min(end - id);
        for j in 0..n {
            let cur = id + j;
            let v = splitmix(cur) % (count.max(1));
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({cur}, {v}, {}, '{padding}')", cur % 10));
        }
        db.execute(&sql).unwrap();
        id += n;
    }
}

/// Best-of-3 wall time of a query, in milliseconds; also returns the row count.
fn bench(db: &mut Database, sql: &str) -> (f64, usize) {
    let mut best = f64::INFINITY;
    let mut rows = 0;
    for _ in 0..3 {
        let t = Instant::now();
        let out = db.execute(sql).unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        best = best.min(ms);
        rows = match out {
            QueryOutput::Rows(rs) => rs.rows.len(),
            _ => 0,
        };
    }
    (best, rows)
}

fn row(label: &str, base: (f64, usize), idx: (f64, usize)) {
    let speedup = if idx.0 > 0.0 { base.0 / idx.0 } else { 0.0 };
    println!(
        "{:<28} {:>8.2}ms {:>8.2}ms {:>8.1}x {:>8}",
        label, base.0, idx.0, speedup, idx.1
    );
}

/// A fast deterministic hash (SplitMix64) for reproducible pseudo-random values.
fn splitmix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn disk_usage(dir: &str) -> Option<String> {
    let out = std::process::Command::new("du").args(["-sh", dir]).output().ok()?;
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
