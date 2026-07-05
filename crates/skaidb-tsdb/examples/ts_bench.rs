//! Phase-1 exit-criteria bench: ingest rate, bytes/sample, replay, query.
//!
//! ```sh
//! cargo run --release --example ts_bench -p skaidb-tsdb -- <dir> [series] [steps]
//! ```
//!
//! Simulates a scrape workload: `series` time series sampled every 15 s for
//! `steps` rounds (half counters, half random-walk gauges), appended one
//! scrape round per batch (one WAL fsync per round, like a real scraper).

use std::time::Instant;

use skaidb_tsdb::{Labels, Matcher, Tsdb, TsdbOptions};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = std::path::PathBuf::from(
        args.next()
            .expect("usage: ts_bench <dir> [series] [steps] [mix|counter|gauge|idle]"),
    );
    let nseries: usize = args.next().map_or(10_000, |s| s.parse().unwrap());
    let steps: usize = args.next().map_or(1_000, |s| s.parse().unwrap());
    let pattern = args.next().unwrap_or_else(|| "mix".into());
    let total = nseries * steps;
    let _ = std::fs::remove_dir_all(&dir);

    let db = Tsdb::open(&dir, TsdbOptions::default()).expect("open");

    // Pre-build label sets (a scraper knows its targets).
    let series: Vec<Labels> = (0..nseries)
        .map(|i| {
            vec![
                ("__name__".into(), if i % 2 == 0 { "http_requests_total".into() } else { "mem_used_ratio".into() }),
                ("host".into(), format!("host-{:04}", i / 8)),
                ("core".into(), format!("{}", i % 8)),
            ]
        })
        .collect();

    let mut lcg = 0x2545F4914F6CDD1Du64;
    let mut next = move || {
        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        lcg >> 33
    };
    let mut values: Vec<f64> = (0..nseries).map(|i| (i % 100) as f64).collect();

    eprintln!("ingesting {total} samples ({nseries} series x {steps} steps, 15s interval)...");
    let t0 = Instant::now();
    let base_ts = 1_700_000_000_000i64;
    let mut batch: Vec<(Labels, i64, f64)> = Vec::with_capacity(nseries);
    for step in 0..steps {
        let ts = base_ts + (step as i64) * 15_000;
        batch.clear();
        for (i, labels) in series.iter().enumerate() {
            let kind = match pattern.as_str() {
                "counter" => 0,
                "gauge" => 1,
                "idle" => 2,
                _ => i % 3, // mix
            };
            match kind {
                // Counter: integer increments, often zero (idle handlers).
                0 => values[i] += (next() % 4) as f64,
                // Active gauge: random walk, full mantissa churn (worst case).
                1 => values[i] += (next() % 100) as f64 / 1000.0 - 0.05,
                // Mostly-idle gauge: real fleets are full of these.
                _ => {
                    if next() % 20 == 0 {
                        values[i] = (next() % 1000) as f64 / 10.0;
                    }
                }
            }
            batch.push((labels.clone(), ts, values[i]));
        }
        let res = db.append_batch(&batch).expect("append");
        assert_eq!(res.appended, nseries);
    }
    let ingest_secs = t0.elapsed().as_secs_f64();
    println!(
        "ingest: {:.0} samples/s ({total} samples in {ingest_secs:.1}s, one fsync per scrape round)",
        total as f64 / ingest_secs
    );

    let t = Instant::now();
    db.flush().expect("flush");
    println!("final flush: {:.2}s", t.elapsed().as_secs_f64());

    let stats = db.stats();
    println!(
        "disk: {} bytes for {} samples = {:.2} bytes/sample ({} blocks)",
        stats.disk_bytes,
        total,
        stats.disk_bytes as f64 / total as f64,
        stats.blocks,
    );

    // Query: one host's series (8 cores x 2 metrics... host-0000 covers ids 0..8),
    // full time range.
    let t = Instant::now();
    let hits = db
        .query(
            &[Matcher::Eq("host".into(), "host-0000".into())],
            0,
            i64::MAX,
        )
        .expect("query");
    let got: usize = hits.iter().map(|(_, s)| s.len()).sum();
    println!(
        "query host-0000 (all time): {} series, {} samples in {:.1} ms",
        hits.len(),
        got,
        t.elapsed().as_secs_f64() * 1000.0
    );

    // Reopen: WAL replay + block scan.
    drop(db);
    let t = Instant::now();
    let db = Tsdb::open(&dir, TsdbOptions::default()).expect("reopen");
    println!("reopen (replay): {:.2}s", t.elapsed().as_secs_f64());
    let check = db.query(&[Matcher::Eq("host".into(), "host-0000".into())], 0, i64::MAX).unwrap();
    let re_got: usize = check.iter().map(|(_, s)| s.len()).sum();
    assert_eq!(re_got, got, "sample count must survive reopen");
    println!("reopen integrity: {re_got} samples match");

    let _ = std::fs::remove_dir_all(&dir);
}
