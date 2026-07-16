//! Layered per-operation timing breakdown for the point-read path, without
//! perf (unavailable in sandboxed environments). Times each stage in
//! isolation so the costs add up to an honest picture of where time goes:
//!
//!   stage A: SQL parse alone (skaidb_sql::parse)
//!   stage B: parse + bind + dispatch + engine lookup + result build,
//!            in-process, no network (Session::execute)
//!   stage C (not here — see bench.rs): the same op over a real loopback
//!            TCP connection, which additionally pays protocol encode/decode
//!            + socket I/O + thread wakeup.
//!
//! B minus A ≈ bind+dispatch+engine cost. Run bench.rs single-threaded on
//! loopback against a real server for stage C; C minus B ≈ wire/protocol
//! overhead.
//!
//! Run: cargo run --release --example read_path_breakdown -p skaidb-engine -- [ops]

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
    let ops: usize = std::env::args()
        .nth(1)
        .map_or(200_000, |s| s.parse().unwrap());

    let dir = std::env::temp_dir().join(format!("read-path-bd-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut session = Session::open(&dir).unwrap();
    session.execute("CREATE TABLE bench (PRIMARY KEY (id))").unwrap();
    for i in 0..1000 {
        session
            .execute(&format!("INSERT INTO bench (id, v) VALUES ({i}, 'payload-{i}')"))
            .unwrap();
    }
    session.execute("SELECT count(*) FROM bench").unwrap(); // force any flush bookkeeping to settle

    // Stage A: parse only, no execution at all — same literal SQL text the
    // C1-C4 `read` mode builds fresh every call (bench.rs:169), so this is
    // an apples-to-apples parse cost for that exact workload.
    let mut a = Vec::with_capacity(ops);
    for i in 0..ops {
        let sql = format!("SELECT v FROM bench WHERE id = {}", i % 1000);
        let t0 = Instant::now();
        std::hint::black_box(skaidb_sql::parse(&sql).unwrap());
        a.push(t0.elapsed().as_nanos());
    }

    // Stage B: parse + bind + dispatch + engine lookup + result materialize,
    // in-process, zero network — the ceiling on how fast a wire round-trip
    // could ever be for this op.
    let mut b = Vec::with_capacity(ops);
    for i in 0..ops {
        let sql = format!("SELECT v FROM bench WHERE id = {}", i % 1000);
        let t0 = Instant::now();
        std::hint::black_box(session.execute(&sql).unwrap());
        b.push(t0.elapsed().as_nanos());
    }

    let (a_avg, a_p50, a_p95, a_p99) = percentiles(a);
    let (b_avg, b_p50, b_p95, b_p99) = percentiles(b);
    println!("stage A (parse only):              avg {a_avg:.3}us p50 {a_p50:.3}us p95 {a_p95:.3}us p99 {a_p99:.3}us");
    println!("stage B (parse+bind+dispatch+get):  avg {b_avg:.3}us p50 {b_p50:.3}us p95 {b_p95:.3}us p99 {b_p99:.3}us");
    println!("B - A (bind+dispatch+engine only):  avg {:.3}us", b_avg - a_avg);

    let _ = std::fs::remove_dir_all(&dir);
}
