//! Simple load generator for a skaidb node/cluster over the binary protocol.
//!
//! Usage:
//!   bench <addr> <user> <pass> <mode> <ops> <threads> [preload]
//!   mode = write | read | mixed          (one-shot SQL text per op)
//!        | writep | readp | mixedp       (prepared statements + bind params)
//!   preload = N or NxS — N rows, each with an S-byte value (default value is
//!   the short "payload-<id>"). Large preloads load in multi-row INSERT
//!   batches and settle for compactions before the timed phase.
//!
//! Each thread holds one authenticated connection (handshake once) and issues
//! operations in a loop. Reports throughput and latency percentiles.

use std::sync::Arc;
use std::time::Instant;

use skaidb_driver::Client;
use skaidb_proto::Response;
use skaidb_types::Value;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        eprintln!("usage: bench <addr> <user> <pass> <write|read|mixed> <ops> <threads> [preload]");
        std::process::exit(2);
    }
    // First arg may be a comma-separated list of node addresses; threads are
    // spread across them round-robin (leaderless: any node coordinates).
    let addrs: Vec<String> = args[1].split(',').map(|s| s.trim().to_string()).collect();
    let user = args[2].clone();
    let pass = args[3].clone();
    let mode = args[4].clone();
    let ops: usize = args[5].parse().expect("ops");
    let threads: usize = args[6].parse().expect("threads");
    let (preload, valsize) = parse_preload(args.get(7).map(String::as_str));
    // READ_SPAN caps the id range reads draw from (default: all preloaded
    // rows). A span smaller than the preload models a hot working set over a
    // large table — the shape that distinguishes cache configurations.
    let read_span: usize = std::env::var("READ_SPAN")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|s| *s > 0)
        .unwrap_or(preload)
        .min(preload.max(1));

    let cfg = Arc::new((addrs, user, pass));

    // Fresh table; preload rows for read/mixed.
    {
        let mut c = connect(&cfg, 0);
        let _ = c.execute("DROP TABLE IF EXISTS bench");
        run_ok(&mut c, "CREATE TABLE bench (PRIMARY KEY (id))");
        if mode != "write" && mode != "writep" {
            // Multi-row INSERT batches: one statement (and one replication
            // round per replica group) per batch instead of per row.
            const BATCH: usize = 500;
            let mut id = 0;
            while id < preload {
                let end = (id + BATCH).min(preload);
                let mut sql = String::with_capacity(64 + (end - id) * (valsize + 24));
                sql.push_str("INSERT INTO bench (id, v) VALUES ");
                for (n, i) in (id..end).enumerate() {
                    if n > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!("({i}, '{}')", padded_payload(i, valsize)));
                }
                run_ok(&mut c, &sql);
                id = end;
            }
            println!("preloaded {preload} rows (value ~{valsize}B)");
            if preload >= 100_000 {
                // Let flushes/compactions from the load quiesce so the timed
                // phase measures steady-state reads, not compaction overlap.
                println!("settling 10s after large preload…");
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
        }
    }

    let per_thread = ops / threads;
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..threads {
        let cfg = Arc::clone(&cfg);
        let mode = mode.clone();
        handles.push(std::thread::spawn(move || {
            let mut client = connect(&cfg, t);
            let mut lat = Vec::with_capacity(per_thread);
            let mut errors = 0u64;
            let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (t as u64).wrapping_mul(0x2545_f491_4f6c_dd1d);
            if let Some(base) = mode.strip_suffix('p') {
                // Prepared path: parse once per statement shape, then bind
                // parameters per op.
                let mut ins = client
                    .prepare("INSERT INTO bench (id, v) VALUES (?, ?)")
                    .expect("prepare insert");
                let mut sel = client
                    .prepare("SELECT v FROM bench WHERE id = ?")
                    .expect("prepare select");
                for i in 0..per_thread {
                    let (read, id) = pick_op(base, t, i, preload, read_span, &mut rng);
                    let op_start = Instant::now();
                    let result = if read {
                        client.execute_prepared(&mut sel, &[Value::Int(id)])
                    } else {
                        client.execute_prepared(
                            &mut ins,
                            &[Value::Int(id), Value::String(format!("payload-{id}"))],
                        )
                    };
                    match result {
                        Ok(Response::Error(_)) | Err(_) => errors += 1,
                        Ok(_) => {}
                    }
                    lat.push(op_start.elapsed().as_micros() as u64);
                }
            } else {
                for i in 0..per_thread {
                    let sql = build_op(&mode, t, i, preload, read_span, &mut rng);
                    let op_start = Instant::now();
                    match client.execute(&sql) {
                        Ok(Response::Error(_)) | Err(_) => errors += 1,
                        Ok(_) => {}
                    }
                    lat.push(op_start.elapsed().as_micros() as u64);
                }
            }
            (lat, errors)
        }));
    }

    let mut all_lat = Vec::with_capacity(ops);
    let mut errors = 0u64;
    for h in handles {
        let (lat, e) = h.join().unwrap();
        all_lat.extend(lat);
        errors += e;
    }
    let elapsed = start.elapsed().as_secs_f64();

    all_lat.sort_unstable();
    let n = all_lat.len().max(1);
    let pct = |p: f64| all_lat[((n as f64 * p) as usize).min(n - 1)] as f64 / 1000.0;
    let avg = all_lat.iter().sum::<u64>() as f64 / n as f64 / 1000.0;

    println!("--- {mode}: {} ops, {threads} threads ---", all_lat.len());
    println!("throughput : {:.0} ops/s", all_lat.len() as f64 / elapsed);
    println!("wall time  : {elapsed:.2} s   errors: {errors}");
    println!(
        "latency ms : avg {avg:.2}  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        pct(1.0)
    );
}

fn build_op(
    mode: &str,
    thread: usize,
    i: usize,
    preload: usize,
    read_span: usize,
    rng: &mut u64,
) -> String {
    let (read, id) = pick_op(mode, thread, i, preload, read_span, rng);
    if read {
        format!("SELECT v FROM bench WHERE id = {id}")
    } else {
        format!("INSERT INTO bench (id, v) VALUES ({id}, 'payload-{id}')")
    }
}

/// The shared op chooser: whether this op is a read, and the row id it
/// targets — identical distribution for the text and prepared paths.
fn pick_op(
    mode: &str,
    thread: usize,
    i: usize,
    preload: usize,
    read_span: usize,
    rng: &mut u64,
) -> (bool, i64) {
    // xorshift for cheap pseudo-randomness (no external rng dependency).
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    let read = mode == "read" || (mode == "mixed" && (*rng & 1 == 0));
    let id = if read {
        ((*rng as usize) % read_span.max(1)) as i64
    } else {
        // Unique id per (thread, i) to avoid PK collisions across threads.
        (preload + thread * 10_000_000 + i) as i64
    };
    (read, id)
}

/// Connect thread `idx` to one of the configured node addresses (round-robin).
fn connect(cfg: &(Vec<String>, String, String), idx: usize) -> Client {
    let addr = &cfg.0[idx % cfg.0.len()];
    Client::connect_with(addr, &cfg.1, &cfg.2).expect("connect")
}

/// Parse the preload argument: `N` or `NxS` (N rows, S-byte values).
fn parse_preload(arg: Option<&str>) -> (usize, usize) {
    match arg {
        None => (1000, 0),
        Some(s) => match s.split_once('x') {
            Some((n, sz)) => (n.parse().expect("preload rows"), sz.parse().expect("value size")),
            None => (s.parse().expect("preload"), 0),
        },
    }
}

/// The preload value for row `i`, padded with dots to `valsize` bytes when a
/// size is given (0 keeps the short default).
fn padded_payload(i: usize, valsize: usize) -> String {
    let mut v = format!("payload-{i}");
    while v.len() < valsize {
        v.push('.');
    }
    v
}

fn run_ok(c: &mut Client, sql: &str) {
    if let Err(e) = c.execute(sql) {
        panic!("setup failed: {sql}: {e}");
    }
}
