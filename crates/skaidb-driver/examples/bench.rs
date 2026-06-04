//! Simple load generator for a skaidb node/cluster over the binary protocol.
//!
//! Usage:
//!   bench <addr> <user> <pass> <mode> <ops> <threads> [preload]
//!   mode = write | read | mixed
//!
//! Each thread holds one authenticated connection (handshake once) and issues
//! operations in a loop. Reports throughput and latency percentiles.

use std::sync::Arc;
use std::time::Instant;

use skaidb_driver::Client;
use skaidb_proto::Response;

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
    let preload: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(1000);

    let cfg = Arc::new((addrs, user, pass));

    // Fresh table; preload rows for read/mixed.
    {
        let mut c = connect(&cfg, 0);
        let _ = c.execute("DROP TABLE IF EXISTS bench");
        run_ok(&mut c, "CREATE TABLE bench (PRIMARY KEY (id))");
        if mode != "write" {
            for id in 0..preload {
                run_ok(
                    &mut c,
                    &format!("INSERT INTO bench (id, v) VALUES ({id}, 'payload-{id}')"),
                );
            }
            println!("preloaded {preload} rows");
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
            for i in 0..per_thread {
                let sql = build_op(&mode, t, i, preload, &mut rng);
                let op_start = Instant::now();
                match client.execute(&sql) {
                    Ok(Response::Error(_)) | Err(_) => errors += 1,
                    Ok(_) => {}
                }
                lat.push(op_start.elapsed().as_micros() as u64);
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

fn build_op(mode: &str, thread: usize, i: usize, preload: usize, rng: &mut u64) -> String {
    // xorshift for cheap pseudo-randomness (no external rng dependency).
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    let read = mode == "read" || (mode == "mixed" && (*rng & 1 == 0));
    if read {
        let id = (*rng as usize) % preload.max(1);
        format!("SELECT v FROM bench WHERE id = {id}")
    } else {
        // Unique id per (thread, i) to avoid PK collisions across threads.
        let id = preload + thread * 10_000_000 + i;
        format!("INSERT INTO bench (id, v) VALUES ({id}, 'payload-{id}')")
    }
}

/// Connect thread `idx` to one of the configured node addresses (round-robin).
fn connect(cfg: &(Vec<String>, String, String), idx: usize) -> Client {
    let addr = &cfg.0[idx % cfg.0.len()];
    Client::connect_with(addr, &cfg.1, &cfg.2).expect("connect")
}

fn run_ok(c: &mut Client, sql: &str) {
    if let Err(e) = c.execute(sql) {
        panic!("setup failed: {sql}: {e}");
    }
}
