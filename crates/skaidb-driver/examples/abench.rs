//! Minimal concurrent load generator for the encryption A/B benchmark.
//!
//! Usage:
//!   abench --endpoints h1:7000,h2:7000 --user U --pass P \
//!          --mode write|read|mixed --threads N --secs S --prepop R \
//!          [--tls-ca ca.crt | --tls-insecure] [--tls-sni skaidb]
//!
//! Pre-connects `threads` clients, waits on a barrier, then runs the workload
//! until the deadline and prints `ops/s`. Setup (table + prepopulate) is timed
//! separately and not counted.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use skaidb_driver::{Client, TlsConfig, TlsVerify};
use skaidb_types::Value;

fn arg(name: &str) -> Option<String> {
    let mut it = std::env::args();
    while let Some(a) = it.next() {
        if a == name {
            return it.next();
        }
    }
    None
}
fn flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

fn build_tls() -> Option<TlsConfig> {
    let sni = arg("--tls-sni").unwrap_or_else(|| "skaidb".into());
    if let Some(ca) = arg("--tls-ca") {
        Some(TlsConfig::new(TlsVerify::CaFile(ca), &sni).unwrap())
    } else if flag("--tls-insecure") {
        Some(TlsConfig::new(TlsVerify::Insecure, &sni).unwrap())
    } else {
        None
    }
}

fn connect(eps: &[String], user: &str, pass: &str) -> Client {
    Client::connect_many_tls(eps, user, pass, build_tls()).expect("connect")
}

fn main() {
    let eps: Vec<String> = arg("--endpoints")
        .expect("--endpoints")
        .split(',')
        .map(String::from)
        .collect();
    let user = arg("--user").unwrap_or_else(|| "skaidb".into());
    let pass = arg("--pass").unwrap_or_default();
    let mode = arg("--mode").unwrap_or_else(|| "mixed".into());
    let threads: usize = arg("--threads").map(|s| s.parse().unwrap()).unwrap_or(4);
    let secs: u64 = arg("--secs").map(|s| s.parse().unwrap()).unwrap_or(10);
    let prepop: u64 = arg("--prepop").map(|s| s.parse().unwrap()).unwrap_or(0);
    // Read key range (defaults to prepop). Set independently to read only a
    // cold sub-range (e.g. keys flushed to SSTables, not the live memtable).
    let readkeys: u64 = arg("--readkeys")
        .map(|s| s.parse().unwrap())
        .unwrap_or(prepop)
        .max(1);
    // Incompressible per-row payload (~200 chars) keyed by id, so SSTables
    // don't lz4 down to nothing and the block cache can't hold the whole table
    // — otherwise reads never miss the cache and the on-disk decrypt cost is
    // invisible. Deterministic in the key so writes and reads agree.
    let payload_for = |seed: u64| -> String {
        let mut x = seed
            .wrapping_mul(0x9E3779B97F4A7C15)
            .wrapping_add(0x2545F4914F6CDD1D);
        let mut s = String::with_capacity(224);
        for _ in 0..28 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            s.push_str(&format!("{x:016x}"));
        }
        s
    };

    // Setup: table + optional prepopulate (single client, not timed into ops/s).
    {
        let mut c = connect(&eps, &user, &pass);
        let _ = c.execute("CREATE TABLE IF NOT EXISTS bench (PRIMARY KEY (id))");
        if prepop > 0 {
            let mut ins = c.prepare("INSERT INTO bench (id, v) VALUES (?, ?)").unwrap();
            for i in 0..prepop {
                c.execute_prepared(&mut ins, &[Value::Int(i as i64), Value::String(payload_for(i))])
                    .unwrap();
            }
        }
    }

    let counter = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let deadline = Duration::from_secs(secs);
    let mut handles = Vec::new();
    for t in 0..threads {
        let (eps, user, pass, mode) = (eps.clone(), user.clone(), pass.clone(), mode.clone());
        let counter = Arc::clone(&counter);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut c = connect(&eps, &user, &pass);
            let mut ins = c.prepare("INSERT INTO bench (id, v) VALUES (?, ?)").unwrap();
            let mut sel = c.prepare("SELECT v FROM bench WHERE id = ?").unwrap();
            // Per-thread key space for writes; per-thread LCG for read keys.
            let mut wid: i64 = (t as i64) * 1_000_000_000 + 1_000_000;
            let mut rng: u64 = 0x9E3779B97F4A7C15 ^ (t as u64).wrapping_mul(0x2545F4914F6CDD1D);
            let mut local: u64 = 0;
            barrier.wait();
            let start = Instant::now();
            while start.elapsed() < deadline {
                let do_write = match mode.as_str() {
                    "write" => true,
                    "read" => false,
                    _ => local.is_multiple_of(2), // mixed
                };
                let ok = if do_write {
                    wid += 1;
                    c.execute_prepared(&mut ins, &[Value::Int(wid), Value::String(payload_for(wid as u64))])
                        .is_ok()
                } else {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let key = (rng % readkeys) as i64;
                    c.execute_prepared(&mut sel, &[Value::Int(key)]).is_ok()
                };
                if ok {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                local += 1;
            }
        }));
    }
    barrier.wait();
    let wall = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = wall.elapsed().as_secs_f64();
    let ops = counter.load(Ordering::Relaxed);
    println!(
        "{mode} threads={threads} secs={secs} -> {ops} ops in {elapsed:.2}s = {:.0} ops/s",
        ops as f64 / elapsed
    );
}
