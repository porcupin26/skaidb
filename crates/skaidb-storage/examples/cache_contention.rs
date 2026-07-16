//! Concurrent point-read throughput under cache contention — the workload
//! the ReadCache/BlockCache sharding change targets. Loads a table, flushes
//! it to SSTables (so every read goes through the caches, not the memtable),
//! then hammers a bounded hot-key set from many threads simultaneously and
//! reports aggregate ops/sec.
//!
//! Run: cargo run --release --example cache_contention -p skaidb-storage -- \
//!      <rows> <hot_keys> <threads> <ops_per_thread>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use skaidb_storage::{Engine, EngineOptions};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = args.get(1).map_or(50_000, |s| s.parse().unwrap());
    let hot_keys: usize = args.get(2).map_or(2_000, |s| s.parse().unwrap());
    let threads: usize = args.get(3).map_or(32, |s| s.parse().unwrap());
    let ops_per_thread: usize = args.get(4).map_or(20_000, |s| s.parse().unwrap());

    let dir = std::env::temp_dir().join(format!("cache-contention-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let opts = EngineOptions {
        flush_threshold_bytes: 1 << 20, // 1 MiB: force real SSTables, not one giant memtable
        ..EngineOptions::default()
    };
    let mut engine = Engine::open_with_options(&dir, opts).unwrap();
    for i in 0..rows {
        engine
            .put(format!("key{i:08}").as_bytes(), vec![b'x'; 64])
            .unwrap();
    }
    engine.flush().unwrap();
    println!("loaded {rows} rows, flushed to SSTables");

    let engine = Arc::new(engine);
    // Warm the caches once so the measured phase is steady-state hits, not
    // the one-time cold-fill cost.
    for i in 0..hot_keys {
        engine.get(format!("key{i:08}").as_bytes()).unwrap();
    }

    let barrier = Arc::new(Barrier::new(threads));
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let total_ops = Arc::clone(&total_ops);
            std::thread::spawn(move || {
                // Deterministic per-thread pseudo-random walk over the hot
                // set — every thread touches the SAME keys, maximizing
                // cross-thread cache contention (the scenario sharding
                // targets: many readers, overlapping working set).
                let mut x: u64 = 0x9E37_79B9 ^ (t as u64);
                barrier.wait();
                for _ in 0..ops_per_thread {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let key = format!("key{:08}", (x as usize) % hot_keys);
                    engine.get(key.as_bytes()).unwrap();
                }
                total_ops.fetch_add(ops_per_thread as u64, Ordering::Relaxed);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    let stats = engine.stats();
    println!(
        "{threads} threads x {ops_per_thread} ops = {ops} total in {:.3}s = {:.0} ops/s (cache hits {} misses {})",
        elapsed.as_secs_f64(),
        ops as f64 / elapsed.as_secs_f64(),
        stats.cache.hits,
        stats.cache.misses,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
