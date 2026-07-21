//! ACID crash harness: durability and atomicity under kill -9.
//!
//! The parent spawns THIS binary in `--child` mode against a fresh data
//! dir, feeds it a workload over stdin (single-row inserts, multi-row
//! inserts, BEGIN…COMMIT batches), records which statements were ACKED,
//! SIGKILLs the child at a random moment, reopens the database in-process,
//! and classifies what survived:
//!
//! - HARD violation — an ACKED write is missing after recovery
//!   (durability), or an ACKED commit/statement is partially applied
//!   (atomicity of acknowledged work).
//! - SOFT observation — an UNacked multi-row STATEMENT is partially
//!   visible after recovery (statements are not journaled). Transactions
//!   are journaled: ANY partial transaction is a HARD violation, acked or
//!   not — the commit journal makes them all-or-nothing across crashes.
//!
//! Exit is nonzero on hard violations. `--rounds N` (default 20),
//! `--seed S`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use skaidb_engine::{Database, QueryOutput};
use skaidb_types::Value;

fn child(dir: &str) {
    let mut db = Database::open(dir).expect("child open");
    let stdin = std::io::stdin();
    let mut out = std::io::stdout();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.is_empty() {
            continue;
        }
        // Lines are "<id>\t<sql>"; ack with the id AFTER execute returns.
        let Some((id, sql)) = line.split_once('\t') else { continue };
        match db.execute(sql) {
            Ok(_) => {
                let _ = writeln!(out, "ACK {id}");
                let _ = out.flush();
            }
            Err(e) => {
                let _ = writeln!(out, "ERR {id} {e}");
                let _ = out.flush();
            }
        }
    }
}

#[derive(Clone)]
enum Op {
    /// One row: (id, group) — group identifies the statement it belonged to.
    Single { row: i64 },
    /// One INSERT statement carrying several rows.
    Multi { rows: Vec<i64> },
    /// BEGIN; k inserts; COMMIT — acked means the COMMIT was acked.
    Txn { rows: Vec<i64> },
}

struct Outcome {
    hard: Vec<String>,
    soft: Vec<String>,
}

fn run_round(exe: &str, seed: u64) -> Outcome {
    let dir = std::env::temp_dir().join(format!("skaidb-acid-{}-{seed}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut childp = Command::new(exe)
        .arg("--child")
        .arg(dir.to_str().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn child");
    let mut cin = childp.stdin.take().unwrap();
    let cout = childp.stdout.take().unwrap();

    // Reader thread: collect acks.
    let acked = std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::<u64>::new()));
    let acked_r = acked.clone();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(cout).lines() {
            let Ok(line) = line else { break };
            if let Some(rest) = line.strip_prefix("ACK ") {
                if let Ok(id) = rest.trim().parse::<u64>() {
                    acked_r.lock().unwrap().insert(id);
                }
            }
        }
    });

    let mut rng = StdRng::seed_from_u64(seed);
    let mut ops: Vec<(u64, Op)> = Vec::new();
    let mut next_row: i64 = 0;
    let mut next_id: u64 = 0;
    let setup = "CREATE TABLE t (PRIMARY KEY (id))";
    let _ = writeln!(cin, "{next_id}\t{setup}");
    next_id += 1;

    // Killer thread arms after a random delay while the writer streams ops.
    let kill_after = Duration::from_millis(rng.gen_range(80..600));
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() >= kill_after {
            break;
        }
        let id = next_id;
        next_id += 1;
        let op = match rng.gen_range(0..3) {
            0 => {
                let row = next_row;
                next_row += 1;
                let _ = writeln!(
                    cin,
                    "{id}\tINSERT INTO t (id, v, tag) VALUES ({row}, {row}, 'single')"
                );
                Op::Single { row }
            }
            1 => {
                let n = rng.gen_range(2..6);
                let rows: Vec<i64> = (0..n).map(|_| { let r = next_row; next_row += 1; r }).collect();
                let vals: Vec<String> =
                    rows.iter().map(|r| format!("({r}, {r}, 'multi{id}')")).collect();
                let _ = writeln!(
                    cin,
                    "{id}\tINSERT INTO t (id, v, tag) VALUES {}",
                    vals.join(", ")
                );
                Op::Multi { rows }
            }
            _ => {
                // The txn's statements share one ack id: the COMMIT's.
                // Occasionally a LARGE transaction: its commit applies
                // hundreds of rows sequentially, opening a real window for
                // the kill to land mid-commit.
                let n = if rng.gen_bool(0.25) {
                    rng.gen_range(100..400)
                } else {
                    rng.gen_range(2..6)
                };
                let rows: Vec<i64> = (0..n).map(|_| { let r = next_row; next_row += 1; r }).collect();
                let _ = writeln!(cin, "{}\tBEGIN", next_id);
                next_id += 1;
                for r in &rows {
                    let _ = writeln!(
                        cin,
                        "{}\tINSERT INTO t (id, v, tag) VALUES ({r}, {r}, 'txn{id}')",
                        next_id
                    );
                    next_id += 1;
                }
                let _ = writeln!(cin, "{id}\tCOMMIT");
                Op::Txn { rows }
            }
        };
        let was_large_txn = matches!(&op, Op::Txn { rows } if rows.len() >= 100);
        ops.push((id, op));
        // Occasional breather so acks interleave with the kill window.
        if rng.gen_bool(0.1) {
            std::thread::sleep(Duration::from_millis(1));
        }
        // Targeted mid-commit kill: right after a large txn's COMMIT went
        // onto the pipe, cut the process down while it is applying.
        if was_large_txn && rng.gen_bool(0.6) {
            std::thread::sleep(Duration::from_micros(rng.gen_range(200..4000)));
            break;
        }
    }
    // SIGKILL: no drop handlers, no flush — the crash.
    let _ = childp.kill();
    let _ = childp.wait();
    drop(cin);
    let _ = reader.join();
    let acked = std::sync::Arc::try_unwrap(acked).unwrap().into_inner().unwrap();

    // Recovery: reopen in-process and inventory surviving rows.
    let mut db = Database::open(&dir).expect("recovery open");
    let present: std::collections::HashSet<i64> = match db.execute("SELECT id FROM t") {
        Ok(QueryOutput::Rows(rs)) => rs
            .rows
            .iter()
            .filter_map(|r| match r.first() {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            })
            .collect(),
        _ => Default::default(),
    };
    let mut out = Outcome { hard: Vec::new(), soft: Vec::new() };
    for (id, op) in &ops {
        let was_acked = acked.contains(id);
        match op {
            Op::Single { row } => {
                if was_acked && !present.contains(row) {
                    out.hard.push(format!("DURABILITY: acked single row {row} missing"));
                }
            }
            Op::Multi { rows } => {
                let have = rows.iter().filter(|r| present.contains(r)).count();
                if was_acked && have != rows.len() {
                    out.hard.push(format!(
                        "ATOMICITY: acked multi-row stmt (op {id}) has {have}/{} rows",
                        rows.len()
                    ));
                } else if !was_acked && have != 0 && have != rows.len() {
                    out.soft.push(format!(
                        "partial unacked multi-row stmt (op {id}): {have}/{} rows visible",
                        rows.len()
                    ));
                }
            }
            Op::Txn { rows } => {
                // With the commit journal, a transaction is all-or-nothing
                // REGARDLESS of whether its COMMIT was acknowledged: the
                // journal is durable before any row applies, and recovery
                // replays it to completion. Any partial is a violation.
                let have = rows.iter().filter(|r| present.contains(r)).count();
                if have != 0 && have != rows.len() {
                    out.hard.push(format!(
                        "ATOMICITY: txn (op {id}, acked={was_acked}) has {have}/{} rows",
                        rows.len()
                    ));
                } else if was_acked && have == 0 {
                    out.hard.push(format!(
                        "DURABILITY: acked txn (op {id}) fully missing"
                    ));
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--child") {
        child(&args[2]);
        return;
    }
    let get = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
    };
    let rounds: u64 = get("--rounds").and_then(|s| s.parse().ok()).unwrap_or(20);
    let seed: u64 = get("--seed").and_then(|s| s.parse().ok()).unwrap_or(7);
    let exe = std::env::current_exe().unwrap();
    let (mut hard, mut soft) = (0usize, 0usize);
    for r in 0..rounds {
        let out = run_round(exe.to_str().unwrap(), seed.wrapping_add(r));
        for h in &out.hard {
            eprintln!("round {r}: HARD {h}");
        }
        for s in &out.soft {
            eprintln!("round {r}: soft {s}");
        }
        hard += out.hard.len();
        soft += out.soft.len();
    }
    eprintln!(
        "acid-crash: {rounds} rounds — {hard} HARD violations, {soft} soft partial-visibility observations"
    );
    std::process::exit(if hard == 0 { 0 } else { 1 });
}
