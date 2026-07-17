//! Validates the E-7 fix (agencik wishlist: an unfiltered `GROUP BY` over a
//! wide-row table OOM-killed the node) at realistic scale: builds a table
//! shaped like the reported crash (many rows, a large text field never
//! referenced by the query) and reports process RSS before/after running
//! the exact query shape that used to blow up memory.
//!
//! Run: cargo run --release --example group_by_memory_check -p skaidb-engine -- [rows] [body_kb]

use skaidb_engine::Session;

fn rss_kb() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().unwrap();
        }
    }
    0
}

fn main() {
    let rows: usize = std::env::args().nth(1).map_or(50_000, |s| s.parse().unwrap());
    let body_kb: usize = std::env::args().nth(2).map_or(10, |s| s.parse().unwrap());

    let dir = std::env::temp_dir().join(format!("skaidb-gbmem-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut session = Session::open(&dir).unwrap();
    session
        .execute("CREATE TABLE emails (PRIMARY KEY (id))")
        .unwrap();

    println!("loading {rows} rows, ~{body_kb} KB body each (~{} MB total)...", rows * body_kb / 1024);
    let body = "x".repeat(body_kb * 1024);
    const BATCH: usize = 500;
    let mut id = 0;
    while id < rows {
        let end = (id + BATCH).min(rows);
        let mut sql = String::with_capacity(BATCH * (body_kb * 1024 + 64));
        sql.push_str("INSERT INTO emails (id, account, body) VALUES ");
        for (n, i) in (id..end).enumerate() {
            if n > 0 {
                sql.push(',');
            }
            let account = format!("acct{}", i % 20);
            sql.push_str(&format!("({i}, '{account}', '{body}')"));
        }
        session.execute(&sql).unwrap();
        id = end;
    }
    session.execute("SELECT count(*) FROM emails").unwrap(); // settle any flush bookkeeping

    let rss_before = rss_kb();
    println!("RSS before GROUP BY: {} MB", rss_before / 1024);

    // The exact reported crash shape: unfiltered GROUP BY + COUNT(*) over a
    // table whose rows are dominated by a field this query never touches.
    let t0 = std::time::Instant::now();
    let out = session
        .execute("SELECT account, COUNT(*) FROM emails GROUP BY account")
        .unwrap();
    let elapsed = t0.elapsed();

    let rss_after = rss_kb();
    println!("RSS after GROUP BY:  {} MB", rss_after / 1024);
    println!(
        "RSS delta: {} MB (query took {:.2}s)",
        rss_after.saturating_sub(rss_before) / 1024,
        elapsed.as_secs_f64()
    );
    match out {
        skaidb_engine::QueryOutput::Rows(rs) => println!("groups returned: {}", rs.rows.len()),
        _ => unreachable!(),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
