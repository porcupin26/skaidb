//! Full-text search ingest + query benchmark on the embedded engine — the
//! real SQL path (INSERT batches through the executor, MATCH/SEARCH through
//! the planner), not raw Tantivy (that's `skaidb-fts/examples/phase0_spike`).
//!
//! Measures, on a synthetic corpus (deliberately worst-case posting lengths:
//! a small vocabulary means every term matches a large slice of the table):
//!   - ingest throughput with a search index maintained, batched INSERTs
//!     (the FTS bulk-ingest path, docs/SEARCH.md: one NRT refresh check per
//!     statement) vs single-row INSERTs,
//!   - backfill speed (`CREATE SEARCH INDEX` over an existing table),
//!   - query latency percentiles: term, bool AND, phrase, ranked top-10.
//!
//!   cargo run --release --example fts_bench -p skaidb-engine -- <data_dir> [rows] [batch]

use std::time::Instant;

use skaidb_engine::{Database, QueryOutput};

/// Deterministic xorshift so runs are comparable (no external rand dep).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn pick(&mut self, pool: &[&'static str]) -> &'static str {
        pool[(self.next() % pool.len() as u64) as usize]
    }
}

const WORDS: &[&str] = &[
    "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "rust",
    "database", "search", "index", "segment", "merge", "commit", "replica",
    "cluster", "vector", "storage", "engine", "query", "phrase", "boolean",
    "latency", "throughput", "benchmark", "wikipedia", "article", "system",
    "distributed", "consistent", "hashing", "ring", "snapshot", "compaction",
    "memtable", "bloom", "filter", "cache", "page", "mmap", "posting",
    "term", "dictionary", "automaton", "levenshtein", "stemmer", "token",
];

fn make_text(rng: &mut Rng, words: usize) -> String {
    let mut s = String::with_capacity(words * 8);
    for i in 0..words {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(rng.pick(WORDS));
    }
    s
}

/// One multi-row INSERT statement covering ids `[from, to)`.
fn insert_stmt(rng: &mut Rng, from: u64, to: u64) -> String {
    let mut sql = String::from("INSERT INTO articles (id, title, body) VALUES ");
    for id in from..to {
        if id > from {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({id}, '{}', '{}')",
            make_text(rng, 4),
            make_text(rng, 80)
        ));
    }
    sql
}

fn row_count(db: &mut Database, sql: &str) -> usize {
    match db.execute(sql).expect(sql) {
        QueryOutput::Rows(rs) => rs.rows.len(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

/// Latency percentiles over `iters` runs, reported in milliseconds.
fn bench(db: &mut Database, label: &str, sql: &str, iters: usize) {
    let mut samples = Vec::with_capacity(iters);
    let mut hits = 0;
    for _ in 0..iters {
        let t = Instant::now();
        hits = row_count(db, sql);
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(f64::total_cmp);
    let pct = |p: f64| samples[((samples.len() - 1) as f64 * p) as usize];
    println!(
        "  {label:<28} p50 {:>7.2} ms   p95 {:>7.2} ms   ({hits} hits)",
        pct(0.50),
        pct(0.95)
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| "/tmp/skaidb-fts-bench".to_string());
    let rows: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let batch: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(1_000);
    let _ = std::fs::remove_dir_all(&dir);

    println!("corpus: {rows} rows × ~80 body words, batch {batch}, dir={dir}\n");

    // Phase A: ingest with the index live, batched statements.
    let mut db = Database::open(&dir).expect("open");
    db.execute("CREATE TABLE articles (PRIMARY KEY (id))").unwrap();
    db.execute("CREATE SEARCH INDEX articles_fts ON articles (title, body) WITH (analyzer = 'english')")
        .unwrap();
    let mut rng = Rng(0x5EED);
    let t = Instant::now();
    let mut id = 0;
    while id < rows {
        let to = (id + batch).min(rows);
        db.execute(&insert_stmt(&mut rng, id, to)).unwrap();
        id = to;
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "ingest, index live, batched:   {:>9.0} rows/s   ({secs:.1}s)",
        rows as f64 / secs
    );

    // Phase B: same volume, one row per statement (the per-row refresh
    // check + per-statement overhead the bulk path removes).
    let single_rows = (rows / 10).max(1);
    let mut db2 = Database::open(format!("{dir}-single")).expect("open");
    db2.execute("CREATE TABLE articles (PRIMARY KEY (id))").unwrap();
    db2.execute("CREATE SEARCH INDEX articles_fts ON articles (title, body) WITH (analyzer = 'english')")
        .unwrap();
    let mut rng2 = Rng(0x5EED);
    let t = Instant::now();
    for id in 0..single_rows {
        db2.execute(&insert_stmt(&mut rng2, id, id + 1)).unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "ingest, index live, per-row:   {:>9.0} rows/s   ({secs:.1}s, {single_rows} rows)",
        single_rows as f64 / secs
    );
    drop(db2);
    let _ = std::fs::remove_dir_all(format!("{dir}-single"));

    // Phase C: backfill — rebuild the whole index from the table.
    let t = Instant::now();
    db.execute("REBUILD SEARCH INDEX articles_fts").unwrap();
    let secs = t.elapsed().as_secs_f64();
    println!(
        "backfill (REBUILD):            {:>9.0} rows/s   ({secs:.1}s)\n",
        rows as f64 / secs
    );

    // Phase D: query latencies (worst-case postings: every term matches
    // ~1/47th of every field position).
    println!("query latencies over {rows} rows (20 iterations):");
    bench(&mut db, "term MATCH", "SELECT id FROM articles WHERE MATCH(body, 'fox')", 20);
    bench(
        &mut db,
        "bool AND (SEARCH)",
        "SELECT id FROM articles WHERE SEARCH('+body:fox +body:rust')",
        20,
    );
    bench(
        &mut db,
        "phrase",
        "SELECT id FROM articles WHERE MATCH_PHRASE(body, 'quick brown')",
        20,
    );
    bench(
        &mut db,
        "ranked top-10",
        "SELECT id FROM articles WHERE MATCH(body, 'fox rust') ORDER BY score() DESC LIMIT 10",
        20,
    );
    bench(
        &mut db,
        "ranked top-10 + highlight",
        "SELECT id, HIGHLIGHT(body, 60) FROM articles WHERE MATCH(body, 'fox rust') \
         ORDER BY score() DESC LIMIT 10",
        20,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
