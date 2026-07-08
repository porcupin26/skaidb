//! Phase 0 spike (docs/FTS_TODO.md): validate the risky Tantivy integration
//! points before committing to it as the FTS core.
//!
//! Measures, on a synthetic corpus:
//!   (a) rebuild-from-table indexing speed (docs/s, MB/s),
//!   (b) commit/refresh semantics + opstamp replay — the WAL-as-translog
//!       recovery model: uncommitted docs are lost on crash and must be
//!       replayable from the row WAL keyed by the last committed opstamp,
//!   (c) writer memory bounds (RSS growth vs configured writer heap),
//!   (d) query latency sanity (term / bool / phrase) over the built index.
//!
//! Run: cargo run --release -p skaidb-fts --example phase0_spike [-- N_DOCS [HEAP_MB]]

use std::time::Instant;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, Value, FAST, STORED, STRING, TEXT};
use tantivy::{doc, Index, IndexWriter, ReloadPolicy, TantivyDocument};

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
    fn pick<'a>(&mut self, pool: &'a [&'a str]) -> &'a str {
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

fn make_body(rng: &mut Rng, words: usize) -> String {
    let mut s = String::with_capacity(words * 8);
    for i in 0..words {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(rng.pick(WORDS));
    }
    s
}

fn rss_mb() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|kb| kb.parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

fn main() -> tantivy::Result<()> {
    let mut args = std::env::args().skip(1);
    let n_docs: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200_000);
    let heap_mb: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(128);

    let dir = std::env::temp_dir().join(format!("skaidb-fts-spike-{n_docs}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let mut schema_builder = Schema::builder();
    let pk = schema_builder.add_text_field("pk", STRING | STORED);
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT);
    let ts = schema_builder.add_u64_field("hlc", FAST);
    let schema = schema_builder.build();

    println!("== phase 0 spike: {n_docs} docs, writer heap {heap_mb} MB ==");
    println!("index dir: {}", dir.display());
    println!("rss before: {:.1} MB", rss_mb());

    // ---- (a) rebuild-from-table speed --------------------------------------
    let index = Index::create_in_dir(&dir, schema.clone())?;
    let mut writer: IndexWriter = index.writer(heap_mb * 1_048_576)?;

    let mut rng = Rng(0x5ca1db_5ca1db);
    let mut bytes: u64 = 0;
    let t0 = Instant::now();
    let mut peak_rss: f64 = 0.0;
    for i in 0..n_docs {
        let t = make_body(&mut rng, 6);
        let b = make_body(&mut rng, 80);
        bytes += (t.len() + b.len()) as u64;
        writer.add_document(doc!(
            pk => format!("k{i:012}"),
            title => t,
            body => b,
            ts => i as u64,
        ))?;
        if i % 20_000 == 0 {
            peak_rss = peak_rss.max(rss_mb());
        }
    }
    let index_secs = t0.elapsed().as_secs_f64();
    peak_rss = peak_rss.max(rss_mb());

    let t_commit = Instant::now();
    let opstamp = writer.commit()?;
    let commit_secs = t_commit.elapsed().as_secs_f64();

    println!("\n(a) rebuild speed");
    println!(
        "  indexed {n_docs} docs ({:.1} MB text) in {index_secs:.2}s = {:.0} docs/s, {:.1} MB/s",
        bytes as f64 / 1e6,
        n_docs as f64 / index_secs,
        bytes as f64 / 1e6 / index_secs
    );
    println!("  final commit: {commit_secs:.3}s (opstamp {opstamp})");
    let disk: u64 = std::fs::read_dir(&dir)?
        .filter_map(|e| e.ok()?.metadata().ok().map(|m| m.len()))
        .sum();
    println!(
        "  index size on disk: {:.1} MB ({:.0}% of raw text)",
        disk as f64 / 1e6,
        disk as f64 / bytes as f64 * 100.0
    );

    println!("\n(c) writer memory bounds");
    println!(
        "  peak rss during indexing: {peak_rss:.1} MB (writer heap budget {heap_mb} MB)"
    );

    // ---- (b) commit/refresh semantics + opstamp replay ----------------------
    println!("\n(b) commit / NRT / opstamp-replay semantics");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    // Add uncommitted docs: must be invisible, then lost on abort (= crash).
    let pre = reader.searcher().num_docs();
    for i in n_docs..n_docs + 1000 {
        writer.add_document(doc!(
            pk => format!("k{i:012}"),
            title => "uncommitted marker xylophone".to_string(),
            body => make_body(&mut rng, 40),
            ts => i as u64,
        ))?;
    }
    reader.reload()?;
    let visible_before_commit = reader.searcher().num_docs() - pre;
    println!("  uncommitted docs visible after reload: {visible_before_commit} (want 0)");

    // Simulated kill -9: drop the writer without committing, reopen the index.
    writer.rollback()?; // rollback == what a crash leaves behind after reopen
    drop(writer);
    let index2 = Index::open_in_dir(&dir)?;
    let reader2 = index2
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let after_crash = reader2.searcher().num_docs();
    println!(
        "  docs after simulated crash+reopen: {after_crash} (committed was {n_docs}) — lost tail must replay from WAL"
    );

    // Replay: the engine would scan the table for rows with HLC > last
    // committed opstamp-mapped watermark and re-add them.
    let mut writer2: IndexWriter = index2.writer(heap_mb * 1_048_576)?;
    let t_replay = Instant::now();
    for i in n_docs..n_docs + 1000 {
        writer2.add_document(doc!(
            pk => format!("k{i:012}"),
            title => "replayed marker xylophone".to_string(),
            body => make_body(&mut rng, 40),
            ts => i as u64,
        ))?;
    }
    let replay_opstamp = writer2.commit()?;
    reader2.reload()?;
    println!(
        "  replayed 1000 docs + commit in {:.3}s (opstamp {replay_opstamp}), total docs now {}",
        t_replay.elapsed().as_secs_f64(),
        reader2.searcher().num_docs()
    );

    // NRT refresh cadence: measure a small-batch commit + reader reload,
    // i.e. the cost of a 1 s refresh tick under sustained ingest.
    let mut tick_commit = 0.0;
    let mut tick_reload = 0.0;
    const TICKS: usize = 10;
    for tick in 0..TICKS {
        for i in 0..2000 {
            let n = n_docs + 1000 + tick * 2000 + i;
            writer2.add_document(doc!(
                pk => format!("k{n:012}"),
                title => make_body(&mut rng, 6),
                body => make_body(&mut rng, 80),
                ts => n as u64,
            ))?;
        }
        let t = Instant::now();
        writer2.commit()?;
        tick_commit += t.elapsed().as_secs_f64();
        let t = Instant::now();
        reader2.reload()?;
        tick_reload += t.elapsed().as_secs_f64();
    }
    println!(
        "  refresh tick (2k-doc batch): commit avg {:.1} ms, reader reload avg {:.1} ms",
        tick_commit / TICKS as f64 * 1e3,
        tick_reload / TICKS as f64 * 1e3
    );

    // Delete + reinsert (the update model): delete by pk term, re-add.
    let t = Instant::now();
    for i in 0..1000 {
        writer2.delete_term(tantivy::Term::from_field_text(pk, &format!("k{i:012}")));
        writer2.add_document(doc!(
            pk => format!("k{i:012}"),
            title => "updated marker".to_string(),
            body => make_body(&mut rng, 80),
            ts => (n_docs * 2 + i) as u64,
        ))?;
    }
    writer2.commit()?;
    reader2.reload()?;
    println!(
        "  1000 delete+reinsert updates + commit: {:.3}s, doc count stable at {}",
        t.elapsed().as_secs_f64(),
        reader2.searcher().num_docs()
    );

    // ---- (d) query latency sanity -------------------------------------------
    println!("\n(d) query latency (median of 200 runs, top-10)");
    let searcher = reader2.searcher();
    let parser = QueryParser::for_index(&index2, vec![title, body]);
    for (label, q) in [
        ("term", "database"),
        ("bool AND", "+quick +database"),
        ("bool OR", "quick database rust"),
        ("phrase", "\"quick brown\""),
    ] {
        let query = parser.parse_query(q)?;
        let mut lat = Vec::with_capacity(200);
        let mut hits = 0;
        for _ in 0..200 {
            let t = Instant::now();
            let top = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
            lat.push(t.elapsed().as_secs_f64() * 1e3);
            hits = top.len();
        }
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "  {label:9} '{q}': p50 {:.2} ms  p99 {:.2} ms  ({hits} hits returned)",
            lat[100],
            lat[198]
        );
    }

    // Retrieve a stored field to prove round-trip.
    let query = parser.parse_query("updated")?;
    let top = searcher.search(&query, &TopDocs::with_limit(1).order_by_score())?;
    if let Some((score, addr)) = top.first() {
        let retrieved: TantivyDocument = searcher.doc(*addr)?;
        let key = retrieved
            .get_first(pk)
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        println!("  stored-field round-trip: pk={key} score={score:.3}");
    }

    println!("\nrss at end: {:.1} MB", rss_mb());
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
