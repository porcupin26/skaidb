//! Differential SQL fuzzer: generate queries in the skaidb∩SQLite dialect,
//! run them against both engines, and treat any result divergence as a
//! finding — frozen to a self-contained `.slt` repro under the findings
//! dir. This is how the original sqllogictest corpus was born (SQLite
//! cross-checked against other engines); here skaidb is the engine under
//! test and SQLite is the oracle.
//!
//! Dialect subset chosen to make the oracle exact (divergences are bugs or
//! genuine semantic decisions, not noise): lowercase ASCII text only (LIKE
//! is case-insensitive in SQLite), no `/` (SQLite integer division), typed
//! comparisons only (no affinity games), NULLs everywhere (skaidb stores
//! them, and absent-vs-NULL is exercised by omitting columns on insert).
//!
//! Usage: slt-diff [--seconds N | --queries N] [--seed S] [--findings DIR]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use skaidb_engine::QueryOutput;
use skaidb_slt::SkaiDb;
use skaidb_types::Value;

const COLS_INT: [&str; 2] = ["a", "b"];
const COL_TEXT: &str = "c";
const COL_REAL: &str = "d";
const WORDS: [&str; 12] = [
    "apple", "banana", "cherry", "date", "elder", "fig", "grape", "kiwi", "lemon", "mango",
    "nashi", "olive",
];

struct Round {
    skai: SkaiDb,
    lite: rusqlite::Connection,
    /// The DDL+inserts that built this round, for repro files.
    setup: Vec<String>,
}

fn build_round(rng: &mut StdRng) -> Round {
    let mut skai = SkaiDb::new();
    let lite = rusqlite::Connection::open_in_memory().expect("sqlite");
    let mut setup = Vec::new();
    lite.execute_batch("CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER, c TEXT, d REAL)")
        .unwrap();
    skai.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
    setup.push("CREATE TABLE t (PRIMARY KEY (id))".to_string());
    let rows = rng.gen_range(40..200);
    for id in 0..rows {
        // Each column is independently NULL ~20% of the time. NULL means:
        // omitted on the skaidb insert (absent ≡ NULL there) and literal
        // NULL on the SQLite side.
        let a = (!rng.gen_bool(0.2)).then(|| rng.gen_range(-50i64..50));
        let b = (!rng.gen_bool(0.2)).then(|| rng.gen_range(0i64..20));
        let c = (!rng.gen_bool(0.2)).then(|| WORDS[rng.gen_range(0..WORDS.len())]);
        let d = (!rng.gen_bool(0.2)).then(|| (rng.gen_range(-1000i64..1000) as f64) / 8.0);
        let mut cols = vec!["id".to_string()];
        let mut vals = vec![id.to_string()];
        if let Some(a) = a {
            cols.push("a".into());
            vals.push(a.to_string());
        }
        if let Some(b) = b {
            cols.push("b".into());
            vals.push(b.to_string());
        }
        if let Some(c) = c {
            cols.push("c".into());
            vals.push(format!("'{c}'"));
        }
        if let Some(d) = d {
            cols.push("d".into());
            vals.push(format!("{d:?}"));
        }
        let ins = format!("INSERT INTO t ({}) VALUES ({})", cols.join(", "), vals.join(", "));
        skai.execute(&ins).unwrap();
        setup.push(ins);
        lite.execute(
            "INSERT INTO t (id, a, b, c, d) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![id, a, b, c, d],
        )
        .unwrap();
    }
    Round { skai, lite, setup }
}

fn gen_pred(rng: &mut StdRng, depth: usize) -> String {
    if depth > 0 && rng.gen_bool(0.4) {
        let op = if rng.gen_bool(0.5) { "AND" } else { "OR" };
        let l = gen_pred(rng, depth - 1);
        let r = gen_pred(rng, depth - 1);
        if rng.gen_bool(0.25) {
            format!("NOT ({l} {op} {r})")
        } else {
            format!("({l} {op} {r})")
        }
    } else {
        match rng.gen_range(0..7) {
            0 => {
                let col = COLS_INT[rng.gen_range(0..2)];
                let cmp = ["=", "!=", "<", "<=", ">", ">="][rng.gen_range(0..6)];
                format!("{col} {cmp} {}", rng.gen_range(-30i64..30))
            }
            1 => format!(
                "{} {} '{}'",
                COL_TEXT,
                ["=", "!=", "<", ">"][rng.gen_range(0..4)],
                WORDS[rng.gen_range(0..WORDS.len())]
            ),
            2 => {
                let col = [COLS_INT[0], COLS_INT[1], COL_TEXT, COL_REAL]
                    [rng.gen_range(0..4)];
                if rng.gen_bool(0.5) {
                    format!("{col} IS NULL")
                } else {
                    format!("{col} IS NOT NULL")
                }
            }
            3 => {
                let col = COLS_INT[rng.gen_range(0..2)];
                let items: Vec<String> = (0..rng.gen_range(1..5))
                    .map(|_| rng.gen_range(-30i64..30).to_string())
                    .collect();
                let neg = if rng.gen_bool(0.3) { "NOT " } else { "" };
                format!("{col} {neg}IN ({})", items.join(", "))
            }
            4 => {
                let col = COLS_INT[rng.gen_range(0..2)];
                let lo = rng.gen_range(-30i64..10);
                format!("{col} BETWEEN {lo} AND {}", lo + rng.gen_range(0i64..30))
            }
            5 => {
                let w = WORDS[rng.gen_range(0..WORDS.len())];
                let pat = match rng.gen_range(0..3) {
                    0 => format!("{}%", &w[..2]),
                    1 => format!("%{}", &w[w.len() - 2..]),
                    _ => format!("%{}%", &w[1..3]),
                };
                format!("{COL_TEXT} LIKE '{pat}'")
            }
            _ => format!("{} > {:?}", COL_REAL, rng.gen_range(-500i64..500) as f64 / 8.0),
        }
    }
}

enum Shape {
    Plain { proj: Vec<String>, ordered: bool, limit: Option<u64> },
    Agg { group: Option<&'static str>, aggs: Vec<String>, having: Option<String> },
}

fn gen_query(rng: &mut StdRng) -> (String, Shape) {
    let pred = rng.gen_bool(0.8).then(|| gen_pred(rng, 2));
    let where_sql = pred.map(|p| format!(" WHERE {p}")).unwrap_or_default();
    if rng.gen_bool(0.6) {
        // Plain projection. Always project id so rows are identifiable.
        let mut proj = vec!["id".to_string()];
        for col in [COLS_INT[0], COLS_INT[1], COL_TEXT, COL_REAL] {
            if rng.gen_bool(0.5) {
                proj.push(col.to_string());
            }
        }
        if rng.gen_bool(0.3) {
            proj.push("a + b".to_string());
        }
        if rng.gen_bool(0.2) {
            proj.push("a * 2".to_string());
        }
        let ordered = rng.gen_bool(0.5);
        let limit = (ordered && rng.gen_bool(0.4)).then(|| rng.gen_range(1u64..20));
        let mut sql = format!("SELECT {} FROM t{where_sql}", proj.join(", "));
        if ordered {
            sql.push_str(" ORDER BY id");
        }
        if let Some(l) = limit {
            let _ = write!(sql, " LIMIT {l}");
        }
        (sql, Shape::Plain { proj, ordered, limit })
    } else {
        let group = rng.gen_bool(0.7).then(|| {
            if rng.gen_bool(0.5) {
                COL_TEXT
            } else {
                "b"
            }
        });
        let mut aggs = vec!["count(*)".to_string()];
        for a in ["count(a)", "sum(a)", "min(a)", "max(b)", "avg(a)", "sum(b)"] {
            if rng.gen_bool(0.35) {
                aggs.push(a.to_string());
            }
        }
        let having = (group.is_some() && rng.gen_bool(0.3))
            .then(|| format!("count(*) > {}", rng.gen_range(0..6)));
        let mut sql = String::from("SELECT ");
        if let Some(g) = group {
            let _ = write!(sql, "{g}, ");
        }
        sql.push_str(&aggs.join(", "));
        sql.push_str(" FROM t");
        sql.push_str(&where_sql);
        if let Some(g) = group {
            let _ = write!(sql, " GROUP BY {g}");
        }
        if let Some(h) = &having {
            let _ = write!(sql, " HAVING {h}");
        }
        (sql, Shape::Agg { group, aggs, having })
    }
}

/// Canonical cell: numbers re-parsed and formatted to kill representation
/// noise (int vs float, 6.0 vs 6), NULL unified.
fn canon(cell: &str) -> String {
    if cell == "NULL" {
        return cell.to_string();
    }
    if let Ok(f) = cell.parse::<f64>() {
        if f == f.trunc() && f.abs() < 1e15 {
            return format!("{}", f as i64);
        }
        return format!("{f:.6}");
    }
    cell.to_string()
}

fn run_skai(db: &mut SkaiDb, sql: &str) -> Result<Vec<Vec<String>>, String> {
    match db.execute(sql).map_err(|e| e.to_string())? {
        QueryOutput::Rows(rs) => Ok(rs
            .rows
            .iter()
            .map(|r| r.iter().map(|v| canon(&render_raw(v))).collect())
            .collect()),
        other => Err(format!("expected rows, got {other:?}")),
    }
}

fn render_raw(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::Bool(b) => b.to_string(),
        Value::String(s) => s.clone(),
        Value::Timestamp(t) => t.to_string(),
        other => format!("{other:?}"),
    }
}

fn run_lite(conn: &rusqlite::Connection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let ncols = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut out = Vec::with_capacity(ncols);
            for i in 0..ncols {
                let v = row.get_ref(i)?;
                out.push(match v {
                    rusqlite::types::ValueRef::Null => "NULL".to_string(),
                    rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                    rusqlite::types::ValueRef::Real(f) => format!("{f:?}"),
                    rusqlite::types::ValueRef::Text(t) => {
                        String::from_utf8_lossy(t).into_owned()
                    }
                    rusqlite::types::ValueRef::Blob(_) => "<blob>".to_string(),
                });
            }
            Ok(out)
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows.into_iter().map(|r| r.into_iter().map(|c| canon(&c)).collect()).collect())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
    };
    let seconds: Option<u64> = get("--seconds").and_then(|s| s.parse().ok());
    let max_queries: u64 = get("--queries").and_then(|s| s.parse().ok()).unwrap_or(u64::MAX);
    let seed: u64 = get("--seed").and_then(|s| s.parse().ok()).unwrap_or(0xdecaf);
    let findings_dir = get("--findings").unwrap_or_else(|| "slt-findings".to_string());
    std::fs::create_dir_all(&findings_dir).expect("findings dir");
    let deadline = seconds.map(|s| Instant::now() + Duration::from_secs(s));
    let mut rng = StdRng::seed_from_u64(seed);
    let started = Instant::now();
    let (mut total, mut divergences, mut skai_errors) = (0u64, 0u64, 0u64);
    // Dedupe findings by (error-or-shape) signature so one bug doesn't
    // flood the dir.
    let mut seen: BTreeMap<String, u64> = BTreeMap::new();
    'outer: loop {
        let mut round = build_round(&mut rng);
        for _ in 0..500 {
            if total >= max_queries || deadline.is_some_and(|d| Instant::now() >= d) {
                break 'outer;
            }
            total += 1;
            let (sql, shape) = gen_query(&mut rng);
            let ordered = matches!(&shape, Shape::Plain { ordered: true, .. });
            let skai_res = run_skai(&mut round.skai, &sql);
            let lite_res = run_lite(&round.lite, &sql);
            let (mut s, mut l) = match (skai_res, lite_res) {
                (Ok(s), Ok(l)) => (s, l),
                (Err(e), Ok(_)) => {
                    skai_errors += 1;
                    let sig = format!("skai-error: {}", e.split(':').next().unwrap_or(&e));
                    let n = {
                        let n = seen.entry(sig.clone()).or_insert(0);
                        *n += 1;
                        *n
                    };
                    if n == 1 {
                        let idx = seen.len();
                        record(&findings_dir, &round.setup, &sql, &format!("skaidb ERROR: {e}"), "", idx);
                    }
                    continue;
                }
                // SQLite refusing our own dialect = generator bug; skip.
                (_, Err(_)) => continue,
            };
            if !ordered {
                s.sort();
                l.sort();
            }
            if s != l {
                divergences += 1;
                let sig = format!("diverge: {}", shape_sig(&shape));
                let n = {
                    let n = seen.entry(sig).or_insert(0);
                    *n += 1;
                    *n
                };
                if n <= 3 {
                    let idx = seen.len() * 10 + n as usize;
                    record(&findings_dir, &round.setup, &sql, &fmt_rows(&s), &fmt_rows(&l), idx);
                }
            }
        }
        if total % 10_000 < 500 {
            eprintln!(
                "[{:6.0}s] {} queries, {} divergences, {} skaidb-errors, {} unique signatures",
                started.elapsed().as_secs_f64(),
                total,
                divergences,
                skai_errors,
                seen.len()
            );
        }
    }
    eprintln!(
        "DONE: {} queries in {:.0}s — {} divergences, {} skaidb-errors, {} unique signatures -> {}",
        total,
        started.elapsed().as_secs_f64(),
        divergences,
        skai_errors,
        seen.len(),
        findings_dir
    );
    std::process::exit(if seen.is_empty() { 0 } else { 1 });
}

fn shape_sig(shape: &Shape) -> String {
    match shape {
        Shape::Plain { proj, ordered, limit } => {
            format!("plain proj={} ordered={ordered} limit={}", proj.join(","), limit.is_some())
        }
        Shape::Agg { group, aggs, having } => format!(
            "agg group={:?} aggs={} having={}",
            group,
            aggs.join(","),
            having.is_some()
        ),
    }
}

fn fmt_rows(rows: &[Vec<String>]) -> String {
    rows.iter().map(|r| r.join(" ")).collect::<Vec<_>>().join("\n")
}

fn record(dir: &str, setup: &[String], sql: &str, skai: &str, lite: &str, n: usize) {
    let mut out = String::new();
    let _ = writeln!(out, "# Differential finding — skaidb vs SQLite oracle.");
    let _ = writeln!(out, "# skaidb said:");
    for line in skai.lines() {
        let _ = writeln!(out, "#   {line}");
    }
    if !lite.is_empty() {
        let _ = writeln!(out, "# sqlite said:");
        for line in lite.lines() {
            let _ = writeln!(out, "#   {line}");
        }
    }
    let _ = writeln!(out);
    for s in setup {
        let _ = writeln!(out, "statement ok\n{s}\n");
    }
    let _ = writeln!(out, "query rowsort\n{sql}\n----");
    for line in lite.lines() {
        let _ = writeln!(out, "{line}");
    }
    let _ = std::fs::write(format!("{dir}/finding-{n:04}.slt"), out);
}
