//! End-to-end vector search walkthrough: vectorize documents, store them,
//! index them, and run nearest-neighbor search — all in skaidb.
//!
//!   cargo run --bin vector_search -- [host:port] [user] [password]
//!
//! Uses a small deterministic "hashing trick" bag-of-bigrams vectorizer (see
//! `vectorize` below) so the example has no ML dependency and produces the
//! same vectors every run. In a real application, replace `vectorize` with a
//! call to a real embedding model/API — everything downstream (storing,
//! indexing, searching) is identical regardless of where the vector comes
//! from.
//!
//! Unlike the other language examples (whose client-side drivers only bind
//! scalar parameter types today), the native protocol's `Value` type
//! supports arrays directly — so this example binds the query vector
//! type-safely through a prepared statement instead of formatting it into
//! the SQL text.

use skaidb_driver::Client;
use skaidb_proto::Response;
use skaidb_types::Value;

const DIM: usize = 32; // embedding dimension — must match CREATE VECTOR INDEX ... DIM

/// FNV-1a over UTF-8 bytes: deterministic across runs, unlike Rust's default
/// `Hash` (randomly seeded per-process for DoS resistance), which would make
/// this example's vectors change every run.
fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// Toy embedding: hash each character bigram into one of DIM buckets and
/// count occurrences, then L2-normalize (so cosine distance is meaningful).
fn vectorize(text: &str) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    let lower = text.to_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    for w in chars.windows(2) {
        let bigram: String = w.iter().collect();
        let bucket = (fnv1a(&bigram) as usize) % DIM;
        v[bucket] += 1.0;
    }
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn as_value_array(v: &[f32]) -> Value {
    Value::Array(v.iter().map(|&x| Value::Float(x as f64)).collect())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).map(String::as_str).unwrap_or("localhost:7000");
    let user = args.get(2).map(String::as_str).unwrap_or("anonymous");
    let pass = args.get(3).map(String::as_str).unwrap_or("");
    let mut client = Client::connect_with(addr, user, pass).expect("connect");

    let docs = [
        (1i64, "tech", "The new GPU doubles inference throughput for transformer models."),
        (2, "tech", "Kubernetes autoscaling reduced our cluster's idle compute cost."),
        (3, "cooking", "Simmer the tomato sauce for twenty minutes before adding basil."),
        (4, "cooking", "A cast iron skillet gives the steak a perfect crust."),
        (5, "tech", "The database's read cache cut point-query latency significantly."),
    ];

    // --- Schema: a normal table plus a vector index on its embedding field ---
    client.execute("DROP TABLE IF EXISTS docs").unwrap();
    client.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
    client
        .execute(&format!(
            "CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM {DIM} USING cosine"
        ))
        .unwrap();

    // --- Vectorize each document's text and insert it alongside the text,
    //     binding the embedding as a typed Value::Array parameter ---
    let mut insert = client
        .prepare("INSERT INTO docs (id, category, text, embedding) VALUES (?, ?, ?, ?)")
        .unwrap();
    for (id, category, text) in docs {
        let vec = vectorize(text);
        client
            .execute_prepared(
                &mut insert,
                &[
                    Value::Int(id),
                    Value::String(category.into()),
                    Value::String(text.into()),
                    as_value_array(&vec),
                ],
            )
            .unwrap();
    }
    println!("indexed {} documents", docs.len());

    // --- Nearest-neighbor search: vectorize the query the same way, then
    //     ask for the k closest documents ---
    let query = "GPU memory bandwidth limits model throughput";
    let query_vec = as_value_array(&vectorize(query));
    let mut nearest = client
        .prepare("SELECT id, category, text, _distance FROM docs NEAREST (embedding, ?, ?)")
        .unwrap();
    match client
        .execute_prepared(&mut nearest, &[query_vec.clone(), Value::Int(3)])
        .unwrap()
    {
        Response::Rows { rows, .. } => {
            println!("\nnearest to: {query:?}");
            for row in rows {
                println!("  {row:?}");
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // --- Filtered nearest-neighbor search: WHERE narrows the candidate set,
    //     results are still nearest-first ---
    let mut filtered = client
        .prepare(
            "SELECT id, text, _distance FROM docs \
             NEAREST (embedding, ?, ?) WHERE category = ?",
        )
        .unwrap();
    match client
        .execute_prepared(
            &mut filtered,
            &[query_vec, Value::Int(3), Value::String("cooking".into())],
        )
        .unwrap()
    {
        Response::Rows { rows, .. } => {
            println!("\nnearest to: {query:?} (category = 'cooking' only)");
            for row in rows {
                println!("  {row:?}");
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    client.execute("DROP VECTOR INDEX docs_emb").unwrap();
    client.execute("DROP TABLE docs").unwrap();
}
