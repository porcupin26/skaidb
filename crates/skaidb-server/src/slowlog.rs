//! A bounded in-memory ring of recent slow queries (SPEC §10).
//!
//! The server already counts slow queries (`skaidb_slow_queries_total`); this
//! keeps a small, masked sample of the most recent ones so an operator can drill
//! down via `GET/POST /admin/slow` without turning on full query logging. It is
//! capped, masked, and holds no result data.

use std::collections::VecDeque;
use std::sync::Mutex;

use serde_json::{json, Value as Json};

/// How many recent slow queries to retain.
const CAPACITY: usize = 64;

/// One recorded slow query: a masked statement and how long it took.
#[derive(Debug, Clone)]
struct Entry {
    sql: String,
    elapsed_ms: u64,
    /// Monotonic sequence number, so consumers can tell ordering/drops.
    seq: u64,
}

/// A thread-safe bounded ring of recent slow queries.
#[derive(Debug, Default)]
pub struct SlowLog {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: VecDeque<Entry>,
    next_seq: u64,
}

impl SlowLog {
    pub fn new() -> SlowLog {
        SlowLog::default()
    }

    /// Record a (already masked) slow statement and its elapsed time.
    pub fn record(&self, masked_sql: &str, elapsed_ms: u64) {
        let mut inner = self.inner.lock().expect("slow log");
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.entries.push_back(Entry {
            sql: masked_sql.to_string(),
            elapsed_ms,
            seq,
        });
        while inner.entries.len() > CAPACITY {
            inner.entries.pop_front();
        }
    }

    /// The retained sample, newest first, as JSON for an admin endpoint.
    pub fn snapshot(&self) -> Json {
        let inner = self.inner.lock().expect("slow log");
        let items: Vec<Json> = inner
            .entries
            .iter()
            .rev()
            .map(|e| json!({"seq": e.seq, "elapsed_ms": e.elapsed_ms, "sql": e.sql}))
            .collect();
        json!({ "slow_queries": items, "capacity": CAPACITY })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_recent_and_bounds_capacity() {
        let log = SlowLog::new();
        for i in 0..(CAPACITY as u64 + 10) {
            log.record(&format!("SELECT {i}"), i);
        }
        let snap = log.snapshot();
        let items = snap["slow_queries"].as_array().unwrap();
        assert_eq!(items.len(), CAPACITY);
        // Newest first: the last recorded is at the front.
        assert_eq!(items[0]["seq"], json!(CAPACITY as u64 + 9));
    }
}
