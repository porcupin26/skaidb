//! Embedded multi-database session.
//!
//! A thin, stateful wrapper over a single [`Database`] that remembers a *current
//! database* and applies `USE`. Databases themselves are a namespace layer in
//! the engine (see [`crate::namespace`]): all of a session's databases share one
//! storage engine, with table/index names prefixed per database. The clustered
//! server keeps its own current-database state per connection and drives the
//! same [`Database::execute_session`] entry point.

use std::path::Path;

use crate::error::Result;
use crate::namespace::DEFAULT_DATABASE;
use crate::{Database, QueryOutput, SessionEffect};

/// An embedded session: one database engine plus a current-database pointer.
#[derive(Debug)]
pub struct Session {
    db: Database,
    current: String,
}

impl Session {
    /// Open the database rooted at `dir`; the current database starts as
    /// `default`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Session> {
        Ok(Session {
            db: Database::open(dir)?,
            current: DEFAULT_DATABASE.to_string(),
        })
    }

    /// [`Session::open`] with explicit engine options (tests tune the scan
    /// meter; embedded callers tune storage knobs).
    pub fn open_with_options(
        dir: impl AsRef<Path>,
        opts: skaidb_storage::EngineOptions,
    ) -> Result<Session> {
        Ok(Session {
            db: Database::open_with_options(dir, opts)?,
            current: DEFAULT_DATABASE.to_string(),
        })
    }

    /// The database statements currently resolve against.
    pub fn current_database(&self) -> &str {
        &self.current
    }

    /// Parse and execute one statement against the current database, applying
    /// `USE` to the session's current-database pointer.
    pub fn execute(&mut self, sql: &str) -> Result<QueryOutput> {
        let effect = self.db.execute_session(&self.current, sql)?;
        // `DROP DATABASE` of the current database (directly or via cascade) leaves
        // the pointer dangling — fall back to `default`.
        if !self.db.has_database(&self.current) {
            self.current = DEFAULT_DATABASE.to_string();
        }
        match effect {
            SessionEffect::Output(out) => Ok(out),
            SessionEffect::UseDatabase(name) => {
                self.current = name;
                Ok(QueryOutput::Ddl)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EngineError;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "skaidb-session-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn rows(out: QueryOutput) -> Vec<Vec<skaidb_types::Value>> {
        match out {
            QueryOutput::Rows(r) => r.rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    /// Filtered `COUNT(*)` served index-only from a covering secondary index
    /// must agree with the gather path across inserts, updates and deletes —
    /// the count_documents shape that wedged two production coordinators.
    #[test]
    fn covering_index_count_matches_gather() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_acct ON emails (account, tomb, is_read);")
            .unwrap();
        for i in 0..300 {
            let acct = if i % 3 == 0 { "a@x" } else { "b@x" };
            let tomb = i % 7 == 0;
            let read = i % 2 == 0;
            s.execute(&format!(
                "INSERT INTO emails (id, account, tomb, is_read, n) VALUES ('k{i}', '{acct}', {tomb}, {read}, {i});"
            ))
            .unwrap();
        }
        // Mutate: flip some is_read (index entries must move), delete some rows.
        for i in (0..300).step_by(11) {
            s.execute(&format!("UPDATE emails SET is_read = true WHERE id = 'k{i}';"))
                .unwrap();
        }
        for i in (0..300).step_by(13) {
            s.execute(&format!("DELETE FROM emails WHERE id = 'k{i}';")).unwrap();
        }
        let count = |s: &mut Session, sql: &str| -> i64 {
            match &rows(s.execute(sql).unwrap())[0][0] {
                skaidb_types::Value::Int(n) => *n,
                other => panic!("expected int, got {other:?}"),
            }
        };
        // Covered shapes (index-only) vs the same predicate counted the slow
        // way (SELECT id + client-side len) — must agree exactly.
        for pred in [
            "account = 'b@x' AND tomb = false",
            "account = 'b@x' AND tomb = false AND is_read = false",
            "account = 'a@x'",
        ] {
            let fast = count(&mut s, &format!("SELECT COUNT(*) FROM emails WHERE {pred};"));
            let slow = rows(
                s.execute(&format!("SELECT id FROM emails WHERE {pred};")).unwrap(),
            )
            .len() as i64;
            assert_eq!(fast, slow, "covered count diverged for: {pred}");
        }
        // Non-covered shapes must still answer correctly via the gather path:
        // residual un-indexed column, and a numeric literal (coercion-unsafe
        // for index probes by design).
        let fast = count(
            &mut s,
            "SELECT COUNT(*) FROM emails WHERE account = 'b@x' AND n = 43;",
        );
        assert_eq!(fast, 1);
        let fast = count(&mut s, "SELECT COUNT(*) FROM emails WHERE n < 10;");
        let slow = rows(s.execute("SELECT id FROM emails WHERE n < 10;").unwrap()).len() as i64;
        assert_eq!(fast, slow);
    }

    /// `ORDER BY <indexed col> [DESC] LIMIT n` must return exactly the
    /// brute-force answer via the index plan — including the DESC tail-walk —
    /// across updates and deletes (the newest/oldest-per-account shape).
    #[test]
    fn ordered_index_scan_asc_and_desc() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_acct_date ON emails (account, date);").unwrap();
        for i in 0..200 {
            let acct = if i % 3 == 0 { "a@x" } else { "b@x" };
            // Deliberately not insertion-ordered dates.
            let date = format!("2026-01-{:02}T{:02}:00:00+00:00", (i * 7) % 28 + 1, i % 24);
            s.execute(&format!(
                "INSERT INTO emails (id, account, date) VALUES ('k{i}', '{acct}', '{date}');"
            ))
            .unwrap();
        }
        for i in (0..200).step_by(17) {
            s.execute(&format!("DELETE FROM emails WHERE id = 'k{i}';")).unwrap();
        }
        // Brute force over a projection without ORDER BY.
        let mut dates: Vec<String> = rows(
            s.execute("SELECT date FROM emails WHERE account = 'b@x';").unwrap(),
        )
        .into_iter()
        .map(|r| match &r[0] {
            skaidb_types::Value::String(d) => d.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
        dates.sort();
        for (dir, want) in [("ASC", dates.first().unwrap()), ("DESC", dates.last().unwrap())] {
            let got = rows(
                s.execute(&format!(
                    "SELECT date FROM emails WHERE account = 'b@x' ORDER BY date {dir} LIMIT 1;"
                ))
                .unwrap(),
            );
            assert_eq!(got.len(), 1, "{dir}");
            assert_eq!(
                got[0][0],
                skaidb_types::Value::String(want.clone()),
                "{dir} head diverged from brute force"
            );
        }
        // Top-5 DESC must equal the brute-force tail reversed.
        let got: Vec<String> = rows(
            s.execute(
                "SELECT date FROM emails WHERE account = 'b@x' ORDER BY date DESC LIMIT 5;",
            )
            .unwrap(),
        )
        .into_iter()
        .map(|r| match &r[0] {
            skaidb_types::Value::String(d) => d.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
        let want: Vec<String> = dates.iter().rev().take(5).cloned().collect();
        assert_eq!(got, want);
    }

    /// The production dedup shape must plan onto its composite index —
    /// probe timing: an indexed point lookup on 20k rows is microseconds;
    /// a fallback full scan is not.
    #[test]
    fn dedup_shape_plans_onto_composite_index() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE gmail_emails (PRIMARY KEY (id));").unwrap();
        // The production index set: sibling indexes whose leading column
        // also appears in the dedup filter. Planner must pick the index
        // consuming BOTH equalities, not whichever HashMap yields first —
        // the account-only prefix matches ~half the table.
        s.execute("CREATE INDEX i_status ON gmail_emails (account, tomb, is_read);").unwrap();
        s.execute("CREATE INDEX i_date ON gmail_emails (account, date);").unwrap();
        s.execute("CREATE INDEX i_dedup ON gmail_emails (gmail_id, account);").unwrap();
        for i in 0..20000 {
            s.execute(&format!(
                "INSERT INTO gmail_emails (id, gmail_id, account, body) VALUES ('k{i}', 'g{i}', 'a@x', 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx');"
            ))
            .unwrap();
        }
        let t0 = std::time::Instant::now();
        for _ in 0..50 {
            let got = rows(
                s.execute(
                    "SELECT id FROM gmail_emails WHERE \"gmail_id\" = 'g19999' AND \"account\" = 'a@x' LIMIT 1;",
                )
                .unwrap(),
            );
            assert_eq!(got.len(), 1);
        }
        let dt = t0.elapsed();
        assert!(dt.as_millis() < 500, "50 indexed point lookups took {dt:?} — not using the index");
    }

    /// A filter matching nothing under ORDER BY .. LIMIT walks the whole
    /// index range; the scan budget must turn that into an error instead of
    /// unbounded work (the categorizer-poll shape that OOM-looped
    /// production, 2026-07-13). Queries that fill their limit early stay
    /// well under budget and are unaffected.
    #[test]
    fn scan_budget_bounds_never_matching_walks() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 500,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_ad ON emails (account, date);").unwrap();
        for i in 0..2000 {
            s.execute(&format!(
                "INSERT INTO emails (id, account, date, flag) VALUES ('k{i:05}', 'a@x', 'd{i:05}', false);"
            ))
            .unwrap();
        }
        // Fills LIMIT immediately: unaffected by the budget.
        let ok = s.execute(
            "SELECT id FROM emails WHERE account = 'a@x' ORDER BY date DESC LIMIT 3;",
        );
        assert_eq!(rows(ok.unwrap()).len(), 3);
        // Residual filter never matches: the walk must die at the budget.
        let err = s
            .execute(
                "SELECT id FROM emails WHERE account = 'a@x' AND flag = true ORDER BY date DESC LIMIT 3;",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("scan budget"),
            "expected scan-budget error, got: {err}"
        );
        // A later statement gets a fresh meter.
        let ok = s.execute("SELECT id FROM emails WHERE id = 'k00007';").unwrap();
        assert_eq!(rows(ok).len(), 1);
    }

    #[test]
    fn default_is_current_on_open() {
        let s = Session::open(tmp()).unwrap();
        assert_eq!(s.current_database(), DEFAULT_DATABASE);
    }

    #[test]
    fn create_use_and_isolate_tables() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE DATABASE shop;").unwrap();
        s.execute("USE shop;").unwrap();
        assert_eq!(s.current_database(), "shop");
        s.execute("CREATE TABLE orders (PRIMARY KEY (id));").unwrap();
        s.execute("INSERT INTO orders (id) VALUES (1);").unwrap();

        // `orders` exists in shop but not in default.
        assert_eq!(rows(s.execute("SHOW TABLES;").unwrap()).len(), 1);
        s.execute("USE default;").unwrap();
        assert!(rows(s.execute("SHOW TABLES;").unwrap()).is_empty());
        // The table is unreachable by bare name from default...
        assert!(s.execute("SELECT id FROM orders;").is_err());
        // ...but reachable via an explicit qualifier from anywhere.
        assert_eq!(rows(s.execute("SELECT id FROM shop.orders;").unwrap()).len(), 1);
    }

    #[test]
    fn databases_and_data_persist_across_reopen() {
        let dir = tmp();
        {
            let mut s = Session::open(&dir).unwrap();
            s.execute("CREATE DATABASE analytics;").unwrap();
            s.execute("USE analytics;").unwrap();
            s.execute("CREATE TABLE events (PRIMARY KEY (id));").unwrap();
            s.execute("INSERT INTO events (id, kind) VALUES (1, 'click');")
                .unwrap();
        }
        let mut s = Session::open(&dir).unwrap();
        // The database is still listed and its data is intact.
        assert_eq!(rows(s.execute("SHOW DATABASES;").unwrap()).len(), 2); // default + analytics
        s.execute("USE analytics;").unwrap();
        assert_eq!(
            rows(s.execute("SELECT kind FROM events WHERE id = 1;").unwrap()),
            vec![vec![skaidb_types::Value::String("click".into())]]
        );
    }

    #[test]
    fn drop_database_cascades_and_clears_current() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE DATABASE staging;").unwrap();
        s.execute("USE staging;").unwrap();
        s.execute("CREATE TABLE t (PRIMARY KEY (id));").unwrap();
        s.execute("INSERT INTO t (id) VALUES (1);").unwrap();
        s.execute("DROP DATABASE staging;").unwrap();
        // Dropping the current database reverts to default.
        assert_eq!(s.current_database(), DEFAULT_DATABASE);
        // Recreating it gives a clean, empty namespace (cascade really deleted).
        s.execute("CREATE DATABASE staging;").unwrap();
        s.execute("USE staging;").unwrap();
        assert!(rows(s.execute("SHOW TABLES;").unwrap()).is_empty());
    }

    #[test]
    fn duplicate_and_missing_errors() {
        let mut s = Session::open(tmp()).unwrap();
        assert!(s.execute("DROP DATABASE default;").is_err());
        s.execute("CREATE DATABASE d1;").unwrap();
        assert!(matches!(
            s.execute("CREATE DATABASE d1;"),
            Err(EngineError::DatabaseExists(_))
        ));
        s.execute("CREATE DATABASE IF NOT EXISTS d1;").unwrap();
        assert!(matches!(
            s.execute("USE nope;"),
            Err(EngineError::DatabaseNotFound(_))
        ));
    }
}
