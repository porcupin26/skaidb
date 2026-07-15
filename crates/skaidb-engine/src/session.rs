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

    /// Exotic search syntax must come back as a query error — `*:*` reached
    /// a panicking path inside tantivy's grammar, unwound through a request
    /// thread, and poisoned the node's auth lock (2026-07-15).
    #[test]
    fn hostile_search_syntax_errors_instead_of_panicking() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE t (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE SEARCH INDEX t_fts ON t (body);").unwrap();
        s.execute("INSERT INTO t (id, body) VALUES (1, 'hello world');").unwrap();
        for q in ["*:*", "field:", "[a TO", "\"unclosed"] {
            let out = s.execute(&format!("SELECT id FROM t WHERE SEARCH('{q}')"));
            assert!(out.is_err(), "hostile query {q:?} must error, not panic/succeed");
        }
        // The session (and its locks) survive to serve normal queries.
        let ok = s.execute("SELECT id FROM t WHERE MATCH(body, 'hello')").unwrap();
        assert_eq!(rows(ok).len(), 1);
    }

    /// A full composite-PK equality must be a point read — even when the
    /// key is ABSENT. LIMIT 1 masked this for present keys (the scan stopped
    /// early); a missing key walked the whole table into the scan budget
    /// (slack dedup probes, 2026-07-15).
    #[test]
    fn composite_pk_equality_is_a_point_read_even_when_absent() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 500,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE msgs (PRIMARY KEY (channel, ts));").unwrap();
        for i in 0..2000 {
            s.execute(&format!(
                "INSERT INTO msgs (channel, ts, body) VALUES ('c{:02}', 't{:06}', 'x');",
                i % 10,
                i
            ))
            .unwrap();
        }
        // Present key: one row.
        let got = rows(
            s.execute("SELECT body FROM msgs WHERE channel = 'c03' AND ts = 't000123' LIMIT 1;")
                .unwrap(),
        );
        assert_eq!(got.len(), 1);
        // Absent key: zero rows, NOT a scan-budget death (this walked 2000
        // rows past the 500-row budget before the fix).
        let got = rows(
            s.execute("SELECT body FROM msgs WHERE channel = 'c03' AND ts = 't999999' LIMIT 1;")
                .unwrap(),
        );
        assert_eq!(got.len(), 0);
        // A residual on a non-PK column must still be applied to the
        // point-read row (the caller re-filters).
        let got = rows(
            s.execute(
                "SELECT body FROM msgs WHERE channel = 'c03' AND ts = 't000123' AND body = 'nope';",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 0);
    }

    /// A leftmost PK-prefix equality (plus optional trailing range on the
    /// next PK column) must scan only that slice of the table — the slack
    /// thread-refresh shape (`channel = ?` on PK (channel, ts)) walked 252k
    /// rows into the scan budget instead of one channel (2026-07-15).
    #[test]
    fn pk_prefix_equality_scans_only_the_slice() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 500,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE msgs (PRIMARY KEY (channel, ts));").unwrap();
        // 2,000 rows across 10 channels; any whole-table walk dies at 500.
        for i in 0..2000 {
            s.execute(&format!(
                "INSERT INTO msgs (channel, ts, reply_count) VALUES ('c{:02}', 't{:06}', {});",
                i % 10,
                i,
                i % 5
            ))
            .unwrap();
        }
        // Prefix equality: reads one channel's 200 rows, well under budget.
        let got = rows(
            s.execute("SELECT ts FROM msgs WHERE channel = 'c03' AND reply_count > 0 ORDER BY ts DESC LIMIT 5;")
                .unwrap(),
        );
        assert_eq!(got.len(), 5);
        // Prefix + trailing range on the next PK column: narrower still.
        let got = rows(
            s.execute(
                "SELECT ts FROM msgs WHERE channel = 'c03' AND ts >= 't001500' ORDER BY ts DESC LIMIT 50;",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 50);
    }

    /// Multikey (`[]`) indexes: one entry per array element makes element
    /// equality an index probe. Counts are exact — entry keys embed the row
    /// key, so a duplicate element in one array collapses to one entry —
    /// and the planner refuses the index when the `[]` component is not
    /// equality-pinned (below that, a row surfaces once per element).
    #[test]
    fn multikey_index_serves_element_equality() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 900,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_al ON emails (account, labels[]);").unwrap();
        // Two multikey components is undefined blow-up — rejected.
        let err = s
            .execute("CREATE INDEX i_bad ON emails (labels[], tags[]);")
            .unwrap_err();
        assert!(err.to_string().contains("multikey"), "{err}");
        for i in 0..2000 {
            let labels = match i % 4 {
                0 => "['news', 'dev']",
                1 => "['news', 'news']", // duplicate element in ONE array
                2 => "['spam']",
                _ => "[]",
            };
            s.execute(&format!(
                "INSERT INTO emails (id, account, labels) VALUES ('k{i:05}', 'a@x', {labels});"
            ))
            .unwrap();
        }
        // Index-only exact count: 1000 rows carry 'news' (the dup-element
        // rows count ONCE). The 900-row budget proves no scan ran — the
        // streamed fallback would tick 2000 rows and die.
        let got = rows(
            s.execute("SELECT count(*) FROM emails WHERE account = 'a@x' AND labels = 'news';")
                .unwrap(),
        );
        assert_eq!(got, vec![vec![skaidb_types::Value::Int(1000)]]);
        // Row fetch through the index: each matching row exactly once.
        let got = rows(
            s.execute(
                "SELECT id FROM emails WHERE account = 'a@x' AND labels = 'spam' LIMIT 600;",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 500);
        let unique: std::collections::BTreeSet<_> =
            got.iter().map(|r| format!("{:?}", r[0])).collect();
        assert_eq!(unique.len(), 500, "no duplicate rows through the multikey index");
        // The [] component NOT equality-pinned: the planner must skip the
        // index (a range spans every element — duplicate rows, overcounts)
        // and the fallback scan dies at the budget, proving the gate held.
        let err = s
            .execute("SELECT count(*) FROM emails WHERE account = 'a@x' AND labels >= 'news';")
            .unwrap_err();
        assert!(err.to_string().contains("scan budget"), "{err}");
    }

    /// A grouped search on a non-fast-field column takes the exact-row
    /// fallback; on a large match set that gather must die at the scan
    /// budget with search-specific guidance — it must NOT silently run to
    /// the statement timeout (the 2026-07-15 coordinator tie-up). Small
    /// match sets keep answering correctly.
    #[test]
    fn grouped_search_fallback_is_budget_bounded() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 50,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE msgs (PRIMARY KEY (id));").unwrap();
        // `text` is analyzed; `user` is NOT on the index at all.
        s.execute("CREATE SEARCH INDEX msgs_fts ON msgs (text);").unwrap();
        for i in 0..300 {
            s.execute(&format!(
                "INSERT INTO msgs (id, text, \"user\") VALUES ({i}, 'hello world {i}', 'u{}');",
                i % 3
            ))
            .unwrap();
        }
        // 300 matches >> budget 50: the fallback gather errors fast with the
        // fix in the message instead of materializing everything.
        let err = s
            .execute("SELECT \"user\", count(*) FROM msgs WHERE MATCH(text, 'hello') GROUP BY \"user\";")
            .unwrap_err()
            .to_string();
        assert!(err.contains("scan budget"), "{err}");
        assert!(err.contains("keyword fast field"), "{err}");
        // A narrow match set stays under budget and answers row-side.
        let got = rows(
            s.execute(
                "SELECT \"user\", count(*) FROM msgs WHERE MATCH(text, '7') GROUP BY \"user\";",
            )
            .unwrap(),
        );
        assert!(!got.is_empty());
    }

    /// `SELECT 1` must work in ANY session database: name resolution used to
    /// qualify the FROM-less sentinel into `db.<nothing>`, so the liveness
    /// probe failed with `table "" does not exist` everywhere except the
    /// default database (found live by agencik, 2026-07-15).
    #[test]
    fn const_select_works_outside_default_database() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE DATABASE app;").unwrap();
        s.execute("USE app;").unwrap();
        let out = rows(s.execute("SELECT 1;").unwrap());
        assert_eq!(out, vec![vec![skaidb_types::Value::Int(1)]]);
    }

    /// `pk IN (...)` resolves to point reads, not a scan: with a scan budget
    /// far below the table size, the fetch-these-N-ids shape must still
    /// answer (a scan would trip the budget), including through a bound
    /// array parameter and on a composite key, and EXPLAIN must say so.
    #[test]
    fn pk_in_list_is_point_reads_not_a_scan() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 50,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE users (PRIMARY KEY (id));").unwrap();
        for i in 0..500 {
            s.execute(&format!("INSERT INTO users (id, grp) VALUES ({i}, {});", i % 7))
                .unwrap();
        }
        // Literal IN list: 3 point reads, well under the 50-row budget.
        let got = rows(
            s.execute("SELECT id FROM users WHERE id IN (7, 300, 499) ORDER BY id;")
                .unwrap(),
        );
        assert_eq!(
            got,
            vec![
                vec![skaidb_types::Value::Int(7)],
                vec![skaidb_types::Value::Int(300)],
                vec![skaidb_types::Value::Int(499)],
            ]
        );
        // Array element in the list — the exact shape a bound parameter
        // (`id IN (?)` with ? = [..]) takes after `bind` — incl. a miss.
        let got = rows(
            s.execute("SELECT id FROM users WHERE id IN ([2, 9999, 444]) ORDER BY id;")
                .unwrap(),
        );
        assert_eq!(
            got,
            vec![
                vec![skaidb_types::Value::Int(2)],
                vec![skaidb_types::Value::Int(444)],
            ]
        );
        // Residual predicates still apply on the fetched rows.
        let got = rows(
            s.execute("SELECT id FROM users WHERE id IN (7, 300, 499) AND grp = 0;")
                .unwrap(),
        );
        assert_eq!(got, vec![vec![skaidb_types::Value::Int(7)]]); // 7 % 7 == 0

        // The plan is visible: EXPLAIN names the point-read set.
        let plan = rows(
            s.execute("EXPLAIN SELECT id FROM users WHERE id IN (7, 300, 499);")
                .unwrap(),
        );
        let all = format!("{plan:?}");
        assert!(all.contains("point-read set"), "{all}");

        // NOT IN cannot use the key set (it excludes) — it scans and trips
        // the budget, proving the fast path is what answered above.
        let err = s
            .execute("SELECT id FROM users WHERE id NOT IN (1, 2);")
            .unwrap_err();
        assert!(err.to_string().contains("scan budget"), "{err}");
    }

    /// Composite key: `a = .. AND b IN (..)` expands the cross product.
    #[test]
    fn composite_pk_in_list_point_reads() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 50,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE msgs (PRIMARY KEY (chan, seq));").unwrap();
        for c in ["a", "b"] {
            for i in 0..300 {
                s.execute(&format!(
                    "INSERT INTO msgs (chan, seq, body) VALUES ('{c}', {i}, 'm{i}');"
                ))
                .unwrap();
            }
        }
        let got = rows(
            s.execute("SELECT seq FROM msgs WHERE chan = 'a' AND seq IN (5, 250) ORDER BY seq;")
                .unwrap(),
        );
        assert_eq!(
            got,
            vec![
                vec![skaidb_types::Value::Int(5)],
                vec![skaidb_types::Value::Int(250)],
            ]
        );
        // Both columns via IN: 2×2 cross product, still point reads.
        let got = rows(
            s.execute(
                "SELECT chan, seq FROM msgs WHERE chan IN ('a', 'b') AND seq IN (0, 299) ORDER BY chan, seq;",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 4);
    }

    /// The scan-budget error names the table and the filter columns, so the
    /// fix (which index to add) is mechanical. Context is attached at the
    /// statement boundary; the meter itself stays context-free.
    #[test]
    fn scan_budget_error_names_table_and_columns() {
        let mut opts = skaidb_storage::EngineOptions {
            scan_row_budget: 100,
            ..Default::default()
        };
        opts.statement_timeout_secs = 0;
        let mut s = Session::open_with_options(tmp(), opts).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        for i in 0..500 {
            s.execute(&format!(
                "INSERT INTO emails (id, sender, date) VALUES ('k{i:05}', 's{i}', {i});"
            ))
            .unwrap();
        }
        let err = s
            .execute("SELECT id FROM emails WHERE sender = 'nobody' AND date > 5;")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("scan budget exceeded"), "{msg}");
        assert!(msg.contains("table emails"), "{msg}");
        assert!(msg.contains("sender"), "{msg}");
        assert!(msg.contains("date"), "{msg}");
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
        // A sibling index pinning one more equality but spanning the whole
        // range must NOT beat the sorted plan when a limit bounds the walk —
        // this exact mis-pick turned a LIMIT-5 poll into 150k row reads.
        s.execute("CREATE INDEX i_af ON emails (account, flag);").unwrap();
        let t0 = std::time::Instant::now();
        let got = rows(
            s.execute(
                "SELECT id FROM emails WHERE account = 'a@x' AND flag = false ORDER BY date DESC LIMIT 3;",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 3);
        assert!(
            t0.elapsed().as_millis() < 200,
            "sorted plan not chosen under ORDER BY + LIMIT: {:?}",
            t0.elapsed()
        );
        // Residual filter never matches but a sibling equality index covers
        // it: the planner's range probe must see the empty range and answer
        // through the index — no walk, no budget death (the empty Archived
        // view burned 9.5 s walking 183k rows per click, 2026-07-14).
        let got = rows(
            s.execute(
                "SELECT id FROM emails WHERE account = 'a@x' AND flag = true ORDER BY date DESC LIMIT 3;",
            )
            .unwrap(),
        );
        assert_eq!(got.len(), 0);
        // Never-matching residual on a column NO index covers: the sorted
        // walk is the only plan, and the budget must bound it.
        let err = s
            .execute(
                "SELECT id FROM emails WHERE account = 'a@x' AND body = 'nope' ORDER BY date DESC LIMIT 3;",
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

    /// A reopened database must serve NEAREST from the persisted HNSW
    /// snapshot plus a watermark replay of rows written after it — including
    /// deletes — without a full rebuild.
    #[test]
    fn vector_snapshot_survives_reopen_with_replay() {
        let dir = tmp();
        let vec_lit = |seed: usize| -> String {
            // xorshift-mixed so distinct seeds give genuinely distinct
            // directions (a linear pattern collides under cosine).
            let mut x = (seed as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let v: Vec<String> = (0..8)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    format!("{:.6}", ((x >> 11) as f64 / (1u64 << 53) as f64) - 0.5)
                })
                .collect();
            format!("[{}]", v.join(", "))
        };
        {
            let mut s = Session::open(&dir).unwrap();
            s.execute("CREATE TABLE docs (PRIMARY KEY (id));").unwrap();
            for i in 0..50 {
                s.execute(&format!(
                    "INSERT INTO docs (id, emb) VALUES ('k{i}', {});",
                    vec_lit(i)
                ))
                .unwrap();
            }
            // DDL builds the graph and writes the snapshot.
            s.execute("CREATE VECTOR INDEX v_docs ON docs (emb) DIM 8 USING cosine;")
                .unwrap();
            // Post-snapshot writes: must reach the graph via watermark replay.
            for i in 50..70 {
                s.execute(&format!(
                    "INSERT INTO docs (id, emb) VALUES ('k{i}', {});",
                    vec_lit(i)
                ))
                .unwrap();
            }
            s.execute("DELETE FROM docs WHERE id = 'k3';").unwrap();
            // No explicit save: the reopen must replay these from the table.
        }
        let mut s = Session::open(&dir).unwrap();
        let n = rows(s.execute("SELECT COUNT(*) FROM docs;").unwrap());
        assert_eq!(n[0][0], skaidb_types::Value::Int(69), "table itself lost rows");
        // A post-snapshot row is findable.
        let got = rows(
            s.execute(&format!(
                "SELECT id FROM docs NEAREST (emb, {}, 1);",
                vec_lit(65)
            ))
            .unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::String("k65".into()));
        // The post-snapshot delete stays deleted.
        let got = rows(
            s.execute(&format!(
                "SELECT id FROM docs NEAREST (emb, {}, 50);",
                vec_lit(3)
            ))
            .unwrap(),
        );
        assert!(
            got.iter().all(|r| r[0] != skaidb_types::Value::String("k3".into())),
            "deleted row resurfaced from the snapshot"
        );
    }

    /// Multi-key ORDER BY with an indexed leading column must gather only
    /// limit + tie-group rows and return exactly the brute-force answer —
    /// the two-key UI sort previously defeated the index path and gathered
    /// every matching row.
    #[test]
    fn multi_key_order_by_uses_index_walk_with_ties() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_ad ON emails (account, date);").unwrap();
        for i in 0..400 {
            // Dates repeat every 8 rows: plenty of leading-key ties, and the
            // tiebreaker column deliberately disagrees with insert order.
            let date = format!("2026-01-{:02}", (i / 8) % 28 + 1);
            let scraped = format!("s{:04}", (i * 37) % 400);
            s.execute(&format!(
                "INSERT INTO emails (id, account, date, scraped_at) VALUES ('k{i:04}', 'a@x', '{date}', '{scraped}');"
            ))
            .unwrap();
        }
        // Brute force: fetch all, sort in the test.
        let mut all: Vec<(String, String, String)> = rows(
            s.execute("SELECT id, date, scraped_at FROM emails WHERE account = 'a@x';").unwrap(),
        )
        .into_iter()
        .map(|r| match (&r[0], &r[1], &r[2]) {
            (
                skaidb_types::Value::String(a),
                skaidb_types::Value::String(b),
                skaidb_types::Value::String(c),
            ) => (b.clone(), c.clone(), a.clone()),
            other => panic!("{other:?}"),
        })
        .collect();
        all.sort_by(|x, y| y.0.cmp(&x.0).then(y.1.cmp(&x.1))); // date DESC, scraped DESC
        let want: Vec<String> = all.iter().take(7).map(|(_, _, id)| id.clone()).collect();
        let got: Vec<String> = rows(
            s.execute(
                "SELECT id FROM emails WHERE account = 'a@x' ORDER BY date DESC, scraped_at DESC LIMIT 7;",
            )
            .unwrap(),
        )
        .into_iter()
        .map(|r| match &r[0] {
            skaidb_types::Value::String(a) => a.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
        assert_eq!(got, want, "multi-key order diverged from brute force");
    }

    /// Filtered COUNT(*) that no covering index serves (a `!=` in the
    /// filter) must still answer — via the streaming count, and exactly.
    #[test]
    fn non_covering_filtered_count_streams() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_a ON emails (account, tomb);").unwrap();
        for i in 0..500 {
            let arch = i % 5 == 0;
            s.execute(&format!(
                "INSERT INTO emails (id, account, tomb, archived) VALUES ('k{i}', 'a@x', false, {arch});"
            ))
            .unwrap();
        }
        let got = rows(
            s.execute(
                "SELECT COUNT(*) FROM emails WHERE account = 'a@x' AND tomb = false AND archived != true;",
            )
            .unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::Int(400));
        // With (account, tomb, archived) indexed, the bare `!=` still streams
        // (SQL semantics exclude nulls; the complement can't) — exactness is
        // what matters here.
        s.execute("CREATE INDEX i_arch ON emails (account, tomb, archived);").unwrap();
        let got = rows(
            s.execute(
                "SELECT COUNT(*) FROM emails WHERE account = 'a@x' AND tomb = false AND archived != true;",
            )
            .unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::Int(400));
        // Bare negated-eq as the WHOLE filter: streamed, null-excluding.
        let got = rows(
            s.execute("SELECT COUNT(*) FROM emails WHERE archived != true;").unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::Int(400));
        // The NULL-safe form Mongo-semantics adapters emit — rows lacking the
        // column count as "not equal". Insert some column-less rows: the
        // complement must include them; the bare != must not.
        for i in 500..520 {
            s.execute(&format!(
                "INSERT INTO emails (id, account, tomb) VALUES ('k{i}', 'a@x', false);"
            ))
            .unwrap();
        }
        let got = rows(
            s.execute(
                "SELECT COUNT(*) FROM emails WHERE account = 'a@x' AND tomb = false AND (archived != true OR archived IS NULL);",
            )
            .unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::Int(420));
        let got = rows(
            s.execute(
                "SELECT COUNT(*) FROM emails WHERE account = 'a@x' AND tomb = false AND archived != true;",
            )
            .unwrap(),
        );
        assert_eq!(got[0][0], skaidb_types::Value::Int(400), "bare != must exclude NULLs");
    }

    /// SELECT DISTINCT <col> streams the value set (arrays dedupe as whole
    /// values, like the gather it replaces) and respects the filter.
    #[test]
    fn distinct_single_column_streams() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE emails (PRIMARY KEY (id));").unwrap();
        for i in 0..300 {
            let labels = match i % 4 {
                0 => "['work']",
                1 => "['home','work']",
                2 => "['spam']",
                _ => "['home','work']",
            };
            let tomb = i % 10 == 0;
            s.execute(&format!(
                "INSERT INTO emails (id, labels, tomb) VALUES ('k{i}', {labels}, {tomb});"
            ))
            .unwrap();
        }
        let got = rows(
            s.execute("SELECT DISTINCT labels FROM emails WHERE tomb = false;").unwrap(),
        );
        // three distinct arrays survive the filter
        assert_eq!(got.len(), 3, "{got:?}");
    }

    /// UPDATE with an unchanged primary key must be a single overwrite that
    /// keeps secondary indexes exact; a PK-changing UPDATE must move the row
    /// (new key exists, old key gone, index follows). Guards the fix for
    /// the delete-then-put pair that lost rows when the put half failed.
    #[test]
    fn update_overwrites_and_moves_keys_with_indexes() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE t (PRIMARY KEY (id));").unwrap();
        s.execute("CREATE INDEX i_v ON t (v);").unwrap();
        s.execute("INSERT INTO t (id, v, x) VALUES ('a', 'old', 1);").unwrap();
        // Key-stable update: value moves in the index, row intact.
        s.execute("UPDATE t SET v = 'new' WHERE id = 'a';").unwrap();
        let got = rows(s.execute("SELECT id FROM t WHERE v = 'new';").unwrap());
        assert_eq!(got.len(), 1);
        let got = rows(s.execute("SELECT id FROM t WHERE v = 'old';").unwrap());
        assert_eq!(got.len(), 0, "stale index entry after overwrite");
        // PK-changing update: row moves keys.
        s.execute("UPDATE t SET id = 'b' WHERE id = 'a';").unwrap();
        let got = rows(s.execute("SELECT v FROM t WHERE id = 'b';").unwrap());
        assert_eq!(got.len(), 1);
        let got = rows(s.execute("SELECT v FROM t WHERE id = 'a';").unwrap());
        assert_eq!(got.len(), 0, "old key survived a PK-changing update");
        let got = rows(s.execute("SELECT id FROM t WHERE v = 'new';").unwrap());
        assert_eq!(got[0][0], skaidb_types::Value::String("b".into()));
    }

    /// Array literals with scientific-notation floats — the form default
    /// float formatting emits below 1e-4 — must parse and round-trip.
    #[test]
    fn scientific_floats_in_array_literals() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE v (PRIMARY KEY (id));").unwrap();
        s.execute("INSERT INTO v (id, emb) VALUES ('a', [1.2e-05, 3E2, -4.5e+1, 0.25]);")
            .unwrap();
        let got = rows(s.execute("SELECT emb FROM v WHERE id = 'a';").unwrap());
        match &got[0][0] {
            skaidb_types::Value::Array(items) => {
                let f = |v: &skaidb_types::Value| match v {
                    skaidb_types::Value::Float(f) => *f,
                    skaidb_types::Value::Int(i) => *i as f64,
                    other => panic!("{other:?}"),
                };
                assert!((f(&items[0]) - 1.2e-05).abs() < 1e-15);
                assert!((f(&items[1]) - 300.0).abs() < 1e-9);
                assert!((f(&items[2]) + 45.0).abs() < 1e-9);
            }
            other => panic!("{other:?}"),
        }
    }

    /// Memory tables: normal reads/writes and TTL, nothing survives reopen,
    /// and indexes on them are rejected.
    #[test]
    fn memory_tables_are_ephemeral() {
        let dir = tmp();
        {
            let mut s = Session::open(&dir).unwrap();
            s.execute("CREATE TABLE stats (PRIMARY KEY (node)) WITH (memory = true);")
                .unwrap();
            for i in 0..50 {
                s.execute(&format!(
                    "INSERT INTO stats (node, cpu) VALUES ('n{i}', {i});"
                ))
                .unwrap();
            }
            let got = rows(s.execute("SELECT COUNT(*) FROM stats;").unwrap());
            assert_eq!(got[0][0], skaidb_types::Value::Int(50));
            // Overwrite on PK works like any table.
            s.execute("INSERT INTO stats (node, cpu) VALUES ('n1', 99);").unwrap();
            let got = rows(s.execute("SELECT cpu FROM stats WHERE node = 'n1';").unwrap());
            assert_eq!(got[0][0], skaidb_types::Value::Int(99));
            // Indexes are rejected.
            let err = s.execute("CREATE INDEX i_c ON stats (cpu);").unwrap_err();
            assert!(err.to_string().contains("memory tables"), "{err}");
        }
        // Reopen: table exists (schema persists), data does not.
        let mut s = Session::open(&dir).unwrap();
        let got = rows(s.execute("SELECT COUNT(*) FROM stats;").unwrap());
        assert_eq!(got[0][0], skaidb_types::Value::Int(0), "memory table data survived reopen");
        s.execute("INSERT INTO stats (node, cpu) VALUES ('again', 1);").unwrap();
        let got = rows(s.execute("SELECT COUNT(*) FROM stats;").unwrap());
        assert_eq!(got[0][0], skaidb_types::Value::Int(1));
    }

    /// `array_col = scalar` is Mongo-style containment (and != is
    /// not-contains); array-to-array stays whole-value equality.
    #[test]
    fn array_column_scalar_equality_is_containment() {
        let mut s = Session::open(tmp()).unwrap();
        s.execute("CREATE TABLE m (PRIMARY KEY (id));").unwrap();
        s.execute("INSERT INTO m (id, labels) VALUES ('a', ['work','urgent']);").unwrap();
        s.execute("INSERT INTO m (id, labels) VALUES ('b', ['home']);").unwrap();
        s.execute("INSERT INTO m (id, labels) VALUES ('c', []);").unwrap();
        let got = rows(s.execute("SELECT id FROM m WHERE labels = 'work';").unwrap());
        assert_eq!(got.len(), 1);
        assert_eq!(got[0][0], skaidb_types::Value::String("a".into()));
        let got = rows(s.execute("SELECT id FROM m WHERE labels != 'work';").unwrap());
        assert_eq!(got.len(), 2, "{got:?}");
        let got = rows(s.execute("SELECT COUNT(*) FROM m WHERE labels = 'home';").unwrap());
        assert_eq!(got[0][0], skaidb_types::Value::Int(1));
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
