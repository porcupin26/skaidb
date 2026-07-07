//! skaidb query engine: binds parsed SQL to the storage engine (SPEC §3).
//!
//! [`Database`] is an embeddable engine — open a directory, then [`Database::execute`]
//! SQL statements and get back a [`QueryOutput`].

pub mod catalog;
mod error;
mod eval;
mod exec;
pub mod namespace;
mod result;
mod session;
mod ts_query;
pub mod vector;

pub use error::EngineError;
pub use exec::{
    filter_rows, run, statement_is_read_only, Cluster, Database, DbStats, IndexScanRange,
    pk_point_key, TableStats, TsRollupInfo,
};
pub use skaidb_storage::{Codec, EngineOptions};
pub use namespace::DEFAULT_DATABASE;
pub use result::{QueryOutput, ResultSet, SessionEffect};
pub use session::Session;
pub use ts_query::{ts_partialize, TsPartial};

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_types::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tempdir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-engine-it-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rows(out: QueryOutput) -> ResultSet {
        match out {
            QueryOutput::Rows(rs) => rs,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn affected(out: QueryOutput) -> usize {
        match out {
            QueryOutput::Mutation { affected } => affected,
            other => panic!("expected mutation, got {other:?}"),
        }
    }

    #[test]
    fn create_insert_select_roundtrip() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE users (PRIMARY KEY (id))").unwrap();
        assert_eq!(
            affected(
                db.execute("INSERT INTO users (id, name) VALUES (1, 'ada'), (2, 'bob')")
                    .unwrap()
            ),
            2
        );

        let rs = rows(
            db.execute("SELECT id, name FROM users ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.columns, vec!["id", "name"]);
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0], vec![Value::Int(1), Value::String("ada".into())]);
        assert_eq!(rs.rows[1], vec![Value::Int(2), Value::String("bob".into())]);
    }

    /// Shared fixture: a TS table with two hosts sampling `value` every 15 s
    /// for two minutes (values rise 1/s), plus a `temp` field on host a.
    fn ts_fixture(db: &mut Database) {
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host, core), RETENTION 30d)")
            .unwrap();
        for host in ["a", "b"] {
            for i in 0..8i64 {
                let ts = i * 15_000;
                let sql = format!(
                    "INSERT INTO cpu (host, core, ts, value{extra}) VALUES ('{host}', '0', {ts}, {v}{extra_v})",
                    v = i * 15,
                    extra = if host == "a" { ", temp" } else { "" },
                    extra_v = if host == "a" { format!(", {}", 50 + i) } else { String::new() },
                );
                assert_eq!(affected(db.execute(&sql).unwrap()), 1);
            }
        }
    }

    #[test]
    fn timeseries_create_insert_select_roundtrip() {
        let mut db = Database::open(tempdir()).unwrap();
        ts_fixture(&mut db);

        // Raw range read with label + ts pushdown, ordered by time.
        let rs = rows(
            db.execute(
                "SELECT ts, value FROM cpu \
                 WHERE host = 'a' AND ts >= 30000 AND ts < 90000 ORDER BY ts",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["ts", "value"]);
        assert_eq!(rs.rows.len(), 4); // 30000, 45000, 60000, 75000
        assert_eq!(rs.rows[0], vec![Value::Timestamp(30_000), Value::Float(30.0)]);
        assert_eq!(rs.rows[3], vec![Value::Timestamp(75_000), Value::Float(75.0)]);

        // Wildcard shows labels + ts + all fields; the hidden series id
        // stays hidden.
        let rs = rows(
            db.execute("SELECT * FROM cpu WHERE host = 'a' AND ts = 0")
                .unwrap(),
        );
        assert!(rs.columns.contains(&"host".to_string()));
        assert!(rs.columns.contains(&"temp".to_string()));
        assert!(!rs.columns.iter().any(|c| c.starts_with("__")));

        // SHOW TABLES lists it with the implicit key.
        let rs = rows(db.execute("SHOW TABLES").unwrap());
        assert!(rs
            .rows
            .iter()
            .any(|r| r[0] == Value::String("cpu".into())
                && r[1] == Value::String("host, core, ts".into())));
    }

    #[test]
    fn timeseries_bucketed_aggregation_and_ts_functions() {
        let mut db = Database::open(tempdir()).unwrap();
        ts_fixture(&mut db);

        // avg per bucket per host: values rise linearly, so bucket 0 of host
        // a averages (0+15+30+45)/4 = 22.5.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(1m, ts) AS t, host, avg(value) FROM cpu \
                 GROUP BY t, host ORDER BY t, host",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["t", "host", "avg"]);
        assert_eq!(rs.rows.len(), 4); // 2 buckets x 2 hosts
        assert_eq!(
            rs.rows[0],
            vec![
                Value::Timestamp(0),
                Value::String("a".into()),
                Value::Float(22.5)
            ]
        );

        // rate(): each series rises 1/s; grouped per bucket per host the
        // per-series rate is 1.0.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(1m, ts) AS t, host, rate(value) FROM cpu \
                 GROUP BY t, host ORDER BY t, host",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows[0][2], Value::Float(1.0));

        // rate() across both hosts sums per-series rates: 2.0.
        let rs = rows(
            db.execute("SELECT rate(value) FROM cpu WHERE ts <= 45000").unwrap(),
        );
        assert_eq!(rs.rows[0][0], Value::Float(2.0));

        // first()/last()/delta() over one host's window.
        let rs = rows(
            db.execute(
                "SELECT first(value), last(value), delta(value) FROM cpu WHERE host = 'b'",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows[0],
            vec![Value::Float(0.0), Value::Float(105.0), Value::Float(105.0)]
        );
    }

    #[test]
    fn timeseries_counter_reset_and_increase() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE reqs (SERIES KEY (h))")
            .unwrap();
        // Counter climbs to 10, resets, climbs to 3: increase = 10 + 3.
        for (ts, v) in [(0, 0.0), (1000, 10.0), (2000, 3.0)] {
            db.execute(&format!(
                "INSERT INTO reqs (h, ts, value) VALUES ('x', {ts}, {v})"
            ))
            .unwrap();
        }
        let rs = rows(db.execute("SELECT increase(value) FROM reqs").unwrap());
        assert_eq!(rs.rows[0][0], Value::Float(13.0));
    }

    /// The partial-aggregate path must be indistinguishable from the raw
    /// path: every eligible query is re-run with `AND 1 = 1` appended (a
    /// residual no pushdown consumes, forcing raw samples) and the results
    /// compared row for row.
    #[test]
    fn timeseries_partials_match_raw_path() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host, core))")
            .unwrap();
        // Two hosts × two cores; value is a counter with a reset on host a,
        // temp exists only on host a (absent-field aggregates).
        for (host, core, ts, v) in [
            ("a", "0", 0, 0.0),
            ("a", "0", 15_000, 40.0),
            ("a", "0", 30_000, 10.0), // reset
            ("a", "0", 75_000, 25.0),
            ("a", "1", 0, 5.0),
            ("a", "1", 45_000, 20.0),
            ("b", "0", 15_000, 100.0),
            ("b", "0", 60_000, 160.0),
        ] {
            let extra = if host == "a" {
                format!(", temp) VALUES ('{host}', '{core}', {ts}, {v}, {t})", t = v / 2.0)
            } else {
                format!(") VALUES ('{host}', '{core}', {ts}, {v})")
            };
            db.execute(&format!("INSERT INTO cpu (host, core, ts, value{extra}"))
                .unwrap();
        }
        for (eligible, raw) in [
            (
                "SELECT time_bucket(1m, ts) AS t, host, count(value), sum(value), avg(value), \
                 min(value), max(value) FROM cpu WHERE ts >= 0 GROUP BY t, host ORDER BY t, host",
                "SELECT time_bucket(1m, ts) AS t, host, count(value), sum(value), avg(value), \
                 min(value), max(value) FROM cpu WHERE ts >= 0 AND 1 = 1 GROUP BY t, host \
                 ORDER BY t, host",
            ),
            (
                "SELECT time_bucket(1m, ts) AS t, rate(value), increase(value), delta(value) \
                 FROM cpu WHERE host = 'a' GROUP BY t ORDER BY t",
                "SELECT time_bucket(1m, ts) AS t, rate(value), increase(value), delta(value) \
                 FROM cpu WHERE host = 'a' AND 1 = 1 GROUP BY t ORDER BY t",
            ),
            (
                "SELECT host, first(value), last(value), first(temp), last(temp) FROM cpu \
                 WHERE ts >= 0 GROUP BY host ORDER BY host",
                "SELECT host, first(value), last(value), first(temp), last(temp) FROM cpu \
                 WHERE ts >= 0 AND 1 = 1 GROUP BY host ORDER BY host",
            ),
            (
                // Absent field on host b: count 0, everything else NULL.
                "SELECT host, count(temp), sum(temp), avg(temp) FROM cpu WHERE ts >= 0 \
                 GROUP BY host ORDER BY host",
                "SELECT host, count(temp), sum(temp), avg(temp) FROM cpu WHERE ts >= 0 AND 1 = 1 \
                 GROUP BY host ORDER BY host",
            ),
            (
                "SELECT core, max(value) - min(value) AS spread FROM cpu WHERE host != 'b' \
                 GROUP BY core HAVING count(value) > 1 ORDER BY spread DESC LIMIT 1",
                "SELECT core, max(value) - min(value) AS spread FROM cpu WHERE host != 'b' \
                 AND 1 = 1 GROUP BY core HAVING count(value) > 1 ORDER BY spread DESC LIMIT 1",
            ),
            (
                "SELECT max(value) FROM cpu WHERE ts >= 15000 AND ts < 60000",
                "SELECT max(value) FROM cpu WHERE ts >= 15000 AND ts < 60000 AND 1 = 1",
            ),
            (
                // Mixed fields: the value stream materializes host b's group,
                // where count(temp) must be 0, not NULL.
                "SELECT host, count(temp), count(value) FROM cpu WHERE ts >= 0 \
                 GROUP BY host ORDER BY host",
                "SELECT host, count(temp), count(value) FROM cpu WHERE ts >= 0 AND 1 = 1 \
                 GROUP BY host ORDER BY host",
            ),
        ] {
            let fast = rows(db.execute(eligible).unwrap());
            let slow = rows(db.execute(raw).unwrap());
            assert_eq!(fast.columns, slow.columns, "{eligible}");
            assert_eq!(fast.rows, slow.rows, "{eligible}");
        }

        // Spot-check hard numbers through the partials path: host a core 0 in
        // bucket 0 rises 0→40, resets to 10 (increase 50); a group that
        // exists via another field counts an absent field as 0, not NULL.
        let rs = rows(
            db.execute(
                "SELECT increase(value) FROM cpu WHERE host = 'a' AND core = '0' AND ts < 60000",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows[0][0], Value::Float(50.0));
        let rs = rows(
            db.execute(
                "SELECT host, count(temp), count(value) FROM cpu GROUP BY host ORDER BY host",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows[1],
            vec![Value::String("b".into()), Value::Int(0), Value::Int(2)]
        );
    }

    /// Aggregates over windows the source's RETENTION has already dropped
    /// are served from a satisfying rollup: raw samples are gone, but
    /// bucketed count/sum/min/max/first/last still answer, and windows
    /// straddling the horizon stitch rollup + source partials.
    #[test]
    fn timeseries_rollup_query_rewrite_serves_aged_buckets() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host), RETENTION 1h)")
            .unwrap();
        db.execute("CREATE ROLLUP cpu_5m ON cpu BUCKET 5m RETENTION 30d")
            .unwrap();
        let (m, h) = (60_000i64, 3_600_000i64);
        // Ten samples in the first 2h window (0..9m, values 0..9)...
        for i in 0..10i64 {
            db.execute(&format!(
                "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, {})",
                i * m,
                i
            ))
            .unwrap();
        }
        // ...then appends at 4h and 6h: each crosses a block window, so the
        // older windows flush (maintaining the rollup) and the 6h flush's
        // retention pass (cutoff 6h - 1h) drops both flushed blocks.
        db.execute(&format!(
            "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, 100)",
            4 * h
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, 200)",
            6 * h
        ))
        .unwrap();

        // The premise: raw samples of the first window are gone.
        let rs = rows(
            db.execute("SELECT ts, value FROM cpu WHERE ts < 600000 ORDER BY ts")
                .unwrap(),
        );
        assert_eq!(rs.rows.len(), 0, "expected the aged block to be dropped");

        // Bucketed aggregates over the aged window answer from the rollup.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(10m, ts) AS t, count(value), sum(value), min(value), \
                 max(value), first(value), last(value) FROM cpu WHERE ts < 600000 GROUP BY t",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![
                Value::Timestamp(0),
                Value::Int(10),
                Value::Float(45.0),
                Value::Float(0.0),
                Value::Float(9.0),
                Value::Float(0.0),
                Value::Float(9.0),
            ]]
        );

        // A window straddling the horizon stitches rollup (aged: the first
        // ten samples and the dropped 4h block) with source (the live 6h
        // sample): 12 samples, sum 45 + 100 + 200.
        let rs = rows(
            db.execute("SELECT count(value), sum(value) FROM cpu WHERE ts >= 0")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(12), Value::Float(345.0)]]);

        // Change aggregates need raw samples and never route to rollups:
        // only the live 6h sample remains (a single sample → NULL), exactly
        // like the raw path.
        let rs = rows(db.execute("SELECT increase(value) FROM cpu WHERE ts >= 0").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Null]]);
    }

    /// A `label != 'x'` matcher reads a missing label as `""` in the store,
    /// but SQL residual semantics drop the row (`NULL != 'x'` is not true).
    /// The partials path must reproduce that for dynamically-labeled series
    /// (remote_write-style ingest, where labels beyond the series key exist).
    #[test]
    fn timeseries_partials_missing_label_semantics() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (name))")
            .unwrap();
        // One series with a `zone` label, one without (direct append, the
        // remote_write path).
        db.ts_append(
            "m",
            &[
                (
                    vec![
                        ("__field__".into(), "value".into()),
                        ("name".into(), "up".into()),
                        ("zone".into(), "us".into()),
                    ],
                    1000,
                    1.0,
                ),
                (
                    vec![
                        ("__field__".into(), "value".into()),
                        ("name".into(), "up".into()),
                    ],
                    1000,
                    5.0,
                ),
            ],
        )
        .unwrap();
        // zone != 'eu' must exclude the zone-less series in both paths.
        let eligible = rows(
            db.execute("SELECT sum(value) FROM m WHERE zone != 'eu' GROUP BY name")
                .unwrap(),
        );
        let raw = rows(
            db.execute("SELECT sum(value) FROM m WHERE zone != 'eu' AND 1 = 1 GROUP BY name")
                .unwrap(),
        );
        assert_eq!(eligible.rows, raw.rows);
        assert_eq!(eligible.rows, vec![vec![Value::Float(1.0)]]);
    }

    #[test]
    fn timeseries_now_and_duration_literals() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h))").unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        db.execute(&format!(
            "INSERT INTO m (h, ts, value) VALUES ('x', {}, 1), ('x', {}, 2)",
            now - 2 * 3600 * 1000, // two hours ago
            now - 60_000,          // a minute ago
        ))
        .unwrap();
        let rs = rows(
            db.execute("SELECT value FROM m WHERE ts >= now() - 1h").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Float(2.0)]]);
    }

    #[test]
    fn timeseries_rollup_maintained_at_flush() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host))").unwrap();
        db.execute("CREATE ROLLUP cpu_30m ON cpu BUCKET 30m RETENTION 30d").unwrap();

        // Fill the first 2h window (15m apart, values = quarter-hours) and
        // cross into the next window so the first flushes.
        let q = 15 * 60 * 1000i64; // 15m
        for i in 0..8i64 {
            db.execute(&format!(
                "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, {})",
                i * q,
                i as f64
            ))
            .unwrap();
        }
        db.execute(&format!(
            "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, 100)",
            8 * q // 2h: completes the first window
        ))
        .unwrap();

        // 4 buckets of 30m in the flushed window, 2 samples each:
        // sums 0+1, 2+3, 4+5, 6+7; count 2 each; last = 1,3,5,7.
        let rs = rows(
            db.execute(
                "SELECT ts, value_count, value_sum, value_last FROM cpu_30m ORDER BY ts",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 4);
        for (i, row) in rs.rows.iter().enumerate() {
            let i = i as i64;
            assert_eq!(row[0], Value::Timestamp(i * 30 * 60 * 1000));
            assert_eq!(row[1], Value::Float(2.0));
            assert_eq!(row[2], Value::Float((4 * i + 1) as f64), "sum");
            assert_eq!(row[3], Value::Float((i * 2 + 1) as f64));
        }

        // Labels carry over: rollup filters by host like the source.
        let rs = rows(
            db.execute("SELECT count(value_sum) FROM cpu_30m WHERE host = 'a'").unwrap(),
        );
        assert_eq!(rs.rows[0][0], Value::Int(4));

        // DROP of the source cascades to the rollup.
        db.execute("DROP TABLE cpu").unwrap();
        let err = db.execute("SELECT * FROM cpu_30m").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn timeseries_ooo_window_and_status() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h), RETENTION 30d, OOO 2m)")
            .unwrap();
        // In-order, then within-window out-of-order, then beyond-window.
        db.execute("INSERT INTO m (h, ts, value) VALUES ('x', 600000, 6)").unwrap();
        assert_eq!(
            affected(db.execute("INSERT INTO m (h, ts, value) VALUES ('x', 500000, 5)").unwrap()),
            1,
            "within the 2m window"
        );
        db.execute("INSERT INTO m (h, ts, value) VALUES ('x', 100000, 99)").unwrap();
        let rs = rows(db.execute("SELECT ts, value FROM m ORDER BY ts").unwrap());
        // Beyond-window sample (ts=100000) was rejected; the OOO one merged
        // in time order.
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Timestamp(500000), Value::Float(5.0)],
                vec![Value::Timestamp(600000), Value::Float(6.0)],
            ]
        );
        // Per-store stats are visible.
        let rs = rows(db.execute("SHOW STATUS").unwrap());
        assert!(rs.rows.iter().any(|r| r[0] == Value::String("timeseries.m.series".into())));
        assert!(rs
            .rows
            .iter()
            .any(|r| r[0] == Value::String("timeseries.m.samples_rejected".into())
                && r[1] == Value::Int(1)));
    }

    #[test]
    fn timeseries_rejects_update_delete_and_bad_inserts() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h))").unwrap();
        db.execute("INSERT INTO m (h, ts, value) VALUES ('x', 1000, 1)")
            .unwrap();

        let err = db.execute("UPDATE m SET value = 2 WHERE h = 'x'").unwrap_err();
        assert!(err.to_string().contains("append-only"), "{err}");
        let err = db.execute("DELETE FROM m WHERE h = 'x'").unwrap_err();
        assert!(err.to_string().contains("append-only"), "{err}");

        // Missing series key / ts / fields, and non-numeric fields.
        for (sql, needle) in [
            ("INSERT INTO m (ts, value) VALUES (1, 1)", "series key"),
            ("INSERT INTO m (h, value) VALUES ('x', 1)", "ts"),
            ("INSERT INTO m (h, ts) VALUES ('x', 1)", "at least one"),
            ("INSERT INTO m (h, ts, value) VALUES ('x', 2000, 'oops')", "numeric"),
        ] {
            let err = db.execute(sql).unwrap_err();
            assert!(err.to_string().contains(needle), "{sql}: {err}");
        }
    }

    #[test]
    fn timeseries_survives_reopen_and_drop() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h), RETENTION 30d)")
                .unwrap();
            db.execute("INSERT INTO m (h, ts, value) VALUES ('x', 1000, 1), ('x', 2000, 2)")
                .unwrap();
        }
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(db.execute("SELECT ts, value FROM m ORDER BY ts").unwrap());
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[1], vec![Value::Timestamp(2000), Value::Float(2.0)]);

        db.execute("DROP TABLE m").unwrap();
        let err = db.execute("SELECT * FROM m").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
        // Name is reusable as a regular table after the drop.
        db.execute("CREATE TABLE m (PRIMARY KEY (id))").unwrap();
    }

    #[test]
    fn users_roles_grants_crud_and_persistence() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE USER ada PASSWORD 'pencil'").unwrap();
            db.execute("CREATE ROLE reader").unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("GRANT SELECT ON t TO reader").unwrap();
            db.execute("GRANT INSERT ON DATABASE sales TO reader").unwrap();
            db.execute("GRANT ROLE reader TO ada").unwrap();
            // Duplicate create errors; IF NOT EXISTS doesn't.
            assert!(db.execute("CREATE USER ada PASSWORD 'x'").is_err());
            db.execute("CREATE USER IF NOT EXISTS ada PASSWORD 'x'").unwrap();
        }
        let mut db = Database::open(&dir).unwrap();
        // Credential survived (and the IF NOT EXISTS didn't clobber it).
        let cred = db.auth_user("ada").unwrap();
        let candidate = skaidb_auth::ScramCredential::new("pencil", &cred.salt, cred.iterations);
        assert_eq!(candidate.stored_key, cred.stored_key);
        // Inherited grant resolves.
        use skaidb_auth::{Object, Privilege};
        assert!(db.has_privilege("ada", Privilege::Select, &Object::Table("t".into())));
        assert!(!db.has_privilege("ada", Privilege::Insert, &Object::Table("t".into())));
        // The database grant survived the reopen with its object intact.
        assert!(db.has_privilege("ada", Privilege::Insert, &Object::Database("sales".into())));
        assert!(!db.has_privilege("ada", Privilege::Insert, &Object::Database("hr".into())));
        // REVOKE on the database object removes exactly it.
        db.execute("REVOKE INSERT ON DATABASE sales FROM reader").unwrap();
        assert!(!db.has_privilege("ada", Privilege::Insert, &Object::Database("sales".into())));
        // SHOW GRANTS lists both the grant and the inheritance edge.
        let rs = rows(db.execute("SHOW GRANTS FOR ada").unwrap());
        assert!(rs.rows.iter().any(|r| r[1] == Value::String("ROLE".into())));
        // ALTER changes the password; REVOKE removes access.
        db.execute("ALTER USER ada PASSWORD 'quill'").unwrap();
        let cred = db.auth_user("ada").unwrap();
        let old = skaidb_auth::ScramCredential::new("pencil", &cred.salt, cred.iterations);
        assert_ne!(old.stored_key, cred.stored_key);
        db.execute("REVOKE ROLE reader FROM ada").unwrap();
        assert!(!db.has_privilege("ada", Privilege::Select, &Object::Table("t".into())));
        // Drops.
        db.execute("DROP ROLE reader").unwrap();
        db.execute("DROP USER ada").unwrap();
        assert!(db.auth_user("ada").is_none());
        let err = db.execute("DROP ROLE nosuch").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "{err}");
    }

    #[test]
    fn where_filtering_and_three_valued_logic() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, age) VALUES (1, 20), (2, 40), (3, 60)")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM t WHERE age >= 40 ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    #[test]
    fn schema_less_missing_fields_are_null() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // Second row omits `name` entirely.
        db.execute("INSERT INTO t (id, name) VALUES (1, 'x')")
            .unwrap();
        db.execute("INSERT INTO t (id) VALUES (2)").unwrap();
        let rs = rows(db.execute("SELECT name FROM t ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::String("x".into())], vec![Value::Null]]
        );
    }

    #[test]
    fn update_and_delete() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, n) VALUES (1, 10), (2, 20)")
            .unwrap();
        assert_eq!(
            affected(db.execute("UPDATE t SET n = 99 WHERE id = 1").unwrap()),
            1
        );
        let rs = rows(db.execute("SELECT n FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(99)]]);
        assert_eq!(
            affected(db.execute("DELETE FROM t WHERE id = 2").unwrap()),
            1
        );
        let rs = rows(db.execute("SELECT id FROM t").unwrap());
        assert_eq!(rs.rows.len(), 1);
    }

    #[test]
    fn aggregates_with_group_by() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE sales (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO sales (id, region, amount) VALUES \
             (1, 'east', 100), (2, 'east', 50), (3, 'west', 70)",
        )
        .unwrap();
        let rs = rows(
            db.execute(
                "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["region", "total"]);
        assert_eq!(
            rs.rows[0],
            vec![Value::String("east".into()), Value::Int(150)]
        );
        assert_eq!(
            rs.rows[1],
            vec![Value::String("west".into()), Value::Int(70)]
        );
    }

    #[test]
    fn count_star_whole_table() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id) VALUES (1), (2), (3)")
            .unwrap();
        let rs = rows(db.execute("SELECT COUNT(*) FROM t").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
    }

    #[test]
    fn wildcard_select() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, a, b) VALUES (1, 10, 20)")
            .unwrap();
        let rs = rows(db.execute("SELECT * FROM t").unwrap());
        // Columns are the sorted union of fields.
        assert_eq!(rs.columns, vec!["a", "b", "id"]);
        assert_eq!(
            rs.rows[0],
            vec![Value::Int(10), Value::Int(20), Value::Int(1)]
        );
    }

    #[test]
    fn data_persists_across_reopen() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("INSERT INTO t (id, v) VALUES (1, 'persisted')")
                .unwrap();
        }
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(db.execute("SELECT v FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("persisted".into())]]);
    }

    #[test]
    fn primary_key_required() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // Inserting without the pk column is a constraint violation.
        assert!(db.execute("INSERT INTO t (name) VALUES ('x')").is_err());
    }

    #[test]
    fn duplicate_table_errors_without_if_not_exists() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        assert!(db.execute("CREATE TABLE t (PRIMARY KEY (id))").is_err());
        assert!(db
            .execute("CREATE TABLE IF NOT EXISTS t (PRIMARY KEY (id))")
            .is_ok());
    }

    #[test]
    fn secondary_index_equality_lookup_stays_correct() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, name) VALUES (1, 'ada'), (2, 'bob'), (3, 'ada')")
            .unwrap();
        db.execute("CREATE INDEX t_name ON t(name)").unwrap();

        // Index lookup returns the matching rows.
        let rs = rows(
            db.execute("SELECT id FROM t WHERE name = 'ada' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

        // Update keeps the index in sync.
        db.execute("UPDATE t SET name = 'cleo' WHERE id = 1")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM t WHERE name = 'ada' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
        let rs = rows(db.execute("SELECT id FROM t WHERE name = 'cleo'").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);

        // Delete keeps the index in sync.
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        let rs = rows(db.execute("SELECT id FROM t WHERE name = 'ada'").unwrap());
        assert!(rs.rows.is_empty());
    }

    /// Build a `people(id, age)` table with `age` indexed and rows 1..=n where
    /// `age = id * 10`.
    fn people_by_age(n: i64) -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE people (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX people_age ON people(age)").unwrap();
        for id in 1..=n {
            db.execute(&format!(
                "INSERT INTO people (id, age) VALUES ({id}, {})",
                id * 10
            ))
            .unwrap();
        }
        db
    }

    fn ids(rs: ResultSet) -> Vec<i64> {
        rs.rows
            .iter()
            .map(|r| match &r[0] {
                Value::Int(i) => *i,
                other => panic!("expected int id, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn index_range_scan_returns_correct_rows() {
        let mut db = people_by_age(10); // ages 10,20,..,100
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE age > 70 ORDER BY id").unwrap())),
            vec![8, 9, 10]
        );
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE age >= 70 ORDER BY id").unwrap())),
            vec![7, 8, 9, 10]
        );
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE age < 30 ORDER BY id").unwrap())),
            vec![1, 2]
        );
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE age <= 30 ORDER BY id").unwrap())),
            vec![1, 2, 3]
        );
        // BETWEEN-style range (two bounds AND-ed on the indexed column).
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM people WHERE age >= 30 AND age <= 60 ORDER BY id")
                    .unwrap()
            )),
            vec![3, 4, 5, 6]
        );
        // Literal-on-the-left is normalized.
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE 80 < age ORDER BY id").unwrap())),
            vec![9, 10]
        );
    }

    #[test]
    fn order_by_indexed_column_is_sorted_and_limited() {
        let mut db = people_by_age(5); // ages 10..50
        // Insert in non-sorted id order to prove ordering comes from the index.
        db.execute("INSERT INTO people (id, age) VALUES (99, 5)").unwrap();
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people ORDER BY age").unwrap())),
            vec![99, 1, 2, 3, 4, 5] // ages 5,10,20,30,40,50
        );
        // Top-N via the index (early stop).
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people ORDER BY age LIMIT 3").unwrap())),
            vec![99, 1, 2]
        );
        // OFFSET + LIMIT windows correctly.
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM people ORDER BY age LIMIT 2 OFFSET 2").unwrap()
            )),
            vec![2, 3]
        );
        // Range + order combined.
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM people WHERE age >= 20 ORDER BY age LIMIT 2").unwrap()
            )),
            vec![2, 3]
        );
    }

    #[test]
    fn order_by_desc_falls_back_to_sort() {
        let mut db = people_by_age(4);
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people ORDER BY age DESC").unwrap())),
            vec![4, 3, 2, 1]
        );
    }

    #[test]
    fn range_on_unindexed_column_still_correct() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE people (PRIMARY KEY (id))").unwrap();
        for id in 1..=6 {
            db.execute(&format!("INSERT INTO people (id, age) VALUES ({id}, {})", id * 10))
                .unwrap();
        }
        assert_eq!(
            ids(rows(db.execute("SELECT id FROM people WHERE age > 30 ORDER BY id").unwrap())),
            vec![4, 5, 6]
        );
    }

    #[test]
    fn index_range_survives_updates_and_deletes() {
        let mut db = people_by_age(50);
        db.execute("UPDATE people SET age = 5 WHERE id = 50").unwrap(); // moves out of range
        db.execute("DELETE FROM people WHERE id = 1").unwrap();
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM people WHERE age >= 480 ORDER BY id").unwrap()
            )),
            vec![48, 49] // ages 480, 490; id 50 now 5, id 1 gone
        );
    }

    /// `t(id, region, age)` with a composite index on `(region, age)`.
    fn region_age_table() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_region_age ON t(region, age)").unwrap();
        let rows = [
            (1, "eu", 30),
            (2, "eu", 20),
            (3, "us", 40),
            (4, "eu", 50),
            (5, "us", 25),
            (6, "eu", 20),
        ];
        for (id, region, age) in rows {
            db.execute(&format!(
                "INSERT INTO t (id, region, age) VALUES ({id}, '{region}', {age})"
            ))
            .unwrap();
        }
        db
    }

    #[test]
    fn composite_index_equality_and_prefix() {
        let mut db = region_age_table();
        // Full composite equality.
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' AND age = 20 ORDER BY id")
                    .unwrap()
            )),
            vec![2, 6]
        );
        // Leftmost-prefix equality (only the leading column).
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' ORDER BY id").unwrap()
            )),
            vec![1, 2, 4, 6]
        );
        // Equality on the prefix + range on the next column.
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' AND age > 25 ORDER BY id")
                    .unwrap()
            )),
            vec![1, 4]
        );
    }

    #[test]
    fn composite_index_orders_by_trailing_column() {
        let db_rows = rows(
            region_age_table()
                .execute("SELECT id FROM t WHERE region = 'eu' ORDER BY age")
                .unwrap(),
        );
        // eu rows by age: 20(2), 20(6), 30(1), 50(4) — ties broken by row key (id).
        assert_eq!(ids(db_rows), vec![2, 6, 1, 4]);
    }

    #[test]
    fn composite_index_top_n_and_maintenance() {
        let mut db = region_age_table();
        // Top-N within the prefix via the index.
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' ORDER BY age LIMIT 2").unwrap()
            )),
            vec![2, 6]
        );
        // Update moves a row across the index; lookups stay correct.
        db.execute("UPDATE t SET region = 'us' WHERE id = 1").unwrap();
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' ORDER BY id").unwrap()
            )),
            vec![2, 4, 6]
        );
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'us' AND age = 30").unwrap()
            )),
            vec![1]
        );
        // Delete keeps it in sync.
        db.execute("DELETE FROM t WHERE id = 6").unwrap();
        assert_eq!(
            ids(rows(
                db.execute("SELECT id FROM t WHERE region = 'eu' AND age = 20").unwrap()
            )),
            vec![2]
        );
    }

    fn doc_id(doc: &skaidb_types::Document) -> i64 {
        match doc.get("id") {
            Some(Value::Int(i)) => *i,
            other => panic!("expected int id, got {other:?}"),
        }
    }

    #[test]
    fn vector_index_search_filtered_and_persists() {
        use skaidb_sql::ast::{BinaryOp, Expr};
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
            db.execute("INSERT INTO docs (id, cat, embedding) VALUES (1, 'a', [1.0, 0.0, 0.0])")
                .unwrap();
            db.execute("INSERT INTO docs (id, cat, embedding) VALUES (2, 'b', [0.0, 1.0, 0.0])")
                .unwrap();
            db.execute("INSERT INTO docs (id, cat, embedding) VALUES (3, 'a', [0.0, 0.0, 1.0])")
                .unwrap();
            db.execute("INSERT INTO docs (id, cat, embedding) VALUES (4, 'b', [0.9, 0.1, 0.0])")
                .unwrap();
            db.create_vector_index("docs_emb", "docs", "embedding", "cosine", None)
                .unwrap();

            // Nearest to [1,0,0]: id 1 (exact), then id 4 (close direction).
            let ids: Vec<i64> = db
                .vector_search("docs_emb", &[1.0, 0.0, 0.0], 2, &None)
                .unwrap()
                .iter()
                .map(|(_, doc, _)| doc_id(doc))
                .collect();
            assert_eq!(ids, vec![1, 4]);

            // Filtered nearest-neighbor: WHERE cat = 'a' excludes id 4 (cat 'b').
            let filter = Some(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column("cat".into())),
                right: Box::new(Expr::Literal(Value::String("a".into()))),
            });
            let ids: Vec<i64> = db
                .vector_search("docs_emb", &[1.0, 0.0, 0.0], 2, &filter)
                .unwrap()
                .iter()
                .map(|(_, doc, _)| doc_id(doc))
                .collect();
            assert_eq!(ids, vec![1, 3]);

            // Maintenance: a new row is indexed; querying near its own vector
            // returns it, and after deletion it's gone.
            db.execute("INSERT INTO docs (id, cat, embedding) VALUES (5, 'a', [0.05, 0.95, 0.0])")
                .unwrap();
            let top = db.vector_search("docs_emb", &[0.05, 0.95, 0.0], 1, &None).unwrap();
            assert_eq!(doc_id(&top[0].1), 5); // exact match to the just-inserted row
            db.execute("DELETE FROM docs WHERE id = 5").unwrap();
            let top = db.vector_search("docs_emb", &[0.05, 0.95, 0.0], 1, &None).unwrap();
            assert_eq!(doc_id(&top[0].1), 2); // id 5 gone → id 2 ([0,1,0]) is nearest
        }

        // Reopen: the in-memory index is rebuilt from the table and still works.
        let db = Database::open(&dir).unwrap();
        let top = db.vector_search("docs_emb", &[0.0, 0.0, 1.0], 1, &None).unwrap();
        assert_eq!(doc_id(&top[0].1), 3);
    }

    #[test]
    fn secondary_index_backfills_existing_rows_and_persists() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("INSERT INTO t (id, city) VALUES (1, 'oslo'), (2, 'oslo'), (3, 'rome')")
                .unwrap();
            // Index created after data exists → must backfill.
            db.execute("CREATE INDEX t_city ON t(city)").unwrap();
            let rs = rows(
                db.execute("SELECT id FROM t WHERE city = 'oslo' ORDER BY id")
                    .unwrap(),
            );
            assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
        }
        // Index survives reopen.
        let mut db = Database::open(&dir).unwrap();
        db.execute("INSERT INTO t (id, city) VALUES (4, 'oslo')")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM t WHERE city = 'oslo' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(4)]
            ]
        );
    }

    #[test]
    fn dropping_index_falls_back_to_scan() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_k ON t(k)").unwrap();
        db.execute("INSERT INTO t (id, k) VALUES (1, 7), (2, 8)")
            .unwrap();
        db.execute("DROP INDEX t_k").unwrap();
        // Still correct via full scan.
        let rs = rows(db.execute("SELECT id FROM t WHERE k = 7").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn limit_and_offset() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id) VALUES (1), (2), (3), (4), (5)")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    }

    // ---- DISTINCT / HAVING ----

    #[test]
    fn select_distinct_dedups() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, c) VALUES (1,'a'),(2,'a'),(3,'b'),(4,'b'),(5,'a')")
            .unwrap();
        let rs = rows(db.execute("SELECT DISTINCT c FROM t ORDER BY c").unwrap());
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("a".into())],
                vec![Value::String("b".into())]
            ]
        );
    }

    #[test]
    fn group_by_having_filters_groups() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, g, v) VALUES (1,'x',10),(2,'x',20),(3,'y',5),(4,'z',40)")
            .unwrap();
        let rs = rows(
            db.execute("SELECT g, SUM(v) FROM t GROUP BY g HAVING SUM(v) > 15 ORDER BY g")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("x".into()), Value::Int(30)],
                vec![Value::String("z".into()), Value::Int(40)],
            ]
        );
    }

    // ---- JOIN ----

    fn join_db() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE users (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE TABLE orders (PRIMARY KEY (oid))")
            .unwrap();
        db.execute("INSERT INTO users (id, name) VALUES (1,'ada'),(2,'bob'),(3,'cy')")
            .unwrap();
        // order 13 has uid 99 — no matching user.
        db.execute(
            "INSERT INTO orders (oid, uid, amt) VALUES (10,1,100),(11,1,50),(12,2,75),(13,99,5)",
        )
        .unwrap();
        db
    }

    #[test]
    fn inner_join_with_qualified_columns() {
        let mut db = join_db();
        let rs = rows(
            db.execute(
                "SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.amt",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["u.name", "o.amt"]);
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("ada".into()), Value::Int(50)],
                vec![Value::String("bob".into()), Value::Int(75)],
                vec![Value::String("ada".into()), Value::Int(100)],
            ]
        );
    }

    #[test]
    fn left_join_keeps_unmatched_left_with_nulls() {
        let mut db = join_db();
        // cy (id 3) has no orders → null right side.
        let rs = rows(
            db.execute(
                "SELECT u.name, o.amt FROM users u LEFT JOIN orders o ON u.id = o.uid \
                 WHERE u.id = 3",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::String("cy".into()), Value::Null]]);
    }

    #[test]
    fn right_join_keeps_unmatched_right() {
        let mut db = join_db();
        // order 13 (uid 99) has no user → surfaces under RIGHT JOIN with NULL user.
        let rs = rows(
            db.execute(
                "SELECT o.oid FROM users u RIGHT JOIN orders o ON u.id = o.uid \
                 WHERE u.id IS NULL",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(13)]]);
    }

    #[test]
    fn cross_join_is_cartesian() {
        let mut db = join_db();
        let rs = rows(
            db.execute("SELECT COUNT(*) FROM users u CROSS JOIN orders o")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(12)]]); // 3 users × 4 orders
    }

    // ---- UNION ----

    fn union_db() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE a (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE TABLE b (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO a (id) VALUES (1),(2),(3)").unwrap();
        db.execute("INSERT INTO b (id) VALUES (3),(4)").unwrap();
        db
    }

    #[test]
    fn union_dedups_union_all_keeps() {
        let mut db = union_db();
        let rs = rows(
            db.execute("SELECT id FROM a UNION SELECT id FROM b ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(4)],
            ]
        );
        let rs = rows(
            db.execute("SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)],
                vec![Value::Int(3)],
                vec![Value::Int(4)],
            ]
        );
    }

    // ---- ALTER ----

    #[test]
    fn alter_table_rename_to() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, name) VALUES (1, 'ada')")
            .unwrap();
        db.execute("ALTER TABLE t RENAME TO people").unwrap();
        let rs = rows(db.execute("SELECT name FROM people WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("ada".into())]]);
        // The old name is gone.
        assert!(db.execute("SELECT id FROM t").is_err());
    }

    #[test]
    fn alter_table_rename_column_rebuilds_index() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_c ON t(c)").unwrap();
        db.execute("INSERT INTO t (id, c) VALUES (1, 5), (2, 7), (3, 5)")
            .unwrap();
        db.execute("ALTER TABLE t RENAME COLUMN c TO d").unwrap();
        // The renamed field is queryable, and the index (now on `d`) still serves it.
        let rs = rows(
            db.execute("SELECT id, d FROM t WHERE d = 5 ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1), Value::Int(5)],
                vec![Value::Int(3), Value::Int(5)],
            ]
        );
        // The old column name now reads as NULL everywhere.
        let rs = rows(db.execute("SELECT id FROM t WHERE c IS NULL ORDER BY id").unwrap());
        assert_eq!(rs.rows.len(), 3);
    }

    #[test]
    fn alter_table_rename_primary_key_column() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE k (PRIMARY KEY (uid))").unwrap();
        db.execute("INSERT INTO k (uid, x) VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        db.execute("ALTER TABLE k RENAME COLUMN uid TO user_id")
            .unwrap();
        let rs = rows(
            db.execute("SELECT x FROM k WHERE user_id = 2")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::String("b".into())]]);
    }

    // ---- read-only execution path ----

    #[test]
    fn execute_read_serves_selects_through_shared_access() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_v ON t(v)").unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();

        // Concurrent readers share the database behind an RwLock read guard —
        // the whole point of the `&self` read path.
        let db = std::sync::RwLock::new(db);
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let d = db.read().unwrap();
                    let rs = rows(d.execute_read("SELECT id FROM t WHERE v >= 20 ORDER BY id").unwrap());
                    assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
                    let rs = rows(d.execute_read("SHOW TABLES").unwrap());
                    assert_eq!(rs.rows.len(), 1);
                });
            }
        });

        // Anything that mutates is rejected on the read-only path.
        let d = db.read().unwrap();
        assert!(d.execute_read("INSERT INTO t (id) VALUES (9)").is_err());
        assert!(d.execute_read("DROP TABLE t").is_err());
        assert!(d.execute_read("BEGIN").is_err());
    }

    #[test]
    fn execute_read_sees_open_transaction_overlay() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t (id) VALUES (2)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        // A read-your-writes SELECT works through `&db` too.
        let rs = rows(db.execute_read("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
        db.execute("ROLLBACK").unwrap();
        let rs = rows(db.execute_read("SELECT id FROM t").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    }

    // ---- transactions ----

    #[test]
    fn transaction_commit_persists_read_your_writes() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, name) VALUES (1, 'a')")
            .unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t (id, name) VALUES (2, 'b')")
            .unwrap();
        db.execute("UPDATE t SET name = 'A' WHERE id = 1").unwrap();
        // Read-your-writes inside the transaction.
        let rs = rows(db.execute("SELECT id, name FROM t ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1), Value::String("A".into())],
                vec![Value::Int(2), Value::String("b".into())],
            ]
        );
        db.execute("COMMIT").unwrap();

        // Durable after commit.
        let rs = rows(db.execute("SELECT id, name FROM t ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(1), Value::String("A".into())],
                vec![Value::Int(2), Value::String("b".into())],
            ]
        );
    }

    #[test]
    fn transaction_rollback_discards_changes() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id) VALUES (1), (2)").unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.execute("INSERT INTO t (id) VALUES (3)").unwrap();
        // Visible inside the transaction.
        let rs = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
        db.execute("ROLLBACK").unwrap();

        // Back to the pre-transaction state.
        let rs = rows(db.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }

    #[test]
    fn transaction_state_errors() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        assert!(db.execute("COMMIT").is_err()); // no transaction
        assert!(db.execute("ROLLBACK").is_err());
        db.execute("BEGIN").unwrap();
        assert!(db.execute("BEGIN").is_err()); // already in a transaction
        db.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn transaction_index_consistent_after_commit() {
        // A committed transaction must leave secondary indexes correct.
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_r ON t(r)").unwrap();
        db.execute("INSERT INTO t (id, r) VALUES (1,'eu'),(2,'us')")
            .unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t (id, r) VALUES (3,'eu')").unwrap();
        db.execute("UPDATE t SET r = 'eu' WHERE id = 2").unwrap();
        db.execute("COMMIT").unwrap();
        // Index-accelerated lookup sees all three 'eu' rows.
        let rs = rows(db.execute("SELECT id FROM t WHERE r = 'eu' ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]]
        );
    }
}
