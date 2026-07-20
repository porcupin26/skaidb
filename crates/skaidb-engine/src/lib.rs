//! skaidb query engine: binds parsed SQL to the storage engine (SPEC §3).
//!
//! [`Database`] is an embeddable engine — open a directory, then [`Database::execute`]
//! SQL statements and get back a [`QueryOutput`].

pub mod catalog;
pub mod embed;
pub mod geo;
mod error;
mod eval;
mod exec;
pub mod namespace;
pub mod scan_meter;
mod result;
mod session;
mod ts_query;
pub mod vector;

pub use embed::{Embedder, HashEmbedder};
pub use error::EngineError;
pub use exec::{
    filter_rows, filter_search_query, matches_filter, run, statement_is_read_only, Cluster,
    Database, DbStats, IndexScanRange, Inventory, InventoryIndex, InventorySearch,
    InventoryTable, InventoryTimeseries, InventoryVector, pk_point_key, pk_point_keys, TableStats,
    TsRollupInfo,
    DecodedMaint, MaintTask,
    gidx_table, global_entry_delta, index_entry_key,
    gidx_entry_key, gidx_entry_row_key, gidx_placement_prefix, gidx_probe_bounds, is_gidx_table,
};
pub use skaidb_storage::{Codec, EngineOptions, Hlc, Kek, DEFAULT_SEARCH_WRITER_HEAP};
pub use namespace::DEFAULT_DATABASE;
pub use result::{QueryOutput, ResultSet, SessionEffect};

/// Build a background flush's SSTable outside any lock (see the Node flusher).
pub fn build_flush_sstable(job: &skaidb_storage::FlushJob) -> skaidb_storage::Result<skaidb_storage::SsTable> {
    skaidb_storage::Engine::build_flush(job)
}

/// Build a background compaction's output outside any lock.
pub fn build_compact_sstable(job: &skaidb_storage::CompactJob) -> skaidb_storage::Result<skaidb_storage::SsTable> {
    skaidb_storage::Engine::build_compact(job)
}

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

    #[test]
    fn in_and_not_in_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE users (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO users (id, name) VALUES \
             (1, 'ada'), (2, 'bob'), (3, 'cy'), (4, 'dot')",
        )
        .unwrap();

        // IN over a literal list.
        let rs = rows(
            db.execute("SELECT id FROM users WHERE id IN (1, 3) ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

        // NOT IN is the complement.
        let rs = rows(
            db.execute("SELECT id FROM users WHERE id NOT IN (1, 3) ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);

        // String IN.
        let rs = rows(
            db.execute("SELECT id FROM users WHERE name IN ('bob', 'dot', 'zzz') ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);

        // Bound array parameter: `id IN (?)` with ? = [2, 4] — the fetch-N-ids
        // shape, driven through prepare/bind.
        let stmt = skaidb_sql::parse("SELECT id FROM users WHERE id IN (?) ORDER BY id").unwrap();
        let bound =
            skaidb_sql::bind(&stmt, &[Value::Array(vec![Value::Int(2), Value::Int(4)])]).unwrap();
        let rs = rows(db.execute_statement(bound).unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
    }

    #[test]
    fn in_against_multikey_array_column_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        // labels is an array column (multikey containment).
        db.execute("INSERT INTO docs (id, labels) VALUES (1, ['work', 'home'])")
            .unwrap();
        db.execute("INSERT INTO docs (id, labels) VALUES (2, ['play'])")
            .unwrap();

        let rs = rows(
            db.execute("SELECT id FROM docs WHERE labels IN ('work', 'x') ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    }

    #[test]
    fn between_and_like_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE mail (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO mail (id, subject, date) VALUES \
             (1, 'Invoice #42 overdue', 100), \
             (2, 'lunch on friday?', 200), \
             (3, 'RE: invoice paid', 300)",
        )
        .unwrap();

        // BETWEEN is an inclusive range (and pushes down onto the PK range).
        let rs = rows(
            db.execute("SELECT id FROM mail WHERE date BETWEEN 100 AND 200 ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
        let rs = rows(
            db.execute("SELECT id FROM mail WHERE date NOT BETWEEN 100 AND 200")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);

        // LIKE is exact substring/prefix matching; ILIKE folds case.
        let rs = rows(
            db.execute("SELECT id FROM mail WHERE subject LIKE '%invoice%'")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
        let rs = rows(
            db.execute("SELECT id FROM mail WHERE subject ILIKE '%invoice%' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
        let rs = rows(
            db.execute("SELECT id FROM mail WHERE subject LIKE 'RE:%'")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
    }

    #[test]
    fn object_literal_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE users (PRIMARY KEY (id))").unwrap();
        // Whole nested document written in plain SQL — no REST round-trip.
        db.execute(
            "INSERT INTO users (id, meta) VALUES (1, {name: 'ada', tags: ['x'], addr: {city: 'paris'}})",
        )
        .unwrap();

        // Dotted-path reads see the nested fields.
        let rs = rows(db.execute("SELECT meta.addr.city FROM users WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("paris".into())]]);

        // UPDATE SET can replace a whole sub-document with an object literal.
        db.execute("UPDATE users SET meta.addr = {city: 'london', zip: 'E1'} WHERE id = 1")
            .unwrap();
        let rs = rows(
            db.execute("SELECT meta.addr.city, meta.addr.zip FROM users WHERE id = 1")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![
                Value::String("london".into()),
                Value::String("E1".into())
            ]]
        );
        // The untouched sibling field survived the sub-document swap.
        let rs = rows(db.execute("SELECT meta.name FROM users WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("ada".into())]]);
    }

    /// `LIMIT ? OFFSET ?` binds end-to-end; an unbound one (one-shot path)
    /// errors instead of executing.
    #[test]
    fn limit_offset_params_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        for i in 0..10 {
            db.execute(&format!("INSERT INTO t (id) VALUES ({i})")).unwrap();
        }
        let stmt = skaidb_sql::parse("SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?").unwrap();
        let bound = skaidb_sql::bind(&stmt, &[Value::Int(3), Value::Int(4)]).unwrap();
        let rs = rows(db.execute_statement(bound).unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(4)], vec![Value::Int(5)], vec![Value::Int(6)]]
        );
        // Unbound `LIMIT ?` through the one-shot path is a clean error.
        let err = db
            .execute("SELECT id FROM t LIMIT ?")
            .unwrap_err()
            .to_string();
        assert!(err.contains("unbound parameter"), "{err}");
    }

    #[test]
    fn const_select_and_to_timestamp_end_to_end() {
        let mut db = Database::open(tempdir()).unwrap();

        // The liveness probe: no table needed.
        let rs = rows(db.execute("SELECT 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
        let rs = rows(db.execute("SELECT 2 + 3 AS five, 'ok'").unwrap());
        assert_eq!(rs.columns, vec!["five", "expr"]);
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(5), Value::String("ok".into())]]
        );
        // now() works without a table.
        let rs = rows(db.execute("SELECT now()").unwrap());
        assert!(matches!(rs.rows[0][0], Value::Timestamp(t) if t > 0));

        // to_timestamp converts Mongo-migrated ISO strings for range filters.
        db.execute("CREATE TABLE ev (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO ev (id, created_at) VALUES \
             (1, '1970-01-01T00:00:01Z'), (2, '1970-01-03T00:00:00Z'), (3, 'garbage')",
        )
        .unwrap();
        let rs = rows(
            db.execute(
                "SELECT id FROM ev \
                 WHERE to_timestamp(created_at) BETWEEN 0 AND 86400000 ORDER BY id",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]); // garbage row is NULL, excluded
        let rs = rows(db.execute("SELECT to_timestamp(created_at) FROM ev WHERE id = 2").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Timestamp(2 * 86_400_000)]]);
    }

    /// A replayed `CREATE ... IF NOT EXISTS` carrying a NEWER schema stamp and
    /// a different definition must REPLACE the stale def and rebuild — the
    /// schema-sync convergence that the production `.6` node lacked (its
    /// search index covered fewer columns than its peers forever).
    #[test]
    fn create_if_not_exists_supersedes_stale_index_defs() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE m (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO m (id, text, chan) VALUES (1, 'hello world', 'general')")
            .unwrap();
        // Narrow def first (the stale state).
        db.execute("CREATE SEARCH INDEX m_fts ON m (text)").unwrap();
        let err = db
            .execute("SELECT id FROM m WHERE MATCH(chan, 'general')")
            .unwrap_err()
            .to_string();
        assert!(err.contains("covers column"), "{err}");
        // The replayed wider def (newer stamp): must replace + rebuild.
        db.execute(
            "CREATE SEARCH INDEX IF NOT EXISTS m_fts ON m (text, chan) \
             WITH (chan.type = 'keyword')",
        )
        .unwrap();
        let rs = rows(db.execute("SELECT id FROM m WHERE MATCH(chan, 'general')").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
        // Same def replayed again: a no-op (no rebuild churn), still serving.
        db.execute(
            "CREATE SEARCH INDEX IF NOT EXISTS m_fts ON m (text, chan) \
             WITH (chan.type = 'keyword')",
        )
        .unwrap();
        let rs = rows(db.execute("SELECT id FROM m WHERE MATCH(chan, 'general')").unwrap());
        assert_eq!(rs.rows.len(), 1);

        // Vector defs converge the same way (path change replaces).
        db.execute("INSERT INTO m (id, va, vb) VALUES (2, [0.1, 0.2], [0.9, 0.8])")
            .unwrap();
        db.execute("CREATE VECTOR INDEX m_vec ON m (va) DIM 2").unwrap();
        db.execute("CREATE VECTOR INDEX IF NOT EXISTS m_vec ON m (vb) DIM 2")
            .unwrap();
        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        let vec_row = rs
            .rows
            .iter()
            .find(|r| r[0] == Value::String("m_vec".into()))
            .unwrap()
            .clone();
        assert_eq!(vec_row[3], Value::String("vb".into())); // replaced path
        // The new `local` health column reports this node's live state.
        assert_eq!(rs.columns[4], "local");
        for r in &rs.rows {
            assert_eq!(r[4], Value::String("ok".into()), "{r:?}");
        }
    }

    /// The vector-checkpoint dirty gate: writes dirty the graph, a save
    /// cleans it, and the next write dirties it again — the periodic
    /// checkpoint tick's read-gate contract.
    #[test]
    fn vector_dirty_gate_tracks_writes_and_saves() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE d (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO d (id, v) VALUES (1, [1.0, 0.0])").unwrap();
        db.execute("CREATE VECTOR INDEX d_v ON d (v) DIM 2").unwrap();
        // create saves a fresh snapshot — clean right after.
        assert!(!db.has_dirty_vector_indexes());
        db.execute("INSERT INTO d (id, v) VALUES (2, [0.0, 1.0])").unwrap();
        assert!(db.has_dirty_vector_indexes());
        db.save_vector_indexes();
        assert!(!db.has_dirty_vector_indexes());
        db.execute("DELETE FROM d WHERE id = 1").unwrap();
        assert!(db.has_dirty_vector_indexes());
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

    /// With rollup backfill keeping rollups exact, whole group buckets
    /// below the head boundary serve from the rollup even **inside**
    /// retention (less raw IO, same numbers). Proven with a phantom bucket
    /// written only to the rollup: it can only appear in the output if the
    /// rollup answered.
    #[test]
    fn timeseries_rollup_serves_in_retention_windows_opportunistically() {
        let mut db = Database::open(tempdir()).unwrap();
        // No RETENTION: the old (required-only) router would never engage.
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host))")
            .unwrap();
        db.execute("CREATE ROLLUP cpu_5m ON cpu BUCKET 5m RETENTION 90d")
            .unwrap();
        let (m, h) = (60_000i64, 3_600_000i64);
        for i in 0..5i64 {
            db.execute(&format!(
                "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, {})",
                i * m,
                i
            ))
            .unwrap();
        }
        // Flush bucket 0 into the rollup; the 4h sample stays in the head.
        db.execute(&format!(
            "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, 100)",
            4 * h
        ))
        .unwrap();
        // Phantom rollup-only bucket at 10m (count 2, sum 42).
        let mut phantom = Vec::new();
        for (suffix, v) in [
            ("count", 2.0),
            ("sum", 42.0),
            ("min", 21.0),
            ("max", 21.0),
            ("first", 21.0),
            ("last", 21.0),
        ] {
            let mut labels: skaidb_tsdb::Labels = vec![
                ("__field__".to_string(), format!("value_{suffix}")),
                ("host".to_string(), "a".to_string()),
            ];
            labels.sort();
            phantom.push((labels, 600_000i64, v));
        }
        db.ts_merge("cpu_5m", &phantom).unwrap();

        // A window fully below the head boundary: the phantom bucket shows
        // up — the rollup served it.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(5m, ts) AS t, count(value), sum(value) FROM cpu \
                 WHERE ts < 900000 GROUP BY t ORDER BY t",
            )
            .unwrap(),
        );
        // Rollup-served bucket keys are Timestamps (the documented typing
        // nuance of the rollup path).
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Timestamp(0), Value::Int(5), Value::Float(10.0)],
                vec![Value::Timestamp(600_000), Value::Int(2), Value::Float(42.0)],
            ]
        );

        // A whole-table window stitches: rollup below the boundary (phantom
        // included), raw head above it.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(5m, ts) AS t, count(value), sum(value) FROM cpu \
                 GROUP BY t ORDER BY t",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Timestamp(0), Value::Int(5), Value::Float(10.0)],
                vec![Value::Timestamp(600_000), Value::Int(2), Value::Float(42.0)],
                vec![Value::Timestamp(4 * h), Value::Int(1), Value::Float(100.0)],
            ]
        );
    }

    /// Repair-merged samples landing in already-aggregated buckets must
    /// retroactively update the rollup: the touched bucket is recomputed
    /// from the source and the newer rows win the dedupe.
    #[test]
    fn timeseries_rollup_backfill_on_repair_merge() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host), RETENTION 30d)")
            .unwrap();
        db.execute("CREATE ROLLUP cpu_5m ON cpu BUCKET 5m RETENTION 90d")
            .unwrap();
        let (m, h) = (60_000i64, 3_600_000i64);
        // Five samples in bucket 0 (0..5m): count 5, sum 0+1+2+3+4 = 10.
        for i in 0..5i64 {
            db.execute(&format!(
                "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, {})",
                i * m,
                i
            ))
            .unwrap();
        }
        // A far-future append flushes the first window → rollup maintained.
        db.execute(&format!(
            "INSERT INTO cpu (host, ts, value) VALUES ('a', {}, 100)",
            4 * h
        ))
        .unwrap();
        let field = |series: &[(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)], name: &str| {
            series
                .iter()
                .find(|(labels, _)| {
                    labels.iter().any(|(k, v)| k == "__field__" && v == name)
                })
                .and_then(|(_, samples)| samples.first())
                .map(|s| s.value)
        };
        let series = db.ts_query("cpu_5m", &[], 0, 0).unwrap();
        assert_eq!(field(&series, "value_sum"), Some(10.0));
        assert_eq!(field(&series, "value_count"), Some(5.0));

        // Repair-merge two gap-filled samples into the aggregated bucket.
        let labels = db.ts_series_labels("cpu").unwrap().remove(0);
        db.ts_merge(
            "cpu",
            &[
                (labels.clone(), 10_000, 100.0),
                (labels, 20_000, 50.0),
            ],
        )
        .unwrap();

        // Sanity: the source sees all 7 samples in the bucket.
        let src = db.ts_query("cpu", &[], 0, 299_999).unwrap();
        let total: usize = src.iter().map(|(_, ss)| ss.len()).sum();
        assert_eq!(total, 7, "source bucket samples: {src:?}");

        // The bucket was recomputed: count 7, sum 160, max 100.
        let series = db.ts_query("cpu_5m", &[], 0, 0).unwrap();
        assert_eq!(field(&series, "value_sum"), Some(160.0));
        assert_eq!(field(&series, "value_count"), Some(7.0));
        assert_eq!(field(&series, "value_max"), Some(100.0));
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

    /// External (GSSAPI) users are passwordless: they resolve to their
    /// own-named role for grants, carry no SCRAM credential, and persist their
    /// external kind across a reopen.
    #[test]
    fn external_gssapi_users_resolve_to_a_role_without_a_credential() {
        use skaidb_auth::{Object, Privilege};
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute(r#"CREATE USER "alice@EXAMPLE.COM" GSSAPI"#).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            // Grants target the principal's own-named role, exactly like a
            // password user.
            db.execute(r#"GRANT SELECT ON t TO "alice@EXAMPLE.COM""#).unwrap();
            // The external principal maps to a role...
            assert_eq!(
                db.external_user_role("alice@EXAMPLE.COM").as_deref(),
                Some("alice@EXAMPLE.COM")
            );
            // ...but has no SCRAM credential and cannot take the password path.
            assert!(db.auth_user("alice@EXAMPLE.COM").is_none());
            // A password user is never reachable via the external path.
            db.execute("CREATE USER bob PASSWORD 'x'").unwrap();
            assert!(db.external_user_role("bob").is_none());
            assert!(db.auth_user("bob").is_some());
        }
        // Reopen: the external kind survived (still no credential, still a role).
        let db = Database::open(&dir).unwrap();
        assert_eq!(
            db.external_user_role("alice@EXAMPLE.COM").as_deref(),
            Some("alice@EXAMPLE.COM")
        );
        assert!(db.auth_user("alice@EXAMPLE.COM").is_none());
        assert!(db.has_privilege(
            "alice@EXAMPLE.COM",
            Privilege::Select,
            &Object::Table("t".into())
        ));
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
            db.create_vector_index("docs_emb", "docs", "embedding", "cosine", None, false, false)
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

    /// Column-pruned GROUP BY gathers (agencik wishlist E-7: an unfiltered
    /// `GROUP BY` over a wide-row table OOM-killed the node — grouped
    /// gathers decoded every column of every row before discarding all but
    /// a handful). These prove the pruned path returns identical results to
    /// the full-decode path across every shape `group_by_projection_columns`
    /// claims to cover; they don't (can't, from a unit test) prove the
    /// memory win, which is the storage-level `codec.rs` tests' job.
    mod group_by_column_pruning {
        use super::*;

        fn wide_table(db: &mut Database) {
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            // `body` stands in for gmail_emails' large body_* fields: never
            // referenced by any query below, so it must never affect a
            // result — only (if the pruning is working) never get decoded.
            db.execute(
                "INSERT INTO t (id, account, amount, label, body) VALUES \
                 (1,'a',10,'p','X'), (2,'a',20,'q','Y'), \
                 (3,'b',5,'r','Z'), (4,'b',40,'s','W')",
            )
            .unwrap();
        }

        #[test]
        fn count_star_matches_the_e7_repro_shape() {
            let mut db = Database::open(tempdir()).unwrap();
            wide_table(&mut db);
            let rs = rows(
                db.execute("SELECT account, COUNT(*) FROM t GROUP BY account ORDER BY account")
                    .unwrap(),
            );
            assert_eq!(
                rs.rows,
                vec![
                    vec![Value::String("a".into()), Value::Int(2)],
                    vec![Value::String("b".into()), Value::Int(2)],
                ]
            );
        }

        #[test]
        fn sum_and_having_and_order_by_and_non_aggregated_item() {
            // Stresses every column source group_by_projection_columns
            // unions: group_by (account), an aggregate arg (amount), HAVING
            // (amount again), ORDER BY (account), and a non-aggregated
            // select item read from the group's representative row (label
            // — MySQL-style "any value" semantics; first-seen row per
            // group is id order, so account 'a' group's rep is id=1).
            let mut db = Database::open(tempdir()).unwrap();
            wide_table(&mut db);
            let rs = rows(
                db.execute(
                    "SELECT account, label, SUM(amount) FROM t GROUP BY account \
                     HAVING SUM(amount) > 10 ORDER BY account",
                )
                .unwrap(),
            );
            assert_eq!(
                rs.rows,
                vec![
                    vec![
                        Value::String("a".into()),
                        Value::String("p".into()),
                        Value::Int(30)
                    ],
                    vec![
                        Value::String("b".into()),
                        Value::String("r".into()),
                        Value::Int(45)
                    ],
                ]
            );
        }

        #[test]
        fn filtered_group_by_still_filters_correctly() {
            // The filter's own column (amount) isn't in group_by/items/
            // having/order_by at all here — proving group_by_projection_
            // columns' explicit filter-column union (not left to callers)
            // is actually wired through, not just unit-tested in isolation.
            let mut db = Database::open(tempdir()).unwrap();
            wide_table(&mut db);
            let rs = rows(
                db.execute(
                    "SELECT account, COUNT(*) FROM t WHERE amount > 8 \
                     GROUP BY account ORDER BY account",
                )
                .unwrap(),
            );
            assert_eq!(
                rs.rows,
                vec![
                    vec![Value::String("a".into()), Value::Int(2)],
                    vec![Value::String("b".into()), Value::Int(1)],
                ]
            );
        }

        #[test]
        fn nested_path_group_by_still_correct() {
            // meta.region: a nested path — group_by_projection_columns wants
            // the whole top-level `meta` field (no sub-field precision), so
            // this exercises decode_document_projected keeping a whole
            // wanted document intact rather than also pruning inside it.
            let mut db = Database::open(tempdir()).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute(
                "INSERT INTO t (id, meta, body) VALUES \
                 (1, {region: 'us'}, 'X'), (2, {region: 'us'}, 'Y'), \
                 (3, {region: 'eu'}, 'Z')",
            )
            .unwrap();
            let rs = rows(
                db.execute(
                    "SELECT meta.region, COUNT(*) FROM t GROUP BY meta.region ORDER BY meta.region",
                )
                .unwrap(),
            );
            assert_eq!(
                rs.rows,
                vec![
                    vec![Value::String("eu".into()), Value::Int(1)],
                    vec![Value::String("us".into()), Value::Int(2)],
                ]
            );
        }

        #[test]
        fn plain_aggregate_without_group_by_is_also_pruned_correctly() {
            // group_by.is_empty() is the single-group case inside
            // select_aggregate — same code path, same column-need analysis.
            let mut db = Database::open(tempdir()).unwrap();
            wide_table(&mut db);
            let rs = rows(db.execute("SELECT SUM(amount) FROM t").unwrap());
            assert_eq!(rs.rows, vec![vec![Value::Int(75)]]);
        }

        #[test]
        fn wildcard_and_top_k_and_joins_bypass_pruning_but_stay_correct() {
            // These are the exact cases group_by_projection_columns returns
            // None for — proving the bypass itself doesn't break anything
            // (a regression here would mean the None-guard conditions
            // themselves are wired wrong, e.g. inverted).
            let mut db = Database::open(tempdir()).unwrap();
            wide_table(&mut db);
            let topk = rows(
                db.execute("SELECT * FROM t GROUP BY account TOP 1 BY amount DESC ORDER BY account")
                    .unwrap(),
            );
            assert_eq!(topk.rows.len(), 2);
            let star = rows(db.execute("SELECT * FROM t WHERE account = 'a'").unwrap());
            assert_eq!(star.rows.len(), 2);
        }

        #[test]
        fn matches_full_decode_across_every_query_shape() {
            // Same queries, twice: once against the wide table (pruning
            // eligible), once against a table with the exact same rows but
            // NO extra wide column (pruning is a no-op there — every column
            // is wanted). Byte-identical results across both prove pruning
            // changes nothing observable.
            let mut wide = Database::open(tempdir()).unwrap();
            wide_table(&mut wide);
            let mut narrow = Database::open(tempdir()).unwrap();
            narrow.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            narrow
                .execute(
                    "INSERT INTO t (id, account, amount, label) VALUES \
                     (1,'a',10,'p'), (2,'a',20,'q'), (3,'b',5,'r'), (4,'b',40,'s')",
                )
                .unwrap();
            for sql in [
                "SELECT account, COUNT(*) FROM t GROUP BY account ORDER BY account",
                "SELECT account, SUM(amount) FROM t GROUP BY account HAVING SUM(amount) > 10 ORDER BY account",
                "SELECT account, label FROM t WHERE amount > 8 GROUP BY account, label ORDER BY account, label",
            ] {
                let w = rows(wide.execute(sql).unwrap());
                let n = rows(narrow.execute(sql).unwrap());
                assert_eq!(w.rows, n.rows, "diverged on: {sql}");
                assert_eq!(w.columns, n.columns, "diverged on: {sql}");
            }
        }
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

    // ---- full-text search ----

    /// `articles(id, body, flag)` with rows 1–3 and a search index on `body`
    /// created **after** the rows exist (exercises the backfill).
    fn search_db() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE articles (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO articles (id, body, flag) VALUES \
             (1, 'the quick brown fox jumps', true), \
             (2, 'quick quick quick delivery', false), \
             (3, 'slow roasted vegetables', true)",
        )
        .unwrap();
        db.execute("CREATE SEARCH INDEX articles_fts ON articles (body)")
            .unwrap();
        db
    }

    /// The `id` column of every row, sorted (search hit order is unspecified
    /// on the predicate-only path).
    fn sorted_ids(rs: ResultSet) -> Vec<i64> {
        let mut out = ids(rs);
        out.sort_unstable();
        out
    }

    #[test]
    fn search_index_backfills_and_ranks_by_score() {
        let mut db = search_db();
        // Backfill is committed at create: immediately searchable, ranked.
        let rs = rows(
            db.execute(
                "SELECT id, score() FROM articles WHERE MATCH(body, 'quick') \
                 ORDER BY score() DESC LIMIT 5",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["id", "score"]);
        assert_eq!(rs.rows.len(), 2);
        // Row 2 repeats the term in a shorter field: the better BM25 score.
        assert_eq!(rs.rows[0][0], Value::Int(2));
        assert_eq!(rs.rows[1][0], Value::Int(1));
        let score = |row: &[Value]| match row[1] {
            Value::Float(f) => f,
            ref other => panic!("expected float score, got {other:?}"),
        };
        assert!(score(&rs.rows[0]) > score(&rs.rows[1]));
        assert!(score(&rs.rows[1]) > 0.0);
        // LIMIT caps the ranked gather.
        let rs = rows(
            db.execute(
                "SELECT id FROM articles WHERE MATCH(body, 'quick') \
                 ORDER BY score() DESC LIMIT 1",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    }

    #[test]
    fn search_write_path_reads_its_own_writes() {
        let mut db = search_db();
        // Insert after create: visible without waiting for the NRT refresh.
        db.execute("INSERT INTO articles (id, body, flag) VALUES (4, 'a very quick reply', true)")
            .unwrap();
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2, 4]);
        // UPDATE re-indexes the row.
        db.execute("UPDATE articles SET body = 'calm response' WHERE id = 4").unwrap();
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'calm')").unwrap());
        assert_eq!(sorted_ids(rs), vec![4]);
        // DELETE removes it from the index.
        db.execute("DELETE FROM articles WHERE id = 2").unwrap();
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1]);
    }

    #[test]
    fn search_residual_predicate_filters_hits() {
        let mut db = search_db();
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick') AND flag = true")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1]);
        // The residual applies on the ranked path too.
        let rs = rows(
            db.execute(
                "SELECT id FROM articles WHERE MATCH(body, 'quick') AND flag = true \
                 ORDER BY score() DESC LIMIT 5",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs), vec![1]);
    }

    /// Hybrid retrieval (`RANK BY RRF`) fuses the text leg (MATCH) and the
    /// vector leg (NEAREST) by Reciprocal Rank Fusion: a doc that ranks well in
    /// BOTH legs beats one that ranks in only a single leg, `rrf_score()` is
    /// exposed and monotone with the order, and the residual filter applies to
    /// both legs.
    #[test]
    fn hybrid_rrf_fuses_text_and_vector_legs() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        // id1: matches "quick" AND is the closest vector to [1,0,0] -> both legs.
        // id2: matches "quick" but a far vector -> text leg (vector rank last).
        // id3: no "quick" but a close vector -> vector leg only.
        db.execute(
            "INSERT INTO docs (id, body, emb, flag) VALUES \
             (1, 'quick brown fox', [1.0, 0.0, 0.0], true), \
             (2, 'quick delivery service', [0.0, 1.0, 0.0], false), \
             (3, 'slow roasted meal', [0.9, 0.1, 0.0], true)",
        )
        .unwrap();
        db.execute("CREATE SEARCH INDEX docs_fts ON docs (body)").unwrap();
        db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb) DIM 3 USING cosine")
            .unwrap();

        // Fuse: NEAREST supplies the vector leg, WHERE MATCH the text leg.
        let rs = rows(
            db.execute(
                "SELECT id, rrf_score() FROM docs \
                 NEAREST (emb, [1.0, 0.0, 0.0], 3) \
                 WHERE MATCH(body, 'quick') \
                 RANK BY RRF LIMIT 10",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["id", "rrf_score"]);
        let score = |r: usize| match rs.rows[r][1] {
            Value::Float(f) => f,
            ref o => panic!("expected float rrf_score, got {o:?}"),
        };
        assert!(score(0) > score(1) && score(1) > score(2), "rrf desc");
        // id1 (both legs, vector rank 1) tops; id2 (both lists) beats id3 (one).
        assert_eq!(ids(rs), vec![1, 2, 3]);

        // Residual filter (flag = true) applies to BOTH legs: id2 (flag=false)
        // drops from both; id1 (both legs) + id3 (vector leg, flag=true) remain.
        let rs = rows(
            db.execute(
                "SELECT id FROM docs NEAREST (emb, [1.0, 0.0, 0.0], 3) \
                 WHERE MATCH(body, 'quick') AND flag = true RANK BY RRF LIMIT 10",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs), vec![1, 3]);

        // A custom RRF constant parses and still ranks id1 first.
        let rs = rows(
            db.execute(
                "SELECT id FROM docs NEAREST (emb, [1.0, 0.0, 0.0], 3) \
                 WHERE MATCH(body, 'quick') RANK BY RRF (10) LIMIT 10",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs)[0], 1);
    }

    /// Geospatial scalar functions: `geo_distance` (haversine metres) filters
    /// and sorts, `geo_bbox` tests a bounding box, and a non-point value yields
    /// NULL (excluded) rather than an error.
    #[test]
    fn geo_distance_and_bbox_filter_and_sort() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE places (PRIMARY KEY (id))").unwrap();
        // `[lat, lon]` points: (0,0), 1° east, 1° north; plus a non-point row.
        db.execute(
            "INSERT INTO places (id, loc) VALUES \
             (1, [0.0, 0.0]), (2, [0.0, 1.0]), (3, [1.0, 0.0]), (4, 'not-a-point')",
        )
        .unwrap();

        // Distance filter: only (0,0) is within 1 km of (0,0).
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 1000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1]);

        // Wider radius: the three points; id4 (non-point -> NULL distance) is out.
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 200000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2, 3]);

        // Nearest-first sort (bounded top-k over the scalar expr).
        let rs = rows(
            db.execute(
                "SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 200000 \
                 ORDER BY geo_distance(loc, 0.0, 0.0) LIMIT 3",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs)[0], 1);

        // Distance value: 1° of longitude at the equator ≈ 111.3 km.
        let rs = rows(
            db.execute("SELECT geo_distance(loc, 0.0, 0.0) AS d FROM places WHERE id = 2")
                .unwrap(),
        );
        match rs.rows[0][0] {
            Value::Float(d) => assert!((d - 111_319.0).abs() < 500.0, "d = {d}"),
            ref o => panic!("expected float distance, got {o:?}"),
        }

        // Bounding box: only (0,0) is inside [-0.5,0.5] × [-0.5,0.5].
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_bbox(loc, -0.5, -0.5, 0.5, 0.5)")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1]);
    }

    /// Geo index: `CREATE GEO INDEX` accelerates `geo_distance` / `geo_bbox`
    /// transparently — same results as the scan, with the index engaged (proven
    /// via EXPLAIN), across backfill of pre-existing rows, live inserts, an
    /// UPDATE that moves a point out of range, and a DELETE.
    #[test]
    fn geo_index_prunes_and_stays_consistent() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE places (PRIMARY KEY (id))").unwrap();
        // Rows inserted BEFORE the index exists — the backfill must pick them up.
        // A spread of points; only those near (0,0) are "in range" below.
        db.execute(
            "INSERT INTO places (id, loc) VALUES \
             (1, {lat: 0.0, lon: 0.0}), (2, {lat: 0.1, lon: 0.1}), \
             (3, {lat: 40.0, lon: 40.0}), (4, {lat: -35.0, lon: 150.0}), \
             (5, 'not-a-point')",
        )
        .unwrap();
        db.execute("CREATE GEO INDEX places_geo ON places (loc)").unwrap();

        // EXPLAIN proves the geo index is chosen for a geo_distance predicate.
        let ex = rows(
            db.execute(
                "EXPLAIN SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000",
            )
            .unwrap(),
        );
        let access: String = ex
            .rows
            .iter()
            .find(|r| r[0] == Value::String("access".into()))
            .map(|r| match &r[1] {
                Value::String(s) => s.clone(),
                o => format!("{o:?}"),
            })
            .unwrap_or_default();
        assert!(access.contains("geo index scan"), "explain access = {access}");

        // Radius query: only (0,0) and (0.1,0.1) are within 50 km of (0,0).
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2]);

        // Bounding box over the same neighborhood — same two rows.
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_bbox(loc, -0.5, -0.5, 0.5, 0.5)")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2]);

        // A row inserted AFTER the index is maintained live.
        db.execute("INSERT INTO places (id, loc) VALUES (6, {lat: 0.2, lon: -0.2})")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2, 6]);

        // UPDATE moves row 2 far away — the old Morton entry must be cleared so
        // it no longer answers the near query (and answers a far one).
        db.execute("UPDATE places SET loc = {lat: 80.0, lon: 80.0} WHERE id = 2")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 6]);
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 80.0, 80.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![2]);

        // DELETE removes row 1 from the index.
        db.execute("DELETE FROM places WHERE id = 1").unwrap();
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![6]);

        // ORDER BY geo_distance still works through the index (re-sorted by the
        // executor since Z-order is not distance order).
        let rs = rows(
            db.execute(
                "SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 20000000 \
                 ORDER BY geo_distance(loc, 0.0, 0.0) LIMIT 1",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs)[0], 6);

        // Survives reopen: entries are durable (no rebuild), still consistent.
        drop(db);
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(
            db.execute("SELECT id FROM places WHERE geo_distance(loc, 0.0, 0.0) <= 50000")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![6]);
    }

    /// Managed (`EMBED`) vector index: skaidb embeds the text column at ingest
    /// (existing rows at create, new rows out-of-band drained at commit) and
    /// auto-embeds a string `NEAREST` query. Uses the deterministic
    /// `HashEmbedder` (shared tokens → similar vectors), so the token-overlap
    /// nearest doc ranks first.
    #[test]
    fn managed_embed_index_embeds_and_auto_embeds_query() {
        use std::sync::Arc;
        let mut db = Database::open(tempdir()).unwrap();
        db.set_embedder(Arc::new(crate::embed::HashEmbedder::new(16)));
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO docs (id, body) VALUES \
             (1, 'quick brown fox'), (2, 'slow green turtle'), (3, 'quick fast rabbit')",
        )
        .unwrap();
        // Managed index: embeds the `body` TEXT column; existing rows embedded
        // at create.
        db.execute("CREATE VECTOR INDEX docs_sem ON docs (body) EMBED DIM 16")
            .unwrap();

        // A string NEAREST auto-embeds the query; id1 (shares 'quick'+'fox')
        // is closest.
        let rs = rows(
            db.execute("SELECT id FROM docs NEAREST (body, 'quick fox', 3)")
                .unwrap(),
        );
        assert_eq!(ids(rs)[0], 1);

        // A row inserted AFTER the index is embedded (drained at commit) and
        // searchable; id4 and id1 both share 'quick'+'fox', so both top.
        db.execute("INSERT INTO docs (id, body) VALUES (4, 'quick fox jumps')")
            .unwrap();
        let rs = rows(
            db.execute("SELECT id FROM docs NEAREST (body, 'quick fox', 4)")
                .unwrap(),
        );
        let top2: Vec<i64> = ids(rs).into_iter().take(2).collect();
        assert!(top2.contains(&1) && top2.contains(&4), "id1 & id4 top: {top2:?}");

        // EMBED needs a configured embedder; a fresh db (none set) errors.
        let mut plain = Database::open(tempdir()).unwrap();
        plain.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let err = plain
            .execute("CREATE VECTOR INDEX t_sem ON t (body) EMBED DIM 16")
            .unwrap_err();
        assert!(err.to_string().contains("inference"), "{err}");
    }

    /// Failure injection: a torn search-index directory (truncated
    /// meta.json, a deleted segment file, or a wiped directory) must
    /// rebuild from the table on reopen — never error, never lose hits.
    #[test]
    fn search_index_rebuilds_from_torn_dir() {
        let dir = tempdir();
        let seed = |db: &mut Database| {
            db.execute("CREATE TABLE articles (PRIMARY KEY (id))").ok();
            db.execute(
                "INSERT INTO articles (id, body) VALUES \
                 (1, 'quick brown fox'), (2, 'quick delivery'), (3, 'slow snail')",
            )
            .ok();
            db.execute("CREATE SEARCH INDEX articles_fts ON articles (body)")
                .ok();
        };
        let hits = |db: &mut Database| {
            sorted_ids(rows(
                db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')")
                    .unwrap(),
            ))
        };
        {
            let mut db = Database::open(&dir).unwrap();
            seed(&mut db);
            assert_eq!(hits(&mut db), vec![1, 2]);
        }
        let idx_dir = dir.join("fts").join("articles_fts");

        // Injection 1: truncate meta.json (a torn write).
        let meta = idx_dir.join("meta.json");
        let content = std::fs::read(&meta).unwrap();
        std::fs::write(&meta, &content[..content.len() / 2]).unwrap();
        {
            let mut db = Database::open(&dir).unwrap();
            assert_eq!(hits(&mut db), vec![1, 2], "after truncated meta.json");
        }

        // Injection 2: delete a segment payload file, keep meta.json.
        let victim = std::fs::read_dir(&idx_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.extension().is_some_and(|e| e != "json") && p.is_file()
            });
        if let Some(victim) = victim {
            std::fs::remove_file(victim).unwrap();
        }
        {
            let mut db = Database::open(&dir).unwrap();
            assert_eq!(hits(&mut db), vec![1, 2], "after deleted segment file");
        }

        // Injection 3: the whole index directory gone.
        std::fs::remove_dir_all(&idx_dir).unwrap();
        {
            let mut db = Database::open(&dir).unwrap();
            assert_eq!(hits(&mut db), vec![1, 2], "after wiped index dir");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Row TTL: a row past its TTL is invisible to reads (point + scan)
    /// and reclaimed by compaction. Uses HLC physical (wall) time, so the
    /// test writes with backdated stamps via a direct low-level put would
    /// be ideal — here we assert the visible behavior with a tiny TTL and
    /// a short sleep, plus the parse/catalog round-trip.
    #[test]
    fn row_ttl_expiry() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE s (PRIMARY KEY (id)) WITH (ttl = 50ms)").unwrap();
        db.execute("INSERT INTO s (id, v) VALUES (1, 'ephemeral')").unwrap();
        // Immediately visible.
        assert_eq!(
            rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()).rows,
            vec![vec![Value::String("ephemeral".into())]]
        );
        std::thread::sleep(std::time::Duration::from_millis(80));
        // Expired: gone from point read and scan.
        assert!(rows(db.execute("SELECT v FROM s WHERE id = 1").unwrap()).rows.is_empty());
        assert!(rows(db.execute("SELECT v FROM s").unwrap()).rows.is_empty());

        // The TTL persists in the catalog and applies after a reopen.
        drop(db);
        let mut db = Database::open(&dir).unwrap();
        db.execute("INSERT INTO s (id, v) VALUES (2, 'also short')").unwrap();
        assert_eq!(rows(db.execute("SELECT id FROM s").unwrap()).rows.len(), 1);
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert!(rows(db.execute("SELECT id FROM s").unwrap()).rows.is_empty());

        // A table without TTL keeps its rows.
        db.execute("CREATE TABLE keep (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO keep (id) VALUES (1)").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert_eq!(rows(db.execute("SELECT id FROM keep").unwrap()).rows.len(), 1);

        // Bad TTL is a parse/exec error.
        assert!(db
            .execute("CREATE TABLE bad (PRIMARY KEY (id)) WITH (ttl = 0)")
            .is_err());
        assert!(db
            .execute("CREATE TABLE bad (PRIMARY KEY (id)) WITH (nope = 1)")
            .is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BACKUP TO + RESTORE FROM: round-trip every store (rows, search
    /// index, time-series), with the pre-restore data kept aside.
    #[test]
    fn backup_and_restore_roundtrip() {
        let dir = tempdir();
        let backup = {
            let mut b = dir.clone();
            b.set_extension("bak");
            let _ = std::fs::remove_dir_all(&b);
            b
        };
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 'keep me')").unwrap();
        db.execute("CREATE SEARCH INDEX t_fts ON t (v)").unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h))").unwrap();
        db.execute("INSERT INTO m (h, ts, value) VALUES ('a', 1000, 1.5)").unwrap();

        let rs = rows(
            db.execute(&format!("BACKUP TO '{}'", backup.display())).unwrap(),
        );
        assert_eq!(rs.columns[0], "path");
        assert!(matches!(rs.rows[0][1], Value::Int(n) if n > 0), "files copied");
        // Refuses to overwrite.
        assert!(db.execute(&format!("BACKUP TO '{}'", backup.display())).is_err());

        // Diverge, then restore — the backup state comes back everywhere.
        db.execute("INSERT INTO t (id, v) VALUES (2, 'post-backup')").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        let rs = rows(
            db.execute(&format!("RESTORE FROM '{}'", backup.display())).unwrap(),
        );
        assert_eq!(rs.columns, vec!["restored_from", "previous_data"]);
        let rs = rows(db.execute("SELECT id, v FROM t ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(1), Value::String("keep me".into())]]
        );
        // Search works on the restored index; TS samples survived.
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(v, 'keep')").unwrap());
        assert_eq!(rs.rows.len(), 1);
        let rs = rows(db.execute("SELECT value FROM m WHERE h = 'a'").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Float(1.5)]]);
        // A garbage path is rejected cleanly.
        assert!(db.execute("RESTORE FROM '/nonexistent/nope'").is_err());
        let _ = std::fs::remove_dir_all(&backup);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// EXPLAIN <statement>: the advisory plan rows track the planner's
    /// actual access-path choices (point read / index scan / full scan /
    /// search pushdowns / vector search) without executing anything.
    #[test]
    fn explain_plan() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE e (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX e_age ON e (age)").unwrap();
        db.execute("CREATE SEARCH INDEX e_fts ON e (body)").unwrap();
        db.execute("CREATE VECTOR INDEX e_emb ON e (emb) DIM 3").unwrap();
        db.execute("INSERT INTO e (id, age, body, emb) VALUES (1, 30, 'hello world', [1.0, 0.0, 0.0])")
            .unwrap();

        let mut decision = |sql: &str, aspect: &str| -> String {
            let rs = rows(db.execute(sql).unwrap());
            assert_eq!(rs.columns, vec!["aspect", "decision"]);
            rs.rows
                .iter()
                .find(|r| r[0] == Value::String(aspect.into()))
                .map(|r| match &r[1] {
                    Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                })
                .unwrap_or_default()
        };
        // Access-path classification mirrors the executor's choices.
        assert!(decision("EXPLAIN SELECT * FROM e WHERE id = 1", "access").contains("point read"));
        assert!(decision("EXPLAIN SELECT * FROM e WHERE age > 10", "access").contains("e_age"));
        assert!(decision("EXPLAIN SELECT * FROM e WHERE name = 'x'", "access")
            .contains("full table scan"));
        assert!(decision(
            "EXPLAIN SELECT id, score() FROM e WHERE MATCH(body, 'hello') ORDER BY score() DESC LIMIT 5",
            "access"
        )
        .contains("BM25 top-k"));
        assert!(decision(
            "EXPLAIN SELECT count(*) FROM e WHERE MATCH(body, 'hello') GROUP BY tag",
            "access"
        )
        .contains("aggregation"));
        assert!(decision("EXPLAIN SELECT id FROM e NEAREST (emb, [1.0, 0.0, 0.0], 1)", "access")
            .contains("e_emb"));
        // DML explains as a write plan; nothing executes.
        assert!(decision("EXPLAIN DELETE FROM e WHERE id = 1", "access").contains("point read"));
        let rs = rows(db.execute("SELECT id FROM e").unwrap());
        assert_eq!(rs.rows.len(), 1, "EXPLAIN DELETE must not delete");
        // Nested EXPLAIN is rejected at parse time.
        assert!(db.execute("EXPLAIN EXPLAIN SELECT * FROM e").is_err());
    }

    /// The WHERE expression of `SELECT * FROM t WHERE <cond>` (test helper).
    fn parse_filter(cond: &str) -> Option<skaidb_sql::Expr> {
        match skaidb_sql::parse(&format!("SELECT * FROM t WHERE {cond}")).unwrap() {
            skaidb_sql::Statement::Select(s) => s.filter,
            other => panic!("expected select, got {other:?}"),
        }
    }

    /// GLOBAL index entry plumbing (phase 1, docs/GLOBAL_INDEXES.md): row
    /// mutations keep the internal `__gidx__` entry table in exact lockstep —
    /// puts add entries, value changes retract the old entry, deletes retract
    /// everything. No reader exists yet; parity of the entry set is the
    /// contract.
    #[test]
    fn global_index_entries_track_row_mutations() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE msgs (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX msgs_user ON msgs (sender) WITH (global = true)")
            .unwrap();
        // SHOW INDEXES reports the global class, healthy.
        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        let row = rs
            .rows
            .iter()
            .find(|r| r[0] == Value::String("msgs_user".into()))
            .expect("global index listed");
        assert_eq!(row[2], Value::String("global".into()));
        assert_eq!(row[4], Value::String("ok".into()));

        let entries = |db: &Database| -> Vec<(Vec<u8>, bool)> {
            db.local_scan_versioned_page(&gidx_table("msgs_user"), None, 100)
                .unwrap()
                .into_iter()
                .map(|(k, _, _, is_put)| (k, is_put))
                .collect()
        };

        db.execute("INSERT INTO msgs (id, sender, body) VALUES (1, 'ala', 'x'), (2, 'bob', 'y'), (3, 'ala', 'z')")
            .unwrap();
        let live: Vec<_> = entries(&db).into_iter().filter(|(_, p)| *p).collect();
        assert_eq!(live.len(), 3, "one entry per row");

        // Changing the indexed value retracts the old entry and adds the new
        // one; the row count stays 3.
        db.execute("UPDATE msgs SET sender = 'cyd' WHERE id = 1").unwrap();
        let now = entries(&db);
        let live: Vec<_> = now.iter().filter(|(_, p)| *p).collect();
        let dead: Vec<_> = now.iter().filter(|(_, p)| !*p).collect();
        assert_eq!(live.len(), 3);
        assert_eq!(dead.len(), 1, "old 'ala' entry tombstoned");

        // Rewriting a row WITHOUT changing the indexed value adds nothing.
        db.execute("UPDATE msgs SET body = 'w' WHERE id = 2").unwrap();
        assert_eq!(entries(&db).iter().filter(|(_, p)| *p).count(), 3);

        // Deleting a row retracts its entry.
        db.execute("DELETE FROM msgs WHERE id = 3").unwrap();
        assert_eq!(entries(&db).iter().filter(|(_, p)| *p).count(), 2);

        // The entry table is internal: hidden from SHOW TABLES.
        let ts = rows(db.execute("SHOW TABLES").unwrap());
        assert!(
            ts.rows.iter().all(|r| r[0] != Value::String("__gidx__msgs_user".to_string())),
            "entry table hidden"
        );

        // Phase 2: a ready global index plans the routed probe (the local
        // planner still refuses it — plan_index excludes global — but the
        // probe planner and EXPLAIN pick it up).
        let rs = rows(db.execute("EXPLAIN SELECT * FROM msgs WHERE sender = 'bob'").unwrap());
        let access = rs
            .rows
            .iter()
            .find(|r| r[0] == Value::String("access".into()))
            .map(|r| format!("{:?}", r[1]))
            .unwrap_or_default();
        assert!(
            access.contains("global-index probe"),
            "ready global index must plan the routed probe, got: {access}"
        );
        assert!(
            db.plan_global_probe("msgs", &parse_filter("sender = 'bob'")).is_some(),
            "full-tuple equality plans a probe"
        );
        assert!(
            db.plan_global_probe("msgs", &parse_filter("sender > 'a'")).is_none(),
            "ranges do not route"
        );
        assert!(
            db.plan_global_probe("msgs", &parse_filter("body = 'x'")).is_none(),
            "non-indexed column does not route"
        );

        // DROP INDEX removes the entry table with it.
        db.execute("DROP INDEX msgs_user").unwrap();
        assert!(db.local_scan_versioned_page(&gidx_table("msgs_user"), None, 10).is_err());
    }

    /// GLOBAL index backfill (phase 2): creating the index on an already
    /// populated table generates entries for every pre-existing row (inline
    /// on a single node), and the probe planner only serves once backfill
    /// completes.
    #[test]
    fn global_index_backfills_preexisting_rows() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE msgs (PRIMARY KEY (id))").unwrap();
        for i in 0..30 {
            db.execute(&format!(
                "INSERT INTO msgs (id, sender) VALUES ({i}, 'user{}')",
                i % 5
            ))
            .unwrap();
        }
        db.execute("CREATE INDEX msgs_sender ON msgs (sender) WITH (global = true)")
            .unwrap();
        let live = db
            .local_scan_versioned_page(&gidx_table("msgs_sender"), None, 100)
            .unwrap()
            .into_iter()
            .filter(|(_, _, _, is_put)| *is_put)
            .count();
        assert_eq!(live, 30, "backfill produced one entry per pre-existing row");
        // Single-node DDL runs the backfill inline: ready immediately.
        assert!(db.plan_global_probe("msgs", &parse_filter("sender = 'user3'")).is_some());
    }

    /// A raw time-series range dump charges the statement scan budget like
    /// any row gather — an unbounded `SELECT *` over a big range fails with
    /// the budget error instead of materializing until OOM. Aggregations
    /// (bounded partials) stay unmetered.
    #[test]
    fn ts_raw_dump_respects_scan_budget() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TIMESERIES TABLE m (SERIES KEY (host))").unwrap();
        for i in 0..300 {
            db.execute(&format!(
                "INSERT INTO m (host, ts, value) VALUES ('h1', {}, {i})",
                1_000_000 + i * 1000
            ))
            .unwrap();
        }
        // Under budget: served.
        let _armed = scan_meter::arm(1_000, 0, None);
        let rs = rows(db.execute("SELECT * FROM m").unwrap());
        assert_eq!(rs.rows.len(), 300);
        drop(_armed);
        // Over budget: the clean metering error, not an unbounded gather.
        let _armed = scan_meter::arm(100, 0, None);
        let err = db.execute("SELECT * FROM m").unwrap_err();
        assert!(
            err.to_string().contains("scan budget"),
            "expected budget error, got: {err}"
        );
        drop(_armed);
        // Aggregations take the partials path: unaffected by a small budget.
        let _armed = scan_meter::arm(100, 0, None);
        let rs = rows(
            db.execute("SELECT time_bucket(1h, ts) AS t, avg(value) FROM m GROUP BY t").unwrap(),
        );
        assert!(!rs.rows.is_empty());
    }

    /// GLOBAL index phase 3: the repair verify pages fix both crash-window
    /// directions — a missing entry (hides its row from probes) is healed
    /// from the row, an orphan entry (row gone / value changed) is GC'd —
    /// and a `ready = true` schema replay converges a stuck `building` flag.
    #[test]
    fn global_index_verify_heals_and_gcs() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE msgs (PRIMARY KEY (id))").unwrap();
        for i in 0..20 {
            db.execute(&format!(
                "INSERT INTO msgs (id, sender) VALUES ({i}, 'user{}')",
                i % 4
            ))
            .unwrap();
        }
        db.execute("CREATE INDEX msgs_sender ON msgs (sender) WITH (global = true)")
            .unwrap();
        let gname = gidx_table("msgs_sender");
        let live = |db: &Database| -> Vec<Vec<u8>> {
            db.local_scan_versioned_page(&gname, None, 100)
                .unwrap()
                .into_iter()
                .filter(|(_, _, _, p)| *p)
                .map(|(k, _, _, _)| k)
                .collect()
        };
        assert_eq!(live(&db).len(), 20);

        // Manufacture the crash window: delete one entry behind the index's
        // back, and plant an orphan for a row that never existed.
        let victim = live(&db)[7].clone();
        let orphan = gidx_entry_key(
            &[Value::String("ghost".into())],
            &Value::Array(vec![Value::Int(999)]).encode_key(),
        );
        let stamp = skaidb_storage::Hlc::new(u64::MAX / 2, 0);
        db.apply_delete(&gname, &victim, stamp).unwrap();
        db.apply_put(
            &gname,
            &orphan,
            Value::encode_document(&skaidb_types::Document::new()),
            stamp,
        )
        .unwrap();
        assert_eq!(live(&db).len(), 20, "19 real + 1 orphan");

        // Heal pass restores the missing entry…
        let mut cursor = None;
        let mut added = 0;
        loop {
            let (next, n) = db.gidx_heal_page("msgs_sender", cursor.take(), 8).unwrap();
            added += n;
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(added, 1, "exactly the deleted entry healed");
        assert!(live(&db).contains(&victim));

        // …and the GC pass removes the orphan.
        let mut cursor = None;
        let mut removed = 0;
        loop {
            let (next, n) = db.gidx_gc_page("msgs_sender", cursor.take(), 8).unwrap();
            removed += n;
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(removed, 1, "exactly the orphan collected");
        assert_eq!(live(&db).len(), 20);
        assert!(!live(&db).contains(&orphan));
    }

    /// A `WITH (global = true, ready = true)` schema replay (what a node
    /// whose backfill completed emits) converges a peer stuck `building`.
    #[test]
    fn global_index_ready_replay_converges_building() {
        let mut db = Database::open(tempdir()).unwrap();
        db.set_defer_backfills(true); // cluster mode: DDL leaves building=true
        db.execute("CREATE TABLE msgs (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO msgs (id, sender) VALUES (1, 'ala')").unwrap();
        db.execute("CREATE INDEX msgs_sender ON msgs (sender) WITH (global = true)")
            .unwrap();
        assert!(
            db.plan_global_probe("msgs", &parse_filter("sender = 'ala'")).is_none(),
            "building index must not route"
        );
        // The ready-marked replay (newer stamp) flips it.
        db.execute(
            "CREATE INDEX IF NOT EXISTS msgs_sender ON msgs (sender) WITH (global = true, ready = true)",
        )
        .unwrap();
        assert!(
            db.plan_global_probe("msgs", &parse_filter("sender = 'ala'")).is_some(),
            "ready replay converges building"
        );
    }

    /// IN-list probes: every index column pinned by `=`/literal `IN` plans
    /// one probe range per value tuple; over-wide and empty pins degrade
    /// correctly.
    #[test]
    fn global_index_in_list_probe_plans_ranges() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE msgs (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX msgs_sender ON msgs (sender) WITH (global = true)")
            .unwrap();
        let plan = |cond: &str| db.plan_global_probe("msgs", &parse_filter(cond));
        assert_eq!(plan("sender = 'a'").unwrap().1.len(), 1);
        assert_eq!(plan("sender IN ('a', 'b', 'c')").unwrap().1.len(), 3);
        assert_eq!(
            plan("sender IN ('a', 'a', 'b')").unwrap().1.len(),
            2,
            "duplicate list values dedup"
        );
        assert!(plan("sender > 'a'").is_none(), "ranges do not route");
    }

    /// Deferred vector backfill (cluster mode): CREATE VECTOR INDEX with an
    /// explicit DIM acks at schema-apply with the index marked `building`;
    /// searches error "retry shortly", SHOW INDEXES reports `building`, and
    /// paging the backfill flips it to `ok` with the rows searchable.
    #[test]
    fn deferred_vector_backfill_reports_building() {
        let mut db = Database::open(tempdir()).unwrap();
        db.set_defer_backfills(true);
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        for i in 0..20 {
            db.execute(&format!(
                "INSERT INTO docs (id, emb) VALUES ({i}, [{}.0, 1.0, 0.0])",
                i % 7
            ))
            .unwrap();
        }
        db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb) DIM 3").unwrap();

        let local = |db: &mut Database| -> String {
            let rs = rows(db.execute("SHOW INDEXES").unwrap());
            let row = rs
                .rows
                .iter()
                .find(|r| r[0] == Value::String("docs_emb".into()))
                .expect("docs_emb listed");
            match &row[4] {
                Value::String(s) => s.clone(),
                other => panic!("expected string local health, got {other:?}"),
            }
        };
        assert_eq!(local(&mut db), "building");
        let err = db
            .vector_search("docs_emb", &[1.0, 1.0, 0.0], 3, &None)
            .unwrap_err();
        assert!(
            err.to_string().contains("rebuilding"),
            "building index must refuse searches, got: {err}"
        );

        // Drive the backfill in deliberately small pages, like the cluster
        // worker does.
        let mut cursor = None;
        let mut pages = 0;
        while let Some(next) = db.backfill_vector_page("docs_emb", cursor.take(), 8).unwrap() {
            cursor = Some(next);
            pages += 1;
        }
        assert!(pages >= 2, "20 rows at page size 8 must take several pages");
        assert_eq!(local(&mut db), "ok");
        let hits = db.vector_search("docs_emb", &[1.0, 1.0, 0.0], 3, &None).unwrap();
        assert_eq!(hits.len(), 3);

        // Writes landing after DDL but before/while paging are not lost:
        // they maintained the live HNSW directly (id 100 was never seen by a
        // page — insert it now and confirm it is searchable).
        db.execute("INSERT INTO docs (id, emb) VALUES (100, [9.0, 9.0, 9.0])").unwrap();
        let hits = db.vector_search("docs_emb", &[9.0, 9.0, 9.0], 1, &None).unwrap();
        assert_eq!(hits.len(), 1);
    }

    /// ALTER VECTOR INDEX SET (ef = n): live recall/latency tuning;
    /// build-time parameters decline with a rebuild pointer.
    #[test]
    fn alter_vector_index_ef() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE VECTOR INDEX docs_emb ON docs (emb) DIM 3").unwrap();
        db.execute("INSERT INTO docs (id, emb) VALUES (1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])")
            .unwrap();
        db.execute("ALTER VECTOR INDEX docs_emb SET (ef = 200)").unwrap();
        // Still searches correctly after the retune.
        let rs = rows(
            db.execute("SELECT id FROM docs NEAREST (emb, [1.0, 0.0, 0.0], 1)").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
        // Persisted: survives a reopen.
        drop(db);
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(
            db.execute("SELECT id FROM docs NEAREST (emb, [0.0, 1.0, 0.0], 1)").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
        // Build-time knobs and unknown options error clearly.
        assert!(db
            .execute("ALTER VECTOR INDEX docs_emb SET (m = 32)")
            .unwrap_err()
            .to_string()
            .contains("build time"));
        assert!(db
            .execute("ALTER VECTOR INDEX docs_emb SET (nope = 1)")
            .is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// DROP TABLE cascades to ALL derived indexes — secondary, search,
    /// and vector. An orphan is not just clutter: a joining node replays
    /// every live schema object, and `CREATE ... ON <dropped table>`
    /// fails the whole join (bit the production join, 2026-07-09).
    #[test]
    fn drop_table_cascades_all_index_kinds() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE doomed (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX doomed_age ON doomed (age)").unwrap();
        db.execute("CREATE SEARCH INDEX doomed_fts ON doomed (body)").unwrap();
        db.execute("CREATE VECTOR INDEX doomed_emb ON doomed (emb) DIM 3").unwrap();
        db.execute("INSERT INTO doomed (id, age, body, emb) VALUES (1, 3, 'x', [1.0,0.0,0.0])")
            .unwrap();
        db.execute("DROP TABLE doomed").unwrap();

        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        assert!(rs.rows.is_empty(), "no orphans: {:?}", rs.rows);
        // The versioned schema must contain no CREATE referencing the
        // dropped table — replaying it on a fresh node (joiner bootstrap)
        // must succeed entry by entry.
        let mut fresh = Database::open(tempdir()).unwrap();
        for (dbname, sql, hlc) in db.schema_sync() {
            fresh
                .execute_session_with_hlc(&dbname, &sql, hlc)
                .unwrap_or_else(|e| panic!("bootstrap replay failed on {sql:?}: {e}"));
        }
        // Recreating the same names works (tombstones advanced, not stuck).
        db.execute("CREATE TABLE doomed (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE SEARCH INDEX doomed_fts ON doomed (body)").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// DESCRIBE <table>: primary-key columns first (in key order, with `(n/m)`
    /// position), then indexed columns alphabetically, each listing its
    /// covering indexes; MULTIKEY `[]` is stripped from the column name.
    #[test]
    fn describe_lists_key_and_index_columns() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE ev (PRIMARY KEY (channel, ts))").unwrap();
        db.execute("CREATE INDEX ev_actor ON ev (actor)").unwrap();
        db.execute("CREATE INDEX ev_labels ON ev (labels[])").unwrap();
        db.execute("CREATE SEARCH INDEX ev_body ON ev (body)").unwrap();
        db.execute("CREATE VECTOR INDEX ev_emb ON ev (emb) DIM 3").unwrap();

        let rs = rows(db.execute("DESCRIBE ev").unwrap());
        let cell = |v: &Value| match v {
            Value::String(x) => x.clone(),
            other => panic!("non-string cell: {other:?}"),
        };
        let got: Vec<(String, String, String)> = rs
            .rows
            .iter()
            .map(|r| (cell(&r[0]), cell(&r[1]), cell(&r[2])))
            .collect();
        assert_eq!(
            got,
            vec![
                ("channel".into(), "primary key (1/2)".into(), String::new()),
                ("ts".into(), "primary key (2/2)".into(), String::new()),
                ("actor".into(), String::new(), "ev_actor (secondary)".into()),
                ("body".into(), String::new(), "ev_body (search)".into()),
                ("emb".into(), String::new(), "ev_emb (vector)".into()),
                ("labels".into(), String::new(), "ev_labels (secondary)".into()),
            ]
        );

        // A single-column PK reads as a bare "primary key" (no position), and
        // `DESC` is an accepted alias for `DESCRIBE`.
        db.execute("CREATE TABLE k (PRIMARY KEY (id))").unwrap();
        let rs = rows(db.execute("DESC k").unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![
                Value::String("id".into()),
                Value::String("primary key".into()),
                Value::String(String::new()),
            ]]
        );

        // Unknown table is an error, not empty output.
        assert!(db.execute("DESCRIBE nope").is_err());
    }

    /// DESCRIBE <table> FULL samples rows to surface every field (not just
    /// key/indexed ones) with its inferred type, merged with the catalog view.
    #[test]
    fn describe_full_samples_all_fields() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE INDEX t_email ON t (email)").unwrap();
        // `note` is neither key nor indexed — invisible to plain DESCRIBE.
        db.execute("INSERT INTO t (id, email, age, note) VALUES (1, 'a@x', 30, 'hi')")
            .unwrap();
        db.execute("INSERT INTO t (id, email, age) VALUES (2, 'b@x', 41)").unwrap();

        // Plain DESCRIBE shows only key + indexed columns (no `note`, no type).
        let plain = rows(db.execute("DESCRIBE t").unwrap());
        let plain_cols: Vec<String> = plain
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::String(s) => s.clone(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(plain_cols, vec!["id", "email"]);

        // FULL adds a `type` column and every sampled field, including `note`.
        let full = rows(db.execute("DESCRIBE t FULL").unwrap());
        assert_eq!(full.columns, vec!["column", "type", "key", "indexes"]);
        let cell = |v: &Value| match v {
            Value::String(x) => x.clone(),
            other => panic!("non-string cell: {other:?}"),
        };
        let got: Vec<(String, String, String, String)> = full
            .rows
            .iter()
            .map(|r| (cell(&r[0]), cell(&r[1]), cell(&r[2]), cell(&r[3])))
            .collect();
        assert_eq!(
            got,
            vec![
                ("id".into(), "int".into(), "primary key".into(), String::new()),
                ("age".into(), "int".into(), String::new(), String::new()),
                ("email".into(), "string".into(), String::new(), "t_email (secondary)".into()),
                ("note".into(), "string".into(), String::new(), String::new()),
            ]
        );
    }

    /// DESCRIBE <table> FULL EXACT scans everything and serves repeats from
    /// the RAM field registry, invalidated by the table's write stamp: a
    /// field added (or removed) by later writes shows up (or disappears) on
    /// the next call — no restart, no manual refresh.
    #[test]
    fn describe_full_exact_tracks_writes() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("INSERT INTO t (id, a) VALUES (1, 'x')").unwrap();

        let cols = |rs: &ResultSet| -> Vec<String> {
            rs.rows
                .iter()
                .map(|r| match &r[0] {
                    Value::String(s) => s.clone(),
                    _ => unreachable!(),
                })
                .collect()
        };
        // First EXACT scans and caches.
        let rs = rows(db.execute("DESCRIBE t FULL EXACT").unwrap());
        assert_eq!(cols(&rs), vec!["id", "a"]);
        // Cache hit: same result without a write in between.
        let rs = rows(db.execute("DESCRIBE t FULL EXACT").unwrap());
        assert_eq!(cols(&rs), vec!["id", "a"]);
        // A write bumps the stamp — the next EXACT rescans and sees `b`.
        db.execute("INSERT INTO t (id, b) VALUES (2, 7)").unwrap();
        let rs = rows(db.execute("DESCRIBE t FULL EXACT").unwrap());
        assert_eq!(cols(&rs), vec!["id", "a", "b"]);
        // Deleting the only row holding `b` removes it again (exact, not
        // adds-only): the delete bumps the stamp, the rescan no longer sees it.
        db.execute("DELETE FROM t WHERE id = 2").unwrap();
        let rs = rows(db.execute("DESCRIBE t FULL EXACT").unwrap());
        assert_eq!(cols(&rs), vec!["id", "a"]);
        // DROP + recreate under the same name must not serve the old cache.
        db.execute("DROP TABLE t").unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let rs = rows(db.execute("DESCRIBE t FULL EXACT").unwrap());
        assert_eq!(cols(&rs), vec!["id"]);
    }

    /// GROUP BY ... TOP k BY <expr> [ASC|DESC]: per-group top-k rows —
    /// each group returns its k best rows instead of one aggregated row;
    /// ranks by score() under a search predicate.
    #[test]
    fn group_top_k() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE g (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO g (id, cat, v, name) VALUES \
             (1, 'a', 10, 'a10'), (2, 'a', 30, 'a30'), (3, 'a', 20, 'a20'), \
             (4, 'b', 5, 'b5'), (5, 'b', 15, 'b15'), \
             (6, 'c', 1, 'c1')",
        )
        .unwrap();

        // Top 2 per group, best-first (DESC default); groups first-seen order.
        let rs = rows(
            db.execute("SELECT cat, name, v FROM g GROUP BY cat TOP 2 BY v ORDER BY cat, v DESC")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("a".into()), Value::String("a30".into()), Value::Int(30)],
                vec![Value::String("a".into()), Value::String("a20".into()), Value::Int(20)],
                vec![Value::String("b".into()), Value::String("b15".into()), Value::Int(15)],
                vec![Value::String("b".into()), Value::String("b5".into()), Value::Int(5)],
                vec![Value::String("c".into()), Value::String("c1".into()), Value::Int(1)],
            ]
        );
        // ASC ranks smallest-first; wildcard projection works (rows out).
        let rs = rows(
            db.execute("SELECT cat, v FROM g GROUP BY cat TOP 1 BY v ASC ORDER BY cat")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("a".into()), Value::Int(10)],
                vec![Value::String("b".into()), Value::Int(5)],
                vec![Value::String("c".into()), Value::Int(1)],
            ]
        );
        // HAVING filters whole groups before the top-k.
        let rs = rows(
            db.execute(
                "SELECT cat, v FROM g GROUP BY cat TOP 1 BY v HAVING count(*) > 1 ORDER BY cat",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::String("a".into()), Value::Int(30)],
                vec![Value::String("b".into()), Value::Int(15)],
            ]
        );
        // LIMIT pages the flattened output.
        let rs = rows(
            db.execute("SELECT cat, v FROM g GROUP BY cat TOP 2 BY v ORDER BY v DESC LIMIT 2")
                .unwrap(),
        );
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][1], Value::Int(30));
        // Aggregates cannot mix with TOP (rows out, not aggregates).
        assert!(db
            .execute("SELECT cat, count(*) FROM g GROUP BY cat TOP 2 BY v")
            .is_err());
        assert!(db
            .execute("SELECT cat FROM g GROUP BY cat TOP 2 BY count(*)")
            .is_err());
        // TOP 0 and TOP without GROUP BY are parse errors.
        assert!(db.execute("SELECT cat FROM g GROUP BY cat TOP 0 BY v").is_err());

        // With a search predicate: per-group best by BM25 score.
        db.execute("CREATE SEARCH INDEX g_fts ON g (name)").unwrap();
        db.execute(
            "INSERT INTO g (id, cat, name) VALUES \
             (7, 'a', 'quick fox'), (8, 'a', 'quick quick quick fox'), (9, 'b', 'quick dog')",
        )
        .unwrap();
        let rs = rows(
            db.execute(
                "SELECT cat, name, score() FROM g WHERE MATCH(name, 'quick') \
                 GROUP BY cat TOP 1 BY score() ORDER BY cat",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][0], Value::String("a".into()));
        assert_eq!(
            rs.rows[0][1],
            Value::String("quick quick quick fox".into()),
            "the more-repeated term scores best in group a"
        );
        assert_eq!(rs.rows[1][1], Value::String("quick dog".into()));
        assert!(matches!(rs.rows[0][2], Value::Float(f) if f > 0.0));
    }

    /// MATCH_BEST: field-centric dis-max over an explicit column subset —
    /// same match set as OR of per-field MATCHes.
    #[test]
    fn search_match_best_subset() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE d2 (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO d2 (id, title, body, footer) VALUES \
             (1, 'rust guide', 'systems text', 'misc'), \
             (2, 'cooking', 'rust removal from pans', 'misc'), \
             (3, 'gardening', 'plants', 'rust fungus notes')",
        )
        .unwrap();
        db.execute("CREATE SEARCH INDEX d2_fts ON d2 (title, body, footer)")
            .unwrap();
        // Only title+body participate: doc 3 (footer-only match) is out.
        let rs = rows(
            db.execute("SELECT id FROM d2 WHERE MATCH_BEST(title, body, 'rust')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2]);
    }

    /// MATCH_CROSS: the fields act as one big field — a query whose terms
    /// are spread across columns still matches (term-centric, ES
    /// multi_match cross_fields), where per-field MATCH cannot.
    #[test]
    fn search_match_cross_spans_fields() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        db.execute(
            "INSERT INTO docs (id, title, body) VALUES \
             (1, 'rust systems', 'a database engine'), \
             (2, 'gardening', 'growing a database of plants'), \
             (3, 'rust belt', 'industrial history')",
        )
        .unwrap();
        db.execute("CREATE SEARCH INDEX docs_fts ON docs (title, body)")
            .unwrap();
        // 'rust' lives in title, 'database' in body: cross matches 1, 2, 3
        // (OR semantics), and doc 1 — matching both terms — ranks first.
        let rs = rows(
            db.execute(
                "SELECT id FROM docs WHERE MATCH_CROSS(title, body, 'rust database') \
                 ORDER BY score() DESC LIMIT 3",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs)[0], 1);
        // Argument validation: one column is rejected.
        let err = db
            .execute("SELECT id FROM docs WHERE MATCH_CROSS(title, 'rust')")
            .unwrap_err();
        assert!(err.to_string().contains("at least two columns"), "{err}");
    }

    /// BOOSTED: the required predicate decides the set; optionals only
    /// re-rank.
    #[test]
    fn search_boosted_reranks_without_filtering() {
        let mut db = search_db();
        let rs = rows(
            db.execute(
                "SELECT id FROM articles WHERE BOOSTED(MATCH(body, 'quick'), MATCH(body, 'fox')) \
                 ORDER BY score() DESC LIMIT 5",
            )
            .unwrap(),
        );
        // Both quick-docs match; the fox doc outranks the tf-heavy one.
        assert_eq!(ids(rs), vec![1, 2]);
        let err = db
            .execute("SELECT id FROM articles WHERE BOOSTED(MATCH(body, 'quick'), flag = true)")
            .unwrap_err();
        assert!(err.to_string().contains("search predicates"), "{err}");
    }

    /// EXPLAIN SCORE: the per-row BM25 breakdown as a statement — one
    /// JSON row for a matching key, zero rows for a non-match, an error
    /// without a search predicate.
    #[test]
    fn explain_score_statement() {
        let mut db = search_db();
        let rs = rows(
            db.execute(
                "EXPLAIN SCORE SELECT id FROM articles WHERE MATCH(body, 'quick') FOR 1",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["explanation"]);
        assert_eq!(rs.rows.len(), 1);
        let Value::String(text) = &rs.rows[0][0] else {
            panic!("expected a JSON string")
        };
        assert!(text.contains("TermQuery"), "{text}");
        assert!(text.contains("idf"), "{text}");

        // Row 3 ('slow roasted vegetables') does not match → zero rows.
        let rs = rows(
            db.execute(
                "EXPLAIN SCORE SELECT id FROM articles WHERE MATCH(body, 'quick') FOR 3",
            )
            .unwrap(),
        );
        assert!(rs.rows.is_empty());

        // No search predicate → a clear error.
        let err = db
            .execute("EXPLAIN SCORE SELECT id FROM articles WHERE flag = true FOR 1")
            .unwrap_err();
        assert!(err.to_string().contains("MATCH"), "{err}");
    }

    #[test]
    fn search_predicate_only_returns_all_matches() {
        let mut db = search_db();
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        // score() still projects (0.0 on the unranked path).
        let rs = rows(
            db.execute("SELECT id, score() FROM articles WHERE MATCH(body, 'slow')").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3), Value::Float(0.0)]]);
    }

    #[test]
    fn search_phrase_fuzzy_and_query_string_predicates() {
        let mut db = search_db();
        // Exact phrase, then slop lets a transposition in.
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE MATCH_PHRASE(body, 'quick brown')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1]);
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE MATCH_PHRASE(body, 'brown quick', 2)")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1]);
        // Typo within the default distance of 1.
        let rs = rows(db.execute("SELECT id FROM articles WHERE FUZZY(body, 'quikc')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        // Query-string mini-language with required/excluded terms.
        let rs = rows(db.execute("SELECT id FROM articles WHERE SEARCH('+quick -fox')").unwrap());
        assert_eq!(sorted_ids(rs), vec![2]);
    }

    #[test]
    fn search_invalid_positions_and_unindexed_columns_error() {
        let mut db = search_db();
        // A search predicate OR-ed with an ordinary condition cannot be
        // pushed to the index (pure search-predicate OR/NOT trees can).
        let err = db
            .execute("SELECT id FROM articles WHERE MATCH(body, 'quick') OR flag = true")
            .unwrap_err();
        assert!(err.to_string().contains("mixing them"), "{err}");
        // score() without a search predicate has nothing to read.
        let err = db
            .execute("SELECT id, score() FROM articles WHERE flag = true")
            .unwrap_err();
        assert!(matches!(err, EngineError::Type(_)), "{err}");
        // MATCH on a column no index covers.
        let err = db
            .execute("SELECT id FROM articles WHERE MATCH(title, 'x')")
            .unwrap_err();
        assert!(err.to_string().contains("covers column 'title'"), "{err}");
        // MATCH on a table without any search index.
        db.execute("CREATE TABLE plain (PRIMARY KEY (id))").unwrap();
        let err = db
            .execute("SELECT id FROM plain WHERE MATCH(body, 'x')")
            .unwrap_err();
        assert!(err.to_string().contains("has no search index"), "{err}");
        // A ranked search needs a bound.
        let err = db
            .execute("SELECT id FROM articles WHERE MATCH(body, 'quick') ORDER BY score() DESC")
            .unwrap_err();
        assert!(err.to_string().contains("requires LIMIT"), "{err}");
        // Column ordering works (phase 7 — falls back to gather-and-sort
        // for a non-fast column like the row id); score() ordering stays
        // DESC-only.
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick') ORDER BY id LIMIT 5")
                .unwrap(),
        );
        assert_eq!(ids(rs), vec![1, 2]);
        let err = db
            .execute("SELECT id FROM articles WHERE MATCH(body, 'quick') ORDER BY score() ASC LIMIT 5")
            .unwrap_err();
        assert!(err.to_string().contains("score() DESC"), "{err}");
    }

    #[test]
    fn search_rebuild_drop_and_show_indexes() {
        let mut db = search_db();
        // SHOW INDEXES lists the index with its analyzer and columns.
        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        assert!(rs.rows.iter().any(|r| r[0] == Value::String("articles_fts".into())
            && r[1] == Value::String("articles".into())
            && r[2] == Value::String("search(standard)".into())
            && r[3] == Value::String("body".into())));
        // REBUILD re-indexes from the table; results are unchanged.
        db.execute("REBUILD SEARCH INDEX articles_fts").unwrap();
        let rs = rows(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        // DROP removes it: subsequent MATCH errors and SHOW INDEXES is empty.
        db.execute("DROP SEARCH INDEX articles_fts").unwrap();
        assert!(db.execute("SELECT id FROM articles WHERE MATCH(body, 'quick')").is_err());
        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        assert!(rs.rows.is_empty());
        // Duplicate create / missing drop honor the IF (NOT) EXISTS forms.
        assert!(db.execute("DROP SEARCH INDEX articles_fts").is_err());
        db.execute("DROP SEARCH INDEX IF EXISTS articles_fts").unwrap();
        assert!(db.execute("REBUILD SEARCH INDEX articles_fts").is_err());
    }

    #[test]
    fn search_read_only_path_serves_committed_state() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // A huge refresh interval: nothing commits behind our back.
        db.execute("CREATE SEARCH INDEX t_fts ON t (body) WITH (refresh_ms = 3600000)")
            .unwrap();
        db.execute("INSERT INTO t (id, body) VALUES (1, 'alpha words')").unwrap();
        // Shared-access reads serve the last-committed state (NRT: the write
        // is applied but not yet committed).
        let rs = rows(db.execute_read("SELECT id FROM t WHERE MATCH(body, 'alpha')").unwrap());
        assert!(rs.rows.is_empty());
        // The write path commits pending index writes before searching...
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(body, 'alpha')").unwrap());
        assert_eq!(ids(rs), vec![1]);
        // ...after which the read path sees them too.
        let rs = rows(db.execute_read("SELECT id FROM t WHERE MATCH(body, 'alpha')").unwrap());
        assert_eq!(ids(rs), vec![1]);
    }

    #[test]
    fn search_restart_replays_uncommitted_writes_from_watermark() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute("CREATE SEARCH INDEX t_fts ON t (body) WITH (refresh_ms = 3600000)")
                .unwrap();
            db.execute("INSERT INTO t (id, body) VALUES (1, 'alpha words'), (2, 'beta words')")
                .unwrap();
            // Dropped with the index writes uncommitted (no search, no flush):
            // durability comes from the table WAL + watermark replay.
        }
        {
            let mut db = Database::open(&dir).unwrap();
            let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(body, 'alpha')").unwrap());
            assert_eq!(ids(rs), vec![1]);
            // An uncommitted delete must replay as a delete too.
            db.execute("DELETE FROM t WHERE id = 2").unwrap();
        }
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(body, 'beta')").unwrap());
        assert!(rs.rows.is_empty());
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(body, 'alpha')").unwrap());
        assert_eq!(ids(rs), vec![1]);
    }

    #[test]
    fn search_show_status_reports_index_stats() {
        let mut db = search_db();
        let rs = rows(db.execute("SHOW STATUS").unwrap());
        let metric = |name: &str| {
            rs.rows
                .iter()
                .find(|r| r[0] == Value::String(name.into()))
                .map(|r| r[1].clone())
        };
        assert_eq!(metric("search_indexes"), Some(Value::Int(1)));
        assert_eq!(metric("search_docs"), Some(Value::Int(3)));
        assert!(metric("search_rebuild_ms").is_some());
        assert_eq!(metric("search.articles_fts.docs"), Some(Value::Int(3)));
        assert!(metric("search.articles_fts.disk_bytes").is_some());
        assert_eq!(metric("search.articles_fts.uncommitted"), Some(Value::Int(0)));
    }

    /// `books(id, title, body, year)` with the phase-2 per-column options:
    /// a boosted title with a `.keyword` twin, a `copy_to` composite, and a
    /// typed numeric column.
    fn phase2_db() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE books (PRIMARY KEY (id))").unwrap();
        db.execute(
            "CREATE SEARCH INDEX books_fts ON books (title, body, year) WITH (\
             title.boost = 5.0, title.keyword = true, \
             title.copy_to = 'everything', body.copy_to = 'everything', \
             year.type = 'long')",
        )
        .unwrap();
        db.execute(
            "INSERT INTO books (id, title, body, year) VALUES \
             (1, 'Rust Handbook', 'nothing relevant here', 2020), \
             (2, 'unrelated words', 'rust rust rust rust', 1999)",
        )
        .unwrap();
        db
    }

    #[test]
    fn search_per_field_boost_ranks_title_hits_first() {
        let mut db = phase2_db();
        // Both rows match "rust"; the boosted single title term must outrank
        // the four body occurrences.
        let rs = rows(
            db.execute(
                "SELECT id FROM books WHERE SEARCH('rust') \
                 ORDER BY score() DESC LIMIT 2",
            )
            .unwrap(),
        );
        assert_eq!(ids(rs), vec![1, 2]);
    }

    #[test]
    fn search_keyword_twin_matches_exact_string_only() {
        let mut db = phase2_db();
        let rs = rows(
            db.execute("SELECT id FROM books WHERE MATCH(title.keyword, 'Rust Handbook')")
                .unwrap(),
        );
        assert_eq!(ids(rs), vec![1]);
        // Case differs → the raw twin does not match (the analyzed field does).
        let rs = rows(
            db.execute("SELECT id FROM books WHERE MATCH(title.keyword, 'rust handbook')")
                .unwrap(),
        );
        assert!(rs.rows.is_empty());
        let rs = rows(db.execute("SELECT id FROM books WHERE MATCH(title, 'rust handbook')").unwrap());
        assert_eq!(ids(rs), vec![1]);
    }

    #[test]
    fn search_copy_to_composite_and_typed_ranges() {
        let mut db = phase2_db();
        // The composite field sees text from both columns.
        let rs = rows(db.execute("SELECT id FROM books WHERE MATCH(everything, 'handbook')").unwrap());
        assert_eq!(ids(rs), vec![1]);
        let rs = rows(db.execute("SELECT id FROM books WHERE MATCH(everything, 'relevant')").unwrap());
        assert_eq!(ids(rs), vec![1]);
        // The typed column serves range and exact queries from SEARCH().
        let rs = rows(db.execute("SELECT id FROM books WHERE SEARCH('year:[2000 TO 2030]')").unwrap());
        assert_eq!(ids(rs), vec![1]);
        let rs = rows(db.execute("SELECT id FROM books WHERE SEARCH('year:1999')").unwrap());
        assert_eq!(ids(rs), vec![2]);
        // MATCH on the numeric column is a clear error.
        assert!(db.execute("SELECT id FROM books WHERE MATCH(year, '1999')").is_err());
    }

    #[test]
    fn search_phase2_options_survive_restart_and_rebuild() {
        let dir = tempdir();
        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
            db.execute(
                "CREATE SEARCH INDEX t_fts ON t (name) WITH (\
                 name.analyzer = 'edge_ngram(2,10)', name.search_analyzer = 'standard')",
            )
            .unwrap();
            db.execute("INSERT INTO t (id, name) VALUES (1, 'Elasticsearch'), (2, 'Postgres')")
                .unwrap();
            let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(name, 'elastic')").unwrap());
            assert_eq!(ids(rs), vec![1]);
        }
        // The declaration round-trips through the catalog: prefix search
        // still works after reopen, and after an explicit rebuild.
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(name, 'elastic')").unwrap());
        assert_eq!(ids(rs), vec![1]);
        db.execute("REBUILD SEARCH INDEX t_fts").unwrap();
        let rs = rows(db.execute("SELECT id FROM t WHERE MATCH(name, 'postg')").unwrap());
        assert_eq!(ids(rs), vec![2]);
    }

    #[test]
    fn search_create_rejects_bad_phase2_options() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        for ddl in [
            "CREATE SEARCH INDEX x ON t (body) WITH (body.wat = 1)",
            "CREATE SEARCH INDEX x ON t (body) WITH (other.boost = 2)",
            "CREATE SEARCH INDEX x ON t (body) WITH (body.boost = -1)",
            "CREATE SEARCH INDEX x ON t (body) WITH (analyzer = 'klingon')",
            "CREATE SEARCH INDEX x ON t (n) WITH (n.type = 'long', n.keyword = true)",
        ] {
            assert!(db.execute(ddl).is_err(), "expected error for {ddl}");
        }
        // Nothing half-created sticks around.
        let rs = rows(db.execute("SHOW INDEXES").unwrap());
        assert!(rs.rows.is_empty());
    }

    // ---- phase 3: bool composition, pattern predicates, highlighting ----

    #[test]
    fn search_bool_composition_or_not() {
        let mut db = search_db();
        // OR of two search predicates pushes to the index.
        let rs = rows(
            db.execute(
                "SELECT id FROM articles \
                 WHERE MATCH(body, 'fox') OR MATCH(body, 'vegetables')",
            )
            .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 3]);
        // NOT excludes matching rows (rows the index knows about).
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE NOT MATCH(body, 'quick')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![3]);
        // Composition under a residual AND: (quick OR slow) minus fox rows,
        // filtered by flag.
        let rs = rows(
            db.execute(
                "SELECT id FROM articles \
                 WHERE (MATCH(body, 'quick') OR MATCH(body, 'slow')) \
                   AND NOT MATCH(body, 'fox') AND flag = false",
            )
            .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![2]);
        // Mixing a search predicate with an ordinary condition under OR
        // cannot push to the index — clear error.
        assert!(db
            .execute("SELECT id FROM articles WHERE MATCH(body, 'quick') OR flag = true")
            .is_err());
    }

    #[test]
    fn search_prefix_wildcard_regexp_predicates() {
        let mut db = search_db();
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE MATCH_PREFIX(body, 'veg')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![3]);
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE WILDCARD(body, 'qu*k')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        let rs = rows(db.execute("SELECT id FROM articles WHERE WILDCARD(body, 'f?x')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1]);
        let rs = rows(
            db.execute("SELECT id FROM articles WHERE REGEXP(body, 'ro(as|ck)ted?')").unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![3]);
        // A broken regex is a user error, not a panic.
        assert!(db.execute("SELECT id FROM articles WHERE REGEXP(body, 'ro(')").is_err());
    }

    #[test]
    fn search_highlight_projects_snippets() {
        let mut db = search_db();
        let rs = rows(
            db.execute(
                "SELECT id, HIGHLIGHT(body, 40) AS snippet FROM articles \
                 WHERE MATCH(body, 'quick fox') \
                 ORDER BY score() DESC LIMIT 2",
            )
            .unwrap(),
        );
        assert_eq!(rs.columns, vec!["id".to_string(), "snippet".to_string()]);
        assert_eq!(rs.rows.len(), 2);
        for row in &rs.rows {
            let Value::String(snippet) = &row[1] else {
                panic!("snippet must be a string, got {:?}", row[1]);
            };
            assert!(snippet.contains("<b>quick</b>"), "{snippet}");
        }
        // Also served on the unranked predicate-only path.
        let rs = rows(
            db.execute(
                "SELECT HIGHLIGHT(body) AS s FROM articles WHERE MATCH(body, 'vegetables')",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(
            rs.rows[0][0],
            Value::String("slow roasted <b>vegetables</b>".into())
        );
        // Custom pre/post tags (ES pre_tags/post_tags) replace the <b> markers.
        let rs = rows(
            db.execute(
                "SELECT HIGHLIGHT(body, 40, '<em>', '</em>') AS s FROM articles \
                 WHERE MATCH(body, 'vegetables')",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows[0][0],
            Value::String("slow roasted <em>vegetables</em>".into())
        );
        // Outside a search query there is nothing to highlight.
        assert!(db.execute("SELECT HIGHLIGHT(body) FROM articles").is_err());
        // Bad arguments are clear errors.
        assert!(db
            .execute("SELECT HIGHLIGHT(body, 0) FROM articles WHERE MATCH(body, 'quick')")
            .is_err());
        assert!(db
            .execute("SELECT HIGHLIGHT('body') FROM articles WHERE MATCH(body, 'quick')")
            .is_err());
    }

    // ---- phase 6: aggregations over search queries ----

    /// `sales(id, product text, region keyword, units long, price double)`
    /// with a search index covering all of it.
    fn agg_db() -> Database {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE sales (PRIMARY KEY (id))").unwrap();
        db.execute(
            "CREATE SEARCH INDEX sales_fts ON sales (product, region, units, price) WITH (\
             region.type = 'keyword', units.type = 'long', price.type = 'double')",
        )
        .unwrap();
        db.execute(
            "INSERT INTO sales (id, product, region, units, price) VALUES \
             (1, 'red widget',  'east', 10, 2.5), \
             (2, 'blue widget', 'east', 20, 4.0), \
             (3, 'red gadget',  'west', 5,  1.0), \
             (4, 'red widget',  'west', 7,  3.5), \
             (5, 'blue gadget', 'east', 2,  9.9)",
        )
        .unwrap();
        // Row 6 has no region: SQL's NULL group.
        db.execute("INSERT INTO sales (id, product, units) VALUES (6, 'red thing', 100)")
            .unwrap();
        db
    }

    /// Sort grouped output rows by their first column for order-insensitive
    /// comparison (GROUP BY output order is unspecified).
    fn sorted_groups(rs: ResultSet) -> Vec<Vec<Value>> {
        let mut rows = rs.rows;
        rows.sort_by_key(|r| r[0].encode_key());
        rows
    }

    #[test]
    fn search_group_by_pushdown_matches_fallback() {
        let mut db = agg_db();
        // The pushdown shape: keyword GROUP BY + simple metrics.
        let sql = "SELECT region, COUNT(*), SUM(units), AVG(price), MAX(units) \
                   FROM sales WHERE MATCH(product, 'red') GROUP BY region";
        let pushed = sorted_groups(rows(db.execute(sql).unwrap()));
        // Rows 1, 3, 4, 6 match 'red': east {1}, west {3,4}, NULL {6}.
        assert_eq!(pushed.len(), 3);
        assert_eq!(
            pushed[0],
            vec![
                Value::Null,
                Value::Int(1),
                Value::Int(100),
                Value::Null, // row 6 has no price → SQL NULL, not 0
                Value::Int(100),
            ]
        );
        assert_eq!(
            pushed[1],
            vec![
                Value::String("east".into()),
                Value::Int(1),
                Value::Int(10),
                Value::Float(2.5),
                Value::Int(10),
            ]
        );
        assert_eq!(
            pushed[2],
            vec![
                Value::String("west".into()),
                Value::Int(2),
                Value::Int(12),
                Value::Float(2.25),
                Value::Int(7),
            ]
        );
        // The same statement with HAVING takes the row-materialization
        // fallback — identical values for the surviving groups.
        let fell = sorted_groups(rows(
            db.execute(
                "SELECT region, COUNT(*), SUM(units), AVG(price), MAX(units) \
                 FROM sales WHERE MATCH(product, 'red') GROUP BY region \
                 HAVING COUNT(*) >= 1",
            )
            .unwrap(),
        ));
        assert_eq!(pushed, fell);
    }

    #[test]
    fn search_global_aggregates_and_fallback_shapes() {
        let mut db = agg_db();
        // No GROUP BY: one global row over the match set.
        let rs = rows(
            db.execute(
                "SELECT COUNT(*), MIN(price), MAX(price) FROM sales \
                 WHERE MATCH(product, 'widget')",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(3), Value::Float(2.5), Value::Float(4.0)]]
        );
        // GROUP BY over an analyzed text column can't push down exactly —
        // the fallback still answers it (grouping by the raw doc value).
        let rs = rows(
            db.execute(
                "SELECT product, COUNT(*) FROM sales \
                 WHERE MATCH(product, 'widget') GROUP BY product",
            )
            .unwrap(),
        );
        assert_eq!(
            sorted_groups(rs),
            vec![
                vec![Value::String("blue widget".into()), Value::Int(1)],
                vec![Value::String("red widget".into()), Value::Int(2)],
            ]
        );
        // Residual predicates force the fallback too, and stay correct.
        let rs = rows(
            db.execute(
                "SELECT region, SUM(units) FROM sales \
                 WHERE MATCH(product, 'widget') AND price > 2.6 GROUP BY region",
            )
            .unwrap(),
        );
        assert_eq!(
            sorted_groups(rs),
            vec![
                vec![Value::String("east".into()), Value::Int(20)],
                vec![Value::String("west".into()), Value::Int(7)],
            ]
        );
        // Aggregate ORDER BY + LIMIT run through the grouped executor.
        let rs = rows(
            db.execute(
                "SELECT region, COUNT(*) AS n FROM sales WHERE MATCH(product, 'red') \
                 GROUP BY region ORDER BY COUNT(*) DESC LIMIT 1",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![Value::String("west".into()), Value::Int(2)]]
        );
    }

    #[test]
    fn search_multi_word_synonyms_expand_both_ways() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE places (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE SEARCH INDEX places_fts ON places (body)")
            .unwrap();
        db.execute(
            "INSERT INTO places (id, body) VALUES \
             (1, 'flights to new york tonight'), \
             (2, 'the nyc marathon'), \
             (3, 'new car in york county'), \
             (4, 'boston tea')",
        )
        .unwrap();
        db.execute(
            "ALTER SEARCH INDEX places_fts SET (synonyms = 'new york,nyc,big apple')",
        )
        .unwrap();

        // Single word → multi-word peers as PHRASES: 'nyc' matches the
        // literal doc and the "new york" phrase doc — but NOT doc 3, whose
        // 'new' and 'york' are not adjacent.
        let rs = rows(db.execute("SELECT id FROM places WHERE MATCH(body, 'nyc')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);

        // Multi-word sequence in the query → single-word peer: the query
        // 'new york' contains the group entry as consecutive tokens, so
        // the nyc doc matches too (docs 1, 2 via synonym; 3 via its own
        // 'new'/'york' terms — MATCH is an OR of terms).
        let rs = rows(
            db.execute("SELECT id FROM places WHERE MATCH(body, 'new york')")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2, 3]);

        // 'big apple' → both peers.
        let rs = rows(
            db.execute("SELECT id FROM places WHERE MATCH(body, 'big apple')")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![1, 2]);
    }

    #[test]
    fn search_alter_index_synonyms_hot_reload() {
        let dir = tempdir();
        let mut db = Database::open(&dir).unwrap();
        db.execute("CREATE TABLE cars (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE SEARCH INDEX cars_fts ON cars (body)").unwrap();
        db.execute(
            "INSERT INTO cars (id, body) VALUES \
             (1, 'a fast red car'), (2, 'a speedy blue automobile'), (3, 'a slow bike')",
        )
        .unwrap();
        // Without synonyms only the literal term matches.
        let rs = rows(db.execute("SELECT id FROM cars WHERE MATCH(body, 'quick')").unwrap());
        assert!(rs.rows.is_empty());

        // ALTER SET applies immediately — query-time expansion, no reindex.
        db.execute(
            "ALTER SEARCH INDEX cars_fts SET (synonyms = 'quick,fast,speedy; car,automobile')",
        )
        .unwrap();
        let rs = rows(db.execute("SELECT id FROM cars WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        let rs = rows(db.execute("SELECT id FROM cars WHERE MATCH(body, 'car')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);

        // Index-time options are rejected with a clear error; bad synonym
        // specs too.
        let err = db
            .execute("ALTER SEARCH INDEX cars_fts SET (analyzer = 'english')")
            .unwrap_err();
        assert!(err.to_string().contains("index-time"), "{err}");
        assert!(db
            .execute("ALTER SEARCH INDEX cars_fts SET (synonyms = 'lonely')")
            .is_err());
        assert!(db
            .execute("ALTER SEARCH INDEX nope SET (synonyms = 'a,b')")
            .is_err());

        // The altered options persist across a restart.
        drop(db);
        let mut db = Database::open(&dir).unwrap();
        let rs = rows(db.execute("SELECT id FROM cars WHERE MATCH(body, 'quick')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
        // And boost/refresh alters go through the same path.
        db.execute("ALTER SEARCH INDEX cars_fts SET (refresh_ms = 250, body.boost = 2.0)")
            .unwrap();
        let rs = rows(db.execute("SELECT id FROM cars WHERE MATCH(body, 'fast')").unwrap());
        assert_eq!(sorted_ids(rs), vec![1, 2]);
    }

    #[test]
    fn search_suggest_and_more_like_this() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE notes (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE SEARCH INDEX notes_fts ON notes (body)").unwrap();
        db.execute(
            "INSERT INTO notes (id, body) VALUES \
             (1, 'the rust database engine'), \
             (2, 'rust database internals'), \
             (3, 'a database for rust services'), \
             (4, 'cooking rustic bread at home'), \
             (5, 'bread baking basics')",
        )
        .unwrap();

        // SUGGEST: term-dictionary "did you mean", default column (the
        // index's only text column), ranked closest-then-most-common.
        let rs = rows(db.execute("SUGGEST 'databsae' ON notes_fts").unwrap());
        assert_eq!(
            rs.columns,
            vec!["input", "suggestion", "distance", "doc_freq"]
        );
        assert_eq!(rs.rows[0][1], Value::String("database".into()));
        assert_eq!(rs.rows[0][3], Value::Int(3));
        // Explicit column + LIMIT; the read-only path serves it too.
        let rs = rows(
            db.execute_read("SUGGEST 'bred' ON notes_fts COLUMN body LIMIT 1")
                .unwrap(),
        );
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][1], Value::String("bread".into()));
        // Unknown index errors.
        assert!(db.execute("SUGGEST 'x' ON nope_fts").is_err());

        // MORE_LIKE_THIS: similar docs rank, unrelated ones don't.
        let rs = rows(
            db.execute(
                "SELECT id FROM notes WHERE MORE_LIKE_THIS(body, 'rust database engine') \
                 ORDER BY score() DESC LIMIT 5",
            )
            .unwrap(),
        );
        let got = ids(rs);
        assert!(got.contains(&1) && got.contains(&2) && got.contains(&3), "{got:?}");
        assert!(!got.contains(&5), "{got:?}");
    }

    #[test]
    fn search_order_by_fast_field() {
        let mut db = agg_db();
        // Index-ordered top-k over a numeric fast field, both directions.
        let rs = rows(
            db.execute(
                "SELECT id, price FROM sales WHERE MATCH(product, 'widget') \
                 ORDER BY price DESC LIMIT 2",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(2), Value::Float(4.0)],
                vec![Value::Int(4), Value::Float(3.5)],
            ]
        );
        let rs = rows(
            db.execute(
                "SELECT id FROM sales WHERE MATCH(product, 'widget') \
                 ORDER BY price ASC LIMIT 1",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
        // Keyword ordering + residual filter (over-fetch discipline). Rows
        // 3 and 4 tie on region — either may fill the second slot.
        let rs = rows(
            db.execute(
                "SELECT id, region FROM sales WHERE MATCH(product, 'red') \
                 AND units < 50 ORDER BY region ASC LIMIT 2",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows[0], vec![Value::Int(1), Value::String("east".into())]);
        assert_eq!(rs.rows[1][1], Value::String("west".into()));
        // OFFSET pages through the ordered result.
        let rs = rows(
            db.execute(
                "SELECT id FROM sales WHERE MATCH(product, 'widget') \
                 ORDER BY price DESC LIMIT 1 OFFSET 1",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(4)]]);
        // Rows missing the sort column exist for 'red' (row 6 has no
        // price): the pushdown declines and the fallback orders with SQL
        // NULL placement — NULLS FIRST on DESC.
        let rs = rows(
            db.execute(
                "SELECT id FROM sales WHERE MATCH(product, 'red') \
                 ORDER BY price DESC LIMIT 2",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(6)], vec![Value::Int(4)]]);
        // Multi-key orderings run through the generic fallback; score()
        // ordering stays DESC-only.
        let rs = rows(
            db.execute(
                "SELECT id FROM sales WHERE MATCH(product, 'widget') \
                 ORDER BY region ASC, price DESC LIMIT 3",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(2)], vec![Value::Int(1)], vec![Value::Int(4)]]
        );
        assert!(db
            .execute(
                "SELECT id FROM sales WHERE MATCH(product, 'widget') \
                 ORDER BY score() ASC LIMIT 2"
            )
            .is_err());
    }

    /// APPROX_COUNT_DISTINCT: the opt-in sketch. On the pushdown path it
    /// answers via HLL (exact at these cardinalities), grouped it takes
    /// the exact row fallback, and it also works with no search predicate
    /// at all — everywhere agreeing with COUNT(DISTINCT) on small sets.
    #[test]
    fn approx_count_distinct_all_paths() {
        let mut db = agg_db();
        // Pushdown (global metric over the match set).
        let rs = rows(
            db.execute(
                "SELECT APPROX_COUNT_DISTINCT(region) FROM sales WHERE MATCH(product, 'red')",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
        // Grouped → exact row fallback (identical to COUNT(DISTINCT)).
        let approx = sorted_groups(rows(
            db.execute(
                "SELECT region, APPROX_COUNT_DISTINCT(product) FROM sales \
                 WHERE MATCH(product, 'widget') GROUP BY region",
            )
            .unwrap(),
        ));
        let exact = sorted_groups(rows(
            db.execute(
                "SELECT region, COUNT(DISTINCT product) FROM sales \
                 WHERE MATCH(product, 'widget') GROUP BY region",
            )
            .unwrap(),
        ));
        assert_eq!(approx, exact);
        // Plain table scan (no search predicate).
        let rs = rows(
            db.execute("SELECT APPROX_COUNT_DISTINCT(region) FROM sales").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]); // east, west (NULL ignored)
        // Only COUNT-shaped: rejected under SUM etc. is the DISTINCT rule.
        assert!(db
            .execute("SELECT APPROX_COUNT_DISTINCT(region, units) FROM sales")
            .is_err());
    }

    #[test]
    fn search_count_distinct_exact_on_both_paths() {
        let mut db = agg_db();
        // Global COUNT(DISTINCT) over the match set — pushdown path.
        let rs = rows(
            db.execute(
                "SELECT COUNT(DISTINCT region) FROM sales WHERE MATCH(product, 'red')",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]); // east, west (NULL ignored)
        // Grouped, and again with a residual predicate forcing the row
        // fallback — identical results.
        let pushed = sorted_groups(rows(
            db.execute(
                "SELECT region, COUNT(DISTINCT product) FROM sales \
                 WHERE MATCH(product, 'widget') GROUP BY region",
            )
            .unwrap(),
        ));
        let fell = sorted_groups(rows(
            db.execute(
                "SELECT region, COUNT(DISTINCT product) FROM sales \
                 WHERE MATCH(product, 'widget') AND id >= 0 GROUP BY region",
            )
            .unwrap(),
        ));
        assert_eq!(pushed, fell);
        assert_eq!(
            pushed,
            vec![
                vec![Value::String("east".into()), Value::Int(2)],
                vec![Value::String("west".into()), Value::Int(1)],
            ]
        );
    }

    #[test]
    fn search_time_bucket_histogram_pushdown() {
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE events (PRIMARY KEY (id))").unwrap();
        db.execute(
            "CREATE SEARCH INDEX events_fts ON events (msg, ts, v) WITH (\
             ts.type = 'date', v.type = 'long')",
        )
        .unwrap();
        const HOUR: i64 = 3_600_000;
        let base: i64 = 1_700_000_000_000;
        let floor = base - base.rem_euclid(HOUR);
        for (i, (t, v)) in [(0, 1), (HOUR / 2, 2), (HOUR, 4), (3 * HOUR, 8)]
            .iter()
            .enumerate()
        {
            db.execute(&format!(
                "INSERT INTO events (id, msg, ts, v) VALUES ({i}, 'alert fired', {}, {v})",
                base + t
            ))
            .unwrap();
        }
        // COUNT-only histogram: the safe pushdown envelope. Keys are typed
        // as timestamps (a `date` column's semantics). Documented typing
        // nuance: integer-stored ts values come back Int from the
        // row-fallback path (`time_bucket` preserves its input type per
        // row) but Timestamp from the pushdown — the same instants.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(1h, ts), COUNT(*) FROM events \
                 WHERE MATCH(msg, 'alert') GROUP BY time_bucket(1h, ts)",
            )
            .unwrap(),
        );
        assert_eq!(
            sorted_groups(rs),
            vec![
                vec![Value::Timestamp(floor), Value::Int(2)],
                vec![Value::Timestamp(floor + HOUR), Value::Int(1)],
                vec![Value::Timestamp(floor + 3 * HOUR), Value::Int(1)],
            ]
        );
        // Grouped per-bucket metrics take the row fallback (the tantivy
        // 0.26.1 sub-aggregation data-loss bug makes that pushdown unsafe
        // — see the guard in skaidb-fts): Int-keyed here, values exact.
        let rs = rows(
            db.execute(
                "SELECT time_bucket(1h, ts), COUNT(*), SUM(v) FROM events \
                 WHERE MATCH(msg, 'alert') GROUP BY time_bucket(1h, ts)",
            )
            .unwrap(),
        );
        assert_eq!(
            sorted_groups(rs),
            vec![
                vec![Value::Int(floor), Value::Int(2), Value::Int(3)],
                vec![Value::Int(floor + HOUR), Value::Int(1), Value::Int(4)],
                vec![Value::Int(floor + 3 * HOUR), Value::Int(1), Value::Int(8)],
            ]
        );
        // A row missing ts makes the histogram inexact (it would lose the
        // NULL group) — the pushdown detects that and the fallback serves
        // it, NULL group included.
        db.execute("INSERT INTO events (id, msg, v) VALUES (99, 'alert without time', 16)")
            .unwrap();
        let rs = rows(
            db.execute(
                "SELECT time_bucket(1h, ts), COUNT(*) FROM events \
                 WHERE MATCH(msg, 'alert') GROUP BY time_bucket(1h, ts)",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 4, "{:?}", rs.rows);
        assert!(rs
            .rows
            .iter()
            .any(|r| r[0] == Value::Null && r[1] == Value::Int(1)));
    }

    #[test]
    fn search_refresh_tick_commits_idle_writes() {
        // Write-path refresh checks only run on the next write: with no
        // follow-up traffic, the read-only path would never see the last
        // writes. The server's background tick closes that gap (found by
        // the fleet FTS bench — the NRT probe hung forever without it).
        let mut db = Database::open(tempdir()).unwrap();
        db.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        db.execute("CREATE SEARCH INDEX t_fts ON t (body) WITH (refresh_ms = 50)")
            .unwrap();
        db.execute("INSERT INTO t (id, body) VALUES (1, 'idle words')").unwrap();
        assert!(db.has_search_indexes());
        std::thread::sleep(std::time::Duration::from_millis(60));
        db.search_refresh_tick().unwrap();
        let rs = rows(
            db.execute_read("SELECT id FROM t WHERE MATCH(body, 'idle')").unwrap(),
        );
        assert_eq!(ids(rs), vec![1]);
    }

    #[test]
    fn search_legacy_catalog_def_still_loads() {
        // A phase-1 catalog stored `analyzer`/`refresh_ms` as dedicated
        // fields; it must deserialize into the options-based def.
        let legacy = r#"{
            "table": "articles",
            "paths": ["body"],
            "analyzer": "english",
            "refresh_ms": 250
        }"#;
        let def: crate::catalog::SearchIndexDef = serde_json::from_str(legacy).unwrap();
        assert_eq!(def.analyzer(), "english");
        assert_eq!(
            def.options,
            vec![
                ("analyzer".to_string(), "english".to_string()),
                ("refresh_ms".to_string(), "250".to_string()),
            ]
        );
        assert_eq!(
            def.with_clause(),
            " WITH (analyzer = 'english', refresh_ms = '250')"
        );
    }
}
