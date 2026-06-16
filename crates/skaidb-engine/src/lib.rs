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
pub mod vector;

pub use error::EngineError;
pub use exec::{filter_rows, run, Cluster, Database, DbStats, IndexScanRange, TableStats};
pub use namespace::DEFAULT_DATABASE;
pub use result::{QueryOutput, ResultSet, SessionEffect};
pub use session::Session;

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
