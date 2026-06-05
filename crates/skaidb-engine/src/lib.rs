//! skaidb query engine: binds parsed SQL to the storage engine (SPEC §3).
//!
//! [`Database`] is an embeddable engine — open a directory, then [`Database::execute`]
//! SQL statements and get back a [`QueryOutput`].

pub mod catalog;
mod error;
mod eval;
mod exec;
mod result;

pub use error::EngineError;
pub use exec::{filter_rows, run, Cluster, Database, IndexScanRange};
pub use result::{QueryOutput, ResultSet};

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
}
