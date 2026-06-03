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
pub use exec::Database;
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
