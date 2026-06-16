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
