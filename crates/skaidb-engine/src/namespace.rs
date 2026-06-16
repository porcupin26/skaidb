//! Database namespacing.
//!
//! A *database* is a namespace prefix on table and index names within one
//! storage engine. The implicit `default` database uses **unprefixed** names so
//! that an existing single-database data directory keeps working unchanged; any
//! other database `D` prefixes its object names as `"D\x1f<name>"` internally
//! (the `\x1f` unit separator can never appear in a SQL identifier, so the
//! split is unambiguous).
//!
//! Resolution — turning the names a user typed into these internal names — runs
//! once at the execute boundary ([`crate::Database::execute_session`]). After
//! it, the executor, the `Cluster` trait, and the DML internode messages all
//! operate on plain resolved strings and need no knowledge of databases, so
//! cluster replication works for databases for free.

use skaidb_sql::Statement;

use crate::error::EngineError;

/// The implicit database backed by unprefixed names.
pub const DEFAULT_DATABASE: &str = "default";

/// Separator between a database name and an object name in an internal name.
const SEP: char = '\u{1f}';

/// The internal name for object `name` in database `db` (`default` ⇒ unprefixed).
pub fn qualify(db: &str, name: &str) -> String {
    if db == DEFAULT_DATABASE {
        name.to_string()
    } else {
        format!("{db}{SEP}{name}")
    }
}

/// Split an internal name back into `(database, object)`.
pub fn split(internal: &str) -> (&str, &str) {
    match internal.split_once(SEP) {
        Some((db, name)) => (db, name),
        None => (DEFAULT_DATABASE, internal),
    }
}

/// Internal names belonging to database `db` (the prefix to scan/strip for it).
pub fn prefix(db: &str) -> String {
    if db == DEFAULT_DATABASE {
        String::new()
    } else {
        format!("{db}{SEP}")
    }
}

/// True if internal `name` belongs to database `db`.
pub fn belongs_to(name: &str, db: &str) -> bool {
    split(name).0 == db
}

/// Resolve a user table reference (`"table"` or `"db.table"`) against the
/// session's `current_db` into an internal name.
pub fn resolve_table_ref(reference: &str, current_db: &str) -> String {
    match reference.split_once('.') {
        Some((db, name)) => qualify(db, name),
        None => qualify(current_db, reference),
    }
}

/// Rewrite every table reference and local object (index) name in `stmt` to its
/// internal, database-resolved form, in place.
pub fn resolve_statement(stmt: &mut Statement, current_db: &str) {
    stmt.for_each_table_mut(|t| *t = resolve_table_ref(t, current_db));
    stmt.for_each_local_name_mut(|n| *n = qualify(current_db, n));
}

/// The user-facing form of an internal name, relative to `current_db`: bare if
/// it belongs to the current database, else `db.name`.
pub fn display_name(internal: &str, current_db: &str) -> String {
    let (db, name) = split(internal);
    if db == current_db {
        name.to_string()
    } else {
        format!("{db}.{name}")
    }
}

/// Rewrite the internal (separator-bearing) object name carried by a
/// table/index error into its user-facing form, so callers never see the
/// internal `\x1f` namespace separator.
pub fn humanize_error(e: EngineError, current_db: &str) -> EngineError {
    match e {
        EngineError::TableNotFound(n) => EngineError::TableNotFound(display_name(&n, current_db)),
        EngineError::TableExists(n) => EngineError::TableExists(display_name(&n, current_db)),
        EngineError::IndexNotFound(n) => EngineError::IndexNotFound(display_name(&n, current_db)),
        EngineError::IndexExists(n) => EngineError::IndexExists(display_name(&n, current_db)),
        other => other,
    }
}

/// Reject database names that are empty, over-long, or contain anything but
/// ASCII letters, digits, `_`, or `-` (so a name can never carry the separator
/// or escape into another namespace).
pub fn valid_database_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != DEFAULT_DATABASE
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_unprefixed_roundtrip() {
        assert_eq!(qualify(DEFAULT_DATABASE, "orders"), "orders");
        assert_eq!(split("orders"), (DEFAULT_DATABASE, "orders"));
    }

    #[test]
    fn named_database_prefixes_and_splits() {
        let q = qualify("shop", "orders");
        assert_eq!(q, "shop\u{1f}orders");
        assert_eq!(split(&q), ("shop", "orders"));
        assert!(belongs_to(&q, "shop"));
        assert!(!belongs_to(&q, "default"));
    }

    #[test]
    fn resolve_reference_uses_explicit_db_then_current() {
        assert_eq!(resolve_table_ref("orders", "shop"), "shop\u{1f}orders");
        assert_eq!(resolve_table_ref("warehouse.orders", "shop"), "warehouse\u{1f}orders");
        assert_eq!(resolve_table_ref("default.orders", "shop"), "orders");
        assert_eq!(resolve_table_ref("orders", "default"), "orders");
    }

    #[test]
    fn name_validation() {
        assert!(valid_database_name("shop_1-x"));
        assert!(!valid_database_name(""));
        assert!(!valid_database_name("default")); // reserved/implicit
        assert!(!valid_database_name("a.b"));
        assert!(!valid_database_name("a/b"));
    }
}
