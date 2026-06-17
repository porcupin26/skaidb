//! `skaidbsh export` — dump schema + data to JSON (NDJSON) or CSV.
//!
//! Writes a directory: `schema.sql` (CREATE DATABASE/TABLE/INDEX to recreate the
//! structure) plus one data file per table — `<db>.<table>.jsonl` (one JSON
//! object per row) or `<db>.<table>.csv`. Scope is `--all`, one or more
//! `--database`, and/or one or more `--table` (`db.table` or a bare name in the
//! current database).

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use clap::Args;
use skaidb_types::Value;

use crate::Backend;

#[derive(Debug, Args)]
pub struct ExportArgs {
    /// Output directory (created if needed): one data file per table + schema.sql.
    #[arg(short = 'o', long = "out", default_value = "skaidb-dump")]
    pub out: String,
    /// Output format.
    #[arg(long, value_parser = ["json", "csv"], default_value = "json")]
    pub format: String,
    /// Export every database and table.
    #[arg(long)]
    pub all: bool,
    /// Export every table in these databases (repeat or comma-separate).
    #[arg(long = "database", value_delimiter = ',')]
    pub databases: Vec<String>,
    /// Export these tables: `db.table`, or a bare name in the current database
    /// (repeat or comma-separate).
    #[arg(long = "table", value_delimiter = ',')]
    pub tables: Vec<String>,
    /// Don't write schema.sql (data only).
    #[arg(long)]
    pub no_schema: bool,
}

/// One table to dump.
struct Target {
    db: String,
    table: String,
    pk: Vec<String>,
}

pub fn run(backend: &mut Backend, args: &ExportArgs) -> ExitCode {
    // Restore the session's database afterwards (matters for an interactive run).
    let original_db = backend.current_db().to_string();
    let result = export(backend, args);
    let _ = backend.exec_quiet(&format!("USE {original_db}"));
    match result {
        Ok(n) => {
            eprintln!("exported {n} table(s) to {}/", args.out);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("export failed: {e}");
            ExitCode::FAILURE
        }
    }
}

fn export(backend: &mut Backend, args: &ExportArgs) -> Result<usize, String> {
    if !args.all && args.databases.is_empty() && args.tables.is_empty() {
        return Err("nothing selected — pass --all, --database <db>, or --table <name>".into());
    }
    let targets = resolve_targets(backend, args)?;
    if targets.is_empty() {
        return Err("no matching tables".into());
    }
    let out = Path::new(&args.out);
    fs::create_dir_all(out).map_err(|e| format!("create {}: {e}", args.out))?;

    if !args.no_schema {
        let schema = build_schema(backend, &targets)?;
        fs::write(out.join("schema.sql"), schema).map_err(|e| format!("write schema.sql: {e}"))?;
    }

    let csv = args.format == "csv";
    for t in &targets {
        let (columns, rows) = backend.query(&format!("SELECT * FROM {}.{}", t.db, t.table))?;
        let ext = if csv { "csv" } else { "jsonl" };
        let fname = format!("{}.{}.{ext}", sanitize(&t.db), sanitize(&t.table));
        let body = if csv {
            to_csv(&columns, &rows)
        } else {
            to_ndjson(&columns, &rows)
        };
        fs::write(out.join(&fname), body).map_err(|e| format!("write {fname}: {e}"))?;
    }
    Ok(targets.len())
}

/// Expand `--all` / `--database` / `--table` into a deduped, sorted table list.
fn resolve_targets(backend: &mut Backend, args: &ExportArgs) -> Result<Vec<Target>, String> {
    let mut out: Vec<Target> = Vec::new();
    if args.all {
        for db in list_databases(backend)? {
            for (table, pk) in list_tables(backend, &db)? {
                out.push(Target { db: db.clone(), table, pk });
            }
        }
    }
    for db in &args.databases {
        for (table, pk) in list_tables(backend, db)? {
            out.push(Target { db: db.clone(), table, pk });
        }
    }
    let current = backend.current_db().to_string();
    for spec in &args.tables {
        let (db, table) = match spec.split_once('.') {
            Some((d, t)) => (d.to_string(), t.to_string()),
            None => (current.clone(), spec.clone()),
        };
        let pk = list_tables(backend, &db)?
            .into_iter()
            .find(|(t, _)| t == &table)
            .map(|(_, pk)| pk)
            .ok_or_else(|| format!("table {db}.{table} not found"))?;
        out.push(Target { db, table, pk });
    }
    out.sort_by(|a, b| (a.db.as_str(), a.table.as_str()).cmp(&(b.db.as_str(), b.table.as_str())));
    out.dedup_by(|a, b| a.db == b.db && a.table == b.table);
    Ok(out)
}

fn list_databases(backend: &mut Backend) -> Result<Vec<String>, String> {
    let (_, rows) = backend.query("SHOW DATABASES")?;
    Ok(rows.into_iter().filter_map(|r| r.into_iter().next().map(cell_text)).collect())
}

/// Tables (with primary-key columns) in `db`. Switches the session to `db`.
fn list_tables(backend: &mut Backend, db: &str) -> Result<Vec<(String, Vec<String>)>, String> {
    backend.exec_quiet(&format!("USE {db}"))?;
    let (_, rows) = backend.query("SHOW TABLES")?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let mut it = r.into_iter();
            let table = cell_text(it.next()?);
            let pk = it.next().map(cell_text).unwrap_or_default();
            // SHOW TABLES joins composite keys; accept comma or space separators.
            let pk: Vec<String> = pk
                .split([',', ' '])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            Some((table, pk))
        })
        .collect())
}

/// `CREATE DATABASE` / `CREATE TABLE` (+ secondary `CREATE INDEX`) to recreate
/// the dumped structure. Vector indexes are noted but not emitted — their
/// dimension/metric aren't exposed by `SHOW INDEXES`.
fn build_schema(backend: &mut Backend, targets: &[Target]) -> Result<String, String> {
    let mut s = String::from("-- skaidb dump schema\n");
    let mut dbs: Vec<&str> = targets.iter().map(|t| t.db.as_str()).collect();
    dbs.sort_unstable();
    dbs.dedup();
    for db in &dbs {
        s.push_str(&format!("CREATE DATABASE IF NOT EXISTS {db};\n"));
    }
    s.push('\n');
    for t in targets {
        s.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (PRIMARY KEY ({}));\n",
            t.db,
            t.table,
            t.pk.join(", ")
        ));
    }
    // Secondary indexes, best-effort from SHOW INDEXES (per database).
    for db in &dbs {
        backend.exec_quiet(&format!("USE {db}"))?;
        let (_, rows) = backend.query("SHOW INDEXES")?;
        for r in rows {
            let c: Vec<String> = r.into_iter().map(cell_text).collect();
            // columns: index, table, kind, columns
            let (Some(name), Some(table), Some(kind), Some(cols)) =
                (c.first(), c.get(1), c.get(2), c.get(3))
            else {
                continue;
            };
            if !targets.iter().any(|t| &t.db == db && &t.table == table) {
                continue;
            }
            if kind.eq_ignore_ascii_case("vector") {
                s.push_str(&format!("-- vector index {name} on {db}.{table}({cols}) — recreate manually (DIM/metric not introspectable)\n"));
            } else {
                s.push_str(&format!("CREATE INDEX IF NOT EXISTS {name} ON {db}.{table}({cols});\n"));
            }
        }
    }
    Ok(s)
}

fn to_ndjson(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut s = String::new();
    for row in rows {
        let mut obj = serde_json::Map::new();
        for (col, val) in columns.iter().zip(row) {
            if !matches!(val, Value::Null) {
                obj.insert(col.clone(), val.to_json());
            }
        }
        s.push_str(&serde_json::Value::Object(obj).to_string());
        s.push('\n');
    }
    s
}

fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut s = String::new();
    s.push_str(&columns.iter().map(|c| csv_quote(c)).collect::<Vec<_>>().join(","));
    s.push('\n');
    for row in rows {
        let line: Vec<String> = row.iter().map(|v| csv_quote(&cell_str(v))).collect();
        s.push_str(&line.join(","));
        s.push('\n');
    }
    s
}

/// A value as a CSV cell: scalars plain, null empty, arrays/documents as JSON.
fn cell_str(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Array(_) | Value::Document(_) => v.to_json().to_string(),
        other => other.to_string(),
    }
}

/// A value used as a textual identifier (db/table name from a result cell).
fn cell_text(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// RFC-4180 quoting: wrap in double quotes (doubling internal quotes) only when
/// the cell contains a comma, quote, or newline.
fn csv_quote(s: &str) -> String {
    if s.contains(['"', ',', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c == '/' || c == '\\' { '_' } else { c })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_types::Document;

    #[test]
    fn csv_quotes_only_when_needed() {
        assert_eq!(csv_quote("plain"), "plain");
        assert_eq!(csv_quote("a,b"), "\"a,b\"");
        assert_eq!(csv_quote("she said \"hi\""), "\"she said \"\"hi\"\"\"");
        assert_eq!(csv_quote("line\nbreak"), "\"line\nbreak\"");
    }

    #[test]
    fn cell_str_renders_scalars_and_nested() {
        assert_eq!(cell_str(&Value::Null), "");
        assert_eq!(cell_str(&Value::Int(42)), "42");
        assert_eq!(cell_str(&Value::String("hi".into())), "hi");
        assert_eq!(
            cell_str(&Value::Array(vec![Value::Int(1), Value::Int(2)])),
            "[1,2]"
        );
    }

    #[test]
    fn ndjson_skips_nulls_and_is_one_object_per_line() {
        let cols = vec!["id".to_string(), "name".to_string(), "note".to_string()];
        let rows = vec![
            vec![Value::Int(1), Value::String("ada".into()), Value::Null],
            vec![Value::Int(2), Value::String("bob".into()), Value::String("x".into())],
        ];
        let out = to_ndjson(&cols, &rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"id":1,"name":"ada"}"#); // null `note` omitted
        assert_eq!(lines[1], r#"{"id":2,"name":"bob","note":"x"}"#);
    }

    #[test]
    fn csv_has_header_and_rows() {
        let cols = vec!["id".to_string(), "tags".to_string()];
        let mut doc = Document::new();
        doc.insert("k", Value::Int(1));
        let rows = vec![vec![
            Value::Int(7),
            Value::Array(vec![Value::String("a,b".into())]),
        ]];
        let out = to_csv(&cols, &rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "id,tags");
        // the array cell is JSON, and contains a comma → quoted
        assert_eq!(lines[1], "7,\"[\"\"a,b\"\"]\"");
    }
}
