//! skaidb SQL client.
//!
//! Phase 1 runs an **embedded** engine against a local data directory: either a
//! one-shot statement via `-e`, or an interactive REPL reading from stdin.
//! Remote connection over the driver lands in a later phase.

use std::io::{self, BufRead, IsTerminal};

use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use skaidb_engine::{QueryOutput, ResultSet, Session};

#[derive(Debug, Parser)]
#[command(name = "skaidb-cli", version, about = "skaidb SQL client (embedded)")]
struct Cli {
    /// Data directory for the embedded engine.
    #[arg(long, default_value = "./skaidb-data")]
    dir: String,

    /// Execute one or more `;`-separated statements, print results, and exit.
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,
}

/// Prompt shown while a statement is still being typed (no terminating `;`).
const CONT_PROMPT: &str = "   ...> ";

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let mut db = match Session::open(&cli.dir) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("skaidb-cli: cannot open {}: {e}", cli.dir);
            return std::process::ExitCode::FAILURE;
        }
    };

    if let Some(script) = cli.execute {
        return match run_script(&mut db, &script) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(()) => std::process::ExitCode::FAILURE,
        };
    }

    if io::stdin().is_terminal() {
        repl_interactive(&mut db);
    } else {
        repl_piped(&mut db);
    }
    std::process::ExitCode::SUCCESS
}

/// Execute a `;`-separated script, stopping at the first error.
fn run_script(db: &mut Session, script: &str) -> Result<(), ()> {
    for stmt in split_statements(script) {
        if let Err(e) = run_one(db, &stmt) {
            eprintln!("error: {e}");
            if let Some(hint) = suggest(&stmt, &e) {
                eprintln!("hint: {hint}");
            }
            return Err(());
        }
    }
    Ok(())
}

/// Interactive REPL with line editing, history, and arrow-key recall.
fn repl_interactive(db: &mut Session) {
    eprintln!("skaidb interactive shell. Type 'help' for commands, Ctrl-D to exit.");

    let mut rl = match DefaultEditor::new() {
        Ok(rl) => rl,
        Err(e) => {
            // Fall back to the dumb reader if the terminal can't be put in raw mode.
            eprintln!("skaidb-cli: line editor unavailable ({e}); using basic input");
            repl_piped(db);
            return;
        }
    };
    let history = history_path();
    if let Some(path) = &history {
        let _ = rl.load_history(path);
    }

    let mut buffer = String::new();
    loop {
        // The fresh prompt names the current database so it's always visible
        // which one statements run against; the continuation prompt is fixed.
        let prompt = if buffer.is_empty() {
            format!("skaidb:{}> ", db.current_database())
        } else {
            CONT_PROMPT.to_string()
        };
        match rl.readline(&prompt) {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());

                // Meta-commands work on their own line, no `;` required, but only
                // when we are not in the middle of typing a SQL statement.
                if buffer.is_empty() {
                    match meta_command(line.trim()) {
                        Meta::Help => {
                            print_help();
                            continue;
                        }
                        Meta::Quit => break,
                        Meta::None => {}
                    }
                }

                buffer.push_str(&line);
                buffer.push('\n');
                drain_statements(db, &mut buffer);
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C abandons the half-typed statement, like a shell.
                buffer.clear();
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
    }

    if let Some(path) = &history {
        let _ = rl.save_history(path);
    }
}

/// REPL for piped / non-tty input: no prompts, no line editing.
fn repl_piped(db: &mut Session) {
    let stdin = io::stdin();
    let mut buffer = String::new();
    loop {
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
        buffer.push_str(&line);
        drain_statements(db, &mut buffer);
    }
}

/// Execute every complete (`;`-terminated) statement sitting in `buffer`,
/// removing it as we go and leaving any trailing partial statement behind.
fn drain_statements(db: &mut Session, buffer: &mut String) {
    while let Some(idx) = find_statement_end(buffer) {
        let stmt: String = buffer.drain(..=idx).collect();
        let stmt = stmt.trim();
        if !stmt.is_empty() && stmt != ";" {
            if let Err(e) = run_one(db, stmt) {
                eprintln!("error: {e}");
                if let Some(hint) = suggest(stmt, &e) {
                    eprintln!("hint: {hint}");
                }
            }
        }
    }
    // A buffer that holds only whitespace can never start a statement; clear it
    // so the next prompt is the fresh `skaidb> ` rather than a continuation.
    if buffer.trim().is_empty() {
        buffer.clear();
    }
}

fn run_one(db: &mut Session, sql: &str) -> Result<(), skaidb_engine::EngineError> {
    match db.execute(sql)? {
        QueryOutput::Rows(rs) => print_table(&rs),
        QueryOutput::Mutation { affected } => println!("OK, {affected} row(s) affected"),
        QueryOutput::Ddl => println!("OK"),
    }
    Ok(())
}

/// Meta (non-SQL) commands recognised at the start of a statement.
enum Meta {
    Help,
    Quit,
    None,
}

fn meta_command(line: &str) -> Meta {
    match line.trim_end_matches(';').trim().to_ascii_lowercase().as_str() {
        "help" | "?" | "\\h" | "\\?" => Meta::Help,
        "quit" | "exit" | "\\q" => Meta::Quit,
        _ => Meta::None,
    }
}

fn print_help() {
    println!(
        "\
skaidb interactive shell — commands

  Meta:
    help, ?            show this help
    quit, exit         leave the shell (or press Ctrl-D)
    Ctrl-C             discard the line you are typing

  Statements (end each with ';'):
    SELECT <cols> FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n];
    INSERT INTO <table> (<cols>) VALUES (...);
    UPDATE <table> SET ... [WHERE ...];
    DELETE FROM <table> [WHERE ...];
    CREATE TABLE <name> (PRIMARY KEY (<col> [, ...]));
    CREATE INDEX <name> ON <table> (<path> [, ...]);
    DROP   TABLE|INDEX <name>;
    SHOW   TABLES;
    SHOW   INDEXES;
    SHOW   STATUS;          storage/runtime stats for the current database
    BEGIN; COMMIT; ROLLBACK;

  Databases (each is an isolated set of tables; the prompt shows the current one):
    CREATE DATABASE <name>;
    DROP   DATABASE <name>;
    USE    <name>;          switch the current database (default: 'default')
    SHOW   DATABASES;
    Reach another database without switching by qualifying: db.table
    (e.g. SELECT * FROM shop.orders;). In a cluster, databases replicate.

  skaidb is schema-less: CREATE TABLE only declares the primary key — there are
  no column definitions.

  Full grammar: docs/QUERY_SYNTAX.md"
    );
}

/// Suggest a fix for a failed statement, when we can recognise the mistake.
///
/// Returns `None` rather than guessing wildly — a misleading hint is worse than
/// none. Recognised cases mirror the parser's own "expected …" messages.
fn suggest(sql: &str, err: &skaidb_engine::EngineError) -> Option<String> {
    let msg = err.to_string();
    let trimmed = sql.trim_start();
    let mut words = trimmed.split_whitespace();
    let first = words.next().unwrap_or("").trim_end_matches(';');
    let second = words.next().unwrap_or("").trim_end_matches(';');
    let first_up = first.to_ascii_uppercase();
    let second_low = second.to_ascii_lowercase();

    // The whole statement could not even be tokenised.
    if msg.contains("lex error") {
        if trimmed.contains('\\') {
            return Some("stray '\\' character — skaidb does not use backslash line continuations; just keep typing until ';'.".into());
        }
        return Some("the statement contains a character skaidb cannot read; type 'help' to see valid syntax.".into());
    }

    // First word is not a known statement keyword.
    if msg.contains("expected a statement") {
        if let Some(close) = closest_statement(&first_up) {
            return Some(format!("did you mean {close}? Type 'help' for the full list of commands."));
        }
        return Some(format!(
            "'{first}' is not a statement. Try SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, SHOW — or 'help'."
        ));
    }

    // SHOW with the wrong target.
    if msg.contains("after SHOW") {
        return Some("try: SHOW TABLES;  SHOW INDEXES;  SHOW STATUS;  or  SHOW DATABASES;".into());
    }

    // CREATE / DROP with the wrong object kind.
    if msg.contains("expected TABLE, INDEX") {
        if matches!(second_low.as_str(), "db" | "schema") {
            return Some(format!("did you mean {first_up} DATABASE <name>?"));
        }
        return Some(format!(
            "{first_up} what? Try {first_up} TABLE <name> (...);  {first_up} INDEX ...;  or  {first_up} DATABASE <name>;"
        ));
    }

    // A USE / DROP DATABASE naming a database that isn't there.
    if msg.contains("does not exist") && msg.contains("database") {
        return Some("no such database — run SHOW DATABASES; to list them, or CREATE DATABASE <name>; first.".into());
    }

    // CREATE TABLE without its primary-key clause: `expected LParen`.
    if msg.contains("expected LParen") && first_up == "CREATE" {
        // The table name is the token after the TABLE keyword.
        let name = words.next().unwrap_or("").trim_end_matches(';');
        let name = if name.is_empty() { "<name>" } else { name };
        return Some(format!(
            "CREATE TABLE needs a primary key, e.g.  CREATE TABLE {name} (PRIMARY KEY (id));"
        ));
    }

    None
}

/// SQL statement keywords, used for "did you mean …?" suggestions.
const STATEMENT_KEYWORDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "SHOW", "USE", "BEGIN",
    "COMMIT", "ROLLBACK",
];

/// Closest statement keyword to `word` within a small edit distance, if any.
fn closest_statement(word: &str) -> Option<&'static str> {
    if word.is_empty() {
        return None;
    }
    let mut best: Option<(&'static str, usize)> = None;
    for &kw in STATEMENT_KEYWORDS {
        let d = edit_distance(word, kw);
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((kw, d));
        }
    }
    // Only suggest when the typo is plausibly the same word.
    best.filter(|&(kw, d)| d <= 2.max(kw.len() / 3)).map(|(kw, _)| kw)
}

/// Levenshtein edit distance between two ASCII-ish words.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// Location of the persistent history file, under the user's home directory.
fn history_path() -> Option<std::path::PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(std::path::Path::new(&home).join(".skaidb_history"))
}

/// Split a script into statements on top-level semicolons (ignoring `;` inside
/// single-quoted string literals).
fn split_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_string = !in_string;
                // Doubled '' is an escaped quote; consume the second one.
                if !in_string && chars.peek() == Some(&'\'') {
                    cur.push(c);
                    cur.push(chars.next().unwrap());
                    in_string = true;
                    continue;
                }
                cur.push(c);
            }
            ';' if !in_string => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// Index of the first top-level `;` in `buffer`, if any.
fn find_statement_end(buffer: &str) -> Option<usize> {
    let mut in_string = false;
    for (i, c) in buffer.char_indices() {
        match c {
            '\'' => in_string = !in_string,
            ';' if !in_string => return Some(i),
            _ => {}
        }
    }
    None
}

/// Render a result set as a simple aligned text table.
fn print_table(rs: &ResultSet) {
    let mut widths: Vec<usize> = rs.columns.iter().map(|c| c.len()).collect();
    let rendered: Vec<Vec<String>> = rs
        .rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_string()).collect())
        .collect();
    for row in &rendered {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", join_padded(&rs.columns, &widths));
    println!("{}", sep.join("-+-"));
    for row in &rendered {
        println!("{}", join_padded(row, &widths));
    }
    println!(
        "({} row{})",
        rs.rows.len(),
        if rs.rows.len() == 1 { "" } else { "s" }
    );
}

fn join_padded<S: AsRef<str>>(cells: &[S], widths: &[usize]) -> String {
    cells
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{:width$}",
                c.as_ref(),
                width = widths.get(i).copied().unwrap_or(0)
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_engine::EngineError;

    fn parse_err(sql: &str) -> EngineError {
        // Drive a real parse failure through the engine's error type.
        EngineError::Parse(skaidb_sql::parse(sql).unwrap_err())
    }

    #[test]
    fn suggests_show_tables() {
        let e = parse_err("show dbs;");
        let hint = suggest("show dbs;", &e).unwrap();
        assert!(hint.contains("SHOW TABLES"), "{hint}");
    }

    #[test]
    fn suggests_database_for_unknown_create_object() {
        // `db` is not a keyword, so the parser rejects the object kind.
        let e = parse_err("create db foo;");
        let hint = suggest("create db foo;", &e).unwrap();
        assert!(hint.contains("CREATE DATABASE"), "{hint}");
    }

    #[test]
    fn suggests_primary_key_clause() {
        let e = parse_err("create table test;");
        let hint = suggest("create table test;", &e).unwrap();
        assert!(hint.contains("PRIMARY KEY"), "{hint}");
        assert!(hint.contains("test"), "{hint}");
    }

    #[test]
    fn suggests_closest_statement_keyword() {
        let e = parse_err("slect 1;");
        let hint = suggest("slect 1;", &e).unwrap();
        assert!(hint.contains("SELECT"), "{hint}");
    }

    #[test]
    fn flags_stray_backslash() {
        let e = parse_err("create table meh;\\");
        let hint = suggest("create table meh;\\", &e).unwrap();
        assert!(hint.contains('\\'), "{hint}");
    }

    #[test]
    fn meta_help_and_quit() {
        assert!(matches!(meta_command("help"), Meta::Help));
        assert!(matches!(meta_command("?"), Meta::Help));
        assert!(matches!(meta_command("exit"), Meta::Quit));
        assert!(matches!(meta_command("select 1"), Meta::None));
    }
}
