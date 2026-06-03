//! skaidb SQL client.
//!
//! Phase 1 runs an **embedded** engine against a local data directory: either a
//! one-shot statement via `-e`, or an interactive REPL reading from stdin.
//! Remote connection over the driver lands in a later phase.

use std::io::{self, BufRead, IsTerminal, Write};

use clap::Parser;
use skaidb_engine::{Database, QueryOutput, ResultSet};

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

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let mut db = match Database::open(&cli.dir) {
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

    repl(&mut db);
    std::process::ExitCode::SUCCESS
}

/// Execute a `;`-separated script, stopping at the first error.
fn run_script(db: &mut Database, script: &str) -> Result<(), ()> {
    for stmt in split_statements(script) {
        if let Err(e) = run_one(db, &stmt) {
            eprintln!("error: {e}");
            return Err(());
        }
    }
    Ok(())
}

fn repl(db: &mut Database) {
    let stdin = io::stdin();
    let interactive = stdin.is_terminal();
    if interactive {
        eprintln!("skaidb interactive shell. End statements with ';'. Ctrl-D to exit.");
    }
    let mut buffer = String::new();
    loop {
        if interactive && buffer.is_empty() {
            eprint!("skaidb> ");
            let _ = io::stderr().flush();
        }
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

        // Execute complete (semicolon-terminated) statements as they accumulate.
        while let Some(idx) = find_statement_end(&buffer) {
            let stmt: String = buffer.drain(..=idx).collect();
            let stmt = stmt.trim();
            if !stmt.is_empty() && stmt != ";" {
                if let Err(e) = run_one(db, stmt) {
                    eprintln!("error: {e}");
                }
            }
        }
    }
}

fn run_one(db: &mut Database, sql: &str) -> Result<(), skaidb_engine::EngineError> {
    match db.execute(sql)? {
        QueryOutput::Rows(rs) => print_table(&rs),
        QueryOutput::Mutation { affected } => println!("OK, {affected} row(s) affected"),
        QueryOutput::Ddl => println!("OK"),
    }
    Ok(())
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
