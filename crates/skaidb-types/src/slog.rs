//! Process-wide server (operational) log.
//!
//! Distinct from the per-query *audit* log: this carries startup, membership,
//! anti-entropy and bind/listen events — the things an operator greps for when
//! a node misbehaves. It lives in this leaf crate so both the server and the
//! cluster background threads can reach it without a dependency cycle.
//!
//! The destination is set once at startup with [`init_server_log`] from
//! `observability.log_file`: a path makes `skaidb.log` the catch-all server log;
//! an empty path keeps lines on stderr (journald under systemd). Writing before
//! init, or with no path configured, falls back to stderr.

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};

/// Where operational lines go: stderr (the default) or an append-mode file.
enum Sink {
    Stderr,
    File(Mutex<std::fs::File>),
}

static SERVER_LOG: OnceLock<Sink> = OnceLock::new();

/// Point the server log at `path` (empty = stderr). Idempotent: only the first
/// call wins, so it should run once early in startup, before workers spawn. A
/// path that can't be opened logs the reason once and stays on stderr.
pub fn init_server_log(path: &str) {
    let sink = if path.is_empty() {
        Sink::Stderr
    } else {
        match OpenOptions::new().create(true).append(true).open(path) {
            Ok(f) => Sink::File(Mutex::new(f)),
            Err(e) => {
                eprintln!("skaidb: cannot open log file {path}: {e}; logging to stderr instead");
                Sink::Stderr
            }
        }
    };
    let _ = SERVER_LOG.set(sink);
}

/// Write one operational line to the configured server log, or stderr if the
/// log is unset/uninitialized. Best-effort: a write error is dropped rather than
/// taking down the server. Prefer the [`slog!`] macro at call sites.
pub fn server_log(msg: &str) {
    match SERVER_LOG.get() {
        Some(Sink::File(f)) => {
            // One write_all of the whole line (incl. newline) so concurrent
            // append writers don't interleave a partial line.
            let line = format!("{msg}\n");
            let mut guard = f.lock().unwrap_or_else(|e| e.into_inner());
            let _ = guard.write_all(line.as_bytes());
        }
        _ => eprintln!("{msg}"),
    }
}

/// Format and emit one server-log line, like `println!` but to the server log.
#[macro_export]
macro_rules! slog {
    ($($arg:tt)*) => { $crate::server_log(&format!($($arg)*)) };
}
