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

/// Current wall time as an ISO-8601 UTC string with millisecond precision
/// (`2026-07-20T18:42:13.123Z`) — every server/audit log line's prefix, and
/// the `ts` field of JSON-format audit lines.
pub fn log_timestamp() -> String {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    // Civil-from-days (Howard Hinnant's algorithm) — same math as the SQL
    // timestamp formatter, kept dependency-free in this leaf crate.
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let (secs, ms_part) = (rem / 1000, rem % 1000);
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms_part:03}Z")
}

/// Write one operational line to the configured server log, or stderr if the
/// log is unset/uninitialized. Every line carries a [`log_timestamp`] prefix.
/// Best-effort: a write error is dropped rather than
/// taking down the server. Prefer the [`slog!`] macro at call sites.
pub fn server_log(msg: &str) {
    let ts = log_timestamp();
    match SERVER_LOG.get() {
        Some(Sink::File(f)) => {
            // One write_all of the whole line (incl. newline) so concurrent
            // append writers don't interleave a partial line.
            let line = format!("{ts} {msg}\n");
            let mut guard = f.lock().unwrap_or_else(|e| e.into_inner());
            let _ = guard.write_all(line.as_bytes());
        }
        _ => eprintln!("{ts} {msg}"),
    }
}

/// Format and emit one server-log line, like `println!` but to the server log.
#[macro_export]
macro_rules! slog {
    ($($arg:tt)*) => { $crate::server_log(&format!($($arg)*)) };
}

#[cfg(test)]
mod tests {
    #[test]
    fn log_timestamp_shape() {
        let ts = super::log_timestamp();
        // 2026-07-20T18:42:13.123Z — fixed width, UTC, millisecond precision.
        assert_eq!(ts.len(), 24, "{ts}");
        assert!(ts.ends_with('Z') && ts.as_bytes()[10] == b'T', "{ts}");
        assert!(ts.starts_with("20"), "{ts}");
    }
}
