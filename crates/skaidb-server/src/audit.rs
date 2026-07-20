//! Configurable audit logging (SPEC §10): query log (masked), slow-query log,
//! and error log. Login/connection logging is wired in when the auth handshake
//! lands; the setting is carried here already.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};

use skaidb_config::ObservabilityConfig;

/// Where one category of audit log lines is written: either the process stderr
/// (the default) or an append-mode file shared across connection threads.
///
/// Cloning shares the same underlying file handle, so two categories pointed at
/// the same path serialize their writes through one mutex rather than opening
/// the file twice.
#[derive(Debug, Clone)]
enum LogSink {
    Stderr,
    File(Arc<Mutex<std::fs::File>>),
}

impl LogSink {
    /// Write one already-formatted line. Logging is best-effort: a write error
    /// (full disk, revoked permissions) must never take down a query, so
    /// failures are dropped silently after the initial open.
    /// Write one line, prefixed with the shared log timestamp. JSON-format
    /// lines carry the timestamp as a `ts` field instead (added by the
    /// caller), so `json` skips the prefix to keep every line parseable.
    fn write_line(&self, line: &str) {
        self.write_stamped(&format!("{} {line}", skaidb_types::log_timestamp()));
    }

    fn write_json(&self, line: &str) {
        self.write_stamped(line);
    }

    fn write_stamped(&self, line: &str) {
        match self {
            LogSink::Stderr => eprintln!("{line}"),
            LogSink::File(f) => {
                let mut guard = f.lock().unwrap_or_else(|e| e.into_inner());
                let _ = writeln!(guard, "{line}");
            }
        }
    }
}

/// Resolve a category's sink: its own `specific` path if set, else the shared
/// `base` path, else stderr. Files are opened once per distinct path (cached in
/// `opened`) so categories sharing a file share a handle. A path that can't be
/// opened logs the reason once and falls back to stderr rather than failing.
fn resolve_sink(opened: &mut HashMap<String, LogSink>, base: &str, specific: &str) -> LogSink {
    let path = if specific.is_empty() { base } else { specific };
    if path.is_empty() {
        return LogSink::Stderr;
    }
    if let Some(sink) = opened.get(path) {
        return sink.clone();
    }
    let sink = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => LogSink::File(Arc::new(Mutex::new(file))),
        Err(e) => {
            eprintln!("skaidb: cannot open log file {path}: {e}; logging to stderr instead");
            LogSink::Stderr
        }
    };
    opened.insert(path.to_string(), sink.clone());
    sink
}

/// Audit logging settings, derived from the observability config.
#[derive(Debug, Clone)]
pub struct AuditSettings {
    pub query_log: bool,
    pub query_masked: bool,
    pub slow_query_ms: u64,
    pub login_log: bool,
    pub error_log: bool,
    /// Emit one JSON object per log line instead of human-readable text, so a
    /// log agent can parse query/slow/error/login records reliably (SPEC §10).
    pub json: bool,
    /// Per-category destinations (see [`LogSink`]). Built from the configured
    /// log files; each defaults to the shared `log_file`, which defaults to
    /// stderr.
    query_sink: LogSink,
    slow_sink: LogSink,
    error_sink: LogSink,
    login_sink: LogSink,
}

impl From<&ObservabilityConfig> for AuditSettings {
    fn from(c: &ObservabilityConfig) -> Self {
        let mut opened = HashMap::new();
        let base = c.log_file.as_str();
        AuditSettings {
            query_log: c.query_log_enabled,
            query_masked: c.query_log_masked,
            slow_query_ms: c.slow_query_ms,
            login_log: c.login_log_enabled,
            error_log: c.error_log_enabled(),
            json: c.log_format.eq_ignore_ascii_case("json"),
            query_sink: resolve_sink(&mut opened, base, &c.query_log_file),
            slow_sink: resolve_sink(&mut opened, base, &c.slow_query_log_file),
            error_sink: resolve_sink(&mut opened, base, &c.error_log_file),
            login_sink: resolve_sink(&mut opened, base, &c.login_log_file),
        }
    }
}

/// Helper so config's `error_log_level` ("off" disables) maps to a bool.
trait ErrorLogEnabled {
    fn error_log_enabled(&self) -> bool;
}
impl ErrorLogEnabled for ObservabilityConfig {
    fn error_log_enabled(&self) -> bool {
        !self.error_log_level.eq_ignore_ascii_case("off")
    }
}

impl AuditSettings {
    /// All categories off, writing nowhere — for tests and quiet startup.
    pub fn quiet() -> Self {
        AuditSettings {
            query_log: false,
            query_masked: true,
            slow_query_ms: 0,
            login_log: false,
            error_log: false,
            json: false,
            query_sink: LogSink::Stderr,
            slow_sink: LogSink::Stderr,
            error_sink: LogSink::Stderr,
            login_sink: LogSink::Stderr,
        }
    }

    /// Record one executed statement per the configured logs. Each category
    /// (query / slow-query / error) is formatted as text or JSON and written to
    /// its own sink, so they can land in separate files.
    pub fn record(&self, sql: &str, elapsed_ms: u64, error: Option<&str>) {
        let slow = self.slow_query_ms > 0 && elapsed_ms >= self.slow_query_ms;
        if self.query_log {
            let shown = if self.query_masked {
                mask_sql(sql)
            } else {
                sql.to_string()
            };
            if self.json {
                self.query_sink.write_json(
                    &serde_json::json!({
                        "ts": skaidb_types::log_timestamp(),
                        "event": "query",
                        "elapsed_ms": elapsed_ms,
                        "slow": slow,
                        "error": error,
                        "sql": shown,
                    })
                    .to_string(),
                );
            } else {
                self.query_sink.write_line(&format!("[query] {elapsed_ms}ms {shown}"));
            }
        }
        if slow {
            if self.json {
                self.slow_sink.write_json(
                    &serde_json::json!({
                        "ts": skaidb_types::log_timestamp(),
                        "event": "slow_query",
                        "elapsed_ms": elapsed_ms,
                        "sql": mask_sql(sql),
                    })
                    .to_string(),
                );
            } else {
                self.slow_sink
                    .write_line(&format!("[slow-query] {elapsed_ms}ms {}", mask_sql(sql)));
            }
        }
        if let Some(msg) = error {
            if self.error_log {
                if self.json {
                    self.error_sink.write_json(
                        &serde_json::json!({
                            "ts": skaidb_types::log_timestamp(),
                            "event": "error",
                            "message": msg,
                        })
                        .to_string(),
                    );
                } else {
                    self.error_sink.write_line(&format!("[error] {msg}"));
                }
            }
        }
    }

    /// Record an executed auth-DDL statement (user/role/grant management)
    /// in the identity log (the login sink/category). `summary` must be
    /// secret-free — the caller renders it from the parsed statement with
    /// passwords and verifiers omitted.
    pub fn log_auth_ddl(&self, actor: &str, summary: &str, ok: bool) {
        if !self.login_log {
            return;
        }
        if self.json {
            self.login_sink.write_json(
                &serde_json::json!({
                    "ts": skaidb_types::log_timestamp(),
                    "event": "auth_ddl",
                    "actor": actor,
                    "ok": ok,
                    "stmt": summary,
                })
                .to_string(),
            );
        } else {
            self.login_sink
                .write_line(&format!("[auth-ddl] actor={actor} ok={ok} {summary}"));
        }
    }

    /// Record a login outcome per the configured login log (text or JSON).
    pub fn log_login(&self, user: &str, role: Option<&str>, ok: bool) {
        if !self.login_log {
            return;
        }
        if self.json {
            self.login_sink.write_json(
                &serde_json::json!({
                    "ts": skaidb_types::log_timestamp(),
                    "event": if ok { "login" } else { "login_failed" },
                    "user": user,
                    "role": role,
                })
                .to_string(),
            );
        } else if ok {
            self.login_sink
                .write_line(&format!("[login] user={user} role={}", role.unwrap_or("")));
        } else {
            self.login_sink.write_line(&format!("[login-failed] user={user}"));
        }
    }
}

/// Replace literal values (string and numeric) with `?` so logged statements
/// reveal shape and tables but not data (SPEC §10.2).
pub fn mask_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // Consume the whole string literal (handling '' escapes) → '?'.
                out.push('?');
                while let Some(&ch) = chars.peek() {
                    chars.next();
                    if ch == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next(); // escaped quote, stay in string
                        } else {
                            break;
                        }
                    }
                }
            }
            c if c.is_ascii_digit() => {
                out.push('?');
                while let Some(&ch) = chars.peek() {
                    if ch.is_ascii_digit() || ch == '.' {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_string_and_number_literals() {
        assert_eq!(
            mask_sql("INSERT INTO t (id, name) VALUES (42, 'ada')"),
            "INSERT INTO t (id, name) VALUES (?, ?)"
        );
    }

    #[test]
    fn masks_floats_and_keeps_identifiers() {
        assert_eq!(
            mask_sql("SELECT a.b FROM t WHERE x >= 3.14"),
            "SELECT a.b FROM t WHERE x >= ?"
        );
    }

    #[test]
    fn handles_escaped_quotes() {
        assert_eq!(mask_sql("SELECT 'it''s' FROM t"), "SELECT ? FROM t");
    }

    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("skaidb-audit-{tag}-{}-{n}.log", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn writes_audit_log_to_shared_file() {
        let path = temp_path("shared");
        let cfg = ObservabilityConfig {
            log_file: path.display().to_string(),
            slow_query_ms: 100,
            ..Default::default()
        };
        let audit = AuditSettings::from(&cfg);
        audit.record("INSERT INTO t (id) VALUES (1)", 5, None);
        audit.record("SELECT * FROM big", 250, Some("boom"));
        audit.log_login("ada", Some("admin"), true);

        let contents = std::fs::read_to_string(&path).unwrap();
        // Query, slow-query, error, and login lines all land in the one file.
        assert!(contents.contains("[query] 5ms"), "got: {contents}");
        assert!(contents.contains("[slow-query] 250ms"), "got: {contents}");
        assert!(contents.contains("[error] boom"), "got: {contents}");
        assert!(contents.contains("[login] user=ada"), "got: {contents}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn per_category_override_splits_files() {
        let base = temp_path("base");
        let errors = temp_path("errors");
        let cfg = ObservabilityConfig {
            log_file: base.display().to_string(),
            error_log_file: errors.display().to_string(),
            ..Default::default()
        };
        let audit = AuditSettings::from(&cfg);
        audit.record("SELECT 1", 1, Some("kaboom"));

        // The error went to its own file; the query went to the shared file.
        let err_contents = std::fs::read_to_string(&errors).unwrap();
        let base_contents = std::fs::read_to_string(&base).unwrap();
        assert!(err_contents.contains("[error] kaboom"), "got: {err_contents}");
        assert!(!base_contents.contains("kaboom"), "got: {base_contents}");
        assert!(base_contents.contains("[query] 1ms"), "got: {base_contents}");
        let _ = std::fs::remove_file(&base);
        let _ = std::fs::remove_file(&errors);
    }
}
