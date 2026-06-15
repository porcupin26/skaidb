//! Configurable audit logging (SPEC §10): query log (masked), slow-query log,
//! and error log. Login/connection logging is wired in when the auth handshake
//! lands; the setting is carried here already.

use skaidb_config::ObservabilityConfig;

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
}

impl From<&ObservabilityConfig> for AuditSettings {
    fn from(c: &ObservabilityConfig) -> Self {
        AuditSettings {
            query_log: c.query_log_enabled,
            query_masked: c.query_log_masked,
            slow_query_ms: c.slow_query_ms,
            login_log: c.login_log_enabled,
            error_log: c.error_log_enabled(),
            json: c.log_format.eq_ignore_ascii_case("json"),
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
    /// Record one executed statement per the configured logs.
    pub fn record(&self, sql: &str, elapsed_ms: u64, error: Option<&str>) {
        let slow = self.slow_query_ms > 0 && elapsed_ms >= self.slow_query_ms;
        if self.json {
            if self.query_log {
                let shown = if self.query_masked {
                    mask_sql(sql)
                } else {
                    sql.to_string()
                };
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "query",
                        "elapsed_ms": elapsed_ms,
                        "slow": slow,
                        "error": error,
                        "sql": shown,
                    })
                );
            }
            if slow {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "slow_query",
                        "elapsed_ms": elapsed_ms,
                        "sql": mask_sql(sql),
                    })
                );
            }
            if let Some(msg) = error {
                if self.error_log {
                    eprintln!("{}", serde_json::json!({"event": "error", "message": msg}));
                }
            }
            return;
        }
        if self.query_log {
            let shown = if self.query_masked {
                mask_sql(sql)
            } else {
                sql.to_string()
            };
            eprintln!("[query] {elapsed_ms}ms {shown}");
        }
        if slow {
            eprintln!("[slow-query] {elapsed_ms}ms {}", mask_sql(sql));
        }
        if let Some(msg) = error {
            if self.error_log {
                eprintln!("[error] {msg}");
            }
        }
    }

    /// Record a login outcome per the configured login log (text or JSON).
    pub fn log_login(&self, user: &str, role: Option<&str>, ok: bool) {
        if !self.login_log {
            return;
        }
        if self.json {
            eprintln!(
                "{}",
                serde_json::json!({
                    "event": if ok { "login" } else { "login_failed" },
                    "user": user,
                    "role": role,
                })
            );
        } else if ok {
            eprintln!("[login] user={user} role={}", role.unwrap_or(""));
        } else {
            eprintln!("[login-failed] user={user}");
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
}
