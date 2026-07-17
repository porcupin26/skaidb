//! Replicated driver/connection registry (`drivers`).
//!
//! Every binary-protocol connection, once authenticated, INSERTs a row
//! describing itself — node, remote address, authenticated user, connect
//! time — and removes it on disconnect. One row per live connection,
//! keyed on a connection id unique to this node. The row replicates like
//! any other write, so any member serves the whole cluster's picture of
//! "who's connected right now" from one local table read:
//! `SELECT node, remote_addr, auth_user, connected_at FROM drivers`.
//!
//! REST connections are NOT registered here: `rest.rs` serves exactly one
//! request per TCP connection (no loop), so registering each one would mean
//! an insert+delete per HTTP call — high-churn replicated writes for very
//! little signal. REST activity stays visible via the existing
//! `skaidb_connections_total{endpoint="rest"}` counter, which is the right
//! shape (a rate) for something this short-lived. "Driver" here means the
//! binary protocol's pooled, long-lived connections, which is what
//! `client_name`/`client_version` (once self-reported identity exists) will
//! actually describe.

use std::sync::atomic::{AtomicU64, Ordering};

use skaidb_proto::Response;

use crate::shared::Shared;

/// The registry table, in the default database (like `node_stats`).
pub const TABLE: &str = "drivers";

/// Process-global monotonic counter for connection ids: `{node}-{seq}` is
/// unique per connection without needing the OS-level port/fd, which callers
/// don't have handy at the point they call `register`. Safe to be a bare
/// static (unlike the per-`Context` "ensured" flag) — sharing the counter
/// across independent test contexts just means ids aren't reused, which is
/// harmless.
static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

fn exec(ctx: &Shared, sql: &str) -> Result<Response, String> {
    let role = ctx.superuser_role.clone();
    let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
    match crate::shared::execute_session_as(ctx, &role, &mut db, sql, None) {
        Response::Error(e) => Err(e),
        resp => Ok(resp),
    }
}

/// Idempotent DDL for the registry table. Memory table: one row per live
/// connection, meaningless across a restart (every connection from before a
/// restart is gone) — RAM-only avoids WAL/repair cost on a table with
/// naturally high write churn.
fn ensure_table(ctx: &Shared) -> Result<(), String> {
    if ctx.backend.table_is_memory(TABLE) == Some(false) {
        exec(ctx, &format!("DROP TABLE IF EXISTS {TABLE}"))?;
    }
    exec(
        ctx,
        &format!("CREATE TABLE IF NOT EXISTS {TABLE} (PRIMARY KEY (conn_id)) WITH (memory = true)"),
    )
    .map(|_| ())
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

/// Register one binary-protocol connection. Returns the connection id used
/// to remove the row again — `None` if registration failed (best-effort:
/// telemetry never blocks or fails an actual client connection). The
/// cluster may not be ready yet at boot, same tolerance `nodestats` has.
pub fn register(ctx: &Shared, node: &str, remote_addr: &str, auth_user: &str) -> Option<String> {
    if !ctx.drivers_table_ensured.load(Ordering::Relaxed) {
        if ensure_table(ctx).is_err() {
            return None;
        }
        ctx.drivers_table_ensured.store(true, Ordering::Relaxed);
    }
    let conn_id = format!("{node}-{}", CONN_SEQ.fetch_add(1, Ordering::Relaxed));
    let connected_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let sql = format!(
        "INSERT INTO {TABLE} (conn_id, node, endpoint, remote_addr, auth_user, connected_at) \
         VALUES ('{}', '{}', 'binary', '{}', '{}', {connected_at_ms})",
        quote(&conn_id),
        quote(node),
        quote(remote_addr),
        quote(auth_user),
    );
    exec(ctx, &sql).ok()?;
    Some(conn_id)
}

/// Deregister a connection (best-effort — a failed delete just leaves a
/// stale row that a restart clears anyway, since the table is memory-only).
pub fn deregister(ctx: &Shared, conn_id: &str) {
    let _ = exec(
        ctx,
        &format!("DELETE FROM {TABLE} WHERE conn_id = '{}'", quote(conn_id)),
    );
}

/// RAII guard: registers on creation, deregisters on drop, so every one of
/// `handle_connection`'s several early-return paths cleans up the row
/// without having to be found and touched individually. A `None` id (failed
/// registration) makes `drop` a no-op.
pub struct ConnGuard<'a> {
    ctx: &'a Shared,
    conn_id: Option<String>,
}

impl<'a> ConnGuard<'a> {
    pub fn new(ctx: &'a Shared, node: &str, remote_addr: &str, auth_user: &str) -> Self {
        Self {
            ctx,
            conn_id: register(ctx, node, remote_addr, auth_user),
        }
    }
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = &self.conn_id {
            deregister(self.ctx, id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_ids_are_unique_per_node() {
        CONN_SEQ.store(0, Ordering::Relaxed);
        let a = format!("n1-{}", CONN_SEQ.fetch_add(1, Ordering::Relaxed));
        let b = format!("n1-{}", CONN_SEQ.fetch_add(1, Ordering::Relaxed));
        assert_ne!(a, b);
    }

    #[test]
    fn quote_escapes_single_quotes() {
        assert_eq!(quote("o'brien"), "o''brien");
    }
}
