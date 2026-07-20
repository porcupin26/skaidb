//! REST/HTTP endpoint (SPEC §7) — a thin gateway over the same query engine.
//!
//! `POST /query` with the SQL as the request body (plain text, or JSON
//! `{"sql": "..."}`). The response is JSON: a result set, an affected count, or
//! an error. A minimal HTTP/1.1 implementation keeps the dependency surface
//! small; it serves one request per connection (`Connection: close`).

use std::io::{self, BufRead, BufReader, Read};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::{json, Value as Json};
use skaidb_proto::Response;

use crate::metrics::{Endpoint, RestPath};
use crate::shared::{collect_runtime_metrics, execute_as, Shared};

/// Socket timeouts for REST connections. A peer that stops reading (or a
/// dead client behind a proxy) must not pin a handler thread — and its fully
/// materialized response — forever: threads stuck mid-write on multi-GB
/// responses held a production node at its cgroup ceiling for hours
/// (2026-07-15 skai2 wedge). A timed-out write errors the handler out and
/// drops the buffers.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);

/// Cap on a request body and on a materialized `/query` result, mirroring the
/// binary protocol's frame limit: past it the gateway answers with guidance
/// instead of ballooning the heap.
const MAX_BODY_LEN: usize = skaidb_proto::MAX_FRAME_LEN as usize;

/// What a REST listener does with a connection.
#[derive(Clone, Copy, Debug)]
pub enum RestRole {
    /// Serve the REST API (plaintext and/or TLS per the acceptor).
    Data,
    /// Serve nothing but an HTTP→HTTPS redirect to `https://<host>:<port><path>`
    /// (308). Used on the plaintext port when client TLS is enabled, so old
    /// `http://…:7080` URLs bounce to the TLS port instead of failing.
    RedirectToTls(u16),
}

/// Bind the REST endpoint and serve it on a background thread.
pub fn spawn(
    addr: &str,
    ctx: Shared,
    role: RestRole,
) -> io::Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let acceptor = crate::tls::build(&ctx)?;
    let handle = thread::spawn(move || serve(listener, ctx, acceptor, role));
    Ok((local, handle))
}

/// Accept connections forever, handling each on its own thread.
pub fn serve(
    listener: TcpListener,
    ctx: Shared,
    acceptor: Option<crate::tls::Acceptor>,
    role: RestRole,
) {
    for stream in listener.incoming().flatten() {
        // Best-effort: a socket that rejects the option still gets served,
        // it just keeps the old unbounded behavior.
        let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
        let _ = stream.set_write_timeout(Some(WRITE_TIMEOUT));
        let ctx = ctx.clone();
        let acceptor = acceptor.clone();
        thread::spawn(move || {
            ctx.metrics.connection_opened(Endpoint::Rest);
            let _ = handle_connection(stream, ctx.clone(), acceptor.as_ref(), role);
            ctx.metrics.connection_closed(Endpoint::Rest);
        });
    }
}

/// Read one plaintext request and answer with a 308 to the TLS port. The
/// redirect listener never touches the data plane — it exists so a client that
/// hits the old plaintext REST port (7080) after TLS was turned on is bounced to
/// `https://…:<tls_port>` rather than getting a connection reset.
fn redirect_to_tls(tcp: TcpStream, tls_port: u16) -> io::Result<()> {
    let mut stream = skaidb_net::Stream::Plain(tcp);
    let req = match read_request(&mut stream) {
        Ok(Some(req)) => req,
        _ => return Ok(()),
    };
    // Reconstruct the target host from the Host header, dropping any :port.
    // Without a Host header (rare on HTTP/1.1) we can't form an absolute URL —
    // close rather than guess.
    let Some(host) = req
        .host
        .as_deref()
        .map(|h| h.rsplit_once(':').map(|(h, _)| h).unwrap_or(h))
        .filter(|h| !h.is_empty())
    else {
        return Ok(());
    };
    let location = format!("https://{host}:{tls_port}{}", req.path);
    write_redirect(&mut stream, &location)
}

fn handle_connection(
    tcp: TcpStream,
    ctx: Shared,
    acceptor: Option<&crate::tls::Acceptor>,
    role: RestRole,
) -> io::Result<()> {
    if let RestRole::RedirectToTls(port) = role {
        return redirect_to_tls(tcp, port);
    }
    // Wrap per the client-TLS policy (or drop a plaintext peer under
    // `required`). Probes (`/health` etc.) share this port, so under
    // `required` they must speak TLS too.
    let Some(net_stream) = crate::tls::wrap(tcp, acceptor) else {
        return Ok(()); // plaintext refused, or handshake failed
    };
    let mut stream = net_stream;
    let req = match read_request(&mut stream) {
        Ok(Some(req)) => req,
        Ok(None) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
            return write_response(
                &mut stream,
                413,
                &json!({"error": format!(
                    "request body exceeds the {} MiB limit; batch smaller or use the binary protocol",
                    MAX_BODY_LEN / (1024 * 1024)
                )}),
            );
        }
        Err(_) => {
            return write_response(&mut stream, 400, &json!({"error": "malformed request"}));
        }
    };

    // Per-request activity accounting (`skaidb_rest_requests_total{path=…}`
    // + duration, surfaced on the status tab): classify once, record on
    // EVERY exit path via Drop — this handler returns from ~a dozen arms.
    struct RestTimer<'a> {
        ctx: &'a Shared,
        path: crate::metrics::RestPath,
        start: Instant,
    }
    impl Drop for RestTimer<'_> {
        fn drop(&mut self) {
            self.ctx
                .metrics
                .observe_rest(self.path, self.start.elapsed().as_secs_f64());
        }
    }
    let _timer = RestTimer {
        ctx: &ctx,
        path: classify_rest(&req.method, &req.path),
        start: Instant::now(),
    };

    // Unauthenticated read-only operational endpoints (SPEC §10). These exist so
    // orchestrators, load balancers, and metrics scrapers need no credentials and
    // no admin rights.
    if req.method == "GET" {
        match req.path.split('?').next().unwrap_or(&req.path) {
            // Prometheus scrape: refresh pull-model gauges, then render.
            "/metrics" => {
                collect_runtime_metrics(&ctx);
                return write_text(&mut stream, 200, &ctx.metrics.render());
            }
            // Liveness: the process is up and serving. Always 200.
            "/health" | "/healthz" => {
                return write_text(&mut stream, 200, "ok\n");
            }
            // Readiness: storage is open (and, clustered, the node has a topology).
            "/ready" | "/readyz" => {
                let (status, body) = if ctx.backend.is_ready() {
                    (200, "ready\n")
                } else {
                    (503, "not ready\n")
                };
                return write_text(&mut stream, status, body);
            }
            // Low-privilege topology read: ring/epoch/members only, no secrets.
            "/status" => {
                return write_response(&mut stream, 200, &status_json(&ctx));
            }
            // The RBAC-filtered schema browser: authenticated, unlike the
            // static assets — resolve the role first, then answer with only
            // what it may see. Same live ui.enabled gate as the rest.
            "/ui/schema" => {
                let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
                if !enabled {
                    return write_response(&mut stream, 404, &json!({"error": "not found"}));
                }
                let role = if ctx.authn.required {
                    match auth_role(&ctx, req.authorization.as_deref()) {
                        Some(role) => role,
                        None => return write_unauthorized(&mut stream, &ctx),
                    }
                } else {
                    ctx.superuser_role.clone()
                };
                let (status, body) = crate::ui::schema_json(&ctx, &role);
                return write_json_body(&mut stream, status, &body);
            }
            "/ui/inventory" => {
                let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
                if !enabled {
                    return write_response(&mut stream, 404, &json!({"error": "not found"}));
                }
                let role = if ctx.authn.required {
                    match auth_role(&ctx, req.authorization.as_deref()) {
                        Some(role) => role,
                        None => return write_unauthorized(&mut stream, &ctx),
                    }
                } else {
                    ctx.superuser_role.clone()
                };
                let (status, body) = crate::ui::inventory_json(&ctx, &role);
                return write_json_body(&mut stream, status, &body);
            }
            // Per-node host stats for the stats tab: authenticated the same
            // way (no table data, but not anonymous either).
            "/ui/hosts" => {
                let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
                if !enabled {
                    return write_response(&mut stream, 404, &json!({"error": "not found"}));
                }
                if ctx.authn.required && auth_role(&ctx, req.authorization.as_deref()).is_none()
                {
                    return write_unauthorized(&mut stream, &ctx);
                }
                let (status, body) = crate::ui::hosts_json(&ctx);
                return write_json_body(&mut stream, status, &body);
            }
            // Live driver connections + registered witnesses for the status
            // tab: same trust level as /ui/hosts (authenticated, no
            // table-RBAC — this is operational metadata, not tenant data).
            "/ui/drivers" => {
                let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
                if !enabled {
                    return write_response(&mut stream, 404, &json!({"error": "not found"}));
                }
                if ctx.authn.required && auth_role(&ctx, req.authorization.as_deref()).is_none()
                {
                    return write_unauthorized(&mut stream, &ctx);
                }
                let (status, body) = crate::ui::drivers_json(&ctx);
                return write_json_body(&mut stream, status, &body);
            }
            "/ui/witnesses" => {
                let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
                if !enabled {
                    return write_response(&mut stream, 404, &json!({"error": "not found"}));
                }
                if ctx.authn.required && auth_role(&ctx, req.authorization.as_deref()).is_none()
                {
                    return write_unauthorized(&mut stream, &ctx);
                }
                let (status, body) = crate::ui::witnesses_json(&ctx);
                return write_json_body(&mut stream, status, &body);
            }
            // The embedded web UI: static shell + /ui/meta. Gated on the
            // live `ui.enabled` config inside try_route (404 when off).
            path => {
                if let Some(asset) = crate::ui::try_route(&ctx, path) {
                    return write_asset(&mut stream, &asset);
                }
            }
        }
    }

    // Cluster control plane: POST /admin/* (RBAC-gated inside admin::handle).
    if req.method == "POST" && req.path.starts_with("/admin/") {
        let body_text = String::from_utf8_lossy(&req.body);
        let Some(cmd) = crate::admin::parse(&req.path, &body_text) else {
            return write_response(&mut stream, 404, &json!({"error": "unknown admin route"}));
        };
        let role = if ctx.authn.required {
            match auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream, &ctx),
            }
        } else {
            ctx.superuser_role.clone()
        };
        let (status, payload) = crate::admin::handle(&ctx, &role, cmd);
        return write_response(&mut stream, status, &payload);
    }

    // Prometheus HTTP query API (GET with a query string, or POST with a
    // form body — Grafana uses both). A `/db/<database>[/table/<table>]`
    // path prefix scopes the API to that database (and optionally to any
    // TS table, whose FIELDS then serve as the metric names) — point a
    // Grafana Prometheus datasource's base URL at the prefix and it works
    // per database/table (docs/GRAFANA.md).
    let bare_path = req.path.split('?').next().unwrap_or(&req.path).to_string();
    let (prom_scope, prom_path) = match prom_scope_of(&bare_path) {
        Ok(v) => v,
        Err(msg) => {
            return write_response(
                &mut stream,
                400,
                &json!({"status": "error", "error": msg}),
            )
        }
    };
    if prom_path.starts_with("/api/v1/") && prom_path != "/api/v1/write" {
        let role = if ctx.authn.required {
            match auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream, &ctx),
            }
        } else {
            ctx.superuser_role.clone()
        };
        // Reading requires Select on the scoped table (a grant on its
        // database also satisfies it).
        if !ctx.allowed_on_table(
            &role,
            skaidb_auth::Privilege::Select,
            &prom_scope.bare,
            &prom_scope.db,
        ) {
            return write_response(
                &mut stream,
                403,
                &json!({"status": "error",
                        "error": format!("permission denied: Select on {} (database {})",
                                          prom_scope.bare, prom_scope.db)}),
            );
        }
        // Merge query-string and form-body params (body wins on conflict).
        let mut params = crate::promql::parse_params(
            req.path.split_once('?').map(|(_, q)| q).unwrap_or(""),
        );
        if req.method == "POST" {
            for (k, v) in crate::promql::parse_params(&String::from_utf8_lossy(&req.body)) {
                params.insert(k, v);
            }
        }
        let started = std::time::Instant::now();
        let (status, payload) = match prom_path.as_str() {
            "/api/v1/query" => crate::promql::query(&ctx, &prom_scope, &params),
            "/api/v1/query_range" => crate::promql::query_range(&ctx, &prom_scope, &params),
            "/api/v1/labels" => crate::promql::labels(&ctx, &prom_scope, None),
            "/api/v1/series" => crate::promql::series(&ctx, &prom_scope, &params),
            "/api/v1/status/buildinfo" => (
                200,
                json!({"status": "success",
                       "data": {"version": env!("CARGO_PKG_VERSION"), "application": "skaidb"}}),
            ),
            "/api/v1/metadata" => (200, json!({"status": "success", "data": {}})),
            path => {
                if let Some(label) = path
                    .strip_prefix("/api/v1/label/")
                    .and_then(|rest| rest.strip_suffix("/values"))
                {
                    crate::promql::labels(&ctx, &prom_scope, Some(label))
                } else {
                    (404, json!({"status": "error", "error": "unknown api route"}))
                }
            }
        };
        // PromQL evaluations flow through the same audit query/error logs as
        // SQL, so a failing dashboard query is visible server-side (this
        // debugging surface did not exist while chasing the Grafana
        // drilldown 4xx chain — every failure was client-side-only).
        if matches!(prom_path.as_str(), "/api/v1/query" | "/api/v1/query_range") {
            let expr = params.get("query").map(String::as_str).unwrap_or("");
            let err = (status != 200)
                .then(|| payload["error"].as_str().unwrap_or("error").to_string());
            ctx.audit().record(
                &format!("promql {} {expr}", prom_path.trim_start_matches("/api/v1/")),
                started.elapsed().as_millis() as u64,
                err.as_deref(),
            );
        }
        return write_response(&mut stream, status, &payload);
    }

    // Prometheus remote_write: snappy-compressed protobuf WriteRequest.
    // Database-scoped via the same `/db/<database>` prefix; a `/table/`
    // scope is rejected (remote_write's shape IS the metrics table).
    if req.method == "POST" && prom_path == "/api/v1/write" {
        if prom_scope.field_metrics {
            return write_response(
                &mut stream,
                400,
                &json!({"status": "error",
                        "error": "remote_write ingests into the metrics table; \
                                  a /table/ scope cannot receive writes"}),
            );
        }
        let role = if ctx.authn.required {
            match auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream, &ctx),
            }
        } else {
            ctx.superuser_role.clone()
        };
        return match crate::promwrite::ingest(&ctx, &role, &req.body, &prom_scope.db) {
            Ok(accepted) => {
                ctx.metrics.add_rows_returned(0); // no rows out; count via queries metric
                write_response(&mut stream, 200, &json!({"accepted": accepted}))
            }
            Err(e) => write_response(&mut stream, 400, &json!({"error": e})),
        };
    }

    // ES-compatible subset (SPEC/FTS phase 8): /{index}/_search, _count,
    // _bulk, _mapping (+ /_bulk). Authenticated like /query.
    if crate::es::path_is_es(&req.path) {
        let role = if ctx.authn.required {
            match auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream, &ctx),
            }
        } else {
            ctx.superuser_role.clone()
        };
        if let Some((status, payload)) =
            crate::es::try_route(&ctx, &role, &req.method, &req.path, &req.body)
        {
            return write_response(&mut stream, status, &payload);
        }
    }

    // JSON-native document insert/upsert: `POST /insert` with
    // `{"db": "...", "table": "...", "rows": [{...}, ...]}`. Writes whole
    // documents — including nested objects and arrays, which SQL `INSERT`
    // cannot express as literals — the document-store-native write path.
    // Overwrites on the primary key (last-writer-wins), so it is also the
    // upsert. RBAC and replication go through the ordinary session path
    // (needs `Insert` on the target table/database).
    if req.method == "POST" && req.path.starts_with("/insert") {
        let role = if ctx.authn.required {
            match auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream, &ctx),
            }
        } else {
            ctx.superuser_role.clone()
        };
        let (status, payload) = handle_insert(&ctx, &role, &req.body);
        let body = payload.to_string();
        ctx.metrics
            .add_bytes_returned(Endpoint::Rest, body.len() as u64);
        return write_json_body(&mut stream, status, &body);
    }

    if req.method != "POST" || !req.path.starts_with("/query") {
        return write_response(
            &mut stream,
            404,
            &json!({"error": "use POST /query with a SQL body, POST /insert with JSON rows, or GET /metrics"}),
        );
    }

    // Resolve the role: HTTP Basic auth when required, else anonymous superuser.
    let role = if ctx.authn.required {
        match auth_role(&ctx, req.authorization.as_deref()) {
            Some(role) => role,
            None => {
                return write_unauthorized(&mut stream, &ctx);
            }
        }
    } else {
        ctx.superuser_role.clone()
    };

    let body_str = String::from_utf8_lossy(&req.body);
    let (sql, db) = extract_sql(&body_str);
    // Optional per-request consistency, mirroring POST /insert: reads at
    // "one" answer from the local replica (bounded, may lag a beat); writes
    // at "one" ack before the slowest replica. Default: the config levels.
    let consistency = match serde_json::from_str::<serde_json::Value>(&body_str)
        .ok()
        .as_ref()
        .and_then(|v| v.get("consistency"))
        .and_then(|v| v.as_str())
    {
        None => None,
        Some(c) => match c.to_ascii_lowercase().as_str() {
            "one" => Some(skaidb_proto::Consistency::One),
            "quorum" => Some(skaidb_proto::Consistency::Quorum),
            "all" => Some(skaidb_proto::Consistency::All),
            other => {
                return write_response(
                    &mut stream,
                    400,
                    &json!({"error": format!("bad consistency {other:?}")}),
                );
            }
        },
    };
    let response = match db {
        // A caller-supplied session database (the gateway itself is
        // stateless): `{"sql": "...", "db": "..."}`. Wrong names fail
        // exactly like `USE <db>` would.
        Some(db) => {
            let mut current_db = db;
            crate::shared::execute_session_as(&ctx, &role, &mut current_db, &sql, consistency)
        }
        None => execute_as(&ctx, &role, &sql),
    };
    // Row results stream as chunked JSON — one bounded buffer at a time, so
    // a big result never materializes a response-sized string (a multi-GB
    // /query serialization pinned a production node at its cgroup ceiling,
    // 2026-07-15; this also lifts the 64 MiB response cap for row results,
    // matching the binary protocol's QueryStream). Everything else keeps the
    // single-frame path.
    if let Response::Rows { columns, rows } = response {
        let sent = write_rows_chunked(&mut stream, &columns, &rows)?;
        ctx.metrics.add_bytes_returned(Endpoint::Rest, sent);
        return Ok(());
    }
    let (status, payload) = response_to_json(response);
    // Serialize the payload exactly once: the same string feeds both the
    // bytes-returned metric and the wire.
    let body = payload.to_string();
    ctx.metrics
        .add_bytes_returned(Endpoint::Rest, body.len() as u64);
    write_json_body(&mut stream, status, &body)
}

/// Stream `{"columns": [...], "rows": [...]}` as a chunked HTTP response,
/// serializing ~64 KiB at a time. Returns the body bytes written. The
/// socket's write timeout bounds each chunk, so a stalled reader aborts the
/// handler instead of pinning the buffers.
fn write_rows_chunked<W: io::Write>(
    stream: &mut W,
    columns: &[String],
    rows: &[Vec<skaidb_types::Value>],
) -> io::Result<u64> {
    const FLUSH_AT: usize = 64 * 1024;
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    )?;
    let mut sent: u64 = 0;
    let mut buf = String::with_capacity(FLUSH_AT + 8 * 1024);
    fn flush_chunk<W: io::Write>(stream: &mut W, buf: &mut String) -> io::Result<u64> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = buf.len() as u64;
        stream.write_all(format!("{:x}\r\n", buf.len()).as_bytes())?;
        stream.write_all(buf.as_bytes())?;
        stream.write_all(b"\r\n")?;
        buf.clear();
        Ok(n)
    }
    buf.push_str("{\"columns\":");
    buf.push_str(&serde_json::to_string(columns).unwrap_or_else(|_| "[]".into()));
    buf.push_str(",\"rows\":[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        let j = Json::Array(row.iter().map(|v| v.to_json()).collect());
        buf.push_str(&j.to_string());
        if buf.len() >= FLUSH_AT {
            sent += flush_chunk(stream, &mut buf)?;
        }
    }
    buf.push_str("]}");
    sent += flush_chunk(stream, &mut buf)?;
    stream.write_all(b"0\r\n\r\n")?;
    stream.flush()?;
    Ok(sent)
}

/// A parsed HTTP request (just the parts the gateway needs).
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    /// The `Host` header value (with any `:port`), for the HTTP→HTTPS redirect.
    host: Option<String>,
    body: Vec<u8>,
}

/// Parse the request line, headers, and body. Returns `None` on a clean EOF.
///
/// The gateway serves one request per connection (`Connection: close`), so a
/// per-call `BufReader` is already per-connection; borrowing the stream avoids
/// the `try_clone` (dup) syscall entirely.
/// The Prometheus-API path-prefix scope: `/db/<database>[/table/<table>]`
/// before `/api/v1/*`, stripped to `(scope, remaining path)`. Paths without
/// the prefix keep the default scope (the `metrics` table, default database).
fn prom_scope_of(path: &str) -> Result<(crate::promql::Scope, String), String> {
    let Some(rest) = path.strip_prefix("/db/") else {
        return Ok((crate::promql::Scope::default(), path.to_string()));
    };
    let ok_name = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    };
    let (db, rest) = rest
        .split_once('/')
        .ok_or("expected /db/<database>/api/v1/…")?;
    if !ok_name(db) {
        return Err("bad database name in /db/ prefix".into());
    }
    let (table, rest) = match rest.strip_prefix("table/") {
        Some(r) => {
            let (t, rr) = r.split_once('/').ok_or("expected /table/<table>/api/v1/…")?;
            if !ok_name(t) {
                return Err("bad table name in /table/ prefix".into());
            }
            (Some(t), rr)
        }
        None => (None, rest),
    };
    let rest = format!("/{rest}");
    if !rest.starts_with("/api/v1/") {
        return Err("the /db/ prefix scopes only the Prometheus /api/v1/* endpoints".into());
    }
    Ok((crate::promql::Scope::new(db, table), rest))
}

#[cfg(test)]
mod prom_scope_tests {
    use super::prom_scope_of;

    #[test]
    fn scope_prefix_parses_and_validates() {
        // No prefix → default scope, path untouched.
        let (s, p) = prom_scope_of("/api/v1/query").unwrap();
        assert_eq!((s.table.as_str(), s.field_metrics), ("metrics", false));
        assert_eq!(p, "/api/v1/query");
        // Database-only scope keeps the metrics-table semantics.
        let (s, p) = prom_scope_of("/db/telemetry/api/v1/query").unwrap();
        assert_eq!(s.db, "telemetry");
        assert_eq!(s.bare, "metrics");
        assert!(!s.field_metrics);
        assert_eq!(p, "/api/v1/query");
        // Table scope switches to field-name metrics.
        let (s, p) = prom_scope_of("/db/pi_air_quality/table/air_quality/api/v1/query_range")
            .unwrap();
        assert_eq!(s.db, "pi_air_quality");
        assert_eq!(s.bare, "air_quality");
        assert!(s.field_metrics);
        assert_eq!(p, "/api/v1/query_range");
        // An explicit `metrics` table scope keeps name-label semantics.
        let (s, _) = prom_scope_of("/db/telemetry/table/metrics/api/v1/query").unwrap();
        assert!(!s.field_metrics);
        // Malformed prefixes are clear errors.
        assert!(prom_scope_of("/db/x/status").is_err()); // not /api/v1/*
        assert!(prom_scope_of("/db//api/v1/query").is_err());
        assert!(prom_scope_of("/db/a b/api/v1/query").is_err());
        assert!(prom_scope_of("/db/x/table//api/v1/query").is_err());
        assert!(prom_scope_of("/db/x/table/../api/v1/query").is_err());
    }
}

/// Which activity class a REST request lands in (see `RestPath`).
fn classify_rest(method: &str, path: &str) -> RestPath {
    let bare = path.split('?').next().unwrap_or(path);
    match (method, bare) {
        (_, p) if p.starts_with("/query") => RestPath::Query,
        (_, p) if p.starts_with("/insert") => RestPath::Insert,
        (_, "/api/v1/write") => RestPath::Prom,
        (_, p) if p.starts_with("/api/v1/") => RestPath::Prom,
        (_, p) if p.starts_with("/db/") => RestPath::Prom,
        (_, p) if p == "/ui" || p.starts_with("/ui/") => RestPath::Ui,
        (_, "/metrics" | "/health" | "/healthz" | "/ready" | "/readyz" | "/status") => {
            RestPath::Ops
        }
        ("POST", p) if p.starts_with("/admin/") => RestPath::Admin,
        (_, p) if crate::es::path_is_es(p) => RestPath::Es,
        _ => RestPath::Other,
    }
}

fn read_request<R: io::Read>(stream: &mut R) -> io::Result<Option<HttpRequest>> {
    let mut reader = BufReader::new(&mut *stream);

    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut authorization = None;
    let mut host = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break; // end of headers
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            let value = value.trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            } else if key.eq_ignore_ascii_case("authorization") {
                authorization = Some(value.to_string());
            } else if key.eq_ignore_ascii_case("host") {
                host = Some(value.to_string());
            }
        }
    }

    // Cap before allocating: a huge (or hostile) Content-Length must not
    // reserve gigabytes up front. Surfaced to the client as 413.
    if content_length > MAX_BODY_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "request body over limit",
        ));
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    Ok(Some(HttpRequest {
        method,
        path,
        authorization,
        host,
        body,
    }))
}

/// Resolve a role from an `Authorization` header: SPNEGO (`Negotiate`, for
/// Kerberos browser/REST SSO) first, then HTTP `Basic`. `None` if neither
/// authenticates.
fn auth_role(ctx: &Shared, authorization: Option<&str>) -> Option<String> {
    if let Some(h) = authorization {
        if h.len() >= 10 && h[..10].eq_ignore_ascii_case("Negotiate ") {
            return negotiate_role(ctx, h);
        }
    }
    basic_auth_role(ctx, authorization)
}

/// Whether this server advertises/accepts SPNEGO: a `kerberos` build with
/// `auth.gssapi_enabled`.
fn gssapi_offered(ctx: &Shared) -> bool {
    cfg!(feature = "kerberos")
        && ctx
            .config
            .read()
            .map(|c| c.auth.gssapi_enabled)
            .unwrap_or(false)
}

/// Resolve a role from an `Authorization: Negotiate <base64 GSS token>` header
/// (RFC 4559 SPNEGO). Single-leg: Kerberos normally establishes the context in
/// one token, so a mechanism that asks for another round (NTLM-style) is
/// rejected rather than carried across stateless REST requests. The
/// authenticated principal maps to an external user's role (phase-2 seam).
#[cfg(feature = "kerberos")]
fn negotiate_role(ctx: &Shared, header: &str) -> Option<String> {
    if !gssapi_offered(ctx) {
        return None;
    }
    let b64 = header.get(10..)?.trim();
    let token = base64_decode(b64)?;
    let spn = {
        let c = ctx.config.read().ok()?;
        (!c.auth.gssapi_service_principal.is_empty())
            .then(|| c.auth.gssapi_service_principal.clone())
    };
    let handshake = skaidb_gssapi::ServerHandshake::new(spn.as_deref()).ok()?;
    match handshake.step(&token).ok()? {
        skaidb_gssapi::ServerStep::Done { principal, .. } => ctx.lookup_external_role(&principal),
        // Multi-leg negotiation isn't supported over stateless REST.
        skaidb_gssapi::ServerStep::Continue { .. } => None,
    }
}

#[cfg(not(feature = "kerberos"))]
fn negotiate_role(_ctx: &Shared, _header: &str) -> Option<String> {
    None
}

/// Resolve a role from an `Authorization: Basic ...` header, or `None`.
fn basic_auth_role(ctx: &Shared, authorization: Option<&str>) -> Option<String> {
    let header = authorization?;
    let b64 = header
        .strip_prefix("Basic ")
        .or_else(|| header.strip_prefix("basic "))?;
    let decoded = base64_decode(b64.trim())?;
    let creds = String::from_utf8(decoded).ok()?;
    let (user, pass) = creds.split_once(':')?;
    crate::authn::AuthState::verify_password(ctx.lookup_account(user).as_ref(), pass)
}

fn write_unauthorized<W: io::Write>(stream: &mut W, ctx: &Shared) -> io::Result<()> {
    let body = json!({"error": "authentication required"}).to_string();
    // Offer SPNEGO first (browsers/`curl --negotiate` pick it) when Kerberos is
    // on, then Basic so password clients still work — a client that can't do
    // Negotiate falls through to it.
    let challenge = if gssapi_offered(ctx) {
        "WWW-Authenticate: Negotiate\r\nWWW-Authenticate: Basic realm=\"skaidb\"\r\n"
    } else {
        "WWW-Authenticate: Basic realm=\"skaidb\"\r\n"
    };
    let head = format!(
        "HTTP/1.1 401 Unauthorized\r\n{challenge}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// Encode bytes as standard base64 (with `=` padding).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn base64_encode(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(A[(n >> 18 & 63) as usize] as char);
        out.push(A[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 { A[(n >> 6 & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { A[(n & 63) as usize] as char } else { '=' });
    }
    out
}

/// Decode standard base64 (no whitespace) into bytes.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &c in s.as_bytes() {
        let v = val(c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Handle `POST /insert`: `{"db"?: "...", "table": "...", "rows": [{...}]}`.
/// Each row is a JSON object; its fields become columns and its values —
/// including nested objects/arrays — become the stored document (via
/// `Value::from_json`, the same conversion the ES gateway uses). Rows with
/// the same set of columns are batched into one multi-row `INSERT` so a
/// homogeneous load is one replicated batch. Returns `{"inserted": n}`.
fn handle_insert(ctx: &Shared, role: &str, body: &[u8]) -> (u16, Json) {
    use skaidb_sql::ast::{Expr, Insert, Statement};
    use skaidb_types::Value;

    let parsed: Json = match serde_json::from_slice(body) {
        Ok(j) => j,
        Err(e) => return (400, json!({"error": format!("invalid JSON body: {e}")})),
    };
    let obj = match parsed.as_object() {
        Some(o) => o,
        None => return (400, json!({"error": "body must be a JSON object"})),
    };
    let table = match obj.get("table").and_then(|v| v.as_str()) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => return (400, json!({"error": "missing \"table\""})),
    };
    let db = match obj.get("db") {
        Some(Json::String(d)) if !d.is_empty() => d.clone(),
        _ => skaidb_engine::DEFAULT_DATABASE.to_string(),
    };
    let rows = match obj.get("rows").and_then(|v| v.as_array()) {
        Some(r) if !r.is_empty() => r,
        Some(_) => return (200, json!({"inserted": 0})),
        None => return (400, json!({"error": "missing \"rows\" array"})),
    };
    // Optional per-request write consistency ("one" | "quorum" | "all").
    // Bulk loaders use ONE so the ack never waits on the slowest replica's
    // flush/compaction window; replication still reaches every replica via
    // the async tail, with hints + anti-entropy as the backstop.
    let consistency = match obj.get("consistency").and_then(|v| v.as_str()) {
        None => None,
        Some(c) => match c.to_ascii_lowercase().as_str() {
            "one" => Some(skaidb_proto::Consistency::One),
            "quorum" => Some(skaidb_proto::Consistency::Quorum),
            "all" => Some(skaidb_proto::Consistency::All),
            other => {
                return (400, json!({"error": format!("bad consistency {other:?}")}))
            }
        },
    };

    // Group rows by their (sorted) column set so a homogeneous batch becomes
    // one multi-row INSERT; heterogeneous docs fall into separate groups.
    // First-seen order is preserved for determinism.
    let mut groups: Vec<(Vec<String>, Vec<Vec<Expr>>)> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let Some(fields) = row.as_object() else {
            return (400, json!({"error": format!("row {i} is not a JSON object")}));
        };
        let mut cols: Vec<String> = fields.keys().cloned().collect();
        cols.sort();
        let values: Vec<Expr> = cols
            .iter()
            .map(|c| Expr::Literal(Value::from_json(fields[c].clone())))
            .collect();
        match groups.iter_mut().find(|(c, _)| *c == cols) {
            Some((_, rows)) => rows.push(values),
            None => groups.push((cols, vec![values])),
        }
    }

    let mut inserted = 0usize;
    for (columns, group_rows) in groups {
        let n = group_rows.len();
        let stmt = Statement::Insert(Insert {
            table: table.clone(),
            columns,
            rows: group_rows,
        });
        let mut current_db = db.clone();
        let resp = crate::shared::execute_session_statement_as(
            ctx,
            role,
            &mut current_db,
            "INSERT (JSON)",
            Ok(stmt),
            consistency,
        );
        match resp {
            Response::Mutation { .. } | Response::Ddl => inserted += n,
            Response::Error(e) => {
                let status = if e.contains("permission denied") { 403 } else { 400 };
                return (
                    status,
                    json!({"error": e, "inserted": inserted}),
                );
            }
            other => {
                return (
                    500,
                    json!({"error": format!("unexpected response: {other:?}"), "inserted": inserted}),
                );
            }
        }
    }
    (200, json!({"inserted": inserted}))
}

/// Accept either a raw SQL body or `{"sql": "..."}`.
fn extract_sql(body: &str) -> (String, Option<String>) {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        if let Ok(Json::Object(map)) = serde_json::from_str::<Json>(trimmed) {
            if let Some(Json::String(sql)) = map.get("sql") {
                let db = match map.get("db") {
                    Some(Json::String(db)) if !db.is_empty() => Some(db.clone()),
                    _ => None,
                };
                return (sql.clone(), db);
            }
        }
    }
    (trimmed.to_string(), None)
}

/// A low-privilege, unauthenticated topology snapshot — ring/epoch/members,
/// the default consistency levels, and the members' client (SQL) endpoints so a
/// client that reached one seed can discover its peers for failover. Carries no
/// credentials or data, so it can be handed to a monitoring scraper.
/// The effective client-facing TLS mode (`off`/`opportunistic`/`required`)
/// from config, for `/status`.
fn client_tls_mode(ctx: &Shared) -> &'static str {
    use skaidb_config::ClientTlsMode;
    match ctx.config_snapshot().encryption.client_tls {
        ClientTlsMode::Off => "off",
        ClientTlsMode::Opportunistic => "opportunistic",
        ClientTlsMode::Required => "required",
    }
}

/// Whether at-rest encryption is on (`on`/`off`), from config, for `/status`.
fn at_rest_mode(ctx: &Shared) -> &'static str {
    if ctx.config_snapshot().encryption.at_rest_enabled {
        "on"
    } else {
        "off"
    }
}

fn status_json(ctx: &Shared) -> Json {
    match ctx.backend.cluster_stats() {
        Some(c) => {
            let quic_port = ctx.config.read().map(|cfg| cfg.server.quic_port).unwrap_or(0);
            // Configured-vs-live discrepancies (no liveness probe here — that's the
            // authenticated /admin/status). Surfaces a node that half-joined (it is
            // catching up data but was never admitted to the ring) or a configured
            // seed that never joined.
            let configured_not_in_ring: Vec<&str> = c
                .peers
                .iter()
                .filter(|p| p.in_config && !p.in_ring)
                .map(|p| p.id.as_str())
                .collect();
            let ring_not_configured: Vec<&str> = c
                .peers
                .iter()
                .filter(|p| p.in_ring && !p.in_config)
                .map(|p| p.id.as_str())
                .collect();
            json!({
                "clustered": true,
                "node_id": c.node_id,
                // Human identity: `<cluster>.<function>.<alias>` (dotted
                // form; empty strings until the naming bootstrap runs).
                "cluster_name": crate::naming::cluster_name(ctx).unwrap_or_default(),
                "node_aliases": crate::naming::all_aliases(ctx)
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
                "epoch": c.epoch,
                "members": c.members,
                // Client SQL endpoints (host:quic_port) of every member.
                "endpoints": ctx.backend.member_client_endpoints(quic_port),
                // Client endpoints of members currently resyncing — a driver
                // removes these from `endpoints` so it never coordinates
                // through a backfilling node (best-effort; correctness is
                // guaranteed server-side regardless).
                "resyncing_endpoints": ctx.backend.resyncing_client_endpoints(quic_port),
                "replication_factor": c.replication_factor,
                "resharding": c.resharding_active,
                "hints_pending": c.hints_pending,
                // Membership as configured (seeds) vs. as actually live (the ring).
                "configured": c.configured,
                "self_in_ring": c.self_in_ring,
                "configured_not_in_ring": configured_not_in_ring,
                "ring_not_configured": ring_not_configured,
                // Per-peer replication status so the UI can show how far behind
                // each node is: `hints_pending` is the exact backlog of writes
                // buffered for that peer, `lag_ms` the gap between the last write
                // this node coordinated and the latest one the peer has confirmed
                // applied (null until a write is confirmed to it).
                "peers": c
                    .peers
                    .iter()
                    .map(|p| {
                        json!({
                            "id": p.id,
                            "in_ring": p.in_ring,
                            "in_config": p.in_config,
                            "hints_pending": p.hints_pending,
                            "lag_ms": p.lag_ms,
                        })
                    })
                    .collect::<Vec<_>>(),
                "read_consistency": c.read_consistency,
                "write_consistency": c.write_consistency,
                // Security posture — surfaced so a plaintext/unauthenticated
                // internode or client port (a silent misconfiguration) is
                // visible. `at_rest` joins these when it lands.
                "internode_auth": ctx.backend.internode_auth_mode().unwrap_or("none"),
                "client_tls": client_tls_mode(ctx),
                "at_rest": at_rest_mode(ctx),
                "ready": ctx.backend.is_ready(),
                // Resync (wipe+backfill) state of THIS node: a resyncing node
                // holds incomplete data and should be routed around by drivers
                // until `resync_progress` reaches 1.0 (filesize-based).
                "resyncing": c.resyncing,
                "resync_progress": c.resync_progress,
                "resync_source": c.resync_source,
            })
        }
        None => json!({ "clustered": false, "client_tls": client_tls_mode(ctx), "at_rest": at_rest_mode(ctx), "ready": ctx.backend.is_ready() }),
    }
}

/// Rough serialized-JSON size of a value, for bounding a response without
/// serializing it twice. Overestimates slightly (escape/base64 headroom).
fn approx_json_len(v: &skaidb_types::Value) -> usize {
    use skaidb_types::Value;
    match v {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Int(_) | Value::Float(_) | Value::Timestamp(_) => 20,
        Value::Decimal(_) => 32,
        Value::Uuid(_) => 40,
        Value::String(s) => s.len() + 8,
        Value::Bytes(b) => b.len() * 4 / 3 + 8,
        Value::Array(items) => 2 + items.len() + items.iter().map(approx_json_len).sum::<usize>(),
        Value::Document(d) => {
            2 + d
                .0
                .iter()
                .map(|(k, v)| k.len() + 4 + approx_json_len(v))
                .sum::<usize>()
        }
    }
}

fn response_to_json(response: Response) -> (u16, Json) {
    match response {
        Response::Rows { columns, rows } => {
            // Bound the materialized result like the binary protocol bounds
            // its frames: past the cap, answer with guidance instead of
            // ballooning the heap (a multi-GB `/query` serialization pinned a
            // production node at its cgroup ceiling, 2026-07-15).
            let mut approx = 0usize;
            for row in &rows {
                approx += 2 + row.iter().map(approx_json_len).sum::<usize>();
                if approx > MAX_BODY_LEN {
                    return (
                        400,
                        json!({ "error": format!(
                            "result set exceeds the {} MiB response limit; add LIMIT or use the \
                             binary protocol's streaming query",
                            MAX_BODY_LEN / (1024 * 1024)
                        )}),
                    );
                }
            }
            let rows: Vec<Json> = rows
                .into_iter()
                .map(|row| Json::Array(row.iter().map(|v| v.to_json()).collect()))
                .collect();
            (200, json!({ "columns": columns, "rows": rows }))
        }
        Response::Mutation { affected } => (200, json!({ "affected": affected })),
        Response::Ddl => (200, json!({ "ok": true })),
        Response::Error(msg) => (400, json!({ "error": msg })),
        // Prepared statements and streamed results are binary-protocol
        // features; the REST path never issues a Prepare or QueryStream, so
        // these are unreachable there.
        Response::Prepared { id, params } => (200, json!({ "prepared": id, "params": params })),
        Response::RowsHeader { .. } | Response::RowsChunk { .. } | Response::RowsEnd => {
            (500, json!({ "error": "streamed response on REST path" }))
        }
    }
}

fn write_response<W: io::Write>(stream: &mut W, status: u16, body: &Json) -> io::Result<()> {
    write_json_body(stream, status, &body.to_string())
}

/// Write an already-serialized JSON body (callers that also meter the body
/// size pass the one serialization here instead of re-serializing).
fn write_json_body<W: io::Write>(stream: &mut W, status: u16, body: &str) -> io::Result<()> {
    let reason = http_reason(status);
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// Write a 308 Permanent Redirect to `location` (the HTTP→HTTPS bounce). 308
/// preserves the method and body, so a redirected POST /query still works.
fn write_redirect<W: io::Write>(stream: &mut W, location: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 308 {}\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        http_reason(308)
    );
    stream.write_all(head.as_bytes())?;
    stream.flush()
}

/// Write a plain-text response (used by `/metrics`, `/health`, `/ready`).
fn write_text<W: io::Write>(stream: &mut W, status: u16, body: &str) -> io::Result<()> {
    let reason = http_reason(status);
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// Write an embedded UI asset with the UI's Content-Security-Policy header.
fn write_asset<W: io::Write>(stream: &mut W, asset: &crate::ui::Asset) -> io::Result<()> {
    let reason = http_reason(asset.status);
    // `no-cache` = the browser may keep a copy but must revalidate before use,
    // so a server upgrade always serves the new UI instead of a stale cached
    // bundle (the assets are embedded and change per build, and carry no
    // ETag/version in their URL). They are tiny, so re-fetching is cheap.
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nContent-Security-Policy: {}\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        asset.status,
        asset.content_type,
        asset.body.len(),
        crate::ui::CSP,
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(asset.body.as_bytes())?;
    stream.flush()
}

/// The HTTP reason phrase for a status code used by this gateway.
fn http_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_types::Value;

    /// A result set past the cap answers with guidance instead of
    /// materializing; small ones convert normally.
    #[test]
    fn rows_response_is_bounded() {
        let small = Response::Rows {
            columns: vec!["v".into()],
            rows: vec![vec![Value::String("x".repeat(100))]],
        };
        let (status, body) = response_to_json(small);
        assert_eq!(status, 200);
        assert!(body["rows"].is_array());

        // 70 rows × ~1 MiB of string comfortably exceeds the 64 MiB cap.
        let big = Response::Rows {
            columns: vec!["v".into()],
            rows: (0..70)
                .map(|_| vec![Value::String("y".repeat(1024 * 1024))])
                .collect(),
        };
        let (status, body) = response_to_json(big);
        assert_eq!(status, 400);
        let msg = body["error"].as_str().unwrap();
        assert!(msg.contains("response limit"), "{msg}");
        assert!(msg.contains("LIMIT"), "{msg}");
    }

    /// base64 encode/decode round-trip (the SPNEGO `Negotiate` token codec and
    /// HTTP Basic share these); padding and every 3-byte alignment.
    #[test]
    fn base64_roundtrip() {
        for case in [
            &b""[..],
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            &[0u8, 255, 128, 1, 2, 3, 254],
        ] {
            let enc = base64_encode(case);
            assert_eq!(base64_decode(&enc).as_deref(), Some(case), "roundtrip {enc}");
        }
        // Known vector.
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"M"), "TQ==");
    }

    /// The size estimate covers every value shape and scales with payload.
    #[test]
    fn approx_json_len_scales() {
        assert!(approx_json_len(&Value::Null) < 10);
        assert!(approx_json_len(&Value::String("abc".into())) >= 3);
        let big = Value::Array(vec![Value::String("z".repeat(1000)); 10]);
        assert!(approx_json_len(&big) >= 10_000);
        let mut d = skaidb_types::Document::new();
        d.insert("k", Value::Bytes(vec![0u8; 3000]));
        assert!(approx_json_len(&Value::Document(d)) >= 4000);
    }
}
