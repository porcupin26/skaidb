//! REST/HTTP endpoint (SPEC §7) — a thin gateway over the same query engine.
//!
//! `POST /query` with the SQL as the request body (plain text, or JSON
//! `{"sql": "..."}`). The response is JSON: a result set, an affected count, or
//! an error. A minimal HTTP/1.1 implementation keeps the dependency surface
//! small; it serves one request per connection (`Connection: close`).

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use serde_json::{json, Value as Json};
use skaidb_proto::Response;

use crate::metrics::Endpoint;
use crate::shared::{collect_runtime_metrics, execute_as, Shared};

/// Bind the REST endpoint and serve it on a background thread.
pub fn spawn(addr: &str, ctx: Shared) -> io::Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let handle = thread::spawn(move || serve(listener, ctx));
    Ok((local, handle))
}

/// Accept connections forever, handling each on its own thread.
pub fn serve(listener: TcpListener, ctx: Shared) {
    for stream in listener.incoming().flatten() {
        let ctx = ctx.clone();
        thread::spawn(move || {
            ctx.metrics.connection_opened(Endpoint::Rest);
            let _ = handle_connection(stream, ctx.clone());
            ctx.metrics.connection_closed(Endpoint::Rest);
        });
    }
}

fn handle_connection(mut stream: TcpStream, ctx: Shared) -> io::Result<()> {
    let req = match read_request(&mut stream) {
        Ok(Some(req)) => req,
        Ok(None) => return Ok(()),
        Err(_) => {
            return write_response(&mut stream, 400, &json!({"error": "malformed request"}));
        }
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
                    match basic_auth_role(&ctx, req.authorization.as_deref()) {
                        Some(role) => role,
                        None => return write_unauthorized(&mut stream),
                    }
                } else {
                    ctx.superuser_role.clone()
                };
                let (status, body) = crate::ui::schema_json(&ctx, &role);
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
            match basic_auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream),
            }
        } else {
            ctx.superuser_role.clone()
        };
        let (status, payload) = crate::admin::handle(&ctx, &role, cmd);
        return write_response(&mut stream, status, &payload);
    }

    // Prometheus HTTP query API (GET with a query string, or POST with a
    // form body — Grafana uses both).
    let bare_path = req.path.split('?').next().unwrap_or(&req.path).to_string();
    if bare_path.starts_with("/api/v1/") && bare_path != "/api/v1/write" {
        let role = if ctx.authn.required {
            match basic_auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream),
            }
        } else {
            ctx.superuser_role.clone()
        };
        // Reading metrics requires Select on the ingest table (a grant on
        // its database also satisfies it).
        if !ctx.allowed_on_table(
            &role,
            skaidb_auth::Privilege::Select,
            "metrics",
            skaidb_engine::DEFAULT_DATABASE,
        ) {
            return write_response(
                &mut stream,
                403,
                &json!({"status": "error", "error": "permission denied: Select on metrics"}),
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
        let (status, payload) = match bare_path.as_str() {
            "/api/v1/query" => crate::promql::query(&ctx, &params),
            "/api/v1/query_range" => crate::promql::query_range(&ctx, &params),
            "/api/v1/labels" => crate::promql::labels(&ctx, None),
            "/api/v1/series" => crate::promql::series(&ctx, &params),
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
                    crate::promql::labels(&ctx, Some(label))
                } else {
                    (404, json!({"status": "error", "error": "unknown api route"}))
                }
            }
        };
        return write_response(&mut stream, status, &payload);
    }

    // Prometheus remote_write: snappy-compressed protobuf WriteRequest.
    if req.method == "POST" && req.path == "/api/v1/write" {
        let role = if ctx.authn.required {
            match basic_auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream),
            }
        } else {
            ctx.superuser_role.clone()
        };
        return match crate::promwrite::ingest(&ctx, &role, &req.body) {
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
            match basic_auth_role(&ctx, req.authorization.as_deref()) {
                Some(role) => role,
                None => return write_unauthorized(&mut stream),
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

    if req.method != "POST" || !req.path.starts_with("/query") {
        return write_response(
            &mut stream,
            404,
            &json!({"error": "use POST /query with a SQL body, or GET /metrics"}),
        );
    }

    // Resolve the role: HTTP Basic auth when required, else anonymous superuser.
    let role = if ctx.authn.required {
        match basic_auth_role(&ctx, req.authorization.as_deref()) {
            Some(role) => role,
            None => {
                return write_unauthorized(&mut stream);
            }
        }
    } else {
        ctx.superuser_role.clone()
    };

    let (sql, db) = extract_sql(&String::from_utf8_lossy(&req.body));
    let response = match db {
        // A caller-supplied session database (the gateway itself is
        // stateless): `{"sql": "...", "db": "..."}`. Wrong names fail
        // exactly like `USE <db>` would.
        Some(db) => {
            let mut current_db = db;
            crate::shared::execute_session_as(&ctx, &role, &mut current_db, &sql, None)
        }
        None => execute_as(&ctx, &role, &sql),
    };
    let (status, payload) = response_to_json(response);
    // Serialize the payload exactly once: the same string feeds both the
    // bytes-returned metric and the wire.
    let body = payload.to_string();
    ctx.metrics
        .add_bytes_returned(Endpoint::Rest, body.len() as u64);
    write_json_body(&mut stream, status, &body)
}

/// A parsed HTTP request (just the parts the gateway needs).
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Vec<u8>,
}

/// Parse the request line, headers, and body. Returns `None` on a clean EOF.
///
/// The gateway serves one request per connection (`Connection: close`), so a
/// per-call `BufReader` is already per-connection; borrowing the stream avoids
/// the `try_clone` (dup) syscall entirely.
fn read_request(stream: &mut TcpStream) -> io::Result<Option<HttpRequest>> {
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
            }
        }
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    Ok(Some(HttpRequest {
        method,
        path,
        authorization,
        body,
    }))
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

fn write_unauthorized(stream: &mut TcpStream) -> io::Result<()> {
    let body = json!({"error": "authentication required"}).to_string();
    let head = format!(
        "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"skaidb\"\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
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
                "epoch": c.epoch,
                "members": c.members,
                // Client SQL endpoints (host:quic_port) of every member.
                "endpoints": ctx.backend.member_client_endpoints(quic_port),
                "replication_factor": c.replication_factor,
                "resharding": c.resharding_active,
                "hints_pending": c.hints_pending,
                // Membership as configured (seeds) vs. as actually live (the ring).
                "configured": c.configured,
                "self_in_ring": c.self_in_ring,
                "configured_not_in_ring": configured_not_in_ring,
                "ring_not_configured": ring_not_configured,
                "read_consistency": c.read_consistency,
                "write_consistency": c.write_consistency,
                "ready": ctx.backend.is_ready(),
            })
        }
        None => json!({ "clustered": false, "ready": ctx.backend.is_ready() }),
    }
}

fn response_to_json(response: Response) -> (u16, Json) {
    match response {
        Response::Rows { columns, rows } => {
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

fn write_response(stream: &mut TcpStream, status: u16, body: &Json) -> io::Result<()> {
    write_json_body(stream, status, &body.to_string())
}

/// Write an already-serialized JSON body (callers that also meter the body
/// size pass the one serialization here instead of re-serializing).
fn write_json_body(stream: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
    let reason = http_reason(status);
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// Write a plain-text response (used by `/metrics`, `/health`, `/ready`).
fn write_text(stream: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
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
fn write_asset(stream: &mut TcpStream, asset: &crate::ui::Asset) -> io::Result<()> {
    let reason = http_reason(asset.status);
    let head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nContent-Security-Policy: {}\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
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
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Error",
    }
}
