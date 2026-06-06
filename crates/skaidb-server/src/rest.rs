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

use crate::shared::{execute_as, Shared};

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
            let _ = handle_connection(stream, ctx);
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

    // Prometheus scrape endpoint stays open for scraping (SPEC §10).
    if req.method == "GET" && req.path.starts_with("/metrics") {
        return write_text(&mut stream, 200, &ctx.metrics.render());
    }

    // Cluster control plane: POST /admin/* (RBAC-gated inside admin::handle).
    if req.method == "POST" && req.path.starts_with("/admin/") {
        let Some(cmd) = crate::admin::parse(&req.path, &req.body) else {
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

    let sql = extract_sql(&req.body);
    let response = execute_as(&ctx, &role, &sql);
    let (status, payload) = response_to_json(response);
    write_response(&mut stream, status, &payload)
}

/// A parsed HTTP request (just the parts the gateway needs).
struct HttpRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    body: String,
}

/// Parse the request line, headers, and body. Returns `None` on a clean EOF.
fn read_request(stream: &mut TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut reader = BufReader::new(stream.try_clone()?);

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
    let body = String::from_utf8(body).unwrap_or_default();
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
    ctx.authn.verify_password(user, pass)
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
fn extract_sql(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.starts_with('{') {
        if let Ok(Json::Object(map)) = serde_json::from_str::<Json>(trimmed) {
            if let Some(Json::String(sql)) = map.get("sql") {
                return sql.clone();
            }
        }
    }
    trimmed.to_string()
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
    }
}

fn write_response(stream: &mut TcpStream, status: u16, body: &Json) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Error",
    };
    let body = body.to_string();
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

/// Write a plain-text response (used by `/metrics`).
fn write_text(stream: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}
