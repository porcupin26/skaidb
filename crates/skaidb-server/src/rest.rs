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

use crate::shared::{execute, SharedDb};

/// Bind the REST endpoint and serve it on a background thread.
pub fn spawn(addr: &str, db: SharedDb) -> io::Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let handle = thread::spawn(move || serve(listener, db));
    Ok((local, handle))
}

/// Accept connections forever, handling each on its own thread.
pub fn serve(listener: TcpListener, db: SharedDb) {
    for stream in listener.incoming().flatten() {
        let db = db.clone();
        thread::spawn(move || {
            let _ = handle_connection(stream, db);
        });
    }
}

fn handle_connection(mut stream: TcpStream, db: SharedDb) -> io::Result<()> {
    let (method, path, body) = match read_request(&mut stream) {
        Ok(Some(req)) => req,
        Ok(None) => return Ok(()),
        Err(_) => {
            return write_response(&mut stream, 400, &json!({"error": "malformed request"}));
        }
    };

    if method != "POST" || !path.starts_with("/query") {
        return write_response(
            &mut stream,
            404,
            &json!({"error": "use POST /query with a SQL body"}),
        );
    }

    let sql = extract_sql(&body);
    let response = execute(&db, &sql);
    let (status, payload) = response_to_json(response);
    write_response(&mut stream, status, &payload)
}

/// Parse the request line, headers, and body. Returns `None` on a clean EOF.
fn read_request(stream: &mut TcpStream) -> io::Result<Option<(String, String, String)>> {
    let mut reader = BufReader::new(stream.try_clone()?);

    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break; // end of headers
        }
        if let Some(value) = trimmed
            .split_once(':')
            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .map(|(_, v)| v.trim())
        {
            content_length = value.parse().unwrap_or(0);
        }
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    let body = String::from_utf8(body).unwrap_or_default();
    Ok(Some((method, path, body)))
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
