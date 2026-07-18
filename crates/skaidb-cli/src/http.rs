//! Minimal HTTP/1.1 client for the REST control plane (`/status`, `/metrics`,
//! `/admin/*`). Dependency-light, matching the server's hand-rolled REST. Every
//! call tries the configured endpoints in order so admin/status commands keep
//! working when one node is down — the same redundancy the SQL driver gives.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

/// Client-side TLS for the REST control plane (mirrors the SQL driver's).
#[derive(Clone)]
pub struct TlsClient {
    pub cfg: Arc<rustls::ClientConfig>,
    pub server_name: String,
}

/// A read+write byte stream, so `request` can hold either plaintext or TLS.
trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

/// Basic-auth credentials for authenticated (`/admin/*`) routes.
pub type Auth<'a> = Option<(&'a str, &'a str)>;

/// `POST path` with `body` to the first reachable endpoint. Returns
/// `(status, body)` or the last connection error if none answered.
pub fn post(
    endpoints: &[String],
    path: &str,
    body: &str,
    auth: Auth,
    tls: Option<&TlsClient>,
) -> io::Result<(u16, String)> {
    try_each(endpoints, |addr| request(addr, "POST", path, body, auth, tls))
}

/// `GET path` from the first reachable endpoint.
pub fn get(
    endpoints: &[String],
    path: &str,
    auth: Auth,
    tls: Option<&TlsClient>,
) -> io::Result<(u16, String)> {
    try_each(endpoints, |addr| request(addr, "GET", path, "", auth, tls))
}

/// Run `f` against each endpoint until one succeeds; return the last error.
fn try_each(
    endpoints: &[String],
    mut f: impl FnMut(&str) -> io::Result<(u16, String)>,
) -> io::Result<(u16, String)> {
    let mut last = io::Error::other("no endpoints given");
    for addr in endpoints {
        match f(addr) {
            Ok(v) => return Ok(v),
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// One HTTP/1.1 request returning `(status_code, body)`.
fn request(
    addr: &str,
    method: &str,
    path: &str,
    body: &str,
    auth: Auth,
    tls: Option<&TlsClient>,
) -> io::Result<(u16, String)> {
    let tcp = TcpStream::connect(addr)?;
    tcp.set_read_timeout(Some(Duration::from_secs(300)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut stream: Box<dyn ReadWrite> = match tls {
        Some(t) => Box::new(skaidb_net::connect_tls(tcp, t.cfg.clone(), &t.server_name)?),
        None => Box::new(tcp),
    };

    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some((user, pass)) = auth {
        let token = base64_encode(format!("{user}:{pass}").as_bytes());
        head.push_str(&format!("Authorization: Basic {token}\r\n"));
    }
    head.push_str("\r\n");

    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw);

    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no HTTP status line"))?;
    let resp_body = match text.split_once("\r\n\r\n") {
        Some((_, b)) => b.to_string(),
        None => String::new(),
    };
    Ok((status, resp_body))
}

/// Pretty-print a JSON body if it parses, else echo it verbatim.
pub fn print_body(body: &str) {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or_else(|_| body.into())),
        Err(_) => println!("{body}"),
    }
}

/// Standard base64 (for the `Authorization: Basic` header).
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { TABLE[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[(n & 63) as usize] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::base64_encode;

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"admin:secret"), "YWRtaW46c2VjcmV0");
    }
}
