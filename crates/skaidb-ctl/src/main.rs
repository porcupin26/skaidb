//! `skaidbctl` — cluster admin client for skaidb.
//!
//! Talks to a node's REST endpoint (`POST /admin/*`) to inspect and change
//! cluster membership: status, add-node, remove-node, repair, reclaim. A thin,
//! dependency-light HTTP/1.1 client (matching the server's hand-rolled REST).

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use serde_json::Value as Json;

#[derive(Parser, Debug)]
#[command(name = "skaidbctl", version, about = "skaidb cluster admin client")]
struct Cli {
    /// Node REST endpoint to talk to (`host:rest_port`).
    #[arg(long, default_value = "127.0.0.1:7080", env = "SKAIDB_ADDR")]
    addr: String,

    /// Username for HTTP Basic auth (if the server requires auth).
    #[arg(long, env = "SKAIDB_USER")]
    user: Option<String>,

    /// Password for HTTP Basic auth.
    #[arg(long, env = "SKAIDB_PASSWORD")]
    password: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Show cluster membership and topology.
    Status,
    /// Add a node to the cluster and migrate it its share of the keyspace.
    AddNode {
        /// The joining node's internode address (`host:internode_port`).
        addr: String,
    },
    /// Gracefully decommission a node (drains its keys, then removes it).
    RemoveNode {
        /// The leaving node's id (`host:internode_port`).
        id: String,
    },
    /// Run a cluster-wide anti-entropy repair pass.
    Repair,
    /// Reclaim disk space for keys former owners no longer own.
    Reclaim,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let (path, body) = match &cli.cmd {
        Cmd::Status => ("/admin/status", String::new()),
        Cmd::Repair => ("/admin/repair", String::new()),
        Cmd::Reclaim => ("/admin/reclaim", String::new()),
        Cmd::AddNode { addr } => ("/admin/add-node", format!(r#"{{"addr":"{addr}"}}"#)),
        Cmd::RemoveNode { id } => ("/admin/remove-node", format!(r#"{{"id":"{id}"}}"#)),
    };

    let auth = match (&cli.user, &cli.password) {
        (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
        _ => None,
    };

    match post(&cli.addr, path, &body, auth) {
        Ok((status, resp)) => {
            // Pretty-print JSON when possible, else echo the raw body.
            match serde_json::from_str::<Json>(&resp) {
                Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or(resp)),
                Err(_) => println!("{resp}"),
            }
            if status < 400 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("skaidbctl: request to {} failed: {e}", cli.addr);
            ExitCode::FAILURE
        }
    }
}

/// Minimal HTTP/1.1 `POST` returning `(status_code, body)`.
fn post(addr: &str, path: &str, body: &str, auth: Option<(&str, &str)>) -> io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(300)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;

    let mut head = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
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

    // Split status line + headers from the body.
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

/// Standard base64 (for the `Authorization: Basic` header).
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
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
