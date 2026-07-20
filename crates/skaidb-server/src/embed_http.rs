//! HTTP embeddings client — the server-side [`skaidb_engine::Embedder`] for
//! managed (`EMBED`) vector indexes. Calls an OpenAI/TEI-compatible endpoint:
//!
//! ```text
//! POST <url>  {"model": <model>, "input": [<texts>]}
//!   -> {"data": [{"embedding": [f32, ...]}, ...]}
//! ```
//!
//! Dependency-light (the same hand-rolled HTTP/1.1 as the REST control plane):
//! `Content-Length` + `Connection: close` responses only (what OpenAI and TEI
//! send). Invoked only off the write/read hot path.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use skaidb_config::InferenceConfig;
use skaidb_engine::{Embedder, EngineError};

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

fn err(msg: impl Into<String>) -> EngineError {
    EngineError::Unsupported(format!("inference: {}", msg.into()))
}

/// An embeddings endpoint reachable over HTTP(S).
#[derive(Debug)]
pub struct HttpEmbedder {
    tls: Option<Arc<rustls::ClientConfig>>, // Some = https
    host: String,
    port: u16,
    path: String,
    model: String,
    dim: usize,
    api_key: String,
    timeout: Duration,
}

impl HttpEmbedder {
    /// Build from `[inference]`. Errors on a malformed URL or TLS material.
    pub fn from_config(c: &InferenceConfig) -> Result<Self, String> {
        let (scheme, rest) = c
            .url
            .split_once("://")
            .ok_or("inference.url must be http://… or https://…")?;
        let (hostport, path) = match rest.split_once('/') {
            Some((hp, p)) => (hp, format!("/{p}")),
            None => (rest, "/".to_string()),
        };
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse::<u16>().map_err(|_| "bad port in inference.url")?,
            ),
            None => (
                hostport.to_string(),
                if scheme == "https" { 443 } else { 80 },
            ),
        };
        if host.is_empty() {
            return Err("inference.url has no host".into());
        }
        let tls = if scheme == "https" {
            let verify = match c.tls_verify.as_str() {
                "insecure" => skaidb_net::ClientVerify::Insecure,
                "system" => skaidb_net::ClientVerify::WebpkiDefaults,
                _ => skaidb_net::ClientVerify::CaFile(c.tls_ca.clone()), // "ca"
            };
            Some(skaidb_net::client_config(verify, None)?)
        } else {
            None
        };
        Ok(HttpEmbedder {
            tls,
            host,
            port,
            path,
            model: c.model.clone(),
            dim: c.dim as usize,
            api_key: c.api_key.clone(),
            timeout: Duration::from_secs(c.timeout_secs.max(1)),
        })
    }
}

impl Embedder for HttpEmbedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EngineError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let body = serde_json::json!({ "model": self.model, "input": texts }).to_string();
        let addr = format!("{}:{}", self.host, self.port);
        let tcp = TcpStream::connect(&addr).map_err(|e| err(format!("connect {addr}: {e}")))?;
        tcp.set_read_timeout(Some(self.timeout)).ok();
        tcp.set_write_timeout(Some(self.timeout)).ok();
        let mut stream: Box<dyn ReadWrite> = match &self.tls {
            Some(cfg) => Box::new(
                skaidb_net::connect_tls(tcp, cfg.clone(), &self.host)
                    .map_err(|e| err(format!("tls handshake: {e}")))?,
            ),
            None => Box::new(tcp),
        };
        let mut head = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n",
            self.path,
            addr,
            body.len()
        );
        if !self.api_key.is_empty() {
            head.push_str(&format!("Authorization: Bearer {}\r\n", self.api_key));
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).map_err(|e| err(e.to_string()))?;
        stream.write_all(body.as_bytes()).map_err(|e| err(e.to_string()))?;
        stream.flush().map_err(|e| err(e.to_string()))?;

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).map_err(|e| err(e.to_string()))?;
        let text = String::from_utf8_lossy(&raw);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|c| c.parse::<u16>().ok())
            .ok_or_else(|| err("no HTTP status line"))?;
        let resp = text.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("");
        if status != 200 {
            let snippet: String = resp.chars().take(200).collect();
            return Err(err(format!("embeddings endpoint returned HTTP {status}: {snippet}")));
        }
        let json: serde_json::Value =
            serde_json::from_str(resp.trim()).map_err(|e| err(format!("bad JSON response: {e}")))?;
        let data = json
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| err("response has no `data` array"))?;
        let mut out = Vec::with_capacity(data.len());
        for item in data {
            let emb = item
                .get("embedding")
                .and_then(|e| e.as_array())
                .ok_or_else(|| err("a `data` item has no `embedding` array"))?;
            out.push(emb.iter().map(|x| x.as_f64().unwrap_or(0.0) as f32).collect());
        }
        if out.len() != texts.len() {
            return Err(err(format!(
                "asked to embed {} texts, got {} vectors",
                texts.len(),
                out.len()
            )));
        }
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.dim
    }
}
