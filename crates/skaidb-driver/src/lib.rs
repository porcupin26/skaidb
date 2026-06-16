//! skaidb client driver (SPEC §7).
//!
//! A synchronous client over the binary fast-path protocol ([`skaidb_proto`]).
//! A [`Client`] holds one or more candidate endpoints: on connect it prefers the
//! **nearest reachable** node (lowest TCP-connect latency) and keeps the rest as
//! failover targets. If the live connection drops mid-session, [`Client::execute`]
//! transparently reconnects to another endpoint and retries once, so a single
//! node dying does not take the session down when peers are available.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use skaidb_auth::scram;
use skaidb_proto::{
    auth_message, read_frame, write_frame, AuthChallenge, AuthFinish, AuthOutcome, AuthStart,
    Consistency, ProtoError, Request, Response,
};

/// How long to wait for the latency probe TCP connect before giving up on an
/// endpoint's ordering (it is still retained as a last-resort failover target).
const PROBE_TIMEOUT: Duration = Duration::from_millis(800);

/// Errors surfaced by the driver.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("connection error: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Proto(#[from] ProtoError),
    #[error("server error: {0}")]
    Server(String),
    #[error("authentication failed: {0}")]
    Auth(String),
    #[error("no endpoint reachable: {0}")]
    NoEndpoint(String),
}

/// A synchronous connection to a skaidb cluster, with failover across endpoints.
#[derive(Debug)]
pub struct Client {
    /// Candidate endpoints (`host:port`), retained for reconnection.
    endpoints: Vec<String>,
    username: String,
    password: String,
    /// The endpoint the live `stream` is connected to.
    connected: String,
    stream: TcpStream,
    default_consistency: Consistency,
}

impl Client {
    /// Connect anonymously to a single endpoint (server with auth disabled).
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client, DriverError> {
        Client::connect_with(addr, "anonymous", "")
    }

    /// Connect and authenticate to a single endpoint (SCRAM-SHA-256).
    ///
    /// `addr` may resolve to several socket addresses; all are kept as failover
    /// targets. For an explicit multi-node list use [`Client::connect_many`].
    pub fn connect_with(
        addr: impl ToSocketAddrs,
        username: &str,
        password: &str,
    ) -> Result<Client, DriverError> {
        let endpoints: Vec<String> = addr
            .to_socket_addrs()?
            .map(|a| a.to_string())
            .collect();
        if endpoints.is_empty() {
            return Err(DriverError::NoEndpoint("address did not resolve".into()));
        }
        Client::connect_many(&endpoints, username, password)
    }

    /// Connect across multiple candidate endpoints, preferring the nearest
    /// reachable one and keeping the others for failover. At least one endpoint
    /// must accept the connection and authenticate.
    pub fn connect_many(
        endpoints: &[String],
        username: &str,
        password: &str,
    ) -> Result<Client, DriverError> {
        let ordered = order_by_latency(&dedup(endpoints));
        if ordered.is_empty() {
            return Err(DriverError::NoEndpoint("no endpoints given".into()));
        }
        let mut last = String::new();
        for ep in &ordered {
            match dial(ep, username, password) {
                Ok(stream) => {
                    let connected = ep.clone();
                    return Ok(Client {
                        endpoints: ordered,
                        username: username.to_string(),
                        password: password.to_string(),
                        connected,
                        stream,
                        default_consistency: Consistency::Quorum,
                    });
                }
                Err(e) => last = format!("{ep}: {e}"),
            }
        }
        Err(DriverError::NoEndpoint(last))
    }

    /// The endpoint the live connection is using (the chosen / failed-over node).
    pub fn endpoint(&self) -> &str {
        &self.connected
    }

    /// All candidate endpoints, in current preference order.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Merge additional failover endpoints into the pool (e.g. peers discovered
    /// from the cluster after connecting to a seed). Duplicates are ignored and
    /// the live connection is left untouched.
    pub fn add_endpoints(&mut self, more: &[String]) {
        for e in more {
            if !self.endpoints.contains(e) {
                self.endpoints.push(e.clone());
            }
        }
    }

    /// Set the consistency level used by [`Client::execute`].
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.default_consistency = consistency;
    }

    /// Execute a statement at the default consistency.
    pub fn execute(&mut self, sql: &str) -> Result<Response, DriverError> {
        self.execute_with(sql, self.default_consistency)
    }

    /// Execute a statement at an explicit consistency level. If the live
    /// connection has dropped, fail over to another endpoint and retry once.
    pub fn execute_with(
        &mut self,
        sql: &str,
        consistency: Consistency,
    ) -> Result<Response, DriverError> {
        match self.try_execute(sql, consistency) {
            // A broken pipe / reset / EOF means the node went away — try a peer.
            Err(DriverError::Io(_)) => {
                self.reconnect()?;
                self.try_execute(sql, consistency)
            }
            other => other,
        }
    }

    /// One attempt over the current stream (no reconnect).
    fn try_execute(
        &mut self,
        sql: &str,
        consistency: Consistency,
    ) -> Result<Response, DriverError> {
        let req = Request {
            sql: sql.to_string(),
            consistency,
        };
        write_frame(&mut self.stream, &req.encode())?;
        let payload = read_frame(&mut self.stream)?;
        match Response::decode(&payload)? {
            Response::Error(msg) => Err(DriverError::Server(msg)),
            other => Ok(other),
        }
    }

    /// Re-establish the connection after a failure, preferring a *different*
    /// node than the one that just died, then falling back to any reachable one.
    fn reconnect(&mut self) -> Result<(), DriverError> {
        let mut ordered = order_by_latency(&self.endpoints);
        // Try endpoints other than the one that just failed first.
        ordered.sort_by_key(|e| *e == self.connected);
        let mut last = String::new();
        for ep in &ordered {
            match dial(ep, &self.username, &self.password) {
                Ok(stream) => {
                    self.stream = stream;
                    self.connected = ep.clone();
                    self.endpoints = ordered;
                    return Ok(());
                }
                Err(e) => last = format!("{ep}: {e}"),
            }
        }
        Err(DriverError::NoEndpoint(last))
    }
}

/// Open a TCP connection to `endpoint` and run the SCRAM handshake.
fn dial(endpoint: &str, username: &str, password: &str) -> Result<TcpStream, DriverError> {
    let mut stream = TcpStream::connect(endpoint)?;
    stream.set_nodelay(true).ok();
    handshake(&mut stream, username, password)?;
    Ok(stream)
}

/// Remove duplicate endpoints while preserving the caller's order.
fn dedup(endpoints: &[String]) -> Vec<String> {
    let mut seen = Vec::new();
    for e in endpoints {
        if !seen.contains(e) {
            seen.push(e.clone());
        }
    }
    seen
}

/// Order endpoints by ascending TCP-connect latency (nearest first). Endpoints
/// that fail the probe sink to the back but are retained as failover targets. A
/// single endpoint is returned as-is (no probe needed).
fn order_by_latency(endpoints: &[String]) -> Vec<String> {
    if endpoints.len() <= 1 {
        return endpoints.to_vec();
    }
    let mut scored: Vec<(Option<Duration>, String)> = endpoints
        .iter()
        .map(|e| (probe(e), e.clone()))
        .collect();
    scored.sort_by(|a, b| match (a.0, b.0) {
        (Some(x), Some(y)) => x.cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    scored.into_iter().map(|(_, e)| e).collect()
}

/// Measure TCP-connect latency to an endpoint, or `None` if it can't be reached.
fn probe(endpoint: &str) -> Option<Duration> {
    let addr = endpoint.to_socket_addrs().ok()?.next()?;
    let start = Instant::now();
    TcpStream::connect_timeout(&addr, PROBE_TIMEOUT).ok()?;
    Some(start.elapsed())
}

/// Run the client side of the SCRAM handshake. When `password` is non-empty,
/// the server's signature is verified for mutual authentication.
fn handshake(stream: &mut TcpStream, username: &str, password: &str) -> Result<(), DriverError> {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let client_nonce = format!(
        "c{}.{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    );

    let start = AuthStart {
        username: username.to_string(),
        client_nonce: client_nonce.clone(),
    };
    write_frame(stream, &start.encode())?;

    let challenge = AuthChallenge::decode(&read_frame(stream)?)?;
    let am = auth_message(
        username,
        &client_nonce,
        &challenge.server_nonce,
        &challenge.salt,
        challenge.iterations,
    );
    let proof = scram::client_proof(password, &challenge.salt, challenge.iterations, &am);
    write_frame(
        stream,
        &AuthFinish {
            client_proof: proof,
        }
        .encode(),
    )?;

    match AuthOutcome::decode(&read_frame(stream)?)? {
        AuthOutcome::Ok { server_signature } => {
            if !password.is_empty() {
                let expected =
                    scram::server_signature(password, &challenge.salt, challenge.iterations, &am);
                if expected != server_signature {
                    return Err(DriverError::Auth("server signature mismatch".into()));
                }
            }
            Ok(())
        }
        AuthOutcome::Denied { reason } => Err(DriverError::Auth(reason)),
    }
}
