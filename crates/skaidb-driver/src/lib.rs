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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use skaidb_auth::scram;
use skaidb_proto::{
    auth_message, decode_tagged_response, encode_tagged_request, read_frame, write_frame,
    AuthChallenge, AuthFinish, AuthMechanism, AuthOutcome, AuthStart, ClientRequest, Consistency,
    ProtoError, Request, Response,
};
use skaidb_types::Value;

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

/// A statement prepared on the server via [`Client::prepare`]: parse once,
/// execute many times with different parameter bindings. Holds the template
/// text so the driver can re-prepare transparently after a failover.
#[derive(Debug, Clone)]
pub struct Prepared {
    id: u32,
    /// How many `?` parameters the statement expects.
    pub params: u16,
    sql: String,
}

/// A synchronous connection to a skaidb cluster, with failover across endpoints.
#[derive(Debug)]
pub struct Client {
    /// Candidate endpoints (`host:port`), retained for reconnection.
    endpoints: Vec<String>,
    username: String,
    password: String,
    /// When set, authenticate with Kerberos (GSSAPI) to this target service
    /// principal instead of SCRAM; `password` is unused. Retained so failover
    /// reconnects re-authenticate the same way.
    gssapi_spn: Option<String>,
    /// The endpoint the live `stream` is connected to.
    connected: String,
    stream: skaidb_net::Stream,
    /// Client-TLS settings applied to every (re)connect; `None` = plaintext.
    tls: Option<TlsConfig>,
    default_consistency: Consistency,
}

/// Client-side TLS settings for a [`Client`]: the built rustls config plus the
/// SNI/verification name. Build with [`TlsConfig::new`].
#[derive(Clone)]
pub struct TlsConfig {
    cfg: Arc<rustls::ClientConfig>,
    server_name: String,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// How the driver verifies the server's certificate.
#[derive(Debug, Clone)]
pub enum TlsVerify {
    /// Trust server certs chaining to this CA file (the cluster CA).
    CaFile(String),
    /// Skip verification — self-signed/dev only. INSECURE.
    Insecure,
}

impl TlsConfig {
    /// Build a client-TLS config. `server_name` is the SNI/verification name
    /// (the SAN on the server's cert — `skaidb` for certs from
    /// `skaidbsh certs gen`).
    pub fn new(verify: TlsVerify, server_name: &str) -> Result<TlsConfig, DriverError> {
        let v = match verify {
            TlsVerify::CaFile(p) => skaidb_net::ClientVerify::CaFile(p),
            TlsVerify::Insecure => skaidb_net::ClientVerify::Insecure,
        };
        let cfg = skaidb_net::client_config(v, None)
            .map_err(|e| DriverError::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
        Ok(TlsConfig {
            cfg,
            server_name: server_name.to_string(),
        })
    }
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
        Client::connect_many_tls(endpoints, username, password, None)
    }

    /// [`Client::connect_many`] with client-side TLS. `tls = None` is plaintext
    /// (identical to `connect_many`); `Some(cfg)` wraps every connection —
    /// including failover reconnects — in TLS.
    pub fn connect_many_tls(
        endpoints: &[String],
        username: &str,
        password: &str,
        tls: Option<TlsConfig>,
    ) -> Result<Client, DriverError> {
        Client::connect_inner(endpoints, username, password, None, tls)
    }

    /// Connect authenticating with Kerberos (SASL GSSAPI) instead of a
    /// password. `principal` is the client identity presented in the handshake
    /// (the authenticated identity actually comes from the Kerberos ticket);
    /// `target_spn` is the skaidb node's service principal, e.g.
    /// `skaidb/host.example.com@REALM`. Uses the ambient ticket cache — run
    /// `kinit` first. Requires a driver built with the `kerberos` feature.
    /// `tls = None` is plaintext; `Some(cfg)` wraps every (re)connect in TLS
    /// (recommended — GSSAPI provides authentication, TLS the confidentiality).
    pub fn connect_gssapi_tls(
        endpoints: &[String],
        principal: &str,
        target_spn: &str,
        tls: Option<TlsConfig>,
    ) -> Result<Client, DriverError> {
        Client::connect_inner(endpoints, principal, "", Some(target_spn.to_string()), tls)
    }

    /// Plaintext [`Client::connect_gssapi_tls`].
    pub fn connect_gssapi(
        endpoints: &[String],
        principal: &str,
        target_spn: &str,
    ) -> Result<Client, DriverError> {
        Client::connect_gssapi_tls(endpoints, principal, target_spn, None)
    }

    /// Shared connect path: order endpoints by latency and dial each until one
    /// authenticates, via SCRAM (`gssapi_spn = None`) or GSSAPI (`Some(spn)`).
    fn connect_inner(
        endpoints: &[String],
        username: &str,
        password: &str,
        gssapi_spn: Option<String>,
        tls: Option<TlsConfig>,
    ) -> Result<Client, DriverError> {
        let ordered = order_by_latency(&dedup(endpoints));
        if ordered.is_empty() {
            return Err(DriverError::NoEndpoint("no endpoints given".into()));
        }
        let mut last = String::new();
        for ep in &ordered {
            match dial(ep, username, password, gssapi_spn.as_deref(), tls.as_ref()) {
                Ok(stream) => {
                    let connected = ep.clone();
                    return Ok(Client {
                        endpoints: ordered,
                        username: username.to_string(),
                        password: password.to_string(),
                        gssapi_spn,
                        connected,
                        stream,
                        tls,
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

    /// Execute a batch of statements **pipelined** at the default
    /// consistency: all requests are written before any response is read, so
    /// the whole batch pays one round-trip of link latency instead of one
    /// per statement. See [`Client::pipeline_with`].
    pub fn pipeline(&mut self, stmts: &[&str]) -> Result<Vec<Response>, DriverError> {
        self.pipeline_with(stmts, self.default_consistency)
    }

    /// Pipelined batch execution at an explicit consistency.
    ///
    /// Statements execute on the server serially, in order, with ordinary
    /// session semantics (a `USE` mid-batch affects the statements after
    /// it). Responses are correlated by request id, and per-statement
    /// failures come back **inline** as [`Response::Error`] entries — a
    /// failed statement does not stop the ones after it. The whole batch is
    /// retried once on a fresh connection if the node dies mid-flight (same
    /// idempotency caveat as [`Client::execute_with`]). Servers older than
    /// the tagged-request opcode fail the batch with "server too old".
    pub fn pipeline_with(
        &mut self,
        stmts: &[&str],
        consistency: Consistency,
    ) -> Result<Vec<Response>, DriverError> {
        if stmts.is_empty() {
            return Ok(Vec::new());
        }
        match self.try_pipeline(stmts, consistency) {
            Err(DriverError::Io(_)) => {
                self.reconnect()?;
                self.try_pipeline(stmts, consistency)
            }
            other => other,
        }
    }

    /// One pipelined attempt over the current stream (no reconnect).
    fn try_pipeline(
        &mut self,
        stmts: &[&str],
        consistency: Consistency,
    ) -> Result<Vec<Response>, DriverError> {
        // Write every request before reading anything: the requests queue in
        // the server's receive buffer and are answered back-to-back.
        let mut buf = Vec::new();
        for (i, sql) in stmts.iter().enumerate() {
            buf.clear();
            encode_tagged_request(
                i as u32,
                &ClientRequest::Query {
                    sql: (*sql).to_string(),
                    consistency,
                },
                &mut buf,
            );
            write_frame(&mut self.stream, &buf)?;
        }
        let mut out: Vec<Option<Response>> = (0..stmts.len()).map(|_| None).collect();
        for _ in 0..stmts.len() {
            let payload = read_frame(&mut self.stream)?;
            let (id, resp) = decode_tagged_response(&payload)?;
            let Some(id) = id else {
                // An untagged response to a tagged request: the server
                // predates pipelining and answered with a plain error.
                return Err(DriverError::Server(
                    "server does not support pipelined requests (upgrade the server)".into(),
                ));
            };
            match out.get_mut(id as usize) {
                Some(slot @ None) => *slot = Some(resp),
                _ => {
                    return Err(DriverError::Server(format!(
                        "unexpected response id {id} in pipeline"
                    )))
                }
            }
        }
        // Every slot filled exactly once (ids 0..n, each seen once).
        Ok(out.into_iter().map(|r| r.expect("all ids seen")).collect())
    }

    /// Parse `sql` (which may contain `?` placeholders) once on the server
    /// and cache it on this connection. Execute it any number of times with
    /// [`Client::execute_prepared`], paying no per-call parse.
    pub fn prepare(&mut self, sql: &str) -> Result<Prepared, DriverError> {
        let req = ClientRequest::Prepare {
            sql: sql.to_string(),
        };
        match self.roundtrip(&req) {
            Err(DriverError::Io(_)) => {
                self.reconnect()?;
                self.roundtrip(&req)
            }
            other => other,
        }
        .and_then(|resp| match resp {
            Response::Prepared { id, params } => Ok(Prepared {
                id,
                params,
                sql: sql.to_string(),
            }),
            other => Err(DriverError::Server(format!(
                "unexpected response to Prepare: {other:?}"
            ))),
        })
    }

    /// Execute a prepared statement at the default consistency.
    pub fn execute_prepared(
        &mut self,
        stmt: &mut Prepared,
        params: &[Value],
    ) -> Result<Response, DriverError> {
        self.execute_prepared_with(stmt, params, self.default_consistency)
    }

    /// Execute a prepared statement at an explicit consistency. Prepared ids
    /// are per-connection, so on failover the statement is transparently
    /// re-prepared on the new connection (updating `stmt`) and retried once.
    pub fn execute_prepared_with(
        &mut self,
        stmt: &mut Prepared,
        params: &[Value],
        consistency: Consistency,
    ) -> Result<Response, DriverError> {
        let req = ClientRequest::Execute {
            id: stmt.id,
            params: params.to_vec(),
            consistency,
        };
        match self.roundtrip(&req) {
            Err(DriverError::Io(_)) => {
                self.reconnect()?;
                *stmt = self.prepare(&stmt.sql)?;
                self.roundtrip(&ClientRequest::Execute {
                    id: stmt.id,
                    params: params.to_vec(),
                    consistency,
                })
            }
            other => other,
        }
    }

    /// Execute a prepared statement once per parameter row in a single
    /// round-trip (the `executemany` wire op) at the default consistency.
    /// Returns the total affected count. Each row autocommits like a looped
    /// `execute_prepared`; on a failure the server error names the row and
    /// earlier rows stay applied.
    pub fn execute_batch(
        &mut self,
        stmt: &mut Prepared,
        rows: Vec<Vec<Value>>,
    ) -> Result<u64, DriverError> {
        let consistency = self.default_consistency;
        let req = ClientRequest::ExecuteBatch {
            id: stmt.id,
            rows: rows.clone(),
            consistency,
        };
        let resp = match self.roundtrip(&req) {
            Err(DriverError::Io(_)) => {
                self.reconnect()?;
                *stmt = self.prepare(&stmt.sql)?;
                self.roundtrip(&ClientRequest::ExecuteBatch {
                    id: stmt.id,
                    rows,
                    consistency,
                })
            }
            other => other,
        }?;
        match resp {
            Response::Mutation { affected } => Ok(affected),
            Response::Error(e) => Err(DriverError::Server(e)),
            other => Err(DriverError::Server(format!(
                "unexpected response to ExecuteBatch: {other:?}"
            ))),
        }
    }

    /// One request/response round-trip over the current stream (no reconnect).
    fn roundtrip(&mut self, req: &ClientRequest) -> Result<Response, DriverError> {
        write_frame(&mut self.stream, &req.encode())?;
        let payload = read_frame(&mut self.stream)?;
        match Response::decode(&payload)? {
            Response::Error(msg) => Err(DriverError::Server(msg)),
            other => Ok(other),
        }
    }

    /// Run a query whose result arrives as a stream of row chunks, at the
    /// default consistency. See [`Client::query_stream_with`].
    pub fn query_stream(&mut self, sql: &str) -> Result<RowStream<'_>, DriverError> {
        self.query_stream_with(sql, self.default_consistency)
    }

    /// Run a query whose result arrives as a stream of row chunks, so the
    /// client never holds more than one chunk in memory — use this for result
    /// sets too large to buffer. Non-row statements yield an empty stream
    /// (check [`RowStream::affected`]).
    ///
    /// The returned [`RowStream`] borrows the connection exclusively until it
    /// is drained or dropped (dropping it reads and discards the rest of the
    /// stream). Failover applies only to sending the request; an endpoint
    /// dying mid-stream surfaces as an error from the iterator, and the caller
    /// decides whether to re-run the query. Servers older than the
    /// `QueryStream` opcode reject it with a server error; use
    /// [`Client::execute`] against those.
    pub fn query_stream_with(
        &mut self,
        sql: &str,
        consistency: Consistency,
    ) -> Result<RowStream<'_>, DriverError> {
        let req = ClientRequest::QueryStream {
            sql: sql.to_string(),
            consistency,
        };
        if write_frame(&mut self.stream, &req.encode()).is_err() {
            // The node died since the last use — fail over before the stream
            // starts, like execute() does.
            self.reconnect()?;
            write_frame(&mut self.stream, &req.encode())?;
        }
        let payload = read_frame(&mut self.stream)?;
        match Response::decode(&payload)? {
            Response::Error(msg) => Err(DriverError::Server(msg)),
            Response::RowsHeader { columns } => Ok(RowStream {
                client: self,
                columns,
                affected: 0,
                chunk: Vec::new().into_iter(),
                done: false,
            }),
            // Non-row statement: a single ordinary response ends the exchange.
            Response::Mutation { affected } => Ok(RowStream {
                client: self,
                columns: Vec::new(),
                affected,
                chunk: Vec::new().into_iter(),
                done: true,
            }),
            Response::Ddl => Ok(RowStream {
                client: self,
                columns: Vec::new(),
                affected: 0,
                chunk: Vec::new().into_iter(),
                done: true,
            }),
            other => Err(DriverError::Server(format!(
                "unexpected response to QueryStream: {other:?}"
            ))),
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
            match dial(
                ep,
                &self.username,
                &self.password,
                self.gssapi_spn.as_deref(),
                self.tls.as_ref(),
            ) {
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

/// A streamed result set from [`Client::query_stream`]: iterate to receive
/// rows chunk by chunk. Holds at most one chunk in memory. Borrows the
/// [`Client`] exclusively; dropping the stream early drains the remaining
/// frames so the connection stays usable.
#[derive(Debug)]
pub struct RowStream<'a> {
    client: &'a mut Client,
    /// Column names of the result set (empty for non-row statements).
    pub columns: Vec<String>,
    /// Rows affected, when the statement turned out to be a mutation.
    pub affected: u64,
    chunk: std::vec::IntoIter<Vec<Value>>,
    done: bool,
}

impl RowStream<'_> {
    /// Fetch the next frame after the current chunk ran dry.
    fn next_frame(&mut self) -> Result<(), DriverError> {
        let payload = read_frame(&mut self.client.stream)?;
        match Response::decode(&payload)? {
            Response::RowsChunk { rows } => self.chunk = rows.into_iter(),
            Response::RowsEnd => self.done = true,
            Response::Error(msg) => {
                self.done = true;
                return Err(DriverError::Server(msg));
            }
            other => {
                self.done = true;
                return Err(DriverError::Server(format!(
                    "unexpected frame in row stream: {other:?}"
                )));
            }
        }
        Ok(())
    }
}

impl Iterator for RowStream<'_> {
    type Item = Result<Vec<Value>, DriverError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.chunk.next() {
                return Some(Ok(row));
            }
            if self.done {
                return None;
            }
            if let Err(e) = self.next_frame() {
                // An I/O error also marks the stream finished: the connection
                // is broken and the next client call will fail over.
                self.done = true;
                return Some(Err(e));
            }
        }
    }
}

impl Drop for RowStream<'_> {
    fn drop(&mut self) {
        // Read out any frames the server is still sending, so the connection
        // is positioned at a request boundary for the next call. On error the
        // socket is broken anyway and the client will reconnect on next use.
        while !self.done {
            if self.next_frame().is_err() {
                break;
            }
        }
    }
}

/// Open a TCP connection to `endpoint` and run the client handshake — GSSAPI
/// when `gssapi_spn` is set, otherwise SCRAM.
fn dial(
    endpoint: &str,
    username: &str,
    password: &str,
    gssapi_spn: Option<&str>,
    tls: Option<&TlsConfig>,
) -> Result<skaidb_net::Stream, DriverError> {
    let tcp = TcpStream::connect(endpoint)?;
    tcp.set_nodelay(true).ok();
    let mut stream = match tls {
        Some(t) => skaidb_net::connect_tls(tcp, t.cfg.clone(), &t.server_name)?,
        None => skaidb_net::Stream::Plain(tcp),
    };
    match gssapi_spn {
        Some(spn) => handshake_gssapi(&mut stream, username, spn)?,
        None => handshake(&mut stream, username, password)?,
    }
    Ok(stream)
}

/// Client side of the GSSAPI handshake: announce the mechanism, send the first
/// GSS token, then shuttle `AuthToken` frames until the server replies with an
/// `AuthOutcome`. Uses the ambient ticket cache (run `kinit`). Built only with
/// the `kerberos` feature.
#[cfg(feature = "kerberos")]
fn handshake_gssapi<S: io::Read + io::Write>(
    stream: &mut S,
    username: &str,
    target_spn: &str,
) -> Result<(), DriverError> {
    use skaidb_gssapi::{ClientHandshake, ClientStep};
    use skaidb_proto::AuthToken;

    static NONCE: AtomicU64 = AtomicU64::new(0);
    let client_nonce = format!(
        "c{}.{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    );
    let (client, token0) = ClientHandshake::new(target_spn, None)
        .map_err(|e| DriverError::Auth(format!("gssapi init failed (did you kinit?): {e}")))?;
    write_frame(
        stream,
        &AuthStart {
            username: username.to_string(),
            client_nonce,
            mechanism: AuthMechanism::Gssapi,
        }
        .encode(),
    )?;
    write_frame(stream, &AuthToken { token: token0 }.encode())?;

    let mut client = Some(client);
    loop {
        let frame = read_frame(stream)?;
        if let Ok(tok) = AuthToken::decode(&frame) {
            let c = client
                .take()
                .ok_or_else(|| DriverError::Auth("gssapi: unexpected server token".into()))?;
            match c.step(&tok.token).map_err(|e| DriverError::Auth(e.to_string()))? {
                ClientStep::Continue { next, token } => {
                    client = Some(next);
                    write_frame(stream, &AuthToken { token }.encode())?;
                }
                ClientStep::Done { token } => {
                    if let Some(token) = token {
                        write_frame(stream, &AuthToken { token }.encode())?;
                    }
                }
            }
        } else if let Ok(outcome) = AuthOutcome::decode(&frame) {
            return match outcome {
                AuthOutcome::Ok { .. } => Ok(()),
                AuthOutcome::Denied { reason } => Err(DriverError::Auth(reason)),
            };
        } else {
            return Err(DriverError::Auth("gssapi: unexpected handshake frame".into()));
        }
    }
}

#[cfg(not(feature = "kerberos"))]
fn handshake_gssapi<S: io::Read + io::Write>(
    _stream: &mut S,
    _username: &str,
    _target_spn: &str,
) -> Result<(), DriverError> {
    Err(DriverError::Auth(
        "this driver was built without Kerberos (GSSAPI) support".into(),
    ))
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

/// Process-global cache of derived SCRAM `SaltedPassword`s, keyed by
/// (password, salt, iterations). The PBKDF2 derivation is deliberately
/// expensive (15k HMAC iterations at the default — tens of ms on small
/// CPUs) and its output is identical for every connection authenticating
/// the same credential against the same stored salt, so pools and
/// reconnects pay it once instead of per connection. The cache holds
/// password-equivalent secrets, but so does the process (the password
/// itself is in memory); bounded small since real processes use a handful
/// of credentials.
fn cached_salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    use std::collections::HashMap;
    use std::sync::Mutex;
    /// (password, salt, iterations) → derived SaltedPassword.
    type SaltedCache = HashMap<(String, Vec<u8>, u32), [u8; 32]>;
    static CACHE: Mutex<Option<SaltedCache>> = Mutex::new(None);

    let key = (password.to_string(), salt.to_vec(), iterations);
    if let Some(hit) = CACHE
        .lock()
        .ok()
        .and_then(|c| c.as_ref().and_then(|m| m.get(&key).copied()))
    {
        return hit;
    }
    // Derive outside the lock: a herd of first connections computes it in
    // parallel rather than serializing behind one derivation.
    let salted = scram::salted_password(password, salt, iterations);
    if let Ok(mut guard) = CACHE.lock() {
        let map = guard.get_or_insert_with(HashMap::new);
        if map.len() >= 64 {
            map.clear(); // crude bound; real processes hold a handful of creds
        }
        map.insert(key, salted);
    }
    salted
}

/// Run the client side of the SCRAM handshake. When `password` is non-empty,
/// the server's signature is verified for mutual authentication.
fn handshake<S: io::Read + io::Write>(
    stream: &mut S,
    username: &str,
    password: &str,
) -> Result<(), DriverError> {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let client_nonce = format!(
        "c{}.{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    );

    let start = AuthStart {
        username: username.to_string(),
        client_nonce: client_nonce.clone(),
        // This handshake helper speaks SCRAM; GSSAPI has its own client path.
        mechanism: AuthMechanism::ScramSha256,
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
    // One PBKDF2 per (credential, salt) per process — not two per
    // connection: the proof and the server-signature check share the
    // derived key, and the cache shares it across connections.
    let salted = cached_salted_password(password, &challenge.salt, challenge.iterations);
    let proof = scram::client_proof_salted(&salted, &am);
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
                let expected = scram::server_signature_salted(&salted, &am);
                if expected != server_signature {
                    return Err(DriverError::Auth("server signature mismatch".into()));
                }
            }
            Ok(())
        }
        AuthOutcome::Denied { reason } => Err(DriverError::Auth(reason)),
    }
}
