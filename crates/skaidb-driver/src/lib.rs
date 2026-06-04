//! skaidb client driver (SPEC §7).
//!
//! Phase 1 provides a synchronous client over the binary fast-path protocol
//! ([`skaidb_proto`]). Token-aware, least-loaded multi-node routing (built on
//! `skaidb-cluster`'s ring) and the REST endpoint are layered on in later
//! phases; this connects to a single node.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};

use skaidb_auth::scram;
use skaidb_proto::{
    auth_message, read_frame, write_frame, AuthChallenge, AuthFinish, AuthOutcome, AuthStart,
    Consistency, ProtoError, Request, Response,
};

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
}

/// A synchronous connection to one skaidb node.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    default_consistency: Consistency,
}

impl Client {
    /// Connect anonymously (for a server with authentication disabled).
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client, DriverError> {
        Client::connect_with(addr, "anonymous", "")
    }

    /// Connect and authenticate as `username` with `password` (SCRAM-SHA-256).
    pub fn connect_with(
        addr: impl ToSocketAddrs,
        username: &str,
        password: &str,
    ) -> Result<Client, DriverError> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        handshake(&mut stream, username, password)?;
        Ok(Client {
            stream,
            default_consistency: Consistency::Quorum,
        })
    }

    /// Set the consistency level used by [`Client::execute`].
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.default_consistency = consistency;
    }

    /// Execute a statement at the default consistency.
    pub fn execute(&mut self, sql: &str) -> Result<Response, DriverError> {
        self.execute_with(sql, self.default_consistency)
    }

    /// Execute a statement at an explicit consistency level.
    pub fn execute_with(
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
        let resp = Response::decode(&payload)?;
        match resp {
            Response::Error(msg) => Err(DriverError::Server(msg)),
            other => Ok(other),
        }
    }
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
