//! skaidb client driver (SPEC §7).
//!
//! Phase 1 provides a synchronous client over the binary fast-path protocol
//! ([`skaidb_proto`]). Token-aware, least-loaded multi-node routing (built on
//! `skaidb-cluster`'s ring) and the REST endpoint are layered on in later
//! phases; this connects to a single node.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};

use skaidb_proto::{read_frame, write_frame, Consistency, ProtoError, Request, Response};

/// Errors surfaced by the driver.
#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("connection error: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Proto(#[from] ProtoError),
    #[error("server error: {0}")]
    Server(String),
}

/// A synchronous connection to one skaidb node.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    default_consistency: Consistency,
}

impl Client {
    /// Connect to a node's binary endpoint.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Client, DriverError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
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
