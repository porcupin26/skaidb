//! Internode RPC for replication and distributed reads (SPEC §4–6).
//!
//! Members talk to each other over the same length-prefixed framing as the
//! client protocol. A coordinator replicates row writes (`ApplyPut`/
//! `ApplyDelete`) to a key's replica set, scatters `LocalScan` to gather a
//! table for a read, and broadcasts `ApplyDdl`.

use std::io;
use std::net::TcpStream;

use skaidb_proto::{read_frame, write_frame};
use skaidb_storage::Hlc;

/// A request from a coordinator to a peer member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    ApplyPut {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: Hlc,
    },
    ApplyDelete {
        table: String,
        key: Vec<u8>,
        hlc: Hlc,
    },
    LocalScan {
        table: String,
    },
    ApplyDdl {
        sql: String,
    },
    Ping,
}

/// A response from a peer member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ack,
    Scan { rows: Vec<(Vec<u8>, Vec<u8>, Hlc)> },
    Err(String),
    Pong,
}

/// Errors decoding an internode message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("malformed internode message: {0}")]
    Malformed(&'static str),
}

const REQ_PUT: u8 = 1;
const REQ_DEL: u8 = 2;
const REQ_SCAN: u8 = 3;
const REQ_DDL: u8 = 4;
const REQ_PING: u8 = 5;

const RES_ACK: u8 = 0;
const RES_SCAN: u8 = 1;
const RES_ERR: u8 = 2;
const RES_PONG: u8 = 3;

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        match self {
            Request::ApplyPut {
                table,
                key,
                value,
                hlc,
            } => {
                o.push(REQ_PUT);
                put_str(&mut o, table);
                put_bytes(&mut o, key);
                put_bytes(&mut o, value);
                o.extend_from_slice(&hlc.to_bytes());
            }
            Request::ApplyDelete { table, key, hlc } => {
                o.push(REQ_DEL);
                put_str(&mut o, table);
                put_bytes(&mut o, key);
                o.extend_from_slice(&hlc.to_bytes());
            }
            Request::LocalScan { table } => {
                o.push(REQ_SCAN);
                put_str(&mut o, table);
            }
            Request::ApplyDdl { sql } => {
                o.push(REQ_DDL);
                put_str(&mut o, sql);
            }
            Request::Ping => o.push(REQ_PING),
        }
        o
    }

    pub fn decode(buf: &[u8]) -> Result<Request, WireError> {
        let mut c = Cur::new(buf);
        Ok(match c.u8()? {
            REQ_PUT => Request::ApplyPut {
                table: c.string()?,
                key: c.bytes()?,
                value: c.bytes()?,
                hlc: c.hlc()?,
            },
            REQ_DEL => Request::ApplyDelete {
                table: c.string()?,
                key: c.bytes()?,
                hlc: c.hlc()?,
            },
            REQ_SCAN => Request::LocalScan { table: c.string()? },
            REQ_DDL => Request::ApplyDdl { sql: c.string()? },
            REQ_PING => Request::Ping,
            _ => return Err(WireError::Malformed("unknown request op")),
        })
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        match self {
            Response::Ack => o.push(RES_ACK),
            Response::Scan { rows } => {
                o.push(RES_SCAN);
                o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for (k, v, hlc) in rows {
                    put_bytes(&mut o, k);
                    put_bytes(&mut o, v);
                    o.extend_from_slice(&hlc.to_bytes());
                }
            }
            Response::Err(msg) => {
                o.push(RES_ERR);
                put_str(&mut o, msg);
            }
            Response::Pong => o.push(RES_PONG),
        }
        o
    }

    pub fn decode(buf: &[u8]) -> Result<Response, WireError> {
        let mut c = Cur::new(buf);
        Ok(match c.u8()? {
            RES_ACK => Response::Ack,
            RES_SCAN => {
                let n = c.u32()? as usize;
                let mut rows = Vec::with_capacity(n);
                for _ in 0..n {
                    rows.push((c.bytes()?, c.bytes()?, c.hlc()?));
                }
                Response::Scan { rows }
            }
            RES_ERR => Response::Err(c.string()?),
            RES_PONG => Response::Pong,
            _ => return Err(WireError::Malformed("unknown response op")),
        })
    }
}

/// Send one request to `addr` and read the response (a fresh connection).
pub fn call(addr: &str, req: &Request) -> io::Result<Response> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    write_frame(&mut stream, &req.encode())?;
    let payload = read_frame(&mut stream)?;
    Response::decode(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn put_str(o: &mut Vec<u8>, s: &str) {
    put_bytes(o, s.as_bytes());
}
fn put_bytes(o: &mut Vec<u8>, b: &[u8]) {
    o.extend_from_slice(&(b.len() as u32).to_le_bytes());
    o.extend_from_slice(b);
}

struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Cur<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cur { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(WireError::Malformed("overflow"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or(WireError::Malformed("short"))?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, WireError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.bytes()?).map_err(|_| WireError::Malformed("bad utf-8"))
    }
    fn hlc(&mut self) -> Result<Hlc, WireError> {
        let b: [u8; 12] = self.take(12)?.try_into().unwrap();
        Ok(Hlc::from_bytes(b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips() {
        for req in [
            Request::ApplyPut {
                table: "t".into(),
                key: vec![1, 2],
                value: vec![3, 4, 5],
                hlc: Hlc::new(10, 1),
            },
            Request::ApplyDelete {
                table: "t".into(),
                key: vec![9],
                hlc: Hlc::new(11, 0),
            },
            Request::LocalScan { table: "t".into() },
            Request::ApplyDdl {
                sql: "CREATE TABLE t (PRIMARY KEY (id))".into(),
            },
            Request::Ping,
        ] {
            assert_eq!(Request::decode(&req.encode()).unwrap(), req);
        }
    }

    #[test]
    fn response_roundtrips() {
        let scan = Response::Scan {
            rows: vec![
                (vec![1], vec![2, 3], Hlc::new(5, 0)),
                (vec![4], vec![5], Hlc::new(6, 2)),
            ],
        };
        for res in [
            Response::Ack,
            scan,
            Response::Err("x".into()),
            Response::Pong,
        ] {
            assert_eq!(Response::decode(&res.encode()).unwrap(), res);
        }
    }
}
