//! Internode RPC for replication and distributed reads (SPEC §4–6).
//!
//! Members talk to each other over the same length-prefixed framing as the
//! client protocol. A coordinator replicates row writes (`ApplyPut`/
//! `ApplyDelete`) to a key's replica set, scatters `LocalScan` to gather a
//! table for a read, and broadcasts `ApplyDdl`.

use std::io;
use std::net::TcpStream;

use skaidb_proto::{read_frame, write_frame};
use skaidb_storage::compress::{compress, decompress, Codec};
use skaidb_storage::Hlc;

/// A request from a coordinator to a peer member.
#[derive(Debug, Clone, PartialEq)]
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
    LocalGet {
        table: String,
        key: Vec<u8>,
    },
    /// Scan the named local secondary index over a byte range and return the
    /// candidate row keys (a superset; the coordinator re-reads each).
    IndexScan {
        index: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    /// Approximate `k` nearest keys to `query` from the node's local vector
    /// index (one shard). The coordinator merges these across nodes.
    VectorSearch {
        index: String,
        query: Vec<f32>,
        k: u32,
    },
    ApplyDdl {
        sql: String,
    },
    /// Replace the recipient's cluster membership/ring with `members`
    /// (`(id, addr)` pairs, including the recipient) at version `epoch`. Broadcast
    /// when a node joins or leaves. The recipient applies it only if `epoch` is
    /// newer than the one it holds, so stale updates and concurrent topology
    /// changes can't move a node's ring backward.
    SetMembership {
        epoch: u64,
        members: Vec<(String, String)>,
    },
    /// Push every locally-held row whose key the named `joiner` now owns (under
    /// the current ring) to that joiner, preserving each row's HLC. Sent to
    /// existing members after a [`Request::SetMembership`] so the new node
    /// receives its share of the keyspace.
    Rebalance {
        joiner: String,
    },
    /// Drain this (leaving) node: push every locally-held row to its new owners
    /// under the post-removal ring described by `members` (which excludes the
    /// leaving node), so no key loses a replica when the node departs. Sent to a
    /// node being gracefully decommissioned, before it is removed from the ring.
    Drain {
        members: Vec<(String, String)>,
    },
    /// Reclaim local disk space: physically drop every locally-held key this node
    /// no longer owns under the current ring, after confirming an owner holds it.
    /// A post-resharding "cleanup" trigger.
    Reclaim,
    Ping,
}

/// A response from a peer member.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Ack,
    /// A versioned table shard: `(key, value, hlc, is_put)`. `is_put == false`
    /// marks a tombstone (empty value), so the coordinator can resolve deletes
    /// across replicas by last-writer-wins.
    Scan {
        rows: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)>,
    },
    /// Point-read result: `(value, stamp, is_put)`, or `None` if absent here.
    Get {
        entry: Option<(Vec<u8>, Hlc, bool)>,
    },
    /// Candidate row keys from an [`Request::IndexScan`].
    Keys {
        keys: Vec<Vec<u8>>,
    },
    /// `(key, distance)` hits from a [`Request::VectorSearch`], nearest-first.
    VectorHits {
        hits: Vec<(Vec<u8>, f32)>,
    },
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
const REQ_GET: u8 = 6;
const REQ_INDEX: u8 = 7;
const REQ_VECTOR: u8 = 8;
const REQ_MEMBERS: u8 = 9;
const REQ_REBAL: u8 = 10;
const REQ_DRAIN: u8 = 11;
const REQ_RECLAIM: u8 = 12;

const RES_ACK: u8 = 0;
const RES_SCAN: u8 = 1;
const RES_ERR: u8 = 2;
const RES_PONG: u8 = 3;
const RES_GET: u8 = 4;
const RES_KEYS: u8 = 5;
const RES_VHITS: u8 = 6;

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
            Request::LocalGet { table, key } => {
                o.push(REQ_GET);
                put_str(&mut o, table);
                put_bytes(&mut o, key);
            }
            Request::IndexScan { index, start, end } => {
                o.push(REQ_INDEX);
                put_str(&mut o, index);
                put_opt_bytes(&mut o, start.as_deref());
                put_opt_bytes(&mut o, end.as_deref());
            }
            Request::VectorSearch { index, query, k } => {
                o.push(REQ_VECTOR);
                put_str(&mut o, index);
                o.extend_from_slice(&(query.len() as u32).to_le_bytes());
                for x in query {
                    o.extend_from_slice(&x.to_le_bytes());
                }
                o.extend_from_slice(&k.to_le_bytes());
            }
            Request::ApplyDdl { sql } => {
                o.push(REQ_DDL);
                put_str(&mut o, sql);
            }
            Request::SetMembership { epoch, members } => {
                o.push(REQ_MEMBERS);
                o.extend_from_slice(&epoch.to_le_bytes());
                o.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (id, addr) in members {
                    put_str(&mut o, id);
                    put_str(&mut o, addr);
                }
            }
            Request::Rebalance { joiner } => {
                o.push(REQ_REBAL);
                put_str(&mut o, joiner);
            }
            Request::Drain { members } => {
                o.push(REQ_DRAIN);
                o.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (id, addr) in members {
                    put_str(&mut o, id);
                    put_str(&mut o, addr);
                }
            }
            Request::Reclaim => o.push(REQ_RECLAIM),
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
            REQ_GET => Request::LocalGet {
                table: c.string()?,
                key: c.bytes()?,
            },
            REQ_INDEX => Request::IndexScan {
                index: c.string()?,
                start: c.opt_bytes()?,
                end: c.opt_bytes()?,
            },
            REQ_VECTOR => {
                let index = c.string()?;
                let n = c.u32()? as usize;
                let mut query = Vec::with_capacity(n);
                for _ in 0..n {
                    query.push(c.f32()?);
                }
                let k = c.u32()?;
                Request::VectorSearch { index, query, k }
            }
            REQ_DDL => Request::ApplyDdl { sql: c.string()? },
            REQ_MEMBERS => {
                let epoch = c.u64()?;
                let n = c.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    let id = c.string()?;
                    let addr = c.string()?;
                    members.push((id, addr));
                }
                Request::SetMembership { epoch, members }
            }
            REQ_REBAL => Request::Rebalance {
                joiner: c.string()?,
            },
            REQ_DRAIN => {
                let n = c.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    let id = c.string()?;
                    let addr = c.string()?;
                    members.push((id, addr));
                }
                Request::Drain { members }
            }
            REQ_RECLAIM => Request::Reclaim,
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
                for (k, v, hlc, is_put) in rows {
                    put_bytes(&mut o, k);
                    put_bytes(&mut o, v);
                    o.extend_from_slice(&hlc.to_bytes());
                    o.push(u8::from(*is_put));
                }
            }
            Response::Get { entry } => {
                o.push(RES_GET);
                match entry {
                    Some((value, hlc, is_put)) => {
                        o.push(1);
                        put_bytes(&mut o, value);
                        o.extend_from_slice(&hlc.to_bytes());
                        o.push(u8::from(*is_put));
                    }
                    None => o.push(0),
                }
            }
            Response::Keys { keys } => {
                o.push(RES_KEYS);
                o.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    put_bytes(&mut o, k);
                }
            }
            Response::VectorHits { hits } => {
                o.push(RES_VHITS);
                o.extend_from_slice(&(hits.len() as u32).to_le_bytes());
                for (key, dist) in hits {
                    put_bytes(&mut o, key);
                    o.extend_from_slice(&dist.to_le_bytes());
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
                    let key = c.bytes()?;
                    let value = c.bytes()?;
                    let hlc = c.hlc()?;
                    let is_put = c.u8()? != 0;
                    rows.push((key, value, hlc, is_put));
                }
                Response::Scan { rows }
            }
            RES_GET => {
                let entry = if c.u8()? == 1 {
                    let value = c.bytes()?;
                    let hlc = c.hlc()?;
                    let is_put = c.u8()? == 1;
                    Some((value, hlc, is_put))
                } else {
                    None
                };
                Response::Get { entry }
            }
            RES_KEYS => {
                let n = c.u32()? as usize;
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(c.bytes()?);
                }
                Response::Keys { keys }
            }
            RES_VHITS => {
                let n = c.u32()? as usize;
                let mut hits = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = c.bytes()?;
                    hits.push((key, c.f32()?));
                }
                Response::VectorHits { hits }
            }
            RES_ERR => Response::Err(c.string()?),
            RES_PONG => Response::Pong,
            _ => return Err(WireError::Malformed("unknown response op")),
        })
    }
}

/// Payloads at or above this size are LZ4-compressed on the wire. Small frames
/// (acks, point writes/reads) stay raw — compression would only add overhead.
const COMPRESS_THRESHOLD: usize = 256;

/// Wrap a raw message payload in a compression envelope so the peer can tell
/// whether to decompress: `[codec u8] [u32 uncompressed_len if codec!=None] [body]`.
///
/// LZ4 is used (fast, cheap on the small cores nodes run on); a payload that
/// doesn't shrink, or is below [`COMPRESS_THRESHOLD`], is sent uncompressed.
pub(crate) fn frame_encode(payload: &[u8]) -> Vec<u8> {
    if payload.len() >= COMPRESS_THRESHOLD {
        let comp = compress(Codec::Lz4, payload);
        if comp.len() + 5 < payload.len() {
            let mut out = Vec::with_capacity(comp.len() + 5);
            out.push(Codec::Lz4.to_u8());
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&comp);
            return out;
        }
    }
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(Codec::None.to_u8());
    out.extend_from_slice(payload);
    out
}

/// Reverse of [`frame_encode`]: recover the raw message payload.
pub(crate) fn frame_decode(framed: &[u8]) -> Result<Vec<u8>, WireError> {
    let (&tag, rest) = framed
        .split_first()
        .ok_or(WireError::Malformed("empty frame"))?;
    match Codec::from_u8(tag) {
        Some(Codec::None) => Ok(rest.to_vec()),
        Some(codec) => {
            let len_bytes = rest
                .get(..4)
                .ok_or(WireError::Malformed("short compressed frame"))?;
            let ulen = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            decompress(codec, &rest[4..], ulen).map_err(|_| WireError::Malformed("decompress"))
        }
        None => Err(WireError::Malformed("unknown wire codec")),
    }
}

/// Send one request to `addr` and read the response (a fresh connection).
pub fn call(addr: &str, req: &Request) -> io::Result<Response> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    roundtrip(&mut stream, req)
}

fn roundtrip(stream: &mut TcpStream, req: &Request) -> io::Result<Response> {
    write_frame(stream, &frame_encode(&req.encode()))?;
    let framed = read_frame(stream)?;
    let payload =
        frame_decode(&framed).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Response::decode(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

/// Max idle connections kept per peer.
const MAX_IDLE_PER_PEER: usize = 32;

/// A pool of persistent internode connections, keyed by peer address. Reuses an
/// idle connection when available (the server keeps connections alive across
/// requests), avoiding a TCP handshake per replicated write.
#[derive(Debug, Default)]
pub struct Pool {
    idle: std::sync::Mutex<std::collections::HashMap<String, Vec<TcpStream>>>,
}

impl Pool {
    pub fn new() -> Self {
        Pool::default()
    }

    /// Send `req` to `addr`, reusing a pooled connection if possible. On any I/O
    /// error the connection is dropped (not returned to the pool).
    pub fn call(&self, addr: &str, req: &Request) -> io::Result<Response> {
        let mut stream = self.take(addr)?;
        let resp = roundtrip(&mut stream, req)?;
        self.put(addr, stream);
        Ok(resp)
    }

    fn take(&self, addr: &str) -> io::Result<TcpStream> {
        if let Some(stream) = self
            .idle
            .lock()
            .expect("pool lock")
            .get_mut(addr)
            .and_then(|v| v.pop())
        {
            return Ok(stream);
        }
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        Ok(stream)
    }

    fn put(&self, addr: &str, stream: TcpStream) {
        let mut idle = self.idle.lock().expect("pool lock");
        let bucket = idle.entry(addr.to_string()).or_default();
        if bucket.len() < MAX_IDLE_PER_PEER {
            bucket.push(stream);
        }
    }
}

fn put_str(o: &mut Vec<u8>, s: &str) {
    put_bytes(o, s.as_bytes());
}
fn put_bytes(o: &mut Vec<u8>, b: &[u8]) {
    o.extend_from_slice(&(b.len() as u32).to_le_bytes());
    o.extend_from_slice(b);
}
/// An optional byte string: a presence flag followed by the bytes when present.
fn put_opt_bytes(o: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        Some(bytes) => {
            o.push(1);
            put_bytes(o, bytes);
        }
        None => o.push(0),
    }
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
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32, WireError> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, WireError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, WireError> {
        Ok(if self.u8()? == 1 {
            Some(self.bytes()?)
        } else {
            None
        })
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
            Request::LocalGet {
                table: "t".into(),
                key: vec![7, 8, 9],
            },
            Request::ApplyDdl {
                sql: "CREATE TABLE t (PRIMARY KEY (id))".into(),
            },
            Request::IndexScan {
                index: "t_age".into(),
                start: Some(vec![1, 2, 3]),
                end: None,
            },
            Request::IndexScan {
                index: "t_age".into(),
                start: None,
                end: Some(vec![9]),
            },
            Request::VectorSearch {
                index: "t_emb".into(),
                query: vec![0.1, -0.2, 0.3, 0.0],
                k: 10,
            },
            Request::SetMembership {
                epoch: 7,
                members: vec![
                    ("a".into(), "127.0.0.1:1".into()),
                    ("b".into(), "127.0.0.1:2".into()),
                ],
            },
            Request::Rebalance {
                joiner: "c".into(),
            },
            Request::Drain {
                members: vec![
                    ("a".into(), "127.0.0.1:1".into()),
                    ("b".into(), "127.0.0.1:2".into()),
                ],
            },
            Request::Reclaim,
            Request::Ping,
        ] {
            assert_eq!(Request::decode(&req.encode()).unwrap(), req);
        }
    }

    #[test]
    fn response_roundtrips() {
        let scan = Response::Scan {
            rows: vec![
                (vec![1], vec![2, 3], Hlc::new(5, 0), true),
                (vec![4], vec![], Hlc::new(6, 2), false),
            ],
        };
        for res in [
            Response::Ack,
            scan,
            Response::Keys {
                keys: vec![vec![1], vec![2, 3], vec![]],
            },
            Response::VectorHits {
                hits: vec![(vec![1], 0.0), (vec![2, 3], 1.5)],
            },
            Response::Get {
                entry: Some((vec![1, 2, 3], Hlc::new(9, 1), true)),
            },
            Response::Get {
                entry: Some((vec![], Hlc::new(9, 2), false)),
            },
            Response::Get { entry: None },
            Response::Err("x".into()),
            Response::Pong,
        ] {
            assert_eq!(Response::decode(&res.encode()).unwrap(), res);
        }
    }

    #[test]
    fn frame_envelope_roundtrips_small_and_large() {
        // Small payload stays raw; large compressible payload is LZ4'd.
        let small = vec![1u8, 2, 3];
        let large = b"row-row-row-your-boat ".repeat(64);

        let small_framed = frame_encode(&small);
        assert_eq!(small_framed[0], Codec::None.to_u8());
        assert_eq!(frame_decode(&small_framed).unwrap(), small);

        let large_framed = frame_encode(&large);
        assert_eq!(large_framed[0], Codec::Lz4.to_u8());
        assert!(large_framed.len() < large.len(), "large frame should shrink");
        assert_eq!(frame_decode(&large_framed).unwrap(), large);
    }

    #[test]
    fn frame_decode_rejects_garbage() {
        assert!(frame_decode(&[]).is_err());
        assert!(frame_decode(&[99]).is_err()); // unknown codec tag
    }

    #[test]
    fn large_scan_response_survives_frame_envelope() {
        let rows: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)> = (0..200)
            .map(|i| {
                (
                    format!("key{i:04}").into_bytes(),
                    format!("a fairly repetitive value number {i}").into_bytes(),
                    Hlc::new(i as u64, 0),
                    i % 7 != 0, // sprinkle in some tombstones
                )
            })
            .collect();
        let res = Response::Scan { rows };
        let framed = frame_encode(&res.encode());
        let payload = frame_decode(&framed).unwrap();
        assert_eq!(Response::decode(&payload).unwrap(), res);
    }
}
