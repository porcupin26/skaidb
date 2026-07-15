//! Request/response message encoding for the binary protocol.
//!
//! Payloads are self-describing byte buffers (see field layouts inline). Values
//! reuse the lossless [`Value`] codec from `skaidb-types`.

use skaidb_types::Value;

/// Consistency level requested for an operation (SPEC §5). A single-node server
/// accepts but does not need it; it travels for cluster routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    One,
    Quorum,
    All,
}

impl Consistency {
    fn to_u8(self) -> u8 {
        match self {
            Consistency::One => 0,
            Consistency::Quorum => 1,
            Consistency::All => 2,
        }
    }

    fn from_u8(b: u8) -> Option<Consistency> {
        match b {
            0 => Some(Consistency::One),
            1 => Some(Consistency::Quorum),
            2 => Some(Consistency::All),
            _ => None,
        }
    }
}

/// A client request: a SQL statement plus the desired consistency.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub sql: String,
    pub consistency: Consistency,
}

/// A client request, including the prepared-statement operations. The
/// one-shot [`ClientRequest::Query`] is wire-identical to the original
/// [`Request`], so old clients keep working against new servers; the other
/// opcodes are rejected as unknown by old servers.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientRequest {
    /// Parse and execute one SQL statement.
    Query { sql: String, consistency: Consistency },
    /// Parse `sql` once (it may contain `?` placeholders) and cache it on
    /// this connection; answered with [`Response::Prepared`].
    Prepare { sql: String },
    /// Execute a statement prepared on this connection, binding `params` to
    /// its `?` placeholders in order.
    Execute {
        id: u32,
        params: Vec<Value>,
        consistency: Consistency,
    },
    /// Discard a prepared statement, freeing its slot. Acked with
    /// [`Response::Ddl`] (no payload).
    Close { id: u32 },
    /// Like `Query`, but a row-returning result comes back as a stream of
    /// frames — [`Response::RowsHeader`], then any number of
    /// [`Response::RowsChunk`], then [`Response::RowsEnd`] — so neither side
    /// ever holds the whole encoded result set. Non-row results are answered
    /// with a single ordinary frame. Old servers reject the opcode with
    /// [`Response::Error`], which callers surface as "server too old".
    QueryStream { sql: String, consistency: Consistency },
    /// Execute a prepared statement once per parameter row — the wire form of
    /// `executemany`: one round-trip for a whole bulk backfill instead of one
    /// per row. Each row autocommits like a looped `Execute`; on a failure
    /// the reply is an error naming the row index and earlier rows stay
    /// applied. Answered with [`Response::Mutation`] carrying the total
    /// affected count. Old servers reject the opcode with
    /// [`Response::Error`], which drivers use to fall back to the loop.
    ExecuteBatch {
        id: u32,
        rows: Vec<Vec<Value>>,
        consistency: Consistency,
    },
}

/// A server response.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// A result set from `SELECT`.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// A DML statement affected `affected` rows.
    Mutation { affected: u64 },
    /// A DDL statement succeeded.
    Ddl,
    /// The statement failed.
    Error(String),
    /// Reply to [`ClientRequest::Prepare`]: the statement handle and how many
    /// `?` parameters it expects.
    Prepared { id: u32, params: u16 },
    /// First frame of a streamed result set: the column names.
    RowsHeader { columns: Vec<String> },
    /// One batch of rows of a streamed result set.
    RowsChunk { rows: Vec<Vec<Value>> },
    /// Terminator of a streamed result set.
    RowsEnd,
}

/// Errors decoding a protocol message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("malformed message: {0}")]
    Malformed(&'static str),
}

const OP_QUERY: u8 = 1;
const OP_PREPARE: u8 = 2;
const OP_EXECUTE: u8 = 3;
const OP_CLOSE: u8 = 4;
const OP_QUERY_STREAM: u8 = 5;
/// Wrapper opcode for pipelining: `u8 OP_TAGGED | u32 id | <inner request>`.
/// The server echoes the id on every frame it sends for this request, so a
/// client may have any number of tagged requests in flight on one connection
/// and correlate responses by id rather than by arrival order. Old servers
/// reject the opcode with an (untagged) [`Response::Error`], which pipelining
/// clients surface as "server too old".
const OP_TAGGED: u8 = 6;
/// `u8 OP_EXECUTE_BATCH | u8 consistency | u32 id | u32 nrows |
///  nrows × (u16 nparams | nparams × (u32 len | value bytes))`.
const OP_EXECUTE_BATCH: u8 = 7;
const RESP_ROWS: u8 = 0;
const RESP_MUTATION: u8 = 1;
const RESP_DDL: u8 = 2;
const RESP_ERROR: u8 = 3;
const RESP_PREPARED: u8 = 4;
const RESP_ROWS_HEADER: u8 = 5;
const RESP_ROWS_CHUNK: u8 = 6;
const RESP_ROWS_END: u8 = 7;
/// Wrapper tag for responses to [`OP_TAGGED`] requests:
/// `u8 RESP_TAGGED | u32 id | <inner response>`.
const RESP_TAGGED: u8 = 8;

/// Encode a pipelined (id-tagged) request into `out`:
/// the [`OP_TAGGED`] wrapper followed by the ordinary request encoding.
pub fn encode_tagged_request(id: u32, req: &ClientRequest, out: &mut Vec<u8>) {
    out.push(OP_TAGGED);
    out.extend_from_slice(&id.to_le_bytes());
    req.encode_into(out);
}

/// Decode a request that may carry the pipelining wrapper: returns the id
/// (`None` for a plain, untagged request) and the inner request. Nested
/// wrappers are rejected.
pub fn decode_client_request(buf: &[u8]) -> Result<(Option<u32>, ClientRequest), ProtoError> {
    if buf.first() == Some(&OP_TAGGED) {
        if buf.len() < 6 {
            return Err(ProtoError::Malformed("unexpected end"));
        }
        let id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        if buf.get(5) == Some(&OP_TAGGED) {
            return Err(ProtoError::Malformed("nested tagged request"));
        }
        Ok((Some(id), ClientRequest::decode(&buf[5..])?))
    } else {
        Ok((None, ClientRequest::decode(buf)?))
    }
}

/// Begin a tagged response: push the [`RESP_TAGGED`] wrapper; the ordinary
/// response encoding follows it in the same buffer.
pub fn tag_response(out: &mut Vec<u8>, id: u32) {
    out.push(RESP_TAGGED);
    out.extend_from_slice(&id.to_le_bytes());
}

/// Decode a response that may carry the pipelining wrapper: returns the
/// echoed id (`None` for a plain response) and the inner response.
pub fn decode_tagged_response(buf: &[u8]) -> Result<(Option<u32>, Response), ProtoError> {
    if buf.first() == Some(&RESP_TAGGED) {
        if buf.len() < 6 {
            return Err(ProtoError::Malformed("unexpected end"));
        }
        let id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        Ok((Some(id), Response::decode(&buf[5..])?))
    } else {
        Ok((None, Response::decode(buf)?))
    }
}

impl Request {
    /// Encode: `u8 OP_QUERY | u8 consistency | u32 sql_len | sql`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.sql.len() + 6);
        out.push(OP_QUERY);
        out.push(self.consistency.to_u8());
        write_bytes(&mut out, self.sql.as_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Request, ProtoError> {
        let mut c = Cursor::new(buf);
        let op = c.u8()?;
        if op != OP_QUERY {
            return Err(ProtoError::Malformed("unknown opcode"));
        }
        let consistency =
            Consistency::from_u8(c.u8()?).ok_or(ProtoError::Malformed("bad consistency"))?;
        let sql = c.string()?;
        Ok(Request { sql, consistency })
    }
}

impl ClientRequest {
    /// Encode:
    /// - Query:   `u8 OP_QUERY | u8 consistency | u32 sql_len | sql`
    /// - Prepare: `u8 OP_PREPARE | u32 sql_len | sql`
    /// - Execute: `u8 OP_EXECUTE | u8 consistency | u32 id | u16 nparams |
    ///             nparams × (u32 len | value bytes)`
    /// - Close:   `u8 OP_CLOSE | u32 id`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Encode appending to `out` (reusable per-connection buffer).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            ClientRequest::Query { sql, consistency } => {
                out.push(OP_QUERY);
                out.push(consistency.to_u8());
                write_bytes(out, sql.as_bytes());
            }
            ClientRequest::Prepare { sql } => {
                out.push(OP_PREPARE);
                write_bytes(out, sql.as_bytes());
            }
            ClientRequest::Execute {
                id,
                params,
                consistency,
            } => {
                out.push(OP_EXECUTE);
                out.push(consistency.to_u8());
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&(params.len() as u16).to_le_bytes());
                for v in params {
                    // Value encoded in place behind a backfilled length
                    // prefix, as in Response::Rows.
                    let len_pos = out.len();
                    out.extend_from_slice(&[0u8; 4]);
                    v.encode_value_into(out);
                    let len = (out.len() - len_pos - 4) as u32;
                    out[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
                }
            }
            ClientRequest::Close { id } => {
                out.push(OP_CLOSE);
                out.extend_from_slice(&id.to_le_bytes());
            }
            ClientRequest::QueryStream { sql, consistency } => {
                out.push(OP_QUERY_STREAM);
                out.push(consistency.to_u8());
                write_bytes(out, sql.as_bytes());
            }
            ClientRequest::ExecuteBatch {
                id,
                rows,
                consistency,
            } => {
                out.push(OP_EXECUTE_BATCH);
                out.push(consistency.to_u8());
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for params in rows {
                    out.extend_from_slice(&(params.len() as u16).to_le_bytes());
                    for v in params {
                        let len_pos = out.len();
                        out.extend_from_slice(&[0u8; 4]);
                        v.encode_value_into(out);
                        let len = (out.len() - len_pos - 4) as u32;
                        out[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
                    }
                }
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Result<ClientRequest, ProtoError> {
        let mut c = Cursor::new(buf);
        Ok(match c.u8()? {
            OP_QUERY => {
                let consistency = Consistency::from_u8(c.u8()?)
                    .ok_or(ProtoError::Malformed("bad consistency"))?;
                ClientRequest::Query {
                    sql: c.string()?,
                    consistency,
                }
            }
            OP_PREPARE => ClientRequest::Prepare { sql: c.string()? },
            OP_EXECUTE => {
                let consistency = Consistency::from_u8(c.u8()?)
                    .ok_or(ProtoError::Malformed("bad consistency"))?;
                let id = c.u32()?;
                let nparams = u16::from_le_bytes(c.take(2)?.try_into().unwrap()) as usize;
                let mut params = Vec::with_capacity(nparams);
                for _ in 0..nparams {
                    let bytes = c.bytes()?;
                    params.push(
                        Value::decode(bytes)
                            .map_err(|_| ProtoError::Malformed("bad value encoding"))?,
                    );
                }
                ClientRequest::Execute {
                    id,
                    params,
                    consistency,
                }
            }
            OP_CLOSE => ClientRequest::Close { id: c.u32()? },
            OP_EXECUTE_BATCH => {
                let consistency = Consistency::from_u8(c.u8()?)
                    .ok_or(ProtoError::Malformed("bad consistency"))?;
                let id = c.u32()?;
                let nrows = c.u32()? as usize;
                let mut rows = Vec::with_capacity(nrows.min(1 << 16));
                for _ in 0..nrows {
                    let nparams =
                        u16::from_le_bytes(c.take(2)?.try_into().unwrap()) as usize;
                    let mut params = Vec::with_capacity(nparams);
                    for _ in 0..nparams {
                        let bytes = c.bytes()?;
                        params.push(
                            Value::decode(bytes)
                                .map_err(|_| ProtoError::Malformed("bad value encoding"))?,
                        );
                    }
                    rows.push(params);
                }
                ClientRequest::ExecuteBatch {
                    id,
                    rows,
                    consistency,
                }
            }
            OP_QUERY_STREAM => {
                let consistency = Consistency::from_u8(c.u8()?)
                    .ok_or(ProtoError::Malformed("bad consistency"))?;
                ClientRequest::QueryStream {
                    sql: c.string()?,
                    consistency,
                }
            }
            _ => return Err(ProtoError::Malformed("unknown opcode")),
        })
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Encode appending to `out`, so a per-connection buffer can be reused
    /// across responses instead of allocating one per message.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Response::Rows { columns, rows } => {
                out.push(RESP_ROWS);
                encode_columns(out, columns);
                out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for row in rows {
                    encode_row_into(out, row);
                }
            }
            Response::Mutation { affected } => {
                out.push(RESP_MUTATION);
                out.extend_from_slice(&affected.to_le_bytes());
            }
            Response::Ddl => out.push(RESP_DDL),
            Response::Error(msg) => {
                out.push(RESP_ERROR);
                write_bytes(out, msg.as_bytes());
            }
            Response::Prepared { id, params } => {
                out.push(RESP_PREPARED);
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&params.to_le_bytes());
            }
            Response::RowsHeader { columns } => {
                out.push(RESP_ROWS_HEADER);
                encode_columns(out, columns);
            }
            Response::RowsChunk { rows } => {
                out.push(RESP_ROWS_CHUNK);
                out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for row in rows {
                    encode_row_into(out, row);
                }
            }
            Response::RowsEnd => out.push(RESP_ROWS_END),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Response, ProtoError> {
        let mut c = Cursor::new(buf);
        let tag = c.u8()?;
        Ok(match tag {
            RESP_ROWS => {
                let columns = decode_columns(&mut c)?;
                let rows = decode_rows(&mut c)?;
                Response::Rows { columns, rows }
            }
            RESP_MUTATION => Response::Mutation { affected: c.u64()? },
            RESP_DDL => Response::Ddl,
            RESP_ERROR => Response::Error(c.string()?),
            RESP_PREPARED => Response::Prepared {
                id: c.u32()?,
                params: u16::from_le_bytes(c.take(2)?.try_into().unwrap()),
            },
            RESP_ROWS_HEADER => Response::RowsHeader {
                columns: decode_columns(&mut c)?,
            },
            RESP_ROWS_CHUNK => Response::RowsChunk {
                rows: decode_rows(&mut c)?,
            },
            RESP_ROWS_END => Response::RowsEnd,
            _ => return Err(ProtoError::Malformed("unknown response tag")),
        })
    }
}

fn encode_columns(out: &mut Vec<u8>, columns: &[String]) {
    out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for col in columns {
        write_bytes(out, col.as_bytes());
    }
}

/// Encode one row: `u32 ncells | ncells × (u32 len | value bytes)`. Each value
/// is encoded in place behind a backfilled length prefix — no per-cell
/// temporary buffer.
fn encode_row_into(out: &mut Vec<u8>, row: &[Value]) {
    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
    for v in row {
        let len_pos = out.len();
        out.extend_from_slice(&[0u8; 4]);
        v.encode_value_into(out);
        let len = (out.len() - len_pos - 4) as u32;
        out[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
    }
}

fn decode_columns(c: &mut Cursor) -> Result<Vec<String>, ProtoError> {
    let ncols = c.u32()? as usize;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        columns.push(c.string()?);
    }
    Ok(columns)
}

fn decode_rows(c: &mut Cursor) -> Result<Vec<Vec<Value>>, ProtoError> {
    let nrows = c.u32()? as usize;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let cells = c.u32()? as usize;
        let mut row = Vec::with_capacity(cells);
        for _ in 0..cells {
            let bytes = c.bytes()?;
            let v =
                Value::decode(bytes).map_err(|_| ProtoError::Malformed("bad value encoding"))?;
            row.push(v);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Incremental encoder for a [`Response::RowsChunk`] payload: lets a sender
/// append rows one at a time (watching the buffer size to pick its own chunk
/// boundary) without first splitting them into per-chunk `Vec`s. The encoding
/// is byte-identical to `Response::RowsChunk { rows }.encode_into(out)`.
#[derive(Debug)]
pub struct RowsChunkEncoder {
    count_pos: usize,
    count: u32,
}

impl RowsChunkEncoder {
    /// Start a chunk, appending to `out` (which may already hold frame-header
    /// bytes). Must be paired with [`RowsChunkEncoder::finish`].
    pub fn begin(out: &mut Vec<u8>) -> RowsChunkEncoder {
        out.push(RESP_ROWS_CHUNK);
        let count_pos = out.len();
        out.extend_from_slice(&[0u8; 4]);
        RowsChunkEncoder { count_pos, count: 0 }
    }

    pub fn push_row(&mut self, out: &mut Vec<u8>, row: &[Value]) {
        encode_row_into(out, row);
        self.count += 1;
    }

    /// Backfill the row count, completing the payload.
    pub fn finish(self, out: &mut [u8]) {
        out[self.count_pos..self.count_pos + 4].copy_from_slice(&self.count.to_le_bytes());
    }
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtoError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ProtoError::Malformed("length overflow"))?;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or(ProtoError::Malformed("unexpected end"))?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, ProtoError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ProtoError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ProtoError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], ProtoError> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, ProtoError> {
        let bytes = self.bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| ProtoError::Malformed("invalid utf-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = Request {
            sql: "SELECT * FROM t".into(),
            consistency: Consistency::Quorum,
        };
        assert_eq!(Request::decode(&req.encode()).unwrap(), req);
    }

    #[test]
    fn client_request_roundtrips() {
        for req in [
            ClientRequest::Query {
                sql: "SELECT 1".into(),
                consistency: Consistency::One,
            },
            ClientRequest::Prepare {
                sql: "INSERT INTO t (id, v) VALUES (?, ?)".into(),
            },
            ClientRequest::Execute {
                id: 3,
                params: vec![Value::Int(7), Value::String("x".into()), Value::Null],
                consistency: Consistency::Quorum,
            },
            ClientRequest::Execute {
                id: 0,
                params: vec![],
                consistency: Consistency::All,
            },
            ClientRequest::Close { id: 9 },
            ClientRequest::ExecuteBatch {
                id: 5,
                rows: vec![
                    vec![Value::Int(1), Value::Array(vec![Value::String("a".into())])],
                    vec![Value::Int(2), Value::Null],
                ],
                consistency: Consistency::Quorum,
            },
            ClientRequest::ExecuteBatch {
                id: 0,
                rows: vec![],
                consistency: Consistency::One,
            },
        ] {
            assert_eq!(ClientRequest::decode(&req.encode()).unwrap(), req);
        }
    }

    #[test]
    fn prepared_response_roundtrips() {
        let resp = Response::Prepared { id: 5, params: 2 };
        assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn query_wire_format_matches_legacy_request() {
        // Old clients send `Request`; new servers decode `ClientRequest`.
        let old = Request {
            sql: "SELECT * FROM t".into(),
            consistency: Consistency::Quorum,
        };
        let new = ClientRequest::decode(&old.encode()).unwrap();
        assert_eq!(
            new,
            ClientRequest::Query {
                sql: "SELECT * FROM t".into(),
                consistency: Consistency::Quorum,
            }
        );
    }

    #[test]
    fn response_rows_roundtrip() {
        let resp = Response::Rows {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Value::Int(1), Value::String("ada".into())],
                vec![Value::Int(2), Value::Null],
            ],
        };
        assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
    }

    #[test]
    fn response_variants_roundtrip() {
        for resp in [
            Response::Mutation { affected: 7 },
            Response::Ddl,
            Response::Error("boom".into()),
        ] {
            assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
        }
    }

    #[test]
    fn query_stream_request_roundtrips() {
        let req = ClientRequest::QueryStream {
            sql: "SELECT * FROM big".into(),
            consistency: Consistency::One,
        };
        assert_eq!(ClientRequest::decode(&req.encode()).unwrap(), req);
    }

    #[test]
    fn streamed_response_frames_roundtrip() {
        for resp in [
            Response::RowsHeader {
                columns: vec!["id".into(), "v".into()],
            },
            Response::RowsChunk {
                rows: vec![
                    vec![Value::Int(1), Value::String("a".into())],
                    vec![Value::Int(2), Value::Null],
                ],
            },
            Response::RowsChunk { rows: vec![] },
            Response::RowsEnd,
        ] {
            assert_eq!(Response::decode(&resp.encode()).unwrap(), resp);
        }
    }

    #[test]
    fn chunk_encoder_matches_rows_chunk_encoding() {
        let rows = vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Float(2.5), Value::Bool(true)],
        ];
        let mut incremental = Vec::new();
        let mut enc = RowsChunkEncoder::begin(&mut incremental);
        for row in &rows {
            enc.push_row(&mut incremental, row);
        }
        enc.finish(&mut incremental);
        assert_eq!(incremental, Response::RowsChunk { rows }.encode());
    }

    #[test]
    fn decode_rejects_truncated() {
        let bytes = Response::Mutation { affected: 1 }.encode();
        assert!(Response::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn tagged_request_roundtrips_and_rejects_nesting() {
        let inner = ClientRequest::Query {
            sql: "SELECT 1".into(),
            consistency: Consistency::Quorum,
        };
        let mut buf = Vec::new();
        encode_tagged_request(42, &inner, &mut buf);
        assert_eq!(decode_client_request(&buf).unwrap(), (Some(42), inner));

        // Plain requests pass through untagged.
        let plain = ClientRequest::Close { id: 1 };
        assert_eq!(
            decode_client_request(&plain.encode()).unwrap(),
            (None, plain)
        );

        // A wrapper inside a wrapper is malformed.
        let mut nested = vec![OP_TAGGED, 0, 0, 0, 0];
        nested.extend_from_slice(&buf);
        assert!(decode_client_request(&nested).is_err());
        assert!(decode_client_request(&buf[..5]).is_err());
    }

    #[test]
    fn tagged_response_roundtrips() {
        let inner = Response::Mutation { affected: 3 };
        let mut buf = Vec::new();
        tag_response(&mut buf, 7);
        inner.encode_into(&mut buf);
        assert_eq!(decode_tagged_response(&buf).unwrap(), (Some(7), inner));

        let plain = Response::Ddl;
        assert_eq!(
            decode_tagged_response(&plain.encode()).unwrap(),
            (None, plain)
        );
    }
}
