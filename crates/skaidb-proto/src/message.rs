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
}

/// Errors decoding a protocol message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("malformed message: {0}")]
    Malformed(&'static str),
}

const OP_QUERY: u8 = 1;
const RESP_ROWS: u8 = 0;
const RESP_MUTATION: u8 = 1;
const RESP_DDL: u8 = 2;
const RESP_ERROR: u8 = 3;

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
                out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
                for col in columns {
                    write_bytes(out, col.as_bytes());
                }
                out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for row in rows {
                    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
                    for v in row {
                        // Encode in place behind a backfilled length prefix —
                        // no per-cell temporary buffer.
                        let len_pos = out.len();
                        out.extend_from_slice(&[0u8; 4]);
                        v.encode_value_into(out);
                        let len = (out.len() - len_pos - 4) as u32;
                        out[len_pos..len_pos + 4].copy_from_slice(&len.to_le_bytes());
                    }
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
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Response, ProtoError> {
        let mut c = Cursor::new(buf);
        let tag = c.u8()?;
        Ok(match tag {
            RESP_ROWS => {
                let ncols = c.u32()? as usize;
                let mut columns = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    columns.push(c.string()?);
                }
                let nrows = c.u32()? as usize;
                let mut rows = Vec::with_capacity(nrows);
                for _ in 0..nrows {
                    let cells = c.u32()? as usize;
                    let mut row = Vec::with_capacity(cells);
                    for _ in 0..cells {
                        let bytes = c.bytes()?;
                        let v = Value::decode(bytes)
                            .map_err(|_| ProtoError::Malformed("bad value encoding"))?;
                        row.push(v);
                    }
                    rows.push(row);
                }
                Response::Rows { columns, rows }
            }
            RESP_MUTATION => Response::Mutation { affected: c.u64()? },
            RESP_DDL => Response::Ddl,
            RESP_ERROR => Response::Error(c.string()?),
            _ => return Err(ProtoError::Malformed("unknown response tag")),
        })
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
    fn decode_rejects_truncated() {
        let bytes = Response::Mutation { affected: 1 }.encode();
        assert!(Response::decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
