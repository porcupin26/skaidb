//! Lossless binary serialization for [`Value`].
//!
//! Distinct from [`Value::encode_key`], which is an order-preserving (and for
//! decimals lossy) *key* encoding. This codec round-trips every variant exactly
//! and is used to store row documents on disk and ship them on the wire.
//!
//! All integers are little-endian; variable-length parts carry a `u32` length.

use crate::value::{Decimal, Document, Uuid, Value, ValueError};

mod tag {
    pub const NULL: u8 = 0;
    pub const BOOL: u8 = 1;
    pub const INT: u8 = 2;
    pub const FLOAT: u8 = 3;
    pub const DECIMAL: u8 = 4;
    pub const STRING: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const UUID: u8 = 7;
    pub const TIMESTAMP: u8 = 8;
    pub const ARRAY: u8 = 9;
    pub const DOCUMENT: u8 = 10;
}

impl Value {
    /// Serialize losslessly to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_value(&mut out);
        out
    }

    /// Serialize losslessly (the [`Value::encode`] format), appending to `out`
    /// — no intermediate allocation. Distinct from [`Value::encode_into`],
    /// which appends the order-preserving *key* encoding.
    pub fn encode_value_into(&self, out: &mut Vec<u8>) {
        self.encode_value(out);
    }

    /// Encode a borrowed document exactly as `Value::Document(doc).encode()`
    /// would, without cloning the document into a `Value` first.
    pub fn encode_document(doc: &Document) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(tag::DOCUMENT);
        out.extend_from_slice(&(doc.0.len() as u32).to_le_bytes());
        for (k, v) in &doc.0 {
            write_bytes(&mut out, k.as_bytes());
            v.encode_value(&mut out);
        }
        out
    }

    /// Deserialize a value previously produced by [`Value::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Value, ValueError> {
        let mut cur = Cursor { bytes, pos: 0 };
        let v = decode_value(&mut cur)?;
        if cur.pos != bytes.len() {
            return Err(ValueError::UnrepresentableJson(
                "trailing bytes after value",
            ));
        }
        Ok(v)
    }

    fn encode_value(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(tag::NULL),
            Value::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Value::Int(i) => {
                out.push(tag::INT);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                out.push(tag::FLOAT);
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
            Value::Decimal(d) => {
                out.push(tag::DECIMAL);
                out.extend_from_slice(&d.mantissa.to_le_bytes());
                out.extend_from_slice(&d.scale.to_le_bytes());
            }
            Value::String(s) => {
                out.push(tag::STRING);
                write_bytes(out, s.as_bytes());
            }
            Value::Bytes(b) => {
                out.push(tag::BYTES);
                write_bytes(out, b);
            }
            Value::Uuid(u) => {
                out.push(tag::UUID);
                out.extend_from_slice(&u.0);
            }
            Value::Timestamp(t) => {
                out.push(tag::TIMESTAMP);
                out.extend_from_slice(&t.to_le_bytes());
            }
            Value::Array(items) => {
                out.push(tag::ARRAY);
                out.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for item in items {
                    item.encode_value(out);
                }
            }
            Value::Document(doc) => {
                out.push(tag::DOCUMENT);
                out.extend_from_slice(&(doc.0.len() as u32).to_le_bytes());
                for (k, v) in &doc.0 {
                    write_bytes(out, k.as_bytes());
                    v.encode_value(out);
                }
            }
        }
    }
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], ValueError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ValueError::UnrepresentableJson("length overflow"))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(ValueError::UnrepresentableJson("unexpected end of input"))?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, ValueError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ValueError> {
        let mut b = [0u8; 4];
        b.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(b))
    }

    fn i64(&mut self) -> Result<i64, ValueError> {
        let mut b = [0u8; 8];
        b.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(b))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, ValueError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String, ValueError> {
        String::from_utf8(self.bytes()?)
            .map_err(|_| ValueError::UnrepresentableJson("invalid utf-8 in string"))
    }
}

fn decode_value(cur: &mut Cursor<'_>) -> Result<Value, ValueError> {
    let t = cur.u8()?;
    Ok(match t {
        tag::NULL => Value::Null,
        tag::BOOL => Value::Bool(cur.u8()? != 0),
        tag::INT => Value::Int(cur.i64()?),
        tag::FLOAT => Value::Float(f64::from_bits(cur.i64()? as u64)),
        tag::DECIMAL => {
            let mut m = [0u8; 16];
            m.copy_from_slice(cur.take(16)?);
            let scale = cur.u32()?;
            Value::Decimal(Decimal::new(i128::from_le_bytes(m), scale))
        }
        tag::STRING => Value::String(cur.string()?),
        tag::BYTES => Value::Bytes(cur.bytes()?),
        tag::UUID => {
            let mut u = [0u8; 16];
            u.copy_from_slice(cur.take(16)?);
            Value::Uuid(Uuid(u))
        }
        tag::TIMESTAMP => Value::Timestamp(cur.i64()?),
        tag::ARRAY => {
            let n = cur.u32()? as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(decode_value(cur)?);
            }
            Value::Array(items)
        }
        tag::DOCUMENT => {
            let n = cur.u32()? as usize;
            let mut doc = Document::new();
            for _ in 0..n {
                let key = cur.string()?;
                let val = decode_value(cur)?;
                doc.insert(key, val);
            }
            Value::Document(doc)
        }
        _ => return Err(ValueError::UnrepresentableJson("unknown value tag")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: Value) {
        let bytes = v.encode();
        assert_eq!(Value::decode(&bytes).unwrap(), v);
    }

    #[test]
    fn roundtrips_all_variants() {
        roundtrip(Value::Null);
        roundtrip(Value::Bool(true));
        roundtrip(Value::Int(-42));
        roundtrip(Value::Float(3.5));
        roundtrip(Value::Decimal(Decimal::new(12345, 2)));
        roundtrip(Value::String("héllo".into()));
        roundtrip(Value::Bytes(vec![0, 1, 2, 255]));
        roundtrip(Value::Uuid(
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        ));
        roundtrip(Value::Timestamp(1_700_000_000_000));
        roundtrip(Value::Array(vec![
            Value::Int(1),
            Value::Null,
            Value::Bool(false),
        ]));
    }

    #[test]
    fn roundtrips_nested_document() {
        let mut inner = Document::new();
        inner.insert("x", Value::Int(1));
        inner.insert("y", Value::Array(vec![Value::String("a".into())]));
        let mut doc = Document::new();
        doc.insert("id", Value::Int(7));
        doc.insert("nested", Value::Document(inner));
        roundtrip(Value::Document(doc));
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let mut bytes = Value::Int(1).encode();
        bytes.push(0xFF);
        assert!(Value::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncated() {
        let bytes = Value::Int(1).encode();
        assert!(Value::decode(&bytes[..bytes.len() - 1]).is_err());
    }
}
