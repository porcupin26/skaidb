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

    /// Deserialize a **top-level document**, materializing only the fields
    /// named in `wanted` (a full document's field names — not paths into
    /// nested documents; nested/array *values* under a wanted top-level key
    /// still decode in full). Fields not in `wanted` are skip-parsed: their
    /// bytes are walked to find the next field's boundary, but never
    /// allocated into a `Value` — this is the whole point. `bytes` must be a
    /// document encoding (as [`Value::encode_document`] produces); anything
    /// else errors, matching [`Value::decode`]'s contract for non-document
    /// top-level values.
    ///
    /// Correctness rests entirely on `wanted` being a superset of every
    /// column the caller will actually read from the result — this function
    /// has no way to know that on its own. A field missing from the
    /// returned `Document` because it was never in `wanted` looks
    /// indistinguishable from a field the source row never had.
    pub fn decode_document_projected(
        bytes: &[u8],
        wanted: &std::collections::HashSet<String>,
    ) -> Result<Document, ValueError> {
        let mut cur = Cursor { bytes, pos: 0 };
        let t = cur.u8()?;
        if t != tag::DOCUMENT {
            return Err(ValueError::UnrepresentableJson(
                "decode_document_projected: not a document",
            ));
        }
        let n = cur.u32()? as usize;
        let mut doc = Document::new();
        for _ in 0..n {
            let key = cur.string()?;
            if wanted.contains(&key) {
                doc.insert(key, decode_value(&mut cur)?);
            } else {
                skip_value(&mut cur)?;
            }
        }
        if cur.pos != bytes.len() {
            return Err(ValueError::UnrepresentableJson(
                "trailing bytes after value",
            ));
        }
        Ok(doc)
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

/// Advance `cur` past one encoded value without allocating it — the same
/// shape as [`decode_value`], but a fixed-size `take`/skip in place of each
/// `Vec`/`String`/`Document` construction. Used by
/// [`Value::decode_document_projected`] to walk past fields the caller
/// doesn't want without paying their decode cost (the entire point: a large
/// unwanted `String`/`Bytes` field skips in O(1) beyond reading its length).
fn skip_value(cur: &mut Cursor<'_>) -> Result<(), ValueError> {
    let t = cur.u8()?;
    match t {
        tag::NULL => {}
        tag::BOOL => {
            cur.u8()?;
        }
        tag::INT | tag::FLOAT | tag::TIMESTAMP => {
            cur.i64()?;
        }
        tag::DECIMAL => {
            cur.take(16)?;
            cur.u32()?;
        }
        tag::STRING | tag::BYTES => {
            let len = cur.u32()? as usize;
            cur.take(len)?;
        }
        tag::UUID => {
            cur.take(16)?;
        }
        tag::ARRAY => {
            let n = cur.u32()? as usize;
            for _ in 0..n {
                skip_value(cur)?;
            }
        }
        tag::DOCUMENT => {
            let n = cur.u32()? as usize;
            for _ in 0..n {
                let len = cur.u32()? as usize; // key length prefix, no UTF-8
                cur.take(len)?; // check/decode needed — the key is discarded
                skip_value(cur)?;
            }
        }
        _ => return Err(ValueError::UnrepresentableJson("unknown value tag")),
    }
    Ok(())
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

    fn wide_doc() -> Document {
        let mut doc = Document::new();
        doc.insert("id", Value::Int(7));
        doc.insert("account", Value::String("alice".into()));
        doc.insert("body", Value::String("x".repeat(10_000))); // the field we want to skip
        doc.insert("tags", Value::Array(vec![Value::String("a".into()), Value::Int(2)]));
        doc.insert(
            "meta",
            Value::Document({
                let mut m = Document::new();
                m.insert("nested_big", Value::String("y".repeat(5_000)));
                m.insert("nested_small", Value::Int(1));
                m
            }),
        );
        doc.insert("flag", Value::Bool(true));
        doc.insert("amount", Value::Decimal(Decimal::new(12345, 2)));
        doc.insert("when", Value::Timestamp(1_700_000_000_000));
        doc.insert("nothing", Value::Null);
        doc
    }

    #[test]
    fn projected_decode_returns_only_wanted_fields() {
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let wanted: std::collections::HashSet<String> =
            ["id".to_string(), "account".to_string()].into_iter().collect();
        let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
        assert_eq!(got.get("id"), Some(&Value::Int(7)));
        assert_eq!(got.get("account"), Some(&Value::String("alice".into())));
        // Every skipped field is simply absent — not null, not present-but-empty.
        assert_eq!(got.get("body"), None);
        assert_eq!(got.get("tags"), None);
        assert_eq!(got.get("meta"), None);
        assert_eq!(got.get("flag"), None);
        assert_eq!(got.0.len(), 2);
    }

    #[test]
    fn projected_decode_wanted_set_covers_every_value_type() {
        // Wanting every field must reproduce the exact full decode — proves
        // the skip-path's cursor bookkeeping for every tag stays in sync
        // with decode_value's (a single off-by-one here would corrupt every
        // field after the first skipped one, not just the skipped one).
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let wanted: std::collections::HashSet<String> = doc.0.keys().cloned().collect();
        let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
        assert_eq!(got, doc);
    }

    #[test]
    fn projected_decode_wanting_nothing_returns_empty_document() {
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let wanted: std::collections::HashSet<String> = std::collections::HashSet::new();
        let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
        assert!(got.0.is_empty());
    }

    #[test]
    fn projected_decode_wanting_a_field_the_doc_lacks_is_fine() {
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let wanted: std::collections::HashSet<String> =
            ["id".to_string(), "does_not_exist".to_string()].into_iter().collect();
        let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
        assert_eq!(got.get("id"), Some(&Value::Int(7)));
        assert_eq!(got.0.len(), 1);
    }

    #[test]
    fn projected_decode_skips_every_field_around_a_wanted_one() {
        // The wanted field isn't first or last — proves skip correctly
        // resumes the cursor so a middle field decodes correctly regardless
        // of what was skipped before or after it.
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let wanted: std::collections::HashSet<String> = ["flag".to_string()].into_iter().collect();
        let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
        assert_eq!(got.get("flag"), Some(&Value::Bool(true)));
        assert_eq!(got.0.len(), 1);
    }

    #[test]
    fn projected_decode_rejects_non_document_top_level() {
        let bytes = Value::Int(1).encode();
        let wanted: std::collections::HashSet<String> = std::collections::HashSet::new();
        assert!(Value::decode_document_projected(&bytes, &wanted).is_err());
    }

    #[test]
    fn projected_decode_rejects_truncated_skip_region() {
        // Torn bytes inside a SKIPPED field's payload must still error, not
        // silently succeed with a short skip — a truncated big text field
        // must not be treated as an empty one.
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let truncated = &bytes[..bytes.len() - 20]; // cuts into the trailing fields
        let wanted: std::collections::HashSet<String> = ["id".to_string()].into_iter().collect();
        assert!(Value::decode_document_projected(truncated, &wanted).is_err());
    }

    #[test]
    fn projected_decode_matches_full_decode_over_every_field_subset() {
        // Exhaustive over every possible wanted-subset (2^9 for this doc's 9
        // fields): whatever the projected path returns must be a byte-exact
        // subset of the fully decoded document, for every combination of
        // which fields get skipped around which — no subset-specific
        // cursor-desync bug is invisible here.
        let doc = wide_doc();
        let bytes = Value::encode_document(&doc);
        let all_keys: Vec<String> = doc.0.keys().cloned().collect();
        for mask in 0..(1u32 << all_keys.len()) {
            let wanted: std::collections::HashSet<String> = all_keys
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, k)| k.clone())
                .collect();
            let got = Value::decode_document_projected(&bytes, &wanted).unwrap();
            for k in &wanted {
                assert_eq!(got.get(k), doc.get(k), "mask {mask:#b} key {k}");
            }
            assert_eq!(got.0.len(), wanted.len(), "mask {mask:#b}");
        }
    }
}
