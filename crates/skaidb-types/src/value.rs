//! The skaidb value model (SPEC §2.1).
//!
//! Values are self-describing and dynamically typed: the database is schema-less,
//! so a row is just a [`Document`] (an ordered map of field name to [`Value`]).
//! Date/time is represented as unixtime in **milliseconds** as an `int64`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

/// A 128-bit UUID stored as raw bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    /// The nil UUID (all zero bytes).
    pub const NIL: Uuid = Uuid([0u8; 16]);

    /// Parse the canonical hyphenated form, e.g. `550e8400-e29b-41d4-a716-446655440000`.
    pub fn parse_str(s: &str) -> Result<Uuid, ValueError> {
        let mut bytes = [0u8; 16];
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 {
            return Err(ValueError::InvalidUuid(s.to_string()));
        }
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|_| ValueError::InvalidUuid(s.to_string()))?;
        }
        Ok(Uuid(bytes))
    }
}

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
        )
    }
}

/// A fixed-point decimal: `mantissa * 10^-scale`.
///
/// Phase 1 keeps an exact mantissa for arithmetic but orders decimals by their
/// `f64` projection in key encoding (see [`Value::encode_into`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    pub mantissa: i128,
    pub scale: u32,
}

impl Decimal {
    pub fn new(mantissa: i128, scale: u32) -> Self {
        Decimal { mantissa, scale }
    }

    /// Lossy projection to `f64`, used for ordering and arithmetic interop.
    pub fn to_f64(self) -> f64 {
        self.mantissa as f64 / 10f64.powi(self.scale as i32)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }
        let neg = self.mantissa < 0;
        let digits = self.mantissa.unsigned_abs().to_string();
        let scale = self.scale as usize;
        let padded = if digits.len() <= scale {
            format!("{:0>width$}", digits, width = scale + 1)
        } else {
            digits
        };
        let split = padded.len() - scale;
        write!(
            f,
            "{}{}.{}",
            if neg { "-" } else { "" },
            &padded[..split],
            &padded[split..]
        )
    }
}

/// An ordered map of field name to [`Value`]; the unit of a stored row.
///
/// Keys are kept sorted so that encodings are canonical and deterministic.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Document(pub BTreeMap<String, Value>);

impl Document {
    pub fn new() -> Self {
        Document(BTreeMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) -> &mut Self {
        self.0.insert(key.into(), value);
        self
    }

    /// Look up a field; a missing field is the caller's cue to treat it as `NULL`.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// Resolve a dotted field path like `a.b.c`, descending nested documents.
    pub fn get_path(&self, path: &str) -> Option<&Value> {
        // Top-level fields (no dot) are the common case in predicates and
        // projections; skip the segment machinery for them.
        match path.split_once('.') {
            None => self.get(path),
            Some((first, rest)) => {
                let mut doc = match self.get(first)? {
                    Value::Document(d) => d,
                    _ => return None,
                };
                let mut rest = rest;
                while let Some((part, tail)) = rest.split_once('.') {
                    match doc.get(part)? {
                        Value::Document(d) => doc = d,
                        _ => return None,
                    }
                    rest = tail;
                }
                doc.get(rest)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The dynamic type tag of a [`Value`]. Ordering matches the key-encoding order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueType {
    Null,
    Bool,
    Int,
    Float,
    Decimal,
    String,
    Bytes,
    Uuid,
    Timestamp,
    Array,
    Document,
}

/// A dynamically typed skaidb value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Uuid(Uuid),
    /// Unixtime in milliseconds.
    Timestamp(i64),
    Array(Vec<Value>),
    Document(Document),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(x) => write!(f, "{x}"),
            Value::Decimal(d) => write!(f, "{d}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Bytes(b) => {
                write!(f, "0x")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
            Value::Uuid(u) => write!(f, "{u}"),
            Value::Timestamp(t) => write!(f, "{t}"),
            // Composite values render as their JSON form for readability.
            Value::Array(_) | Value::Document(_) => write!(f, "{}", self.to_json()),
        }
    }
}

/// Type-tag byte prefixes for order-preserving key encoding. Distinct constants
/// keep the encoding stable even if [`ValueType`] variants are reordered.
mod tag {
    pub const NULL: u8 = 0x00;
    pub const BOOL: u8 = 0x01;
    pub const INT: u8 = 0x02;
    pub const FLOAT: u8 = 0x03;
    pub const DECIMAL: u8 = 0x04;
    pub const STRING: u8 = 0x05;
    pub const BYTES: u8 = 0x06;
    pub const UUID: u8 = 0x07;
    pub const TIMESTAMP: u8 = 0x08;
    pub const ARRAY: u8 = 0x09;
    pub const DOCUMENT: u8 = 0x0a;
    /// Terminator for variable-length / nested encodings.
    pub const END: u8 = 0x00;
}

impl Value {
    /// The dynamic type of this value.
    pub fn type_of(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::Int(_) => ValueType::Int,
            Value::Float(_) => ValueType::Float,
            Value::Decimal(_) => ValueType::Decimal,
            Value::String(_) => ValueType::String,
            Value::Bytes(_) => ValueType::Bytes,
            Value::Uuid(_) => ValueType::Uuid,
            Value::Timestamp(_) => ValueType::Timestamp,
            Value::Array(_) => ValueType::Array,
            Value::Document(_) => ValueType::Document,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Encode an order-preserving key for this value.
    ///
    /// Byte order over the result equals the logical order of values, so the
    /// storage engine can store keys as plain byte strings (SPEC §12).
    pub fn encode_key(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Append the order-preserving encoding of this value to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(tag::NULL),
            Value::Bool(b) => {
                out.push(tag::BOOL);
                out.push(u8::from(*b));
            }
            Value::Int(v) => {
                out.push(tag::INT);
                encode_i64(*v, out);
            }
            Value::Timestamp(v) => {
                out.push(tag::TIMESTAMP);
                encode_i64(*v, out);
            }
            Value::Float(v) => {
                out.push(tag::FLOAT);
                encode_f64(*v, out);
            }
            Value::Decimal(d) => {
                out.push(tag::DECIMAL);
                encode_f64(d.to_f64(), out);
            }
            Value::String(s) => {
                out.push(tag::STRING);
                encode_bytes(s.as_bytes(), out);
            }
            Value::Bytes(b) => {
                out.push(tag::BYTES);
                encode_bytes(b, out);
            }
            Value::Uuid(u) => {
                out.push(tag::UUID);
                out.extend_from_slice(&u.0);
            }
            Value::Array(items) => {
                out.push(tag::ARRAY);
                for item in items {
                    item.encode_into(out);
                }
                out.push(tag::END);
            }
            Value::Document(doc) => {
                out.push(tag::DOCUMENT);
                for (k, v) in &doc.0 {
                    encode_bytes(k.as_bytes(), out);
                    v.encode_into(out);
                }
                out.push(tag::END);
            }
        }
    }

    /// Total order over values consistent with [`Value::encode_key`].
    ///
    /// Unlike `PartialOrd` on `f64`, this is total (NaN is ordered by bit
    /// pattern), so it is safe to use as a sort key.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        self.encode_key().cmp(&other.encode_key())
    }
}

fn encode_i64(v: i64, out: &mut Vec<u8>) {
    // Flip the sign bit so two's-complement order becomes unsigned byte order.
    let u = (v as u64) ^ 0x8000_0000_0000_0000;
    out.extend_from_slice(&u.to_be_bytes());
}

fn encode_f64(v: f64, out: &mut Vec<u8>) {
    let bits = v.to_bits();
    // Order-preserving transform: positives flip the sign bit, negatives invert.
    let ord = if bits & 0x8000_0000_0000_0000 == 0 {
        bits | 0x8000_0000_0000_0000
    } else {
        !bits
    };
    out.extend_from_slice(&ord.to_be_bytes());
}

/// Encode a byte string with `0x00` escaped as `0x00 0xFF`, terminated by
/// `0x00 0x00`, so encodings remain prefix-free and order-preserving.
fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        if b == 0x00 {
            out.push(0x00);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0x00);
    out.push(0x00);
}

/// Errors produced when constructing or converting values.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ValueError {
    #[error("invalid UUID: {0}")]
    InvalidUuid(String),
    #[error("value out of range for {0}")]
    OutOfRange(&'static str),
    #[error("cannot represent value as JSON: {0}")]
    UnrepresentableJson(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp(a: Value, b: Value) -> Ordering {
        a.total_cmp(&b)
    }

    #[test]
    fn int_order_preserved() {
        assert_eq!(cmp(Value::Int(-5), Value::Int(3)), Ordering::Less);
        assert_eq!(
            cmp(Value::Int(i64::MIN), Value::Int(i64::MAX)),
            Ordering::Less
        );
        assert_eq!(cmp(Value::Int(7), Value::Int(7)), Ordering::Equal);
    }

    #[test]
    fn float_order_preserved() {
        assert_eq!(cmp(Value::Float(-1.0), Value::Float(1.0)), Ordering::Less);
        assert_eq!(cmp(Value::Float(0.0), Value::Float(0.5)), Ordering::Less);
    }

    #[test]
    fn type_rank_orders_across_types() {
        // Null sorts before Bool sorts before Int, etc.
        assert_eq!(cmp(Value::Null, Value::Bool(false)), Ordering::Less);
        assert_eq!(cmp(Value::Bool(true), Value::Int(0)), Ordering::Less);
    }

    #[test]
    fn string_order_is_lexicographic_and_prefix_free() {
        assert_eq!(
            cmp(Value::String("ab".into()), Value::String("abc".into())),
            Ordering::Less
        );
        assert_eq!(
            cmp(Value::String("ab".into()), Value::String("b".into())),
            Ordering::Less
        );
    }

    #[test]
    fn array_orders_elementwise() {
        let a = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let b = Value::Array(vec![Value::Int(1), Value::Int(3)]);
        assert_eq!(a.total_cmp(&b), Ordering::Less);
    }

    #[test]
    fn document_path_lookup() {
        let mut inner = Document::new();
        inner.insert("c", Value::Int(42));
        let mut outer = Document::new();
        outer.insert("a", Value::Document(inner));
        assert_eq!(outer.get_path("a.c"), Some(&Value::Int(42)));
        assert_eq!(outer.get_path("a.x"), None);
        assert_eq!(outer.get_path("missing"), None);
    }

    #[test]
    fn uuid_roundtrip() {
        let s = "550e8400-e29b-41d4-a716-446655440000";
        let u = Uuid::parse_str(s).unwrap();
        assert_eq!(u.to_string(), s);
        assert!(Uuid::parse_str("not-a-uuid").is_err());
    }

    #[test]
    fn decimal_display() {
        assert_eq!(Decimal::new(12345, 2).to_string(), "123.45");
        assert_eq!(Decimal::new(5, 3).to_string(), "0.005");
        assert_eq!(Decimal::new(-5, 3).to_string(), "-0.005");
        assert_eq!(Decimal::new(42, 0).to_string(), "42");
    }
}
