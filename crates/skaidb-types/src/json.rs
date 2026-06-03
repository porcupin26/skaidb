//! JSON interop (SPEC §2.1: "JSON-like types supported in addition").
//!
//! The mapping is intentionally lossy in one direction: JSON has no native
//! bytes/uuid/decimal/timestamp, so [`Value::from_json`] produces only the
//! JSON-expressible variants, while [`Value::to_json`] renders the richer
//! variants in conventional textual/array forms.

use crate::value::{Document, Value};
use serde_json::Value as Json;

impl Value {
    /// Build a [`Value`] from a `serde_json::Value`.
    ///
    /// Whole numbers become [`Value::Int`]; everything else with a fractional
    /// part or beyond `i64` range becomes [`Value::Float`].
    pub fn from_json(json: Json) -> Value {
        match json {
            Json::Null => Value::Null,
            Json::Bool(b) => Value::Bool(b),
            Json::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            Json::String(s) => Value::String(s),
            Json::Array(items) => Value::Array(items.into_iter().map(Value::from_json).collect()),
            Json::Object(map) => {
                let mut doc = Document::new();
                for (k, v) in map {
                    doc.insert(k, Value::from_json(v));
                }
                Value::Document(doc)
            }
        }
    }

    /// Render this value as a `serde_json::Value`.
    pub fn to_json(&self) -> Json {
        match self {
            Value::Null => Json::Null,
            Value::Bool(b) => Json::Bool(*b),
            Value::Int(i) => Json::Number((*i).into()),
            Value::Timestamp(t) => Json::Number((*t).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(Json::Number)
                .unwrap_or(Json::Null),
            Value::Decimal(d) => Json::String(d.to_string()),
            Value::String(s) => Json::String(s.clone()),
            Value::Bytes(b) => {
                Json::Array(b.iter().map(|byte| Json::Number((*byte).into())).collect())
            }
            Value::Uuid(u) => Json::String(u.to_string()),
            Value::Array(items) => Json::Array(items.iter().map(Value::to_json).collect()),
            Value::Document(doc) => {
                let map = doc
                    .0
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect();
                Json::Object(map)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Decimal, Uuid};

    #[test]
    fn json_roundtrip_basic() {
        let json: Json = serde_json::json!({
            "name": "ada",
            "age": 36,
            "score": 9.5,
            "tags": ["a", "b"],
            "active": true,
            "nested": {"x": 1}
        });
        let v = Value::from_json(json.clone());
        // Object becomes a Document with sorted keys; round-trips back to JSON.
        assert_eq!(v.to_json(), json);
    }

    #[test]
    fn integers_stay_integers() {
        let v = Value::from_json(serde_json::json!(7));
        assert_eq!(v, Value::Int(7));
    }

    #[test]
    fn rich_types_render_textually() {
        let u = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            Value::Uuid(u).to_json(),
            Json::String("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(
            Value::Decimal(Decimal::new(12345, 2)).to_json(),
            Json::String("123.45".into())
        );
    }
}
