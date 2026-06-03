//! Tunable quorum and last-writer-wins conflict resolution (SPEC §5).

use skaidb_storage::Hlc;
use skaidb_types::{Document, Value};

/// Per-query consistency level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Acknowledge after a single replica.
    One,
    /// Acknowledge after a majority (`rf/2 + 1`).
    Quorum,
    /// Acknowledge after all replicas.
    All,
}

impl Consistency {
    /// Number of replica acknowledgements required for `rf` replicas.
    pub fn required(self, rf: usize) -> usize {
        let rf = rf.max(1);
        match self {
            Consistency::One => 1,
            Consistency::Quorum => rf / 2 + 1,
            Consistency::All => rf,
        }
    }
}

/// Whether a read at `read` and write at `write` over `rf` replicas guarantees
/// the read sees the latest write (the `R + W > N` rule).
pub fn is_strong(read: Consistency, write: Consistency, rf: usize) -> bool {
    read.required(rf) + write.required(rf) > rf
}

/// A value with the version stamp under which it was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned<T> {
    pub hlc: Hlc,
    pub value: T,
}

impl<T> Versioned<T> {
    pub fn new(hlc: Hlc, value: T) -> Self {
        Versioned { hlc, value }
    }
}

/// Resolve conflicting replica copies of a whole value by last-writer-wins:
/// highest [`Hlc`] wins, ties broken by the canonical value order so the result
/// is deterministic across replicas.
pub fn resolve_value(versions: &[Versioned<Value>]) -> Option<Value> {
    versions
        .iter()
        .max_by(|a, b| a.hlc.cmp(&b.hlc).then_with(|| a.value.total_cmp(&b.value)))
        .map(|v| v.value.clone())
}

/// Field-level last-writer-wins merge of conflicting document copies (SPEC §5).
///
/// For each field present in any replica copy, the value is taken from the copy
/// with the highest write stamp that contains that field. This lets concurrent
/// updates to *different* fields of the same row both survive.
pub fn merge_documents(versions: &[Versioned<Document>]) -> Document {
    use std::collections::BTreeMap;
    // field -> (winning stamp, value)
    let mut fields: BTreeMap<String, (Hlc, Value)> = BTreeMap::new();
    for v in versions {
        for (field, value) in &v.value.0 {
            fields
                .entry(field.clone())
                .and_modify(|cur| {
                    if v.hlc > cur.0 {
                        *cur = (v.hlc, value.clone());
                    }
                })
                .or_insert((v.hlc, value.clone()));
        }
    }
    let mut doc = Document::new();
    for (field, (_, value)) in fields {
        doc.insert(field, value);
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_counts() {
        assert_eq!(Consistency::One.required(3), 1);
        assert_eq!(Consistency::Quorum.required(3), 2);
        assert_eq!(Consistency::Quorum.required(5), 3);
        assert_eq!(Consistency::All.required(3), 3);
    }

    #[test]
    fn strong_when_r_plus_w_exceeds_n() {
        // QUORUM read + QUORUM write over rf=3: 2+2 > 3 → strong.
        assert!(is_strong(Consistency::Quorum, Consistency::Quorum, 3));
        // ONE + ONE over rf=3: 1+1 = 2, not > 3 → eventual.
        assert!(!is_strong(Consistency::One, Consistency::One, 3));
        // ONE read + ALL write over rf=3: 1+3 > 3 → strong.
        assert!(is_strong(Consistency::One, Consistency::All, 3));
    }

    #[test]
    fn lww_picks_highest_stamp() {
        let versions = vec![
            Versioned::new(Hlc::new(1, 0), Value::Int(1)),
            Versioned::new(Hlc::new(3, 0), Value::Int(3)),
            Versioned::new(Hlc::new(2, 0), Value::Int(2)),
        ];
        assert_eq!(resolve_value(&versions), Some(Value::Int(3)));
    }

    #[test]
    fn lww_tie_break_is_deterministic() {
        let a = vec![
            Versioned::new(Hlc::new(5, 0), Value::Int(1)),
            Versioned::new(Hlc::new(5, 0), Value::Int(9)),
        ];
        let b = vec![
            Versioned::new(Hlc::new(5, 0), Value::Int(9)),
            Versioned::new(Hlc::new(5, 0), Value::Int(1)),
        ];
        assert_eq!(resolve_value(&a), resolve_value(&b));
        assert_eq!(resolve_value(&a), Some(Value::Int(9)));
    }

    #[test]
    fn field_level_merge_keeps_concurrent_field_writes() {
        // Replica X updated `name` at t=5; replica Y updated `age` at t=7.
        let mut x = Document::new();
        x.insert("id", Value::Int(1));
        x.insert("name", Value::String("ada".into()));
        x.insert("age", Value::Int(30));

        let mut y = Document::new();
        y.insert("id", Value::Int(1));
        y.insert("name", Value::String("old".into()));
        y.insert("age", Value::Int(31));

        let merged = merge_documents(&[
            Versioned::new(Hlc::new(5, 0), x),
            Versioned::new(Hlc::new(7, 0), y),
        ]);
        // name from the t=5 copy is older than the t=7 copy → t=7 wins per field.
        assert_eq!(merged.get("age"), Some(&Value::Int(31)));
        assert_eq!(merged.get("name"), Some(&Value::String("old".into())));
    }
}
