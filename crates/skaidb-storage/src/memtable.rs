//! In-memory write buffer with MVCC versioning (SPEC §5, §12).
//!
//! Every write is stored as its own version keyed by `(user_key, hlc)`, ordered
//! so that the newest version of a key sorts first. This yields snapshot reads
//! "for free": a read at timestamp `T` returns the newest version whose stamp is
//! `<= T`. Deletes are tombstones rather than removals so they shadow older
//! versions until compaction (a later phase) drops them.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use crate::hlc::Hlc;

/// A stored version: either a value or a deletion tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionValue {
    Put(Vec<u8>),
    Delete,
}

/// An ordered, multi-version in-memory table.
#[derive(Debug, Default)]
pub struct Memtable {
    // Versions sort by user key ascending, then by `Reverse(hlc)` so the newest
    // version of each key comes first.
    map: BTreeMap<(Vec<u8>, Reverse<Hlc>), VersionValue>,
    approx_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable::default()
    }

    /// Record a versioned mutation.
    pub fn insert(&mut self, key: Vec<u8>, hlc: Hlc, value: VersionValue) {
        let val_bytes = match &value {
            VersionValue::Put(v) => v.len(),
            VersionValue::Delete => 0,
        };
        // Rough accounting: key + version stamp + value + map overhead estimate.
        self.approx_bytes += key.len() + std::mem::size_of::<Hlc>() + val_bytes + 32;
        self.map.insert((key, Reverse(hlc)), value);
    }

    /// Latest value for `key`, or `None` if absent or tombstoned.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.latest_version(key, Hlc::MAX).and_then(|v| match v {
            VersionValue::Put(bytes) => Some(bytes.as_slice()),
            VersionValue::Delete => None,
        })
    }

    /// Value for `key` as visible at snapshot `as_of` (MVCC read).
    pub fn get_as_of(&self, key: &[u8], as_of: Hlc) -> Option<&[u8]> {
        self.latest_version(key, as_of).and_then(|v| match v {
            VersionValue::Put(bytes) => Some(bytes.as_slice()),
            VersionValue::Delete => None,
        })
    }

    /// The latest stored version for `key` including tombstones, or `None` if
    /// the key has never been written here. Distinguishes "deleted" (returns
    /// `Some(Delete)`) from "absent" (returns `None`) — unlike [`Memtable::get`].
    pub fn get_entry(&self, key: &[u8]) -> Option<&VersionValue> {
        self.latest_version(key, Hlc::MAX)
    }

    /// Like [`Memtable::get_entry`] but also returns the version stamp, so a
    /// coordinator can resolve replicas by last-writer-wins on a point read.
    pub fn get_entry_versioned(&self, key: &[u8]) -> Option<(Hlc, VersionValue)> {
        // The first entry at or after `(key, Reverse(MAX))` is the newest
        // version of `key`, if the key is present at all.
        let start = (key.to_vec(), std::cmp::Reverse(Hlc::MAX));
        self.map
            .range(start..)
            .next()
            .filter(|((k, _), _)| k.as_slice() == key)
            .map(|((_, std::cmp::Reverse(stamp)), value)| (*stamp, value.clone()))
    }

    /// Latest version per distinct key (including tombstones), with its stamp,
    /// in key order. Used to flush the memtable into an SSTable.
    pub fn iter_latest_entries(&self) -> Vec<(Vec<u8>, Hlc, VersionValue)> {
        let mut out = Vec::new();
        let mut current: Option<&[u8]> = None;
        for ((k, std::cmp::Reverse(stamp)), value) in &self.map {
            if current == Some(k.as_slice()) {
                continue;
            }
            current = Some(k.as_slice());
            out.push((k.clone(), *stamp, value.clone()));
        }
        out
    }

    /// The newest version of `key` whose stamp is `<= as_of`, if any.
    fn latest_version(&self, key: &[u8], as_of: Hlc) -> Option<&VersionValue> {
        let start = (key.to_vec(), Reverse(Hlc::MAX));
        for ((k, Reverse(stamp)), value) in self.map.range(start..) {
            if k.as_slice() != key {
                break; // moved past this key's versions
            }
            if *stamp <= as_of {
                return Some(value);
            }
        }
        None
    }

    /// Approximate in-memory footprint, used to decide when to flush.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Number of stored versions (not distinct keys).
    pub fn version_count(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Latest live (non-tombstone) value for each distinct key, in key order.
    pub fn iter_latest(&self) -> Vec<(Vec<u8>, &[u8])> {
        let mut out = Vec::new();
        let mut current: Option<&[u8]> = None;
        for ((k, _), value) in &self.map {
            if current == Some(k.as_slice()) {
                continue; // already emitted the newest version of this key
            }
            current = Some(k.as_slice());
            if let VersionValue::Put(bytes) = value {
                out.push((k.clone(), bytes.as_slice()));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(s: &str) -> VersionValue {
        VersionValue::Put(s.as_bytes().to_vec())
    }

    #[test]
    fn latest_version_wins() {
        let mut m = Memtable::new();
        m.insert(b"k".to_vec(), Hlc::new(1, 0), put("v1"));
        m.insert(b"k".to_vec(), Hlc::new(2, 0), put("v2"));
        assert_eq!(m.get(b"k"), Some(&b"v2"[..]));
    }

    #[test]
    fn tombstone_hides_value() {
        let mut m = Memtable::new();
        m.insert(b"k".to_vec(), Hlc::new(1, 0), put("v1"));
        m.insert(b"k".to_vec(), Hlc::new(2, 0), VersionValue::Delete);
        assert_eq!(m.get(b"k"), None);
    }

    #[test]
    fn snapshot_read_sees_old_version() {
        let mut m = Memtable::new();
        m.insert(b"k".to_vec(), Hlc::new(1, 0), put("v1"));
        m.insert(b"k".to_vec(), Hlc::new(5, 0), put("v2"));
        assert_eq!(m.get_as_of(b"k", Hlc::new(3, 0)), Some(&b"v1"[..]));
        assert_eq!(m.get_as_of(b"k", Hlc::new(5, 0)), Some(&b"v2"[..]));
        assert_eq!(m.get_as_of(b"k", Hlc::new(0, 0)), None);
    }

    #[test]
    fn iter_latest_skips_tombstones_and_old_versions() {
        let mut m = Memtable::new();
        m.insert(b"a".to_vec(), Hlc::new(1, 0), put("a1"));
        m.insert(b"a".to_vec(), Hlc::new(2, 0), put("a2"));
        m.insert(b"b".to_vec(), Hlc::new(1, 0), put("b1"));
        m.insert(b"b".to_vec(), Hlc::new(2, 0), VersionValue::Delete);
        m.insert(b"c".to_vec(), Hlc::new(1, 0), put("c1"));

        let live: Vec<_> = m
            .iter_latest()
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8(k).unwrap(),
                    std::str::from_utf8(v).unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            live,
            vec![("a".into(), "a2".into()), ("c".into(), "c1".into())]
        );
    }
}
