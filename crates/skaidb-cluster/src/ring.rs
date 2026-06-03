//! Consistent-hash ring with virtual nodes (SPEC §4).
//!
//! Each physical node is placed at many points ("vnodes") around a 64-bit ring
//! so load and ownership stay balanced as nodes join and leave. The replica set
//! for a key is the next `rf` *distinct* nodes clockwise from the key's hash —
//! the same scheme drivers use for token-aware routing.

use std::collections::{BTreeMap, BTreeSet};

/// A node identifier (host:port or logical name).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A consistent-hash ring.
#[derive(Debug, Clone)]
pub struct Ring {
    vnodes: BTreeMap<u64, NodeId>,
    nodes: BTreeSet<NodeId>,
    vnodes_per_node: u32,
}

impl Ring {
    /// Create an empty ring with `vnodes_per_node` virtual nodes per member.
    pub fn new(vnodes_per_node: u32) -> Self {
        Ring {
            vnodes: BTreeMap::new(),
            nodes: BTreeSet::new(),
            vnodes_per_node: vnodes_per_node.max(1),
        }
    }

    /// Add a node, placing its virtual nodes around the ring. Idempotent.
    pub fn add_node(&mut self, node: NodeId) {
        if !self.nodes.insert(node.clone()) {
            return;
        }
        for i in 0..self.vnodes_per_node {
            let token = hash_token(&node, i);
            self.vnodes.insert(token, node.clone());
        }
    }

    /// Remove a node and all of its virtual nodes.
    pub fn remove_node(&mut self, node: &NodeId) {
        if self.nodes.remove(node) {
            self.vnodes.retain(|_, n| n != node);
        }
    }

    /// Number of physical nodes on the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The replica set for `key`: the next `rf` distinct nodes clockwise from
    /// the key's hash. Returns fewer than `rf` only if the ring has fewer nodes.
    pub fn replicas_for(&self, key: &[u8], rf: usize) -> Vec<NodeId> {
        let want = rf.min(self.nodes.len());
        if want == 0 {
            return Vec::new();
        }
        let h = hash_bytes(key);
        let mut out: Vec<NodeId> = Vec::with_capacity(want);
        // Walk tokens >= h, then wrap to the start of the ring.
        for (_, node) in self.vnodes.range(h..).chain(self.vnodes.range(..h)) {
            if !out.contains(node) {
                out.push(node.clone());
                if out.len() == want {
                    break;
                }
            }
        }
        out
    }

    /// The single primary (coordinator) owner of `key`, if any.
    pub fn primary_for(&self, key: &[u8]) -> Option<NodeId> {
        self.replicas_for(key, 1).into_iter().next()
    }
}

fn hash_token(node: &NodeId, vnode: u32) -> u64 {
    let mut buf = node.0.clone().into_bytes();
    buf.extend_from_slice(&vnode.to_le_bytes());
    hash_bytes(&buf)
}

/// 64-bit FNV-1a followed by a splitmix64 finalizer for good avalanche, so
/// short, similar inputs (node names, vnode indices) spread across the ring.
fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut z = h;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(nodes: &[&str]) -> Ring {
        let mut r = Ring::new(64);
        for n in nodes {
            r.add_node(NodeId::new(*n));
        }
        r
    }

    #[test]
    fn replicas_are_distinct_and_sized() {
        let r = ring(&["a", "b", "c", "d"]);
        let reps = r.replicas_for(b"some-key", 3);
        assert_eq!(reps.len(), 3);
        let set: BTreeSet<_> = reps.iter().collect();
        assert_eq!(set.len(), 3, "replicas must be distinct nodes");
    }

    #[test]
    fn rf_capped_by_node_count() {
        let r = ring(&["a", "b"]);
        assert_eq!(r.replicas_for(b"k", 5).len(), 2);
    }

    #[test]
    fn empty_ring_returns_no_replicas() {
        let r = Ring::new(8);
        assert!(r.replicas_for(b"k", 3).is_empty());
        assert!(r.primary_for(b"k").is_none());
    }

    #[test]
    fn assignment_is_deterministic() {
        let r1 = ring(&["a", "b", "c"]);
        let r2 = ring(&["c", "b", "a"]); // different insertion order
        assert_eq!(r1.replicas_for(b"key-42", 2), r2.replicas_for(b"key-42", 2));
    }

    #[test]
    fn removing_a_node_reassigns_only_its_keys() {
        let mut r = ring(&["a", "b", "c"]);
        let before: Vec<_> = (0..200)
            .map(|i| {
                let k = format!("k{i}");
                (k.clone(), r.primary_for(k.as_bytes()).unwrap())
            })
            .collect();

        r.remove_node(&NodeId::new("b"));
        let mut moved = 0;
        let mut unmoved_to_b = 0;
        for (k, old) in &before {
            let new = r.primary_for(k.as_bytes()).unwrap();
            if &new != old {
                moved += 1;
                // Keys only move off of the removed node, never onto others arbitrarily.
                assert_eq!(*old, NodeId::new("b"));
            } else if *old != NodeId::new("b") {
                unmoved_to_b += 1;
            }
        }
        assert!(moved > 0 && unmoved_to_b > 0, "only b's keys should move");
    }

    #[test]
    fn load_is_roughly_balanced() {
        let r = ring(&["a", "b", "c", "d"]);
        let mut counts = std::collections::HashMap::new();
        for i in 0..4000 {
            let k = format!("item-{i}");
            *counts
                .entry(r.primary_for(k.as_bytes()).unwrap())
                .or_insert(0u32) += 1;
        }
        // With 64 vnodes/node, no node should be wildly over- or under-loaded.
        for (_, c) in counts {
            assert!((400..1600).contains(&c), "unbalanced: {c}");
        }
    }
}
