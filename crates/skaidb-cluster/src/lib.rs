//! skaidb clustering (SPEC §4–6): consistent-hash partitioning, tunable quorum
//! consistency, and HLC last-writer-wins conflict resolution.
//!
//! This crate is the placement/coordination logic; the network transport that
//! ships requests between nodes lives in `skaidb-proto`/`skaidb-server`.

pub mod internode;
mod node;
mod quorum;
mod ring;

pub use node::{ClusterStats, Node, NodeConfig};
pub use quorum::{is_strong, merge_documents, resolve_value, Consistency, Versioned};
pub use ring::{NodeId, Ring};
