//! Core value and type model for skaidb (SPEC §2).
//!
//! skaidb is schema-less: there is no table-level column list. A stored row is a
//! [`Document`] — an ordered map from field name to a dynamically typed
//! [`Value`]. A field that is absent from a document reads as `NULL` under the
//! three-valued logic in [`ternary`].

mod codec;
mod json;
pub mod slog;
pub mod ternary;

/// The cluster's key-placement hash: 64-bit FNV-1a followed by a splitmix64
/// finalizer for good avalanche, so short, similar inputs spread across the
/// ring. Lives here (the bottom of the crate graph) because **two** layers
/// must agree on it byte-for-byte: the cluster ring places keys with it, and
/// the search indexes store it per document (the `_ring` fast field) so
/// sharded scatters can filter to each node's owned key-space. Changing it
/// is a data-placement migration, not a refactor.
pub fn ring_hash(bytes: &[u8]) -> u64 {
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
mod value;

pub use slog::{init_server_log, server_log};
pub use ternary::Ternary;
pub use value::{Decimal, Document, Uuid, Value, ValueError, ValueType};
