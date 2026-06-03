//! Core value and type model for skaidb (SPEC §2).
//!
//! skaidb is schema-less: there is no table-level column list. A stored row is a
//! [`Document`] — an ordered map from field name to a dynamically typed
//! [`Value`]. A field that is absent from a document reads as `NULL` under the
//! three-valued logic in [`ternary`].

mod codec;
mod json;
pub mod ternary;
mod value;

pub use ternary::Ternary;
pub use value::{Decimal, Document, Uuid, Value, ValueError, ValueType};
