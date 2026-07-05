//! skaidb authentication and authorization (SPEC §8).
//!
//! - [`crypto`] — SHA-256 / HMAC / PBKDF2 primitives (no external deps),
//! - [`scram`] — SCRAM-SHA-256 credentials and proof verification,
//! - [`rbac`] — role-based access control with inheritance.

pub mod crypto;
pub mod rbac;
pub mod scram;

pub use rbac::{privilege_from_name, privilege_name, Object, Privilege, RbacError, RoleStore};
pub use scram::{ScramCredential, DEFAULT_ITERATIONS};
