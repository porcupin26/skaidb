//! skaidb wire protocol (SCP, SPEC §11).
//!
//! Phase 1 implements the raw-TCP fast path described in `scp.txt`: a simple
//! length-prefixed framing ([`frame`]) carrying self-describing request/response
//! [`message`]s. QUIC (the WAN default, with streams and the push-based control
//! plane) builds on these message types in a later phase.

pub mod frame;
pub mod handshake;
pub mod message;

pub use frame::{begin_frame, finish_frame, read_frame, read_frame_into, write_frame, MAX_FRAME_LEN};
pub use handshake::{auth_message, AuthChallenge, AuthFinish, AuthOutcome, AuthStart};
pub use message::{ClientRequest, Consistency, ProtoError, Request, Response};
