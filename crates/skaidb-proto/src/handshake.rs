//! Client authentication handshake messages.
//!
//! `AuthStart` names a **mechanism**. The default, `SCRAM_SHA_256`, is a fixed
//! four-frame exchange run even when the server has auth disabled (it then
//! accepts any proof), so both peers run one uniform code path:
//!
//! ```text
//! client → AuthStart    { username, client_nonce, mechanism: SCRAM_SHA_256 }
//! server → AuthChallenge{ salt, iterations, server_nonce }
//! client → AuthFinish   { client_proof }
//! server → AuthOutcome  Ok{ server_signature } | Denied{ reason }
//! ```
//!
//! The `AuthMessage` that the proof is computed over is built identically on
//! both sides by [`auth_message`].
//!
//! `GSSAPI` (Kerberos) is an N-round context negotiation instead: after
//! `AuthStart` the peers shuttle [`AuthToken`] frames in both directions until
//! the GSS context reports complete, then the server sends `AuthOutcome`.
//!
//! **Wire compatibility:** the mechanism is a single trailing byte on
//! `AuthStart`. A client that predates it simply omits the byte and
//! [`AuthStart::decode`] defaults to `SCRAM_SHA_256`; a server that predates it
//! ignores the extra trailing byte (the reader never inspects past the last
//! field it needs). So old/new clients and servers interoperate on SCRAM in
//! every combination.

use crate::message::ProtoError;

const T_START: u8 = 10;
const T_CHALLENGE: u8 = 11;
const T_FINISH: u8 = 12;
const T_OUTCOME: u8 = 13;
const T_TOKEN: u8 = 14;

/// Client authentication mechanism, negotiated by [`AuthStart`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMechanism {
    /// SCRAM-SHA-256 password proof — the default and the only mechanism a
    /// pre-mechanism client requests (by omitting the selector entirely).
    #[default]
    ScramSha256,
    /// Kerberos via SASL GSSAPI: a token-exchange context negotiation, no
    /// password. The authenticated principal maps to an external user's role.
    Gssapi,
}

impl AuthMechanism {
    fn to_byte(self) -> u8 {
        match self {
            AuthMechanism::ScramSha256 => 0,
            AuthMechanism::Gssapi => 1,
        }
    }
    fn from_byte(b: u8) -> Result<Self, ProtoError> {
        match b {
            0 => Ok(AuthMechanism::ScramSha256),
            1 => Ok(AuthMechanism::Gssapi),
            _ => Err(ProtoError::Malformed("unknown auth mechanism")),
        }
    }
}

/// First client message: who is connecting, a fresh client nonce, and the
/// mechanism the client wishes to authenticate with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthStart {
    pub username: String,
    pub client_nonce: String,
    pub mechanism: AuthMechanism,
}

/// One leg of a multi-round token exchange (GSSAPI): an opaque security-context
/// token, passed in either direction until the context is established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthToken {
    pub token: Vec<u8>,
}

/// Server's challenge: the user's salt, iteration count, and a combined nonce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthChallenge {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub server_nonce: String,
}

/// Client's proof over the auth message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthFinish {
    pub client_proof: [u8; 32],
}

/// Server's verdict; on success carries the server signature for mutual auth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthOutcome {
    Ok { server_signature: [u8; 32] },
    Denied { reason: String },
}

/// Build the auth message both sides sign over.
pub fn auth_message(
    username: &str,
    client_nonce: &str,
    server_nonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Vec<u8> {
    let salt_hex: String = salt.iter().map(|b| format!("{b:02x}")).collect();
    format!("{username}\0{client_nonce}\0{server_nonce}\0{salt_hex}\0{iterations}").into_bytes()
}

impl AuthStart {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![T_START];
        put_str(&mut out, &self.username);
        put_str(&mut out, &self.client_nonce);
        // Trailing mechanism selector. A pre-mechanism peer stops decoding
        // after `client_nonce` and never sees this byte; a pre-mechanism
        // server likewise ignores it — SCRAM stays wire-compatible.
        out.push(self.mechanism.to_byte());
        out
    }
    pub fn decode(buf: &[u8]) -> Result<AuthStart, ProtoError> {
        let mut c = Reader::new(buf, T_START)?;
        let username = c.string()?;
        let client_nonce = c.string()?;
        // Absent selector (older client) == SCRAM-SHA-256.
        let mechanism = if c.remaining() > 0 {
            AuthMechanism::from_byte(c.u8()?)?
        } else {
            AuthMechanism::default()
        };
        Ok(AuthStart {
            username,
            client_nonce,
            mechanism,
        })
    }
}

impl AuthToken {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![T_TOKEN];
        put_bytes(&mut out, &self.token);
        out
    }
    pub fn decode(buf: &[u8]) -> Result<AuthToken, ProtoError> {
        let mut c = Reader::new(buf, T_TOKEN)?;
        Ok(AuthToken {
            token: c.bytes()?.to_vec(),
        })
    }
}

impl AuthChallenge {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![T_CHALLENGE];
        put_bytes(&mut out, &self.salt);
        out.extend_from_slice(&self.iterations.to_le_bytes());
        put_str(&mut out, &self.server_nonce);
        out
    }
    pub fn decode(buf: &[u8]) -> Result<AuthChallenge, ProtoError> {
        let mut c = Reader::new(buf, T_CHALLENGE)?;
        Ok(AuthChallenge {
            salt: c.bytes()?.to_vec(),
            iterations: c.u32()?,
            server_nonce: c.string()?,
        })
    }
}

impl AuthFinish {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![T_FINISH];
        out.extend_from_slice(&self.client_proof);
        out
    }
    pub fn decode(buf: &[u8]) -> Result<AuthFinish, ProtoError> {
        let mut c = Reader::new(buf, T_FINISH)?;
        let proof: [u8; 32] = c
            .take(32)?
            .try_into()
            .map_err(|_| ProtoError::Malformed("bad proof length"))?;
        Ok(AuthFinish {
            client_proof: proof,
        })
    }
}

impl AuthOutcome {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![T_OUTCOME];
        match self {
            AuthOutcome::Ok { server_signature } => {
                out.push(1);
                out.extend_from_slice(server_signature);
            }
            AuthOutcome::Denied { reason } => {
                out.push(0);
                put_str(&mut out, reason);
            }
        }
        out
    }
    pub fn decode(buf: &[u8]) -> Result<AuthOutcome, ProtoError> {
        let mut c = Reader::new(buf, T_OUTCOME)?;
        Ok(match c.u8()? {
            1 => {
                let sig: [u8; 32] = c
                    .take(32)?
                    .try_into()
                    .map_err(|_| ProtoError::Malformed("bad signature length"))?;
                AuthOutcome::Ok {
                    server_signature: sig,
                }
            }
            0 => AuthOutcome::Denied {
                reason: c.string()?,
            },
            _ => return Err(ProtoError::Malformed("bad outcome tag")),
        })
    }
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], expect_tag: u8) -> Result<Self, ProtoError> {
        let mut r = Reader { buf, pos: 0 };
        if r.u8()? != expect_tag {
            return Err(ProtoError::Malformed("unexpected handshake tag"));
        }
        Ok(r)
    }
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtoError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(ProtoError::Malformed("overflow"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or(ProtoError::Malformed("short handshake message"))?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, ProtoError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ProtoError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8], ProtoError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> Result<String, ProtoError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| ProtoError::Malformed("bad utf-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_roundtrip() {
        for mechanism in [AuthMechanism::ScramSha256, AuthMechanism::Gssapi] {
            let m = AuthStart {
                username: "ada".into(),
                client_nonce: "abc123".into(),
                mechanism,
            };
            assert_eq!(AuthStart::decode(&m.encode()).unwrap(), m);
        }
    }

    /// A pre-mechanism client encodes `AuthStart` with no trailing selector
    /// byte; a current server must decode that as SCRAM, not error. This is
    /// the rolling-upgrade contract — old drivers keep authenticating.
    #[test]
    fn start_without_mechanism_byte_defaults_to_scram() {
        // Hand-build the legacy wire form: tag + username + client_nonce only.
        let mut legacy = vec![T_START];
        put_str(&mut legacy, "ada");
        put_str(&mut legacy, "abc123");
        let decoded = AuthStart::decode(&legacy).unwrap();
        assert_eq!(decoded.mechanism, AuthMechanism::ScramSha256);
        assert_eq!(decoded.username, "ada");
        assert_eq!(decoded.client_nonce, "abc123");
    }

    /// A pre-mechanism server ignores the trailing selector byte: decoding a
    /// current SCRAM `AuthStart` with the legacy two-field reader still yields
    /// the username and nonce (the extra byte is never inspected).
    #[test]
    fn trailing_mechanism_byte_is_ignored_by_legacy_reader() {
        let m = AuthStart {
            username: "ada".into(),
            client_nonce: "abc123".into(),
            mechanism: AuthMechanism::ScramSha256,
        };
        let wire = m.encode();
        let mut c = Reader::new(&wire, T_START).unwrap();
        assert_eq!(c.string().unwrap(), "ada");
        assert_eq!(c.string().unwrap(), "abc123");
    }

    #[test]
    fn token_roundtrip() {
        let m = AuthToken {
            token: vec![0xde, 0xad, 0xbe, 0xef],
        };
        assert_eq!(AuthToken::decode(&m.encode()).unwrap(), m);
        // Wrong tag is rejected like every other frame.
        assert!(AuthToken::decode(&m.encode()[1..]).is_err());
    }

    #[test]
    fn challenge_roundtrip() {
        let m = AuthChallenge {
            salt: vec![1, 2, 3, 4],
            iterations: 4096,
            server_nonce: "abc123.server".into(),
        };
        assert_eq!(AuthChallenge::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn finish_and_outcome_roundtrip() {
        let f = AuthFinish {
            client_proof: [7u8; 32],
        };
        assert_eq!(AuthFinish::decode(&f.encode()).unwrap(), f);

        let ok = AuthOutcome::Ok {
            server_signature: [9u8; 32],
        };
        assert_eq!(AuthOutcome::decode(&ok.encode()).unwrap(), ok);

        let denied = AuthOutcome::Denied {
            reason: "no".into(),
        };
        assert_eq!(AuthOutcome::decode(&denied.encode()).unwrap(), denied);
    }

    #[test]
    fn auth_message_is_deterministic() {
        let a = auth_message("u", "cn", "sn", &[1, 2], 4096);
        let b = auth_message("u", "cn", "sn", &[1, 2], 4096);
        assert_eq!(a, b);
        assert_ne!(a, auth_message("u", "cn", "sn", &[1, 2], 4097));
    }

    #[test]
    fn wrong_tag_is_rejected() {
        let bytes = AuthStart {
            username: "x".into(),
            client_nonce: "y".into(),
            mechanism: AuthMechanism::ScramSha256,
        }
        .encode();
        assert!(AuthChallenge::decode(&bytes).is_err());
    }
}
