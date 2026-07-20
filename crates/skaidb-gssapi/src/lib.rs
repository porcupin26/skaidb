//! GSSAPI (Kerberos) security-context helpers for skaidb's client auth.
//!
//! A thin, safe wrapper over [`cross-krb5`], compiled only when the `kerberos`
//! feature is enabled (glibc/macOS/Windows — never the static-musl build). All
//! `unsafe` FFI lives inside the dependency, so this crate keeps the workspace
//! `unsafe_code = "forbid"` lint.
//!
//! Both sides drive a step loop over opaque tokens that the caller shuttles as
//! `AuthToken` frames: the client sends the first token, then each side feeds
//! the peer's token to `step` until the context is established. The server
//! then reads the cryptographically-authenticated client principal.
//!
//! The context is raw GSS establishment (not the RFC-4752 SASL security-layer
//! negotiation): skaidb owns both ends and runs it inside the existing client
//! TLS, so no post-auth security-layer byte is exchanged — establishing the
//! context and reading the principal is the whole job.

use thiserror::Error;

/// A GSSAPI error, kept free of the `anyhow`/`cross-krb5` types so callers
/// don't take those dependencies.
#[derive(Debug, Error)]
pub enum GssError {
    /// This build was compiled without the `kerberos` feature.
    #[error("GSSAPI authentication is not supported in this build")]
    NotSupported,
    /// The underlying Kerberos mechanism reported an error.
    #[error("kerberos: {0}")]
    Krb5(String),
}

/// Whether this build links Kerberos (the `kerberos` feature is enabled).
/// Lets feature-agnostic code (config validation, capability reporting) branch
/// without its own `cfg`.
pub const fn is_supported() -> bool {
    cfg!(feature = "kerberos")
}

#[cfg(feature = "kerberos")]
pub use imp::{set_keytab, ClientHandshake, ClientStep, ServerHandshake, ServerStep};

#[cfg(feature = "kerberos")]
mod imp {
    use super::GssError;
    use cross_krb5::{
        AcceptFlags, ClientCtx, InitiateFlags, K5ServerCtx, PendingClientCtx, PendingServerCtx,
        ServerCtx, Step,
    };

    /// Select the keytab the acceptor authenticates against (the GSSAPI
    /// standard `KRB5_KTNAME`). Call once at server startup, before accepting;
    /// it is process-global. Idempotent for a given path.
    pub fn set_keytab(path: &str) {
        std::env::set_var("KRB5_KTNAME", path);
    }

    /// One leg of the server-side context negotiation.
    #[must_use]
    #[derive(Debug)]
    pub enum ServerStep {
        /// Not finished: send `token` to the client, then feed the client's
        /// reply into `next.step`.
        Continue { next: ServerHandshake, token: Vec<u8> },
        /// Established: `principal` is the authenticated client identity
        /// (`user@REALM`). If `token` is `Some`, send it as the final leg.
        Done {
            principal: String,
            token: Option<Vec<u8>>,
        },
    }

    /// Server side of a GSS context negotiation (the acceptor).
    pub struct ServerHandshake(PendingServerCtx);

    impl std::fmt::Debug for ServerHandshake {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ServerHandshake(..)")
        }
    }

    impl ServerHandshake {
        /// Begin accepting a context. `spn` is the service principal to accept
        /// as (`None` = the default resolved from the keytab set by
        /// [`set_keytab`]). Mutual auth and confidentiality are required
        /// (cross-krb5's secure default).
        pub fn new(spn: Option<&str>) -> Result<Self, GssError> {
            ServerCtx::new(AcceptFlags::default(), spn, None)
                .map(Self)
                .map_err(|e| GssError::Krb5(e.to_string()))
        }

        /// Feed the next client token, advancing one step. Consumes `self`:
        /// the returned [`ServerStep`] carries the next state.
        pub fn step(self, client_token: &[u8]) -> Result<ServerStep, GssError> {
            match self.0.step(client_token).map_err(|e| GssError::Krb5(e.to_string()))? {
                Step::Continue((next, token)) => Ok(ServerStep::Continue {
                    next: Self(next),
                    token: token.to_vec(),
                }),
                Step::Finished((mut ctx, token)) => {
                    let principal = ctx.client().map_err(|e| GssError::Krb5(e.to_string()))?;
                    Ok(ServerStep::Done {
                        principal,
                        token: token.map(|t| t.to_vec()),
                    })
                }
            }
        }
    }

    /// One leg of the client-side context negotiation.
    #[must_use]
    #[derive(Debug)]
    pub enum ClientStep {
        /// Not finished: send `token` to the server, then feed the server's
        /// reply into `next.step`.
        Continue { next: ClientHandshake, token: Vec<u8> },
        /// Established: send `token` to the server if present (final leg).
        Done { token: Option<Vec<u8>> },
    }

    /// Client side of a GSS context negotiation (the initiator).
    pub struct ClientHandshake(PendingClientCtx);

    impl std::fmt::Debug for ClientHandshake {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ClientHandshake(..)")
        }
    }

    impl ClientHandshake {
        /// Begin authenticating to `target_spn` (e.g.
        /// `skaidb/host.example.com@REALM`). `principal` selects the client
        /// identity (`None` = the ambient ticket cache from `kinit`). Returns
        /// the handshake and the initial token to send to the server.
        pub fn new(
            target_spn: &str,
            principal: Option<&str>,
        ) -> Result<(Self, Vec<u8>), GssError> {
            let (pending, token) =
                ClientCtx::new(InitiateFlags::default(), principal, target_spn, None)
                    .map_err(|e| GssError::Krb5(e.to_string()))?;
            Ok((Self(pending), token.to_vec()))
        }

        /// Feed the next server token, advancing one step.
        pub fn step(self, server_token: &[u8]) -> Result<ClientStep, GssError> {
            match self.0.step(server_token).map_err(|e| GssError::Krb5(e.to_string()))? {
                Step::Continue((next, token)) => Ok(ClientStep::Continue {
                    next: Self(next),
                    token: token.to_vec(),
                }),
                Step::Finished((_ctx, token)) => Ok(ClientStep::Done {
                    token: token.map(|t| t.to_vec()),
                }),
            }
        }
    }
}
