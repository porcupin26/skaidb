//! Client-facing TLS for the binary + REST ports.
//!
//! One acceptor built once at startup from `[encryption]` config and shared
//! across every accepted connection. A misconfiguration (mode on but cert/key
//! missing or unreadable) fails **loud** — the listener refuses to start —
//! rather than silently falling back to plaintext.

use std::io;
use std::net::TcpStream;
use std::sync::Arc;

use rustls::ServerConfig;
use skaidb_config::ClientTlsMode;

use crate::shared::Shared;

/// The resolved client-TLS acceptor: the mode plus the prebuilt server config.
/// `None` means plaintext-only (`client_tls = off`).
#[derive(Clone)]
pub struct Acceptor {
    pub mode: ClientTlsMode,
    pub cfg: Arc<ServerConfig>,
}

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptor").field("mode", &self.mode).finish_non_exhaustive()
    }
}

/// Build the acceptor from config, or `None` when TLS is off. Errors (loudly)
/// when TLS is requested but the cert/key are missing or unloadable — the
/// caller should propagate the error so the server does not come up with TLS
/// silently disabled.
pub fn build(ctx: &Shared) -> io::Result<Option<Acceptor>> {
    let cfg = ctx.config_snapshot();
    let e = &cfg.encryption;
    match e.client_tls {
        ClientTlsMode::Off => Ok(None),
        mode => {
            if e.tls_cert_file.is_empty() || e.tls_key_file.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "client_tls is on but encryption.tls_cert_file / tls_key_file are unset",
                ));
            }
            let server_cfg = skaidb_net::server_config(&e.tls_cert_file, &e.tls_key_file)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            Ok(Some(Acceptor {
                mode,
                cfg: server_cfg,
            }))
        }
    }
}

/// Wrap an accepted TCP socket according to the acceptor policy:
/// - no acceptor → plaintext;
/// - `opportunistic` → peek the first byte: a TLS ClientHello is wrapped,
///   anything else stays plaintext;
/// - `required` → a TLS ClientHello is wrapped; a plaintext peer is refused
///   (`None`).
///
/// Returns `None` when the connection must be dropped (refused plaintext or a
/// failed handshake).
pub fn wrap(stream: TcpStream, acceptor: Option<&Acceptor>) -> Option<skaidb_net::Stream> {
    stream.set_nodelay(true).ok();
    let Some(acc) = acceptor else {
        return Some(skaidb_net::Stream::Plain(stream));
    };
    let is_tls = skaidb_net::peek_is_tls(&stream).unwrap_or(false);
    if is_tls {
        skaidb_net::accept_tls(stream, acc.cfg.clone()).ok()
    } else if acc.mode == ClientTlsMode::Required {
        None // plaintext refused under `required`
    } else {
        Some(skaidb_net::Stream::Plain(stream)) // opportunistic: plaintext allowed
    }
}

/// The effective client-TLS mode as a stable string for `/status`.
pub fn mode_str(acceptor: Option<&Acceptor>) -> &'static str {
    match acceptor {
        None => "off",
        Some(a) => match a.mode {
            ClientTlsMode::Off => "off",
            ClientTlsMode::Opportunistic => "opportunistic",
            ClientTlsMode::Required => "required",
        },
    }
}
