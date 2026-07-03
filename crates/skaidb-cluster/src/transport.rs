//! Internode transport authentication (SPEC §8.1).
//!
//! Every internode connection is established through an [`Authenticator`], which
//! enforces one of three modes before any RPC is served:
//!
//! - [`Authenticator::None`] — plain TCP, no authentication (trusted network).
//! - [`Authenticator::Token`] — a shared secret, proven via a mutual
//!   HMAC-SHA256 challenge-response. The secret never crosses the wire and a
//!   fresh nonce per connection defeats replay. No encryption.
//! - [`Authenticator::Cert`] — mutual TLS: both ends present a certificate
//!   signed by a shared CA, and the channel is encrypted.
//!
//! All three yield a [`Conn`], which is `Read + Write`, so the framing/RPC layer
//! above is transport-agnostic.

use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use skaidb_auth::crypto::{ct_eq, hmac_sha256};
use skaidb_proto::{read_frame, write_frame};

/// The TLS server name every cluster node's certificate must carry as a SAN, so
/// clients can verify the peer chain without per-node hostnames. (Operators
/// generate node certs with `DNS:skaidb`.)
const TLS_SERVER_NAME: &str = "skaidb";

/// An authenticated internode connection: plain TCP, or TLS over TCP. Reads go
/// through an internal buffer so a length-prefixed frame read doesn't hit the
/// stream twice per message (once for the length, once for the payload); writes
/// pass straight through (`write_frame` already coalesces header + payload).
pub struct Conn {
    stream: BufReader<Stream>,
}

/// The underlying byte stream of a [`Conn`].
enum Stream {
    Plain(TcpStream),
    ClientTls(Box<StreamOwned<ClientConnection, TcpStream>>),
    ServerTls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Conn {
    fn new(stream: Stream) -> Conn {
        Conn {
            stream: BufReader::new(stream),
        }
    }
}

impl fmt::Debug for Conn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.stream.get_ref() {
            Stream::Plain(_) => "Plain",
            Stream::ClientTls(_) => "ClientTls",
            Stream::ServerTls(_) => "ServerTls",
        };
        f.debug_tuple("Conn").field(&kind).finish()
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::ClientTls(s) => s.read(buf),
            Stream::ServerTls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::ClientTls(s) => s.write(buf),
            Stream::ServerTls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::ClientTls(s) => s.flush(),
            Stream::ServerTls(s) => s.flush(),
        }
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.get_mut().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.get_mut().flush()
    }
}

/// How internode connections authenticate. Shared (cloned) across the pool and
/// the accept loop, so it is built once at startup.
pub enum Authenticator {
    None,
    Token(Vec<u8>),
    Cert {
        client: Arc<ClientConfig>,
        server: Arc<ServerConfig>,
    },
}

impl fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self {
            Authenticator::None => "none",
            Authenticator::Token(_) => "token", // secret redacted
            Authenticator::Cert { .. } => "cert",
        };
        write!(f, "Authenticator({mode})")
    }
}

impl Authenticator {
    /// Build the token-mode authenticator from a shared secret.
    pub fn token(secret: Vec<u8>) -> Self {
        Authenticator::Token(secret)
    }

    /// Build the cert-mode (mutual TLS) authenticator from PEM files: this
    /// node's certificate and private key, plus the CA that signs every node's
    /// certificate.
    pub fn cert(cert_path: &str, key_path: &str, ca_path: &str) -> Result<Self, String> {
        // Install the ring crypto provider for this process (idempotent).
        let _ = rustls::crypto::ring::default_provider().install_default();

        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;
        let ca = load_certs(ca_path)?;

        let mut roots = RootCertStore::empty();
        for c in ca {
            roots.add(c).map_err(|e| format!("add CA cert: {e}"))?;
        }
        let roots = Arc::new(roots);

        let verifier = rustls::server::WebPkiClientVerifier::builder(roots.clone())
            .build()
            .map_err(|e| format!("build client-cert verifier: {e}"))?;
        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key.clone_key())
            .map_err(|e| format!("server TLS config: {e}"))?;

        let client = ClientConfig::builder()
            .with_root_certificates((*roots).clone())
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("client TLS config: {e}"))?;

        Ok(Authenticator::Cert {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }

    /// Open and authenticate an outbound connection to `addr`. With `timeout`
    /// set, the connect and the handshake are time-bounded (used for probes).
    pub fn connect(&self, addr: &str, timeout: Option<Duration>) -> io::Result<Conn> {
        let tcp = match timeout {
            Some(t) => {
                let sa = addr
                    .to_socket_addrs()?
                    .next()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "no address"))?;
                let s = TcpStream::connect_timeout(&sa, t)?;
                s.set_read_timeout(Some(t)).ok();
                s.set_write_timeout(Some(t)).ok();
                s
            }
            None => TcpStream::connect(addr)?,
        };
        tcp.set_nodelay(true).ok();

        match self {
            Authenticator::None => Ok(Conn::new(Stream::Plain(tcp))),
            Authenticator::Token(secret) => {
                let mut conn = Conn::new(Stream::Plain(tcp));
                token_handshake_client(&mut conn, secret)?;
                Ok(conn)
            }
            Authenticator::Cert { client, .. } => {
                let name = ServerName::try_from(TLS_SERVER_NAME)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
                let mut conn = ClientConnection::new(client.clone(), name).map_err(tls_err)?;
                let mut tcp = tcp;
                while conn.is_handshaking() {
                    conn.complete_io(&mut tcp)?;
                }
                Ok(Conn::new(Stream::ClientTls(Box::new(StreamOwned::new(
                    conn, tcp,
                )))))
            }
        }
    }

    /// Authenticate an inbound connection that was just accepted.
    pub fn accept(&self, tcp: TcpStream) -> io::Result<Conn> {
        tcp.set_nodelay(true).ok();
        match self {
            Authenticator::None => Ok(Conn::new(Stream::Plain(tcp))),
            Authenticator::Token(secret) => {
                let mut conn = Conn::new(Stream::Plain(tcp));
                token_handshake_server(&mut conn, secret)?;
                Ok(conn)
            }
            Authenticator::Cert { server, .. } => {
                let mut conn = ServerConnection::new(server.clone()).map_err(tls_err)?;
                let mut tcp = tcp;
                while conn.is_handshaking() {
                    conn.complete_io(&mut tcp)?;
                }
                Ok(Conn::new(Stream::ServerTls(Box::new(StreamOwned::new(
                    conn, tcp,
                )))))
            }
        }
    }
}

/// Mutual HMAC-SHA256 challenge-response, connector side: prove knowledge of the
/// token over the peer's nonce, then verify the peer's proof over ours.
fn token_handshake_client(conn: &mut Conn, secret: &[u8]) -> io::Result<()> {
    let server_nonce = read_frame(conn)?;
    let my_nonce = random_nonce();
    write_frame(conn, &hmac_sha256(secret, &server_nonce))?;
    write_frame(conn, &my_nonce)?;
    let server_proof = read_frame(conn)?;
    if !ct_eq(&server_proof, &hmac_sha256(secret, &my_nonce)) {
        return Err(auth_failed("peer failed token challenge"));
    }
    Ok(())
}

/// Acceptor side: challenge the connector, verify its proof, then prove back.
fn token_handshake_server(conn: &mut Conn, secret: &[u8]) -> io::Result<()> {
    let my_nonce = random_nonce();
    write_frame(conn, &my_nonce)?;
    let client_proof = read_frame(conn)?;
    let client_nonce = read_frame(conn)?;
    if !ct_eq(&client_proof, &hmac_sha256(secret, &my_nonce)) {
        return Err(auth_failed("connecting peer failed token challenge"));
    }
    write_frame(conn, &hmac_sha256(secret, &client_nonce))?;
    Ok(())
}

fn auth_failed(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, format!("internode auth: {msg}"))
}

fn tls_err(e: rustls::Error) -> io::Error {
    io::Error::other(format!("internode TLS: {e}"))
}

/// 32 random bytes for a handshake nonce. Prefers the OS CSPRNG; falls back to a
/// hash of time + pid + a per-call counter if `/dev/urandom` is unavailable.
fn random_nonce() -> [u8; 32] {
    let mut buf = [0u8; 32];
    if File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .is_ok()
    {
        return buf;
    }
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seed = t
        ^ (std::process::id() as u64).rotate_left(32)
        ^ CTR.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    hmac_sha256(&seed.to_le_bytes(), b"skaidb-nonce")
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let f = File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut r = BufReader::new(f);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut r).collect();
    let certs = certs.map_err(|e| format!("parse certs {path}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("no certificates in {path}"));
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let f = File::open(path).map_err(|e| format!("open {path}: {e}"))?;
    let mut r = BufReader::new(f);
    rustls_pemfile::private_key(&mut r)
        .map_err(|e| format!("parse key {path}: {e}"))?
        .ok_or_else(|| format!("no private key in {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    /// Run one client/server handshake with the given secrets, returning whether
    /// each side accepted.
    fn handshake(server_secret: &[u8], client_secret: &[u8]) -> (bool, bool) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let s = server_secret.to_vec();
        let server = thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            Authenticator::token(s).accept(sock).is_ok()
        });
        let client_ok = Authenticator::token(client_secret.to_vec())
            .connect(&addr, None)
            .is_ok();
        let server_ok = server.join().unwrap();
        (server_ok, client_ok)
    }

    #[test]
    fn token_handshake_accepts_matching_secret() {
        let (server_ok, client_ok) = handshake(b"shared-secret", b"shared-secret");
        assert!(server_ok && client_ok, "matching tokens must authenticate");
    }

    #[test]
    fn token_handshake_rejects_mismatched_secret() {
        let (server_ok, client_ok) = handshake(b"server-secret", b"client-secret");
        assert!(!server_ok, "server must reject a wrong token");
        assert!(!client_ok, "client must see the rejection");
    }

    #[test]
    fn cert_authenticator_errors_on_missing_files() {
        let err = Authenticator::cert("/no/such/cert", "/no/such/key", "/no/such/ca");
        assert!(err.is_err(), "missing cert material must be an error");
    }
}
