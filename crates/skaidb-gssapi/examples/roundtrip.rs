//! Phase-0 spike: drive a full GSS context negotiation in-process (client ↔
//! server) and print the authenticated principal — the exit criterion for the
//! Kerberos accept/init round-trip.
//!
//! Requires a live KDC, a service keytab, and a client credential. Run it on
//! (or against) the bench KDC:
//!
//! ```text
//! export KRB5_CONFIG=/etc/krb5.conf                 # realm -> KDC mapping
//! export KRB5_KTNAME=/etc/skaidb.keytab             # the service keytab
//! kinit alice@SKAIDB.TEST                            # a client ticket
//! cargo run -p skaidb-gssapi --features kerberos --example roundtrip -- \
//!     skaidb/krb-kdc.skaidb.test@SKAIDB.TEST
//! ```
//!
//! Prints `AUTHENTICATED principal=<user@REALM>` on success.

#[cfg(not(feature = "kerberos"))]
fn main() {
    eprintln!("build with --features kerberos");
    std::process::exit(2);
}

#[cfg(feature = "kerberos")]
fn main() {
    use skaidb_gssapi::{ClientHandshake, ClientStep, ServerHandshake, ServerStep};

    let spn = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: roundtrip <service-principal-name>");
        std::process::exit(2);
    });
    // Keytab from KRB5_KTNAME (set in the environment above); the acceptor
    // resolves its own SPN from it.
    if let Ok(kt) = std::env::var("KRB5_KTNAME") {
        skaidb_gssapi::set_keytab(&kt);
    }

    // Client opens with the initial token targeting the service SPN.
    let (client, client_token) = ClientHandshake::new(&spn, None).expect("client init");
    // `Option` holders so the loop can `take` the owned states across
    // iterations without fighting the borrow checker (each `step` consumes).
    let mut client = Some(client);
    let mut server = Some(ServerHandshake::new(None).expect("server new"));
    let mut to_server = client_token;

    let mut principal = None;
    for round in 0..16 {
        assert!(round < 15, "handshake did not converge");
        // Server consumes the client's token.
        match server.take().expect("server present").step(&to_server).expect("server step") {
            ServerStep::Done { principal: p, token } => {
                principal = Some(p);
                // Final mutual-auth token (if any) lets the client verify the
                // server; its result doesn't affect acceptance.
                if let (Some(t), Some(c)) = (token, client.take()) {
                    let _ = c.step(&t).expect("client final step");
                }
                break;
            }
            ServerStep::Continue { next, token: server_token } => {
                server = Some(next);
                match client.take().expect("client present").step(&server_token).expect("client step") {
                    ClientStep::Continue { next, token } => {
                        client = Some(next);
                        to_server = token;
                    }
                    ClientStep::Done { token } => {
                        to_server = token.unwrap_or_default();
                    }
                }
            }
        }
    }

    match principal {
        Some(p) => println!("AUTHENTICATED principal={p}"),
        None => {
            eprintln!("FAILED: no principal");
            std::process::exit(1);
        }
    }
}
