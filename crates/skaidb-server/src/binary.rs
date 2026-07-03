//! Binary (raw-TCP fast-path) endpoint (SPEC §11).

use std::io::{self, BufReader};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use skaidb_proto::{
    auth_message, read_frame, write_frame, AuthChallenge, AuthFinish, AuthOutcome, AuthStart,
    Request, Response,
};

use crate::authn::AuthResult;
use crate::metrics::Endpoint;
use crate::shared::{execute_session_as, Shared};

/// Bind the binary endpoint and serve it on a background thread.
///
/// Returns the bound address (useful when binding to port 0 in tests) and the
/// accept-loop join handle.
pub fn spawn(addr: &str, ctx: Shared) -> io::Result<(std::net::SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?;
    let handle = thread::spawn(move || serve(listener, ctx));
    Ok((local, handle))
}

/// Accept connections forever, handling each on its own thread.
pub fn serve(listener: TcpListener, ctx: Shared) {
    for stream in listener.incoming().flatten() {
        let ctx = ctx.clone();
        thread::spawn(move || {
            ctx.metrics.connection_opened(Endpoint::Binary);
            handle_connection(stream, ctx.clone());
            ctx.metrics.connection_closed(Endpoint::Binary);
        });
    }
}

/// Serve requests on one connection until the peer disconnects.
fn handle_connection(stream: TcpStream, ctx: Shared) {
    stream.set_nodelay(true).ok();

    // Split the socket once per connection: the read side goes behind a
    // `BufReader` so each frame costs one read syscall instead of two
    // (length prefix + payload). The write side stays unbuffered —
    // `write_frame` already coalesces header and payload into a single
    // write, so flush semantics are unchanged.
    let mut writer = match stream.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut reader = BufReader::new(stream);

    // SCRAM handshake first; the resolved role authorizes every later
    // statement. The handshake shares this connection's `BufReader`, so bytes
    // it buffered ahead are still available to the request loop below.
    let role = match authenticate(&mut reader, &mut writer, &ctx) {
        Ok(role) => role,
        Err(()) => return, // denied or framing error → drop the connection
    };

    // The current database is per-connection state: `USE` sets it for the life
    // of this connection; it starts at `default`.
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();

    loop {
        let payload = match read_frame(&mut reader) {
            Ok(p) => p,
            Err(_) => return, // disconnect or framing error
        };
        let response = match Request::decode(&payload) {
            Ok(req) => {
                execute_session_as(&ctx, &role, &mut current_db, &req.sql, Some(req.consistency))
            }
            Err(e) => Response::Error(format!("protocol error: {e}")),
        };
        let encoded = response.encode();
        ctx.metrics
            .add_bytes_returned(Endpoint::Binary, encoded.len() as u64);
        if write_frame(&mut writer, &encoded).is_err() {
            return;
        }
    }
}

/// Run the server side of the SCRAM handshake, returning the authorized role.
fn authenticate(
    reader: &mut BufReader<TcpStream>,
    writer: &mut TcpStream,
    ctx: &Shared,
) -> Result<String, ()> {
    let start = AuthStart::decode(&read_frame(reader).map_err(|_| ())?).map_err(|_| ())?;

    let (salt, iterations) = ctx.authn.salt_for(&start.username);
    let server_nonce = ctx.authn.server_nonce(&start.client_nonce);
    let challenge = AuthChallenge {
        salt: salt.clone(),
        iterations,
        server_nonce: server_nonce.clone(),
    };
    write_frame(writer, &challenge.encode()).map_err(|_| ())?;

    let finish = AuthFinish::decode(&read_frame(reader).map_err(|_| ())?).map_err(|_| ())?;
    let am = auth_message(
        &start.username,
        &start.client_nonce,
        &server_nonce,
        &salt,
        iterations,
    );

    match ctx.authn.verify(
        &start.username,
        &am,
        &finish.client_proof,
        &ctx.superuser_role,
    ) {
        AuthResult::Authenticated {
            role,
            server_signature,
        } => {
            write_frame(writer, &AuthOutcome::Ok { server_signature }.encode()).map_err(|_| ())?;
            ctx.metrics.incr_login();
            ctx.audit().log_login(&start.username, Some(&role), true);
            Ok(role)
        }
        AuthResult::Denied(reason) => {
            let _ = write_frame(writer, &AuthOutcome::Denied { reason }.encode());
            ctx.metrics.incr_login_failure();
            ctx.audit().log_login(&start.username, None, false);
            Err(())
        }
    }
}
