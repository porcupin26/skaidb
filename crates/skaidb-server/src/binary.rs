//! Binary (raw-TCP fast-path) endpoint (SPEC §11).

use std::io;
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use skaidb_proto::{
    auth_message, read_frame, write_frame, AuthChallenge, AuthFinish, AuthOutcome, AuthStart,
    Request, Response,
};

use crate::authn::AuthResult;
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
            ctx.metrics
                .incr("skaidb_connections_total{endpoint=\"binary\"}");
            ctx.metrics
                .gauge_inc("skaidb_connections_active{endpoint=\"binary\"}");
            handle_connection(stream, ctx.clone());
            ctx.metrics
                .gauge_dec("skaidb_connections_active{endpoint=\"binary\"}");
        });
    }
}

/// Serve requests on one connection until the peer disconnects.
fn handle_connection(mut stream: TcpStream, ctx: Shared) {
    stream.set_nodelay(true).ok();

    // SCRAM handshake first; the resolved role authorizes every later statement.
    let role = match authenticate(&mut stream, &ctx) {
        Ok(role) => role,
        Err(()) => return, // denied or framing error → drop the connection
    };

    // The current database is per-connection state: `USE` sets it for the life
    // of this connection; it starts at `default`.
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();

    loop {
        let payload = match read_frame(&mut stream) {
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
        ctx.metrics.add(
            "skaidb_bytes_returned_total{endpoint=\"binary\"}",
            encoded.len() as u64,
        );
        if write_frame(&mut stream, &encoded).is_err() {
            return;
        }
    }
}

/// Run the server side of the SCRAM handshake, returning the authorized role.
fn authenticate(stream: &mut TcpStream, ctx: &Shared) -> Result<String, ()> {
    let start = AuthStart::decode(&read_frame(stream).map_err(|_| ())?).map_err(|_| ())?;

    let (salt, iterations) = ctx.authn.salt_for(&start.username);
    let server_nonce = ctx.authn.server_nonce(&start.client_nonce);
    let challenge = AuthChallenge {
        salt: salt.clone(),
        iterations,
        server_nonce: server_nonce.clone(),
    };
    write_frame(stream, &challenge.encode()).map_err(|_| ())?;

    let finish = AuthFinish::decode(&read_frame(stream).map_err(|_| ())?).map_err(|_| ())?;
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
            write_frame(stream, &AuthOutcome::Ok { server_signature }.encode()).map_err(|_| ())?;
            ctx.metrics.incr("skaidb_logins_total");
            ctx.audit().log_login(&start.username, Some(&role), true);
            Ok(role)
        }
        AuthResult::Denied(reason) => {
            let _ = write_frame(stream, &AuthOutcome::Denied { reason }.encode());
            ctx.metrics.incr("skaidb_login_failures_total");
            ctx.audit().log_login(&start.username, None, false);
            Err(())
        }
    }
}
