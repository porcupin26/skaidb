//! Binary (raw-TCP fast-path) endpoint (SPEC §11).

use std::io::{self, BufReader};
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use skaidb_proto::{
    auth_message, begin_frame, finish_frame, read_frame, read_frame_into, write_frame,
    AuthChallenge, AuthFinish, AuthOutcome, AuthStart, ClientRequest, Response,
};
use skaidb_sql::ast::Statement;

use crate::authn::AuthResult;
use crate::metrics::Endpoint;
use crate::shared::{execute_session_as, execute_session_statement_as, Shared};

/// Cap on statements prepared per connection, so a misbehaving client can't
/// grow the per-connection cache without bound. `Close` frees slots.
const MAX_PREPARED_PER_CONN: usize = 256;

/// One statement prepared on a connection: the original template text (for
/// audit/metrics) and its parsed form, bound with fresh parameters on every
/// `Execute`.
struct PreparedStmt {
    sql: String,
    stmt: Statement,
}

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

    // Request and response buffers are reused across the connection's life,
    // so steady-state a request costs no allocation in the framing layer.
    // Prepared statements are per-connection state, like `current_db`.
    let mut inbuf = Vec::new();
    let mut outbuf = Vec::new();
    let mut prepared: Vec<Option<PreparedStmt>> = Vec::new();
    loop {
        if read_frame_into(&mut reader, &mut inbuf).is_err() {
            return; // disconnect or framing error
        }
        let response = match ClientRequest::decode(&inbuf) {
            Ok(ClientRequest::Query { sql, consistency }) => {
                execute_session_as(&ctx, &role, &mut current_db, &sql, Some(consistency))
            }
            Ok(ClientRequest::Prepare { sql }) => prepare(&mut prepared, sql),
            Ok(ClientRequest::Execute {
                id,
                params,
                consistency,
            }) => match prepared.get(id as usize).and_then(Option::as_ref) {
                Some(ps) => match skaidb_sql::bind(&ps.stmt, &params) {
                    Ok(bound) => execute_session_statement_as(
                        &ctx,
                        &role,
                        &mut current_db,
                        &ps.sql,
                        Ok(bound),
                        Some(consistency),
                    ),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error(format!("unknown prepared statement id {id}")),
            },
            Ok(ClientRequest::Close { id }) => {
                match prepared.get_mut(id as usize).map(Option::take) {
                    Some(Some(_)) => Response::Ddl,
                    _ => Response::Error(format!("unknown prepared statement id {id}")),
                }
            }
            Err(e) => Response::Error(format!("protocol error: {e}")),
        };
        begin_frame(&mut outbuf);
        response.encode_into(&mut outbuf);
        ctx.metrics
            .add_bytes_returned(Endpoint::Binary, (outbuf.len() - 4) as u64);
        if finish_frame(&mut writer, &mut outbuf).is_err() {
            return;
        }
    }
}

/// Handle a `Prepare`: parse once, reject unpreparable statement kinds, and
/// stash the template in the connection's statement table (reusing a closed
/// slot when one is free).
fn prepare(prepared: &mut Vec<Option<PreparedStmt>>, sql: String) -> Response {
    let stmt = match skaidb_sql::parse(&sql) {
        Ok(s) => s,
        Err(e) => return Response::Error(e.to_string()),
    };
    let Some(nparams) = skaidb_sql::param_count(&stmt) else {
        return Response::Error("statement kind cannot be prepared".into());
    };
    let entry = PreparedStmt { sql, stmt };
    let id = match prepared.iter().position(Option::is_none) {
        Some(free) => {
            prepared[free] = Some(entry);
            free
        }
        None if prepared.len() >= MAX_PREPARED_PER_CONN => {
            return Response::Error(format!(
                "too many prepared statements (max {MAX_PREPARED_PER_CONN}); close some first"
            ));
        }
        None => {
            prepared.push(Some(entry));
            prepared.len() - 1
        }
    };
    Response::Prepared {
        id: id as u32,
        params: nparams as u16,
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
