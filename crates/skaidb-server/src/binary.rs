//! Binary (raw-TCP fast-path) endpoint (SPEC §11).

use std::io;
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use skaidb_proto::{
    auth_message, begin_frame, decode_client_request, finish_frame, read_frame, read_frame_into,
    tag_response, write_frame, AuthChallenge, AuthFinish, AuthMechanism, AuthOutcome, AuthStart,
    ClientRequest, Response, RowsChunkEncoder,
};
use skaidb_sql::ast::Statement;

use crate::authn::AuthResult;
use crate::metrics::Endpoint;
use crate::shared::{execute_session_statement_via, Shared};

/// Cap on statements prepared per connection, so a misbehaving client can't
/// grow the per-connection cache without bound. `Close` frees slots.
const MAX_PREPARED_PER_CONN: usize = 256;

/// Target payload size for one streamed-result chunk frame. A chunk closes at
/// the first row boundary past this, so the write buffer stays near this size
/// however large the result set is.
#[cfg(not(test))]
const STREAM_CHUNK_BYTES: usize = 256 * 1024;
#[cfg(test)]
const STREAM_CHUNK_BYTES: usize = 512;

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
    // Build the client-TLS acceptor once; a misconfiguration fails startup here
    // (loud) instead of silently serving plaintext.
    let acceptor = crate::tls::build(&ctx)?;
    let handle = thread::spawn(move || serve(listener, ctx, acceptor));
    Ok((local, handle))
}

/// Accept connections forever, handling each on its own thread.
pub fn serve(listener: TcpListener, ctx: Shared, acceptor: Option<crate::tls::Acceptor>) {
    for stream in listener.incoming().flatten() {
        let ctx = ctx.clone();
        let acceptor = acceptor.clone();
        thread::spawn(move || {
            ctx.metrics.connection_opened(Endpoint::Binary);
            handle_connection(stream, ctx.clone(), acceptor.as_ref());
            ctx.metrics.connection_closed(Endpoint::Binary);
        });
    }
}

/// Serve requests on one connection until the peer disconnects.
fn handle_connection(stream: TcpStream, ctx: Shared, acceptor: Option<&crate::tls::Acceptor>) {
    // Wrap in TLS per the acceptor policy (or refuse a plaintext peer under
    // `required`). A rustls session can't be split into separate reader/writer
    // handles, so the whole connection runs on one buffered `Conn`: reads go
    // through its buffer (one syscall per frame), writes go straight to the
    // stream (`write_frame` already coalesces header + payload).
    let Some(net_stream) = crate::tls::wrap(stream, acceptor) else {
        return; // plaintext refused, or TLS handshake failed
    };
    let mut conn = skaidb_net::Conn::new(net_stream);
    let remote_addr = conn
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // SCRAM handshake first; the resolved role authorizes every later
    // statement. The handshake shares this connection's read buffer, so bytes
    // it buffered ahead are still available to the request loop below.
    let role = match authenticate(&mut conn, &ctx) {
        Ok(role) => role,
        Err(()) => return, // denied or framing error → drop the connection
    };

    // Registers this connection in the replicated `drivers` table and
    // deregisters on every exit path (including the early returns below)
    // via `Drop` — see drivers.rs for why REST doesn't get this treatment.
    let node = ctx
        .backend
        .cluster_stats()
        .map_or_else(|| "local".to_string(), |c| c.node_id);
    let _driver_guard = crate::drivers::ConnGuard::new(&ctx, &node, &remote_addr, &role);

    // The current database is per-connection state: `USE` sets it for the life
    // of this connection; it starts at `default`.
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    // Per-connection transaction state: BEGIN/COMMIT/ROLLBACK (standalone
    // backend only) buffer into THIS connection's slot — private to it by
    // construction (the 2026-07-21 ACID audit's session-identity fix).
    let mut session_txn = skaidb_engine::SessionTxn::default();
    // `SET CONSISTENCY` session override: when set, it wins over the
    // per-request wire value until changed (SQL-only clients get the
    // knob; drivers that pass consistency explicitly can keep doing so
    // by not issuing SET CONSISTENCY).
    let mut session_consistency: Option<skaidb_proto::Consistency> = None;

    // Request and response buffers are reused across the connection's life,
    // so steady-state a request costs no allocation in the framing layer.
    // Prepared statements are per-connection state, like `current_db`.
    let mut inbuf = Vec::new();
    let mut outbuf = Vec::new();
    let mut prepared: Vec<Option<PreparedStmt>> = Vec::new();
    loop {
        if read_frame_into(&mut conn, &mut inbuf).is_err() {
            return; // disconnect or framing error
        }
        // Pipelining: a tagged request's id is echoed on every frame the
        // server sends for it, so a client may keep many requests in flight
        // and correlate by id. Execution stays serial and in submission
        // order on this connection (session state — current db, prepared
        // statements — keeps its sequential semantics); the win is the
        // eliminated per-request round-trip wait, not reordering.
        let (tag, request) = match decode_client_request(&inbuf) {
            Ok((tag, req)) => (tag, Ok(req)),
            Err(e) => (None, Err(e)),
        };
        let response = match request {
            Ok(ClientRequest::Query { sql, consistency }) => {
                if let Some(resp) = set_consistency_stmt(&sql, &mut session_consistency) {
                    resp
                } else {
                    let c = session_consistency.unwrap_or(consistency);
                    execute_session_statement_via(
                        &ctx,
                        &role,
                        &mut current_db,
                        &sql,
                        skaidb_sql::parse(&sql),
                        Some(c),
                        "driver",
                        Some(&mut session_txn),
                    )
                }
            }
            Ok(ClientRequest::QueryStream { sql, consistency }) => {
                let c = session_consistency.unwrap_or(consistency);
                let response = execute_session_statement_via(
                    &ctx,
                    &role,
                    &mut current_db,
                    &sql,
                    skaidb_sql::parse(&sql),
                    Some(c),
                    "driver",
                    Some(&mut session_txn),
                );
                if write_streamed(&mut conn, &mut outbuf, response, &ctx, tag).is_err() {
                    return;
                }
                continue;
            }
            Ok(ClientRequest::Prepare { sql }) => prepare(&mut prepared, sql),
            Ok(ClientRequest::Execute {
                id,
                params,
                consistency,
            }) => match prepared.get(id as usize).and_then(Option::as_ref) {
                Some(ps) => match skaidb_sql::bind(&ps.stmt, &params) {
                    Ok(bound) => execute_session_statement_via(
                        &ctx,
                        &role,
                        &mut current_db,
                        &ps.sql,
                        Ok(bound),
                        Some(session_consistency.unwrap_or(consistency)),
                        "driver",
                        Some(&mut session_txn),
                    ),
                    Err(e) => Response::Error(e.to_string()),
                },
                None => Response::Error(format!("unknown prepared statement id {id}")),
            },
            Ok(ClientRequest::ExecuteBatch {
                id,
                rows,
                consistency,
            }) => match prepared.get(id as usize).and_then(Option::as_ref) {
                Some(ps) => {
                    // One round-trip for the whole batch; each row binds and
                    // executes exactly like a looped Execute (per-row
                    // autocommit, RBAC on the bound statement). On the first
                    // failure the error names the row — earlier rows stay
                    // applied, matching the loop the driver used to run.
                    let c = Some(session_consistency.unwrap_or(consistency));
                    let mut affected: u64 = 0;
                    let mut failure = None;
                    for (i, params) in rows.iter().enumerate() {
                        let response = match skaidb_sql::bind(&ps.stmt, params) {
                            Ok(bound) => execute_session_statement_via(
                                &ctx,
                                &role,
                                &mut current_db,
                                &ps.sql,
                                Ok(bound),
                                c,
                                "driver",
                                Some(&mut session_txn),
                            ),
                            Err(e) => Response::Error(e.to_string()),
                        };
                        match response {
                            Response::Mutation { affected: n } => affected += n,
                            Response::Ddl | Response::Rows { .. } => {}
                            Response::Error(msg) => {
                                failure = Some(Response::Error(format!(
                                    "batch row {i}: {msg} ({affected} rows applied before it)"
                                )));
                                break;
                            }
                            other => {
                                failure = Some(other);
                                break;
                            }
                        }
                    }
                    failure.unwrap_or(Response::Mutation { affected })
                }
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
        if let Some(id) = tag {
            tag_response(&mut outbuf, id);
        }
        response.encode_into(&mut outbuf);
        // A result set past the frame limit used to fail `finish_frame` and
        // silently drop the connection; answer with a real error instead and
        // point at the way out.
        if outbuf.len() - 4 > skaidb_proto::MAX_FRAME_LEN as usize {
            begin_frame(&mut outbuf);
            if let Some(id) = tag {
                tag_response(&mut outbuf, id);
            }
            Response::Error(format!(
                "result set exceeds the {} MiB response frame limit; use a streaming query or add LIMIT",
                skaidb_proto::MAX_FRAME_LEN / (1024 * 1024)
            ))
            .encode_into(&mut outbuf);
        }
        ctx.metrics
            .add_bytes_returned(Endpoint::Binary, (outbuf.len() - 4) as u64);
        if finish_frame(&mut conn, &mut outbuf).is_err() {
            return;
        }
    }
}

/// Answer a `QueryStream` request. A row result goes out as a header frame,
/// chunk frames of ~[`STREAM_CHUNK_BYTES`], and an end frame; rows are
/// consumed (and their heap values freed) as they are encoded, so the peak
/// cost is the materialized rows plus one chunk — never a second full encoded
/// copy of the result. Non-row results go out as one ordinary frame. Under
/// pipelining (`tag`), every frame of the stream carries the request id.
fn write_streamed<W: io::Write>(
    writer: &mut W,
    outbuf: &mut Vec<u8>,
    response: Response,
    ctx: &Shared,
    tag: Option<u32>,
) -> io::Result<()> {
    let begin = |outbuf: &mut Vec<u8>| {
        begin_frame(outbuf);
        if let Some(id) = tag {
            tag_response(outbuf, id);
        }
    };
    let send = |writer: &mut W, outbuf: &mut Vec<u8>| {
        ctx.metrics
            .add_bytes_returned(Endpoint::Binary, (outbuf.len() - 4) as u64);
        finish_frame(writer, outbuf)
    };

    let (columns, rows) = match response {
        Response::Rows { columns, rows } => (columns, rows),
        other => {
            begin(outbuf);
            other.encode_into(outbuf);
            return send(writer, outbuf);
        }
    };

    begin(outbuf);
    Response::RowsHeader { columns }.encode_into(outbuf);
    send(writer, outbuf)?;

    let mut it = rows.into_iter().peekable();
    while it.peek().is_some() {
        begin(outbuf);
        let mut enc = RowsChunkEncoder::begin(outbuf);
        for row in it.by_ref() {
            enc.push_row(outbuf, &row);
            if outbuf.len() >= STREAM_CHUNK_BYTES {
                break;
            }
        }
        enc.finish(outbuf);
        send(writer, outbuf)?;
    }

    begin(outbuf);
    Response::RowsEnd.encode_into(outbuf);
    send(writer, outbuf)
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

/// Run the server side of the client handshake, returning the authorized role.
/// Dispatches on the mechanism the client selected in `AuthStart` (SCRAM by
/// default; GSSAPI when the client and server both support it).
fn authenticate<C: io::Read + io::Write>(conn: &mut C, ctx: &Shared) -> Result<String, ()> {
    let start = AuthStart::decode(&read_frame(conn).map_err(|_| ())?).map_err(|_| ())?;
    match start.mechanism {
        AuthMechanism::ScramSha256 => authenticate_scram(conn, ctx, &start),
        AuthMechanism::Gssapi => authenticate_gssapi(conn, ctx, &start),
    }
}

/// SCRAM-SHA-256: the four-frame password proof (SPEC §8.1).
fn authenticate_scram<C: io::Read + io::Write>(
    conn: &mut C,
    ctx: &Shared,
    start: &AuthStart,
) -> Result<String, ()> {
    let account = ctx.lookup_account(&start.username);
    let (salt, iterations) = ctx.authn.salt_for(account.as_ref());
    let server_nonce = ctx.authn.server_nonce(&start.client_nonce);
    let challenge = AuthChallenge {
        salt: salt.clone(),
        iterations,
        server_nonce: server_nonce.clone(),
    };
    write_frame(conn, &challenge.encode()).map_err(|_| ())?;

    let finish = AuthFinish::decode(&read_frame(conn).map_err(|_| ())?).map_err(|_| ())?;
    let am = auth_message(
        &start.username,
        &start.client_nonce,
        &server_nonce,
        &salt,
        iterations,
    );

    match ctx.authn.verify(
        account.as_ref(),
        &am,
        &finish.client_proof,
        &ctx.superuser_role,
    ) {
        AuthResult::Authenticated {
            role,
            server_signature,
        } => {
            write_frame(conn, &AuthOutcome::Ok { server_signature }.encode()).map_err(|_| ())?;
            ctx.metrics.incr_login();
            ctx.audit().log_login(&start.username, Some(&role), true);
            Ok(role)
        }
        AuthResult::Denied(reason) => {
            let _ = write_frame(conn, &AuthOutcome::Denied { reason }.encode());
            ctx.metrics.incr_login_failure();
            ctx.audit().log_login(&start.username, None, false);
            Err(())
        }
    }
}

/// Write a GSSAPI denial, record the failed login, and drop the connection.
fn deny_gssapi<C: io::Write>(
    conn: &mut C,
    ctx: &Shared,
    username: &str,
    reason: &str,
) -> Result<String, ()> {
    let _ = write_frame(conn, &AuthOutcome::Denied { reason: reason.to_string() }.encode());
    ctx.metrics.incr_login_failure();
    ctx.audit().log_login(username, None, false);
    Err(())
}

/// GSSAPI (Kerberos): an N-round `AuthToken` exchange driving the GSS accept
/// loop, ending in `AuthOutcome`. The client sends the first token; each side
/// shuttles tokens until the context is established, then the server maps the
/// authenticated principal to an external user's role (the phase-2 identity
/// seam). Compiled only with the `kerberos` feature (glibc/macOS/Windows).
#[cfg(feature = "kerberos")]
fn authenticate_gssapi<C: io::Read + io::Write>(
    conn: &mut C,
    ctx: &Shared,
    start: &AuthStart,
) -> Result<String, ()> {
    use skaidb_gssapi::{ServerHandshake, ServerStep};
    use skaidb_proto::AuthToken;

    // Config gate + optional explicit SPN (empty = accept any keytab key).
    let (enabled, spn) = {
        let cfg = ctx.config.read().unwrap_or_else(|e| e.into_inner());
        (
            cfg.auth.gssapi_enabled,
            (!cfg.auth.gssapi_service_principal.is_empty())
                .then(|| cfg.auth.gssapi_service_principal.clone()),
        )
    };
    if !enabled {
        return deny_gssapi(conn, ctx, &start.username, "GSSAPI authentication is not enabled");
    }

    let read_token = |conn: &mut C| -> Option<Vec<u8>> {
        AuthToken::decode(&read_frame(conn).ok()?).ok().map(|t| t.token)
    };
    let Some(mut client_token) = read_token(conn) else {
        return deny_gssapi(conn, ctx, &start.username, "expected an initial GSSAPI token");
    };
    let mut handshake = match ServerHandshake::new(spn.as_deref()) {
        Ok(h) => h,
        Err(e) => {
            return deny_gssapi(conn, ctx, &start.username, &format!("GSSAPI unavailable: {e}"));
        }
    };
    let principal = loop {
        match handshake.step(&client_token) {
            Ok(ServerStep::Continue { next, token }) => {
                if write_frame(conn, &AuthToken { token }.encode()).is_err() {
                    return Err(());
                }
                handshake = next;
                match read_token(conn) {
                    Some(t) => client_token = t,
                    None => return Err(()),
                }
            }
            Ok(ServerStep::Done { principal, token }) => {
                // Final mutual-auth token to the client, if the mechanism
                // produced one.
                if let Some(token) = token {
                    let _ = write_frame(conn, &AuthToken { token }.encode());
                }
                break principal;
            }
            Err(e) => {
                return deny_gssapi(
                    conn,
                    ctx,
                    &start.username,
                    &format!("GSSAPI authentication failed: {e}"),
                );
            }
        }
    };

    // Map the cryptographically-authenticated principal to a role. The
    // AuthStart username is untrusted here — only the GSS principal counts.
    match ctx.lookup_external_role(&principal) {
        Some(role) => {
            // GSSAPI has no SCRAM server signature; the GSS mutual-auth token
            // already proved the server. Send a zero signature for the shared
            // AuthOutcome shape.
            let _ = write_frame(conn, &AuthOutcome::Ok { server_signature: [0u8; 32] }.encode());
            ctx.metrics.incr_login();
            ctx.audit().log_login(&principal, Some(&role), true);
            Ok(role)
        }
        None => deny_gssapi(
            conn,
            ctx,
            &principal,
            &format!("no GSSAPI user for principal {principal:?} (create one with CREATE USER … GSSAPI)"),
        ),
    }
}

/// Without the `kerberos` feature (e.g. the static-musl build), GSSAPI is
/// refused cleanly so a client that requests it gets a definite answer.
#[cfg(not(feature = "kerberos"))]
fn authenticate_gssapi<C: io::Read + io::Write>(
    conn: &mut C,
    ctx: &Shared,
    start: &AuthStart,
) -> Result<String, ()> {
    deny_gssapi(conn, ctx, &start.username, "GSSAPI authentication is not enabled on this server")
}

/// Intercept `SET CONSISTENCY { ONE | QUORUM | ALL }` — per-connection
/// session state that never reaches the engine. `None` = not that
/// statement (execute normally). A cheap prefix check gates the parse so
/// ordinary statements pay nothing.
fn set_consistency_stmt(
    sql: &str,
    session: &mut Option<skaidb_proto::Consistency>,
) -> Option<skaidb_proto::Response> {
    let trimmed = sql.trim_start();
    if !trimmed
        .get(..3)
        .is_some_and(|w| w.eq_ignore_ascii_case("set"))
    {
        return None;
    }
    match skaidb_sql::parse(sql) {
        Ok(skaidb_sql::ast::Statement::SetConsistency { level }) => {
            *session = Some(match level.as_str() {
                "ONE" => skaidb_proto::Consistency::One,
                "ALL" => skaidb_proto::Consistency::All,
                _ => skaidb_proto::Consistency::Quorum,
            });
            Some(skaidb_proto::Response::Ddl)
        }
        _ => None, // not SET CONSISTENCY (e.g. SET CONFIG) — execute normally
    }
}
