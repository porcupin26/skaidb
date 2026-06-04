//! Binary (raw-TCP fast-path) endpoint (SPEC §11).

use std::io;
use std::net::{TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use skaidb_proto::{read_frame, write_frame, Request, Response};

use crate::shared::{execute, Shared};

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
        thread::spawn(move || handle_connection(stream, ctx));
    }
}

/// Serve requests on one connection until the peer disconnects.
fn handle_connection(mut stream: TcpStream, ctx: Shared) {
    stream.set_nodelay(true).ok();
    loop {
        let payload = match read_frame(&mut stream) {
            Ok(p) => p,
            Err(_) => return, // disconnect or framing error
        };
        let response = match Request::decode(&payload) {
            Ok(req) => execute(&ctx, &req.sql),
            Err(e) => Response::Error(format!("protocol error: {e}")),
        };
        if write_frame(&mut stream, &response.encode()).is_err() {
            return;
        }
    }
}
