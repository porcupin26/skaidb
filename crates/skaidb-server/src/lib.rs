//! skaidb server runtime: brings up the binary and REST endpoints over a shared
//! query engine (SPEC §7, §11).
//!
//! The binary endpoint is the raw-TCP fast path from `scp.txt`; QUIC is the
//! eventual WAN default. Both endpoints execute SQL against one [`Database`]
//! guarded by a mutex (thread-per-connection model).

pub mod binary;
pub mod rest;
pub mod shared;

use std::sync::{Arc, Mutex};

use skaidb_config::Config;
use skaidb_engine::Database;

use crate::shared::SharedDb;

/// Open the database from `config` and serve the binary + REST endpoints,
/// blocking until the binary accept loop ends.
pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let db: SharedDb = Arc::new(Mutex::new(Database::open(&config.server.data_dir)?));

    let bind = &config.server.bind_addr;
    let binary_addr = format!("{}:{}", bind, config.server.quic_port);
    let rest_addr = format!("{}:{}", bind, config.server.rest_port);

    let (binary_local, binary_handle) = binary::spawn(&binary_addr, db.clone())?;
    let (rest_local, _rest_handle) = rest::spawn(&rest_addr, db.clone())?;

    println!("skaidb binary endpoint listening on {binary_local}");
    println!("skaidb REST endpoint listening on http://{rest_local}/query");

    binary_handle.join().ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicU64, Ordering};

    use skaidb_driver::Client;
    use skaidb_proto::Response;
    use skaidb_types::Value;

    fn temp_db() -> SharedDb {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-server-it-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(Mutex::new(Database::open(dir).unwrap()))
    }

    #[test]
    fn binary_endpoint_end_to_end() {
        let db = temp_db();
        let (addr, _h) = binary::spawn("127.0.0.1:0", db).unwrap();

        let mut client = Client::connect(addr).unwrap();
        assert_eq!(
            client.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap(),
            Response::Ddl
        );
        assert_eq!(
            client
                .execute("INSERT INTO t (id, name) VALUES (1, 'ada'), (2, 'bob')")
                .unwrap(),
            Response::Mutation { affected: 2 }
        );

        let resp = client
            .execute("SELECT id, name FROM t ORDER BY id")
            .unwrap();
        match resp {
            Response::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows[0], vec![Value::Int(1), Value::String("ada".into())]);
                assert_eq!(rows[1], vec![Value::Int(2), Value::String("bob".into())]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn binary_endpoint_reports_errors() {
        let db = temp_db();
        let (addr, _h) = binary::spawn("127.0.0.1:0", db).unwrap();
        let mut client = Client::connect(addr).unwrap();
        // Selecting a missing table is a server-side error.
        let err = client.execute("SELECT * FROM missing").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {err}");
    }

    #[test]
    fn rest_endpoint_end_to_end() {
        let db = temp_db();
        let (addr, _h) = rest::spawn("127.0.0.1:0", db).unwrap();

        // DDL + insert, then a query — each over its own connection.
        assert!(http_post(addr, "CREATE TABLE t (PRIMARY KEY (id))").contains("\"ok\":true"));
        assert!(
            http_post(addr, "INSERT INTO t (id, v) VALUES (1, 'hello')").contains("\"affected\":1")
        );

        let body = http_post(addr, "{\"sql\": \"SELECT v FROM t\"}");
        assert!(body.contains("\"columns\":[\"v\"]"), "got: {body}");
        assert!(body.contains("hello"), "got: {body}");
    }

    /// Send a `POST /query` with `sql` as the body and return the response body.
    fn http_post(addr: std::net::SocketAddr, sql: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        let req = format!(
            "POST /query HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            sql.len(),
            sql
        );
        stream.write_all(req.as_bytes()).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        // Return just the body (after the header terminator).
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or(response)
    }
}
