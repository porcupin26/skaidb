//! skaidb server runtime: brings up the binary and REST endpoints over a shared
//! query engine (SPEC §7, §11).
//!
//! The binary endpoint is the raw-TCP fast path from `scp.txt`; QUIC is the
//! eventual WAN default. Both endpoints execute SQL against one [`Database`]
//! guarded by a mutex (thread-per-connection model).

pub mod audit;
pub mod authn;
pub mod binary;
pub mod metrics;
pub mod rest;
pub mod shared;

use std::sync::{Arc, Mutex};

use skaidb_auth::RoleStore;
use skaidb_cluster::{Consistency as ClusterConsistency, Node, NodeConfig, NodeId};
use skaidb_config::{Config, Consistency};
use skaidb_engine::Database;

use crate::audit::AuditSettings;
use crate::authn::AuthState;
use crate::metrics::Metrics;
use crate::shared::{Backend, Context, Shared};

/// Build the execution backend: a cluster coordinator when seeds are
/// configured, otherwise a standalone local engine.
fn build_backend(db: Database, config: &Config) -> Result<Backend, Box<dyn std::error::Error>> {
    if config.cluster.seeds.is_empty() {
        return Ok(Backend::Local(Mutex::new(db)));
    }
    let internode_addr = format!(
        "{}:{}",
        config.server.bind_addr, config.cluster.internode_port
    );
    let members: Vec<(NodeId, String)> = config
        .cluster
        .seeds
        .iter()
        .map(|addr| (NodeId::new(addr.clone()), addr.clone()))
        .collect();
    let node_cfg = NodeConfig {
        id: NodeId::new(internode_addr.clone()),
        internode_addr,
        members,
        replication_factor: config.cluster.replication_factor as usize,
        vnodes_per_node: config.cluster.vnodes_per_node,
        read_consistency: map_consistency(config.cluster.default_read_consistency),
        write_consistency: map_consistency(config.cluster.default_write_consistency),
    };
    let node = Node::new(db, node_cfg);
    node.serve_internode()?;
    Ok(Backend::Cluster(node))
}

fn map_consistency(c: Consistency) -> ClusterConsistency {
    match c {
        Consistency::One => ClusterConsistency::One,
        Consistency::Quorum => ClusterConsistency::Quorum,
        Consistency::All => ClusterConsistency::All,
    }
}

/// Open the database from `config` and serve the binary + REST endpoints,
/// blocking until the binary accept loop ends.
pub fn run(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open(&config.server.data_dir)?;

    // Bootstrap the configured superuser role (SPEC §8.2).
    let mut roles = RoleStore::new();
    roles.create_superuser(&config.auth.superuser);

    // Require auth only when SCRAM is enabled and a superuser password is set.
    let auth_required = config.auth.scram_enabled && !config.auth.superuser_password.is_empty();
    let mut authn = if auth_required {
        AuthState::required()
    } else {
        AuthState::disabled()
    };
    if auth_required {
        authn.add_user(
            &config.auth.superuser,
            &config.auth.superuser_password,
            &config.auth.superuser,
        );
    }

    let metrics = Metrics::new();
    metrics.set("skaidb_up", 1);

    let clustered = !config.cluster.seeds.is_empty();
    let backend = build_backend(db, &config)?;

    let ctx: Shared = Arc::new(Context {
        backend,
        metrics,
        audit: AuditSettings::from(&config.observability),
        roles,
        authn,
        superuser_role: config.auth.superuser.clone(),
    });

    println!(
        "skaidb mode: {}",
        if clustered {
            format!(
                "cluster ({} members, internode :{})",
                config.cluster.seeds.len(),
                config.cluster.internode_port
            )
        } else {
            "standalone".to_string()
        }
    );
    println!(
        "skaidb authentication: {}",
        if auth_required {
            "required (SCRAM)"
        } else {
            "disabled (anonymous)"
        }
    );

    let bind = &config.server.bind_addr;
    let binary_addr = format!("{}:{}", bind, config.server.quic_port);
    let rest_addr = format!("{}:{}", bind, config.server.rest_port);

    let (binary_local, binary_handle) = binary::spawn(&binary_addr, ctx.clone())?;
    let (rest_local, _rest_handle) = rest::spawn(&rest_addr, ctx.clone())?;

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

    use skaidb_auth::{Object, Privilege};
    use skaidb_driver::Client;
    use skaidb_proto::Response;
    use skaidb_types::Value;

    fn quiet_audit() -> AuditSettings {
        AuditSettings {
            query_log: false,
            query_masked: true,
            slow_query_ms: 0,
            login_log: false,
            error_log: false,
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-server-it-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Context with auth disabled; anonymous connections act as the superuser.
    fn temp_ctx() -> Shared {
        let mut roles = RoleStore::new();
        roles.create_superuser("superuser");
        Arc::new(Context {
            backend: Backend::Local(Mutex::new(Database::open(temp_dir()).unwrap())),
            metrics: Metrics::new(),
            audit: quiet_audit(),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
        })
    }

    #[test]
    fn binary_endpoint_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();

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
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut client = Client::connect(addr).unwrap();
        // Selecting a missing table is a server-side error.
        let err = client.execute("SELECT * FROM missing").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {err}");
    }

    #[test]
    fn rest_endpoint_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();

        // DDL + insert, then a query — each over its own connection.
        assert!(http_post(addr, "CREATE TABLE t (PRIMARY KEY (id))").contains("\"ok\":true"));
        assert!(
            http_post(addr, "INSERT INTO t (id, v) VALUES (1, 'hello')").contains("\"affected\":1")
        );

        let body = http_post(addr, "{\"sql\": \"SELECT v FROM t\"}");
        assert!(body.contains("\"columns\":[\"v\"]"), "got: {body}");
        assert!(body.contains("hello"), "got: {body}");
    }

    #[test]
    fn metrics_endpoint_reports_query_counts() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        http_post(addr, "CREATE TABLE t (PRIMARY KEY (id))");
        http_post(addr, "INSERT INTO t (id) VALUES (1)");
        http_post(addr, "SELECT * FROM t");

        let metrics = http_get(addr, "/metrics");
        assert!(
            metrics.contains("# TYPE skaidb_queries_total counter"),
            "got: {metrics}"
        );
        assert!(
            metrics.contains("skaidb_queries_total{type=\"select\"} 1"),
            "got: {metrics}"
        );
        assert!(
            metrics.contains("skaidb_queries_total{type=\"ddl\"} 1"),
            "got: {metrics}"
        );
    }

    /// Context requiring SCRAM auth: user `ada`/`pencil` acting as `admin`.
    fn auth_ctx() -> Shared {
        let mut roles = RoleStore::new();
        roles.create_superuser("admin");
        let mut authn = AuthState::required();
        authn.add_user("ada", "pencil", "admin");
        Arc::new(Context {
            backend: Backend::Local(Mutex::new(Database::open(temp_dir()).unwrap())),
            metrics: Metrics::new(),
            audit: quiet_audit(),
            roles,
            authn,
            superuser_role: "admin".into(),
        })
    }

    #[test]
    fn scram_handshake_accepts_correct_and_rejects_otherwise() {
        let (addr, _h) = binary::spawn("127.0.0.1:0", auth_ctx()).unwrap();

        // Correct password authenticates and can run statements.
        let mut client = Client::connect_with(addr, "ada", "pencil").unwrap();
        assert_eq!(
            client.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap(),
            Response::Ddl
        );

        // Wrong password, unknown user, and anonymous are all rejected.
        assert!(Client::connect_with(addr, "ada", "WRONG").is_err());
        assert!(Client::connect_with(addr, "ghost", "x").is_err());
        assert!(Client::connect(addr).is_err());
    }

    #[test]
    fn rbac_enforced_per_statement() {
        use crate::shared::{execute, execute_as};
        let mut roles = RoleStore::new();
        roles.create_superuser("superuser");
        roles.create_role("reader").unwrap();
        roles
            .grant("reader", Privilege::Select, Object::Table("t".into()))
            .unwrap();
        let ctx: Shared = Arc::new(Context {
            backend: Backend::Local(Mutex::new(Database::open(temp_dir()).unwrap())),
            metrics: Metrics::new(),
            audit: quiet_audit(),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
        });

        // Superuser sets up the table.
        assert_eq!(
            execute(&ctx, "CREATE TABLE t (PRIMARY KEY (id))"),
            Response::Ddl
        );
        // Reader may SELECT.
        assert!(matches!(
            execute_as(&ctx, "reader", "SELECT id FROM t"),
            Response::Rows { .. }
        ));
        // Reader may not INSERT.
        match execute_as(&ctx, "reader", "INSERT INTO t (id) VALUES (1)") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
    }

    #[test]
    fn rest_basic_auth_enforced() {
        let (addr, _h) = rest::spawn("127.0.0.1:0", auth_ctx()).unwrap();

        // Correct credentials (ada:pencil → base64) authenticate.
        let (status, _) = http_post_auth(
            addr,
            "CREATE TABLE t (PRIMARY KEY (id))",
            Some("YWRhOnBlbmNpbA=="),
        );
        assert_eq!(status, 200, "valid basic auth should succeed");

        // No credentials → 401.
        let (status, _) = http_post_auth(addr, "SELECT 1 FROM t", None);
        assert_eq!(status, 401);

        // Wrong password (ada:wrong → base64) → 401.
        let (status, _) = http_post_auth(addr, "SELECT 1 FROM t", Some("YWRhOndyb25n"));
        assert_eq!(status, 401);
    }

    /// POST /query with an optional `Authorization: Basic` value; returns
    /// (status code, body).
    fn http_post_auth(addr: std::net::SocketAddr, sql: &str, basic: Option<&str>) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        let auth = basic
            .map(|b| format!("Authorization: Basic {b}\r\n"))
            .unwrap_or_default();
        let req = format!(
            "POST /query HTTP/1.1\r\nHost: localhost\r\n{auth}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            sql.len(),
            sql
        );
        stream.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        let status = resp
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = resp
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }

    fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or(response)
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
