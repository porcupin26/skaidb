//! skaidb server runtime: brings up the binary and REST endpoints over a shared
//! query engine (SPEC §7, §11).
//!
//! The binary endpoint is the raw-TCP fast path from `scp.txt`; QUIC is the
//! eventual WAN default. Both endpoints execute SQL against one [`Database`]
//! guarded by a mutex (thread-per-connection model).

pub mod admin;
pub mod audit;
pub mod authn;
pub mod binary;
pub mod metrics;
pub mod rest;
pub mod shared;
pub mod slowlog;

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use skaidb_auth::RoleStore;
use skaidb_cluster::{Authenticator, Consistency as ClusterConsistency, Node, NodeConfig, NodeId};
use skaidb_config::{Config, Consistency, InternodeAuth};
use skaidb_engine::Database;

use crate::audit::AuditSettings;
use crate::authn::AuthState;
use crate::metrics::Metrics;
use crate::shared::{Backend, Context, Shared};

/// Build the execution backend: a cluster coordinator when seeds are
/// configured, otherwise a standalone local engine.
fn build_backend(db: Database, config: &Config) -> Result<Backend, Box<dyn std::error::Error>> {
    if config.cluster.seeds.is_empty() {
        return Ok(Backend::Local(Box::new(Mutex::new(db))));
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
        auth: build_internode_auth(config)?,
        auto_join: true,
        anti_entropy_interval_secs: config.cluster.anti_entropy_interval_secs,
    };
    let node = Node::new(db, node_cfg);
    node.serve_internode()?;
    Ok(Backend::Cluster(node))
}

/// Build the internode authenticator from `[auth]` config. Fails closed: a mode
/// that's selected but missing its secret/cert material is an error, not a
/// silent fallback to no auth.
fn build_internode_auth(config: &Config) -> Result<Arc<Authenticator>, Box<dyn std::error::Error>> {
    let a = &config.auth;
    match a.internode_auth {
        InternodeAuth::None => Ok(Arc::new(Authenticator::None)),
        InternodeAuth::Token => {
            let secret = if !a.internode_token.is_empty() {
                a.internode_token.clone().into_bytes()
            } else if !a.internode_keyfile.is_empty() {
                std::fs::read(&a.internode_keyfile)
                    .map_err(|e| format!("internode token keyfile {}: {e}", a.internode_keyfile))?
            } else {
                return Err(
                    "internode_auth=token requires internode_token or internode_keyfile".into(),
                );
            };
            // File-based tokens usually have a trailing newline; ignore edge whitespace.
            let start = secret.iter().position(|b| !b.is_ascii_whitespace());
            let end = secret.iter().rposition(|b| !b.is_ascii_whitespace());
            let secret = match (start, end) {
                (Some(s), Some(e)) => secret[s..=e].to_vec(),
                _ => Vec::new(),
            };
            if secret.is_empty() {
                return Err("internode token is empty".into());
            }
            Ok(Arc::new(Authenticator::token(secret)))
        }
        InternodeAuth::Cert => {
            if a.internode_tls_cert.is_empty()
                || a.internode_tls_key.is_empty()
                || a.internode_tls_ca.is_empty()
            {
                return Err("internode_auth=cert requires internode_tls_cert, internode_tls_key, and internode_tls_ca".into());
            }
            Ok(Arc::new(Authenticator::cert(
                &a.internode_tls_cert,
                &a.internode_tls_key,
                &a.internode_tls_ca,
            )?))
        }
    }
}

fn map_consistency(c: Consistency) -> ClusterConsistency {
    match c {
        Consistency::One => ClusterConsistency::One,
        Consistency::Quorum => ClusterConsistency::Quorum,
        Consistency::All => ClusterConsistency::All,
    }
}

/// Open the database from `config` and serve the binary + REST endpoints,
/// blocking until the binary accept loop ends. `config_path` is the file the
/// config was loaded from (if any), so `config set` can persist changes back.
pub fn run(
    config: Config,
    config_path: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let clustered = !config.cluster.seeds.is_empty();
    let bind = config.server.bind_addr.clone();
    // Node identity: its ring id when clustered, else its client endpoint.
    let node_id = if clustered {
        format!("{}:{}", bind, config.cluster.internode_port)
    } else {
        format!("{}:{}", bind, config.server.quic_port)
    };
    let role_label = match config.server.node_role {
        skaidb_config::NodeRole::Member => "member",
        skaidb_config::NodeRole::Agent => "agent",
    };

    let metrics = Metrics::new();
    metrics.set("skaidb_up", 1);
    // Build/identity info as labelled `=1` gauges (standard deploy/restart
    // tracking), plus the process start time for uptime computation.
    metrics.set(
        &format!(
            "skaidb_build_info{{version=\"{}\",git_sha=\"{}\",rustc=\"{}\"}}",
            env!("CARGO_PKG_VERSION"),
            option_env!("SKAIDB_GIT_SHA").unwrap_or("unknown"),
            option_env!("SKAIDB_RUSTC").unwrap_or("unknown"),
        ),
        1,
    );
    metrics.set(
        &format!("skaidb_node_info{{node_id=\"{node_id}\",role=\"{role_label}\"}}"),
        1,
    );
    let start_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    metrics.set("skaidb_start_time_seconds", start_unix);

    let backend = build_backend(db, &config)?;

    let ctx: Shared = Arc::new(Context {
        backend,
        metrics,
        audit: RwLock::new(AuditSettings::from(&config.observability)),
        roles,
        authn,
        superuser_role: config.auth.superuser.clone(),
        admin_lock: Mutex::new(()),
        start: Instant::now(),
        slow_log: crate::slowlog::SlowLog::new(),
        config: RwLock::new(config.clone()),
        config_path,
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

    let binary_addr = format!("{}:{}", bind, config.server.quic_port);
    let rest_addr = format!("{}:{}", bind, config.server.rest_port);

    let (binary_local, binary_handle) = binary::spawn(&binary_addr, ctx.clone())?;
    let (rest_local, _rest_handle) = rest::spawn(&rest_addr, ctx.clone())?;

    println!("skaidb binary endpoint listening on {binary_local}");
    println!("skaidb REST endpoint listening on http://{rest_local}/query");

    // Dedicated metrics/health listener on `observability.prometheus_port`. It
    // reuses the same handler (so `/metrics`, `/health`, `/ready`, `/status` are
    // served), giving scrapers a port separate from the data plane. Bound only
    // when it differs from the REST port (otherwise `/metrics` on the REST port
    // already covers it) and is non-zero.
    let prom_port = config.observability.prometheus_port;
    if prom_port != 0 && prom_port != config.server.rest_port {
        let prom_addr = format!("{}:{}", bind, prom_port);
        match rest::spawn(&prom_addr, ctx.clone()) {
            Ok((prom_local, _h)) => {
                println!("skaidb metrics endpoint listening on http://{prom_local}/metrics")
            }
            Err(e) => eprintln!("skaidb: could not bind metrics port {prom_addr}: {e}"),
        }
    }

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
        AuditSettings::quiet()
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
            backend: Backend::Local(Box::new(Mutex::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
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

    #[test]
    fn metrics_render_correct_types_and_histogram() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        http_post(addr, "CREATE TABLE t (PRIMARY KEY (id))");
        http_post(addr, "INSERT INTO t (id) VALUES (1)");
        http_post(addr, "SELECT * FROM t");

        let m = http_get(addr, "/metrics");
        // Query latency is a histogram with buckets/sum/count.
        assert!(m.contains("# TYPE skaidb_query_duration_seconds histogram"));
        assert!(m.contains("skaidb_query_duration_seconds_count{type=\"select\"}"));
        // HELP lines are emitted.
        assert!(m.contains("# HELP skaidb_queries_total"));
        // Storage gauges are populated at scrape time via the pull model.
        assert!(m.contains("# TYPE skaidb_storage_tables gauge"));
        assert!(m.contains("skaidb_uptime_seconds"));
    }

    #[test]
    fn health_ready_and_status_endpoints() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();

        assert_eq!(http_get_status(addr, "/health"), 200);
        assert_eq!(http_get_status(addr, "/ready"), 200);
        let status = http_get(addr, "/status");
        assert!(status.contains("\"clustered\":false"), "got: {status}");
    }

    #[test]
    fn show_tables_over_rest() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        http_post(addr, "CREATE TABLE alpha (PRIMARY KEY (id))");
        http_post(addr, "CREATE TABLE beta (PRIMARY KEY (id))");
        let body = http_post(addr, "SHOW TABLES");
        assert!(body.contains("alpha"), "got: {body}");
        assert!(body.contains("beta"), "got: {body}");
        assert!(body.contains("\"columns\":[\"table\",\"primary_key\"]"), "got: {body}");
    }

    /// GET `path`, returning the HTTP status code.
    fn http_get_status(addr: std::net::SocketAddr, path: &str) -> u16 {
        let mut stream = TcpStream::connect(addr).unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Context requiring SCRAM auth: user `ada`/`pencil` acting as `admin`.
    fn auth_ctx() -> Shared {
        let mut roles = RoleStore::new();
        roles.create_superuser("admin");
        let mut authn = AuthState::required();
        authn.add_user("ada", "pencil", "admin");
        Arc::new(Context {
            backend: Backend::Local(Box::new(Mutex::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            roles,
            authn,
            superuser_role: "admin".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
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
            backend: Backend::Local(Box::new(Mutex::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
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
    fn connection_scoped_current_database() {
        use crate::shared::execute_session_as;
        let mut roles = RoleStore::new();
        roles.create_superuser("su");
        let ctx: Shared = Arc::new(Context {
            backend: Backend::Local(Box::new(Mutex::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "su".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
        });

        // One connection's current database, carried across statements.
        let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
        let run = |db: &mut String, sql: &str| execute_session_as(&ctx, "su", db, sql, None);

        assert_eq!(run(&mut db, "CREATE DATABASE shop"), Response::Ddl);
        assert_eq!(run(&mut db, "USE shop"), Response::Ddl);
        assert_eq!(db, "shop"); // USE updated the connection state
        run(&mut db, "CREATE TABLE orders (PRIMARY KEY (id))");
        run(&mut db, "INSERT INTO orders (id) VALUES (1)");

        // The table is visible in shop...
        assert!(matches!(
            run(&mut db, "SELECT id FROM orders"),
            Response::Rows { rows, .. } if rows.len() == 1
        ));
        // ...isolated from default...
        run(&mut db, "USE default");
        assert!(matches!(run(&mut db, "SELECT id FROM orders"), Response::Error(_)));
        // ...and reachable cross-database by qualifier.
        assert!(matches!(
            run(&mut db, "SELECT id FROM shop.orders"),
            Response::Rows { rows, .. } if rows.len() == 1
        ));
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

    /// POST `body` to an arbitrary `path`; returns `(status, body)`.
    fn http_post_path(addr: std::net::SocketAddr, path: &str, body: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
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

    /// A single-node cluster `Context`: superuser has `Admin`, `reader` does not.
    fn cluster_ctx() -> Shared {
        let mut roles = RoleStore::new();
        roles.create_superuser("superuser");
        roles.create_role("reader").unwrap();
        roles
            .grant("reader", Privilege::Select, Object::Global)
            .unwrap();
        let id = "127.0.0.1:0"; // not served — status needs no network
        let cfg = NodeConfig {
            id: NodeId::new(id),
            internode_addr: id.to_string(),
            members: vec![(NodeId::new(id), id.to_string())],
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: ClusterConsistency::Quorum,
            write_consistency: ClusterConsistency::Quorum,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let node = Node::new(Database::open(temp_dir()).unwrap(), cfg);
        Arc::new(Context {
            backend: Backend::Cluster(node),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            roles,
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
        })
    }

    #[test]
    fn admin_status_and_rbac() {
        use crate::admin::{self, AdminCmd};
        let ctx = cluster_ctx();

        // Admin sees topology.
        let (status, body) = admin::handle(&ctx, "superuser", AdminCmd::Status);
        assert_eq!(status, 200);
        let s = body.to_string();
        assert!(s.contains("\"clustered\":true"), "got: {s}");
        assert!(s.contains("\"replication_factor\":1"), "got: {s}");
        assert!(s.contains("\"epoch\":0"), "got: {s}");

        // A non-admin role is denied even read-only status.
        let (status, _) = admin::handle(&ctx, "reader", AdminCmd::Status);
        assert_eq!(status, 403);

        // A standalone (non-cluster) node reports clustered:false and rejects ops.
        let local = temp_ctx();
        assert_eq!(admin::handle(&local, "superuser", AdminCmd::Status).0, 200);
        assert!(admin::handle(&local, "superuser", AdminCmd::Status)
            .1
            .to_string()
            .contains("\"clustered\":false"));
        assert_eq!(
            admin::handle(&local, "superuser", AdminCmd::AddNode("x:1".into())).0,
            400
        );
    }

    #[test]
    fn admin_status_over_rest() {
        let ctx = cluster_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        let (status, body) = http_post_path(addr, "/admin/status", "");
        assert_eq!(status, 200);
        assert!(body.contains("\"clustered\":true"), "got: {body}");
        // Unknown admin route → 404.
        assert_eq!(http_post_path(addr, "/admin/bogus", "").0, 404);
    }

    #[test]
    fn status_advertises_member_client_endpoints() {
        let ctx = cluster_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        // /status carries the members' client SQL endpoints (host:quic_port) so a
        // client that reached one seed can discover its peers for failover.
        let body = http_get(addr, "/status");
        assert!(body.contains("\"endpoints\""), "got: {body}");
        assert!(body.contains("127.0.0.1:7000"), "got: {body}");
    }

    #[test]
    fn admin_config_show_get_set_over_rest() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();

        // Show: full config, with the (empty) superuser password present.
        let (status, body) = http_post_path(addr, "/admin/config", "");
        assert_eq!(status, 200);
        assert!(body.contains("\"observability\""), "got: {body}");

        // Get one key.
        let (status, body) =
            http_post_path(addr, "/admin/config/get", r#"{"key":"observability.slow_query_ms"}"#);
        assert_eq!(status, 200);
        assert!(body.contains("\"value\":200"), "got: {body}");

        // Set a runtime-mutable key: applied live, no restart, not persisted
        // (this ctx has no config file).
        let (status, body) = http_post_path(
            addr,
            "/admin/config/set",
            r#"{"key":"observability.slow_query_ms","value":"250"}"#,
        );
        assert_eq!(status, 200);
        assert!(body.contains("\"applied\":true"), "got: {body}");
        assert!(body.contains("\"restart_required\":false"), "got: {body}");
        assert!(body.contains("\"persisted\":false"), "got: {body}");

        // The change is visible on a subsequent get.
        let (_, body) =
            http_post_path(addr, "/admin/config/get", r#"{"key":"observability.slow_query_ms"}"#);
        assert!(body.contains("\"value\":250"), "got: {body}");

        // A startup-only key validates but reports restart_required.
        let (status, body) = http_post_path(
            addr,
            "/admin/config/set",
            r#"{"key":"cluster.replication_factor","value":"5"}"#,
        );
        assert_eq!(status, 200);
        assert!(body.contains("\"restart_required\":true"), "got: {body}");

        // An invalid value is rejected.
        let (status, _) = http_post_path(
            addr,
            "/admin/config/set",
            r#"{"key":"cluster.replication_factor","value":"lots"}"#,
        );
        assert_eq!(status, 400);
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
