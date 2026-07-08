//! skaidb server runtime: brings up the binary and REST endpoints over a shared
//! query engine (SPEC §7, §11).
//!
//! The binary endpoint is the raw-TCP fast path from `scp.txt`; QUIC is the
//! eventual WAN default. Both endpoints execute SQL against one [`Database`]
//! guarded by a reader-writer lock (thread-per-connection model; concurrent
//! readers, exclusive writers).

pub mod admin;
pub mod audit;
pub mod authn;
mod es;
pub mod binary;
pub mod memory;
mod promql;
mod promwrite;
pub mod metrics;
pub mod rest;
pub mod shared;
pub mod slowlog;
mod ui;

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
        return Ok(Backend::Local(Box::new(RwLock::new(db))));
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
    // Apply the `[storage]` tuning to every table/index engine the database
    // opens (memtable flush threshold and read-cache capacity; the remaining
    // engine knobs keep their built-in defaults). A `memory_target` budget
    // overrides the two fixed knobs — sized to the node's actual (cgroup)
    // memory limit when set to "auto".
    let mut flush_threshold_bytes = (config.storage.memtable_size_mb.max(1) as usize) * 1024 * 1024;
    let mut read_cache_capacity = config.storage.read_cache_entries as usize;
    let mut search_writer_heap_bytes = skaidb_engine::DEFAULT_SEARCH_WRITER_HEAP;
    match memory::resolve(&config.storage.memory_target) {
        Ok(Some(plan)) => {
            flush_threshold_bytes = plan.memtable_bytes as usize;
            read_cache_capacity = plan.read_cache_entries as usize;
            search_writer_heap_bytes = plan.search_writer_bytes as usize;
            skaidb_types::slog!(
                "skaidb: storage memory target {} MB (memtable {} MB, read cache {} entries, \
                 search writer {} MB/index)",
                plan.budget / (1024 * 1024),
                plan.memtable_bytes / (1024 * 1024),
                plan.read_cache_entries,
                plan.search_writer_bytes / (1024 * 1024)
            );
        }
        Ok(None) => {}
        Err(e) => return Err(e.into()),
    }
    let storage_opts = skaidb_engine::EngineOptions {
        flush_threshold_bytes,
        read_cache_capacity,
        search_writer_heap_bytes,
        ..Default::default()
    };
    let db = Database::open_with_options(&config.server.data_dir, storage_opts)?;

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

    // Point the operational log at `observability.log_file` before any worker
    // (cluster background threads, listeners) spawns, so startup and membership
    // events land in the same file as the audit log when one is configured.
    skaidb_types::init_server_log(&config.observability.log_file);

    let backend = build_backend(db, &config)?;

    let ctx: Shared = Arc::new(Context {
        backend,
        metrics,
        audit: RwLock::new(AuditSettings::from(&config.observability)),
        authn,
        superuser_role: config.auth.superuser.clone(),
        admin_lock: Mutex::new(()),
        start: Instant::now(),
        slow_log: crate::slowlog::SlowLog::new(),
        config: RwLock::new(config.clone()),
        config_path,
    });

    skaidb_types::slog!(
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
    skaidb_types::slog!(
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

    skaidb_types::slog!("skaidb binary endpoint listening on {binary_local}");
    skaidb_types::slog!("skaidb REST endpoint listening on http://{rest_local}/query");

    // Background NRT refresher: search-index refresh checks otherwise run
    // only on the write path, so an idle table's last index writes would
    // stay invisible to shared/read-only searches until traffic resumes.
    // The tick makes writes searchable within refresh_ms + the tick period.
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            ctx.backend.search_refresh_tick();
        });
    }

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
                skaidb_types::slog!("skaidb metrics endpoint listening on http://{prom_local}/metrics")
            }
            Err(e) => skaidb_types::slog!("skaidb: could not bind metrics port {prom_addr}: {e}"),
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
        Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
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

    /// Pipelined batches: all requests written before any response is read,
    /// responses correlated by id, per-statement errors inline, session
    /// state sequential across the batch, and the connection reusable after.
    #[test]
    fn pipelined_requests_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut client = Client::connect(addr).unwrap();

        let responses = client
            .pipeline(&[
                "CREATE TABLE t (PRIMARY KEY (id))",
                "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
                "SELECT nope FROM missing_table",
                "SELECT id, v FROM t ORDER BY id",
            ])
            .unwrap();
        assert_eq!(responses.len(), 4);
        assert_eq!(responses[0], Response::Ddl);
        assert_eq!(responses[1], Response::Mutation { affected: 2 });
        // The failing statement errors inline without stopping the batch.
        assert!(matches!(&responses[2], Response::Error(m) if m.contains("does not exist")));
        match &responses[3] {
            Response::Rows { rows, .. } => {
                assert_eq!(rows[0], vec![Value::Int(1), Value::Int(10)]);
                assert_eq!(rows[1], vec![Value::Int(2), Value::Int(20)]);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // The connection stays usable for ordinary (untagged) requests, and
        // an empty batch is a no-op.
        assert!(client.pipeline(&[]).unwrap().is_empty());
        assert!(matches!(
            client.execute("SELECT id FROM t").unwrap(),
            Response::Rows { .. }
        ));
    }

    #[test]
    fn prepared_statements_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut client = Client::connect(addr).unwrap();
        client.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        // Prepare once, execute repeatedly with different bindings.
        let mut ins = client
            .prepare("INSERT INTO t (id, v) VALUES (?, ?)")
            .unwrap();
        assert_eq!(ins.params, 2);
        for i in 0..3i64 {
            assert_eq!(
                client
                    .execute_prepared(&mut ins, &[Value::Int(i), Value::String(format!("v{i}"))])
                    .unwrap(),
                Response::Mutation { affected: 1 }
            );
        }

        let mut sel = client.prepare("SELECT v FROM t WHERE id = ?").unwrap();
        assert_eq!(sel.params, 1);
        match client.execute_prepared(&mut sel, &[Value::Int(1)]).unwrap() {
            Response::Rows { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::String("v1".into())]]);
            }
            other => panic!("expected rows, got {other:?}"),
        }

        // Arity mismatch and unpreparable statements are errors.
        let err = client
            .execute_prepared(&mut sel, &[Value::Int(1), Value::Int(2)])
            .unwrap_err();
        assert!(err.to_string().contains("parameters"), "got: {err}");
        let err = client.prepare("CREATE TABLE u (PRIMARY KEY (id))").unwrap_err();
        assert!(err.to_string().contains("cannot be prepared"), "got: {err}");

        // A `?` through the one-shot text path fails at execution, not parse.
        let err = client.execute("SELECT v FROM t WHERE id = ?").unwrap_err();
        assert!(err.to_string().contains("unbound parameter"), "got: {err}");

        // RBAC applies to prepared executes: the bound statement is checked.
        // (Privilege enforcement itself is covered by the RBAC tests; here we
        // just confirm the prepared path routes through the same check by
        // running a statement the superuser is allowed — no panic — and
        // verifying the audit/metrics path doesn't reject it.)
        assert!(matches!(
            client.execute_prepared(&mut sel, &[Value::Int(0)]).unwrap(),
            Response::Rows { .. }
        ));
    }

    #[test]
    fn streamed_query_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut client = Client::connect(addr).unwrap();
        client.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        // ~60 rows × ~100 B ≫ the test-profile chunk budget (512 B), so the
        // result crosses many chunk frames.
        let mut ins = client
            .prepare("INSERT INTO t (id, pad) VALUES (?, ?)")
            .unwrap();
        for i in 0..60i64 {
            client
                .execute_prepared(&mut ins, &[Value::Int(i), Value::String("x".repeat(100))])
                .unwrap();
        }

        // Full drain: all rows arrive, in order, with the right columns.
        {
            let stream = client
                .query_stream("SELECT id, pad FROM t ORDER BY id")
                .unwrap();
            assert_eq!(stream.columns, vec!["id", "pad"]);
            let rows: Vec<Vec<Value>> = stream.map(|r| r.unwrap()).collect();
            assert_eq!(rows.len(), 60);
            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row[0], Value::Int(i as i64));
            }
        }

        // Early drop mid-stream: the remaining frames are drained so the
        // connection stays usable for the next request.
        {
            let mut stream = client.query_stream("SELECT id FROM t ORDER BY id").unwrap();
            assert_eq!(stream.next().unwrap().unwrap(), vec![Value::Int(0)]);
        }
        assert!(matches!(
            client.execute("SELECT id FROM t WHERE id = 5").unwrap(),
            Response::Rows { .. }
        ));

        // Non-row statements through the stream API: single-frame answers.
        let s = client
            .query_stream("INSERT INTO t (id, pad) VALUES (100, 'y')")
            .unwrap();
        assert_eq!(s.affected, 1);
        assert_eq!(s.count(), 0);
        let err = client.query_stream("SELECT * FROM missing").unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {err}");

        // An empty result set still streams (header + end, no chunks).
        let stream = client
            .query_stream("SELECT id FROM t WHERE id = 9999")
            .unwrap();
        assert_eq!(stream.count(), 0);
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
    fn es_rest_subset_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        let http = |method: &str, path: &str, body: &str| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            let head = format!(
                "{method} {path} HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(body.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };

        // The ES index is a table + search index (pre-created; the subset
        // does not auto-create).
        http("POST", "/query", "CREATE TABLE logs (PRIMARY KEY (id))");
        http(
            "POST",
            "/query",
            // refresh_ms = 0 commits every write (the test spawns no
            // background refresher thread; ES-wise this is refresh=true).
            "CREATE SEARCH INDEX logs_fts ON logs (msg, level, bytes) \
             WITH (refresh_ms = 0, level.type = 'keyword', bytes.type = 'long')",
        );

        // _bulk: index with and without _id, plus a delete.
        let bulk = concat!(
            "{\"index\":{\"_id\":\"a1\"}}\n",
            "{\"msg\":\"error connecting to db\",\"level\":\"error\",\"bytes\":100}\n",
            "{\"index\":{\"_id\":\"a2\"}}\n",
            "{\"msg\":\"request served fine\",\"level\":\"info\",\"bytes\":250}\n",
            "{\"index\":{}}\n",
            "{\"msg\":\"another error in worker\",\"level\":\"error\",\"bytes\":50}\n",
            "{\"index\":{\"_id\":\"gone\"}}\n",
            "{\"msg\":\"to be deleted\",\"level\":\"info\",\"bytes\":1}\n",
            "{\"delete\":{\"_id\":\"gone\"}}\n",
        );
        let resp = http("POST", "/logs/_bulk", bulk);
        assert!(resp.contains("\"errors\":false"), "{resp}");

        // _count with a query.
        let resp = http(
            "POST",
            "/logs/_count",
            r#"{"query":{"match":{"msg":"error"}}}"#,
        );
        assert!(resp.contains("\"count\":2"), "{resp}");

        // _search: match + highlight, relevance-ordered by default.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"match":{"msg":"error"}},"highlight":{"fields":{"msg":{}}}}"#,
        );
        assert!(resp.contains("\"total\":{\"relation\":\"eq\",\"value\":2}"), "{resp}");
        assert!(resp.contains("<b>error</b>"), "{resp}");
        assert!(resp.contains("\"_id\":\"a1\""), "{resp}");
        // The deleted doc is gone.
        assert!(!resp.contains("to be deleted"), "{resp}");

        // bool + range + sort by a fast field.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"bool":{"must":[{"match":{"msg":"error"}}],"filter":[{"range":{"bytes":{"gte":60}}}]}},"sort":[{"bytes":{"order":"desc"}}]}"#,
        );
        assert!(resp.contains("\"total\":{\"relation\":\"eq\",\"value\":1}"), "{resp}");
        assert!(resp.contains("connecting"), "{resp}");

        // Aggregations: terms buckets + a metric, hits suppressed.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"size":0,"query":{"match_all":{}},"aggs":{"levels":{"terms":{"field":"level"},"aggs":{"b":{"sum":{"field":"bytes"}}}}}}"#,
        );
        assert!(resp.contains("\"key\":\"error\""), "{resp}");
        assert!(resp.contains("\"doc_count\":2"), "{resp}");
        assert!(resp.contains("\"b\":{\"value\":150"), "{resp}");

        // _mapping mirrors the search-index declaration.
        let resp = http("GET", "/logs/_mapping", "");
        assert!(resp.contains("\"msg\":{\"type\":\"text\"}"), "{resp}");
        assert!(resp.contains("\"level\":{\"type\":\"keyword\"}"), "{resp}");
        assert!(resp.contains("\"bytes\":{\"type\":\"long\"}"), "{resp}");

        // Unknown index → clean ES-shaped error.
        let resp = http("POST", "/nope/_count", "{}");
        assert!(resp.contains("does not exist") || resp.contains("error"), "{resp}");
    }

    /// The embedded web UI: assets served with CSP, `/ui/meta` shape, and the
    /// live `ui.enabled` toggle 404ing the whole prefix without a restart.
    #[test]
    fn web_ui_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        let get = |path: &str| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nConnection: close\r\n\r\n").as_bytes())
                .unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };

        // The shell and its assets, each with the locked-down CSP.
        let resp = get("/ui");
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains("Content-Type: text/html"), "{resp}");
        assert!(resp.contains("Content-Security-Policy: default-src 'none'"), "{resp}");
        assert!(resp.contains("<title>skaidb</title>"), "{resp}");
        assert!(get("/ui/app.css").contains("Content-Type: text/css"));
        assert!(get("/ui/app.js").contains("Content-Type: text/javascript"));

        // /ui/meta: version + auth mode, nothing secret.
        let resp = get("/ui/meta");
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains(&format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"))), "{resp}");
        assert!(resp.contains("\"auth_required\":false"), "{resp}");
        assert!(resp.contains("\"clustered\":false"), "{resp}");

        // Unknown paths under the prefix 404 even while enabled.
        assert!(get("/ui/nope").starts_with("HTTP/1.1 404"));

        // Live disable: every /ui path 404s immediately, and back.
        let (status, _) = ctx.config_set("ui.enabled", "false");
        assert_eq!(status, 200);
        assert!(get("/ui").starts_with("HTTP/1.1 404"));
        assert!(get("/ui/meta").starts_with("HTTP/1.1 404"));
        let (status, _) = ctx.config_set("ui.enabled", "true");
        assert_eq!(status, 200);
        assert!(get("/ui").starts_with("HTTP/1.1 200"));
    }

    /// The query console's per-request session database: `{"sql", "db"}`
    /// runs the statement with `db` current (the stateless gateway carries
    /// no session), and a bad name errors like `USE` would.
    #[test]
    fn rest_query_db_parameter() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        let post = |body: &str| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            let head = format!(
                "POST /query HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(body.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };

        post(r#"{"sql": "CREATE DATABASE app"}"#);
        post(r#"{"sql": "CREATE TABLE t (PRIMARY KEY (id))", "db": "app"}"#);
        post(r#"{"sql": "INSERT INTO t (id, v) VALUES (1, 'in-app')", "db": "app"}"#);

        // Visible with db=app, absent from the default database.
        let resp = post(r#"{"sql": "SELECT v FROM t", "db": "app"}"#);
        assert!(resp.contains("in-app"), "{resp}");
        let resp = post(r#"{"sql": "SELECT v FROM t"}"#);
        assert!(resp.contains("does not exist"), "{resp}");

        // A bad database name errors cleanly.
        let resp = post(r#"{"sql": "SELECT 1 FROM t", "db": "nope"}"#);
        assert!(resp.starts_with("HTTP/1.1 400") || resp.contains("error"), "{resp}");
    }

    #[test]
    fn remote_write_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        let body = crate::promwrite::tests::encode_write_request(&[
            (
                &[("__name__", "http_requests_total"), ("job", "api")],
                &[(0, 0.0), (60_000, 60.0)],
            ),
            (&[("__name__", "up"), ("job", "api")], &[(60_000, 1.0)]),
        ]);
        // Raw HTTP POST (binary body).
        let mut stream = TcpStream::connect(addr).unwrap();
        let head = format!(
            "POST /api/v1/write HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("200"), "{resp}");
        assert!(resp.contains("\"accepted\":3"), "{resp}");

        // The auto-created metrics table serves SQL, labels intact and the
        // counter math works over the ingested samples.
        let sql = "SELECT rate(value) FROM metrics WHERE name = 'http_requests_total' AND job = 'api'";
        let mut stream = TcpStream::connect(addr).unwrap();
        let head = format!(
            "POST /query HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            sql.len()
        );
        stream.write_all(head.as_bytes()).unwrap();
        stream.write_all(sql.as_bytes()).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        assert!(resp.contains("[[1.0]]"), "{resp}");
    }

    #[test]
    fn promql_api_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        // Ingest counters via remote_write: two jobs, 60s apart, +60/step.
        let body = crate::promwrite::tests::encode_write_request(&[
            (
                &[("__name__", "http_requests_total"), ("job", "api")],
                &[(0, 0.0), (60_000, 60.0), (120_000, 120.0)],
            ),
            (
                &[("__name__", "http_requests_total"), ("job", "web")],
                &[(0, 0.0), (60_000, 30.0), (120_000, 60.0)],
            ),
        ]);
        let post = |path: &str, body: &[u8], form: bool| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            let ctype = if form {
                "Content-Type: application/x-www-form-urlencoded\r\n"
            } else {
                ""
            };
            let head = format!(
                "POST {path} HTTP/1.1\r\n{ctype}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };
        let get = |path: &str| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            let head = format!("GET {path} HTTP/1.1\r\nConnection: close\r\n\r\n");
            stream.write_all(head.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };
        assert!(post("/api/v1/write", &body, false).contains("200"));

        // Instant query at t=120s: raw selector returns both series.
        let resp = get("/api/v1/query?query=http_requests_total&time=120");
        assert!(resp.contains("\"resultType\":\"vector\""), "{resp}");
        assert!(resp.contains("\"__name__\":\"http_requests_total\""), "{resp}");
        assert!(resp.contains("\"job\":\"api\"") && resp.contains("\"job\":\"web\""));

        // Range query, form-POST like Grafana: sum(rate(...)[2m])) = 1+0.5.
        let form = "query=sum%28rate%28http_requests_total%5B2m%5D%29%29&start=120&end=120&step=60";
        let resp = post("/api/v1/query_range", form.as_bytes(), true);
        assert!(resp.contains("\"resultType\":\"matrix\""), "{resp}");
        assert!(resp.contains("[120.0,\"1.5\"]") || resp.contains("[120,\"1.5\"]"), "{resp}");

        // by-clause keeps job separate: api rates 1/s.
        let resp = get(
            "/api/v1/query_range?query=sum%20by%20%28job%29%20%28rate%28http_requests_total%5B2m%5D%29%29&start=120&end=120&step=60",
        );
        assert!(resp.contains("\"job\":\"api\"") && resp.contains("\"1\""), "{resp}");

        // Metadata endpoints.
        let resp = get("/api/v1/labels");
        assert!(resp.contains("__name__") && resp.contains("job"), "{resp}");
        let resp = get("/api/v1/label/job/values");
        assert!(resp.contains("api") && resp.contains("web"), "{resp}");
        let resp = get("/api/v1/label/__name__/values");
        assert!(resp.contains("http_requests_total"), "{resp}");
        let resp = get("/api/v1/status/buildinfo");
        assert!(resp.contains("skaidb"), "{resp}");
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
        let mut authn = AuthState::required();
        authn.add_user("ada", "pencil", "admin");
        Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
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
    fn sql_created_user_authenticates_and_is_rbac_limited() {
        use skaidb_driver::Client;
        // Auth required; superuser is ada (config). She creates a limited
        // user via SQL, who then logs in over SCRAM and hits RBAC walls.
        let ctx = auth_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut su = Client::connect_with(addr, "ada", "pencil").unwrap();
        su.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        su.execute("INSERT INTO t (id) VALUES (1)").unwrap();
        su.execute("CREATE USER bob PASSWORD 'hunter2'").unwrap();
        su.execute("GRANT SELECT ON t TO bob").unwrap();

        // Wrong password rejected; right password in.
        assert!(Client::connect_with(addr, "bob", "wrong").is_err());
        let mut bob = Client::connect_with(addr, "bob", "hunter2").unwrap();
        assert!(matches!(
            bob.execute("SELECT id FROM t").unwrap(),
            Response::Rows { .. }
        ));
        let err = bob.execute("INSERT INTO t (id) VALUES (2)").unwrap_err();
        assert!(err.to_string().contains("permission denied"), "{err}");
        // User management needs Grant, which bob lacks.
        let err = bob.execute("CREATE USER eve PASSWORD 'x'").unwrap_err();
        assert!(err.to_string().contains("permission denied"), "{err}");

        // Password rotation invalidates the old secret.
        su.execute("ALTER USER bob PASSWORD 'correct-horse'").unwrap();
        assert!(Client::connect_with(addr, "bob", "hunter2").is_err());
        assert!(Client::connect_with(addr, "bob", "correct-horse").is_ok());

        // Dropped users can't come back.
        su.execute("DROP USER bob").unwrap();
        assert!(Client::connect_with(addr, "bob", "correct-horse").is_err());
    }

    #[test]
    fn rbac_enforced_per_statement() {
        use crate::shared::{execute, execute_as};
        let ctx: Shared = Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
        });

        // Superuser sets up the table, the role, and its grant — via SQL.
        assert_eq!(
            execute(&ctx, "CREATE TABLE t (PRIMARY KEY (id))"),
            Response::Ddl
        );
        assert_eq!(execute(&ctx, "CREATE ROLE reader"), Response::Ddl);
        assert_eq!(execute(&ctx, "GRANT SELECT ON t TO reader"), Response::Ddl);
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

        // Per-database grants: SELECT on the session's whole database covers
        // its tables; a grant on some other database does not.
        assert_eq!(execute(&ctx, "CREATE ROLE analyst"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "GRANT SELECT ON DATABASE default TO analyst"),
            Response::Ddl
        );
        assert!(matches!(
            execute_as(&ctx, "analyst", "SELECT id FROM t"),
            Response::Rows { .. }
        ));
        match execute_as(&ctx, "analyst", "INSERT INTO t (id) VALUES (2)") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
        assert_eq!(execute(&ctx, "CREATE ROLE outsider"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "GRANT SELECT ON DATABASE elsewhere TO outsider"),
            Response::Ddl
        );
        match execute_as(&ctx, "outsider", "SELECT id FROM t") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
        // The database grant shows with its db: object form.
        match execute(&ctx, "SHOW GRANTS FOR analyst") {
            Response::Rows { rows, .. } => assert!(
                rows.iter().any(|r| r[2] == Value::String("db:default".into())),
                "got: {rows:?}"
            ),
            other => panic!("expected rows, got {other:?}"),
        }

        // Self-inspection: a role without the Grant privilege may look at its
        // own grants — and only its own.
        match execute_as(&ctx, "reader", "SHOW GRANTS FOR reader") {
            Response::Rows { rows, .. } => {
                assert!(!rows.is_empty());
                assert!(rows.iter().all(|r| r[0] == Value::String("reader".into())));
            }
            other => panic!("expected rows, got {other:?}"),
        }
        match execute_as(&ctx, "reader", "SHOW GRANTS FOR analyst") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
        match execute_as(&ctx, "reader", "SHOW GRANTS") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
    }

    /// Auth DDL leaves secret-free audit entries in the identity log.
    #[test]
    fn auth_ddl_is_audit_logged_without_secrets() {
        use crate::shared::execute;
        let log_path = {
            let mut p = std::env::temp_dir();
            p.push(format!("skaidb-auth-audit-{}.log", std::process::id()));
            let _ = std::fs::remove_file(&p);
            p
        };
        let obs = skaidb_config::ObservabilityConfig {
            log_file: log_path.display().to_string(),
            ..Default::default()
        };
        let ctx: Shared = Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(crate::audit::AuditSettings::from(&obs)),
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            config_path: None,
        });

        assert_eq!(
            execute(&ctx, "CREATE USER bob PASSWORD 'hunter2secret'"),
            Response::Ddl
        );
        assert_eq!(execute(&ctx, "CREATE ROLE reader"), Response::Ddl);
        assert_eq!(execute(&ctx, "GRANT ROLE reader TO bob"), Response::Ddl);
        // A failed auth DDL is recorded too, marked ok=false.
        assert!(matches!(
            execute(&ctx, "DROP ROLE ghost"),
            Response::Error(_)
        ));

        let log = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            log.contains("[auth-ddl] actor=superuser ok=true CREATE USER bob"),
            "got: {log}"
        );
        assert!(
            log.contains("[auth-ddl] actor=superuser ok=true GRANT ROLE reader TO bob"),
            "got: {log}"
        );
        assert!(
            log.contains("[auth-ddl] actor=superuser ok=false DROP ROLE ghost"),
            "got: {log}"
        );
        // Never the password — not in the auth-ddl line, and masked in the
        // query log sharing the file.
        assert!(!log.contains("hunter2secret"), "got: {log}");
        let _ = std::fs::remove_file(&log_path);
    }

    #[test]
    fn connection_scoped_current_database() {
        use crate::shared::execute_session_as;
        let ctx: Shared = Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
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
