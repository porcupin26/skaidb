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
mod drivers;
pub mod memory;
mod naming;
mod nodestats;
mod promql;
mod promwrite;
pub mod metrics;
pub mod rest;
pub mod shared;
pub mod slowlog;
mod ui;
mod witness_pull;
mod witnesses;

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
    // Config-file seed for the live-mutable duty ceiling (SET CONFIG
    // updates it at runtime through the same setter).
    node.set_bootstrap_duty_pct(config.cluster.bootstrap_duty_pct);
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
    let mut ts_head_max_bytes = 0u64; // unbounded without a budget
    match memory::resolve(&config.storage.memory_target) {
        Ok(Some(plan)) => {
            flush_threshold_bytes = plan.memtable_bytes as usize;
            read_cache_capacity = plan.read_cache_entries as usize;
            search_writer_heap_bytes = plan.search_writer_bytes as usize;
            ts_head_max_bytes = plan.ts_head_bytes;
            skaidb_types::slog!(
                "skaidb: storage memory target {} MB (memtable {} MB, read cache {} entries, \
                 search writer {} MB/index, ts head {} MB/table)",
                plan.budget / (1024 * 1024),
                plan.memtable_bytes / (1024 * 1024),
                plan.read_cache_entries,
                plan.search_writer_bytes / (1024 * 1024),
                plan.ts_head_bytes / (1024 * 1024)
            );
        }
        Ok(None) => {}
        Err(e) => return Err(e.into()),
    }
    let storage_opts = skaidb_engine::EngineOptions {
        flush_threshold_bytes,
        read_cache_capacity,
        search_writer_heap_bytes,
        ts_head_max_bytes,
        scan_row_budget: config.storage.scan_row_budget as usize,
        statement_timeout_secs: config.storage.statement_timeout_secs,
        // Server mode: a large FTS rebuild pages in the background instead
        // of blocking the listener for minutes at startup (#75).
        defer_search_startup: true,
        ..Default::default()
    };
    let db = Database::open_with_options(&config.server.data_dir, storage_opts)?;

    // Restart accounting: bump the persistent start counter and, when the
    // cgroup's OOM-kill count moved since the previous start, say so — the
    // most common "why did this node restart" answer on small nodes, and
    // otherwise invisible (the OOM killer leaves no trace in our own logs).
    {
        let (starts, new_ooms) = skaidb_cluster::host::record_start(std::path::Path::new(
            &config.server.data_dir,
        ));
        if new_ooms > 0 {
            skaidb_types::slog!(
                "skaidb: start #{starts} — {new_ooms} kernel OOM kill(s) in this cgroup since \
                 the previous start (the last run likely died to the OOM killer)"
            );
        } else if starts > 1 {
            skaidb_types::slog!("skaidb: start #{starts}");
        }
    }

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
        drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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
        std::thread::spawn(move || {
            // Vector snapshots checkpoint on a slow cadence riding the same
            // thread: a crash then replays minutes of writes, not everything
            // since the last graceful shutdown.
            const VECTOR_CHECKPOINT_EVERY: std::time::Duration =
                std::time::Duration::from_secs(600);
            let mut last_ckpt = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(200));
                ctx.backend.search_refresh_tick();
                if last_ckpt.elapsed() >= VECTOR_CHECKPOINT_EVERY {
                    last_ckpt = std::time::Instant::now();
                    ctx.backend.vector_checkpoint_tick();
                }
            }
        });
    }

    // Self-scrape (`observability.self_scrape`, live-mutable): ingest the
    // node's own /metrics into the `metrics` time-series table so the node
    // can dashboard itself with no external Prometheus. The loop re-reads
    // the live config each second, so `config set` toggles it immediately.
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let mut last = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let (on, every) = ctx
                    .config
                    .read()
                    .map(|c| {
                        (
                            c.observability.self_scrape,
                            c.observability.self_scrape_interval_secs.max(1),
                        )
                    })
                    .unwrap_or((false, 15));
                if !on || last.elapsed().as_secs() < every {
                    continue;
                }
                last = std::time::Instant::now();
                if let Err(e) = crate::promwrite::self_scrape_tick(&ctx) {
                    eprintln!("skaidb self-scrape: {e}");
                }
            }
        });
    }

    // Node-stats publishing (`observability.node_stats`, live-mutable):
    // Graceful shutdown: SIGTERM/SIGINT flush memtables and commit search
    // writers before exit, so the next start replays (almost) nothing — an
    // unclean kill costs a full search-index rebuild from a stale watermark
    // (the known ~15-minute restart penalty). systemd's `restart`/`stop`
    // send SIGTERM, so every routine deploy goes through this path.
    // Unix-only: signal_hook's iterator API doesn't exist on Windows, where
    // stops are hard kills anyway.
    #[cfg(unix)]
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            use signal_hook::consts::{SIGINT, SIGTERM};
            use signal_hook::iterator::Signals;
            let Ok(mut signals) = Signals::new([SIGTERM, SIGINT]) else {
                return;
            };
            if signals.forever().next().is_some() {
                skaidb_types::slog!(
                    "skaidb: shutdown signal — flushing memtables, committing search writers"
                );
                ctx.backend.prepare_shutdown();
                skaidb_types::slog!("skaidb: clean shutdown");
                std::process::exit(0);
            }
        });
    }

    // INSERT this node's host stats into the replicated `node_stats` table
    // every `node_stats_interval_secs` (default 1s), timestamped — the
    // dashboard reads the table (data + age) instead of probing peers, so a
    // missed probe can't flap a live node to "unreachable".
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let mut ensured = false;
            let mut last = std::time::Instant::now();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let (on, every) = ctx
                    .config
                    .read()
                    .map(|c| {
                        (
                            c.observability.node_stats,
                            c.observability.node_stats_interval_secs.max(1),
                        )
                    })
                    .unwrap_or((false, 1));
                if !on || last.elapsed().as_secs() < every {
                    continue;
                }
                last = std::time::Instant::now();
                if !ensured {
                    // The cluster may not be ready at boot; keep trying.
                    ensured = crate::nodestats::ensure_table(&ctx).is_ok();
                    if !ensured {
                        continue;
                    }
                }
                // Best-effort: a shedding/degraded tick is skipped, and the
                // row's age surfaces exactly that on the dashboard.
                let _ = crate::nodestats::publish_tick(&ctx);
            }
        });
    }

    // Cluster + node naming: seed a random cluster name (first member to
    // find none wins the benign LWW race) and self-register this node's
    // random alias, keyed by the stable internode id. Witness-mode nodes
    // SKIP local bootstrap — their identity mirrors the primary (the pull
    // cycle writes the learned names into the local tables instead).
    if !ctx.config_snapshot().witness.enabled {
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let self_id = ctx
                .backend
                .cluster_stats()
                .map_or_else(|| "local".to_string(), |c| c.node_id);
            loop {
                if crate::naming::bootstrap(&ctx, &self_id).is_ok() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
    }

    // Witness mode: start the pull loop when [witness] is enabled (no-op,
    // with a logged refusal, otherwise — see witness_pull.rs).
    witness_pull::spawn_if_enabled(ctx.clone());

    // Witness-aware garbage collection: every minute, size the deepest-
    // level tombstone-retention window from the witness registry (how far
    // back the least-caught-up live witness is, capped at the grace
    // period) and push it into the storage engines — a tombstone purged
    // before a witness pulls it would resurrect the deleted row on the
    // backup forever. With no registered witnesses the floor is 0 and
    // tombstones drop immediately, exactly as before.
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(60));
            let floor = crate::witnesses::tombstone_retention_floor_ms(&ctx);
            match &ctx.backend {
                crate::shared::Backend::Local(db) => {
                    if let Ok(mut db) = db.write() {
                        db.set_tombstone_retention_ms(floor);
                    }
                }
                crate::shared::Backend::Cluster(node) => {
                    node.with_local_write(|db| db.set_tombstone_retention_ms(floor));
                }
            }
        });
    }

    // Ensure the witness-registry tables exist. Unlike `node_stats`, nothing
    // here self-publishes on a tick — rows arrive from witness processes
    // registering themselves over ordinary SQL connections (see
    // `witnesses.rs` and `.priv/witness-node-plan.md`) — so this only needs
    // to run once, with the same "cluster may not be ready at boot" retry
    // tolerance `node_stats` has.
    {
        let ctx = ctx.clone();
        std::thread::spawn(move || loop {
            if crate::witnesses::ensure_tables(&ctx).is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
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
pub(crate) mod tests {
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
    pub(crate) fn temp_ctx() -> Shared {
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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

        // SET CONSISTENCY is per-connection session state (acks as DDL).
        assert_eq!(
            client.execute("SET CONSISTENCY ALL").unwrap(),
            Response::Ddl
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

    /// ExecuteBatch runs a prepared statement once per row in one
    /// round-trip; a mid-batch failure names the row and keeps earlier rows.
    #[test]
    fn execute_batch_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx).unwrap();
        let mut client = Client::connect(addr).unwrap();
        client.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        let mut ins = client
            .prepare("INSERT INTO t (id, tags) VALUES (?, ?)")
            .unwrap();
        let rows: Vec<Vec<Value>> = (0..100i64)
            .map(|i| {
                vec![
                    Value::Int(i),
                    Value::Array(vec![Value::String(format!("t{}", i % 5))]),
                ]
            })
            .collect();
        assert_eq!(client.execute_batch(&mut ins, rows).unwrap(), 100);
        match client.execute("SELECT count(*) FROM t").unwrap() {
            Response::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Int(100)),
            other => panic!("{other:?}"),
        }
        // A bad row (arity) fails naming the row; the earlier row applied.
        let mixed = vec![
            vec![Value::Int(200), Value::Array(vec![])],
            vec![Value::Int(201)], // wrong arity
        ];
        let err = client.execute_batch(&mut ins, mixed).unwrap_err().to_string();
        assert!(err.contains("batch row 1"), "{err}");
        assert!(err.contains("1 rows applied"), "{err}");
        match client.execute("SELECT count(*) FROM t WHERE id = 200").unwrap() {
            Response::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Int(1)),
            other => panic!("{other:?}"),
        }
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

    /// `/query` row results stream as chunked JSON (no response-sized buffer,
    /// no 64 MiB row cap) and reassemble into exactly the old payload shape;
    /// non-row responses keep the Content-Length single-frame path.
    #[test]
    fn rest_query_rows_stream_chunked() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();
        let http = |body: &str| -> String {
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
        // DDL/mutations: plain Content-Length responses.
        let resp = http("CREATE TABLE t (PRIMARY KEY (id))");
        assert!(resp.contains("Content-Length:"), "{resp}");
        // Enough rows to cross the 64 KiB flush threshold mid-stream.
        for batch in 0..5 {
            let mut vals = Vec::new();
            for i in 0..200 {
                let id = batch * 200 + i;
                vals.push(format!("({id}, '{}')", "x".repeat(120)));
            }
            http(&format!("INSERT INTO t (id, v) VALUES {}", vals.join(", ")));
        }
        let resp = http("SELECT id, v FROM t");
        assert!(resp.contains("Transfer-Encoding: chunked"), "{resp}");
        // Reassemble the chunked body and parse it.
        let body_start = resp.find("\r\n\r\n").unwrap() + 4;
        let mut body = String::new();
        let mut rest = &resp[body_start..];
        loop {
            let nl = rest.find("\r\n").unwrap();
            let size = usize::from_str_radix(rest[..nl].trim(), 16).unwrap();
            if size == 0 {
                break;
            }
            body.push_str(&rest[nl + 2..nl + 2 + size]);
            rest = &rest[nl + 2 + size + 2..];
        }
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["columns"], serde_json::json!(["id", "v"]));
        assert_eq!(parsed["rows"].as_array().unwrap().len(), 1000);
        // Errors keep the single-frame path too.
        let resp = http("SELECT * FROM missing");
        assert!(resp.contains("Content-Length:"), "{resp}");
        assert!(resp.contains("does not exist"), "{resp}");
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

        // top_hits: per-bucket top documents, relevance-ordered.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"size":0,"query":{"match":{"msg":"error"}},"aggs":{"levels":{"terms":{"field":"level"},"aggs":{"best":{"top_hits":{"size":1}}}}}}"#,
        );
        assert!(resp.contains("\"best\""), "{resp}");
        assert!(resp.contains("error connecting") || resp.contains("another error"), "{resp}");
        assert!(resp.contains("\"_id\""), "{resp}");

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

        // GET by id — string _id, then a missing one.
        let resp = http("GET", "/logs/_doc/a1", "");
        assert!(resp.contains("\"found\":true"), "{resp}");
        assert!(resp.contains("error connecting"), "{resp}");
        let resp = http("GET", "/logs/_doc/never-was", "");
        assert!(resp.contains("\"found\":false"), "{resp}");

        // Multi-key sort: level asc, bytes desc within level.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"match_all":{}},"sort":[{"level":{"order":"asc"}},{"bytes":{"order":"desc"}}]}"#,
        );
        let a1 = resp.find("\"_id\":\"a1\"").unwrap();
        let anon = resp.find("another error").unwrap();
        let a2 = resp.find("\"_id\":\"a2\"").unwrap();
        assert!(a1 < anon && anon < a2, "expected error(100), error(50), info: {resp}");

        // _source include/exclude lists.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"match":{"msg":"error"}},"_source":["level","by*"]}"#,
        );
        assert!(resp.contains("\"level\""), "{resp}");
        assert!(resp.contains("\"bytes\""), "{resp}");
        assert!(!resp.contains("connecting"), "msg must be filtered out: {resp}");
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"match":{"msg":"error"}},"_source":{"excludes":["msg"]}}"#,
        );
        assert!(!resp.contains("connecting"), "{resp}");
        assert!(resp.contains("\"bytes\""), "{resp}");

        // bool.should beside must: default = optional scoring (BOOSTED
        // pushdown). Both error docs match; the one also matching the
        // should ranks first.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"bool":{"must":[{"match":{"msg":"error"}}],"should":[{"match":{"msg":"worker"}}]}}}"#,
        );
        assert!(resp.contains("\"total\":{\"relation\":\"eq\",\"value\":2}"), "{resp}");
        let worker = resp.find("another error in worker").unwrap();
        let db = resp.find("error connecting to db").unwrap();
        assert!(worker < db, "should-boosted hit must rank first: {resp}");
        // minimum_should_match: 1 makes the should required.
        let resp = http(
            "POST",
            "/logs/_count",
            r#"{"query":{"bool":{"must":[{"match":{"msg":"error"}}],"should":[{"match":{"msg":"worker"}}],"minimum_should_match":1}}}"#,
        );
        assert!(resp.contains("\"count\":1"), "{resp}");

        // multi_match cross_fields (terms spread across fields still hit)
        // and per-hit explain.
        let resp = http(
            "POST",
            "/logs/_search",
            r#"{"query":{"multi_match":{"query":"error","fields":["msg","level"],"type":"cross_fields"}},"explain":true}"#,
        );
        assert!(resp.contains("\"total\":{\"relation\":\"eq\",\"value\":2}"), "{resp}");
        assert!(resp.contains("\"_explanation\""), "{resp}");
        // most_fields and best_fields route too.
        let resp = http(
            "POST",
            "/logs/_count",
            r#"{"query":{"multi_match":{"query":"error","fields":["msg","level"],"type":"most_fields"}}}"#,
        );
        assert!(resp.contains("\"count\":2"), "{resp}");
        let resp = http(
            "POST",
            "/logs/_count",
            r#"{"query":{"multi_match":{"query":"error","fields":["msg","level"]}}}"#,
        );
        assert!(resp.contains("\"count\":2"), "{resp}");

        // Auto-create on bulk: unknown index springs into existence with a
        // dynamic mapping from the first document.
        let bulk = concat!(
            "{\"index\":{\"_id\":\"n1\"}}\n",
            "{\"note\":\"fresh index works\",\"severity\":\"low\",\"count\":7,\"ratio\":0.5,\"ok\":true}\n",
        );
        let resp = http("POST", "/autoidx/_bulk", bulk);
        assert!(resp.contains("\"errors\":false"), "{resp}");
        let resp = http("GET", "/autoidx/_mapping", "");
        assert!(resp.contains("\"note\":{\"type\":\"text\"}"), "{resp}");
        assert!(resp.contains("\"count\":{\"type\":\"long\"}"), "{resp}");
        assert!(resp.contains("\"ratio\":{\"type\":\"double\"}"), "{resp}");
        assert!(resp.contains("\"ok\":{\"type\":\"boolean\"}"), "{resp}");
        let resp = http("GET", "/autoidx/_doc/n1", "");
        assert!(resp.contains("\"found\":true") && resp.contains("fresh index"), "{resp}");
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
        // Status tab carries the drivers/witnesses sections.
        assert!(resp.contains(r#"<table id="drivers">"#), "{resp}");
        assert!(resp.contains(r#"<table id="witnesses">"#), "{resp}");
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

        // /ui/hosts: per-node host stats + cluster aggregate (standalone =
        // one "local" node). Sampled from /proc, so values are live.
        let resp = get("/ui/hosts");
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let body = resp.split("\r\n\r\n").nth(1).unwrap_or_default();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 1);
        let n = &v["nodes"][0];
        assert_eq!(n["id"], "local");
        assert_eq!(n["reachable"], true);
        assert!(n["mem_total_bytes"].as_u64().unwrap() > 0);
        assert!(n["rss_bytes"].as_u64().unwrap() > 0);
        assert!(n["disk_total_bytes"].as_u64().unwrap() > 0);
        assert_eq!(v["cluster"]["nodes"], 1);
        assert_eq!(v["cluster"]["reachable"], 1);
    }

    #[test]
    fn ui_drivers_and_witnesses_over_rest() {
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
        let json_body = |resp: &str| -> serde_json::Value {
            serde_json::from_str(resp.split("\r\n\r\n").nth(1).unwrap_or_default()).unwrap()
        };

        // No connection yet: an empty list, not an error (the `drivers`
        // table may not even be created yet — nothing has landed on it).
        let v = json_body(&get("/ui/drivers"));
        assert_eq!(v["drivers"].as_array().unwrap().len(), 0);

        // A live binary connection shows up with its endpoint tag. A
        // round-trip guarantees the server has finished registering before
        // we read the table back (auth completing on the client side
        // doesn't guarantee the server thread's registration INSERT, on a
        // separate thread, has landed yet).
        let (bin_addr, _bh) = binary::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        let mut client = Client::connect(bin_addr).unwrap();
        client.execute("SELECT 1").unwrap();
        let v = json_body(&get("/ui/drivers"));
        let drivers = v["drivers"].as_array().unwrap();
        assert_eq!(drivers.len(), 1, "{v}");
        assert_eq!(drivers[0]["endpoint"], "binary");
        assert!(!drivers[0]["remote_addr"].as_str().unwrap().is_empty());

        // Witnesses: empty until one registers (via plain SQL, exactly like
        // a real witness process would — see witnesses.rs).
        let v = json_body(&get("/ui/witnesses"));
        assert_eq!(v["witnesses"].as_array().unwrap().len(), 0);
        assert!(v["grace_period_secs"].as_i64().unwrap() > 0);

        crate::witnesses::ensure_tables(&ctx).unwrap();
        crate::witnesses::upsert_for_test(&ctx, "witness-eu", "eu-central-1", 12_345);
        let v = json_body(&get("/ui/witnesses"));
        let witnesses = v["witnesses"].as_array().unwrap();
        assert_eq!(witnesses.len(), 1, "{v}");
        assert_eq!(witnesses[0]["witness_id"], "witness-eu");
        assert_eq!(witnesses[0]["region"], "eu-central-1");
        // No heartbeat yet: sync summary is explicitly empty, not missing.
        assert_eq!(witnesses[0]["synced_tables"], 0, "{v}");

        // A heartbeat with watermarks surfaces the per-table sync detail.
        crate::shared::execute_as(
            &ctx,
            "superuser",
            "UPDATE witnesses SET watermarks = {'mirror.items': {rows: 42, synced_at: 12345}}              WHERE witness_id = 'witness-eu'",
        );
        let v = json_body(&get("/ui/witnesses"));
        let w = &v["witnesses"][0];
        assert_eq!(w["synced_tables"], 1, "{v}");
        assert_eq!(w["synced_rows"], 42, "{v}");
        assert_eq!(w["tables"][0]["table"], "mirror.items", "{v}");

        // REST activity: the /ui/* calls this test already made are counted
        // with a usable average.
        let v = json_body(&get("/ui/drivers"));
        let rest = v["rest"].as_array().unwrap();
        let ui = rest.iter().find(|r| r["path"] == "ui").expect("ui class present");
        assert!(ui["requests"].as_u64().unwrap() >= 3, "{v}");
        assert!(ui["avg_ms"].as_f64().unwrap() >= 0.0, "{v}");
    }

    /// An unwritable config file must not block a live-mutable key from
    /// applying (packaged installs may run with a read-only /etc): the set
    /// succeeds with `persisted: false` + a warning. A restart-required key
    /// in the same situation took no effect at all and errors.
    #[test]
    fn config_set_applies_live_when_persist_fails() {
        let ctx = Arc::new(Context {
            backend: Backend::Local(Box::new(RwLock::new(Database::open(temp_dir()).unwrap()))),
            metrics: Metrics::new(),
            audit: RwLock::new(quiet_audit()),
            authn: AuthState::disabled(),
            superuser_role: "superuser".into(),
            admin_lock: Mutex::new(()),
            start: Instant::now(),
            slow_log: crate::slowlog::SlowLog::new(),
            config: RwLock::new(Config::default()),
            // A directory is never writable as a file.
            config_path: Some(std::env::temp_dir().to_string_lossy().into_owned()),
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
        });

        let (status, body) = ctx.config_set("ui.enabled", "false");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["applied"], serde_json::json!(true));
        assert_eq!(body["persisted"], serde_json::json!(false));
        assert!(body["warning"].as_str().unwrap().contains("not persisted"));
        assert!(!ctx.config.read().unwrap().ui.enabled, "must be live");

        // Restart-required key + failed persist = nothing happened → error.
        let (status, body) = ctx.config_set("server.rest_port", "9999");
        assert_eq!(status, 500, "{body}");
    }

    /// `GET /ui/schema` returns only what the role may see: the superuser
    /// sees everything; a role with one table grant sees that table (and
    /// its database) and nothing else.
    #[test]
    fn ui_schema_filters_by_role() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        let post = |sql: &str| {
            let mut stream = TcpStream::connect(addr).unwrap();
            let head = format!(
                "POST /query HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                sql.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(sql.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };
        post("CREATE TABLE open_t (PRIMARY KEY (id))");
        post("CREATE TABLE secret_t (PRIMARY KEY (id))");
        post("CREATE DATABASE other");
        post(r#"{"sql": "CREATE TABLE hidden_t (PRIMARY KEY (id))", "db": "other"}"#);
        post("CREATE ROLE viewer");
        post("GRANT SELECT ON open_t TO viewer");

        // Superuser (auth disabled): everything.
        let get = |path: &str| {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nConnection: close\r\n\r\n").as_bytes())
                .unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };
        let resp = get("/ui/schema");
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        for name in ["open_t", "secret_t", "hidden_t", "\"name\":\"other\""] {
            assert!(resp.contains(name), "missing {name}: {resp}");
        }

        // A table-granted role: its table and database only.
        let (status, body) = crate::ui::schema_json(&ctx, "viewer");
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("open_t"), "{body}");
        assert!(!body.contains("secret_t"), "{body}");
        assert!(!body.contains("hidden_t"), "{body}");
        assert!(!body.contains("\"name\":\"other\""), "{body}");
    }

    /// The admin control plane's SQL spellings: SHOW CLUSTER / CONFIG /
    /// SLOW QUERIES, SET CONFIG, REPAIR CLUSTER, RECLAIM — same handler,
    /// RBAC, and results as the HTTP endpoints, spoken over /query.
    #[test]
    fn sql_admin_statements() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        let post = |sql: &str| -> String {
            let mut stream = TcpStream::connect(addr).unwrap();
            let head = format!(
                "POST /query HTTP/1.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                sql.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(sql.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            resp
        };

        // SHOW CONFIG with a LIKE filter — masked, flattened keys.
        let resp = post("SHOW CONFIG LIKE 'ui.%'");
        assert!(resp.contains("\"ui.enabled\""), "{resp}");
        assert!(!resp.contains("superuser_password"), "filtered out: {resp}");

        // SET CONFIG applies live and SHOW CONFIG reflects it.
        let resp = post("SET CONFIG ui.enabled = 'false'");
        assert!(resp.contains("applied"), "{resp}");
        let resp = post("SHOW CONFIG LIKE 'ui.enabled'");
        assert!(resp.contains("false"), "{resp}");
        post("SET CONFIG ui.enabled = 'true'");

        // SHOW CLUSTER answers (standalone: clustered=false).
        let resp = post("SHOW CLUSTER");
        assert!(resp.contains("clustered"), "{resp}");

        // SHOW SLOW QUERIES answers with the right columns.
        let resp = post("SHOW SLOW QUERIES LIMIT 5");
        assert!(resp.contains("\"columns\":[\"seq\",\"elapsed_ms\",\"sql\"]"), "{resp}");

        // RBAC: a non-admin role is denied with the standard message.
        post("CREATE ROLE viewer2");
        post("CREATE TABLE vt (PRIMARY KEY (id))");
        post("GRANT SELECT ON vt TO viewer2");
        let resp = {
            let ctx = ctx.clone();
            let r = crate::shared::execute_as(&ctx, "viewer2", "SHOW CONFIG");
            format!("{r:?}")
        };
        assert!(resp.contains("permission denied"), "{resp}");

        // SET CONSISTENCY on the stateless REST gateway explains itself.
        let resp = post("SET CONSISTENCY QUORUM");
        assert!(resp.contains("session"), "{resp}");
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

    /// One self-scrape tick lands the node's own gauges in the `metrics`
    /// TS table, queryable over SQL like any remote_write data.
    #[test]
    fn self_scrape_ingests_own_metrics() {
        let ctx = temp_ctx();
        // The startup path sets these in production; drive them here.
        ctx.metrics.set("skaidb_up", 1);
        ctx.metrics.connection_opened(crate::metrics::Endpoint::Rest);
        let n = crate::promwrite::self_scrape_tick(&ctx).unwrap();
        assert!(n > 0, "expected some series ingested");
        let resp = crate::shared::execute_as(
            &ctx,
            "superuser",
            "SELECT value FROM metrics WHERE name = 'skaidb_up' ORDER BY ts",
        );
        match resp {
            Response::Rows { rows, .. } => {
                assert!(!rows.is_empty(), "skaidb_up not ingested");
                assert_eq!(rows[0], vec![Value::Float(1.0)]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
        // Labelled series keep their labels as queryable columns.
        let resp = crate::shared::execute_as(
            &ctx,
            "superuser",
            "SELECT value FROM metrics WHERE name = 'skaidb_connections_active' AND endpoint = 'rest'",
        );
        match resp {
            Response::Rows { rows, .. } => {
                assert!(!rows.is_empty(), "labeled series not ingested");
                assert_eq!(rows[0], vec![Value::Float(1.0)]);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn node_stats_publish_and_read_back() {
        let ctx = temp_ctx();
        crate::nodestats::ensure_table(&ctx).unwrap();
        crate::nodestats::publish_tick(&ctx).unwrap();
        // Read back through the dashboard path: one fresh row for this node,
        // with real host numbers and ~zero age.
        let rows = crate::nodestats::read_all(&ctx);
        assert_eq!(rows.len(), 1, "one row per node");
        let (node, stats, age) = &rows[0];
        assert!(!node.is_empty());
        assert!(stats.mem_total_bytes > 0, "host stats round-trip");
        assert!(*age <= 2, "fresh row, age {age}s");
        // Re-publishing overwrites the same row (PK = node), never grows.
        crate::nodestats::publish_tick(&ctx).unwrap();
        assert_eq!(crate::nodestats::read_all(&ctx).len(), 1);
        // The stats view is plain SQL — exactly what the dashboard reads.
        let resp = crate::shared::execute_as(
            &ctx,
            "superuser",
            "SELECT node, restarts FROM node_stats",
        );
        assert!(matches!(resp, Response::Rows { rows, .. } if rows.len() == 1));
    }

    #[test]
    fn drivers_registers_binary_connection_and_deregisters_on_disconnect() {
        use skaidb_driver::Client;

        let ctx = temp_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx.clone()).unwrap();

        let mut client = Client::connect(addr).unwrap();
        // A round-trip guarantees the server has finished authenticating and
        // registering before we read the table back.
        client.execute("SELECT 1").unwrap();
        let resp = crate::shared::execute_as(&ctx, "superuser", "SELECT * FROM drivers");
        let Response::Rows { columns, rows } = resp else {
            panic!("expected rows, got a non-row response reading drivers");
        };
        assert_eq!(rows.len(), 1, "one row for the live connection");
        let idx = |name: &str| columns.iter().position(|c| c == name).unwrap();
        assert_eq!(rows[0][idx("endpoint")], Value::String("binary".into()));
        assert!(!matches!(rows[0][idx("remote_addr")], Value::Null));

        drop(client);
        // Disconnection is detected asynchronously by the server thread;
        // poll briefly rather than asserting immediately.
        let mut remaining = 50;
        loop {
            let resp = crate::shared::execute_as(&ctx, "superuser", "SELECT * FROM drivers");
            let Response::Rows { rows, .. } = resp else {
                panic!("expected rows reading drivers");
            };
            if rows.is_empty() || remaining == 0 {
                assert!(rows.is_empty(), "row should be gone after disconnect");
                break;
            }
            remaining -= 1;
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
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

    /// `POST /insert` writes whole JSON documents — including nested
    /// objects/arrays that SQL `INSERT` cannot express — into a chosen
    /// database, and overwrites on the primary key (upsert).
    #[test]
    fn rest_json_insert_end_to_end() {
        let ctx = temp_ctx();
        let (addr, _h) = rest::spawn("127.0.0.1:0", ctx).unwrap();

        let post = |path: &str, body: &str| -> (u16, String) {
            let mut stream = TcpStream::connect(addr).unwrap();
            let req = format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(req.as_bytes()).unwrap();
            let mut resp = String::new();
            stream.read_to_string(&mut resp).unwrap();
            let status = resp.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            let b = resp.split_once("\r\n\r\n").map(|(_, x)| x.to_string()).unwrap_or_default();
            (status, b)
        };

        assert!(http_post(addr, "CREATE DATABASE app").contains("\"ok\":true"));
        assert!(
            http_post(addr, "{\"sql\":\"CREATE TABLE cache (PRIMARY KEY (k))\",\"db\":\"app\"}")
                .contains("\"ok\":true")
        );

        // Insert two docs with a nested `data` object + array, into db `app`.
        let (st, body) = post(
            "/insert",
            r#"{"db":"app","table":"cache","rows":[
                 {"k":"a","data":{"page":2,"products":[{"id":1},{"id":2}]}},
                 {"k":"b","data":{"page":0,"products":[]}}
               ]}"#,
        );
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"inserted\":2"), "{body}");

        // Read back: the nested doc round-trips, and a dotted path into it
        // is queryable (proving it's a real document, not an opaque blob).
        let got = http_post(addr, "{\"sql\":\"SELECT data.page FROM cache WHERE k = 'a'\",\"db\":\"app\"}");
        assert!(got.contains('2'), "nested path not queryable: {got}");

        // Same PK overwrites (upsert).
        let (st, _) = post("/insert", r#"{"db":"app","table":"cache","rows":[{"k":"a","data":{"page":9}}]}"#);
        assert_eq!(st, 200);
        let got = http_post(addr, "{\"sql\":\"SELECT data.page FROM cache WHERE k = 'a'\",\"db\":\"app\"}");
        assert!(got.contains('9'), "upsert did not overwrite: {got}");

        // Empty rows is a no-op; a missing table is a 400.
        assert_eq!(post("/insert", r#"{"db":"app","table":"cache","rows":[]}"#).0, 200);
        assert_eq!(post("/insert", r#"{"db":"app","rows":[{"k":"z"}]}"#).0, 400);
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
        // The INSERT affected 1 row — recorded as write volume (not just a
        // statement count), so bulk imports are visible on the dashboard.
        assert!(
            metrics.contains("skaidb_rows_written_total 1"),
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
        // Host system stats are exported per node.
        assert!(m.contains("skaidb_host_mem_total_bytes"));
        assert!(m.contains("skaidb_host_cpu_percent"));
        assert!(m.contains("skaidb_host_disk_available_bytes"));
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
        assert!(
            body.contains("\"columns\":[\"table\",\"primary_key\",\"replication\",\"nodes\",\"witness\"]"),
            "got: {body}"
        );
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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
    fn read_only_rejects_mutations_but_serves_reads() {
        use skaidb_driver::Client;
        // ada is the superuser (exempt); bob holds real Insert/Update/Delete
        // grants, so a rejection under read_only is the read-only gate, not
        // an RBAC denial wearing its clothes.
        let ctx = auth_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        let mut su = Client::connect_with(addr, "ada", "pencil").unwrap();
        su.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        su.execute("INSERT INTO t (id, x) VALUES (1, 'a')").unwrap();
        su.execute("CREATE USER bob PASSWORD 'hunter2'").unwrap();
        for grant in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            su.execute(&format!("GRANT {grant} ON t TO bob")).unwrap();
        }
        let mut bob = Client::connect_with(addr, "bob", "hunter2").unwrap();
        assert!(matches!(
            bob.execute("INSERT INTO t (id, x) VALUES (2, 'b')").unwrap(),
            Response::Mutation { .. }
        ));

        // Flip read-only live over the wire (SET CONFIG is Admin-gated and
        // live-mutable; no restart, no config file needed).
        su.execute("SET CONFIG server.read_only = 'true'").unwrap();

        // bob: reads fine, every mutation shape rejected with the read-only
        // error (not "permission denied" — his grants are intact).
        assert!(matches!(
            bob.execute("SELECT id FROM t").unwrap(),
            Response::Rows { .. }
        ));
        assert!(matches!(bob.execute("SHOW TABLES").unwrap(), Response::Rows { .. }));
        for sql in [
            "INSERT INTO t (id, x) VALUES (3, 'c')",
            "UPDATE t SET x = 'z' WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
        ] {
            let err = bob.execute(sql).unwrap_err().to_string();
            assert!(err.contains("read-only node"), "{sql}: {err}");
        }

        // The superuser stays exempt — the witness's own applier (and the
        // node's internal telemetry) must keep writing.
        assert!(matches!(
            su.execute("INSERT INTO t (id, x) VALUES (4, 'd')").unwrap(),
            Response::Mutation { .. }
        ));
        // Internal telemetry proof: this connection's own drivers-registry
        // row landed while the node was read-only.
        assert!(matches!(
            su.execute("SELECT conn_id FROM drivers").unwrap(),
            Response::Rows { rows, .. } if !rows.is_empty()
        ));

        // Flip back off: bob writes again.
        su.execute("SET CONFIG server.read_only = 'false'").unwrap();
        assert!(matches!(
            bob.execute("INSERT INTO t (id, x) VALUES (5, 'e')").unwrap(),
            Response::Mutation { .. }
        ));
    }

    #[test]
    fn read_only_gates_remote_write_ingestion() {
        // remote_write bypasses the SQL statement path, so it carries its
        // own read-only check — prove it fires for a granted non-superuser
        // role and not for the superuser.
        let ctx = temp_ctx();
        crate::shared::execute_as(&ctx, "superuser", "CREATE ROLE carol");
        crate::shared::execute_as(&ctx, "superuser", "GRANT INSERT ON metrics TO carol");
        let (status, _) = ctx.config_set("server.read_only", "true");
        assert_eq!(status, 200);
        // The gate fires before body decode, so an empty body suffices.
        let err = crate::promwrite::ingest(&ctx, "carol", &[]).unwrap_err();
        assert!(err.contains("read-only node"), "{err}");
        // Superuser-exempt, so the request proceeds to (and fails at)
        // snappy decode instead — proving the gate didn't fire.
        let err = crate::promwrite::ingest(&ctx, "superuser", &[]).unwrap_err();
        assert!(err.contains("snappy"), "{err}");
    }

    /// Phase-0 naming end to end: rename statements over SQL, Admin
    /// gating, and the witness one-way rule (a witness-mode node refuses
    /// renames outright, even with admin credentials).
    #[test]
    fn naming_renames_over_sql_and_witness_refuses() {
        use skaidb_driver::Client;
        let ctx = auth_ctx();
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        crate::naming::bootstrap(&ctx, "local").unwrap();
        let mut su = Client::connect_with(addr, "ada", "pencil").unwrap();
        su.execute("ALTER CLUSTER SET NAME 'ember-lynx'").unwrap();
        su.execute("ALTER NODE 'local' SET NAME 'skai1'").unwrap();
        assert_eq!(crate::naming::cluster_name(&ctx).as_deref(), Some("ember-lynx"));
        assert_eq!(crate::naming::node_alias(&ctx, "local").as_deref(), Some("skai1"));
        // Dotted resolution works through the SQL-set names.
        su.execute("ALTER NODE 'ember-lynx.node.skai1' SET NAME 'primary-1'")
            .unwrap();
        assert_eq!(
            crate::naming::node_alias(&ctx, "local").as_deref(),
            Some("primary-1")
        );
        // Non-admin denied (Admin on Global).
        su.execute("CREATE USER bob PASSWORD 'hunter2'").unwrap();
        let mut bob = Client::connect_with(addr, "bob", "hunter2").unwrap();
        let err = bob
            .execute("ALTER CLUSTER SET NAME 'nope'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("permission denied"), "{err}");

        // Witness-mode node: refused even for the superuser.
        let wctx = temp_ctx();
        {
            let mut c = wctx.config.write().unwrap();
            c.witness.enabled = true;
        }
        let (waddr, _wh) = binary::spawn("127.0.0.1:0", wctx).unwrap();
        let mut w = Client::connect(waddr).unwrap();
        let err = w
            .execute("ALTER CLUSTER SET NAME 'forked'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("witness"), "{err}");
        let err = w
            .execute("ALTER NODE 'x' SET NAME 'y'")
            .unwrap_err()
            .to_string();
        assert!(err.contains("witness"), "{err}");
    }

    #[test]
    fn pin_references_resolve_aliases_and_validate_membership() {
        use skaidb_cluster::{Consistency as ClusterConsistency, Node, NodeConfig, NodeId};
        use skaidb_driver::Client;
        // Single-member cluster backend: the only real member id is its
        // internode address.
        let internode_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let a = l.local_addr().unwrap().to_string();
            drop(l);
            a
        };
        let cfg = NodeConfig {
            id: NodeId::new(&internode_addr),
            internode_addr: internode_addr.clone(),
            members: vec![(NodeId::new(&internode_addr), internode_addr.clone())],
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: ClusterConsistency::Quorum,
            write_consistency: ClusterConsistency::Quorum,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let node = Node::new(Database::open(temp_dir()).unwrap(), cfg);
        node.serve_internode().unwrap();
        let ctx: Shared = Arc::new(Context {
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
        });
        let (addr, _h) = binary::spawn("127.0.0.1:0", ctx.clone()).unwrap();
        crate::naming::bootstrap(&ctx, &internode_addr).unwrap();
        let mut c = Client::connect(addr).unwrap();
        c.execute("ALTER NODE '{ID}' SET NAME 'pin-me'".replace("{ID}", &internode_addr).as_str())
            .unwrap();

        // Pin by ALIAS: stored as the stable id, visible in SHOW TABLES.
        c.execute("CREATE TABLE t1 (PRIMARY KEY (id)) WITH (nodes = ['pin-me'])")
            .unwrap();
        let rows = c.execute("SHOW TABLES").unwrap();
        let shown = format!("{rows:?}");
        assert!(shown.contains(&internode_addr), "pin stored as id: {shown}");
        assert!(!shown.contains("pin-me"), "alias is sugar, not storage: {shown}");

        // Unknown reference refused, error lists the membership.
        let err = c
            .execute("CREATE TABLE t2 (PRIMARY KEY (id)) WITH (nodes = ['ghost'])")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a current member"), "{err}");
        assert!(err.contains(&internode_addr), "{err}");

        // Standalone backend: pins are meaningless, refused outright.
        let sctx = auth_ctx();
        let (saddr, _sh) = binary::spawn("127.0.0.1:0", sctx).unwrap();
        let mut sc = Client::connect_with(saddr, "ada", "pencil").unwrap();
        let err = sc
            .execute("CREATE TABLE t3 (PRIMARY KEY (id)) WITH (nodes = ['x'])")
            .unwrap_err()
            .to_string();
        assert!(err.contains("standalone"), "{err}");
    }

    fn rbac_test_ctx() -> Shared {
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// An app role that can CREATE its own indexes can also DROP/replace them
    /// (index DDL scoped to the owning table); other roles stay denied.
    #[test]
    fn index_ddl_scoped_to_owning_table() {
        use crate::shared::{execute, execute_as};
        let ctx = rbac_test_ctx();
        assert_eq!(
            execute(&ctx, "CREATE TABLE t (PRIMARY KEY (id))"),
            Response::Ddl
        );
        assert_eq!(execute(&ctx, "CREATE ROLE app"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "GRANT CREATE ON DATABASE default TO app"),
            Response::Ddl
        );
        // The app can create its own index...
        assert_eq!(
            execute_as(&ctx, "app", "CREATE INDEX i_probe ON t (name)"),
            Response::Ddl
        );
        // ...and — the C-5 fix — drop it again (previously `permission
        // denied: Drop on Global`, making index DDL append-only for apps).
        assert_eq!(execute_as(&ctx, "app", "DROP INDEX i_probe"), Response::Ddl);
        // Idempotent bootstrap: IF EXISTS on a nonexistent index stays a
        // permissionless no-op instead of a spurious denial.
        assert_eq!(
            execute_as(&ctx, "app", "DROP INDEX IF EXISTS i_probe"),
            Response::Ddl
        );
        // A role without Create on the owning table is still denied.
        assert_eq!(execute(&ctx, "CREATE ROLE reader"), Response::Ddl);
        assert_eq!(execute(&ctx, "GRANT SELECT ON t TO reader"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "CREATE INDEX i_admin ON t (name)"),
            Response::Ddl
        );
        match execute_as(&ctx, "reader", "DROP INDEX i_admin") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
    }

    /// `GRANT MONITOR ON *` opens read-only control-plane introspection
    /// (SHOW CONFIG / SHOW CLUSTER / SHOW SLOW QUERIES) without Admin; the
    /// mutating control plane stays Admin-only.
    #[test]
    fn monitor_grants_read_only_introspection() {
        use crate::shared::{execute, execute_as};
        let ctx = rbac_test_ctx();
        assert_eq!(execute(&ctx, "CREATE ROLE watcher"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "GRANT MONITOR ON * TO watcher"),
            Response::Ddl
        );
        // Read-only introspection is allowed (never a permission error)...
        for sql in ["SHOW CONFIG", "SHOW CLUSTER", "SHOW SLOW QUERIES"] {
            if let Response::Error(m) = execute_as(&ctx, "watcher", sql) {
                assert!(!m.contains("permission denied"), "{sql}: {m}");
            }
        }
        // ...but mutation still requires Admin.
        match execute_as(
            &ctx,
            "watcher",
            "SET CONFIG storage.scan_row_budget = 1000",
        ) {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
        // A role without the grant stays locked out of introspection.
        assert_eq!(execute(&ctx, "CREATE ROLE plain"), Response::Ddl);
        match execute_as(&ctx, "plain", "SHOW CONFIG") {
            Response::Error(m) => assert!(m.contains("permission denied"), "got: {m}"),
            other => panic!("expected denial, got {other:?}"),
        }
    }

    #[test]
    fn rbac_enforced_per_statement() {
        use crate::shared::{execute, execute_as};
        let ctx: Shared = rbac_test_ctx();

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

        // A database grant must NOT authorize another database's tables via a
        // `db.table` qualifier. `scoped` holds SELECT on database `walled`
        // only; while its session db is `walled`, a `default.t` reference is
        // a different database and must be denied for both read and write —
        // the check widens to the qualifier's database, not the session's.
        use crate::shared::execute_session_as;
        assert_eq!(execute(&ctx, "CREATE DATABASE walled"), Response::Ddl);
        assert_eq!(execute(&ctx, "CREATE ROLE scoped"), Response::Ddl);
        assert_eq!(
            execute(&ctx, "GRANT SELECT ON DATABASE walled TO scoped"),
            Response::Ddl
        );
        let cross = |sql: &str| {
            let mut db = "walled".to_string();
            execute_session_as(&ctx, "scoped", &mut db, sql, None)
        };
        match cross("SELECT id FROM default.t") {
            Response::Error(m) => assert!(m.contains("permission denied"), "read escalation: {m}"),
            other => panic!("cross-db read must be denied, got {other:?}"),
        }
        match cross("INSERT INTO default.t (id) VALUES (7)") {
            Response::Error(m) => assert!(m.contains("permission denied"), "write escalation: {m}"),
            other => panic!("cross-db write must be denied, got {other:?}"),
        }
        // The database grant still reaches its OWN database's tables: a bare
        // reference in session db `walled` passes the privilege check (it
        // fails later only because the table doesn't exist — not on RBAC).
        match cross("SELECT id FROM own_table") {
            Response::Error(m) => assert!(
                !m.contains("permission denied"),
                "own-db select must pass RBAC (table-not-found is fine): {m}"
            ),
            Response::Rows { .. } => {}
            other => panic!("unexpected: {other:?}"),
        }

        // A `Create` grant on a database authorizes creating tables IN that
        // database (standard SQL), but not in another database or globally.
        assert_eq!(
            execute(&ctx, "GRANT CREATE ON DATABASE walled TO scoped"),
            Response::Ddl
        );
        match cross("CREATE TABLE mine (PRIMARY KEY (id))") {
            Response::Ddl => {}
            other => panic!("own-db CREATE TABLE should be allowed, got {other:?}"),
        }
        // ...but not into a different database via a qualifier.
        match cross("CREATE TABLE default.sneaky (PRIMARY KEY (id))") {
            Response::Error(m) => {
                assert!(m.contains("permission denied"), "cross-db create: {m}")
            }
            other => panic!("cross-db CREATE TABLE must be denied, got {other:?}"),
        }
        // A role with no create grant anywhere still cannot create tables.
        match execute_as(&ctx, "reader", "CREATE TABLE reader.t (PRIMARY KEY (id))") {
            Response::Error(m) => {
                assert!(m.contains("permission denied"), "ungranted create: {m}")
            }
            other => panic!("ungranted CREATE TABLE must be denied, got {other:?}"),
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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
    /// Witness pull, end to end in-process: a real single-member primary
    /// (internode served — the pull's data path) with a binary SQL endpoint
    /// (the pull's control path), mirrored into a standalone witness
    /// context. Covers: schema sync, byte-exact data with nested docs,
    /// registration + heartbeat + watermarks on the primary, UPDATE and
    /// tombstone (DELETE) propagation on re-pull, and idempotency (third
    /// cycle applies nothing).
    #[test]
    fn witness_pull_mirrors_a_primary_end_to_end() {
        // Primary: cluster backend with a LIVE internode listener.
        let internode_addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let a = l.local_addr().unwrap().to_string();
            drop(l);
            a
        };
        let cfg = NodeConfig {
            id: NodeId::new(&internode_addr),
            internode_addr: internode_addr.clone(),
            members: vec![(NodeId::new(&internode_addr), internode_addr.clone())],
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: ClusterConsistency::Quorum,
            write_consistency: ClusterConsistency::Quorum,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let node = Node::new(Database::open(temp_dir()).unwrap(), cfg);
        node.serve_internode().unwrap();
        let primary: Shared = Arc::new(Context {
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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
        });
        let (sql_addr, _h) = binary::spawn("127.0.0.1:0", primary.clone()).unwrap();
        crate::witnesses::ensure_tables(&primary).unwrap();
        let exec_p = |sql: &str| match crate::shared::execute_as(&primary, "superuser", sql) {
            Response::Error(e) => panic!("primary {sql}: {e}"),
            other => other,
        };
        exec_p("CREATE DATABASE mirror");
        exec_p("CREATE TABLE mirror.items (PRIMARY KEY (id))");
        exec_p("INSERT INTO mirror.items (id, x, meta) VALUES (1, 'a', {n: 1}), (2, 'b', {n: 2}), (3, 'c', {n: 3})");

        // Witness: standalone, read-only (the intended deployment shape).
        let witness = temp_ctx();
        let (status, _) = witness.config_set("server.read_only", "true");
        assert_eq!(status, 200);
        let wcfg = skaidb_config::WitnessConfig {
            enabled: true,
            primary_sql_addrs: vec![sql_addr.to_string()],
            primary_internode_addrs: vec![internode_addr.clone()],
            user: "anonymous".into(),
            password: String::new(),
            databases: vec!["mirror".into()],
            interval_secs: 3600,
            full_sweep_interval_secs: 86_400,
            duty_pct: 90, // tests want speed, not politeness
            witness_id: "w-test".into(),
            region: "unit".into(),
        };

        // Cycle 1: full copy arrives, registration lands on the primary.
        let s1 = crate::witness_pull::run_cycle(&witness, &wcfg).unwrap();
        assert_eq!(s1.tables, vec![("mirror.items".to_string(), 3, 3, 3)]);
        let exec_w = |sql: &str| match crate::shared::execute_as(&witness, "superuser", sql) {
            Response::Error(e) => panic!("witness {sql}: {e}"),
            other => other,
        };
        let rows_of = |resp: Response| match resp {
            Response::Rows { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        };
        let got = rows_of(exec_w("SELECT id, x, meta FROM mirror.items ORDER BY id"));
        assert_eq!(got.len(), 3);
        assert_eq!(got[0][1], Value::String("a".into()));
        // Nested docs survive byte-exact (the internode path carries raw
        // encoded values, not a SQL/JSON re-serialization).
        match &got[1][2] {
            Value::Document(d) => assert_eq!(d.0.get("n"), Some(&Value::Int(2))),
            other => panic!("expected nested doc, got {other:?}"),
        }
        let reg = rows_of(exec_p(
            "SELECT witness_id, region, last_seen_at FROM witnesses",
        ));
        assert_eq!(reg.len(), 1);
        assert_eq!(reg[0][0], Value::String("w-test".into()));
        assert_eq!(reg[0][1], Value::String("unit".into()));
        assert!(matches!(reg[0][2], Value::Int(n) if n > 0));

        // A cycle-start registration must NOT clobber the previous cycle's
        // watermarks (the INSERT-overwrites-the-row wart): re-register and
        // confirm the sync detail survives.
        let mut reg_sql = Client::connect(sql_addr).unwrap();
        crate::witness_pull::register(&mut reg_sql, &wcfg).unwrap();
        let wm = rows_of(exec_p("SELECT watermarks FROM witnesses"));
        assert!(
            matches!(&wm[0][0], Value::Document(d) if !d.0.is_empty()),
            "watermarks wiped by re-registration: {:?}",
            wm[0][0]
        );

        // Mutate the primary: update, delete, insert — then cycle 2.
        exec_p("UPDATE mirror.items SET x = 'A2' WHERE id = 1");
        exec_p("DELETE FROM mirror.items WHERE id = 2");
        exec_p("INSERT INTO mirror.items (id, x) VALUES (4, 'd')");
        let s2 = crate::witness_pull::run_cycle(&witness, &wcfg).unwrap();
        // Incremental: only the 3-row delta crosses the wire, not the table.
        let (_n, pulled2, applied2, rows_now2) = &s2.tables[0];
        assert!(*pulled2 <= 4, "delta pull, not a sweep: pulled {pulled2}");
        assert_eq!(*applied2, 3, "update + tombstone + insert applied");
        assert_eq!(*rows_now2, 3, "heartbeat reports the LOCAL table size");
        let got = rows_of(exec_w("SELECT id, x FROM mirror.items ORDER BY id"));
        assert_eq!(
            got.iter().map(|r| r[0].clone()).collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(3), Value::Int(4)],
            "the DELETE must propagate as a tombstone"
        );
        assert_eq!(got[0][1], Value::String("A2".into()), "the UPDATE must propagate");

        // Witness targeting: a `witness = false` table never reaches the
        // mirror, and flipping it back on picks it up next cycle.
        exec_p("CREATE TABLE mirror.private (PRIMARY KEY (id)) WITH (witness = false)");
        exec_p("INSERT INTO mirror.private (id) VALUES (1)");
        let s2b = crate::witness_pull::run_cycle(&witness, &wcfg).unwrap();
        assert!(
            !s2b.tables.iter().any(|(n, ..)| n == "mirror.private"),
            "excluded table must not appear in the cycle: {:?}",
            s2b.tables
        );
        match crate::shared::execute_as(&witness, "superuser", "SELECT id FROM mirror.private") {
            Response::Error(e) => assert!(e.contains("does not exist"), "{e}"),
            other => panic!("excluded table must not exist on the witness: {other:?}"),
        }
        exec_p("ALTER TABLE mirror.private SET (witness = true)");
        let s2c = crate::witness_pull::run_cycle(&witness, &wcfg).unwrap();
        assert!(
            s2c.tables.iter().any(|(n, _, _, rows)| n == "mirror.private" && *rows == 1),
            "re-included table mirrors on the next cycle: {:?}",
            s2c.tables
        );
        // System tables refuse placement/witness options.
        if let Response::Error(e) = crate::shared::execute_as(&primary, "superuser",
            "CREATE TABLE drivers2x (PRIMARY KEY (id)) WITH (witness = false)") {
            panic!("non-system table must accept the option: {e}");
        }
        match crate::shared::execute_as(&primary, "superuser",
            "ALTER TABLE witnesses SET (witness = false)") {
            Response::Error(e) => assert!(e.contains("system table"), "{e}"),
            other => panic!("system table must refuse: {other:?}"),
        }

        // Cycle 3: nothing changed — the write_seq hint skips the table
        // without moving a byte (the near-live steady state).
        let s3 = crate::witness_pull::run_cycle(&witness, &wcfg).unwrap();
        let (_name, pulled, applied, rows_now) = &s3.tables[0];
        assert_eq!(*pulled, 0, "unchanged table skipped entirely");
        assert_eq!(*applied, 0);
        assert_eq!(*rows_now, 3, "skipped table still reports its size");
    }

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
            drivers_table_ensured: std::sync::atomic::AtomicBool::new(false),
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
