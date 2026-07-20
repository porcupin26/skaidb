//! skaidbsh — the skaidb interactive shell and admin client.
//!
//! Network-first: SQL runs over the binary fast-path driver (with nearest-node
//! selection and failover across cluster members), while cluster/config/status
//! operations use the REST control plane. An embedded engine is still available
//! offline via `--local <dir>`.

mod certs;
mod cluster;
mod dump;
mod http;
mod render;

use std::io::{self, BufRead, IsTerminal};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use skaidb_driver::{Client, TlsConfig, TlsVerify};
use skaidb_engine::{QueryOutput, Session};
use skaidb_proto::{Consistency, Response};
use skaidb_types::Value;

/// Prompt shown while a statement is still being typed (no terminating `;`).
const CONT_PROMPT: &str = "   ...> ";

#[derive(Debug, Parser)]
#[command(name = "skaidbsh", version, about = "skaidb interactive shell and admin client")]
struct Cli {
    /// Server host(s). Repeat or comma-separate for cluster failover, e.g.
    /// `-H node1,node2:7000`. An entry may include its own SQL port.
    #[arg(short = 'H', long = "host", default_value = "127.0.0.1", value_delimiter = ',')]
    host: Vec<String>,

    /// SQL (binary protocol) port, used when a host omits its own port.
    #[arg(short = 'P', long = "port", default_value_t = 7000)]
    port: u16,

    /// REST/admin port for status, metrics, and config.
    #[arg(long = "rest-port", default_value_t = 7080)]
    rest_port: u16,

    /// Username (SCRAM for SQL, HTTP Basic for admin).
    #[arg(short = 'u', long, env = "SKAIDB_USER")]
    user: Option<String>,

    /// Password.
    #[arg(short = 'p', long, env = "SKAIDB_PASSWORD")]
    password: Option<String>,

    /// Authentication mechanism: `scram` (password, default) or `gssapi`
    /// (Kerberos — run `kinit` first; requires a `kerberos`-feature build and
    /// `--gssapi-spn`). With `gssapi`, `--user` is the client principal and no
    /// password is sent.
    #[arg(long = "auth-mechanism", default_value = "scram", env = "SKAIDB_AUTH_MECHANISM")]
    auth_mechanism: String,

    /// Target service principal for `--auth-mechanism gssapi`, e.g.
    /// `skaidb/host.example.com@REALM`.
    #[arg(long = "gssapi-spn", env = "SKAIDB_GSSAPI_SPN")]
    gssapi_spn: Option<String>,

    /// Default SQL consistency: one | quorum | all.
    #[arg(long, default_value = "quorum")]
    consistency: String,

    /// Connect over TLS (binary + REST). Implied by --tls-ca / --tls-insecure.
    #[arg(long)]
    tls: bool,

    /// CA certificate file to verify the server's cert (the cluster `ca.crt`).
    #[arg(long = "tls-ca")]
    tls_ca: Option<String>,

    /// Skip TLS certificate verification (self-signed/dev only; INSECURE).
    #[arg(long = "tls-insecure")]
    tls_insecure: bool,

    /// TLS server name (SNI / cert SAN to verify). Default `skaidb`.
    #[arg(long = "tls-server-name", default_value = "skaidb")]
    tls_server_name: String,

    /// Run against an embedded engine on a local data directory instead of
    /// connecting to a server (offline/dev). Admin commands are unavailable.
    #[arg(long)]
    local: Option<String>,

    /// Execute one or more `;`-separated statements, print results, and exit.
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,

    /// Execute statements read from a file, then exit.
    #[arg(short = 'f', long = "file")]
    file: Option<String>,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Quick health/topology (GET /status, no auth required).
    Status,
    /// Print Prometheus metrics (GET /metrics).
    Metrics,
    /// Cluster membership operations.
    Cluster {
        #[command(subcommand)]
        op: ClusterOp,
    },
    /// Inspect or change server configuration.
    Config {
        #[command(subcommand)]
        op: ConfigOp,
    },
    /// Dump schema + data to JSON or CSV (chosen tables, databases, or all).
    Export(dump::ExportArgs),
    /// Generate TLS material for internode mutual TLS (offline; no server).
    Certs {
        #[command(subcommand)]
        op: CertsOp,
    },
    /// Generate an at-rest encryption keyfile (offline; no server).
    Keyfile {
        #[command(subcommand)]
        op: KeyfileOp,
    },
}

#[derive(Debug, Subcommand)]
enum KeyfileOp {
    /// Write a fresh random 32-byte KEK to `out` (0600). Back it up off-box:
    /// losing it makes all encrypted data unrecoverable.
    Gen {
        /// Output path for the keyfile.
        #[arg(long, default_value = "./skaidb-at-rest.key")]
        out: String,
    },
}

#[derive(Debug, Subcommand)]
enum CertsOp {
    /// Mint a cluster CA + per-node leaf certs for `internode_auth = cert`.
    Gen {
        /// Output directory for ca.crt/ca.key and node*.crt/node*.key.
        #[arg(long, default_value = "./skaidb-certs")]
        out: String,
        /// Number of node certificates to mint (one per cluster member).
        #[arg(long, default_value_t = 3)]
        nodes: usize,
    },
}

#[derive(Debug, Subcommand)]
enum ClusterOp {
    /// Show cluster membership and topology.
    Status,
    /// Add a node and migrate it its share (`host:internode_port`).
    AddNode { addr: String },
    /// Gracefully decommission a node by id (`host:internode_port`).
    RemoveNode { id: String },
    /// Run a cluster-wide anti-entropy repair pass.
    Repair,
    /// Reclaim space former owners no longer own.
    Reclaim,
}

#[derive(Debug, Subcommand)]
enum ConfigOp {
    /// Show the whole configuration (secrets masked).
    Show,
    /// Read one dotted `section.field` key.
    Get { key: String },
    /// Set one key (applies live when mutable, else persisted for restart).
    Set { key: String, value: String },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Some(cmd) = &cli.cmd {
        // Cert generation is fully offline — no server, no backend.
        if let Cmd::Certs { op } = cmd {
            let CertsOp::Gen { out, nodes } = op;
            match certs::generate(out, *nodes) {
                Ok(paths) => {
                    println!("wrote {} files to {out}:", paths.len());
                    for p in &paths {
                        if let Some(name) = p.file_name() {
                            println!("  {}", name.to_string_lossy());
                        }
                    }
                    println!(
                        "\nConfigure each node with its own leaf: internode_auth = \"cert\",\n\
                         internode_tls_cert = node<i>.crt, internode_tls_key = node<i>.key,\n\
                         internode_tls_ca = ca.crt. Keep ca.key OFF the nodes (issuing root)."
                    );
                    return ExitCode::SUCCESS;
                }
                Err(e) => {
                    eprintln!("skaidbsh: certs gen: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        // Keyfile generation is fully offline — no server, no backend.
        if let Cmd::Keyfile { op } = cmd {
            let KeyfileOp::Gen { out } = op;
            match generate_keyfile(out) {
                Ok(()) => {
                    println!("wrote a 32-byte at-rest keyfile to {out} (0600).");
                    println!(
                        "Back it up OFF-BOX now — losing it makes all encrypted data\n\
                         unrecoverable. Configure: encryption.at_rest_enabled = true,\n\
                         encryption.at_rest_keyfile = \"{out}\"."
                    );
                    return ExitCode::SUCCESS;
                }
                Err(e) => {
                    eprintln!("skaidbsh: keyfile gen: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        // Export runs SQL, so it uses a full backend (works against a server or
        // `--local`), not the REST control plane.
        if let Cmd::Export(args) = cmd {
            let mut shell = match Shell::connect(&cli) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("skaidbsh: {e}");
                    return ExitCode::FAILURE;
                }
            };
            return dump::run(&mut shell.backend, args);
        }
        // Other admin subcommands always use the REST control plane.
        if cli.local.is_some() {
            eprintln!("skaidbsh: admin commands need a server; --local is offline only");
            return ExitCode::FAILURE;
        }
        return run_admin(&cli, cmd);
    }

    let mut shell = match Shell::connect(&cli) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skaidbsh: {e}");
            return ExitCode::FAILURE;
        }
    };

    // One-shot script from `-e` or `-f`, else an interactive/piped REPL.
    if let Some(script) = &cli.execute {
        return shell.run_script(script);
    }
    if let Some(path) = &cli.file {
        let script = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("skaidbsh: cannot read {path}: {e}");
                return ExitCode::FAILURE;
            }
        };
        return shell.run_script(&script);
    }

    if io::stdin().is_terminal() {
        shell.repl_interactive();
    } else {
        shell.repl_piped();
    }
    ExitCode::SUCCESS
}

/// Where statements execute: a remote node over the driver, or a local engine.
pub(crate) enum Backend {
    Net { client: Client, current_db: String },
    // Boxed: the embedded engine is large, dwarfing the network variant.
    Local(Box<Session>),
}

impl Backend {
    /// Execute one statement, rendering its result. Returns the error message
    /// on failure (so the caller can print a hint).
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        match self {
            Backend::Net { client, current_db } => match client.execute(sql) {
                Ok(resp) => {
                    render::print_response(&resp);
                    track_use(sql, current_db);
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            },
            Backend::Local(session) => match session.execute(sql) {
                Ok(out) => {
                    render::print_output(&out);
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            },
        }
    }

    pub(crate) fn current_db(&self) -> &str {
        match self {
            Backend::Net { current_db, .. } => current_db,
            Backend::Local(session) => session.current_database(),
        }
    }

    /// Run a query and return its columns + rows, without rendering (for
    /// export/import). Non-row results yield empty vecs.
    pub(crate) fn query(&mut self, sql: &str) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
        match self {
            Backend::Net { client, current_db } => {
                let resp = client.execute(sql).map_err(|e| e.to_string())?;
                track_use(sql, current_db);
                match resp {
                    Response::Rows { columns, rows } => Ok((columns, rows)),
                    Response::Error(msg) => Err(msg),
                    _ => Ok((Vec::new(), Vec::new())),
                }
            }
            Backend::Local(session) => match session.execute(sql).map_err(|e| e.to_string())? {
                QueryOutput::Rows(rs) => Ok((rs.columns, rs.rows)),
                _ => Ok((Vec::new(), Vec::new())),
            },
        }
    }

    /// Execute a statement without rendering, returning rows affected (0 for
    /// DDL). For bulk import and dump-schema replay.
    pub(crate) fn exec_quiet(&mut self, sql: &str) -> Result<u64, String> {
        match self {
            Backend::Net { client, current_db } => {
                let resp = client.execute(sql).map_err(|e| e.to_string())?;
                track_use(sql, current_db);
                match resp {
                    Response::Mutation { affected } => Ok(affected),
                    Response::Error(msg) => Err(msg),
                    _ => Ok(0),
                }
            }
            Backend::Local(session) => match session.execute(sql).map_err(|e| e.to_string())? {
                QueryOutput::Mutation { affected } => Ok(affected as u64),
                _ => Ok(0),
            },
        }
    }

    fn set_consistency(&mut self, c: Consistency) -> bool {
        match self {
            Backend::Net { client, .. } => {
                client.set_consistency(c);
                true
            }
            Backend::Local(_) => false,
        }
    }
}

/// The running shell: a backend plus the REST port/credentials used by the admin
/// helper commands. REST targets are derived live from the driver's endpoint
/// pool so admin commands fail over to a surviving node just like SQL does.
struct Shell {
    backend: Backend,
    rest_port: u16,
    user: Option<String>,
    password: Option<String>,
    rest_tls: Option<http::TlsClient>,
}

impl Shell {
    fn connect(cli: &Cli) -> Result<Shell, String> {
        let backend = if let Some(dir) = &cli.local {
            let session = Session::open(dir).map_err(|e| format!("cannot open {dir}: {e}"))?;
            eprintln!("skaidbsh: embedded engine on {dir} (offline). Type 'help', Ctrl-D to exit.");
            Backend::Local(Box::new(session))
        } else {
            let endpoints = sql_endpoints(cli);
            let user = cli.user.as_deref().unwrap_or("anonymous");
            let pass = cli.password.as_deref().unwrap_or("");
            let mut client = match cli.auth_mechanism.to_ascii_lowercase().as_str() {
                "gssapi" | "kerberos" => {
                    let spn = cli.gssapi_spn.as_deref().ok_or_else(|| {
                        "--auth-mechanism gssapi requires --gssapi-spn <service-principal> \
                         (e.g. skaidb/host@REALM)"
                            .to_string()
                    })?;
                    Client::connect_gssapi_tls(&endpoints, user, spn, build_tls(cli)?)
                        .map_err(|e| format!("could not connect (GSSAPI): {e}"))?
                }
                "scram" | "password" => Client::connect_many_tls(&endpoints, user, pass, build_tls(cli)?)
                    .map_err(|e| format!("could not connect: {e}"))?,
                other => {
                    return Err(format!("unknown --auth-mechanism {other:?} (use scram or gssapi)"))
                }
            };
            if let Some(c) = parse_consistency(&cli.consistency) {
                client.set_consistency(c);
            }
            // Discover the rest of the cluster from the seed so a single --host
            // still gives full failover: ask /status for the members' client
            // endpoints and add any new ones to the driver's failover pool.
            let rest_tls = build_rest_tls(cli)?;
            let discovered = discover_peers(&rest_endpoints(cli), rest_tls.as_ref());
            let new_peers = discovered.len();
            client.add_endpoints(&discovered);
            let total = client.endpoints().len();
            eprintln!(
                "skaidbsh: connected to {} ({} endpoint{}{}). Type 'help', Ctrl-D to exit.",
                client.endpoint(),
                total,
                if total == 1 { "" } else { "s" },
                if new_peers > 0 { format!(", {new_peers} discovered") } else { String::new() }
            );
            Backend::Net {
                client,
                current_db: skaidb_engine::DEFAULT_DATABASE.to_string(),
            }
        };
        Ok(Shell {
            backend,
            rest_port: effective_rest_port(cli),
            user: cli.user.clone(),
            password: cli.password.clone(),
            rest_tls: build_rest_tls(cli)?,
        })
    }

    fn auth(&self) -> http::Auth<'_> {
        match (&self.user, &self.password) {
            (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
            _ => None,
        }
    }

    /// REST targets for admin helpers, derived from the driver's current
    /// endpoint pool (seed + discovered peers) so a command tries the live node
    /// first and fails over to the others — the same redundancy SQL gets. Empty
    /// in `--local` mode.
    fn rest_targets(&self) -> Vec<String> {
        let Backend::Net { client, .. } = &self.backend else {
            return Vec::new();
        };
        let to_rest = |e: &str| {
            let host = e.rsplit_once(':').map(|(h, _)| h).unwrap_or(e);
            format!("{host}:{}", self.rest_port)
        };
        // Currently-connected node first, then every other known endpoint.
        let mut out = vec![to_rest(client.endpoint())];
        for e in client.endpoints() {
            let r = to_rest(e);
            if !out.contains(&r) {
                out.push(r);
            }
        }
        out
    }

    /// Run a `;`-separated script, stopping at the first error.
    fn run_script(&mut self, script: &str) -> ExitCode {
        for stmt in split_statements(script) {
            if self.run_sql(&stmt).is_err() {
                return ExitCode::FAILURE;
            }
        }
        ExitCode::SUCCESS
    }

    /// Execute one statement, printing any error and a hint.
    fn run_sql(&mut self, sql: &str) -> Result<(), ()> {
        match self.backend.execute(sql) {
            Ok(()) => Ok(()),
            Err(msg) => {
                eprintln!("error: {msg}");
                if let Some(hint) = suggest(sql, &msg) {
                    eprintln!("hint: {hint}");
                }
                Err(())
            }
        }
    }

    fn repl_interactive(&mut self) {
        let mut rl = match DefaultEditor::new() {
            Ok(rl) => rl,
            Err(e) => {
                eprintln!("skaidbsh: line editor unavailable ({e}); using basic input");
                self.repl_piped();
                return;
            }
        };
        let history = history_path();
        if let Some(path) = &history {
            let _ = rl.load_history(path);
        }

        let mut buffer = String::new();
        loop {
            let prompt = if buffer.is_empty() {
                format!("skaidb:{}> ", self.backend.current_db())
            } else {
                CONT_PROMPT.to_string()
            };
            match rl.readline(&prompt) {
                Ok(line) => {
                    let _ = rl.add_history_entry(line.as_str());
                    if buffer.is_empty() {
                        match self.handle_meta(line.trim()) {
                            Flow::Quit => break,
                            Flow::Handled => continue,
                            Flow::NotMeta => {}
                        }
                    }
                    buffer.push_str(&line);
                    buffer.push('\n');
                    self.drain_statements(&mut buffer);
                }
                Err(ReadlineError::Interrupted) => buffer.clear(),
                Err(ReadlineError::Eof) => break,
                Err(e) => {
                    eprintln!("read error: {e}");
                    break;
                }
            }
        }

        if let Some(path) = &history {
            let _ = rl.save_history(path);
        }
    }

    fn repl_piped(&mut self) {
        let stdin = io::stdin();
        let mut buffer = String::new();
        loop {
            let mut line = String::new();
            match stdin.lock().read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    eprintln!("read error: {e}");
                    break;
                }
            }
            // Backslash/meta commands work on their own line in piped mode too.
            if buffer.is_empty() && is_meta(line.trim()) {
                if let Flow::Quit = self.handle_meta(line.trim()) {
                    break;
                }
                continue;
            }
            buffer.push_str(&line);
            self.drain_statements(&mut buffer);
        }
    }

    /// Execute every complete (`;`-terminated) statement in `buffer`.
    fn drain_statements(&mut self, buffer: &mut String) {
        while let Some(idx) = find_statement_end(buffer) {
            let stmt: String = buffer.drain(..=idx).collect();
            let stmt = stmt.trim();
            if !stmt.is_empty() && stmt != ";" {
                let _ = self.run_sql(stmt);
            }
        }
        if buffer.trim().is_empty() {
            buffer.clear();
        }
    }

    /// Handle a meta command (help/quit or a `\`-prefixed helper). Returns
    /// whether the line was a meta command and whether to quit.
    fn handle_meta(&mut self, line: &str) -> Flow {
        let line = line.trim_end_matches(';').trim();
        let lower = line.to_ascii_lowercase();
        match lower.as_str() {
            "help" | "?" | "\\h" | "\\?" => {
                print_help();
                return Flow::Handled;
            }
            "quit" | "exit" | "\\q" => return Flow::Quit,
            _ => {}
        }
        if !line.starts_with('\\') {
            return Flow::NotMeta;
        }

        let mut parts = line.split_whitespace();
        let cmd = parts.next().unwrap_or("");
        let rest: Vec<&str> = parts.collect();
        match cmd {
            // SQL shortcuts.
            "\\l" => { let _ = self.run_sql("SHOW DATABASES"); }
            "\\dt" => { let _ = self.run_sql("SHOW TABLES"); }
            "\\di" => { let _ = self.run_sql("SHOW INDEXES"); }
            "\\consistency" => match rest.first().and_then(|s| parse_consistency(s)) {
                Some(c) => {
                    if self.backend.set_consistency(c) {
                        println!("consistency set to {}", rest[0].to_ascii_uppercase());
                    } else {
                        eprintln!("consistency applies to network sessions only");
                    }
                }
                None => eprintln!("usage: \\consistency one|quorum|all"),
            },
            // REST control-plane helpers.
            "\\status" => self.rest_get("/status", false),
            "\\metrics" => self.rest_get("/metrics", true),
            "\\cluster" => match rest.first().copied() {
                Some("raw") => self.rest_admin("/admin/status", String::new()),
                _ => self.rest_cluster(),
            },
            "\\repair" => self.rest_admin("/admin/repair", String::new()),
            "\\reclaim" => self.rest_admin("/admin/reclaim", String::new()),
            "\\node" => match (rest.first().copied(), rest.get(1).copied()) {
                (Some("add"), Some(addr)) => {
                    self.rest_admin("/admin/add-node", json_kv("addr", addr))
                }
                (Some("remove"), Some(id)) => {
                    self.rest_admin("/admin/remove-node", json_kv("id", id))
                }
                _ => eprintln!("usage: \\node add <addr> | \\node remove <id>"),
            },
            "\\config" => match (rest.first().copied(), rest.get(1).copied(), rest.get(2).copied()) {
                (None, _, _) => self.rest_admin("/admin/config", String::new()),
                (Some("get"), Some(key), _) => {
                    self.rest_admin("/admin/config/get", json_kv("key", key))
                }
                (Some("set"), Some(key), Some(value)) => {
                    self.rest_admin("/admin/config/set", json_kv2("key", key, "value", value))
                }
                _ => eprintln!("usage: \\config | \\config get <key> | \\config set <key> <value>"),
            },
            "\\ui" => match rest.first().copied() {
                None => self.ui_info(),
                Some(v @ ("on" | "off")) => {
                    let value = if v == "on" { "true" } else { "false" };
                    self.rest_admin(
                        "/admin/config/set",
                        json_kv2("key", "ui.enabled", "value", value),
                    );
                }
                _ => eprintln!("usage: \\ui | \\ui on | \\ui off"),
            },
            other => eprintln!("unknown command: {other} (type 'help')"),
        }
        Flow::Handled
    }

    /// Print the web UI URL(s) and whether the UI answers there right now.
    /// `/ui/meta` 404s when `ui.enabled` is off, so its status *is* the state.
    fn ui_info(&self) {
        if self.is_local() {
            eprintln!("the web UI is served by a network node (not in --local mode)");
            return;
        }
        for target in self.rest_targets() {
            let state = match http::get(std::slice::from_ref(&target), "/ui/meta", None, self.rest_tls.as_ref()) {
                Ok((200, _)) => "enabled",
                Ok((404, _)) => "disabled (\\ui on to enable)",
                Ok((code, _)) => return eprintln!("http://{target}/ui — unexpected HTTP {code}"),
                Err(e) => {
                    eprintln!("http://{target}/ui — unreachable: {e}");
                    continue;
                }
            };
            println!("http://{target}/ui — {state}");
        }
    }

    /// Unauthenticated GET helper (`/status`, `/metrics`).
    fn rest_get(&self, path: &str, raw: bool) {
        if self.is_local() {
            return;
        }
        match http::get(&self.rest_targets(), path, None, self.rest_tls.as_ref()) {
            Ok((_, body)) if raw => print!("{body}"),
            Ok((_, body)) => http::print_body(&body),
            Err(e) => eprintln!("error: {e}"),
        }
    }

    /// Authenticated POST helper for `/admin/*`.
    fn rest_admin(&self, path: &str, body: String) {
        if self.is_local() {
            return;
        }
        match http::post(&self.rest_targets(), path, &body, self.auth(), self.rest_tls.as_ref()) {
            Ok((_, resp)) => http::print_body(&resp),
            Err(e) => eprintln!("error: {e}"),
        }
    }

    /// `\cluster`: fetch `/admin/status` and render the human-readable summary
    /// (verdict + per-peer table + actions) instead of the raw JSON blob.
    fn rest_cluster(&self) {
        if self.is_local() {
            return;
        }
        match http::post(&self.rest_targets(), "/admin/status", "", self.auth(), self.rest_tls.as_ref()) {
            Ok((_, resp)) => cluster::render(&resp),
            Err(e) => eprintln!("error: {e}"),
        }
    }

    fn is_local(&self) -> bool {
        if matches!(self.backend, Backend::Local(_)) {
            eprintln!("not connected to a server (running with --local)");
            return true;
        }
        false
    }
}

/// Result of inspecting a line for meta commands.
enum Flow {
    /// The line was a meta command and has been handled.
    Handled,
    /// The user asked to quit.
    Quit,
    /// Not a meta command — treat as SQL.
    NotMeta,
}

/// Run a one-shot admin subcommand over REST and exit accordingly.
/// Write a fresh random 32-byte at-rest KEK to `path` with 0600 permissions.
fn generate_keyfile(path: &str) -> Result<(), String> {
    let kek = skaidb_engine::Kek::generate().map_err(|e| e.to_string())?;
    std::fs::write(path, kek.as_bytes()).map_err(|e| format!("write {path}: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

/// Whether any TLS flag is set (any of `--tls`, `--tls-ca`, `--tls-insecure`).
fn tls_requested(cli: &Cli) -> bool {
    cli.tls || cli.tls_ca.is_some() || cli.tls_insecure
}

/// Resolve the client verification policy from the flags (shared by the SQL
/// and REST TLS builders): a CA file, insecure skip, or — with a bare `--tls`
/// and no CA — insecure (there is no bundled public-root store).
fn tls_verify(cli: &Cli) -> skaidb_net::ClientVerify {
    if let Some(ca) = &cli.tls_ca {
        skaidb_net::ClientVerify::CaFile(ca.clone())
    } else {
        skaidb_net::ClientVerify::Insecure
    }
}

/// Build the SQL-driver TLS config, or `None` when no TLS flag is set.
fn build_tls(cli: &Cli) -> Result<Option<TlsConfig>, String> {
    if !tls_requested(cli) {
        return Ok(None);
    }
    let verify = match &cli.tls_ca {
        Some(ca) => TlsVerify::CaFile(ca.clone()),
        None => TlsVerify::Insecure,
    };
    Ok(Some(
        TlsConfig::new(verify, &cli.tls_server_name).map_err(|e| e.to_string())?,
    ))
}

/// Build the REST-control-plane TLS config, or `None` when no TLS flag is set.
fn build_rest_tls(cli: &Cli) -> Result<Option<http::TlsClient>, String> {
    if !tls_requested(cli) {
        return Ok(None);
    }
    let cfg = skaidb_net::client_config(tls_verify(cli), None)?;
    Ok(Some(http::TlsClient {
        cfg,
        server_name: cli.tls_server_name.clone(),
    }))
}

fn run_admin(cli: &Cli, cmd: &Cmd) -> ExitCode {
    let endpoints = rest_endpoints(cli);
    let auth = match (&cli.user, &cli.password) {
        (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
        _ => None,
    };
    let tls = match build_rest_tls(cli) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("skaidbsh: {e}");
            return ExitCode::FAILURE;
        }
    };
    let tls = tls.as_ref();

    let result = match cmd {
        Cmd::Status => http::get(&endpoints, "/status", None, tls),
        Cmd::Metrics => http::get(&endpoints, "/metrics", None, tls),
        Cmd::Cluster { op } => {
            let (path, body) = match op {
                ClusterOp::Status => ("/admin/status", String::new()),
                ClusterOp::Repair => ("/admin/repair", String::new()),
                ClusterOp::Reclaim => ("/admin/reclaim", String::new()),
                ClusterOp::AddNode { addr } => ("/admin/add-node", json_kv("addr", addr)),
                ClusterOp::RemoveNode { id } => ("/admin/remove-node", json_kv("id", id)),
            };
            http::post(&endpoints, path, &body, auth, tls)
        }
        Cmd::Config { op } => {
            let (path, body) = match op {
                ConfigOp::Show => ("/admin/config", String::new()),
                ConfigOp::Get { key } => ("/admin/config/get", json_kv("key", key)),
                ConfigOp::Set { key, value } => {
                    ("/admin/config/set", json_kv2("key", key, "value", value))
                }
            };
            http::post(&endpoints, path, &body, auth, tls)
        }
        // Export is intercepted in main() (it needs a SQL backend, not REST).
        Cmd::Export(_) => unreachable!("export is handled before run_admin"),
        // Certs is intercepted in main() (fully offline, no server).
        Cmd::Certs { .. } => unreachable!("certs is handled before run_admin"),
        // Keyfile is intercepted in main() (fully offline, no server).
        Cmd::Keyfile { .. } => unreachable!("keyfile is handled before run_admin"),
    };

    match result {
        Ok((status, body)) => {
            // /metrics is plain text; `cluster status` gets the human-readable
            // summary; everything else is pretty-printed JSON.
            if matches!(cmd, Cmd::Metrics) {
                print!("{body}");
            } else if matches!(cmd, Cmd::Cluster { op: ClusterOp::Status }) {
                cluster::render(&body);
            } else {
                http::print_body(&body);
            }
            if status < 400 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("skaidbsh: request failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// SQL (binary) endpoints: each host keeps its own port, else gets `--port`.
fn sql_endpoints(cli: &Cli) -> Vec<String> {
    cli.host
        .iter()
        .map(|h| if h.contains(':') { h.clone() } else { format!("{h}:{}", cli.port) })
        .collect()
}

/// REST endpoints: the bare host of each entry with `--rest-port`.
/// REST port to target. An explicit `--rest-port` wins; otherwise TLS moves the
/// default to the HTTPS port (7443), since a TLS-enabled server serves HTTPS
/// there and only redirects on 7080.
fn effective_rest_port(cli: &Cli) -> u16 {
    if cli.rest_port != 7080 {
        cli.rest_port
    } else if tls_requested(cli) {
        7443
    } else {
        7080
    }
}

fn rest_endpoints(cli: &Cli) -> Vec<String> {
    let port = effective_rest_port(cli);
    cli.host
        .iter()
        .map(|h| {
            let host = h.split(':').next().unwrap_or(h);
            format!("{host}:{port}")
        })
        .collect()
}

/// Ask `/status` (unauthenticated) for the cluster members' client endpoints, so
/// connecting to one seed yields the whole failover set. Members currently
/// resyncing (backfilling from empty) are excluded — a resyncing node holds
/// incomplete data, so the driver routes around it. Best-effort: returns an
/// empty list for a standalone node or any error.
fn discover_peers(rest_endpoints: &[String], tls: Option<&http::TlsClient>) -> Vec<String> {
    let Ok((_, body)) = http::get(rest_endpoints, "/status", None, tls) else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) else {
        return Vec::new();
    };
    let str_array = |key: &str| -> Vec<String> {
        v.get(key)
            .and_then(|e| e.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default()
    };
    let resyncing: std::collections::HashSet<String> =
        str_array("resyncing_endpoints").into_iter().collect();
    str_array("endpoints")
        .into_iter()
        .filter(|ep| !resyncing.contains(ep))
        .collect()
}

/// Build a `{"k":"v"}` JSON body.
fn json_kv(k: &str, v: &str) -> String {
    serde_json::json!({ k: v }).to_string()
}

/// Build a `{"k1":"v1","k2":"v2"}` JSON body.
fn json_kv2(k1: &str, v1: &str, k2: &str, v2: &str) -> String {
    serde_json::json!({ k1: v1, k2: v2 }).to_string()
}

fn parse_consistency(s: &str) -> Option<Consistency> {
    match s.trim().to_ascii_lowercase().as_str() {
        "one" => Some(Consistency::One),
        "quorum" => Some(Consistency::Quorum),
        "all" => Some(Consistency::All),
        _ => None,
    }
}

/// Track the connection's current database client-side by watching `USE`, so the
/// prompt reflects it. (The server keeps the authoritative per-connection state.)
fn track_use(sql: &str, current_db: &mut String) {
    let mut words = sql.split_whitespace();
    if !words.next().is_some_and(|w| w.eq_ignore_ascii_case("use")) {
        return;
    }
    let mut next = words.next().unwrap_or("");
    if next.eq_ignore_ascii_case("database") {
        next = words.next().unwrap_or("");
    }
    let name = next.trim_end_matches(';').trim();
    if !name.is_empty() {
        *current_db = name.to_string();
    }
}

/// Whether a line is a meta command (for piped mode).
fn is_meta(line: &str) -> bool {
    let l = line.trim_end_matches(';').trim().to_ascii_lowercase();
    l.starts_with('\\') || matches!(l.as_str(), "help" | "?" | "quit" | "exit")
}

fn print_help() {
    println!(
        "\
skaidbsh — commands

  Meta:
    help, ?            show this help
    quit, exit         leave the shell (or press Ctrl-D)
    Ctrl-C             discard the line you are typing

  SQL (end each with ';'):
    SELECT / INSERT / UPDATE / DELETE / CREATE / DROP / SHOW / USE / BEGIN ...
    \\l                 list databases (SHOW DATABASES)
    \\dt                list tables    (SHOW TABLES)
    \\di                list indexes   (SHOW INDEXES)
    \\consistency LVL   set read/write consistency: one | quorum | all

  Server / cluster (network mode):
    \\status            node health & topology (GET /status)
    \\metrics           Prometheus metrics (GET /metrics)
    \\cluster           cluster health & membership (\\cluster raw for JSON)
    \\node add <addr>   add a node       \\node remove <id>   decommission a node
    \\repair            anti-entropy repair    \\reclaim    reclaim space

  Configuration:
    \\config                  show all settings (secrets masked)
    \\config get <key>        read one section.field key
    \\config set <key> <val>  change a key (live if mutable, else needs restart)
    \\ui [on|off]             show the web UI URL, or toggle it live

  Full grammar: docs/QUERY_SYNTAX.md"
    );
}

/// Suggest a fix for a failed statement, recognising common mistakes from the
/// error `msg`. Returns `None` rather than guessing wildly.
fn suggest(sql: &str, msg: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let mut words = trimmed.split_whitespace();
    let first = words.next().unwrap_or("").trim_end_matches(';');
    let second = words.next().unwrap_or("").trim_end_matches(';');
    let first_up = first.to_ascii_uppercase();
    let second_low = second.to_ascii_lowercase();

    if msg.contains("lex error") {
        if trimmed.contains('\\') {
            return Some("stray '\\' character — skaidb does not use backslash line continuations; just keep typing until ';'.".into());
        }
        return Some("the statement contains a character skaidb cannot read; type 'help' to see valid syntax.".into());
    }

    if msg.contains("expected a statement") {
        if let Some(close) = closest_statement(&first_up) {
            return Some(format!("did you mean {close}? Type 'help' for the full list of commands."));
        }
        return Some(format!(
            "'{first}' is not a statement. Try SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, SHOW — or 'help'."
        ));
    }

    if msg.contains("after SHOW") {
        return Some("try: SHOW TABLES;  SHOW INDEXES;  SHOW STATUS;  or  SHOW DATABASES;".into());
    }

    if msg.contains("expected TABLE, INDEX") {
        if matches!(second_low.as_str(), "db" | "schema") {
            return Some(format!("did you mean {first_up} DATABASE <name>?"));
        }
        return Some(format!(
            "{first_up} what? Try {first_up} TABLE <name> (...);  {first_up} INDEX ...;  or  {first_up} DATABASE <name>;"
        ));
    }

    if msg.contains("does not exist") && msg.contains("database") {
        return Some("no such database — run SHOW DATABASES; to list them, or CREATE DATABASE <name>; first.".into());
    }

    if msg.contains("expected LParen") && first_up == "CREATE" {
        let name = words.next().unwrap_or("").trim_end_matches(';');
        let name = if name.is_empty() { "<name>" } else { name };
        return Some(format!(
            "CREATE TABLE needs a primary key, e.g.  CREATE TABLE {name} (PRIMARY KEY (id));"
        ));
    }

    None
}

/// SQL statement keywords, used for "did you mean …?" suggestions.
const STATEMENT_KEYWORDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "SHOW", "USE", "BEGIN",
    "COMMIT", "ROLLBACK",
];

fn closest_statement(word: &str) -> Option<&'static str> {
    if word.is_empty() {
        return None;
    }
    let mut best: Option<(&'static str, usize)> = None;
    for &kw in STATEMENT_KEYWORDS {
        let d = edit_distance(word, kw);
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((kw, d));
        }
    }
    best.filter(|&(kw, d)| d <= 2.max(kw.len() / 3)).map(|(kw, _)| kw)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

fn history_path() -> Option<std::path::PathBuf> {
    let home = std::env::var_os("HOME")?;
    Some(std::path::Path::new(&home).join(".skaidb_history"))
}

/// Split a script into statements on top-level semicolons (ignoring `;` inside
/// single-quoted string literals).
fn split_statements(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_string = !in_string;
                if !in_string && chars.peek() == Some(&'\'') {
                    cur.push(c);
                    cur.push(chars.next().unwrap());
                    in_string = true;
                    continue;
                }
                cur.push(c);
            }
            ';' if !in_string => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// Index of the first top-level `;` in `buffer`, if any.
fn find_statement_end(buffer: &str) -> Option<usize> {
    let mut in_string = false;
    for (i, c) in buffer.char_indices() {
        match c {
            '\'' => in_string = !in_string,
            ';' if !in_string => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_err_msg(sql: &str) -> String {
        skaidb_engine::EngineError::Parse(skaidb_sql::parse(sql).unwrap_err()).to_string()
    }

    #[test]
    fn suggests_show_tables() {
        let hint = suggest("show dbs;", &parse_err_msg("show dbs;")).unwrap();
        assert!(hint.contains("SHOW TABLES"), "{hint}");
    }

    #[test]
    fn suggests_database_for_unknown_create_object() {
        let hint = suggest("create db foo;", &parse_err_msg("create db foo;")).unwrap();
        assert!(hint.contains("CREATE DATABASE"), "{hint}");
    }

    #[test]
    fn suggests_primary_key_clause() {
        let hint = suggest("create table test;", &parse_err_msg("create table test;")).unwrap();
        assert!(hint.contains("PRIMARY KEY"), "{hint}");
        assert!(hint.contains("test"), "{hint}");
    }

    #[test]
    fn suggests_closest_statement_keyword() {
        let hint = suggest("slect 1;", &parse_err_msg("slect 1;")).unwrap();
        assert!(hint.contains("SELECT"), "{hint}");
    }

    #[test]
    fn tracks_use_for_prompt() {
        let mut db = "default".to_string();
        track_use("USE shop;", &mut db);
        assert_eq!(db, "shop");
        track_use("USE DATABASE analytics", &mut db);
        assert_eq!(db, "analytics");
        track_use("SELECT 1", &mut db);
        assert_eq!(db, "analytics"); // unchanged by non-USE
    }

    #[test]
    fn endpoints_apply_default_ports() {
        let cli = Cli {
            host: vec!["a".into(), "b:7001".into()],
            port: 7000,
            rest_port: 7080,
            user: None,
            password: None,
            auth_mechanism: "scram".into(),
            gssapi_spn: None,
            consistency: "quorum".into(),
            tls: false,
            tls_ca: None,
            tls_insecure: false,
            tls_server_name: "skaidb".into(),
            local: None,
            execute: None,
            file: None,
            cmd: None,
        };
        assert_eq!(sql_endpoints(&cli), vec!["a:7000", "b:7001"]);
        assert_eq!(rest_endpoints(&cli), vec!["a:7080", "b:7080"]);
    }

    #[test]
    fn meta_detection() {
        assert!(is_meta("help"));
        assert!(is_meta("\\dt"));
        assert!(is_meta("quit"));
        assert!(!is_meta("select 1"));
    }
}
