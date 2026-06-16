//! skaidbsh — the skaidb interactive shell and admin client.
//!
//! Network-first: SQL runs over the binary fast-path driver (with nearest-node
//! selection and failover across cluster members), while cluster/config/status
//! operations use the REST control plane. An embedded engine is still available
//! offline via `--local <dir>`.

mod http;
mod render;

use std::io::{self, BufRead, IsTerminal};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use skaidb_driver::Client;
use skaidb_engine::Session;
use skaidb_proto::Consistency;

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

    /// Default SQL consistency: one | quorum | all.
    #[arg(long, default_value = "quorum")]
    consistency: String,

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

    // Admin subcommands always use the REST control plane.
    if let Some(cmd) = &cli.cmd {
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
enum Backend {
    Net { client: Client, current_db: String },
    Local(Session),
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

    fn current_db(&self) -> &str {
        match self {
            Backend::Net { current_db, .. } => current_db,
            Backend::Local(session) => session.current_database(),
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
}

impl Shell {
    fn connect(cli: &Cli) -> Result<Shell, String> {
        let backend = if let Some(dir) = &cli.local {
            let session = Session::open(dir).map_err(|e| format!("cannot open {dir}: {e}"))?;
            eprintln!("skaidbsh: embedded engine on {dir} (offline). Type 'help', Ctrl-D to exit.");
            Backend::Local(session)
        } else {
            let endpoints = sql_endpoints(cli);
            let user = cli.user.as_deref().unwrap_or("anonymous");
            let pass = cli.password.as_deref().unwrap_or("");
            let mut client = Client::connect_many(&endpoints, user, pass)
                .map_err(|e| format!("could not connect: {e}"))?;
            if let Some(c) = parse_consistency(&cli.consistency) {
                client.set_consistency(c);
            }
            // Discover the rest of the cluster from the seed so a single --host
            // still gives full failover: ask /status for the members' client
            // endpoints and add any new ones to the driver's failover pool.
            let discovered = discover_peers(&rest_endpoints(cli));
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
            rest_port: cli.rest_port,
            user: cli.user.clone(),
            password: cli.password.clone(),
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
            "\\cluster" => self.rest_admin("/admin/status", String::new()),
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
            other => eprintln!("unknown command: {other} (type 'help')"),
        }
        Flow::Handled
    }

    /// Unauthenticated GET helper (`/status`, `/metrics`).
    fn rest_get(&self, path: &str, raw: bool) {
        if self.is_local() {
            return;
        }
        match http::get(&self.rest_targets(), path, None) {
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
        match http::post(&self.rest_targets(), path, &body, self.auth()) {
            Ok((_, resp)) => http::print_body(&resp),
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
fn run_admin(cli: &Cli, cmd: &Cmd) -> ExitCode {
    let endpoints = rest_endpoints(cli);
    let auth = match (&cli.user, &cli.password) {
        (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
        _ => None,
    };

    let result = match cmd {
        Cmd::Status => http::get(&endpoints, "/status", None),
        Cmd::Metrics => http::get(&endpoints, "/metrics", None),
        Cmd::Cluster { op } => {
            let (path, body) = match op {
                ClusterOp::Status => ("/admin/status", String::new()),
                ClusterOp::Repair => ("/admin/repair", String::new()),
                ClusterOp::Reclaim => ("/admin/reclaim", String::new()),
                ClusterOp::AddNode { addr } => ("/admin/add-node", json_kv("addr", addr)),
                ClusterOp::RemoveNode { id } => ("/admin/remove-node", json_kv("id", id)),
            };
            http::post(&endpoints, path, &body, auth)
        }
        Cmd::Config { op } => {
            let (path, body) = match op {
                ConfigOp::Show => ("/admin/config", String::new()),
                ConfigOp::Get { key } => ("/admin/config/get", json_kv("key", key)),
                ConfigOp::Set { key, value } => {
                    ("/admin/config/set", json_kv2("key", key, "value", value))
                }
            };
            http::post(&endpoints, path, &body, auth)
        }
    };

    match result {
        Ok((status, body)) => {
            // /metrics is plain text; everything else is JSON.
            if matches!(cmd, Cmd::Metrics) {
                print!("{body}");
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
fn rest_endpoints(cli: &Cli) -> Vec<String> {
    cli.host
        .iter()
        .map(|h| {
            let host = h.split(':').next().unwrap_or(h);
            format!("{host}:{}", cli.rest_port)
        })
        .collect()
}

/// Ask `/status` (unauthenticated) for the cluster members' client endpoints, so
/// connecting to one seed yields the whole failover set. Best-effort: returns an
/// empty list for a standalone node or any error.
fn discover_peers(rest_endpoints: &[String]) -> Vec<String> {
    let Ok((_, body)) = http::get(rest_endpoints, "/status", None) else {
        return Vec::new();
    };
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("endpoints")
                .and_then(|e| e.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
        })
        .unwrap_or_default()
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
    \\cluster           cluster membership (admin)
    \\node add <addr>   add a node       \\node remove <id>   decommission a node
    \\repair            anti-entropy repair    \\reclaim    reclaim space

  Configuration:
    \\config                  show all settings (secrets masked)
    \\config get <key>        read one section.field key
    \\config set <key> <val>  change a key (live if mutable, else needs restart)

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
            consistency: "quorum".into(),
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
