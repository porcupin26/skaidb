//! skaidb configuration (SPEC §9).
//!
//! The on-disk format is TOML. Every option also has a built-in default, so a
//! config file may specify only the fields it wants to override. CLI flags and
//! environment variables are layered on top by the server binary, giving the
//! precedence: CLI args > env vars > config file > built-in defaults.

use std::path::Path;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Errors raised while loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}

/// Role this node plays in the cluster (SPEC §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Full read/write authoritative replica.
    Member,
    /// Read-through cache holding a configurable subset; forwards writes.
    Agent,
}

/// Tunable per-query consistency level (SPEC §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "UPPERCASE")]
pub enum Consistency {
    One,
    Quorum,
    All,
}

/// How an internode connection authenticates (SPEC §8.1).
///
/// - `None` — no authentication (trusted/isolated network only; the default,
///   for backward compatibility with existing clusters).
/// - `Token` — a shared secret; peers prove knowledge of it via an HMAC-SHA256
///   challenge-response (the token never crosses the wire).
/// - `Cert` — mutual TLS: every node presents a certificate signed by a shared
///   CA, and connections are encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum InternodeAuth {
    #[serde(alias = "off", alias = "disabled")]
    None,
    /// Shared-secret challenge-response. (Accepts the legacy name `keyfile`.)
    #[serde(alias = "keyfile")]
    Token,
    /// Mutual TLS with a shared CA. (Accepts the legacy name `x509`.)
    #[serde(alias = "x509")]
    Cert,
}

/// Source of the key-encryption key for at-rest encryption (SPEC §8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum KekSource {
    Keyfile,
    Kms,
}

/// Client-facing TLS mode for the binary + REST ports.
///
/// - `Off` — plaintext only (default; back-compat).
/// - `Opportunistic` — serve BOTH on the same port: a TLS ClientHello is
///   wrapped, anything else stays plaintext (smooth migration).
/// - `Required` — TLS only; plaintext connections are refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ClientTlsMode {
    #[serde(alias = "disabled")]
    Off,
    #[serde(alias = "opt")]
    Opportunistic,
    #[serde(alias = "require")]
    Required,
}

/// Top-level configuration mirroring SPEC §9.1.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub cluster: ClusterConfig,
    pub agent: AgentConfig,
    pub auth: AuthConfig,
    pub encryption: EncryptionConfig,
    pub storage: StorageConfig,
    pub observability: ObservabilityConfig,
    pub ui: UiConfig,
    pub witness: WitnessConfig,
}

/// Witness mode (`[witness]`): this node periodically PULLS a full copy of
/// the configured databases from a primary cluster it is NOT a member of —
/// a cross-region backup that never participates in the primary's quorums
/// and sets its own pace. Data moves over the internode protocol
/// (`ScanPage` pages: byte-exact rows with HLC stamps and tombstones, so
/// re-pulls converge by last-writer-wins and deletes propagate); schema
/// listing and the registration/heartbeat rows in the primary's
/// `witnesses` table go over the ordinary SQL protocol with
/// witness-scoped credentials. Requires standalone (non-cluster) mode;
/// pair it with `server.read_only = true` so drivers can read the copy
/// but nothing can diverge it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WitnessConfig {
    /// Run the pull loop. Off by default; a node with this off is just a
    /// normal server.
    pub enabled: bool,
    /// Primary members' SQL (binary-protocol) endpoints, `host:port` —
    /// tried in order (failover) for schema listing and registration.
    pub primary_sql_addrs: Vec<String>,
    /// Primary members' internode endpoints, `host:internode_port` —
    /// tried in order for `ScanPage` data pulls. On a full-copy primary
    /// (replication_factor >= members) any single member serves the
    /// whole table.
    pub primary_internode_addrs: Vec<String>,
    /// Credentials on the PRIMARY for schema listing + the `witnesses`
    /// registry writes (an operator-created role: `GRANT INSERT, UPDATE
    /// ON witnesses TO <role>`). The password is masked in `config show`.
    pub user: String,
    pub password: String,
    /// Databases to mirror. Empty = witness mode refuses to start (an
    /// explicit list is the operator stating what the backup covers).
    pub databases: Vec<String>,
    /// Seconds between pull cycles — default 60 (near-live): a cycle is
    /// cheap at steady state, since unchanged tables are skipped by the
    /// per-table `write_seq` hint and changed tables pull only their
    /// stamps-walked delta.
    pub interval_secs: u64,
    /// Seconds between FULL sweeps per table (default 24h) — the
    /// anti-entropy backstop for the one delta blind spot: a delayed
    /// hint-replay can land an old-stamped row on the primary after the
    /// witness's watermark already passed that timestamp, and only a full
    /// sweep re-observes it.
    pub full_sweep_interval_secs: u64,
    /// Ceiling, in percent (1–90), on how much of the serving primary's
    /// capacity the pull may take (same rest rule as
    /// `cluster.bootstrap_duty_pct`). **Live-mutable**:
    /// `SET CONFIG witness.duty_pct = '25'`.
    pub duty_pct: u32,
    /// Identity registered in the primary's `witnesses` table.
    pub witness_id: String,
    pub region: String,
    /// Wrap the SQL control-plane connection (`primary_sql_addrs`: schema
    /// listing, registration, heartbeat) in client TLS. REQUIRED when the
    /// primary runs `encryption.client_tls = "required"` — that path is a
    /// plain client connection and a required-TLS primary resets it
    /// otherwise. The bulk DATA pull over the internode port is already
    /// secured by `[auth]`, independent of this. Off by default.
    pub primary_tls: bool,
    /// CA file that verifies the primary's SQL cert. Empty falls back to the
    /// internode CA (`[auth].internode_tls_ca`) — one cluster CA usually
    /// secures both ports. If both are empty, TLS proceeds without
    /// verification (dev only).
    pub primary_tls_ca: String,
    /// SNI / cert-verification name for the SQL connection — the SAN on the
    /// primary's cert (`skaidb` for certs from `skaidbsh certs gen`).
    pub primary_tls_server_name: String,
}

impl Default for WitnessConfig {
    fn default() -> Self {
        WitnessConfig {
            enabled: false,
            primary_sql_addrs: Vec::new(),
            primary_internode_addrs: Vec::new(),
            user: String::new(),
            password: String::new(),
            databases: Vec::new(),
            interval_secs: 60,
            full_sweep_interval_secs: 86_400,
            duty_pct: 50,
            witness_id: "witness".to_string(),
            region: String::new(),
            primary_tls: false,
            primary_tls_ca: String::new(),
            primary_tls_server_name: "skaidb".to_string(),
        }
    }
}

/// The built-in web UI (`/ui` on the REST port).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UiConfig {
    /// Serve the embedded admin UI. **Live-mutable**: `config set
    /// ui.enabled false` makes `/ui` return 404 immediately, no restart.
    /// The UI is auth-gated exactly like `POST /query` either way.
    pub enabled: bool,
}

impl Default for UiConfig {
    fn default() -> Self {
        UiConfig { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub quic_port: u16,
    pub rest_port: u16,
    /// HTTPS REST port, used only when `encryption.client_tls` is on: the TLS
    /// REST API is served here (default 7443) and `rest_port` (7080) becomes a
    /// plaintext HTTP→HTTPS redirect to it. With `client_tls = off`, `rest_port`
    /// serves plaintext REST as before and this port is not bound.
    pub rest_tls_port: u16,
    pub node_role: NodeRole,
    pub data_dir: String,
    /// Reject every client-initiated mutation (INSERT/UPDATE/DELETE, DDL,
    /// user management, transactions, remote-write ingestion) with a clear
    /// error, while reads keep working normally. The node's own superuser
    /// role is exempt — internal telemetry (`node_stats`, the `drivers`
    /// registry) and a witness's data-pull applier both run as it, and a
    /// witness that refused its own applier could never receive data.
    /// **Live-mutable**: `SET CONFIG server.read_only = true` takes effect
    /// immediately, no restart — a maintenance write-freeze switch as much
    /// as a witness-node mode.
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Internode addresses of all cluster members, `host:port` (including this
    /// node). Empty means single-node / standalone mode.
    pub seeds: Vec<String>,
    /// Port this node serves internode replication RPC on.
    pub internode_port: u16,
    pub replication_factor: u32,
    pub vnodes_per_node: u32,
    pub default_read_consistency: Consistency,
    pub default_write_consistency: Consistency,
    /// How often (seconds) each node runs a background anti-entropy repair pass
    /// so missed DDL/writes converge without operator action. `0` disables it.
    pub anti_entropy_interval_secs: u64,
    /// Ceiling, in percent (1–90), on how much of THIS node's capacity a
    /// joining node's bootstrap push may take: each sent chunk is followed
    /// by a rest sized `work × (100 − pct) / pct`, so at the default 50 the
    /// sync never exceeds half duty. **Live-mutable** per node:
    /// `SET CONFIG cluster.bootstrap_duty_pct = '30'` (set it on the
    /// members that will serve the bootstrap).
    pub bootstrap_duty_pct: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentConfig {
    pub subset_tables: Vec<String>,
    pub max_staleness_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub scram_enabled: bool,
    pub x509_enabled: bool,
    pub x509_ca_file: String,
    /// Accept Kerberos (SASL GSSAPI) client authentication. Requires a build
    /// with the `kerberos` feature (glibc/macOS/Windows — the static-musl
    /// binary ships without it) and a readable `gssapi_keytab`. Clients then
    /// authenticate as external users (`CREATE USER "<principal>" GSSAPI`) with
    /// no password. SCRAM stays available alongside it.
    pub gssapi_enabled: bool,
    /// Path to the service keytab holding the skaidb service principal's
    /// long-term key (the GSSAPI `KRB5_KTNAME`). Read once at startup.
    pub gssapi_keytab: String,
    /// Service principal to accept as, e.g. `skaidb/host.example.com@REALM`.
    /// Empty accepts whatever principals the keytab holds (the usual case —
    /// the acceptor tries every key in the keytab).
    pub gssapi_service_principal: String,
    pub internode_auth: InternodeAuth,
    /// Token mode: inline shared secret. Takes precedence over `internode_keyfile`.
    pub internode_token: String,
    /// Token mode: path to a file holding the shared secret (used when
    /// `internode_token` is empty).
    pub internode_keyfile: String,
    /// Cert mode: this node's certificate (PEM), its private key (PEM), and the
    /// CA (PEM) that signs every node's certificate.
    pub internode_tls_cert: String,
    pub internode_tls_key: String,
    pub internode_tls_ca: String,
    pub superuser: String,
    /// Password for the bootstrapped superuser. When `scram_enabled` is true and
    /// this is non-empty, the server requires authentication; otherwise
    /// connections are accepted anonymously (development default).
    pub superuser_password: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionConfig {
    pub tls_cert_file: String,
    pub tls_key_file: String,
    /// Client-facing TLS mode (binary + REST ports). Default `off`.
    pub client_tls: ClientTlsMode,
    pub at_rest_enabled: bool,
    pub at_rest_kek_source: KekSource,
    pub at_rest_keyfile: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Overall memory budget for storage (memtable + read cache). `"auto"`
    /// sizes it to half the node's memory limit (cgroup-aware, so containers
    /// get their *container* limit, not the host's); an explicit size like
    /// `"256MB"`/`"1GB"` uses that budget; empty (the default) disables
    /// budgeting and the individual knobs below apply as-is. When set, it
    /// **overrides** `memtable_size_mb` and `read_cache_entries`.
    pub memory_target: String,
    pub memtable_size_mb: u64,
    /// Entry capacity of the RAM read cache for point reads that miss the
    /// memtable (0 disables it). Larger values trade RAM for hit rate on
    /// datasets bigger than the memtable.
    pub read_cache_entries: u64,
    /// Per-statement scan budget: the maximum rows one statement may examine
    /// (decode + filter) across all its gathers before it errors. `LIMIT`
    /// bounds output, not scan work — a filter that matches (almost)
    /// nothing under `ORDER BY .. LIMIT` otherwise walks whole tables per
    /// query. `0` disables.
    pub scan_row_budget: u64,
    /// Per-statement byte budget: the maximum bytes one statement may
    /// MATERIALIZE into a result set (retained rows, across every gather)
    /// before it errors. `scan_row_budget` bounds rows *examined*; this bounds
    /// *memory held* — a 250k-row scan of multi-KB rows stays under the row
    /// budget yet materializes gigabytes on the coordinator (the read path
    /// that OOM-killed 4 GB nodes). Streaming `COUNT`/`DISTINCT` retain nothing
    /// and are never charged. `0` disables. Default 256 MB.
    pub scan_byte_budget: u64,
    /// Wall-clock ceiling per statement in seconds; past it the statement
    /// errors at its next scan-meter check (kills queries whose client has
    /// long since disconnected). `0` disables.
    pub statement_timeout_secs: u64,
    pub compaction_strategy: String,
    pub use_io_uring: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub prometheus_port: u16,
    /// Ingest this node's own `/metrics` into the `metrics` time-series
    /// table every [`Self::self_scrape_interval_secs`] — the node
    /// dashboards itself with no external Prometheus. **Live-mutable.**
    pub self_scrape: bool,
    /// Seconds between self-scrapes (minimum 1). **Live-mutable.**
    pub self_scrape_interval_secs: u64,
    /// Publish this node's host statistics (CPU, memory, disk, uptime,
    /// restarts, OOM kills) into the replicated `node_stats` table — one row
    /// per node, keyed on the node id, stamped with the sample time. Any
    /// member then serves the whole cluster's dashboard from a local read
    /// (with per-node data age) instead of probing peers on every page load.
    /// **Live-mutable.**
    pub node_stats: bool,
    /// Seconds between node-stats rows (minimum 1). **Live-mutable.**
    pub node_stats_interval_secs: u64,
    pub slow_query_ms: u64,
    pub query_log_enabled: bool,
    pub query_log_masked: bool,
    pub login_log_enabled: bool,
    pub error_log_level: String,
    /// Emit per-table metrics (live keys, tombstones, on-disk bytes). Off by
    /// default because table count is unbounded — keep label cardinality bounded
    /// (SPEC §10) and only enable this when the table set is known and small.
    pub per_table_metrics: bool,
    /// Audit/query/login log format: `"text"` (human-readable, default) or
    /// `"json"` (one JSON object per line, for a log agent to parse reliably).
    pub log_format: String,
    /// Default destination file for audit logs. Empty (the default) writes to
    /// the process's stderr, preserving the original behavior. A relative path
    /// is resolved against the working directory; the file is created if absent
    /// and appended to otherwise.
    ///
    /// Each log category below can override this with its own file, so
    /// individual streams can be split out (e.g. send the error log to its own
    /// file while everything else shares `log_file`). Adding a new log category
    /// in the future is just another `*_log_file` override here.
    pub log_file: String,
    /// Override file for the query log. Empty falls back to [`log_file`].
    pub query_log_file: String,
    /// Override file for the slow-query log. Empty falls back to [`log_file`].
    pub slow_query_log_file: String,
    /// Override file for the error log. Empty falls back to [`log_file`].
    pub error_log_file: String,
    /// Override file for the login/auth log. Empty falls back to [`log_file`].
    pub login_log_file: String,
}

impl Config {
    /// Parse a configuration from a TOML string, filling unspecified fields
    /// with their defaults.
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError> {
        Ok(toml::from_str(s)?)
    }

    /// Load configuration from a TOML file on disk.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Config::from_toml_str(&text)
    }

    /// Serialize this configuration back to TOML (used to emit a sample config).
    pub fn to_toml_string(&self) -> String {
        toml::to_string_pretty(self).expect("config serializes to TOML")
    }

    /// The whole configuration as a nested JSON object (`section.field`), with
    /// secrets masked — for `config show` over the admin API.
    pub fn to_redacted_json(&self) -> serde_json::Value {
        let mut root = serde_json::to_value(self).expect("config serializes to JSON");
        redact(&mut root);
        root
    }

    /// Read one dotted `section.field` key, with secrets masked. `None` if the
    /// key does not exist.
    pub fn get_key_redacted(&self, key: &str) -> Option<serde_json::Value> {
        let (section, field) = key.split_once('.')?;
        self.to_redacted_json()
            .get(section)?
            .get(field)
            .cloned()
    }

    /// Return a copy of this config with `section.field` set from the string
    /// `value`. The value is coerced to the field's existing JSON type and the
    /// result is validated by round-tripping through the typed `Config`, so an
    /// unknown key or an ill-typed / invalid-enum value is rejected with a
    /// descriptive error rather than silently corrupting the config.
    pub fn with_key_set(&self, key: &str, value: &str) -> Result<Config, String> {
        let (section, field) = key
            .split_once('.')
            .ok_or_else(|| format!("key must be `section.field`, got `{key}`"))?;
        let mut root = serde_json::to_value(self).expect("config serializes to JSON");
        let obj = root
            .get_mut(section)
            .and_then(|s| s.as_object_mut())
            .ok_or_else(|| format!("unknown config section: `{section}`"))?;
        let existing = obj
            .get(field)
            .ok_or_else(|| format!("unknown config key: `{key}`"))?;
        let coerced = coerce(existing, value)
            .map_err(|e| format!("invalid value for `{key}`: {e}"))?;
        obj.insert(field.to_string(), coerced);
        serde_json::from_value(root).map_err(|e| format!("invalid value for `{key}`: {e}"))
    }
}

/// Dotted config keys whose changes take effect immediately, without a restart.
/// Everything else is read once at startup, so changing it only takes effect
/// after the server is restarted (it is still persisted to the config file).
pub const RUNTIME_MUTABLE_KEYS: &[&str] = &[
    "observability.self_scrape",
    "observability.self_scrape_interval_secs",
    "observability.node_stats",
    "observability.node_stats_interval_secs",
    "observability.slow_query_ms",
    "observability.query_log_enabled",
    "observability.query_log_masked",
    "observability.login_log_enabled",
    "observability.error_log_level",
    "observability.per_table_metrics",
    "observability.log_format",
    "observability.log_file",
    "observability.query_log_file",
    "observability.slow_query_log_file",
    "observability.error_log_file",
    "observability.login_log_file",
    "ui.enabled",
    "server.read_only",
    "cluster.bootstrap_duty_pct",
    "witness.duty_pct",
];

/// Whether changing `key` takes effect live (see [`RUNTIME_MUTABLE_KEYS`]).
pub fn is_runtime_mutable(key: &str) -> bool {
    RUNTIME_MUTABLE_KEYS.contains(&key)
}

/// Mask secret fields in a serialized config so they are never echoed back.
fn redact(root: &mut serde_json::Value) {
    for (section, field) in [("auth", "superuser_password"), ("witness", "password")] {
        if let Some(p) = root.get_mut(section).and_then(|a| a.get_mut(field)) {
            if p.as_str().is_some_and(|s| !s.is_empty()) {
                *p = serde_json::Value::String("***".into());
            }
        }
    }
}

/// Coerce a string into the JSON type of the field's `existing` value, so a
/// plain CLI string lands as the right type for re-deserialization.
fn coerce(existing: &serde_json::Value, value: &str) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    match existing {
        Value::Bool(_) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
            "false" | "0" | "no" | "off" => Ok(Value::Bool(false)),
            _ => Err(format!("expected a boolean, got `{value}`")),
        },
        Value::Number(_) => value
            .trim()
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .map_err(|_| format!("expected an integer, got `{value}`")),
        // Comma-separated list (e.g. cluster seeds, agent subset tables).
        Value::Array(_) => Ok(Value::Array(
            value
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| Value::String(s.to_string()))
                .collect(),
        )),
        // Strings and enums (which serialize as strings); validation happens on
        // the round-trip back into the typed Config.
        _ => Ok(Value::String(value.to_string())),
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: "127.0.0.1".to_string(),
            quic_port: 7000,
            rest_port: 7080,
            rest_tls_port: 7443,
            node_role: NodeRole::Member,
            data_dir: "/var/lib/skaidb".to_string(),
            read_only: false,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            seeds: Vec::new(),
            internode_port: 7100,
            replication_factor: 3,
            vnodes_per_node: 256,
            default_read_consistency: Consistency::Quorum,
            default_write_consistency: Consistency::Quorum,
            anti_entropy_interval_secs: 60,
            bootstrap_duty_pct: 50,
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            subset_tables: Vec::new(),
            max_staleness_ms: 5000,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            scram_enabled: true,
            x509_enabled: false,
            x509_ca_file: String::new(),
            gssapi_enabled: false,
            gssapi_keytab: String::new(),
            gssapi_service_principal: String::new(),
            internode_auth: InternodeAuth::None,
            internode_token: String::new(),
            internode_keyfile: String::new(),
            internode_tls_cert: String::new(),
            internode_tls_key: String::new(),
            internode_tls_ca: String::new(),
            superuser: "admin".to_string(),
            superuser_password: String::new(),
        }
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        EncryptionConfig {
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            client_tls: ClientTlsMode::Off,
            at_rest_enabled: false,
            at_rest_kek_source: KekSource::Keyfile,
            at_rest_keyfile: String::new(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            memory_target: String::new(),
            memtable_size_mb: 256,
            scan_row_budget: 250_000,
            scan_byte_budget: 256 * 1024 * 1024,
            statement_timeout_secs: 120,
            read_cache_entries: 16_384,
            compaction_strategy: "lazy_leveled".to_string(),
            use_io_uring: true,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        ObservabilityConfig {
            prometheus_port: 9090,
            self_scrape: false,
            self_scrape_interval_secs: 15,
            node_stats: true,
            node_stats_interval_secs: 1,
            slow_query_ms: 200,
            query_log_enabled: true,
            query_log_masked: true,
            login_log_enabled: true,
            error_log_level: "warn".to_string(),
            per_table_metrics: false,
            log_format: "text".to_string(),
            log_file: String::new(),
            query_log_file: String::new(),
            slow_query_log_file: String::new(),
            error_log_file: String::new(),
            login_log_file: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_spec() {
        let c = Config::default();
        assert_eq!(c.server.bind_addr, "127.0.0.1");
        assert_eq!(c.server.quic_port, 7000);
        assert_eq!(c.server.node_role, NodeRole::Member);
        assert_eq!(c.cluster.replication_factor, 3);
        assert_eq!(c.cluster.default_read_consistency, Consistency::Quorum);
        assert!(c.auth.scram_enabled);
    }

    #[test]
    fn partial_toml_overrides_only_specified_fields() {
        let toml = r#"
            [server]
            quic_port = 9999
            node_role = "agent"

            [cluster]
            replication_factor = 5
            default_read_consistency = "ONE"
        "#;
        let c = Config::from_toml_str(toml).unwrap();
        assert_eq!(c.server.quic_port, 9999);
        assert_eq!(c.server.node_role, NodeRole::Agent);
        assert_eq!(c.cluster.replication_factor, 5);
        assert_eq!(c.cluster.default_read_consistency, Consistency::One);
        // Untouched fields keep their defaults.
        assert_eq!(c.server.bind_addr, "127.0.0.1");
        assert_eq!(c.cluster.default_write_consistency, Consistency::Quorum);
    }

    #[test]
    fn set_key_coerces_and_validates() {
        let c = Config::default();
        // Number, bool, enum, and list coercions.
        let c = c.with_key_set("observability.slow_query_ms", "500").unwrap();
        assert_eq!(c.observability.slow_query_ms, 500);
        let c = c.with_key_set("observability.per_table_metrics", "yes").unwrap();
        assert!(c.observability.per_table_metrics);
        let c = c
            .with_key_set("cluster.default_read_consistency", "ONE")
            .unwrap();
        assert_eq!(c.cluster.default_read_consistency, Consistency::One);
        let c = c
            .with_key_set("cluster.seeds", "a:7100, b:7100 , c:7100")
            .unwrap();
        assert_eq!(c.cluster.seeds, vec!["a:7100", "b:7100", "c:7100"]);
    }

    #[test]
    fn set_key_rejects_bad_input() {
        let c = Config::default();
        assert!(c.with_key_set("cluster.nope", "1").is_err()); // unknown key
        assert!(c.with_key_set("nope.field", "1").is_err()); // unknown section
        assert!(c.with_key_set("cluster.replication_factor", "abc").is_err()); // not a number
        assert!(c
            .with_key_set("cluster.default_read_consistency", "MAYBE")
            .is_err()); // bad enum
        assert!(c.with_key_set("server", "x").is_err()); // not dotted
    }

    #[test]
    fn redacts_superuser_password() {
        let mut c = Config::default();
        c.auth.superuser_password = "hunter2".into();
        let json = c.to_redacted_json();
        assert_eq!(json["auth"]["superuser_password"], "***");
        assert_eq!(
            c.get_key_redacted("auth.superuser_password").unwrap(),
            "***"
        );
        // An empty password is shown as empty, not masked.
        assert_eq!(Config::default().to_redacted_json()["auth"]["superuser_password"], "");
    }

    #[test]
    fn roundtrips_through_toml() {
        let c = Config::default();
        let text = c.to_toml_string();
        let parsed = Config::from_toml_str(&text).unwrap();
        assert_eq!(c, parsed);
    }
}
