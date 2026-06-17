//! Command-line surface for the skaidb server.
//!
//! Every configuration option (SPEC §9.1) is exposed as a CLI flag that also
//! reads from an environment variable. Flags are `Option`s: a flag left unset
//! does not touch the value loaded from the config file (or the default),
//! giving precedence CLI args > env vars > config file > defaults.

use clap::Parser;
use skaidb_config::{Config, Consistency, InternodeAuth, KekSource, NodeRole};

#[derive(Debug, Parser)]
#[command(name = "skaidb", version, about = "skaidb database server")]
pub struct Cli {
    /// Path to a TOML config file. Unspecified options fall back to defaults.
    #[arg(long, env = "SKAIDB_CONFIG")]
    pub config: Option<String>,

    /// Print the effective configuration as TOML and exit.
    #[arg(long)]
    pub print_config: bool,

    // ---- [server] ----
    #[arg(long, env = "SKAIDB_BIND_ADDR")]
    pub bind_addr: Option<String>,
    #[arg(long, env = "SKAIDB_QUIC_PORT")]
    pub quic_port: Option<u16>,
    #[arg(long, env = "SKAIDB_REST_PORT")]
    pub rest_port: Option<u16>,
    #[arg(long, value_enum, ignore_case = true, env = "SKAIDB_NODE_ROLE")]
    pub node_role: Option<NodeRole>,
    #[arg(long, env = "SKAIDB_DATA_DIR")]
    pub data_dir: Option<String>,

    // ---- [cluster] ----
    #[arg(long, env = "SKAIDB_SEEDS", value_delimiter = ',')]
    pub seeds: Option<Vec<String>>,
    #[arg(long, env = "SKAIDB_INTERNODE_PORT")]
    pub internode_port: Option<u16>,
    #[arg(long, env = "SKAIDB_REPLICATION_FACTOR")]
    pub replication_factor: Option<u32>,
    #[arg(long, env = "SKAIDB_VNODES_PER_NODE")]
    pub vnodes_per_node: Option<u32>,
    #[arg(
        long,
        value_enum,
        ignore_case = true,
        env = "SKAIDB_DEFAULT_READ_CONSISTENCY"
    )]
    pub default_read_consistency: Option<Consistency>,
    #[arg(
        long,
        value_enum,
        ignore_case = true,
        env = "SKAIDB_DEFAULT_WRITE_CONSISTENCY"
    )]
    pub default_write_consistency: Option<Consistency>,
    #[arg(long, env = "SKAIDB_ANTI_ENTROPY_INTERVAL_SECS")]
    pub anti_entropy_interval_secs: Option<u64>,

    // ---- [agent] ----
    #[arg(long, env = "SKAIDB_SUBSET_TABLES", value_delimiter = ',')]
    pub subset_tables: Option<Vec<String>>,
    #[arg(long, env = "SKAIDB_MAX_STALENESS_MS")]
    pub max_staleness_ms: Option<u64>,

    // ---- [auth] ----
    #[arg(long, env = "SKAIDB_SCRAM_ENABLED")]
    pub scram_enabled: Option<bool>,
    #[arg(long, env = "SKAIDB_X509_ENABLED")]
    pub x509_enabled: Option<bool>,
    #[arg(long, env = "SKAIDB_X509_CA_FILE")]
    pub x509_ca_file: Option<String>,
    #[arg(long, value_enum, ignore_case = true, env = "SKAIDB_INTERNODE_AUTH")]
    pub internode_auth: Option<InternodeAuth>,
    #[arg(long, env = "SKAIDB_INTERNODE_TOKEN")]
    pub internode_token: Option<String>,
    #[arg(long, env = "SKAIDB_INTERNODE_KEYFILE")]
    pub internode_keyfile: Option<String>,
    #[arg(long, env = "SKAIDB_INTERNODE_TLS_CERT")]
    pub internode_tls_cert: Option<String>,
    #[arg(long, env = "SKAIDB_INTERNODE_TLS_KEY")]
    pub internode_tls_key: Option<String>,
    #[arg(long, env = "SKAIDB_INTERNODE_TLS_CA")]
    pub internode_tls_ca: Option<String>,
    #[arg(long, env = "SKAIDB_SUPERUSER")]
    pub superuser: Option<String>,
    #[arg(long, env = "SKAIDB_SUPERUSER_PASSWORD")]
    pub superuser_password: Option<String>,

    // ---- [encryption] ----
    #[arg(long, env = "SKAIDB_TLS_CERT_FILE")]
    pub tls_cert_file: Option<String>,
    #[arg(long, env = "SKAIDB_TLS_KEY_FILE")]
    pub tls_key_file: Option<String>,
    #[arg(long, env = "SKAIDB_AT_REST_ENABLED")]
    pub at_rest_enabled: Option<bool>,
    #[arg(long, value_enum, ignore_case = true, env = "SKAIDB_AT_REST_KEK_SOURCE")]
    pub at_rest_kek_source: Option<KekSource>,
    #[arg(long, env = "SKAIDB_AT_REST_KEYFILE")]
    pub at_rest_keyfile: Option<String>,

    // ---- [storage] ----
    #[arg(long, env = "SKAIDB_MEMTABLE_SIZE_MB")]
    pub memtable_size_mb: Option<u64>,
    #[arg(long, env = "SKAIDB_COMPACTION_STRATEGY")]
    pub compaction_strategy: Option<String>,
    #[arg(long, env = "SKAIDB_USE_IO_URING")]
    pub use_io_uring: Option<bool>,

    // ---- [observability] ----
    #[arg(long, env = "SKAIDB_PROMETHEUS_PORT")]
    pub prometheus_port: Option<u16>,
    #[arg(long, env = "SKAIDB_SLOW_QUERY_MS")]
    pub slow_query_ms: Option<u64>,
    #[arg(long, env = "SKAIDB_QUERY_LOG_ENABLED")]
    pub query_log_enabled: Option<bool>,
    #[arg(long, env = "SKAIDB_QUERY_LOG_MASKED")]
    pub query_log_masked: Option<bool>,
    #[arg(long, env = "SKAIDB_LOGIN_LOG_ENABLED")]
    pub login_log_enabled: Option<bool>,
    #[arg(long, env = "SKAIDB_ERROR_LOG_LEVEL")]
    pub error_log_level: Option<String>,
    #[arg(long, env = "SKAIDB_PER_TABLE_METRICS")]
    pub per_table_metrics: Option<bool>,
    #[arg(long, env = "SKAIDB_LOG_FORMAT")]
    pub log_format: Option<String>,
}

/// For each `field => target` pair, copy `self.field` onto `target` when the
/// CLI/env flag was provided (`Some`), leaving `None` fields untouched.
macro_rules! overlay {
    ($src:expr, $( $field:ident => $target:expr ),+ $(,)?) => {
        $(
            if let Some(v) = $src.$field.clone() {
                $target = v;
            }
        )+
    };
}

impl Cli {
    /// Overlay any explicitly-provided CLI/env values onto `config`.
    pub fn apply_overrides(&self, config: &mut Config) {
        overlay!(self,
            bind_addr => config.server.bind_addr,
            quic_port => config.server.quic_port,
            rest_port => config.server.rest_port,
            node_role => config.server.node_role,
            data_dir => config.server.data_dir,

            seeds => config.cluster.seeds,
            internode_port => config.cluster.internode_port,
            replication_factor => config.cluster.replication_factor,
            vnodes_per_node => config.cluster.vnodes_per_node,
            default_read_consistency => config.cluster.default_read_consistency,
            default_write_consistency => config.cluster.default_write_consistency,
            anti_entropy_interval_secs => config.cluster.anti_entropy_interval_secs,

            subset_tables => config.agent.subset_tables,
            max_staleness_ms => config.agent.max_staleness_ms,

            scram_enabled => config.auth.scram_enabled,
            x509_enabled => config.auth.x509_enabled,
            x509_ca_file => config.auth.x509_ca_file,
            internode_auth => config.auth.internode_auth,
            internode_token => config.auth.internode_token,
            internode_keyfile => config.auth.internode_keyfile,
            internode_tls_cert => config.auth.internode_tls_cert,
            internode_tls_key => config.auth.internode_tls_key,
            internode_tls_ca => config.auth.internode_tls_ca,
            superuser => config.auth.superuser,
            superuser_password => config.auth.superuser_password,

            tls_cert_file => config.encryption.tls_cert_file,
            tls_key_file => config.encryption.tls_key_file,
            at_rest_enabled => config.encryption.at_rest_enabled,
            at_rest_kek_source => config.encryption.at_rest_kek_source,
            at_rest_keyfile => config.encryption.at_rest_keyfile,

            memtable_size_mb => config.storage.memtable_size_mb,
            compaction_strategy => config.storage.compaction_strategy,
            use_io_uring => config.storage.use_io_uring,

            prometheus_port => config.observability.prometheus_port,
            slow_query_ms => config.observability.slow_query_ms,
            query_log_enabled => config.observability.query_log_enabled,
            query_log_masked => config.observability.query_log_masked,
            login_log_enabled => config.observability.login_log_enabled,
            error_log_level => config.observability.error_log_level,
            per_table_metrics => config.observability.per_table_metrics,
            log_format => config.observability.log_format,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The enum flags accept any letter case (docs and the TOML config use
    /// upper-case `ALL`/`QUORUM`/`ONE`; the CLI must too).
    #[test]
    fn enum_flags_are_case_insensitive() {
        for v in ["ALL", "all", "All"] {
            let cli = Cli::try_parse_from(["skaidb", "--default-read-consistency", v]).unwrap();
            assert_eq!(cli.default_read_consistency, Some(Consistency::All));
        }
        let cli = Cli::try_parse_from([
            "skaidb",
            "--default-write-consistency",
            "QUORUM",
            "--node-role",
            "AGENT",
        ])
        .unwrap();
        assert_eq!(cli.default_write_consistency, Some(Consistency::Quorum));
        assert_eq!(cli.node_role, Some(NodeRole::Agent));
    }
}
