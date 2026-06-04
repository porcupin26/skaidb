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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum InternodeAuth {
    Keyfile,
    X509,
}

/// Source of the key-encryption key for at-rest encryption (SPEC §8.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum KekSource {
    Keyfile,
    Kms,
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub quic_port: u16,
    pub rest_port: u16,
    pub node_role: NodeRole,
    pub data_dir: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub seeds: Vec<String>,
    pub replication_factor: u32,
    pub vnodes_per_node: u32,
    pub default_read_consistency: Consistency,
    pub default_write_consistency: Consistency,
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
    pub internode_auth: InternodeAuth,
    pub internode_keyfile: String,
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
    pub at_rest_enabled: bool,
    pub at_rest_kek_source: KekSource,
    pub at_rest_keyfile: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub memtable_size_mb: u64,
    pub compaction_strategy: String,
    pub use_io_uring: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub prometheus_port: u16,
    pub slow_query_ms: u64,
    pub query_log_enabled: bool,
    pub query_log_masked: bool,
    pub login_log_enabled: bool,
    pub error_log_level: String,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: "127.0.0.1".to_string(),
            quic_port: 7000,
            rest_port: 7080,
            node_role: NodeRole::Member,
            data_dir: "/var/lib/skaidb".to_string(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            seeds: Vec::new(),
            replication_factor: 3,
            vnodes_per_node: 256,
            default_read_consistency: Consistency::Quorum,
            default_write_consistency: Consistency::Quorum,
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
            internode_auth: InternodeAuth::Keyfile,
            internode_keyfile: String::new(),
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
            at_rest_enabled: false,
            at_rest_kek_source: KekSource::Keyfile,
            at_rest_keyfile: String::new(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            memtable_size_mb: 256,
            compaction_strategy: "lazy_leveled".to_string(),
            use_io_uring: true,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        ObservabilityConfig {
            prometheus_port: 9090,
            slow_query_ms: 200,
            query_log_enabled: true,
            query_log_masked: true,
            login_log_enabled: true,
            error_log_level: "warn".to_string(),
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
    fn roundtrips_through_toml() {
        let c = Config::default();
        let text = c.to_toml_string();
        let parsed = Config::from_toml_str(&text).unwrap();
        assert_eq!(c, parsed);
    }
}
