//! skaidb server entry point (binary name: `skaidb`).
//!
//! Loads configuration (file + CLI + env, SPEC §9), then will bring up the
//! storage engine, query engine, cluster membership, and the SCP/REST
//! listeners. Subsystem wiring lands in later phases; this entry point currently
//! resolves configuration and reports the effective settings.

use clap::Parser;
use skaidb_config::Config;

// jemalloc as the global allocator: glibc's default allocator retains freed
// memory instead of returning it to the OS, so bulk-load + index-build churn
// ratcheted RSS to the cgroup limit and OOM-looped small nodes even after the
// data was flushed/freed. jemalloc reclaims and fragments far better.
// Aggressive page return to the OS (`_RJEM_MALLOC_CONF=background_thread:true,
// dirty_decay_ms:1000,muzzy_decay_ms:1000`) is set in the systemd unit rather
// than a `#[export_name]` symbol, which the workspace's `forbid(unsafe_code)`
// rejects. jemalloc's defaults already return memory far better than glibc.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cli;

/// Give memory-pressure logs jemalloc's allocated/resident/retained split, so
/// "the node is at its limit" is attributable to live heap vs pages the
/// allocator hasn't returned yet. Only the binary knows which allocator is in
/// use, so the hook is registered here.
#[cfg(not(target_env = "msvc"))]
fn register_alloc_stats() {
    skaidb_cluster::memguard::set_alloc_stats_hook(Box::new(|| {
        use tikv_jemalloc_ctl::{epoch, stats};
        epoch::advance().ok()?; // refresh the stats snapshot
        Some(skaidb_cluster::memguard::AllocStats {
            allocated: stats::allocated::read().ok()? as u64,
            resident: stats::resident::read().ok()? as u64,
            retained: stats::retained::read().ok()? as u64,
        })
    }));
}

#[cfg(target_env = "msvc")]
fn register_alloc_stats() {}

fn main() -> std::process::ExitCode {
    register_alloc_stats();
    let args = cli::Cli::parse();
    match run(args) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("skaidb: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run(args: cli::Cli) -> Result<(), Box<dyn std::error::Error>> {
    // Base layer: config file if provided, otherwise built-in defaults.
    let mut config = match &args.config {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };

    // Overlay: SKAIDB_INFERENCE_* environment overrides (secrets/endpoints
    // kept out of the shared config file), then CLI flags (which themselves
    // read from env via clap) win.
    config = config.with_env_overrides()?;
    args.apply_overrides(&mut config);

    if args.print_config {
        print!("{}", config.to_toml_string());
        return Ok(());
    }

    println!(
        "skaidb {} starting as {:?} (data_dir={})",
        env!("CARGO_PKG_VERSION"),
        config.server.node_role,
        config.server.data_dir,
    );
    skaidb_server::run(config, args.config.clone())
}
