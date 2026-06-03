//! skaidb server entry point (binary name: `skaidb`).
//!
//! Loads configuration (file + CLI + env, SPEC §9), then will bring up the
//! storage engine, query engine, cluster membership, and the SCP/REST
//! listeners. Subsystem wiring lands in later phases; this entry point currently
//! resolves configuration and reports the effective settings.

use clap::Parser;
use skaidb_config::Config;

mod cli;

fn main() -> std::process::ExitCode {
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

    // Overlay: CLI flags (which themselves read from env via clap) win.
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
    skaidb_server::run(config)
}
