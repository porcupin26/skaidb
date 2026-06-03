//! skaidb interactive SQL client (placeholder).
//!
//! Wires up to the driver in a later phase; for now it only parses args so the
//! binary builds and the CLI surface is reserved.
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "skaidb-cli", version, about = "skaidb SQL client")]
struct Cli {
    /// Address of a skaidb node to connect to.
    #[arg(long, default_value = "127.0.0.1:7000")]
    host: String,
}

fn main() {
    let cli = Cli::parse();
    println!(
        "skaidb-cli: client not yet implemented (target host {})",
        cli.host
    );
}
