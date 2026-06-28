//! hammer - cross-platform VPP clone daemon

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "hammer", version, about = "hammer - cross-platform VPP clone")]
struct Args {
    /// Path to TOML startup config
    #[arg(short = 'c', long = "config")]
    config: Option<String>,

    /// Run as daemon (background)
    #[arg(long = "daemon")]
    daemon: bool,

    /// Interactive mode (CLI on stdin)
    #[arg(short = 'i', long = "interactive")]
    interactive: bool,

    /// IPC socket path
    #[arg(long = "sock", default_value = "/run/hammer.sock")]
    sock: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long = "log-level", default_value = "info")]
    log_level: String,

    /// Print version and exit
    #[arg(short = 'v', long = "version")]
    version: bool,
}

fn main() {
    let args = Args::parse();

    if args.version {
        println!("hammer {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    // Initialize tracing
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tracing::info!("hammer starting (stub)");
    println!(
        "hammer: stub - config={:?}, daemon={}, sock={}",
        args.config, args.daemon, args.sock
    );
}
