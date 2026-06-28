//! hammerctl - CLI client for hammer daemon

use clap::{Parser, Subcommand};
use hammer_ipc::{IpcClient, IpcReply, IpcRequest, MetricsFormat};

#[derive(Parser, Debug)]
#[command(
    name = "hammerctl",
    version,
    about = "hammerctl - CLI for hammer daemon"
)]
struct Cli {
    /// IPC socket path
    #[arg(long = "sock", default_value = "/run/hammer.sock", global = true)]
    sock: String,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Pause the dataplane
    Pause,
    /// Wake the dataplane
    Wake,
    /// Reset network state
    #[command(name = "reset-network")]
    ResetNetwork,
    /// Shutdown the daemon
    Shutdown,
    /// Get metrics snapshot
    Metrics {
        /// Output format
        #[arg(long = "format", default_value = "table")]
        format: MetricsFormatArg,
    },
    /// Get runtime status
    Status,
    /// List listeners
    #[command(name = "list-listeners")]
    ListListeners,
    /// List sessions
    #[command(name = "list-sessions")]
    ListSessions,
    /// Reload configuration
    #[command(name = "config")]
    Config {
        #[command(subcommand)]
        sub: ConfigSub,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum MetricsFormatArg {
    Table,
    Json,
    Prometheus,
}

#[derive(Subcommand, Debug)]
enum ConfigSub {
    /// Reload config from file or stdin
    Reload {
        /// Path to TOML config (or - for stdin)
        path: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let mut client = match IpcClient::connect(&cli.sock) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", cli.sock, e);
            std::process::exit(1);
        }
    };

    let req = match &cli.cmd {
        Command::Pause => IpcRequest::Pause,
        Command::Wake => IpcRequest::Wake,
        Command::ResetNetwork => IpcRequest::ResetNetwork,
        Command::Shutdown => IpcRequest::Shutdown,
        Command::Metrics { format } => {
            let f = match format {
                MetricsFormatArg::Table => MetricsFormat::Table,
                MetricsFormatArg::Json => MetricsFormat::Json,
                MetricsFormatArg::Prometheus => MetricsFormat::Prometheus,
            };
            IpcRequest::Metrics { format: f }
        }
        Command::Status => IpcRequest::Status,
        Command::ListListeners => IpcRequest::ListListeners,
        Command::ListSessions => IpcRequest::ListSessions,
        Command::Config { sub } => match sub {
            ConfigSub::Reload { path } => {
                let toml = if path == "-" {
                    use std::io::Read;
                    let mut buf = String::new();
                    std::io::stdin().read_to_string(&mut buf).unwrap();
                    buf
                } else {
                    std::fs::read_to_string(&path).unwrap_or_else(|e| {
                        eprintln!("Failed to read {}: {}", path, e);
                        std::process::exit(1);
                    })
                };
                IpcRequest::ConfigReload { toml }
            }
        },
    };

    let reply = match client.request(req) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Request failed: {}", e);
            std::process::exit(1);
        }
    };

    print_reply(&reply);
}

fn print_reply(reply: &IpcReply) {
    match reply {
        IpcReply::Ok => println!("OK"),
        IpcReply::Error(e) => eprintln!("Error: {}", e),
        IpcReply::Metrics(data) => {
            if let Ok(s) = String::from_utf8(data.clone()) {
                println!("{}", s);
            } else {
                println!("<binary metrics data, {} bytes>", data.len());
            }
        }
        IpcReply::Status(s) => {
            println!("Running: {}", s.running);
            println!("Workers: {}", s.n_workers);
            println!("Sessions: {}", s.n_sessions);
            println!("Uptime: {}s", s.uptime_secs);
        }
        IpcReply::Listeners(list) => {
            for l in list {
                println!("  {} {}:{} (id={})", l.protocol, l.address, l.port, l.id);
            }
        }
        IpcReply::Sessions(list) => {
            for s in list {
                println!(
                    "  {} {} {} -> {} (id={})",
                    s.protocol, s.state, s.local_addr, s.remote_addr, s.id
                );
            }
        }
    }
}
