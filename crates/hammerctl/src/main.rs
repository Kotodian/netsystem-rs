//! hammerctl — CLI client for hammer daemon

use clap::{Parser, Subcommand};
use hammer_ipc::{IpcResponse, PluginCommandError, PluginCommandReply};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(
    name = "hammerctl",
    version,
    about = "hammerctl — CLI for hammer daemon"
)]
struct Cli {
    /// IPC TCP address
    #[arg(long = "addr", default_value = "127.0.0.1:7299", global = true)]
    addr: String,

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
    /// Get runtime status
    Status,
    /// Inspect or add runtime plugins
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Send a raw IPC command
    Send {
        /// Handler name
        name: String,
        /// Payload as hex string
        #[arg(default_value = "")]
        payload: String,
    },
}

#[derive(Subcommand, Debug)]
enum PluginCommand {
    /// List activated plugins in load order
    List,
    /// Add plugin roots and their missing dependencies
    Load {
        /// Plugin root names
        #[arg(required = true)]
        roots: Vec<String>,
    },
}

fn main() {
    hammer_infra::main_heap::init_default().unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
        std::process::exit(1);
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("Failed to initialize Tokio runtime: {error}");
            std::process::exit(1);
        });
    runtime.block_on(run());
}

async fn run() {
    let cli = Cli::parse();

    let mut stream = match TcpStream::connect(&cli.addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {}: {e}", cli.addr);
            std::process::exit(1);
        }
    };

    let expects_plugin_reply = matches!(&cli.cmd, Command::Plugin { .. });
    let (name, payload): (&str, Vec<u8>) = match &cli.cmd {
        Command::Pause => ("pause", Vec::new()),
        Command::Wake => ("wake", Vec::new()),
        Command::ResetNetwork => ("reset_network", Vec::new()),
        Command::Shutdown => ("shutdown", Vec::new()),
        Command::Status => ("status", Vec::new()),
        Command::Plugin {
            command: PluginCommand::List,
        } => ("plugin_list", Vec::new()),
        Command::Plugin {
            command: PluginCommand::Load { roots },
        } => {
            let encoded = match bincode::serialize(roots) {
                Ok(encoded) => encoded,
                Err(_) => {
                    eprintln!("Failed to encode plugin roots");
                    std::process::exit(1);
                }
            };
            ("plugin_load", Vec::from(encoded))
        }
        Command::Send { name, payload } => {
            let bytes = if payload.is_empty() {
                Vec::new()
            } else {
                Vec::from(hex::decode(payload).unwrap_or_else(|e| {
                    eprintln!("Invalid hex payload: {e}");
                    std::process::exit(1);
                }))
            };
            (name.as_str(), bytes)
        }
    };

    let request = hammer_ipc::handler::IpcRequest {
        name: name.to_string(),
        payload,
    };

    let data = bincode::serialize(&request).unwrap_or_else(|e| {
        eprintln!("Serialize error: {e}");
        std::process::exit(1);
    });

    hammer_ipc::frame::async_write_frame(&mut stream, &data)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Write error: {e}");
            std::process::exit(1);
        });

    let mut buf = vec![0u8; 65536];
    match hammer_ipc::frame::async_read_frame(&mut stream, &mut buf).await {
        Ok(Some(data)) => {
            let response: IpcResponse = match bincode::deserialize(&data) {
                Ok(response) => response,
                Err(_) => {
                    eprintln!("Invalid daemon response");
                    std::process::exit(1);
                }
            };
            if expects_plugin_reply {
                print_plugin_reply(&response.payload);
            } else if response.payload.is_empty() {
                println!("OK");
            } else {
                match std::str::from_utf8(&response.payload) {
                    Ok(s) => println!("{s}"),
                    Err(_) => println!("<binary response, {} bytes>", response.payload.len()),
                }
            }
        }
        Ok(None) => {
            eprintln!("Connection closed by server");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Read error: {e}");
            std::process::exit(1);
        }
    }
}

fn print_plugin_reply(payload: &[u8]) {
    let reply: PluginCommandReply<'_> = match bincode::deserialize(payload) {
        Ok(reply) => reply,
        Err(_) => {
            eprintln!("Invalid plugin response");
            std::process::exit(1);
        }
    };
    match reply {
        PluginCommandReply::Loaded(names) => {
            for name in names {
                println!("{name}");
            }
        }
        PluginCommandReply::Error(error) => {
            eprintln!("{}", plugin_command_error_message(error));
            std::process::exit(1);
        }
    }
}

fn plugin_command_error_message(error: PluginCommandError) -> &'static str {
    match error {
        PluginCommandError::InvalidRequest => "invalid plugin request",
        PluginCommandError::MemoryNotInitialized => "runtime memory is not initialized",
        PluginCommandError::DuplicateRoot => "plugin root was requested more than once",
        PluginCommandError::DependencyCycle => "plugin dependency cycle",
        PluginCommandError::HostVersionInvalid => "invalid host plugin version",
        PluginCommandError::RequiredVersionInvalid => "invalid required plugin version",
        PluginCommandError::VersionMismatch => "plugin version requirement is not satisfied",
        PluginCommandError::LibraryOpen => "plugin library could not be opened",
        PluginCommandError::RegistrationSymbol => "plugin registration symbol is unavailable",
        PluginCommandError::RegistrationNull => "plugin registration is null",
        PluginCommandError::RegistrationNameMismatch => "plugin registration name mismatch",
        PluginCommandError::ExecutablePath => "daemon executable path is unavailable",
        PluginCommandError::ExecutableParentMissing => "daemon executable has no plugin directory",
        PluginCommandError::Configuration => "plugin configuration failed",
        PluginCommandError::GraphMaterialization => "plugin graph materialization failed",
        PluginCommandError::WorkerCountOverflow => "configured worker count is unsupported",
        PluginCommandError::WorkerGraphUpdatePending => "worker graph update is already pending",
        PluginCommandError::WorkerGraphUpdate => "worker graph update failed",
        PluginCommandError::Lifecycle => "plugin lifecycle failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_plugin_commands() {
        let list = Cli::try_parse_from(["hammerctl", "plugin", "list"]).expect("plugin list");
        assert!(matches!(
            list.cmd,
            Command::Plugin {
                command: PluginCommand::List
            }
        ));

        let load = Cli::try_parse_from(["hammerctl", "plugin", "load", "tcp", "udp"])
            .expect("plugin load");
        assert!(matches!(
            load.cmd,
            Command::Plugin {
                command: PluginCommand::Load { roots }
            } if roots.as_slice() == ["tcp", "udp"]
        ));
    }
}
