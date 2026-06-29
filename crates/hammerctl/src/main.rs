//! hammerctl — CLI client for hammer daemon

use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    /// Send a raw IPC command
    Send {
        /// Handler name
        name: String,
        /// Payload as hex string
        #[arg(default_value = "")]
        payload: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();

    let mut stream = match TcpStream::connect(&cli.addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {}: {e}", cli.addr);
            std::process::exit(1);
        }
    };

    let (name, payload): (&str, Vec<u8>) = match &cli.cmd {
        Command::Pause => ("pause", Vec::new()),
        Command::Wake => ("wake", Vec::new()),
        Command::ResetNetwork => ("reset_network", Vec::new()),
        Command::Shutdown => ("shutdown", Vec::new()),
        Command::Status => ("status", Vec::new()),
        Command::Send { name, payload } => {
            let bytes = if payload.is_empty() {
                Vec::new()
            } else {
                hex::decode(payload).unwrap_or_else(|e| {
                    eprintln!("Invalid hex payload: {e}");
                    std::process::exit(1);
                })
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
            if data.is_empty() {
                println!("OK");
            } else {
                match std::str::from_utf8(&data) {
                    Ok(s) => println!("{s}"),
                    Err(_) => println!("<binary response, {} bytes>", data.len()),
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
