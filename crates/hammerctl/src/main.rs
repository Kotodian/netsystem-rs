//! hammerctl — CLI client for hammer daemon

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use hammer_ipc::binary_api::BinaryApiClient;

/// Default Binary API Unix socket path, matching the convention documented
/// in the example daemon config (`examples/tun-tcp-echo.toml`).
const DEFAULT_SOCKET: &str = "/tmp/hammer-tcp-integration.binary-api.sock";

#[derive(Parser, Debug)]
#[command(
    name = "hammerctl",
    version,
    about = "hammerctl — CLI for hammer daemon"
)]
struct Cli {
    /// Binary API Unix socket path
    #[arg(long = "socket", default_value = DEFAULT_SOCKET, global = true)]
    socket: PathBuf,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send one Binary API request and print the reply
    Send {
        /// Binary API method name
        method: String,
        /// Request payload as hex; omitted or empty sends an empty payload
        #[arg(default_value = "")]
        payload_hex: String,
    },
}

fn main() -> ExitCode {
    hammer_infra::main_heap::init_default().unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
        std::process::exit(1);
    });
    let cli = Cli::parse();
    match &cli.cmd {
        Command::Send {
            method,
            payload_hex,
        } => {
            let payload = match decode_payload(payload_hex) {
                Ok(payload) => payload,
                Err(error) => {
                    eprintln!("Invalid hex payload: {error}");
                    return ExitCode::FAILURE;
                }
            };
            let mut client = match BinaryApiClient::connect(&cli.socket) {
                Ok(client) => client,
                Err(error) => {
                    eprintln!("Failed to {error}");
                    return ExitCode::FAILURE;
                }
            };
            match client.call(method, &payload) {
                Ok(reply) => println!("{}", format_reply(&reply)),
                Err(error) => {
                    eprintln!("Binary API call `{method}` failed: {error}");
                    return ExitCode::FAILURE;
                }
            }
            ExitCode::SUCCESS
        }
    }
}

fn decode_payload(payload_hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if payload_hex.is_empty() {
        return Ok(Vec::new());
    }
    hex::decode(payload_hex)
}

fn format_reply(payload: &[u8]) -> String {
    match std::str::from_utf8(payload) {
        Ok(text) => text.to_owned(),
        Err(_) => format!("<binary response, {} bytes>", payload.len()),
    }
}
