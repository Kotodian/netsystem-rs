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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_generic_send_with_optional_hex_payload() {
        let bare = Cli::try_parse_from(["hammerctl", "send", "show_version"]).expect("send");
        let Command::Send {
            method,
            payload_hex,
        } = bare.cmd
        else {
            panic!("expected send")
        };
        assert_eq!(method, "show_version");
        assert_eq!(payload_hex, "");

        let hex = Cli::try_parse_from(["hammerctl", "send", "method", "00ff10"]).expect("send");
        let Command::Send {
            method,
            payload_hex,
        } = hex.cmd
        else {
            panic!("expected send")
        };
        assert_eq!(method, "method");
        assert_eq!(payload_hex, "00ff10");
    }

    #[test]
    fn socket_flag_is_global() {
        let before = Cli::try_parse_from(["hammerctl", "--socket", "/tmp/a.sock", "send", "m"])
            .expect("socket before subcommand");
        assert_eq!(before.socket, PathBuf::from("/tmp/a.sock"));

        let after = Cli::try_parse_from(["hammerctl", "send", "m", "--socket", "/tmp/b.sock"])
            .expect("socket after subcommand");
        assert_eq!(after.socket, PathBuf::from("/tmp/b.sock"));

        let default = Cli::try_parse_from(["hammerctl", "send", "m"]).expect("default socket");
        assert_eq!(default.socket, PathBuf::from(DEFAULT_SOCKET));
    }

    #[test]
    fn decodes_empty_and_hex_payloads() {
        assert_eq!(decode_payload("").unwrap(), Vec::<u8>::new());
        assert_eq!(decode_payload("00ff10").unwrap(), vec![0x00, 0xff, 0x10]);
        assert!(decode_payload("zz").is_err());
    }

    #[test]
    fn formats_utf8_reply_and_binary_byte_count() {
        assert_eq!(format_reply(b"hello"), "hello");
        assert_eq!(format_reply(b"\xff\x00"), "<binary response, 2 bytes>");
        assert_eq!(format_reply(b""), "");
    }
}
