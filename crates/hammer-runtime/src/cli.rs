//! Main-thread CLI command directory and command result formatting.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::DataPlaneMain;
use crate::error::RuntimeResult;
use crate::plugin::{PluginError, PluginMain};

pub type CliCommandFn =
    fn(&mut DataPlaneMain, &str) -> Result<JoinHandle<Result<String, CliError>>, CliError>;

#[derive(Clone, Copy)]
pub struct CliCommandRegistration {
    pub path: &'static str,
    pub short_help: &'static str,
    pub long_help: &'static str,
    pub mp_safe: bool,
    pub start: CliCommandFn,
}

pub struct CliMain {
    commands: Vec<CliCommandRegistration>,
    command_index_by_path: HashMap<&'static str, usize>,
}

static CLI_MAIN: OnceLock<CliMain> = OnceLock::new();

impl CliMain {
    pub fn init() -> Result<&'static Self, CliError> {
        crate::ensure_main_thread().expect("CLI directory initializes on thread zero");
        if let Some(main) = CLI_MAIN.get() {
            return Ok(main);
        }
        let mut main = Self {
            commands: Vec::new(),
            command_index_by_path: HashMap::new(),
        };
        let plugins = PluginMain::global().map_err(|source| CliError::PluginCatalog { source })?;
        for image in plugins.registration_images() {
            for command in image.cli_commands() {
                main.register(*command)?;
            }
        }
        assert!(CLI_MAIN.set(main).is_ok(), "CLI directory publishes once");
        Ok(CLI_MAIN.get().expect("CLI directory was just published"))
    }

    #[inline]
    pub fn global() -> &'static Self {
        CLI_MAIN
            .get()
            .expect("CLI directory initializes before command dispatch")
    }

    pub fn register(&mut self, command: CliCommandRegistration) -> Result<(), CliError> {
        if self.command_index_by_path.contains_key(command.path) {
            return Err(CliError::DuplicateCommand { path: command.path });
        }
        let index = self.commands.len();
        self.commands.push(command);
        self.command_index_by_path.insert(command.path, index);
        Ok(())
    }

    pub fn dispatch(
        &self,
        runtime: &mut DataPlaneMain,
        line: &str,
    ) -> Result<JoinHandle<Result<String, CliError>>, CliError> {
        let line = line.trim_start();
        let command = self
            .commands
            .iter()
            .filter_map(|command| {
                let arguments = line.strip_prefix(command.path)?;
                if !arguments.is_empty()
                    && !arguments.chars().next().is_some_and(char::is_whitespace)
                {
                    return None;
                }
                Some((command, arguments.trim_start()))
            })
            .max_by_key(|(command, _)| command.path.len())
            .ok_or_else(|| CliError::UnknownCommand {
                input: line.to_owned(),
            })?;
        if command.0.mp_safe {
            (command.0.start)(runtime, command.1)
        } else {
            crate::worker_thread_barrier_sync!({ (command.0.start)(runtime, command.1) })
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("duplicate CLI command: {path}")]
    DuplicateCommand { path: &'static str },
    #[error("unknown CLI command: {input}")]
    UnknownCommand { input: String },
    #[error("unexpected CLI argument: {argument}")]
    UnexpectedArgument { argument: String },
    #[error("invalid CLI argument: {argument}")]
    InvalidArgument { argument: String },
    #[error("CLI input is not UTF-8")]
    InputEncoding {
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("create CLI socket directory: {path}")]
    SocketDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("bind CLI socket: {path}")]
    SocketBind {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("configure CLI socket: {path}")]
    SocketConfigure {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("remove CLI socket: {path}")]
    SocketRemove {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("register CLI File")]
    FileRegister {
        #[source]
        source: Box<crate::RuntimeError>,
    },
    #[error("start CLI Process")]
    ProcessStart {
        #[source]
        source: Box<crate::RuntimeError>,
    },
    #[error("load CLI command declarations")]
    PluginCatalog {
        #[source]
        source: PluginError,
    },
}

#[hammer_component_macros::init_function(name = "cli_main_init")]
fn init_cli_main(_: &mut DataPlaneMain) -> RuntimeResult<()> {
    CliMain::init()?;
    Ok(())
}

struct VersionArgs;

impl FromStr for VersionArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::UnexpectedArgument {
                argument: input.trim().to_owned(),
            });
        }
        Ok(Self)
    }
}

#[hammer_component_macros::cli_command(
    path = "show version",
    args = VersionArgs,
    short_help = "show version",
    mp_safe = true,
)]
async fn show_version(_: VersionArgs) -> Result<String, CliError> {
    Ok(format!("hammer v{}\n", env!("CARGO_PKG_VERSION")))
}

struct WaitArgs {
    duration: Duration,
}

impl FromStr for WaitArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let argument = input.trim();
        let seconds = if argument.is_empty() {
            1.0
        } else {
            argument.parse::<f64>().map_err(|_| CliError::InvalidArgument {
                argument: argument.to_owned(),
            })?
        };
        if !seconds.is_finite()
            || seconds <= 0.0
            || seconds > 86_400.0
            || (seconds * 1_000.0).floor() / 1_000.0 != seconds
        {
            return Err(CliError::InvalidArgument {
                argument: argument.to_owned(),
            });
        }
        Ok(Self {
            duration: Duration::from_secs_f64(seconds),
        })
    }
}

// VPP: vlib/unix/cli.c:3887-3923.
#[hammer_component_macros::cli_command(
    path = "wait",
    args = WaitArgs,
    short_help = "wait <sec>",
    mp_safe = false,
)]
async fn wait(args: WaitArgs) -> Result<String, CliError> {
    tokio::time::sleep(args.duration).await;
    Ok(format!("waited {:.3} sec.\n", args.duration.as_secs_f64()))
}
