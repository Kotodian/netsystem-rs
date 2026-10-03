//! hammer — VPP-clone daemon

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::OnceLock;

use hammer_runtime::global_main::GlobalMain;
use hammer_runtime::log::Level;
#[cfg(target_os = "linux")]
use hammer_runtime::unix_cli::UnixCliMain;
use hammer_runtime::{
    DataPlaneMain, PluginMain, RuntimeError, RuntimeResult, ThreadMain, UnixMain,
};

// Shared device/interface/transport/session infrastructure contributes host
// builtins; loadable protocol and device-driver code comes only from DSOs.
use hammer_service as _;

mod cli;

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    num_workers_change_functions = [];
    api_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [];
    stats_registrations = [];
    cli_commands = [
        cli::runtime::__CLI_COMMAND_RUNTIME,
        cli::runtime::__CLI_COMMAND_CLEAR_RUNTIME,
        cli::errors::__CLI_COMMAND_ERRORS,
        cli::errors::__CLI_COMMAND_CLEAR_ERRORS,
        cli::memory::__CLI_COMMAND_MEMORY,
        cli::buffer::__CLI_COMMAND_BUFFER,
    ];
);

static STARTUP_CONFIG_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Fields that must exist before the Main Heap can be published. Unknown
/// sections are deliberately ignored here; their owning registration parses
/// them later on the initialized heap.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonEarlyConfig {
    memory: hammer_infra::mem::MainHeapConfig,
    log: DaemonLogConfig,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonLogConfig {
    level: Level,
}

impl DaemonEarlyConfig {
    fn validate(&self) -> Result<(), hammer_infra::mem::MemError> {
        self.memory.validate()
    }
}

/// Daemon-owned process options. Plugin schemas are not part of this type.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonStartupConfig {
    plugins: Vec<String>,
    cli: DaemonCliConfig,
}

#[derive(Debug, serde::Deserialize)]
#[serde(default)]
struct DaemonCliConfig {
    socket: PathBuf,
}

impl Default for DaemonCliConfig {
    fn default() -> Self {
        Self {
            socket: PathBuf::from("/run/hammer/cli.sock"),
        }
    }
}

fn main() {
    let config_path = config_path_from_args();
    let config_document = read_config(&config_path).unwrap_or_else(|error| {
        eprintln!("Failed to read config {}: {error}", config_path.display());
        exit_daemon(1);
    });
    let early: DaemonEarlyConfig = toml::from_str(&config_document).unwrap_or_else(|error| {
        eprintln!(
            "Failed to deserialize early config {}: {error}",
            config_path.display()
        );
        exit_daemon(1);
    });
    early.validate().unwrap_or_else(|error| {
        eprintln!("Invalid early config {}: {error}", config_path.display());
        exit_daemon(1);
    });
    let DaemonEarlyConfig { memory, log } = early;
    let log_level = log.level;
    drop(config_document);
    drop(config_path);
    memory.initialize().unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
        exit_daemon(1);
    });
    let config_path = config_path_from_args();
    install_tracing(log_level).unwrap_or_else(|error| {
        eprintln!("Failed to initialize logging: {error}");
        exit_daemon(1);
    });

    let config = read_config(&config_path).unwrap_or_else(|error| {
        eprintln!(
            "Failed to read config {} on the main heap: {error}",
            config_path.display()
        );
        exit_daemon(1);
    });
    let startup = parse_startup_config(&config).unwrap_or_else(|error| {
        eprintln!(
            "Failed to deserialize daemon config {}: {error}",
            config_path.display()
        );
        exit_daemon(1);
    });
    if STARTUP_CONFIG_PATH.set(config_path.clone()).is_err() {
        eprintln!("startup configuration path was initialized more than once");
        exit_daemon(1);
    }

    let status = run(config, startup, config_path, log_level).unwrap_or_else(|error| {
        tracing::error!(%error, "hammer runtime failed");
        1
    });
    exit_daemon(status);
}

fn exit_daemon(status: i32) -> ! {
    // Main Heap publication deliberately does not classify allocations made
    // before interception. Rust runtime cleanup would free those System
    // allocations through the active Main Heap, so terminate without teardown.
    unsafe { libc::_exit(status) }
}

fn install_tracing(default_level: Level) -> Result<(), String> {
    let directive = match std::env::var("HAMMER_LOG") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => match std::env::var("RUST_LOG") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => match default_level {
                Level::Panic | Level::Fatal | Level::Error => "error".to_owned(),
                Level::Warn => "warn".to_owned(),
                Level::Info => "info".to_owned(),
                Level::Debug => "debug".to_owned(),
                Level::Trace => "trace".to_owned(),
            },
            Err(error) => return Err(format!("RUST_LOG is not valid Unicode: {error}")),
        },
        Err(error) => return Err(format!("HAMMER_LOG is not valid Unicode: {error}")),
    };
    let filter = tracing_subscriber::EnvFilter::try_new(&directive)
        .map_err(|error| format!("invalid log directive `{directive}`: {error}"))?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_names(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| format!("install global tracing subscriber: {error}"))
}

fn config_path_from_args() -> PathBuf {
    std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("Usage: hammer <config.toml>");
            exit_daemon(1);
        })
}

fn run(
    config: String,
    startup: DaemonStartupConfig,
    config_path: PathBuf,
    log_level: Level,
) -> RuntimeResult<i32> {
    let mut unix = UnixMain::new(config_path, log_level);
    let argv = std::env::args().collect::<Vec<_>>();
    let exec_path = argv.first().cloned().unwrap_or_default();
    let mut global = GlobalMain::new("hammer".to_owned(), exec_path, argv, config.clone());
    let mut plugins = PluginMain::default();
    plugins.register_image(&__HAMMER_REGISTRATION_IMAGE);
    plugins.register_image(hammer_service::registration_image());
    plugins.load(env!("CARGO_PKG_VERSION"), &startup.plugins)?;
    plugins.register_global_declarations(&mut global);

    let mut threads = ThreadMain::new()?;
    hammer_runtime::init::run_config_functions(&global, None, true, &config)?;
    threads.configure()?;
    let main = Rc::new(RefCell::new(DataPlaneMain::new_main(&threads)?));
    let future = run_main_thread(&mut unix, Rc::clone(&main), &startup.cli.socket);
    let run_result = hammer_runtime::main_loop::run(global, threads, plugins, &main, future);
    let status = run_result.as_ref().copied().unwrap_or(1);
    let unix_result = unix.shutdown(status);

    run_result?;
    unix_result?;
    Ok(status)
}

async fn run_main_thread(
    unix: &mut UnixMain,
    main: Rc<RefCell<DataPlaneMain>>,
    socket: &Path,
) -> RuntimeResult<i32> {
    #[cfg(target_os = "linux")]
    {
        let cli = UnixCliMain::global();
        cli.listen(socket).await?;
        tracing::info!(socket = %socket.display(), "hammer CLI listening");
        let result = tokio::select! {
            signal = unix.wait_for_exit_signal() => signal,
            accepted = cli.accept(&main) => accepted.map(|()| 1),
        };
        let cleanup = cli
            .close_listener(hammer_runtime::AsyncFileMain::global())
            .await;
        return match (result, cleanup) {
            (Err(primary), Err(cleanup)) => {
                tracing::error!(%cleanup, "CLI listener cleanup failed after main-thread error");
                Err(primary)
            }
            (Err(primary), _) => Err(primary),
            (_, Err(cleanup)) => Err(cleanup),
            (Ok(status), Ok(())) => Ok(status),
        };
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (&main, &socket);
    tracing::info!("hammer started");
    unix.wait_for_exit_signal().await
}

fn read_config(path: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(path)
}

fn parse_startup_config(document: &str) -> RuntimeResult<DaemonStartupConfig> {
    let config: DaemonStartupConfig = toml::from_str(document)
        .map_err(|error| RuntimeError::config_parse(format!("parse startup TOML: {error}")))?;
    Ok(config)
}

#[derive(Debug, thiserror::Error)]
#[error("startup configuration path is not initialized")]
struct StartupConfigPathUnset;

pub(crate) fn load_current_config() -> std::io::Result<String> {
    let path = STARTUP_CONFIG_PATH
        .get()
        .ok_or_else(|| std::io::Error::other(StartupConfigPathUnset))?;
    read_config(path)
}
