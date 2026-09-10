//! hammer — VPP-clone daemon

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use hammer_runtime::attach::AppServer;
use hammer_runtime::config::Memory;
use hammer_runtime::global_main::GlobalMain;
use hammer_runtime::log::Level;
use hammer_runtime::{
    DataPlaneMain, PluginMain, RuntimeError, RuntimeResult, ThreadMain, UnixMain,
};

// Shared device/interface/transport/session infrastructure contributes host
// builtins; loadable protocol and device-driver code comes only from DSOs.
use hammer_service as _;

static STARTUP_CONFIG_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Fields that must exist before the Main Heap can be published. Unknown
/// sections are deliberately ignored here; their owning registration parses
/// them later on the initialized heap.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonEarlyConfig {
    memory: Memory,
    log: DaemonLogConfig,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonLogConfig {
    level: Level,
}

impl DaemonEarlyConfig {
    fn validate(&self) -> RuntimeResult<()> {
        self.memory.validate()
    }
}

/// Daemon-owned process options. Plugin schemas are not part of this type.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonStartupConfig {
    plugins: Vec<String>,
}

fn main() {
    let config_path = config_path_from_args();
    let bootstrap_document = read_config(&config_path).unwrap_or_else(|error| {
        eprintln!("Failed to read config {}: {error}", config_path.display());
        std::process::exit(1);
    });
    let early: DaemonEarlyConfig = toml::from_str(&bootstrap_document).unwrap_or_else(|error| {
        eprintln!(
            "Failed to deserialize early config {}: {error}",
            config_path.display()
        );
        std::process::exit(1);
    });
    early.validate().unwrap_or_else(|error| {
        eprintln!("Invalid early config {}: {error}", config_path.display());
        std::process::exit(1);
    });
    let DaemonEarlyConfig { memory, log } = early;
    let log_level = log.level;
    drop(bootstrap_document);
    memory.ensure_main_heap().unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
        std::process::exit(1);
    });
    install_tracing(log_level).unwrap_or_else(|error| {
        eprintln!("Failed to initialize logging: {error}");
        std::process::exit(1);
    });

    let config = read_config(&config_path).unwrap_or_else(|error| {
        eprintln!(
            "Failed to read config {} on the main heap: {error}",
            config_path.display()
        );
        std::process::exit(1);
    });
    let roots = parse_startup_config(&config).unwrap_or_else(|error| {
        eprintln!(
            "Failed to deserialize daemon config {}: {error}",
            config_path.display()
        );
        std::process::exit(1);
    });
    if STARTUP_CONFIG_PATH.set(config_path.clone()).is_err() {
        eprintln!("startup configuration path was initialized more than once");
        std::process::exit(1);
    }

    let status = run(config, roots, config_path, log_level).unwrap_or_else(|error| {
        tracing::error!(%error, "hammer runtime failed");
        1
    });
    std::process::exit(status);
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
            std::process::exit(1);
        })
}

fn run(
    config: String,
    roots: Vec<String>,
    config_path: PathBuf,
    log_level: Level,
) -> RuntimeResult<i32> {
    let mut unix = UnixMain::new(config_path, log_level);
    let argv = std::env::args().collect::<Vec<_>>();
    let exec_path = argv.first().cloned().unwrap_or_default();
    let mut global = GlobalMain::new("hammer".to_owned(), exec_path, argv, config.clone());
    let mut plugins = PluginMain::default();
    plugins.register_image(hammer_service::registration_image());
    plugins.load(env!("CARGO_PKG_VERSION"), &roots)?;
    plugins.register_global_declarations(&mut global);

    hammer_runtime::init::run_config_functions(&global, None, true, &config)?;
    let mut threads = ThreadMain::new()?;
    threads.configure()?;
    let mut main = DataPlaneMain::new_main(&threads)?;
    let run_result = hammer_runtime::main_loop::run(
        global,
        threads,
        plugins,
        &mut main,
        run_main_thread(&mut unix),
    );
    let status = run_result.as_ref().copied().unwrap_or(1);
    let unix_result = unix.shutdown(status);

    run_result?;
    unix_result?;
    Ok(status)
}

async fn run_main_thread(unix: &mut UnixMain) -> RuntimeResult<i32> {
    let attach_server = hammer_service::session::app_server();
    let applications = hammer_service::session::ApplicationMain::global()?;
    tracing::info!("hammer started");
    let exit_signal = unix.wait_for_exit_signal();
    let attach = serve_applications(attach_server, applications);
    tokio::pin!(exit_signal);
    tokio::pin!(attach);
    loop {
        tokio::select! {
            status = &mut exit_signal => return status,
            result = &mut attach => {
                result?;
                return Err(RuntimeError::service_closed());
            }
        }
    }
}

async fn serve_applications(
    attach_server: Option<Arc<AppServer>>,
    applications: &'static hammer_service::session::ApplicationMain,
) -> RuntimeResult<()> {
    let Some(attach) = attach_server else {
        return std::future::pending::<RuntimeResult<()>>().await;
    };
    let attach_applications = applications;
    let publish_applications = applications;
    let detach_applications = applications;
    attach
        .serve(
            move || attach_applications.attach_external_with_runtime(),
            move |application| publish_applications.application_mq_publication(application),
            move |application, requests, replies| {
                hammer_service::session::runtime::SessionMain::global()?
                    .dispatch_application_session_mq(application, requests, replies)
            },
            move |application| {
                if detach_applications.contains(application).unwrap_or(false)
                    && let Err(error) = detach_applications.detach(application)
                {
                    tracing::error!(%error, ?application, "failed to detach Application after attach connection closed");
                }
            },
        )
        .await
}

fn read_config(path: &Path) -> std::io::Result<String> {
    std::fs::read_to_string(path)
}

fn parse_startup_config(document: &str) -> RuntimeResult<Vec<String>> {
    let config: DaemonStartupConfig = toml::from_str(document)
        .map_err(|error| RuntimeError::config_parse(format!("parse startup TOML: {error}")))?;
    Ok(config.plugins)
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
