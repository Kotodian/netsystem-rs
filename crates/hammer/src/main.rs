//! hammer — VPP-clone daemon

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;

use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::{Memory, Worker};
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::{RuntimeError, RuntimeResult};

// Shared device/interface/transport/session infrastructure contributes host
// builtins; loadable protocol and device-driver code comes only from DSOs.
use hammer_service as _;

mod ipc_handlers;
mod ipc_loop;

static STARTUP_CONFIG_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Fields that must exist before the Main Heap can be published. Unknown
/// sections are deliberately ignored here; their owning registration parses
/// them later on the initialized heap.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct DaemonEarlyConfig {
    memory: Memory,
    worker: Worker,
}

impl DaemonEarlyConfig {
    fn validate(&self) -> RuntimeResult<()> {
        self.memory.validate()?;
        self.worker.validate()
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
    #[cfg(target_os = "linux")]
    if let Some(core) = early.worker.cpu.main_core {
        let available = core_affinity::get_core_ids()
            .is_some_and(|cores| cores.into_iter().any(|candidate| candidate.id == core));
        if !available || !core_affinity::set_for_current(core_affinity::CoreId { id: core }) {
            eprintln!("Failed to pin main thread to configured core {core}");
            std::process::exit(1);
        }
    }
    let memory = early.memory;
    drop(early);
    drop(bootstrap_document);
    hammer_runtime::memory::ensure_main_heap(&memory).unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
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
    let worker = toml::from_str::<DaemonEarlyConfig>(&config)
        .map(|config| config.worker)
        .unwrap_or_else(|error| {
            eprintln!(
                "Failed to deserialize worker config {}: {error}",
                config_path.display()
            );
            std::process::exit(1);
        });
    if STARTUP_CONFIG_PATH.set(config_path).is_err() {
        eprintln!("startup configuration path was initialized more than once");
        std::process::exit(1);
    }

    run(config, roots, worker);
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

fn run(config: String, roots: Vec<String>, worker: Worker) {
    let registry = Arc::new(RuntimeRegistry::new());
    let engine = Engine::new_configured(Arc::clone(&registry), worker).unwrap_or_else(|error| {
        eprintln!("Failed to construct configured runtime: {error}");
        std::process::exit(1);
    });
    let mut pool = EnginePool::new(engine);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build tokio runtime");
    let listener = {
        let enter = rt.enter();
        let listener = bind_ipc_socket();
        drop(enter);
        listener
    };
    pool.set_ipc_listener(listener);

    let pool_engine = pool.main_engine_mut();
    pool_engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    EnginePool::main_loop_enter(pool_engine, &roots, &config).unwrap_or_else(|e| {
        eprintln!("main_loop_enter failed: {e}");
        std::process::exit(1);
    });
    drop(config);

    let listener = pool.take_ipc_listener().expect("IPC listener configured");

    tracing::info!("hammer started");

    rt.block_on(ipc_loop::clnt_loop(listener));

    let pool_engine = pool.main_engine_mut();
    EnginePool::main_loop_exit(pool_engine);
    pool_engine
        .shutdown_process_nodes(&rt)
        .unwrap_or_else(|error| tracing::error!(%error, "Process Node shutdown failed"));
    pool.close()
        .unwrap_or_else(|error| tracing::error!(%error, "Main-loop exit hook failed"));
    Engine::uninstall_current();
}

fn read_config(path: &Path) -> RuntimeResult<String> {
    std::fs::read_to_string(path).map_err(|error| {
        RuntimeError::invariant(format!("read config {}: {error}", path.display()))
    })
}

fn parse_startup_config(document: &str) -> RuntimeResult<Vec<String>> {
    let config: DaemonStartupConfig = toml::from_str(document)
        .map_err(|error| RuntimeError::config_parse(format!("parse startup TOML: {error}")))?;
    Ok(config.plugins)
}

pub(crate) fn load_current_config() -> RuntimeResult<String> {
    let path = STARTUP_CONFIG_PATH
        .get()
        .ok_or_else(|| RuntimeError::invariant("startup configuration path is not initialized"))?;
    read_config(path)
}

fn bind_ipc_socket() -> tokio::net::TcpListener {
    let addr = std::env::var("HAMMER_IPC_ADDR").unwrap_or_else(|_| "127.0.0.1:7299".to_string());
    let sock_addr: SocketAddr = addr.parse().unwrap_or_else(|e| {
        eprintln!("Invalid IPC address {addr}: {e}");
        std::process::exit(1);
    });
    let listener = std::net::TcpListener::bind(sock_addr).unwrap_or_else(|e| {
        eprintln!("Failed to bind IPC {sock_addr}: {e}");
        std::process::exit(1);
    });
    listener.set_nonblocking(true).unwrap_or_else(|e| {
        eprintln!("Failed to set nonblocking: {e}");
        std::process::exit(1);
    });
    tokio::net::TcpListener::from_std(listener).unwrap_or_else(|e| {
        eprintln!("Failed to create tokio listener: {e}");
        std::process::exit(1);
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn early_deserialization_ignores_later_owner_sections() {
        let config: super::DaemonEarlyConfig = toml::from_str(
            r#"
[memory]
main_heap_size = "256 MiB"

[plugin.tcp]
mss = "validated by tcp"
"#,
        )
        .expect("deserialize early config");

        assert_eq!(config.memory.main_heap_size.as_u64(), 256 << 20);
    }
}
