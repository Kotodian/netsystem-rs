//! hammer — VPP-clone daemon

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::new_worker_runtime;

// Shared device/interface/transport/session infrastructure contributes host
// builtins; loadable protocol and device-driver code comes only from DSOs.
use hammer_service as _;

mod ipc_handlers;
mod ipc_loop;

fn main() {
    let requested_capacity = {
        let config_path = config_path_from_args();
        let bootstrap =
            hammer_core::config::load_bootstrap_config(&config_path).unwrap_or_else(|error| {
                eprintln!(
                    "Failed to load bootstrap config {}: {error}",
                    config_path.display()
                );
                std::process::exit(1);
            });
        bootstrap.memory.main_heap_size
    };
    hammer_infra::main_heap::init(requested_capacity).unwrap_or_else(|error| {
        eprintln!("Failed to initialize main heap: {error}");
        std::process::exit(1);
    });

    let config_path = config_path_from_args();
    let config = hammer_core::config::load_config(&config_path).unwrap_or_else(|error| {
        eprintln!(
            "Failed to load config {} on the main heap: {error}",
            config_path.display()
        );
        std::process::exit(1);
    });
    hammer_infra::main_heap::init(config.memory.main_heap_size).unwrap_or_else(|error| {
        eprintln!("Main heap configuration changed during bootstrap: {error}");
        std::process::exit(1);
    });
    drop(config_path);

    run(config);
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

fn run(config: Config) {
    let registry = Arc::new(RuntimeRegistry::new());
    registry.set::<Config>(Arc::new(config.clone()));

    let runtime = new_worker_runtime(&config).unwrap_or_else(|error| {
        eprintln!("Failed to initialize data-plane runtime: {error}");
        std::process::exit(1);
    });
    let engine = Engine::new(runtime, Arc::clone(&registry));
    let mut pool = EnginePool::new(engine);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build tokio runtime");
    let _enter = rt.enter();

    let listener = bind_ipc_socket();
    pool.set_ipc_listener(listener);

    let pool_engine = pool.main_engine_mut();
    EnginePool::main_loop_enter(pool_engine).unwrap_or_else(|e| {
        eprintln!("main_loop_enter failed: {e}");
        std::process::exit(1);
    });

    let listener = pool.take_ipc_listener().expect("IPC listener configured");

    tracing::info!("hammer started");

    pool.main_engine().run_processes_until(&rt, async {
        ipc_loop::clnt_loop(listener).await;
    });

    let pool_engine = pool.main_engine_mut();
    EnginePool::main_loop_exit(pool_engine);
    pool_engine
        .shutdown_process_nodes(&rt)
        .unwrap_or_else(|error| tracing::error!(%error, "Process Node shutdown failed"));
    pool.close()
        .unwrap_or_else(|error| tracing::error!(%error, "Main-loop exit hook failed"));
    Engine::uninstall_current();
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
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn bootstrap_capacity_loads_the_complete_include_chain() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "hammer-bootstrap-config-{}-{}-{sequence}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create config test directory");

        let main = root.join("main.toml");
        let first = root.join("first.toml");
        let memory = root.join("memory.toml");
        fs::write(&main, "include = [\"first.toml\"]\n").expect("write main config");
        fs::write(&first, "include = [\"memory.toml\"]\n").expect("write nested config");
        fs::write(&memory, "[memory]\nmain_heap_size = \"256 MiB\"\n")
            .expect("write memory config");

        let bootstrap =
            hammer_core::config::load_bootstrap_config(&main).expect("load bootstrap config");

        fs::remove_dir_all(root).expect("remove config test directory");
        assert_eq!(bootstrap.memory.main_heap_size, 256 << 20);
    }
}
