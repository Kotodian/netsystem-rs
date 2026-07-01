//! hammer — VPP-clone daemon

use std::net::SocketAddr;
use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::config::SessionBackend;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::new_worker_runtime;

mod ipc_handlers;
mod ipc_loop;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: hammer <config.toml>");
        std::process::exit(1);
    }

    let config_path = &args[1];
    let config_content = std::fs::read_to_string(config_path).unwrap_or_else(|e| {
        eprintln!("Failed to read config {config_path}: {e}");
        std::process::exit(1);
    });

    let config: Config = hammer_core::config::parse_config(&config_content).unwrap_or_else(|e| {
        eprintln!("Failed to parse config: {e}");
        std::process::exit(1);
    });

    let registry = Arc::new(RuntimeRegistry::new());
    registry.set::<Config>(Arc::new(config.clone()));

    let runtime = new_worker_runtime(&config);
    let engine = Engine::new(runtime, Arc::clone(&registry));
    let mut pool = EnginePool::new(engine);

    let listener = bind_ipc_socket();
    pool.set_ipc_listener(listener);

    let mut attach_server: Option<hammer_runtime::attach::AttachServer> = None;
    {
        let config = registry.require::<Config>().unwrap_or_else(|e| {
            eprintln!("failed to get config from registry: {e}");
            std::process::exit(1);
        });
        if config.network.session.backend == SessionBackend::Svm {
            let path = config
                .network
                .session
                .attach_socket_path
                .as_deref()
                .unwrap_or_else(|| {
                    eprintln!("attach_socket_path is required when session.backend = \"svm\"");
                    std::process::exit(1);
                });
            attach_server = Some(
                hammer_runtime::attach::AttachServer::bind(path).unwrap_or_else(|e| {
                    eprintln!("failed to bind attach server: {e}");
                    std::process::exit(1);
                }),
            );
            eprintln!("attach server bound at {path}");
        }
    }

    let pool_engine = pool.main_engine_mut();
    EnginePool::main_loop_enter(pool_engine).unwrap_or_else(|e| {
        eprintln!("main_loop_enter failed: {e}");
        std::process::exit(1);
    });

    let listener = pool.take_ipc_listener().expect("IPC listener configured");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build tokio runtime");

    tracing::info!("hammer started");

    rt.block_on(async {
        ipc_loop::clnt_loop(listener).await;
    });

    let pool_engine = pool.main_engine();
    EnginePool::main_loop_exit(pool_engine);
    pool.close();
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
