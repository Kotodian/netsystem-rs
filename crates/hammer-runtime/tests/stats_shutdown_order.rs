use std::path::PathBuf;

use hammer_runtime::config::Worker;
use hammer_runtime::{Engine, EnginePool, RuntimeRegistry};

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "hammer-stats-shutdown-order-{}.sock",
        std::process::id()
    ))
}

fn main_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build main runtime")
}

#[test]
fn close_unlinks_stats_socket_before_process_nodes_stop() {
    let path = socket_path();
    let config = format!("[stats]\nsocket_path = \"{}\"\n", path.display());
    let engine = Engine::new_configured(RuntimeRegistry::new(), Worker::default())
        .expect("configure engine");
    let runtime = main_runtime();
    let mut pool = EnginePool::new(engine, &runtime).expect("create engine pool");
    pool.main_loop_enter(&[], &config, &runtime)
        .expect("enter main loop");
    let process = pool
        .main_engine()
        .process_handle("statseg-collector-process")
        .expect("stats collector Process Node");

    pool.close().expect("close engine pool");
    assert!(
        !path.exists(),
        "stats socket must be unlinked before process stop"
    );
    process
        .signal(1, 0)
        .expect("Process Node remains alive until explicit shutdown");

    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("stop Process Nodes");
    assert!(
        process.signal(1, 0).is_err(),
        "Process Node handle closes after explicit shutdown"
    );
}
