use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use hammer_ipc::StatsClient;
use hammer_runtime::config::Worker;
use hammer_runtime::{Engine, EnginePool, RuntimeRegistry};
use hammer_stats::{StatsError, StatsMain};

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("hammer-stats-control-{}.sock", std::process::id()))
}

fn main_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("build main runtime")
}

#[test]
fn control_dispatch_survives_disconnect_after_process_nodes_stop() {
    let path = socket_path();
    let config = format!("[stats]\nsocket_path = \"{}\"\n", path.display());
    let engine = Engine::new_configured(RuntimeRegistry::new(), Worker::default())
        .expect("configure engine");
    let runtime = main_runtime();
    let mut pool = EnginePool::new(engine, &runtime).expect("create engine pool");
    pool.main_loop_enter(&[], &config, &runtime)
        .expect("enter main loop");
    pool.main_engine_mut()
        .shutdown_process_nodes(&runtime)
        .expect("stop Process Nodes before control dispatch");

    let disconnected_client = UnixStream::connect(&path).expect("connect disposable client");
    disconnected_client
        .shutdown(std::net::Shutdown::Both)
        .expect("disconnect disposable client");
    drop(disconnected_client);

    let (result_sender, result_receiver) = mpsc::channel();
    let client_path = path.clone();
    let client_thread = thread::spawn(move || {
        let mut last_error = None;
        for _ in 0..500 {
            match StatsClient::connect(&client_path) {
                Ok(client) => {
                    let first_names = client.list();
                    let second_names =
                        StatsClient::connect(&client_path).and_then(|client| client.list());
                    let result = first_names.and_then(|first_names| {
                        assert!(!first_names.is_empty(), "first stats client sees metrics");
                        second_names
                    });
                    result_sender
                        .send(result)
                        .expect("send stats client result");
                    return;
                }
                Err(error) => {
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(2));
                }
            }
        }
        result_sender
            .send(Err(
                last_error.expect("stats client retry must record an error")
            ))
            .expect("send stats client failure");
    });

    let names = pool
        .run_processes_until(&runtime, async move {
            tokio::time::timeout(Duration::from_secs(5), async move {
                loop {
                    match result_receiver.try_recv() {
                        Ok(result) => break result,
                        Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            panic!("stats client thread disconnected")
                        }
                    }
                }
            })
            .await
            .expect("control dispatcher must accept stats client")
        })
        .expect("run process and control dispatchers")
        .expect("read stats directory");

    assert!(!names.is_empty(), "stats registration must publish metrics");
    pool.close().expect("close engine pool");
    assert!(!path.exists(), "stats socket must be unlinked on close");
    assert!(matches!(
        StatsMain::init("stats-after-close", 2 * 1024 * 1024, &path),
        Err(StatsError::AlreadyInitialized)
    ));
    client_thread.join().expect("join stats client thread");
}
