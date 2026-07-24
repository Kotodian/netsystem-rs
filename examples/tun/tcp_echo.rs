//! Tun TCP echo Lab.
//!
//! ```text
//! cargo run -p hammer --example tun_tcp_echo
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use hammer_app::attach::{AppClient, AppClientError};
use hammer_app::{AppSession, AppSessionError, SessionHandle};
use hammer_infra::segment::Svm;
use hammer_runtime::app::SessionEvtType;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, RuntimeError, RuntimeRegistry};

const ATTACH_SOCKET: &str = "/tmp/hammer-tun-tcp-lab.attach.sock";
const ECHO_BUFFER_BYTES: usize = 64 * 1024;
const PLUGINS: [&str; 4] = ["tun", "ip", "tcp", "udp"];
const LAB_CONFIG: &str = r#"
plugins = ["tun", "ip", "tcp", "udp"]

[log]
level = "debug"

[worker]
workers = 1
poll_sleep = "50ms"

[worker.buffer]
data_size = 2048
buffers_per_numa = 2048

[network.session]
backend = "svm"
attach_socket_path = "/tmp/hammer-tun-tcp-lab.attach.sock"
preallocated_sessions = 64
event_queue_length = 256
ooo_capacity = 32

[network.session.buffer]
slot_bytes = 2048
slots = 4

[plugin.tcp]
mss = 1440
receive_window = 65535
congestion = "bbr"
nagle = true
time_wait = "2s"
paws_idle = "24h"

[plugin.tcp.pmtu]
enabled = true

[plugin.tcp.retransmit]
initial = "50ms"
min = "50ms"
max = "3s"

[plugin.tcp.keepalive]
idle = "3s"
probe_interval = "1s"
probe_limit = 3

[[plugin.tcp.listen]]
address = "10.66.77.1:7300"

[plugin.tun]

[[plugin.tun.interfaces]]
name = "utun"
address = ["10.66.77.1/30"]
mtu = { l3 = 1500, ip4 = 1500, ip6 = 1500, mpls = 1500 }

[[network.route]]
prefix = "10.66.77.0/30"
interface = "utun"
"#;

#[derive(Debug, thiserror::Error)]
enum EchoError {
    #[error(transparent)]
    MainHeap(#[from] hammer_infra::main_heap::MainHeapError),
    #[error(transparent)]
    Attach(#[from] AppClientError),
    #[error(transparent)]
    Session(#[from] AppSessionError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("failed to resolve the Cargo example executable")]
    CurrentExecutable {
        #[source]
        source: std::io::Error,
    },
    #[error("Cargo example executable has no profile directory")]
    ProfileDirectory,
    #[error("plugin artifact is missing: {path}")]
    PluginArtifactMissing { path: PathBuf },
    #[error("failed to stage plugin artifact from {source_path} to {destination_path}")]
    PluginArtifactStage {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build the echo Tokio runtime")]
    TokioRuntime {
        #[source]
        source: std::io::Error,
    },
}

fn main() -> Result<(), EchoError> {
    hammer_infra::main_heap::init_default()?;
    stage_plugin_artifacts()?;

    let registry = Arc::new(RuntimeRegistry::new());
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let engine = Engine::new(runtime, Arc::clone(&registry));
    let mut pool = EnginePool::new(engine);
    let roots = PLUGINS.map(str::to_owned);

    let main_engine = pool.main_engine_mut();
    main_engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    EnginePool::main_loop_enter(main_engine, &roots, LAB_CONFIG)?;

    let echo_result = run_attached_echo();
    EnginePool::main_loop_exit(pool.main_engine());

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;
    let shutdown_result = pool
        .main_engine_mut()
        .shutdown_process_nodes(&tokio_runtime);
    let close_result = pool.close();
    Engine::uninstall_current();

    echo_result?;
    shutdown_result?;
    close_result?;
    Ok(())
}

fn stage_plugin_artifacts() -> Result<(), EchoError> {
    let executable =
        std::env::current_exe().map_err(|source| EchoError::CurrentExecutable { source })?;
    let example_directory = executable.parent().ok_or(EchoError::ProfileDirectory)?;
    let profile_directory = example_directory
        .parent()
        .ok_or(EchoError::ProfileDirectory)?;

    for plugin in PLUGINS {
        let filename = libloading::library_filename(format!("hammer_plugin_{plugin}"));
        let source_path = profile_directory.join(&filename);
        if !source_path.is_file() {
            return Err(EchoError::PluginArtifactMissing { path: source_path });
        }
        let destination_path = example_directory.join(filename);
        std::fs::copy(&source_path, &destination_path).map_err(|source| {
            EchoError::PluginArtifactStage {
                source_path,
                destination_path,
                source,
            }
        })?;
    }
    Ok(())
}

fn run_attached_echo() -> Result<(), EchoError> {
    let handle = SessionHandle::new(0, 0);
    let session = AppClient::connect(ATTACH_SOCKET, handle)?;
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|source| EchoError::TokioRuntime { source })?;
    tokio_runtime.block_on(run_echo(&session, handle))
}

async fn run_echo(session: &AppSession<Svm>, handle: SessionHandle) -> Result<(), EchoError> {
    let mut buffer = vec![0; ECHO_BUFFER_BYTES];
    loop {
        let event = session.next_event().await?;
        if event.session_index() != handle.session_index() {
            continue;
        }
        match event.evt_type {
            SessionEvtType::Connect | SessionEvtType::TxDeq => {}
            SessionEvtType::RxEnq => {
                let read = session.recv(&mut buffer).await?;
                if read != 0 {
                    session.send_all(&buffer[..read]).await?;
                }
            }
            SessionEvtType::Close => return Ok(()),
        }
    }
}
