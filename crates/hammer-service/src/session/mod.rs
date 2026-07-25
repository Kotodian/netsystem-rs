//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_core::data_plane::NodeState;
use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AppServer;
use hammer_runtime::{Engine, RuntimeError, RuntimeResult};

pub mod app;
pub mod config;
pub mod error;
pub mod id;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod state;

pub use app::AppWorker;
pub use config::Session;
pub use error::SessionQueueError;
pub use id::SessionId;
pub use node::{SESSION_QUEUE_IO_BUDGET, SessionQueueNext, SessionQueueNode};
pub use runtime::SessionWorker;

#[hammer_component_macros::config_function(
    name = "session_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_session(config: config::NetworkSessionConfig) -> RuntimeResult<Arc<Session>> {
    let session = config.session.unwrap_or_default();
    session.validate()?;
    Ok(Arc::new(session))
}

#[hammer_component_macros::init_function(name = "session_init")]
fn init_session(engine: &mut Engine) -> RuntimeResult<Arc<runtime::SessionMain>> {
    Ok(Arc::new(runtime::SessionMain::new(
        engine.configured_worker_count(),
    )))
}

#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(engine: &mut Engine, main: Arc<runtime::SessionMain>) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or_else(|| RuntimeError::subsystem("session", error::SessionQueueError::NodeMissing))?;
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    let sessions =
        SessionWorker::<PoolIndex>::with_app_session_config(worker, AppSessionConfig::default());
    runtime::install_session_worker(&main, engine, session_queue, sessions)
}

#[hammer_component_macros::init_function(name = "session_attach_server")]
fn configure_attach_server(
    #[inject(optional)] session: Arc<Session>,
) -> RuntimeResult<Option<Arc<AppServer>>> {
    let Some(path) = session.attach_socket_path.as_deref() else {
        return Ok(None);
    };
    let server = Arc::new(AppServer::bind(path)?);
    Ok(Some(server))
}
