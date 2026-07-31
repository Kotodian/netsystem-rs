//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_core::data_plane::NodeState;
use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AppServer;
use hammer_runtime::{Engine, RuntimeError, RuntimeResult};

pub mod app;
pub mod application;
pub mod config;
mod control;
pub mod error;
pub mod id;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod state;

pub use app::AppWorker;
pub use application::{ApplicationError, ApplicationMain, ApplicationRegistration};
pub use config::Session;
pub use error::SessionQueueError;
pub use hammer_runtime::app::AppSessionProtocol;
pub use id::SessionId;
pub use node::{AppSessionInputNode, SESSION_QUEUE_IO_BUDGET, SessionQueueNext, SessionQueueNode};
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
fn init_session(
    engine: &mut Engine,
    applications: Arc<ApplicationMain>,
) -> RuntimeResult<Arc<runtime::SessionMain>> {
    Ok(Arc::new(runtime::SessionMain::new(
        engine.configured_worker_count(),
        applications,
    )))
}

#[hammer_component_macros::init_function(name = "application_init")]
fn init_application(
    engine: &mut Engine,
    session: Arc<Session>,
) -> RuntimeResult<Arc<ApplicationMain>> {
    Ok(ApplicationMain::with_protocols(
        session.app_session_capacity,
        engine.plugin_main().app_session_protocols(),
    ))
}

#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(
    engine: &mut Engine,
    main: Arc<runtime::SessionMain>,
    applications: Arc<ApplicationMain>,
) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or_else(|| RuntimeError::subsystem("session", error::SessionQueueError::NodeMissing))?;
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    let app_session_input = engine
        .runtime
        .node_by_name("appsl-rx-mqs-input")
        .ok_or_else(|| RuntimeError::subsystem("session", error::SessionQueueError::NodeMissing))?;
    engine
        .runtime
        .nodes()
        .set_node_state(app_session_input, NodeState::Disabled)?;
    let mut sessions = if let Some(server) = engine.registry.get::<AppServer>() {
        SessionWorker::<PoolIndex>::with_app_session_attach(
            worker,
            engine.configured_worker_count(),
            AppSessionConfig::default(),
            Arc::clone(&applications),
            server.publisher(),
        )?
    } else {
        SessionWorker::<PoolIndex>::with_application_main(
            worker,
            engine.configured_worker_count(),
            AppSessionConfig::default(),
            applications,
        )?
    };
    sessions.set_listener_main(Arc::clone(&main));
    runtime::install_session_worker(&main, engine, app_session_input, session_queue, sessions)
}

#[hammer_component_macros::init_function(name = "session_attach_server")]
fn configure_attach_server(
    #[inject(optional)] session: Arc<Session>,
) -> RuntimeResult<Option<Arc<AppServer>>> {
    let Some(path) = session.attach_socket_path.as_deref() else {
        return Ok(None);
    };
    let server = Arc::new(AppServer::bind(path, session.app_session_capacity)?);
    Ok(Some(server))
}
