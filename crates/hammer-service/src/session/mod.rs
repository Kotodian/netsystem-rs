//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_core::data_plane::NodeState;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AppServer;
use hammer_runtime::{Engine, RuntimeResult};

pub mod app;
pub mod application;
pub mod config;
mod control;
pub mod error;
mod lookup;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod state;

pub use app::AppWorker;
pub use application::{
    APPLICATION_MAIN, ApplicationError, ApplicationMain, ApplicationMqResources, application_main,
};
pub use config::Session;
pub use error::{SessionConnectError, SessionQueueError};
pub use node::{AppSessionInputNode, SESSION_QUEUE_IO_BUDGET, SessionQueueNext, SessionQueueNode};
pub use protocol::{SessionAppVft, register_session_app};
pub use runtime::{
    SESSION_MAIN, SessionAcceptMetadata, SessionEndpointRole, SessionWorker, session_main,
};

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

#[hammer_component_macros::init_function(
    name = "session_init",
    runs_after = ["transport_main_init", "application_init"]
)]
fn init_session(engine: &mut Engine, session: Arc<Session>) -> RuntimeResult<()> {
    engine.registry.set(Arc::clone(&session));
    runtime::SessionMain::init(engine.configured_worker_count())
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_session(_engine: &mut Engine) -> RuntimeResult<()> {
    session_main().begin_session_migration_shutdown();
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "application_init",
    runs_after = ["transport_main_init"]
)]
fn init_application(_: &mut Engine, session: Arc<Session>) -> RuntimeResult<()> {
    ApplicationMain::init()
}

#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(engine: &mut Engine, session: Arc<Session>) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    let app_session_input = engine
        .runtime
        .node_by_name("appsl-rx-mqs-input")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    engine
        .runtime
        .nodes()
        .set_node_state(app_session_input, NodeState::Disabled)?;
    let publisher = engine
        .registry
        .get::<AppServer>()
        .map(|server| server.publisher());
    let sessions = SessionWorker::new(
        worker,
        engine.configured_worker_count(),
        AppSessionConfig::default(),
        session.pool_capacity,
        publisher,
    )?;
    runtime::install_session_worker(engine, app_session_input, session_queue, sessions)?;
    Ok(())
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
