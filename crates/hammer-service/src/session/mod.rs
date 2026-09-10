//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::{Arc, OnceLock};

use hammer_core::data_plane::{NodeId, NodeState};
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AppServer;
use hammer_runtime::{DataPlaneMain, RuntimeResult};

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

static SESSION_CONFIG: OnceLock<Session> = OnceLock::new();
static APP_SERVER: OnceLock<Arc<AppServer>> = OnceLock::new();
static APP_SESSION_INPUT_NODE: OnceLock<NodeId> = OnceLock::new();

pub fn app_server() -> Option<Arc<AppServer>> {
    APP_SERVER.get().map(Arc::clone)
}

pub fn session_config() -> &'static Session {
    SESSION_CONFIG
        .get()
        .expect("Session configuration is installed before Session initialization")
}

#[hammer_component_macros::config_function(
    name = "session_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_session(config: config::NetworkSessionConfig) -> RuntimeResult<()> {
    let session = config.session.unwrap_or_default();
    session.validate()?;
    assert!(
        SESSION_CONFIG.set(session).is_ok(),
        "Session configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "session_init",
    runs_after = ["transport_main_init", "application_init"]
)]
fn init_session() -> RuntimeResult<()> {
    runtime::SessionMain::init(hammer_runtime::config::worker::worker_count())
}

#[hammer_component_macros::main_loop_exit_function]
fn exit_session() -> RuntimeResult<()> {
    session_main().begin_session_migration_shutdown();
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "application_init",
    runs_after = ["transport_main_init"]
)]
fn init_application() -> RuntimeResult<()> {
    ApplicationMain::init()
}

#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let session = session_config();
    let worker = engine.data_worker_id()?;
    let session_queue = engine
        .node_by_name("session-queue")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    engine
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    let app_session_input = engine
        .node_by_name("appsl-rx-mqs-input")
        .ok_or(error::SessionQueueError::NodeMissing)?;
    if let Some(installed) = APP_SESSION_INPUT_NODE.get() {
        assert_eq!(
            *installed, app_session_input,
            "Session workers share graph node identities"
        );
    } else {
        APP_SESSION_INPUT_NODE
            .set(app_session_input)
            .expect("Session input node identity is installed once");
    }
    engine
        .nodes()
        .set_node_state(app_session_input, NodeState::Disabled)?;
    let publisher = APP_SERVER.get().map(|server| server.publisher());
    let sessions = SessionWorker::new(
        worker,
        hammer_runtime::config::worker::worker_count(),
        AppSessionConfig::default(),
        session.pool_capacity,
        publisher,
    )?;
    runtime::install_session_worker(engine, app_session_input, session_queue, sessions)?;
    Ok(())
}

#[hammer_component_macros::init_function(name = "session_attach_server")]
fn configure_attach_server() -> RuntimeResult<()> {
    let session = session_config();
    let Some(path) = session.attach_socket_path.as_deref() else {
        return Ok(());
    };
    let server = Arc::new(AppServer::bind(path, session.app_session_capacity)?);
    assert!(
        APP_SERVER.set(server).is_ok(),
        "Session attach server configuration callback executes once"
    );
    Ok(())
}
