//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_core::data_plane::NodeState;
use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::attach::AppServer;
use hammer_runtime::{Engine, RuntimeResult};

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
pub use application::{
    ApplicationError, ApplicationMain, ApplicationMqResources, ApplicationRegistration,
};
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

#[hammer_component_macros::init_function(
    name = "session_init",
    runs_after = ["application_init"]
)]
fn init_session(
    engine: &mut Engine,
    applications: Arc<ApplicationMain>,
    session: Arc<Session>,
) -> RuntimeResult<Arc<runtime::SessionMain>> {
    engine.registry.set(Arc::clone(&session));
    Ok(Arc::new(runtime::SessionMain::with_pool_capacity(
        engine.configured_worker_count(),
        applications,
        session.pool_capacity,
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
    session: Arc<Session>,
) -> RuntimeResult<()> {
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
    let mut sessions = SessionWorker::<PoolIndex>::new(
        worker,
        engine.configured_worker_count(),
        AppSessionConfig::default(),
        session.pool_capacity,
        applications,
        publisher,
    )?;
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

#[cfg(test)]
mod tests {
    use hammer_runtime::init::topological_order;

    use super::{__INIT_FN_APPLICATION_INIT, __INIT_FN_SESSION_INIT};

    #[test]
    fn application_initializes_before_session() {
        let functions = [__INIT_FN_APPLICATION_INIT, __INIT_FN_SESSION_INIT];
        let order = topological_order(&functions).expect("session init order");
        let names = order
            .into_iter()
            .map(|index| functions[index].name)
            .collect::<Vec<_>>();

        assert_eq!(names, ["application_init", "session_init"]);
    }
}
