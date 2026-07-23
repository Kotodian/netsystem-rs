//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_runtime::attach::AppServer;
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};

pub mod app;
pub mod config;
pub mod error;
pub mod id;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod state;

pub use app::SessionAppRuntime;
pub use config::{Session, SessionBackend};
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
fn configure_session(config: config::NetworkSessionConfig) -> RuntimeResult<Option<Arc<Session>>> {
    let Some(session) = config.session else {
        return Ok(None);
    };
    session.validate()?;
    crate::transport::publish_session_backend(session.backend);
    Ok(Some(Arc::new(session)))
}

#[hammer_component_macros::init_function(name = "session_attach_server")]
fn configure_attach_server(
    #[inject(optional)] session: Arc<Session>,
) -> RuntimeResult<Option<Arc<AppServer>>> {
    if session.backend == SessionBackend::Local {
        return Ok(None);
    }
    let path = session.attach_socket_path.as_deref().ok_or_else(|| {
        RuntimeError::config_validation(
            "network.session.attach_socket_path is required for the SVM backend",
        )
    })?;
    let server = Arc::new(AppServer::bind(path)?);
    Ok(Some(server))
}
