//! Session layer — shared in `hammer-service` (not a loadable plugin).

use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::config::SessionBackend;
use hammer_core::config::network::Session;
use hammer_core::error::{HammerError, HammerResult};
use hammer_runtime::attach::AttachServer;

pub mod app;
pub mod error;
pub mod id;
pub mod node;
pub mod protocol;
pub mod runtime;
pub mod state;

pub use app::SessionAppRuntime;
pub use error::SessionQueueError;
pub use id::SessionId;
pub use node::{SESSION_QUEUE_IO_BUDGET, SessionQueueNext, SessionQueueNode};
pub use runtime::SessionWorker;

#[hammer_component_macros::config_function(name = "session_config")]
fn configure_session(config: Arc<Config>) -> HammerResult<Option<Arc<Session>>> {
    let Some(session) = config.network.session.clone() else {
        return Ok(None);
    };
    crate::transport::publish_session_backend(session.backend);
    Ok(Some(Arc::new(session)))
}

#[hammer_component_macros::config_function(
    name = "session_attach_config",
    runs_after = ["session_config"]
)]
fn configure_attach_server(
    #[inject(optional)] session: Arc<Session>,
) -> HammerResult<Option<Arc<AttachServer>>> {
    if session.backend == SessionBackend::Local {
        return Ok(None);
    }
    let path = session.attach_socket_path.as_deref().ok_or_else(|| {
        HammerError::config_validation(
            "network.session.attach_socket_path is required for the SVM backend",
        )
    })?;
    let server = Arc::new(AttachServer::bind(path)?);
    tracing::info!(path, "session attach server bound");
    Ok(Some(server))
}
