//! Application-facing access to Hammer's VPP-shaped app/session boundary.
//!
//! Embedding process entry points must initialize
//! [`hammer_infra::main_heap`] before constructing a runtime or loading DSOs.

pub mod attach;
pub mod echo;
mod session;

pub use hammer_runtime::app::{
    APP_SESSION_POLICY_VERSION, AppSession, AppSessionConfig, AppSessionError, AppSessionPolicy,
    AppSessionPolicyError, AppSessionProtocolSelection, ApplicationId, ApplicationListenerId,
    SessionHandle,
};
pub use hammer_runtime::{DataWorkerId, SessionListenEndpoint, SessionListenerId};
