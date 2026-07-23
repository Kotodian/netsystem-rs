//! Application-facing access to Hammer's VPP-shaped app/session boundary.
//!
//! Embedding process entry points must initialize
//! [`hammer_infra::main_heap`] before constructing a runtime or loading DSOs.

pub mod attach;
pub mod echo;
pub mod tcp;
pub mod udp;

pub use hammer_runtime::app::{AppSession, AppSessionAsyncError, AppSessionConfig, SessionHandle};
