use std::any::Any;
use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;

/// `adapter.Inbound` in Go — Lifecycle-managed entity that accepts user traffic
/// (TUN, mixed, http, socks, …). M2 surfaces only what Service / Manager
/// orchestration needs; protocol-specific methods land in M5/M7.
pub trait Inbound: Lifecycle {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
}

/// `adapter.InboundManager` — owns the live set of inbounds.
pub trait InboundManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn Inbound>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn Inbound>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
