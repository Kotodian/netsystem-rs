use std::any::Any;
use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;

/// Lifecycle-managed entity that accepts user traffic (TUN, mixed, HTTP,
/// SOCKS, ...).
pub trait Inbound: Lifecycle {
    fn type_name(&self) -> &str;
    fn id(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
}

/// `adapter.InboundManager` — owns the live set of inbounds.
pub trait InboundManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn Inbound>>;
    fn get(&self, id: &str) -> Option<Arc<dyn Inbound>>;
    fn remove(&self, id: &str) -> Result<(), CoreError>;
}
