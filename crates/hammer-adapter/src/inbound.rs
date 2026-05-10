use crate::{AsAnyComponent, RuntimeComponent};
use hammer_core::error::CoreResult;
use hammer_core::lifecycle::Lifecycle;

pub type InboundComponent = RuntimeComponent<dyn Inbound>;

/// Lifecycle-managed entity that accepts user traffic (TUN, mixed, HTTP,
/// SOCKS, ...).
pub trait Inbound: Lifecycle + AsAnyComponent {}

/// `adapter.InboundManager` — owns the live set of inbounds.
pub trait InboundManager: Lifecycle {
    fn list(&self) -> Vec<InboundComponent>;
    fn get(&self, id: &str) -> Option<InboundComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
