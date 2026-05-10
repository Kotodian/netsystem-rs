use hammer_core::error::CoreResult;
use hammer_core::lifecycle::Lifecycle;

use crate::RuntimeComponent;
use crate::outbound::Outbound;

pub type EndpointComponent = RuntimeComponent<dyn Endpoint>;

/// `adapter.Endpoint` — Outbound that also has its own Lifecycle. In Go this
/// is the integration point for protocols that maintain long-lived state
/// (e.g. wireguard, tun-via-outbound, etc.). Hammer doesn't actually expose
/// such an outbound today but the manager set in service.go expects it,
/// so the trait must exist.
pub trait Endpoint: Outbound + Lifecycle {}

pub trait EndpointManager: Lifecycle {
    fn list(&self) -> Vec<EndpointComponent>;
    fn get(&self, id: &str) -> Option<EndpointComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
