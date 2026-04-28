use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;

use crate::dialer::Network;

/// `adapter.Outbound` in Go — represents a single egress (hysteria2, direct,
/// block, dns, …). Async dial/listen methods are deferred to M6 when the
/// Hysteria2 outbound brings real I/O. The shape stays stable so adding those
/// later is an extension, not a refactor.
pub trait Outbound: Send + Sync + 'static {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
    fn networks(&self) -> &[Network];
    fn dependencies(&self) -> &[String];
}

/// `adapter.OutboundManager` — owns the live set of outbounds and a default
/// fallback (used when a route rule has no explicit outbound match).
pub trait OutboundManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn Outbound>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn Outbound>>;
    fn default(&self) -> Option<Arc<dyn Outbound>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
