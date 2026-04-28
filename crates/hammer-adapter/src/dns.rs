use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::Lifecycle;

/// `adapter.DNSRouter` — picks a `DnsTransport` for each query and caches
/// results. M2 keeps the lifecycle + side-effect surface; query semantics
/// land in M3.
pub trait DnsRouter: Lifecycle {
    fn clear_cache(&self);
    fn reset_network(&self);
}

/// `adapter.DNSTransport` — single upstream resolver (UDP / TCP / HTTPS / hosts
/// / local). Exchange method lands in M3 once the hickory-based stack is wired.
pub trait DnsTransport: Lifecycle {
    fn type_name(&self) -> &str;
    fn tag(&self) -> &str;
}

pub trait DnsTransportManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn DnsTransport>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn DnsTransport>>;
    fn default(&self) -> Option<Arc<dyn DnsTransport>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
