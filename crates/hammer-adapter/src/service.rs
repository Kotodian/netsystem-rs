use std::sync::Arc;

use hammer_core::error::CoreError;
use hammer_core::lifecycle::{Lifecycle, LifecycleService};

/// `adapter.ServiceManager` — extra services that are neither inbound nor
/// outbound (e.g. dnsmasq, derp, etc.). Hammer currently registers an empty
/// list; the trait exists so the manager set in `service.go` matches one-to-one.
pub trait ServiceManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn LifecycleService>>;
    fn get(&self, tag: &str) -> Option<Arc<dyn LifecycleService>>;
    fn remove(&self, tag: &str) -> Result<(), CoreError>;
}
