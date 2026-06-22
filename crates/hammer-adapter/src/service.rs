use std::sync::Arc;

use hammer_core::error::CoreResult;
use hammer_core::lifecycle::{Lifecycle, LifecycleService};

/// `adapter.ServiceManager` — extra services that are neither inbound nor
/// outbound. Hammer currently registers an empty list; the trait exists so the
/// manager set in `service.go` matches one-to-one.
pub trait ServiceManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn LifecycleService>>;
    fn get(&self, id: &str) -> Option<Arc<dyn LifecycleService>>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
