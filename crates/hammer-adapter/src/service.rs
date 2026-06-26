use std::sync::Arc;

use hammer_core::error::CoreResult;
use hammer_core::lifecycle::{Lifecycle, LifecycleService};

/// `adapter.ServiceManager` — extra lifecycle services registered alongside
/// the core runtime managers.
pub trait ServiceManager: Lifecycle {
    fn list(&self) -> Vec<Arc<dyn LifecycleService>>;
    fn get(&self, id: &str) -> Option<Arc<dyn LifecycleService>>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
