use std::sync::Arc;

use hammer_core::lifecycle::Lifecycle;

pub trait ConnectionHandle: Send + Sync + 'static {
    fn close(&self);
}

/// Tracks live TCP/UDP connections so they can be enumerated and closed
/// together on service shutdown or network changes.
pub trait ConnectionManager: Lifecycle {
    fn count(&self) -> usize;
    fn track(&self, handle: Arc<dyn ConnectionHandle>) -> u64;
    fn remove(&self, id: u64) -> bool;
    fn close_all(&self);
}
