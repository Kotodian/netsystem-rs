use std::sync::Arc;

pub trait ConnectionHandle: Send + Sync + 'static {
    fn close(&self);
}

/// Tracks live TCP/UDP connections so they can be enumerated and closed
/// together on service shutdown or network changes.
pub trait ConnectionManager {
    fn count(&self) -> usize;
    fn track(&self, handle: Arc<dyn ConnectionHandle>) -> u64;
    fn remove(&self, id: u64) -> bool;
    fn close_all(&self);
}
