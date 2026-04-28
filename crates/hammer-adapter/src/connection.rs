use std::sync::Arc;

use hammer_core::lifecycle::Lifecycle;

pub trait ConnectionHandle: Send + Sync + 'static {
    fn close(&self);
}

/// `adapter.ConnectionManager` — tracks live TCP/UDP connections produced by
/// the router so they can be enumerated and closed en masse on Service.close.
/// Hot-path methods (TrackConn / NewConnection / NewPacketConnection) wait
/// until M5/M7 when the actual data plane needs them.
pub trait ConnectionManager: Lifecycle {
    fn count(&self) -> usize;
    fn track(&self, handle: Arc<dyn ConnectionHandle>) -> u64;
    fn remove(&self, id: u64) -> bool;
    fn close_all(&self);
}
