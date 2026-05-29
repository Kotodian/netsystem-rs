use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::debug;

use hammer_core::error::HammerResult;
use hammer_runtime::adapter::{
    ConnectionHandle, ConnectionManager as ConnectionManagerTrait, Lifecycle, StartStage,
};

pub struct ConnectionRegistration(Arc<dyn ConnectionHandle>);

impl<H> From<Arc<H>> for ConnectionRegistration
where
    H: ConnectionHandle + 'static,
{
    fn from(handle: Arc<H>) -> Self {
        let handle: Arc<dyn ConnectionHandle> = handle;
        Self(handle)
    }
}

impl From<Arc<dyn ConnectionHandle>> for ConnectionRegistration {
    fn from(handle: Arc<dyn ConnectionHandle>) -> Self {
        Self(handle)
    }
}

pub struct ConnectionManager {
    next_id: AtomicU64,
    connections: Mutex<HashMap<u64, Arc<dyn ConnectionHandle>>>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub fn track(&self, handle: impl Into<ConnectionRegistration>) -> u64 {
        <Self as ConnectionManagerTrait>::track(self, handle.into().0)
    }

    pub fn remove(&self, id: u64) -> bool {
        <Self as ConnectionManagerTrait>::remove(self, id)
    }

    pub fn count(&self) -> usize {
        <Self as ConnectionManagerTrait>::count(self)
    }

    pub fn close_all(&self) {
        <Self as ConnectionManagerTrait>::close_all(self);
    }
}

impl Lifecycle for ConnectionManager {
    fn name(&self) -> &str {
        "connection"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!(target: "connection", "stage {}", stage.name());
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        self.close_all();
        debug!(target: "connection", "close");
        Ok(())
    }
}

impl ConnectionManagerTrait for ConnectionManager {
    fn count(&self) -> usize {
        self.connections
            .lock()
            .expect("ConnectionManager poisoned")
            .len()
    }

    fn track(&self, handle: Arc<dyn ConnectionHandle>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.connections
            .lock()
            .expect("ConnectionManager poisoned")
            .insert(id, handle);
        id
    }

    fn remove(&self, id: u64) -> bool {
        self.connections
            .lock()
            .expect("ConnectionManager poisoned")
            .remove(&id)
            .is_some()
    }

    fn close_all(&self) {
        let handles = {
            let mut connections = self.connections.lock().expect("ConnectionManager poisoned");
            connections
                .drain()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>()
        };
        for handle in handles {
            handle.close();
        }
        debug!("close_all");
    }
}
