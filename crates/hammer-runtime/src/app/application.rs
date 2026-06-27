use std::cell::RefCell;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::ring::LockFreeRing;

use crate::app::handle::SessionHandle;
use crate::app::session::{AppSession, AppSessionConfig};

/// VPP `application` per-worker state: app sessions owned by one app worker,
/// plus the worker-level event queue poll loop.
/// The registry itself stays on the owning data worker thread.
#[derive(Debug, Clone)]
pub struct AppWorker {
    worker_index: usize,
    sessions: FlatHashTable<u64, Arc<AppSession>>,
}

impl AppWorker {
    pub fn new(worker_index: usize) -> Self {
        Self {
            worker_index,
            sessions: FlatHashTable::new(),
        }
    }

    #[inline]
    pub const fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// Create a fresh per-session FIFO/msgq object and register it.
    /// Mirrors VPP `app_worker_add_session` (in-process variant).
    pub fn attach_session(
        &mut self,
        handle: SessionHandle,
        config: AppSessionConfig,
        tx_evt_q: Arc<LockFreeRing<u32>>,
    ) -> HammerResult<Arc<AppSession>> {
        if self.sessions.lookup(&handle.raw()).is_some() {
            return Err(HammerError::internal(format!(
                "app worker {} already has session {}",
                self.worker_index,
                handle.raw()
            )));
        }
        let session = Arc::new(AppSession::new(config, handle, tx_evt_q)?);
        self.sessions.insert(handle.raw(), Arc::clone(&session));
        Ok(session)
    }

    /// Look up a session handle without detaching.
    #[inline]
    pub fn session(&self, handle: SessionHandle) -> Option<Arc<AppSession>> {
        self.sessions.lookup(&handle.raw())
    }

    /// Remove a session. Mirrors VPP `app_worker_del_session`.
    pub fn detach_session(&mut self, handle: SessionHandle) -> Option<Arc<AppSession>> {
        self.sessions.remove(&handle.raw())
    }

    /// Number of sessions currently attached.
    #[inline]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

#[derive(Debug, Default, Clone)]
pub struct AppWorkerRegistry {
    by_worker: FlatHashTable<usize, AppWorker>,
}

impl AppWorkerRegistry {
    fn new() -> Self {
        Self {
            by_worker: FlatHashTable::new(),
        }
    }

    fn get_or_insert(&mut self, worker_index: usize) -> &mut AppWorker {
        if self.by_worker.lookup(&worker_index).is_none() {
            self.by_worker
                .insert(worker_index, AppWorker::new(worker_index));
        }
        self.by_worker
            .get_mut(&worker_index)
            .expect("app worker was just inserted")
    }
}

thread_local! {
    static APP_WORKERS: RefCell<AppWorkerRegistry> = RefCell::new(AppWorkerRegistry::new());
}

/// Access the app-worker registry on the current data worker thread.
#[inline]
pub fn with_current_app_worker<R>(worker_index: usize, f: impl FnOnce(&mut AppWorker) -> R) -> R {
    APP_WORKERS.with(|slot| {
        let mut registry = slot.borrow_mut();
        let worker = registry.get_or_insert(worker_index);
        f(worker)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_event_queue() -> Arc<LockFreeRing<u32>> {
        Arc::new(LockFreeRing::with_capacity(8).expect("tx event queue"))
    }

    #[test]
    fn app_worker_attach_detach_round_trips() {
        let mut worker = AppWorker::new(0);
        let handle = SessionHandle::new(1, 7);
        let session = worker
            .attach_session(handle, AppSessionConfig::default(), tx_event_queue())
            .expect("attach");
        assert!(worker.session(handle).is_some());
        let detached = worker.detach_session(handle).expect("detach");
        assert!(Arc::ptr_eq(&session, &detached));
        assert!(worker.session(handle).is_none());
    }

    #[test]
    fn app_worker_attach_rejects_duplicate() {
        let mut worker = AppWorker::new(0);
        let handle = SessionHandle::new(1, 7);
        worker
            .attach_session(handle, AppSessionConfig::default(), tx_event_queue())
            .expect("first attach");
        assert!(
            worker
                .attach_session(handle, AppSessionConfig::default(), tx_event_queue())
                .is_err()
        );
    }
}
