use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::segment::Segment;

use crate::app::handle::SessionHandle;
use crate::app::session::{AppSession, AppSessionConfig};

/// VPP `application` per-worker state: app sessions owned by one app worker,
/// plus the worker-level event queue poll loop.
/// The registry itself stays on the owning data worker thread.
#[derive(Clone)]
pub struct AppWorker<S: Segment> {
    worker_index: usize,
    sessions: FlatHashTable<u64, Arc<AppSession<S>>>,
}

impl<S: Segment> AppWorker<S> {
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

    /// Look up a session handle without detaching.
    #[inline]
    pub fn session(&self, handle: SessionHandle) -> Option<Arc<AppSession<S>>> {
        self.sessions.lookup(&handle.raw())
    }

    /// Remove a session. Mirrors VPP `app_worker_del_session`.
    pub fn detach_session(&mut self, handle: SessionHandle) -> Option<Arc<AppSession<S>>> {
        self.sessions.remove(&handle.raw())
    }

    /// Number of sessions currently attached.
    #[inline]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

impl<S: Segment> fmt::Debug for AppWorker<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppWorker")
            .field("worker_index", &self.worker_index)
            .field("session_count", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

use hammer_infra::segment::Local;

impl AppWorker<Local> {
    /// Create a fresh per-session FIFO/msgq object and register it.
    /// Mirrors VPP `app_worker_add_session` (in-process variant).
    pub fn attach_session_local(
        &mut self,
        handle: SessionHandle,
        config: AppSessionConfig,
    ) -> HammerResult<Arc<AppSession<Local>>> {
        if self.sessions.lookup(&handle.raw()).is_some() {
            return Err(HammerError::internal(format!(
                "app worker {} already has session {}",
                self.worker_index,
                handle.raw()
            )));
        }
        let session = Arc::new(AppSession::<Local>::local(config, handle)?);
        self.sessions.insert(handle.raw(), Arc::clone(&session));
        Ok(session)
    }
}

#[derive(Debug, Default, Clone)]
pub struct AppWorkerRegistry {
    by_worker: FlatHashTable<usize, AppWorker<Local>>,
}

impl AppWorkerRegistry {
    fn new() -> Self {
        Self {
            by_worker: FlatHashTable::new(),
        }
    }

    fn get_or_insert(&mut self, worker_index: usize) -> &mut AppWorker<Local> {
        if self.by_worker.lookup(&worker_index).is_none() {
            self.by_worker
                .insert(worker_index, AppWorker::<Local>::new(worker_index));
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
pub fn with_current_app_worker<R>(
    worker_index: usize,
    f: impl FnOnce(&mut AppWorker<Local>) -> R,
) -> R {
    APP_WORKERS.with(|slot| {
        let mut registry = slot.borrow_mut();
        let worker = registry.get_or_insert(worker_index);
        f(worker)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::segment::Local;

    #[test]
    fn app_worker_attach_detach_round_trips() {
        let mut worker = AppWorker::<Local>::new(0);
        let handle = SessionHandle::new(1, 7);
        let session = worker
            .attach_session_local(handle, AppSessionConfig::default())
            .expect("attach");
        assert!(worker.session(handle).is_some());
        let detached = worker.detach_session(handle).expect("detach");
        assert!(Arc::ptr_eq(&session, &detached));
        assert!(worker.session(handle).is_none());
    }

    #[test]
    fn app_worker_attach_rejects_duplicate() {
        let mut worker = AppWorker::<Local>::new(0);
        let handle = SessionHandle::new(1, 7);
        worker
            .attach_session_local(handle, AppSessionConfig::default())
            .expect("first attach");
        assert!(
            worker
                .attach_session_local(handle, AppSessionConfig::default())
                .is_err()
        );
    }
}
