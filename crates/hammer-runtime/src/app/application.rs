use std::cell::RefCell;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_msg_q::SessionEvt;
use hammer_infra::vec::Vec;

use crate::app::session::{AppSessionConfig, SessionAppBoundary};

/// VPP `application` per-worker state: the set of `SessionAppBoundary` handles
/// owned by one app worker, plus the worker-level event queue poll loop.
/// Each worker thread has one `AppWorker` registered in TLS.
#[derive(Debug, Clone)]
pub struct AppWorker {
    worker_index: usize,
    /// session_index → boundary handle. Owned by this worker.
    sessions: FlatHashTable<u32, Arc<SessionAppBoundary>>,
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

    /// Create a fresh per-session boundary, register it, and return the handle.
    /// Mirrors VPP `app_worker_add_session` (in-process variant).
    pub fn attach_session(
        &mut self,
        session_index: u32,
        config: AppSessionConfig,
    ) -> HammerResult<Arc<SessionAppBoundary>> {
        if self.sessions.lookup(&session_index).is_some() {
            return Err(HammerError::internal(format!(
                "app worker {} already has session {session_index}",
                self.worker_index
            )));
        }
        let boundary = Arc::new(SessionAppBoundary::new(config, session_index)?);
        self.sessions.insert(session_index, Arc::clone(&boundary));
        Ok(boundary)
    }

    /// Look up a session handle without detaching.
    #[inline]
    pub fn session(&self, session_index: u32) -> Option<Arc<SessionAppBoundary>> {
        self.sessions.lookup(&session_index)
    }

    /// Remove a session. Mirrors VPP `app_worker_del_session`. The returned
    /// `Arc` is the last ref if the caller drops it; `clear()` is invoked on
    /// the boundary to reset fifos before drop.
    pub fn detach_session(&mut self, session_index: u32) -> Option<Arc<SessionAppBoundary>> {
        let boundary = self.sessions.remove(&session_index)?;
        boundary.clear();
        Some(boundary)
    }

    /// Drain events from every attached session's evt_q into `out`. The session
    /// runtime calls this once per poll tick to dispatch events to transport.
    /// Returns the total count appended. Does NOT call `read_signal` — the app
    /// side (or runtime) calls `read_signal` on its own poll loop.
    pub fn poll_session_events(&mut self, out: &mut Vec<SessionEvt>) -> usize {
        let start = out.len();
        let mut batch = [SessionEvt {
            session_index: 0,
            evt_type: hammer_infra::svm_msg_q::SessionEvtType::RxEnq,
        }; 16];
        for (_, boundary) in self.sessions.iter() {
            let took = boundary.evt_q().dequeue_batch(&mut batch);
            for evt in batch[..took].iter().copied() {
                out.push(evt);
            }
        }
        out.len() - start
    }

    /// Number of sessions currently attached.
    #[inline]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

thread_local! {
    static APP_WORKERS: RefCell<AppWorkerRegistry> = RefCell::new(AppWorkerRegistry::new());
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

/// Opaque handle returned by `current_app_worker` so callers can borrow the
/// worker's session table without exposing the registry. (C2/C3 use this to
/// attach sessions during connection setup.)
#[derive(Clone, Copy)]
pub struct AppWorkerHandle {
    worker_index: usize,
}

impl AppWorkerHandle {
    #[inline]
    pub const fn new(worker_index: usize) -> Self {
        Self { worker_index }
    }

    #[inline]
    pub const fn worker_index(self) -> usize {
        self.worker_index
    }
}

/// Get-or-insert the calling worker's `AppWorker` and run `f` against it.
/// Mirrors the old `worker_app_ring` TLS pattern but for VPP `AppWorker`.
#[inline]
pub fn with_current_app_worker<R>(worker_index: usize, f: impl FnOnce(&mut AppWorker) -> R) -> R {
    APP_WORKERS.with(|slot| {
        let mut registry = slot.borrow_mut();
        let worker = registry.get_or_insert(worker_index);
        f(worker)
    })
}

/// Look up an attached session boundary for the calling worker.
pub fn current_session_boundary(session_index: u32) -> Option<Arc<SessionAppBoundary>> {
    APP_WORKERS.with(|slot| {
        slot.borrow()
            .by_worker
            .iter()
            .find_map(|(_, worker)| worker.session(session_index))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::svm_msg_q::SessionEvtType;

    #[test]
    fn app_worker_attach_detach_round_trips() {
        let mut worker = AppWorker::new(0);
        let boundary = worker
            .attach_session(1, AppSessionConfig::default())
            .expect("attach");
        assert!(worker.session(1).is_some());
        let detached = worker.detach_session(1).expect("detach");
        assert!(Arc::ptr_eq(&boundary, &detached));
        assert!(worker.session(1).is_none());
    }

    #[test]
    fn app_worker_attach_rejects_duplicate() {
        let mut worker = AppWorker::new(0);
        worker
            .attach_session(1, AppSessionConfig::default())
            .expect("first attach");
        assert!(
            worker
                .attach_session(1, AppSessionConfig::default())
                .is_err()
        );
    }

    #[test]
    fn app_worker_poll_session_events_drains_all_attached() {
        let mut worker = AppWorker::new(0);
        let b1 = worker
            .attach_session(1, AppSessionConfig::default())
            .expect("attach 1");
        let b2 = worker
            .attach_session(2, AppSessionConfig::default())
            .expect("attach 2");
        for _ in 0..2 {
            b1.push_event(SessionEvtType::RxEnq).expect("evt 1");
            b2.push_event(SessionEvtType::TxDeq).expect("evt 2");
        }
        let mut out = Vec::with_capacity(8);
        assert_eq!(worker.poll_session_events(&mut out), 4);
        assert_eq!(out.iter().filter(|e| e.session_index == 1).count(), 2);
        assert_eq!(out.iter().filter(|e| e.session_index == 2).count(), 2);
    }

    #[test]
    fn with_current_app_worker_get_or_insert() {
        let idx = with_current_app_worker(0, |w| w.worker_index());
        assert_eq!(idx, 0);
        let count = with_current_app_worker(0, |w| w.session_count());
        assert_eq!(count, 0);
    }

    #[test]
    fn current_session_boundary_returns_attached() {
        with_current_app_worker(0, |w| {
            w.attach_session(42, AppSessionConfig::default())
                .expect("attach");
        });
        assert!(current_session_boundary(42).is_some());
        with_current_app_worker(0, |w| {
            w.detach_session(42);
        });
        assert!(current_session_boundary(42).is_none());
    }
}
