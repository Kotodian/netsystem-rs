use std::cell::RefCell;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_msg_q::SessionEvt;
use hammer_infra::vec::Vec;

use crate::app::session::{AppSession, AppSessionConfig};

/// VPP `application` per-worker state: app sessions owned by one app worker,
/// plus the worker-level event queue poll loop.
/// Each worker thread has one `AppWorker` registered in TLS.
#[derive(Debug, Clone)]
pub struct AppWorker {
    worker_index: usize,
    sessions: FlatHashTable<u32, Arc<AppSession>>,
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
        session_index: u32,
        config: AppSessionConfig,
    ) -> HammerResult<Arc<AppSession>> {
        if self.sessions.lookup(&session_index).is_some() {
            return Err(HammerError::internal(format!(
                "app worker {} already has session {session_index}",
                self.worker_index
            )));
        }
        let session = Arc::new(AppSession::new(config, session_index)?);
        self.sessions.insert(session_index, Arc::clone(&session));
        Ok(session)
    }

    /// Look up a session handle without detaching.
    #[inline]
    pub fn session(&self, session_index: u32) -> Option<Arc<AppSession>> {
        self.sessions.lookup(&session_index)
    }

    /// Remove a session. Mirrors VPP `app_worker_del_session`.
    pub fn detach_session(&mut self, session_index: u32) -> Option<Arc<AppSession>> {
        let session = self.sessions.remove(&session_index)?;
        session.clear();
        Some(session)
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
        for (_, session) in self.sessions.iter() {
            let took = session.evt_q().dequeue_batch(&mut batch);
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

/// Get-or-insert the calling worker's `AppWorker` and run `f` against it.
#[inline]
pub fn with_current_app_worker<R>(worker_index: usize, f: impl FnOnce(&mut AppWorker) -> R) -> R {
    APP_WORKERS.with(|slot| {
        let mut registry = slot.borrow_mut();
        let worker = registry.get_or_insert(worker_index);
        f(worker)
    })
}

pub fn current_app_session(session_index: u32) -> Option<Arc<AppSession>> {
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
        let session = worker
            .attach_session(1, AppSessionConfig::default())
            .expect("attach");
        assert!(worker.session(1).is_some());
        let detached = worker.detach_session(1).expect("detach");
        assert!(Arc::ptr_eq(&session, &detached));
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
    fn current_app_session_returns_attached() {
        with_current_app_worker(0, |w| {
            w.attach_session(42, AppSessionConfig::default())
                .expect("attach");
        });
        assert!(current_app_session(42).is_some());
        with_current_app_worker(0, |w| {
            w.detach_session(42);
        });
        assert!(current_app_session(42).is_none());
    }
}
