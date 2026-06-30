use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::msg_queue::{MsgQueue, SessionEvt};
use hammer_infra::segment::Segment;
use tokio::sync::Notify;

use crate::app::handle::SessionHandle;
use crate::app::session::{AppSession, AppSessionConfig};

struct SessionNotify {
    rx_readable: Notify,
    tx_writable: Notify,
    evt_readable: Notify,
}

impl SessionNotify {
    fn new() -> Self {
        Self {
            rx_readable: Notify::new(),
            tx_writable: Notify::new(),
            evt_readable: Notify::new(),
        }
    }
}

/// VPP `application` per-worker state: app sessions owned by one app worker,
/// plus the worker-level event queue poll loop.
/// The registry itself stays on the owning data worker thread.
#[derive(Clone)]
pub struct AppWorker<S: Segment> {
    worker_index: usize,
    sessions: FlatHashTable<u64, Arc<AppSession<S>>>,
    notifies: FlatHashTable<u64, Arc<SessionNotify>>,
}

impl<S: Segment> AppWorker<S> {
    pub fn new(worker_index: usize) -> Self {
        Self {
            worker_index,
            sessions: FlatHashTable::new(),
            notifies: FlatHashTable::new(),
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
        self.notifies.remove(&handle.raw());
        self.sessions.remove(&handle.raw())
    }

    /// Convenience: look up the per-session notify table entry. Returned as
    /// `Arc<SessionNotify>` so callers can await `rx_readable` / `tx_writable`
    /// / `evt_readable` without holding a borrow on the worker.
    pub fn notify_entry(&self, handle: SessionHandle) -> Option<Arc<SessionNotify>> {
        self.notifies.lookup(&handle.raw())
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
        let tx_evt_q = Arc::new(
            MsgQueue::<Local>::with_capacity(64)
                .map_err(|_| HammerError::internal("invalid tx_evt_q capacity"))?,
        );
        let session = Arc::new(AppSession::<Local>::new_in_segment(
            Local::default(),
            config,
            handle,
            tx_evt_q,
        )?);
        self.sessions.insert(handle.raw(), Arc::clone(&session));
        self.notifies
            .insert(handle.raw(), Arc::new(SessionNotify::new()));
        Ok(session)
    }

    /// In-process variant that joins the shared runtime-side TX event queue
    /// owned by `SessionAppRuntime` instead of creating a private per-session
    /// queue.  The dataplane drains all sessions' TX events from this single
    /// shared `MsgQueue` via `SessionAppRuntime::drain_tx_events_to`.
    pub fn attach_session_local_with_runtime_tx(
        &mut self,
        handle: SessionHandle,
        config: AppSessionConfig,
        runtime_tx_evt_q: Arc<MsgQueue<Local>>,
    ) -> HammerResult<Arc<AppSession<Local>>> {
        if self.sessions.lookup(&handle.raw()).is_some() {
            return Err(HammerError::internal(format!(
                "app worker {} already has session {}",
                self.worker_index,
                handle.raw()
            )));
        }
        let session = Arc::new(AppSession::<Local>::new_in_segment(
            Local::default(),
            config,
            handle,
            runtime_tx_evt_q,
        )?);
        self.sessions.insert(handle.raw(), Arc::clone(&session));
        self.notifies
            .insert(handle.raw(), Arc::new(SessionNotify::new()));
        Ok(session)
    }

    /// App-side async send via worker. Applies FIFO backpressure and completes
    /// only after every byte has entered the session-owned TX FIFO.
    pub async fn send_all(&self, handle: SessionHandle, bytes: &[u8]) -> HammerResult<usize> {
        let session = self
            .sessions
            .lookup(&handle.raw())
            .ok_or_else(|| HammerError::internal("app session not found"))?;
        let notify = self.notify_entry(handle);
        let mut written = 0usize;
        while written < bytes.len() {
            let accepted = session.send_bytes(&bytes[written..])?;
            if accepted != 0 {
                written += accepted;
                continue;
            }
            session.want_tx_notification();
            if session.tx_fifo().max_enqueue() != 0 {
                session.clear_tx_notification();
                continue;
            }
            if let Some(ref n) = notify {
                n.tx_writable.notified().await;
            }
            session.clear_tx_notification();
        }
        Ok(written)
    }

    /// App-side async receive via worker. Waits until RX FIFO has data.
    pub async fn recv(&self, handle: SessionHandle, out: &mut [u8]) -> usize {
        let Some(session) = self.sessions.lookup(&handle.raw()) else {
            return 0;
        };
        let notify = self.notify_entry(handle);
        loop {
            let read = session.rx_fifo().peek(0, out.len(), out);
            if read != 0 || out.is_empty() {
                session.rx_fifo().dequeue_drop(read);
                return read;
            }
            session.want_rx_notification();
            if session.rx_fifo().max_dequeue() != 0 {
                session.clear_rx_notification();
                continue;
            }
            match notify {
                Some(ref n) => n.rx_readable.notified().await,
                None => return 0,
            }
            session.clear_rx_notification();
        }
    }

    /// App-side async event receive via worker.
    pub async fn next_event(&self, handle: SessionHandle) -> Option<SessionEvt> {
        let session = self.sessions.lookup(&handle.raw())?;
        let notify = self.notify_entry(handle);
        loop {
            if let Some(evt) = session.evt_q().dequeue() {
                return Some(evt);
            }
            match notify {
                Some(ref n) => n.evt_readable.notified().await,
                None => return None,
            }
        }
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
