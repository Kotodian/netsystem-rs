use std::fmt;
use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::sync::Arc;

use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use thiserror::Error;
use tokio::io::unix::AsyncFd;
use tokio::sync::OnceCell;

use crate::app::SessionHandle;
use crate::app::SessionOffsets;
use crate::app::session_msg_queue::{
    SessionEventQueue, SessionEvt, SessionEvtFlags, SessionEvtType, SessionMsgQueue,
};

/// VPP-style app/session object: per-session byte FIFOs plus event queue.
///
/// Direction convention (mirrors VPP):
///   rx_fifo: transport → app  (transport enqueues bytes received from the
///          network; app peeks + dequeue_drops after consuming).
///   tx_fifo: app → transport  (app enqueues bytes to send; transport peeks
///          at the read window and dequeue_drops on ACK).
pub struct AppSession {
    rx_fifo: Arc<Fifo>,
    tx_fifo: Arc<Fifo>,
    evt_q: Arc<SessionMsgQueue>,
    tx_evt_q: Arc<SessionMsgQueue>,
    handle: SessionHandle,
    async_fd: OnceCell<AsyncFd<OwnedFd>>,
}

/// Failures owned by the app/session boundary.
#[derive(Debug, Error)]
pub enum AppSessionError {
    #[error("app worker {app_worker} already has session {session}")]
    AlreadyAttached { app_worker: usize, session: u64 },
    #[error("app worker {app_worker} has no session {session}")]
    NotFound { app_worker: usize, session: u64 },
    #[error("app session {session} event queue is full for {event:?}")]
    EventQueueFull { session: u64, event: SessionEvtType },
    #[error("app session {session} TX event queue is full")]
    TxEventQueueFull { session: u64 },
    #[error("app session RX FIFO capacity {capacity} is invalid")]
    RxFifoCapacityInvalid { capacity: usize },
    #[error("app session TX FIFO capacity {capacity} is invalid")]
    TxFifoCapacityInvalid { capacity: usize },
    #[error("app session event queue capacity {capacity} is invalid")]
    EventQueueCapacityInvalid { capacity: usize },
    #[error("app session requires a signal-read descriptor")]
    SessionSignalMissing,
    #[error("failed to duplicate app session signal descriptor")]
    SessionSignalDuplicate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register app session readiness")]
    SessionReadiness {
        #[source]
        source: std::io::Error,
    },
}

impl AppSession {
    /// Construct an `AppSession` from pre-created FIFOs and event queues.
    /// Used by Session Runtime to assemble a session from allocated queues.
    pub fn from_parts(
        rx_fifo: Arc<Fifo>,
        tx_fifo: Arc<Fifo>,
        evt_q: Arc<SessionMsgQueue>,
        tx_evt_q: Arc<SessionMsgQueue>,
        handle: SessionHandle,
    ) -> Self {
        Self {
            rx_fifo,
            tx_fifo,
            evt_q,
            tx_evt_q,
            handle,
            async_fd: OnceCell::new(),
        }
    }
    #[inline]
    pub const fn session_index(&self) -> u32 {
        self.handle.session_index()
    }

    #[inline]
    pub const fn session_handle(&self) -> SessionHandle {
        self.handle
    }

    /// Transport side: FIFO into which received bytes are copied.
    #[inline]
    pub fn rx_fifo(&self) -> &Arc<Fifo> {
        &self.rx_fifo
    }

    /// Transport side: FIFO from which send bytes are peeked.
    #[inline]
    pub fn tx_fifo(&self) -> &Arc<Fifo> {
        &self.tx_fifo
    }

    /// Both sides: shared event queue for this session.
    #[inline]
    pub fn evt_q(&self) -> &Arc<SessionMsgQueue> {
        &self.evt_q
    }

    /// Accessor for the transport-side tx event queue consumer.
    /// Used by SessionAppRuntime to drain tx completion events.
    #[inline]
    pub fn tx_evt_q(&self) -> &Arc<SessionMsgQueue> {
        &self.tx_evt_q
    }

    /// App-side convenience: enqueue bytes to send (app → transport). Returns
    /// number of bytes accepted (may be < `bytes.len()` if fifo full; caller
    /// retries or backpressures).
    #[inline]
    pub fn send_bytes(&self, bytes: &[u8]) -> Result<usize, AppSessionError> {
        let wrote = self.tx_fifo.enqueue(bytes);
        self.notify_tx_event(wrote)?;
        Ok(wrote)
    }

    /// App-side convenience: copy received bytes out (transport → app). Returns
    /// number of bytes copied into `out`; caller calls `consume_rx` after
    /// processing.
    #[inline]
    pub fn recv_bytes(&self, out: &mut [u8]) -> usize {
        self.rx_fifo.peek(0, out.len(), out)
    }

    /// App-side convenience: drop `len` bytes from the head of `rx_fifo` after
    /// the app has consumed them. Mirrors VPP `svm_fifo_dequeue`.
    #[inline]
    pub fn consume_rx(&self, len: usize) -> usize {
        self.rx_fifo.dequeue_drop(len)
    }

    /// App-side convenience: drain up to `out.len()` events from this session's
    /// event queue. Returns the count written.
    #[inline]
    pub fn poll_events(&self, out: &mut [SessionEvt]) -> usize {
        self.evt_q.dequeue_batch(out)
    }

    /// App-side convenience: read the edge-triggered signal flag. Returns true
    /// if a producer signalled since the last read.
    #[inline]
    pub fn read_signal(&self) -> bool {
        self.evt_q.read_signal()
    }

    /// Transport-side convenience: enqueue received bytes and emit a
    /// `SessionEvtType::RxEnq` event with edge-triggered signal semantics.
    /// Returns bytes enqueued. The event is only signalled if the fifo
    /// transitioned empty → non-empty this call AND the app had set
    /// `want_notification`.
    #[inline]
    pub fn enqueue_rx(&self, bytes: &[u8]) -> Result<usize, AppSessionError> {
        self.enqueue_rx_with_flags(bytes, SessionEvtFlags::empty())
    }

    /// Like [`Self::enqueue_rx`], but attaches [`SessionEvtFlags`] on the RxEnq
    /// event. Urgent delivery always posts an event (still requires
    /// `want_notification`) so the app observes the mark even when the FIFO
    /// was already non-empty.
    #[inline]
    pub fn enqueue_rx_with_flags(
        &self,
        bytes: &[u8],
        flags: SessionEvtFlags,
    ) -> Result<usize, AppSessionError> {
        let wrote = self.rx_fifo.enqueue(bytes);
        if wrote == 0 {
            return Ok(0);
        }
        let urgent = flags.contains(SessionEvtFlags::URGENT);
        if urgent || self.rx_fifo.should_signal(wrote) {
            self.push_event_with_flags(SessionEvtType::RxEnq, flags)?;
        }
        Ok(wrote)
    }

    /// Transport-side convenience: drop acked bytes from tx_fifo and emit
    /// `SessionEvtType::TxDeq` (edge-triggered by FIFO dequeue notification).
    #[inline]
    pub fn drop_tx_acked(&self, len: usize) -> Result<usize, AppSessionError> {
        let dropped = self.tx_fifo.dequeue_drop(len);
        if dropped > 0 && self.tx_fifo.needs_deq_notification(dropped) {
            self.push_event(SessionEvtType::TxDeq)?;
        }
        Ok(dropped)
    }

    #[inline]
    pub fn clear_tx_event(&self) {
        self.tx_fifo.unset_event();
    }

    /// Transport-side convenience: post a session event to the app's queue.
    /// Used by session runtime on RX enqueue / connect / close.
    ///
    /// IO events (`RxEnq` / `TxDeq`) carry session index only. Control events
    /// (`Connect` / `Close`) carry the full Session Handle, matching VPP
    /// `session_event_t` identity rules.
    #[inline]
    pub fn push_event(&self, evt_type: SessionEvtType) -> Result<(), AppSessionError> {
        self.push_event_with_flags(evt_type, SessionEvtFlags::empty())
    }

    #[inline]
    pub fn push_event_with_flags(
        &self,
        evt_type: SessionEvtType,
        flags: SessionEvtFlags,
    ) -> Result<(), AppSessionError> {
        match evt_type {
            SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                let evt = SessionEvt::io_with_flags(self.handle.session_index(), evt_type, flags);
                self.evt_q
                    .enqueue_io(evt)
                    .map_err(|_| AppSessionError::EventQueueFull {
                        session: self.handle.raw(),
                        event: evt_type,
                    })?;
            }
            SessionEvtType::Connect | SessionEvtType::Close => {
                let evt = SessionEvt::ctrl(
                    self.handle.session_index(),
                    self.handle.worker_index(),
                    evt_type,
                );
                self.evt_q
                    .enqueue_ctrl(evt)
                    .map_err(|_| AppSessionError::EventQueueFull {
                        session: self.handle.raw(),
                        event: evt_type,
                    })?;
            }
        }
        Ok(())
    }

    #[inline]
    fn notify_tx_event(&self, wrote: usize) -> Result<(), AppSessionError> {
        if wrote == 0 || !self.tx_fifo.set_event() {
            return Ok(());
        }
        if self
            .tx_evt_q
            .enqueue_io(SessionEvt::io(
                self.handle.session_index(),
                SessionEvtType::TxDeq,
            ))
            .is_err()
        {
            self.tx_fifo.unset_event();
            return Err(AppSessionError::TxEventQueueFull {
                session: self.handle.raw(),
            });
        }
        Ok(())
    }

    /// App-side: ask the runtime to wake the app when the fifo transitions
    /// empty → non-empty. Mirrors VPP `svm_fifo_set_event`.
    #[inline]
    pub fn want_rx_notification(&self) {
        self.rx_fifo.want_notification();
    }

    /// App-side: clear the want-notification flag before sleeping.
    #[inline]
    pub fn clear_rx_notification(&self) {
        self.rx_fifo.clear_notification();
    }

    /// Same for tx (app wants wake when tx fifo has space again).
    #[inline]
    pub fn want_tx_notification(&self) {
        self.tx_fifo.want_deq_notification();
    }

    #[inline]
    pub fn clear_tx_notification(&self) {
        self.tx_fifo.clear_deq_notification();
    }

    /// Reset all state (used on session close / reuse).
    pub fn clear(&self) {
        self.rx_fifo.clear();
        self.tx_fifo.clear();
        self.evt_q.clear();
    }

    /// App-side async receive. Waits for the RX FIFO to become readable and
    /// copies up to `out.len()` bytes, advancing the FIFO head.
    pub async fn recv(&self, out: &mut [u8]) -> Result<usize, AppSessionError> {
        loop {
            let read = self.rx_fifo.peek(0, out.len(), out);
            if read != 0 || out.is_empty() {
                self.rx_fifo.dequeue_drop(read);
                return Ok(read);
            }
            self.rx_fifo.want_notification();
            if self.rx_fifo.max_dequeue() != 0 {
                self.rx_fifo.clear_notification();
                continue;
            }
            let wait_result = self.wait_for_event().await;
            self.rx_fifo.clear_notification();
            wait_result?;
        }
    }

    /// App-side async send. Applies backpressure while the TX FIFO is full.
    pub async fn send_all(&self, bytes: &[u8]) -> Result<usize, AppSessionError> {
        let mut written = 0usize;
        while written < bytes.len() {
            let accepted = self.send_bytes(&bytes[written..])?;
            if accepted != 0 {
                written += accepted;
                continue;
            }
            self.tx_fifo.want_deq_notification();
            if self.tx_fifo.max_enqueue() != 0 {
                self.tx_fifo.clear_deq_notification();
                continue;
            }
            let wait_result = self.wait_for_event().await;
            self.tx_fifo.clear_deq_notification();
            wait_result?;
        }
        Ok(written)
    }

    /// App-side async event receive from the session event queue.
    pub async fn next_event(&self) -> Result<SessionEvt, AppSessionError> {
        loop {
            if let Some(event) = self.evt_q.dequeue() {
                return Ok(event);
            }
            self.wait_for_event().await?;
        }
    }

    async fn wait_for_event(&self) -> Result<(), AppSessionError> {
        let mut guard = self
            .async_fd()
            .await?
            .readable()
            .await
            .map_err(|source| AppSessionError::SessionReadiness { source })?;
        self.evt_q.drain();
        guard.clear_ready();
        Ok(())
    }

    async fn async_fd(&self) -> Result<&AsyncFd<OwnedFd>, AppSessionError> {
        self.async_fd
            .get_or_try_init(|| async {
                let read_fd = self
                    .evt_q
                    .read_fd()
                    .ok_or(AppSessionError::SessionSignalMissing)?;
                // SAFETY: the queue owns the live descriptor for the session
                // lifetime; this borrow is used only to make an owned duplicate.
                let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
                let owned = borrowed
                    .try_clone_to_owned()
                    .map_err(|source| AppSessionError::SessionSignalDuplicate { source })?;
                AsyncFd::new(owned).map_err(|source| AppSessionError::SessionReadiness { source })
            })
            .await
    }
}

impl AppSession {
    /// Reconstruct an app session from a pre-allocated shared segment.
    /// Called by AppClient (app process) after receiving offsets over
    /// the Unix socket. The segment must already contain valid Fifo /
    /// Session Message Queue headers at the given offsets; the signal fds
    /// must be open for reading.
    ///
    /// # Safety
    /// Caller must guarantee the segment is valid and the offsets point to
    /// correctly initialised queue headers.
    pub unsafe fn from_segment(
        handle: SessionHandle,
        seg: &Segment,
        offsets: &SessionOffsets,
        evt_q_read: Option<RawFd>,
        evt_q_write: Option<RawFd>,
        tx_evt_q_read: Option<RawFd>,
        tx_evt_q_write: Option<RawFd>,
    ) -> Self {
        let evt_q = Arc::new(unsafe {
            SessionMsgQueue::from_shared(seg.clone(), offsets.evt_q_off, evt_q_read, evt_q_write)
        });
        let tx_evt_q = Arc::new(unsafe {
            SessionMsgQueue::from_shared(
                seg.clone(),
                offsets.tx_evt_q_off,
                tx_evt_q_read,
                tx_evt_q_write,
            )
        });
        Self {
            rx_fifo: Arc::new(unsafe { Fifo::from_shared(seg.clone(), offsets.rx_fifo_off) }),
            tx_fifo: Arc::new(unsafe { Fifo::from_shared(seg.clone(), offsets.tx_fifo_off) }),
            evt_q,
            tx_evt_q,
            handle,
            async_fd: OnceCell::new(),
        }
    }
}

impl AppSession {
    /// Construct a session in an application-owned Segment.
    pub fn new_in_segment(
        seg: Segment,
        config: AppSessionConfig,
        handle: SessionHandle,
        tx_evt_q: Arc<SessionMsgQueue>,
    ) -> Result<Self, AppSessionError> {
        let mut rx_fifo = Fifo::new(seg.clone(), config.fifo_capacity).map_err(|_| {
            AppSessionError::RxFifoCapacityInvalid {
                capacity: config.fifo_capacity,
            }
        })?;
        rx_fifo.enable_ooo();
        let rx_fifo = Arc::new(rx_fifo);
        let tx_fifo = Arc::new(Fifo::new(seg.clone(), config.fifo_capacity).map_err(|_| {
            AppSessionError::TxFifoCapacityInvalid {
                capacity: config.fifo_capacity,
            }
        })?);
        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let evt_q_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).map_err(|_| {
            AppSessionError::EventQueueCapacityInvalid {
                capacity: config.evt_q_capacity,
            }
        })?;
        let evt_q_offset =
            seg.alloc(evt_q_bytes, 64)
                .ok_or(AppSessionError::EventQueueCapacityInvalid {
                    capacity: config.evt_q_capacity,
                })?;
        let evt_q = Arc::new(
            unsafe {
                SessionMsgQueue::init_at_with_signal(seg, evt_q_offset, q_nitems, ring_nitems)
            }
            .map_err(|_| AppSessionError::EventQueueCapacityInvalid {
                capacity: config.evt_q_capacity,
            })?,
        );
        Ok(Self {
            rx_fifo,
            tx_fifo,
            evt_q,
            tx_evt_q,
            handle,
            async_fd: OnceCell::new(),
        })
    }
}

impl fmt::Debug for AppSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppSession")
            .field("session_index", &self.handle.session_index())
            .field("worker_index", &self.handle.worker_index())
            .finish_non_exhaustive()
    }
}

/// Configuration for `AppSession::new`. Fifo capacity must be a power of two
/// >= 2. `evt_q_capacity` is the usable event count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionConfig {
    pub fifo_capacity: usize,
    pub evt_q_capacity: usize,
}

impl AppSessionConfig {
    /// Defaults aligned with VPP typical session fifos.
    pub const DEFAULT: Self = Self {
        fifo_capacity: 64 * 1024,
        evt_q_capacity: 16,
    };

    #[inline]
    pub const fn new(fifo_capacity: usize, evt_q_capacity: usize) -> Self {
        Self {
            fifo_capacity,
            evt_q_capacity,
        }
    }
}

impl Default for AppSessionConfig {
    #[inline]
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::segment::Segment;

    fn new_session(config: AppSessionConfig, session_index: u32) -> AppSession {
        let handle = SessionHandle::new(session_index, 0);
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("tx_evt_q"));
        AppSession::new_in_segment(Segment::default(), config, handle, tx_evt_q).expect("session")
    }

    #[test]
    fn app_session_send_bytes_clears_tx_event_when_tx_evt_q_full() {
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(2, 1).expect("tx_evt_q"));
        tx_evt_q
            .enqueue_io(SessionEvt::io(99, SessionEvtType::TxDeq))
            .expect("fill io ring");
        let session = AppSession::new_in_segment(
            Segment::default(),
            AppSessionConfig::new(64, 4),
            SessionHandle::new(1, 0),
            Arc::clone(&tx_evt_q),
        )
        .expect("session");

        let error = session
            .send_bytes(b"x")
            .expect_err("full TX event queue must reject notification");
        assert!(matches!(
            error,
            AppSessionError::TxEventQueueFull { session }
                if session == SessionHandle::new(1, 0).raw()
        ));
        assert!(!session.tx_fifo().has_event());

        assert!(tx_evt_q.dequeue().is_some());
        assert_eq!(session.send_bytes(b"y").expect("retry after drain"), 1);
        assert!(session.tx_fifo().has_event());
        let evt = tx_evt_q.dequeue().expect("tx deq after retry");
        assert_eq!(evt.evt_type, SessionEvtType::TxDeq);
        assert_eq!(evt.session_index(), 1);
    }

    #[test]
    fn app_session_send_recv_round_trips() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        assert_eq!(session.send_bytes(b"hello").expect("send"), 5);
        let mut tx_out = [0u8; 8];
        assert_eq!(session.tx_fifo().peek(0, 8, &mut tx_out), 5);
        assert_eq!(&tx_out[..5], b"hello");

        assert_eq!(session.enqueue_rx(b"hello").expect("enqueue rx"), 5);
        let mut out = [0u8; 8];
        assert_eq!(session.recv_bytes(&mut out), 5);
        assert_eq!(&out[..5], b"hello");
        assert_eq!(session.consume_rx(5), 5);
        assert_eq!(session.recv_bytes(&mut out), 0);
    }

    #[test]
    fn app_session_enqueue_rx_emits_edge_triggered_signal() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        session.want_rx_notification();
        assert_eq!(session.enqueue_rx(b"abc").expect("enqueue rx"), 3);
        assert!(session.read_signal());
        assert!(!session.read_signal());
        assert_eq!(session.enqueue_rx(b"de").expect("enqueue rx"), 2);
        assert!(!session.read_signal());
    }

    #[test]
    fn app_session_enqueue_rx_emits_rx_event_when_requested() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        assert_eq!(session.enqueue_rx(b"abc").expect("enqueue rx"), 3);
        assert_eq!(
            session.poll_events(&mut [SessionEvt::io(0, SessionEvtType::Close)]),
            0
        );

        session.want_rx_notification();
        assert_eq!(session.enqueue_rx(b"def").expect("enqueue rx"), 3);
        assert_eq!(
            session.poll_events(&mut [SessionEvt::io(0, SessionEvtType::Close)]),
            0
        );

        assert_eq!(session.consume_rx(6), 6);
        session.want_rx_notification();
        assert_eq!(session.enqueue_rx(b"ghi").expect("enqueue rx"), 3);
        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::RxEnq);
    }

    #[test]
    fn app_session_enqueue_rx_urgent_marks_rx_event_flag() {
        use crate::app::SessionEvtFlags;

        let session = new_session(AppSessionConfig::new(64, 4), 1);
        session.want_rx_notification();
        assert_eq!(
            session
                .enqueue_rx_with_flags(b"urg", SessionEvtFlags::URGENT)
                .expect("enqueue urgent"),
            3
        );
        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::RxEnq);
        assert!(out[0].flags().contains(SessionEvtFlags::URGENT));

        // Non-urgent path keeps flags clear (edge-triggered: may not fire while data pending).
        assert_eq!(session.consume_rx(3), 3);
        session.want_rx_notification();
        assert_eq!(session.enqueue_rx(b"ok").expect("enqueue"), 2);
        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::RxEnq);
        assert!(out[0].flags().is_empty());
    }

    #[test]
    fn app_session_push_event_round_trips() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        session
            .push_event(SessionEvtType::Connect)
            .expect("push connect");
        let mut out = [SessionEvt::io(0, SessionEvtType::RxEnq); 4];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].session_index(), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::Connect);
        assert_eq!(out[0].worker_index(), 0);
    }

    #[test]
    fn app_session_close_event_carries_session_handle() {
        let handle = SessionHandle::new(11, 7);
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("tx_evt_q"));
        let session = AppSession::new_in_segment(
            Segment::default(),
            AppSessionConfig::new(64, 4),
            handle,
            tx_evt_q,
        )
        .expect("session");
        session
            .push_event(SessionEvtType::Close)
            .expect("push close");
        let mut out = [SessionEvt::io(0, SessionEvtType::RxEnq)];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::Close);
        assert_eq!(out[0].session_index(), 11);
        assert_eq!(out[0].worker_index(), 7);
    }

    #[test]
    fn app_session_drop_tx_acked_advances_tx_fifo() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        assert_eq!(session.send_bytes(b"abcdef").expect("send"), 6);
        assert_eq!(session.drop_tx_acked(3).expect("drop tx"), 3);
        assert_eq!(session.tx_fifo().max_dequeue(), 3);
        let mut out = [0u8; 6];
        assert_eq!(session.tx_fifo().peek(0, 6, &mut out), 3);
        assert_eq!(&out[..3], b"def");
    }

    #[test]
    fn app_session_drop_tx_acked_emits_dequeue_notification() {
        let session = new_session(AppSessionConfig::new(8, 4), 1);
        assert_eq!(session.send_bytes(b"abcdefgh").expect("send"), 8);
        assert!(session.tx_fifo().is_full());

        assert_eq!(session.drop_tx_acked(1).expect("drop tx"), 1);
        assert!(!session.read_signal());

        session.want_tx_notification();
        assert_eq!(session.drop_tx_acked(1).expect("drop tx"), 1);
        assert!(session.read_signal());
        assert!(!session.read_signal());
        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(session.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::TxDeq);

        assert_eq!(session.drop_tx_acked(1).expect("drop tx"), 1);
        assert!(!session.read_signal());
    }

    #[test]
    fn app_session_clear_resets_all() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        session.send_bytes(b"x").expect("send");
        session.enqueue_rx(b"y").expect("enqueue rx");
        session
            .push_event(SessionEvtType::Close)
            .expect("push close");
        session.clear();
        assert!(session.rx_fifo().is_empty());
        assert!(session.tx_fifo().is_empty());
        let mut out = [SessionEvt::io(0, SessionEvtType::RxEnq); 4];
        assert_eq!(session.poll_events(&mut out), 0);
    }

    #[test]
    fn app_session_rejects_invalid_fifo_capacity() {
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("tx_evt_q"));
        let error = AppSession::new_in_segment(
            Segment::default(),
            AppSessionConfig::new(3, 4),
            SessionHandle::new(0, 0),
            tx_evt_q,
        )
        .expect_err("invalid FIFO capacity must be rejected");
        assert!(matches!(
            error,
            AppSessionError::RxFifoCapacityInvalid { capacity: 3 }
        ));
    }

    #[test]
    fn app_session_evt_q_capacity_holds_requested_count() {
        let session = new_session(AppSessionConfig::new(64, 3), 1);
        for _ in 0..3 {
            session
                .push_event(SessionEvtType::Connect)
                .expect("push connect");
        }
        let error = session
            .push_event(SessionEvtType::Connect)
            .expect_err("full event queue must reject event");
        assert!(matches!(
            error,
            AppSessionError::EventQueueFull {
                event: SessionEvtType::Connect,
                ..
            }
        ));
    }

    #[test]
    fn app_session_rx_fifo_supports_ooo_enqueue() {
        let session = new_session(AppSessionConfig::new(64, 4), 1);
        let result = session
            .rx_fifo()
            .enqueue_ooo(5, b"world")
            .expect("rx fifo should support ooo");
        assert_eq!(result.delivered, 0);
        assert_eq!(session.rx_fifo().max_dequeue(), 0);
    }
}
