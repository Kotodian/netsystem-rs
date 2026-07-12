use std::fmt;
use std::os::fd::RawFd;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::{Local, Svm};

use crate::app::SessionHandle;
use crate::app::SessionOffsets;
use crate::app::session_msg_queue::{
    SessionEventQueue, SessionEvt, SessionEvtFlags, SessionEvtType, SessionMsgQueue, SessionSegment,
};

/// VPP-style app/session object: per-session byte FIFOs plus event queue.
///
/// Direction convention (mirrors VPP):
///   rx_fifo: transport → app  (transport enqueues bytes received from the
///          network; app peeks + dequeue_drops after consuming).
///   tx_fifo: app → transport  (app enqueues bytes to send; transport peeks
///          at the read window and dequeue_drops on ACK).
pub struct AppSession<S: SessionSegment> {
    rx_fifo: Arc<Fifo<S>>,
    tx_fifo: Arc<Fifo<S>>,
    evt_q: Arc<S::EventQueue>,
    tx_evt_q: Arc<S::EventQueue>,
    handle: SessionHandle,
}

impl<S: SessionSegment> AppSession<S> {
    /// Construct an `AppSession` from pre-created FIFOs and event queues.
    /// Used by `SessionAppRuntime<Svm>::create_app_session` to assemble a session
    /// from individually allocated shared-memory queues.
    pub fn from_parts(
        rx_fifo: Arc<Fifo<S>>,
        tx_fifo: Arc<Fifo<S>>,
        evt_q: Arc<S::EventQueue>,
        tx_evt_q: Arc<S::EventQueue>,
        handle: SessionHandle,
    ) -> Self {
        Self {
            rx_fifo,
            tx_fifo,
            evt_q,
            tx_evt_q,
            handle,
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
    pub fn rx_fifo(&self) -> &Arc<Fifo<S>> {
        &self.rx_fifo
    }

    /// Transport side: FIFO from which send bytes are peeked.
    #[inline]
    pub fn tx_fifo(&self) -> &Arc<Fifo<S>> {
        &self.tx_fifo
    }

    /// Both sides: shared event queue for this session.
    #[inline]
    pub fn evt_q(&self) -> &Arc<S::EventQueue> {
        &self.evt_q
    }

    /// Accessor for the transport-side tx event queue consumer.
    /// Used by SessionAppRuntime to drain tx completion events.
    #[inline]
    pub fn tx_evt_q(&self) -> &Arc<S::EventQueue> {
        &self.tx_evt_q
    }

    /// App-side convenience: enqueue bytes to send (app → transport). Returns
    /// number of bytes accepted (may be < `bytes.len()` if fifo full; caller
    /// retries or backpressures).
    #[inline]
    pub fn send_bytes(&self, bytes: &[u8]) -> HammerResult<usize> {
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
    pub fn enqueue_rx(&self, bytes: &[u8]) -> HammerResult<usize> {
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
    ) -> HammerResult<usize> {
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
    pub fn drop_tx_acked(&self, len: usize) -> HammerResult<usize> {
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
    pub fn push_event(&self, evt_type: SessionEvtType) -> HammerResult<()> {
        self.push_event_with_flags(evt_type, SessionEvtFlags::empty())
    }

    #[inline]
    pub fn push_event_with_flags(
        &self,
        evt_type: SessionEvtType,
        flags: SessionEvtFlags,
    ) -> HammerResult<()> {
        match evt_type {
            SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                let evt = SessionEvt::io_with_flags(self.handle.session_index(), evt_type, flags);
                self.evt_q
                    .enqueue_io(evt)
                    .map_err(|_| HammerError::internal("app session evt_q full"))?;
            }
            SessionEvtType::Connect | SessionEvtType::Close => {
                let evt = SessionEvt::ctrl(
                    self.handle.session_index(),
                    self.handle.worker_index(),
                    evt_type,
                );
                self.evt_q
                    .enqueue_ctrl(evt)
                    .map_err(|_| HammerError::internal("app session evt_q full"))?;
            }
        }
        Ok(())
    }

    #[inline]
    fn notify_tx_event(&self, wrote: usize) -> HammerResult<()> {
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
            return Err(HammerError::internal("session tx event queue full"));
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
}

impl AppSession<Svm> {
    /// Reconstruct an app session from a pre-allocated shared segment.
    /// Called by AttachClient (app process) after receiving offsets over
    /// the Unix socket. The segment must already contain valid Fifo /
    /// Session Message Queue headers at the given offsets; the signal fds
    /// must be open for reading.
    ///
    /// # Safety
    /// Caller must guarantee the segment is valid and the offsets point to
    /// correctly initialised queue headers.
    pub unsafe fn from_segment(
        handle: SessionHandle,
        seg: &Svm,
        offsets: &SessionOffsets,
        evt_q_read: Option<RawFd>,
        evt_q_write: Option<RawFd>,
        tx_evt_q_read: Option<RawFd>,
        tx_evt_q_write: Option<RawFd>,
    ) -> Self {
        let evt_q = Arc::new(unsafe {
            SessionMsgQueue::<Svm>::from_shared(
                seg.clone(),
                offsets.evt_q_off,
                evt_q_read,
                evt_q_write,
            )
        });
        let tx_evt_q = Arc::new(unsafe {
            SessionMsgQueue::<Svm>::from_shared(
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
        }
    }
}

impl AppSession<Local> {
    /// Local session: FIFOs on a Local segment; Session Message Queue for evt/tx rings.
    pub fn new_in_segment(
        seg: Local,
        config: AppSessionConfig,
        handle: SessionHandle,
        tx_evt_q: Arc<SessionMsgQueue>,
    ) -> HammerResult<Self> {
        let mut rx_fifo = Fifo::<Local>::new(seg.clone(), config.fifo_capacity)
            .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?;
        rx_fifo.enable_ooo();
        let rx_fifo = Arc::new(rx_fifo);
        let tx_fifo = Arc::new(
            Fifo::<Local>::new(seg, config.fifo_capacity)
                .map_err(|_| HammerError::internal("invalid tx fifo capacity"))?,
        );
        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let evt_q = Arc::new(
            SessionMsgQueue::with_cfg(q_nitems, ring_nitems)
                .map_err(|_| HammerError::internal("invalid app session evt_q capacity"))?,
        );
        Ok(Self {
            rx_fifo,
            tx_fifo,
            evt_q,
            tx_evt_q,
            handle,
        })
    }
}

impl<S: SessionSegment> fmt::Debug for AppSession<S> {
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
    use hammer_infra::segment::Local;

    fn new_session(config: AppSessionConfig, session_index: u32) -> AppSession<Local> {
        let handle = SessionHandle::new(session_index, 0);
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("tx_evt_q"));
        AppSession::<Local>::new_in_segment(Local::default(), config, handle, tx_evt_q)
            .expect("session")
    }

    #[test]
    fn app_session_send_bytes_clears_tx_event_when_tx_evt_q_full() {
        let tx_evt_q: Arc<SessionMsgQueue> =
            Arc::new(SessionMsgQueue::with_cfg(2, 1).expect("tx_evt_q"));
        tx_evt_q
            .enqueue_io(SessionEvt::io(99, SessionEvtType::TxDeq))
            .expect("fill io ring");
        let session = AppSession::<Local>::new_in_segment(
            Local::default(),
            AppSessionConfig::new(64, 4),
            SessionHandle::new(1, 0),
            Arc::clone(&tx_evt_q),
        )
        .expect("session");

        assert!(session.send_bytes(b"x").is_err());
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
        let session = AppSession::<Local>::new_in_segment(
            Local::default(),
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
        assert!(
            AppSession::<Local>::new_in_segment(
                Local::default(),
                AppSessionConfig::new(3, 4),
                SessionHandle::new(0, 0),
                tx_evt_q,
            )
            .is_err()
        );
    }

    #[test]
    fn app_session_evt_q_capacity_holds_requested_count() {
        let session = new_session(AppSessionConfig::new(64, 3), 1);
        for _ in 0..3 {
            session
                .push_event(SessionEvtType::Connect)
                .expect("push connect");
        }
        assert!(session.push_event(SessionEvtType::Connect).is_err());
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
