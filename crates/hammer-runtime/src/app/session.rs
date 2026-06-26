use std::fmt;
use std::sync::Arc;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::svm_fifo::SvmFifo;
use hammer_infra::svm_msg_q::{SessionEvt, SessionEvtType, SvmMsgQ};

/// VPP `app_session` analog: per-session byte FIFOs + event-queue handle.
/// Transport owns the session side; app owns the app side; both reference the
/// same `Arc<SvmFifo>`/`Arc<SvmMsgQ>` (cross-process ready: future Stage F
/// replaces the `Arc` with an mmap+offset handle, no caller change).
///
/// Direction convention (mirrors VPP):
///   rx_fifo: transport → app  (transport enqueues bytes received from the
///          network; app peeks + dequeue_drops after consuming).
///   tx_fifo: app → transport  (app enqueues bytes to send; transport peeks
///          at the read window and dequeue_drops on ACK).
#[derive(Clone)]
pub struct SessionAppBoundary {
    rx_fifo: Arc<SvmFifo>,
    tx_fifo: Arc<SvmFifo>,
    evt_q: Arc<SvmMsgQ>,
    session_index: u32,
}

impl SessionAppBoundary {
    /// Construct a fresh in-process boundary. `config` fixes fifo capacities
    /// and event-queue depth. The fifos and event queue are heap-allocated
    /// (Stage F swaps to mmap-backed storage without changing this API).
    pub fn new(config: AppSessionConfig, session_index: u32) -> HammerResult<Self> {
        let rx_fifo = Arc::new(
            SvmFifo::with_capacity(config.fifo_capacity)
                .map_err(|_| HammerError::internal("invalid rx fifo capacity"))?,
        );
        let tx_fifo = Arc::new(
            SvmFifo::with_capacity(config.fifo_capacity)
                .map_err(|_| HammerError::internal("invalid tx fifo capacity"))?,
        );
        // SvmMsgQ::with_capacity(N) uses LockFreeRing, which requires a power-of-two
        // size and holds N-1 events (one slot reserved). Request enough ring slots
        // for `evt_q_capacity` usable events, then round up to the next power of two.
        let ring_slots = config
            .evt_q_capacity
            .checked_add(1)
            .ok_or_else(|| HammerError::internal("app session evt_q capacity overflow"))?;
        let ring_size = ring_slots.next_power_of_two().max(2);
        let evt_q = Arc::new(
            SvmMsgQ::with_capacity(ring_size)
                .map_err(|_| HammerError::internal("invalid app session evt_q capacity"))?,
        );
        Ok(Self {
            rx_fifo,
            tx_fifo,
            evt_q,
            session_index,
        })
    }

    #[inline]
    pub const fn session_index(&self) -> u32 {
        self.session_index
    }

    /// Transport side: FIFO into which received bytes are copied.
    #[inline]
    pub fn rx_fifo(&self) -> &Arc<SvmFifo> {
        &self.rx_fifo
    }

    /// Transport side: FIFO from which send bytes are peeked.
    #[inline]
    pub fn tx_fifo(&self) -> &Arc<SvmFifo> {
        &self.tx_fifo
    }

    /// Both sides: shared event queue for this session.
    #[inline]
    pub fn evt_q(&self) -> &Arc<SvmMsgQ> {
        &self.evt_q
    }

    /// App-side convenience: enqueue bytes to send (app → transport). Returns
    /// number of bytes accepted (may be < `bytes.len()` if fifo full; caller
    /// retries or backpressures).
    #[inline]
    pub fn send_bytes(&self, bytes: &[u8]) -> usize {
        self.tx_fifo.enqueue(bytes)
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

    /// Transport-side convenience: post a session event to the app's queue.
    /// Used by session runtime on RX enqueue / connect / close. Caller checks
    /// `SvmFifo::should_signal` to decide whether to also `evt_q.signal()`.
    #[inline]
    pub fn push_event(&self, evt_type: SessionEvtType) -> HammerResult<()> {
        self.evt_q
            .enqueue(SessionEvt {
                session_index: self.session_index,
                evt_type,
            })
            .map_err(|_| HammerError::internal("app session evt_q full"))
    }

    /// Transport-side convenience: enqueue received bytes and emit a
    /// `SessionEvtType::RxEnq` event with edge-triggered signal semantics.
    /// Returns bytes enqueued. The event is only signalled if the fifo
    /// transitioned empty → non-empty this call AND the app had set
    /// `want_notification`.
    #[inline]
    pub fn enqueue_rx(&self, bytes: &[u8]) -> usize {
        let wrote = self.rx_fifo.enqueue(bytes);
        if wrote > 0 && self.rx_fifo.should_signal(wrote) {
            self.evt_q.signal();
        }
        wrote
    }

    /// Transport-side convenience: drop acked bytes from tx_fifo and emit
    /// `SessionEvtType::TxDeq` (edge-triggered by `should_signal`).
    #[inline]
    pub fn drop_tx_acked(&self, len: usize) -> usize {
        let dropped = self.tx_fifo.dequeue_drop(len);
        if dropped > 0 && self.tx_fifo.should_signal(dropped) {
            self.evt_q.signal();
        }
        dropped
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
        self.tx_fifo.want_notification();
    }

    #[inline]
    pub fn clear_tx_notification(&self) {
        self.tx_fifo.clear_notification();
    }

    /// Reset all state (used on session close / reuse).
    pub fn clear(&self) {
        self.rx_fifo.clear();
        self.tx_fifo.clear();
        self.evt_q.clear();
    }
}

impl fmt::Debug for SessionAppBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAppBoundary")
            .field("session_index", &self.session_index)
            .finish_non_exhaustive()
    }
}

/// Configuration for `SessionAppBoundary::new`. Fifo capacity must be a
/// power of two >= 2. `evt_q_capacity` is the usable event count (the
/// implementation adds +1 for the reserved ring slot).
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

    #[test]
    fn session_app_boundary_send_recv_round_trips() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 4), 1).expect("boundary");
        assert_eq!(boundary.send_bytes(b"hello"), 5);
        let mut tx_out = [0u8; 8];
        assert_eq!(boundary.tx_fifo().peek(0, 8, &mut tx_out), 5);
        assert_eq!(&tx_out[..5], b"hello");

        assert_eq!(boundary.enqueue_rx(b"hello"), 5);
        let mut out = [0u8; 8];
        assert_eq!(boundary.recv_bytes(&mut out), 5);
        assert_eq!(&out[..5], b"hello");
        assert_eq!(boundary.consume_rx(5), 5);
        assert_eq!(boundary.recv_bytes(&mut out), 0);
    }

    #[test]
    fn session_app_boundary_enqueue_rx_emits_edge_triggered_signal() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 4), 1).expect("boundary");
        boundary.want_rx_notification();
        assert_eq!(boundary.enqueue_rx(b"abc"), 3);
        assert!(boundary.read_signal());
        assert!(!boundary.read_signal());
        assert_eq!(boundary.enqueue_rx(b"de"), 2);
        assert!(!boundary.read_signal());
    }

    #[test]
    fn session_app_boundary_push_event_round_trips() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 4), 1).expect("boundary");
        boundary
            .push_event(SessionEvtType::Connect)
            .expect("push connect");
        let mut out = [SessionEvt {
            session_index: 0,
            evt_type: SessionEvtType::RxEnq,
        }; 4];
        assert_eq!(boundary.poll_events(&mut out), 1);
        assert_eq!(out[0].session_index, 1);
        assert_eq!(out[0].evt_type, SessionEvtType::Connect);
    }

    #[test]
    fn session_app_boundary_drop_tx_acked_advances_tx_fifo() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 4), 1).expect("boundary");
        assert_eq!(boundary.send_bytes(b"abcdef"), 6);
        assert_eq!(boundary.drop_tx_acked(3), 3);
        assert_eq!(boundary.tx_fifo().max_dequeue(), 3);
        let mut out = [0u8; 6];
        assert_eq!(boundary.tx_fifo().peek(0, 6, &mut out), 3);
        assert_eq!(&out[..3], b"def");
    }

    #[test]
    fn session_app_boundary_clear_resets_all() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 4), 1).expect("boundary");
        boundary.send_bytes(b"x");
        boundary.enqueue_rx(b"y");
        boundary
            .push_event(SessionEvtType::Close)
            .expect("push close");
        boundary.clear();
        assert!(boundary.rx_fifo().is_empty());
        assert!(boundary.tx_fifo().is_empty());
        let mut out = [SessionEvt {
            session_index: 0,
            evt_type: SessionEvtType::RxEnq,
        }; 4];
        assert_eq!(boundary.poll_events(&mut out), 0);
    }

    #[test]
    fn session_app_boundary_rejects_invalid_fifo_capacity() {
        assert!(SessionAppBoundary::new(AppSessionConfig::new(3, 4), 0).is_err());
    }

    #[test]
    fn session_app_boundary_evt_q_capacity_holds_requested_count() {
        let boundary = SessionAppBoundary::new(AppSessionConfig::new(64, 3), 1).expect("boundary");
        for _ in 0..3 {
            boundary
                .push_event(SessionEvtType::Connect)
                .expect("push connect");
        }
        assert!(boundary.push_event(SessionEvtType::Connect).is_err());
    }
}
