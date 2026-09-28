use std::time::Instant;

use crate::{TcpError, TcpSeq, TcpState};
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::Pool;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::{DataPlaneMain, DataWorkerId};
use hammer_runtime::{RuntimeError, RuntimeResult};

use super::connection::TcpTimerAction;
use super::lookup::TcpLookupState;
use super::timers::{TCP_TIMER_EXPIRY_BUDGET, TCP_TIMER_KIND_COUNT, TcpTimerKind, TcpTimerToken};
use super::{TcpConnection, TcpNodeError, enqueue_tcp_segment};
use hammer_service::session::node::{SessionQueueNext, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionPacketizedTransport, SessionPacketizedTx, SessionTransport, SessionWorker,
    TransportSendFlags, TransportSendParams, TxBatchBuffer,
};
const DEFAULT_TCP_CONNECTION_CAPACITY: usize = 1024;
const TCP_APP_RX_MIN_FREE: usize = 4 << 10;
const TCP_APP_RX_MAX_FREE: usize = 128 << 10;

#[repr(C)]
pub struct TcpWorker {
    cacheline0: CacheLineAlignMark,
    pub(crate) connections: Pool<TcpConnection>,
    pending_deq_acked: Vec<u32>,
    pending_disconnects: Vec<u32>,
    pending_resets: Vec<u32>,
    pub(crate) time_us: f64,
    pub(crate) time_tstamp: u32,
    pub(crate) max_timers_per_loop: u32,
    pub(super) pending_timers: FifoQueue<TcpTimerToken>,
    cacheline1: CacheLineAlignMark,
    cached_opts: [u8; 40],
    tx_buffers: Vec<u32>,
    pending_cleanups: FifoQueue<TcpCleanupRequest>,
    pub(crate) tco_next_node: [SessionQueueNext; 2],
    pub(super) timer_wheel: TimerWheel1t2w2048sl<u32>,
    pub(super) expired_timers: Vec<u32>,
    pub(super) last_timer_update: Instant,
    pub(crate) time_origin: Instant,
    pub(crate) time_origin_seconds: Option<f64>,
    cacheline2: CacheLineAlignMark,
    pub(crate) lookup: TcpLookupState,
    pub(crate) protocol: u8,
}

struct TcpCleanupRequest {
    free_time: Instant,
    connection_index: u32,
}

const _: () = {
    assert!(core::mem::align_of::<TcpWorker>() == CACHE_LINE);
    assert!(core::mem::offset_of!(TcpWorker, cacheline0) == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline1) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(TcpWorker, cacheline2) % CACHE_LINE == 0);
};

impl TcpWorker {
    #[inline]
    pub fn new(worker: DataWorkerId, protocol: u8) -> Self {
        let time_origin = Instant::now();
        Self {
            cacheline0: CacheLineAlignMark,
            connections: Pool::with_capacity(DEFAULT_TCP_CONNECTION_CAPACITY),
            pending_deq_acked: Vec::with_capacity(256),
            pending_disconnects: Vec::with_capacity(256),
            pending_resets: Vec::with_capacity(256),
            time_us: 0.0,
            time_tstamp: 0,
            max_timers_per_loop: 10,
            pending_timers: FifoQueue::new(),
            cacheline1: CacheLineAlignMark,
            cached_opts: [0; 40],
            tx_buffers: Vec::new(),
            pending_cleanups: FifoQueue::new(),
            tco_next_node: [SessionQueueNext::from_slot(0); 2],
            timer_wheel: TimerWheel1t2w2048sl::with_timer_ids(
                TCP_TIMER_EXPIRY_BUDGET,
                TCP_TIMER_KIND_COUNT,
            ),
            expired_timers: Vec::new(),
            last_timer_update: time_origin,
            time_origin,
            time_origin_seconds: None,
            cacheline2: CacheLineAlignMark,
            lookup: TcpLookupState::new(worker),
            protocol,
        }
    }

    #[inline]
    pub(crate) fn has_connection_capacity(&self) -> bool {
        self.connections.len() < self.connections.capacity()
    }

    #[inline]
    pub(crate) fn insert_connection(&mut self, connection: TcpConnection) -> u32 {
        let index = self.connections.insert(connection);
        self.connections
            .get_mut(index)
            .expect("inserted TCP connection remains in its pool")
            .base
            .connection_index = index;
        index
    }

    #[inline]
    pub(crate) fn connection(&self, index: u32) -> Option<&TcpConnection> {
        self.connections.get(index)
    }

    #[inline]
    pub(crate) fn connection_mut(&mut self, index: u32) -> Option<&mut TcpConnection> {
        self.connections.get_mut(index)
    }

    #[inline]
    pub(crate) fn remove_connection(&mut self, index: u32) -> TcpConnection {
        self.connections
            .remove(index)
            .expect("TCP connection index remains live during removal")
    }

    fn remove_closed_connection(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
    ) -> RuntimeResult<()> {
        let Some(connection) = self.connections.get(index) else {
            return Ok(());
        };
        if connection.state() != TcpState::Closed {
            return Ok(());
        }
        let close_reason = connection.close_reason();
        let session_id = connection.session_id();
        if close_reason == Some(crate::TcpCloseReason::RemoteReset) {
            sessions.notify_transport_reset(session_id, index)?;
        } else {
            sessions.notify_transport_closed(session_id, index)?;
        }
        self.lookup.forget_session(session_id);
        self.lookup.forget_pending_open(session_id);
        self.connections.remove(index);
        sessions.notify_transport_deleted(session_id, index)?;
        Ok(())
    }

    fn control_output(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &mut DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::Frame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let (session_id, segment, has_pending_sack, is_ip4) = {
            let Self {
                connections,
                lookup,
                timer_wheel: timers,
                ..
            } = self;
            let connection = connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            let session_id = connection.session_id();
            let has_pending_tx = sessions.has_pending_send(session_id);
            let capabilities = lookup
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let segment =
                connection.on_tcp_ready(index, timers, has_pending_tx, capabilities, now)?;
            (
                session_id,
                segment,
                connection.has_pending_sack_output(),
                connection.remote().is_ipv4(),
            )
        };
        if let Some(segment) = segment {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(
                    runtime,
                    frame,
                    self.tco_next_node[usize::from(!is_ip4)],
                    output,
                    index,
                    segment,
                )?;
            } else {
                sessions.mark_ready(session_id);
            }
        }
        let has_pending_tx = sessions.has_pending_send(session_id);
        if segment.is_none() && !has_pending_tx && has_pending_sack {
            sessions.mark_ready(session_id);
        }
        self.remove_closed_connection(sessions, index)?;
        Ok(())
    }
}

/// Deprecated compatibility implementation for the old Session Queue path.
/// New transport operations belong to `Transport<IpTransportEndpointConfig> for TcpMain`.
impl SessionTransport for TcpWorker {
    type Tx = SessionPacketizedTx;

    #[inline]
    fn protocol(&self) -> u8 {
        self.protocol
    }

    fn app_rx_evt(
        &mut self,
        index: u32,
        rx_available: usize,
        rx_capacity: usize,
        runtime: &mut DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::Frame,
        output: &mut SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        let zero_receive_window_sent = {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            connection.set_rcv_wnd(rx_available);
            connection.zero_receive_window_sent()
        };
        if !zero_receive_window_sent {
            return Ok(false);
        }
        let min_free = (rx_capacity >> 3).clamp(TCP_APP_RX_MIN_FREE, TCP_APP_RX_MAX_FREE);
        if rx_available < min_free {
            return Ok(true);
        }
        let mut candidate = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?
            .clone();
        let segment = candidate.receive_window_update_segment(rx_available)?;
        enqueue_tcp_segment(
            runtime,
            frame,
            self.tco_next_node[usize::from(!candidate.remote().is_ipv4())],
            output,
            index,
            segment,
        )?;
        *self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)? = candidate;
        Ok(false)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker,
        runtime: &mut DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::Frame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.advance_timer_wheel(now);
        let mut dispatched = 0;
        while dispatched < self.max_timers_per_loop {
            let Some(token) = self.take_pending_timer() else {
                break;
            };
            dispatched += 1;
            let (session_id, outcome, is_ip4) = {
                let Self {
                    connections,
                    lookup,
                    timer_wheel: timers,
                    ..
                } = self;
                let connection = connections
                    .get_mut(token.index)
                    .ok_or(TcpNodeError::SessionMissing)?;
                let session_id = connection.session_id();
                let capabilities = lookup
                    .pending_open_capabilities(session_id)
                    .unwrap_or_default();
                let outcome = connection.on_typed_timer_expiry(
                    token.index,
                    timers,
                    token.kind,
                    capabilities,
                    now,
                )?;
                (session_id, outcome, connection.remote().is_ipv4())
            };
            if let Some(action) = outcome.action {
                let counter = match action {
                    TcpTimerAction::RtoRetransmit => TcpNodeError::Retransmit,
                    TcpTimerAction::RackRetransmit => TcpNodeError::RackRetransmit,
                    TcpTimerAction::TlpProbe => TcpNodeError::TlpProbe,
                    TcpTimerAction::PersistProbe => TcpNodeError::PersistProbe,
                    TcpTimerAction::KeepaliveProbe => TcpNodeError::KeepaliveProbe,
                };
                let _ = runtime.record_current_node_error(counter);
            }
            if let Some(segment) = outcome.segment {
                if segment.payload_len() == 0 {
                    enqueue_tcp_segment(
                        runtime,
                        frame,
                        self.tco_next_node[usize::from(!is_ip4)],
                        output,
                        token.index,
                        segment,
                    )?;
                } else {
                    sessions.mark_ready(session_id);
                }
            } else if matches!(
                token.kind,
                TcpTimerKind::Retransmit
                    | TcpTimerKind::Rack
                    | TcpTimerKind::Tlp
                    | TcpTimerKind::Persist
                    | TcpTimerKind::Pacing
            ) {
                sessions.mark_ready(session_id);
            }
            self.remove_closed_connection(sessions, token.index)?;
        }
        Ok(())
    }

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &mut DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::Frame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            connection.on_session_close(index, &mut self.timer_wheel);
        }
        self.control_output(sessions, index, runtime, output_next, frame, output, now)
    }
}

/// Deprecated compatibility implementation for the old packetized Session Queue.
/// Its trait is marked `#[deprecated]` in service; do not add new callers.
impl SessionPacketizedTransport for TcpWorker {
    #[inline]
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &mut DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::Frame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.control_output(sessions, index, runtime, output_next, frame, output, now)
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker,
        index: u32,
        pending_len: usize,
        now: Instant,
    ) -> RuntimeResult<TransportSendParams> {
        let connection = self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let _ = connection.refresh_path_mtu_from_cache();
        let start = if connection.state() == TcpState::SynSent {
            connection.iss()
        } else {
            connection.snd_una()
        };
        let tx_offset =
            usize::try_from(TcpSeq::from(start).distance_to(connection.tx_payload_sequence()))
                .map_err(|_| TcpNodeError::TxOffsetOverflow)?;
        let capabilities = self
            .lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        let snd_space =
            connection.tx_payload_budget(pending_len.saturating_sub(tx_offset), now, capabilities);
        let flags = if snd_space == 0 {
            TransportSendFlags::DESCHED
        } else {
            TransportSendFlags::default()
        };
        Ok(TransportSendParams {
            snd_space,
            tx_offset,
            send_goal_size: connection.send_goal_size(),
            flags,
        })
    }

    fn tx_action(
        &mut self,
        index: u32,
        batch: &[TxBatchBuffer],
        runtime: &mut DataPlaneMain,
        now: Instant,
    ) -> RuntimeResult<()> {
        let connection = self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let capabilities = self
            .lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        if connection.state() == TcpState::SynSent {
            if connection.local().is_none() {
                return Err(TcpError::InvalidConnection.into());
            }
        } else {
            connection.ensure_state(TcpState::Established)?;
        }
        for entry in batch {
            u32::try_from(entry.payload_len).map_err(|_| RuntimeError::from(TcpError::Dispatch))?;
        }
        for entry in batch {
            let segment = connection.tx_segment(entry.payload_len, capabilities)?;
            segment.write_to_buffer(&mut *runtime.buffer_mut(entry.index))?;
            connection.commit_payload_tx(entry.payload_len, now)?;
        }
        connection.sync_payload_tx_timers(index, &mut self.timer_wheel, now)
    }
}
