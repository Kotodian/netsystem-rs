use std::time::{Duration, Instant};

#[cfg(test)]
use crate::TcpPacket;
use crate::{TcpSeq, TcpState};
use hammer_core::data_plane::DataPlaneBuffers;
use hammer_infra::pool::{Index, Pool};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneRuntime, DataWorkerId};

use super::connection::TcpTimerAction;
use super::lookup::TcpLookupState;
use super::timers::{TcpTimerKind, TcpTimers};
use super::{TcpConnection, TcpNodeError, enqueue_tcp_segment};
use hammer_service::session::node::{SessionQueueNext, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionPacketizedTransport, SessionPacketizedTx, SessionTransport, SessionTransportId,
    SessionWorker, TransportSendFlags, TransportSendParams, TxBatchBuffer,
};
const DEFAULT_TCP_CONNECTION_CAPACITY: usize = 1024;
const TCP_APP_RX_MIN_FREE: usize = 4 << 10;
const TCP_APP_RX_MAX_FREE: usize = 128 << 10;

#[hammer_component_macros::session_transport(
    name = "tcp",
    upper = "ordered-reliable-byte-stream",
    start_listen = crate::start_listen,
    stop_listen = crate::stop_listen
)]
pub struct TcpWorker {
    pub(crate) connections: Pool<TcpConnection>,
    pub(crate) lookup: TcpLookupState,
    pub(super) timers: TcpTimers,
}

impl TcpWorker {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            connections: Pool::with_capacity(DEFAULT_TCP_CONNECTION_CAPACITY),
            lookup: TcpLookupState::new(worker),
            timers: TcpTimers::new(Instant::now(), Duration::from_millis(10)),
        }
    }

    #[inline]
    pub(crate) fn has_connection_capacity(&self) -> bool {
        self.connections.len() < self.connections.capacity()
    }

    #[inline]
    pub(crate) fn insert_connection(&mut self, connection: TcpConnection) -> RuntimeResult<Index> {
        self.connections
            .insert(connection)
            .ok_or_else(|| TcpNodeError::ConnectionCreate.into())
    }

    #[inline]
    pub(crate) fn connection(&self, index: Index) -> Option<&TcpConnection> {
        self.connections.get(index)
    }

    #[inline]
    pub(crate) fn connection_mut(&mut self, index: Index) -> Option<&mut TcpConnection> {
        self.connections.get_mut(index)
    }

    #[inline]
    pub(crate) fn remove_connection(&mut self, index: Index) -> Option<TcpConnection> {
        self.connections.remove(index)
    }

    #[cfg(test)]
    pub(crate) fn receive_close_side_for_test(
        &mut self,
        index: Index,
        packet: &TcpPacket,
    ) -> RuntimeResult<()> {
        let Self {
            connections,
            timers,
            ..
        } = self;
        connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)?
            .receive_close_side(index, timers, packet, Instant::now())?;
        Ok(())
    }

    fn remove_closed_connection(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
    ) -> RuntimeResult<()> {
        let Some(connection) = self.connections.get(index) else {
            return Ok(());
        };
        if connection.state() != TcpState::Closed {
            return Ok(());
        }
        let session_id = connection.session_id();
        sessions.notify_transport_closed(session_id, index)?;
        self.lookup.forget_session(session_id);
        self.lookup.forget_pending_open(session_id);
        let _ = self.connections.remove(index);
        sessions.notify_transport_deleted(session_id, index)?;
        Ok(())
    }

    fn control_output(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::BufferFrame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let (session_id, segment, has_pending_sack) = {
            let Self {
                connections,
                lookup,
                timers,
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
            (session_id, segment, connection.has_pending_sack_output())
        };
        if let Some(segment) = segment {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, frame, output_next, output, segment)?;
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

impl SessionTransport<Index> for TcpWorker {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    fn app_rx_evt(
        &mut self,
        index: Index,
        rx_available: usize,
        rx_capacity: usize,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::BufferFrame,
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
        enqueue_tcp_segment(runtime, frame, output_next, output, segment)?;
        *self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)? = candidate;
        Ok(false)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::BufferFrame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.timers.advance(now, &mut self.connections);
        while let Some(token) = self.timers.take_pending(&mut self.connections) {
            let (session_id, outcome) = {
                let Self {
                    connections,
                    lookup,
                    timers,
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
                (session_id, outcome)
            };
            if let Some(action) = outcome.action {
                let counter = match action {
                    TcpTimerAction::RtoRetransmit => TcpNodeError::Retransmit,
                    TcpTimerAction::RackRetransmit => TcpNodeError::RackRetransmit,
                    TcpTimerAction::TlpProbe => TcpNodeError::TlpProbe,
                    TcpTimerAction::PersistProbe => TcpNodeError::PersistProbe,
                    TcpTimerAction::KeepaliveProbe => TcpNodeError::KeepaliveProbe,
                };
                let _ = runtime.record_current_node_error(counter.code());
            }
            if let Some(segment) = outcome.segment {
                if segment.payload_len() == 0 {
                    enqueue_tcp_segment(runtime, frame, output_next, output, segment)?;
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
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::BufferFrame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            connection.on_session_close(index, &mut self.timers);
        }
        self.control_output(sessions, index, runtime, output_next, frame, output, now)
    }
}

impl SessionPacketizedTransport<Index> for TcpWorker {
    #[inline]
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut hammer_core::data_plane::BufferFrame,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.control_output(sessions, index, runtime, output_next, frame, output, now)
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker<Index>,
        index: Index,
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
        Ok(TransportSendParams {
            snd_space,
            tx_offset,
            send_goal_size: connection.send_goal_size(),
            flags: TransportSendFlags::default(),
        })
    }

    fn tx_action(
        &mut self,
        index: Index,
        batch: &[TxBatchBuffer],
        buffers: &DataPlaneBuffers,
        now: Instant,
    ) -> RuntimeResult<()> {
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let capabilities = self
            .lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        let previous_timer_state = *connection.timer_state();
        let mut candidate = connection.clone();
        for entry in batch {
            let segment = candidate.tx_segment(entry.payload_len, capabilities)?;
            segment.write_to_buffer(buffers, entry.index)?;
            candidate.commit_payload_tx(entry.payload_len, now)?;
        }
        *candidate.timer_state_mut() = previous_timer_state;
        candidate.sync_payload_tx_timers(index, &mut self.timers, now)?;
        *self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)? = candidate;
        Ok(())
    }
}
