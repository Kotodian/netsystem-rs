use std::time::Instant;

use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpSeq, TcpState};
use hammer_infra::pool::{Index, Pool};
use hammer_infra::segment::Segment;
use hammer_runtime::{DataPlaneRuntime, DataWorkerId};

use super::connection::{
    TCP_TIMER_PACING, TCP_TIMER_PERSIST, TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TLP,
    sync_all_tcp_timers, sync_tcp_timer,
};
use super::lookup::TcpLookupState;
use super::{TcpConnection, TcpNodeError, enqueue_tcp_segment};
use crate::session::app::SessionAppRuntimeCreate;
use crate::session::node::{SessionQueueNext, SessionQueueOutput};
use crate::session::runtime::{
    SessionPacketizedTransport, SessionPacketizedTx, SessionTransport, SessionTransportId,
    SessionWorker, TransportSendFlags, TransportSendParams, TxBatchBuffer,
};
use crate::session::{SessionAppRuntime, SessionId};
use crate::transport::congestion::CongestionController;

const DEFAULT_TCP_CONNECTION_CAPACITY: usize = 1024;

pub(crate) struct TcpWorker<C>
where
    C: CongestionController,
{
    pub(crate) connections: Pool<TcpConnection<C>>,
    pub(crate) lookup: TcpLookupState,
}

impl<C> TcpWorker<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    pub(crate) fn new(worker: DataWorkerId) -> Self {
        Self {
            connections: Pool::with_capacity(DEFAULT_TCP_CONNECTION_CAPACITY),
            lookup: TcpLookupState::new(worker),
        }
    }

    #[inline]
    pub(crate) fn has_connection_capacity(&self) -> bool {
        self.connections.len() < self.connections.capacity()
    }

    #[inline]
    pub(crate) fn insert_connection(&mut self, connection: TcpConnection<C>) -> CoreResult<Index> {
        self.connections
            .insert(connection)
            .ok_or_else(|| CoreError::internal("TCP connection pool capacity exhausted"))
    }

    #[inline]
    pub(crate) fn connection(&self, index: Index) -> Option<&TcpConnection<C>> {
        self.connections.get(index)
    }

    #[inline]
    pub(crate) fn connection_mut(&mut self, index: Index) -> Option<&mut TcpConnection<C>> {
        self.connections.get_mut(index)
    }

    #[inline]
    pub(crate) fn remove_connection(&mut self, index: Index) -> Option<TcpConnection<C>> {
        self.connections.remove(index)
    }

    fn remove_closed_connection<Seg: Segment>(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
    ) where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let Some(connection) = self.connections.get(index) else {
            return;
        };
        if connection.state() != TcpState::Closed {
            return;
        }
        let session_id = connection.session_id();
        self.lookup.forget_session(session_id);
        self.lookup.forget_pending_open(session_id);
        let _ = self.connections.remove(index);
        sessions.notify_transport_deleted(session_id, index);
    }

    fn control_output<Seg: Segment>(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let (session_id, segment, has_pending_sack) = {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            let session_id = connection.session_id();
            let has_pending_tx = sessions.app().has_pending_send(session_id);
            let capabilities = self
                .lookup
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let segment = connection.on_tcp_ready(has_pending_tx, capabilities);
            (session_id, segment, connection.has_pending_sack_output())
        };
        if let Some(segment) = segment {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, output_next, output, segment)?;
            } else {
                sessions.mark_ready(session_id);
            }
        }
        let has_pending_tx = sessions.app().has_pending_send(session_id);
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let timer_ticks = std::array::from_fn(|timer_id| {
            let timer_id = timer_id as u32;
            connection
                .timer_is_active(timer_id)
                .then(|| connection.timer_ticks(timer_id, now))
                .flatten()
        });
        sync_all_tcp_timers(sessions.timers_mut(), timer_ticks, session_id.pool_index())?;
        if segment.is_none() && !has_pending_tx && has_pending_sack {
            sessions.mark_ready(session_id);
        }
        self.remove_closed_connection(sessions, index);
        Ok(())
    }
}

impl<C, Seg> SessionTransport<Index, Seg> for TcpWorker<C>
where
    C: CongestionController + 'static,
    Seg: Segment,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    #[inline]
    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index, Seg>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut SessionQueueOutput,
        _: Instant,
    ) -> CoreResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        let session_id = {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            connection.on_session_close();
            connection.session_id()
        };
        sessions.notify_transport_closed(session_id, index)?;
        self.control_output(sessions, index, runtime, output_next, output, now)
    }

    fn handle_legacy_timer(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        timer_id: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        let (session_id, control) = {
            let connection = self
                .connections
                .get_mut(index)
                .ok_or(TcpNodeError::SessionMissing)?;
            let session_id = connection.session_id();
            let capabilities = self
                .lookup
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let control = connection.on_tcp_timer_expiry(timer_id, capabilities);
            (session_id, control)
        };
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let timer_ticks = connection
            .timer_is_active(timer_id)
            .then(|| connection.timer_ticks(timer_id, now))
            .flatten();
        sync_tcp_timer(
            sessions.timers_mut(),
            timer_ticks,
            session_id.pool_index(),
            timer_id,
        )?;
        if let Some(segment) = control {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, output_next, output, segment)?;
            } else {
                sessions.mark_ready(session_id);
            }
        } else if matches!(
            timer_id,
            TCP_TIMER_RETRANSMIT
                | TCP_TIMER_RACK
                | TCP_TIMER_TLP
                | TCP_TIMER_PERSIST
                | TCP_TIMER_PACING
        ) {
            sessions.mark_ready(session_id);
        }
        self.remove_closed_connection(sessions, index);
        Ok(())
    }
}

impl<C, Seg> SessionPacketizedTransport<Index, Seg> for TcpWorker<C>
where
    C: CongestionController + 'static,
    Seg: Segment,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    #[inline]
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        self.control_output(sessions, index, runtime, output_next, output, now)
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker<Index, Seg>,
        index: Index,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<TransportSendParams> {
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
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
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        batch: &[TxBatchBuffer],
        now: Instant,
    ) -> CoreResult<()> {
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let capabilities = self
            .lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        let mut candidate = connection.clone();
        for entry in batch {
            let segment = candidate.tx_segment(entry.payload_len, capabilities)?;
            segment.write_to_buffer(sessions.buffers(), entry.index)?;
            let _ = candidate.commit_payload_tx(entry.payload_len, now)?;
        }
        let session_id = candidate.session_id();
        *self
            .connections
            .get_mut(index)
            .ok_or(TcpNodeError::SessionMissing)? = candidate;
        let connection = self
            .connections
            .get(index)
            .ok_or(TcpNodeError::SessionMissing)?;
        let timer_ticks = std::array::from_fn(|timer_id| {
            let timer_id = timer_id as u32;
            connection
                .timer_is_active(timer_id)
                .then(|| connection.timer_ticks(timer_id, now))
                .flatten()
        });
        sync_all_tcp_timers(sessions.timers_mut(), timer_ticks, session_id.pool_index())
    }
}
