use std::cell::RefCell;
use std::net::SocketAddr;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpSegmentFlags, TcpSegmentHeader};
#[cfg(test)]
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::transport::congestion::BbrController;

use super::connection::TcpConnection;
use super::segment::alloc_tcp_segment;
use super::segment::tcp_segment_metadata;
use super::state_machine::{
    CloseWait, Closed, Closing, Established, FinWait1, FinWait2, LastAck, Listen, SynRcvd, SynSent,
    TimeWait,
};
use super::{
    TcpConnectionState, TcpConnectionTimerKind, TcpInputNext, TcpPendingIndex,
    TcpSessionConnectionIndex,
};
use crate::session::node::{
    SessionQueueDispatchFn, SessionQueueHandle, SessionQueueNext, SessionQueueOutput,
    register_session_queue, with_session_queue,
};
use crate::session::runtime::{
    SessionDriverRuntime, SessionEntry, SessionQueueProtocol, SessionStateFactory,
    dispatch_session_queue_once_at,
};
#[cfg(test)]
use crate::session::runtime::{SessionQueueStep, dispatch_session_queue_for_ticks};
use crate::session::{
    SessionAppCloseSubmission, SessionAppSendSubmission, SessionId, SessionProtocolContext,
    SessionTimerExpiry, SessionTimerToken,
};

const TCP_ACTIVE_OPEN_TIMER_TICKS: u64 = 2;
pub(crate) type TcpServiceController = BbrController;
pub(crate) type TcpServiceConnectionState = TcpConnectionState<TcpServiceController>;

thread_local! {
    static TCP_SESSION_QUEUES: RefCell<hammer_infra::vec::Vec<TcpSessionQueue>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

pub(crate) struct TcpSessionQueue {
    driver: SessionDriverRuntime<TcpServiceConnectionState>,
    protocol: TcpSessionProtocol,
}

impl TcpSessionQueue {
    #[inline]
    pub(crate) fn new(worker: DataWorkerId, buffers: DataPlaneBuffers) -> Self {
        Self {
            driver: SessionDriverRuntime::new(worker, buffers),
            protocol: TcpSessionProtocol::new(worker),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            driver: SessionDriverRuntime::with_timer_clock(
                worker,
                buffers,
                timer_tick_duration,
                last_timer_tick,
            ),
            protocol: TcpSessionProtocol::new(worker),
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.protocol.worker()
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn insert_session(&mut self, connection: TcpServiceConnectionState) -> SessionId {
        self.driver.insert_session(connection)
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> SessionId
    where
        F: SessionStateFactory<TcpServiceConnectionState>,
    {
        self.driver.insert_session_with_id(f)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn session_state(
        &self,
        session_id: SessionId,
    ) -> Option<&TcpServiceConnectionState> {
        self.driver.session_state(session_id)
    }

    pub(crate) fn take_connection<S>(
        &mut self,
        session_id: SessionId,
    ) -> CoreResult<TcpConnection<S, TcpServiceController>>
    where
        TcpConnection<S, TcpServiceController>:
            TryFrom<TcpServiceConnectionState, Error = CoreError>,
    {
        self.driver
            .session_state(session_id)
            .ok_or_else(|| CoreError::internal("tcp session is missing"))?
            .clone()
            .try_into()
    }

    #[inline]
    pub(crate) fn close_session(
        &mut self,
        session_id: SessionId,
    ) -> CoreResult<Option<SessionEntry<TcpServiceConnectionState>>> {
        let closed = self.driver.close_session(session_id)?;
        if closed.is_some() {
            self.protocol.forget_session(session_id);
        }
        Ok(closed)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn bind_session_app_ring(
        &mut self,
        session_id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        self.driver.bind_session_app_ring(session_id, op, ring)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn app_mut(&mut self) -> &mut crate::session::SessionAppRuntime {
        self.driver.app_mut()
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn mark_session_ready(&mut self, session_id: SessionId) {
        self.driver.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn session_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.protocol.session_route_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn pending_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.protocol.pending_route_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn arm_retransmit_timer(
        &mut self,
        session_id: SessionId,
        ticks: u64,
    ) -> CoreResult<()> {
        self.arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::RETRANSMIT, ticks)
    }

    #[inline]
    pub(crate) fn cancel_retransmit_timer(&mut self, session_id: SessionId) -> bool {
        self.cancel_tcp_timer(session_id, TcpConnectionTimerKind::RETRANSMIT)
    }

    #[inline]
    pub(crate) fn arm_tcp_timer_ticks(
        &mut self,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        ticks: u64,
    ) -> CoreResult<()> {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::arm_tcp_timer_ticks(&mut context, session_id, kind, ticks)
    }

    #[inline]
    pub(crate) fn cancel_tcp_timer(
        &mut self,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
    ) -> bool {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::cancel_tcp_timer(&mut context, session_id, kind)
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &mut self,
        session_id: SessionId,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<bool> {
        self.driver.enqueue_rx(session_id, index, fin)
    }

    pub(crate) fn complete_connected(&mut self, session_id: SessionId) -> CoreResult<()> {
        let Some(op) = self
            .driver
            .session(session_id)
            .and_then(|entry| entry.app_op())
        else {
            return Ok(());
        };
        self.driver.app().complete_connected(op)
    }

    pub(crate) fn connect(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> CoreResult<SessionId> {
        let iss = self.protocol.next_initial_sequence(local, remote);
        let connection: TcpConnection<Closed, TcpServiceController> =
            TcpConnection::new(None, self.worker(), local.port(), Some(local), remote);
        connection.connect(self, iss)
    }

    #[cfg(test)]
    pub(crate) fn dispatch_for_ticks(
        &mut self,
        runtime: &DataPlaneRuntime,
        timer_ticks: u32,
        output_next: SessionQueueNext,
    ) -> CoreResult<SessionQueueStep> {
        dispatch_session_queue_for_ticks(
            runtime,
            &mut self.driver,
            &mut self.protocol,
            timer_ticks,
            output_next,
        )
    }

    pub(crate) fn dispatch_once_at(
        &mut self,
        runtime: &DataPlaneRuntime,
        now: Instant,
        output_next: SessionQueueNext,
    ) -> CoreResult<()> {
        dispatch_session_queue_once_at(
            runtime,
            &mut self.driver,
            &mut self.protocol,
            now,
            output_next,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn expire_timers_for_test(&mut self, ticks: u32) -> CoreResult<usize> {
        self.driver
            .poll_once_for_ticks(ticks)
            .map(|step| step.expired_timers)
    }
}

impl TcpConnection<Closed, TcpServiceController> {
    pub(crate) fn connect(
        self,
        queue: &mut TcpSessionQueue,
        initial_sequence: u32,
    ) -> CoreResult<SessionId> {
        let connection = self.connect_state(initial_sequence);
        let session_id = queue.driver.insert_session(connection.clone().into());
        queue.protocol.remember_pending_open(
            session_id,
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        queue.driver.mark_ready(session_id);
        Ok(session_id)
    }
}

impl TcpConnection<Listen, TcpServiceController> {
    pub(crate) fn receive_syn(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if !packet.flags.contains(TcpSegmentFlags::SYN)
            || packet
                .flags
                .intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
        {
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(None);
        }
        let (connection, header) = self.accept_syn(packet);
        queue.protocol.remember_session(
            session_id,
            connection.connection_id(),
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        queue.arm_retransmit_timer(session_id, 1)?;
        queue.driver.mark_ready(session_id);
        Ok(Some(header))
    }
}

impl TcpConnection<SynSent, TcpServiceController> {
    pub(crate) fn receive_open_reply(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && self.unacceptable_ack(acknowledgment)
        {
            if packet.flags.contains(TcpSegmentFlags::RST) {
                queue.driver.replace_session_state(session_id, self.into());
                return Ok(None);
            }
            let header = self.control_header(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(Some(header));
        }

        if packet.flags.contains(TcpSegmentFlags::RST) {
            let Some(acknowledgment) = packet.acknowledgment else {
                queue.driver.replace_session_state(session_id, self.into());
                return Ok(None);
            };
            if self.accepts_segment_ack(acknowledgment) {
                let mut connection = self.close_remote_reset();
                connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
                queue.protocol.forget_pending_open(session_id);
                queue
                    .driver
                    .replace_session_state(session_id, connection.into());
                queue.close_session(session_id)?;
                return Ok(None);
            }
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(None);
        }

        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(None);
        }

        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            if !self.accepts_segment_ack(acknowledgment) {
                queue.driver.replace_session_state(session_id, self.into());
                return Ok(None);
            }
            let (mut connection, header) = self.accept_syn_ack(packet, acknowledgment);
            connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
            queue.cancel_retransmit_timer(session_id);
            queue.protocol.forget_pending_open(session_id);
            queue.protocol.remember_session(
                session_id,
                connection.connection_id(),
                connection.local(),
                connection.remote(),
                connection.owner_worker(),
                connection.next_node(),
            );
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.complete_connected(session_id)?;
            return Ok(Some(header));
        }

        let (connection, header) = self.accept_simultaneous_syn(packet);
        queue.protocol.remember_pending_open(
            session_id,
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        Ok(Some(header))
    }
}

impl TcpConnection<SynRcvd, TcpServiceController> {
    pub(crate) fn receive_final_ack(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue.protocol.forget_pending_open(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        let Some(acknowledgment) = packet.acknowledgment else {
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(None);
        };
        if !packet.flags.contains(TcpSegmentFlags::ACK) || !self.accepts_final_ack(acknowledgment) {
            let header = self.control_header(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(Some(header));
        }
        let (mut connection, header) = self.accept_final_ack(packet, acknowledgment);
        connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);
        queue.cancel_retransmit_timer(session_id);
        queue.protocol.remember_session(
            session_id,
            connection.connection_id(),
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        Ok(Some(header))
    }
}

impl TcpConnection<Established, TcpServiceController> {
    pub(crate) fn receive_data(
        mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            if packet.payload_len != 0 {
                runtime.free_index(index);
            }
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            self.receive_ack(acknowledgment, packet.advertised_window);
        }

        let mut ack = false;
        let mut accepted_payload_len = None;
        if packet.payload_len != 0 {
            ack = true;
            accepted_payload_len = self.accept_payload(packet);
        }

        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, header) = self.accept_fin(packet);
            queue.protocol.remember_session(
                session_id,
                connection.connection_id(),
                connection.local(),
                connection.remote(),
                connection.owner_worker(),
                connection.next_node(),
            );
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            if let Some(payload_len) = accepted_payload_len {
                runtime.advance(index, packet.payload_offset)?;
                runtime.truncate_chain(index, payload_len)?;
                queue.enqueue_rx(session_id, index, true)?;
            } else if packet.payload_len != 0 {
                runtime.free_index(index);
            }
            return Ok(Some(header));
        }

        let header = ack.then(|| self.control_header(packet, TcpSegmentFlags::ACK, None));
        queue.protocol.remember_session(
            session_id,
            self.connection_id(),
            self.local(),
            self.remote(),
            self.owner_worker(),
            self.next_node(),
        );
        queue.driver.replace_session_state(session_id, self.into());
        if let Some(payload_len) = accepted_payload_len {
            runtime.advance(index, packet.payload_offset)?;
            runtime.truncate_chain(index, payload_len)?;
            queue.enqueue_rx(session_id, index, false)?;
        } else if packet.payload_len != 0 {
            runtime.free_index(index);
        }
        Ok(header)
    }
}

impl TcpConnection<CloseWait, TcpServiceController> {
    pub(crate) fn receive_close_wait(
        mut self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            self.receive_ack(acknowledgment, packet.advertised_window);
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, header) = self.accept_repeated_fin(packet);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            return Ok(Some(header));
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl TcpConnection<FinWait1, TcpServiceController> {
    pub(crate) fn receive_fin_wait1(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        let fin = packet.flags.contains(TcpSegmentFlags::FIN);
        let ack = packet
            .acknowledgment
            .filter(|_| packet.flags.contains(TcpSegmentFlags::ACK));
        match (fin, ack) {
            (true, Some(acknowledgment)) => {
                let (connection, header) = self.accept_fin_ack(packet, acknowledgment);
                queue.protocol.remember_session(
                    session_id,
                    connection.connection_id(),
                    connection.local(),
                    connection.remote(),
                    connection.owner_worker(),
                    connection.next_node(),
                );
                queue
                    .driver
                    .replace_session_state(session_id, connection.into());
                Ok(Some(header))
            }
            (false, Some(acknowledgment)) => {
                let connection = self.accept_ack(packet, acknowledgment);
                queue.protocol.remember_session(
                    session_id,
                    connection.connection_id(),
                    connection.local(),
                    connection.remote(),
                    connection.owner_worker(),
                    connection.next_node(),
                );
                queue
                    .driver
                    .replace_session_state(session_id, connection.into());
                Ok(None)
            }
            (true, _) => {
                let (connection, header) = self.accept_fin(packet);
                queue.protocol.remember_session(
                    session_id,
                    connection.connection_id(),
                    connection.local(),
                    connection.remote(),
                    connection.owner_worker(),
                    connection.next_node(),
                );
                queue
                    .driver
                    .replace_session_state(session_id, connection.into());
                Ok(Some(header))
            }
            _ => {
                queue.driver.replace_session_state(session_id, self.into());
                Ok(None)
            }
        }
    }
}

impl TcpConnection<FinWait2, TcpServiceController> {
    pub(crate) fn receive_fin_wait2(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, header) = self.accept_fin(packet);
            queue.protocol.remember_session(
                session_id,
                connection.connection_id(),
                connection.local(),
                connection.remote(),
                connection.owner_worker(),
                connection.next_node(),
            );
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            return Ok(Some(header));
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl TcpConnection<Closing, TcpServiceController> {
    pub(crate) fn receive_closing(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let connection = self.accept_ack(packet, acknowledgment);
            queue.protocol.remember_session(
                session_id,
                connection.connection_id(),
                connection.local(),
                connection.remote(),
                connection.owner_worker(),
                connection.next_node(),
            );
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            return Ok(None);
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl TcpConnection<LastAck, TcpServiceController> {
    pub(crate) fn receive_last_ack(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            let connection = self.accept_ack(packet, acknowledgment);
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl TcpConnection<TimeWait, TcpServiceController> {
    pub(crate) fn receive_time_wait(
        self,
        queue: &mut TcpSessionQueue,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegmentHeader>> {
        if packet.flags.contains(TcpSegmentFlags::RST) {
            let connection = self.close_remote_reset();
            queue.protocol.forget_session(session_id);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            queue.close_session(session_id)?;
            return Ok(None);
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl SessionQueueProtocol<TcpServiceConnectionState> for TcpSessionProtocol {
    fn handle_timer_expiry(
        &mut self,
        driver: &mut SessionDriverRuntime<TcpServiceConnectionState>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()> {
        let Some(kind) = Self::timer_kind(expiry.token()) else {
            return Ok(());
        };
        if let Some(connection) = driver.session_state_mut(expiry.session_id()) {
            connection.tcp_timer_expire(kind);
        }
        driver.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpServiceConnectionState>,
        session_id: SessionId,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        let timer_output = driver
            .session_state_mut(session_id)
            .and_then(TcpConnectionState::on_tcp_timer_expiry);
        if let Some((kind, local, remote, header)) = timer_output {
            let index = alloc_tcp_segment(
                driver.buffers(),
                tcp_segment_metadata(local, remote),
                header,
            )?;
            output.enqueue(runtime, output_next.node(), index)?;
            if let Some(token) = TcpSessionProtocol::timer_token(kind) {
                driver.arm_timer_ticks(session_id, token, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
            }
        }
        if driver
            .session(session_id)
            .and_then(|entry| entry.app_op())
            .is_none()
        {
            return Ok(());
        }
        driver.app_mut().drain_submissions()
    }
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    index: TcpSessionConnectionIndex,
    pending_index: TcpPendingIndex,
    next_iss: u32,
}

impl TcpSessionProtocol {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            index: TcpSessionConnectionIndex::empty(),
            pending_index: TcpPendingIndex::empty(),
            next_iss: 81_000,
        }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn index(&self) -> &TcpSessionConnectionIndex {
        &self.index
    }

    #[inline]
    pub fn index_mut(&mut self) -> &mut TcpSessionConnectionIndex {
        &mut self.index
    }

    #[inline]
    pub fn remember_session(
        &mut self,
        session_id: SessionId,
        connection_id: Option<TcpConnectionId>,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        self.index
            .upsert(session_id, connection_id, local, remote, owner, next);
    }

    #[inline]
    pub fn remember_pending_open(
        &mut self,
        id: SessionId,
        local: Option<SocketAddr>,
        remote: SocketAddr,
        owner: DataWorkerId,
        next: TcpInputNext,
    ) {
        self.pending_index
            .remember_pending_open(id, local, remote, owner, next);
    }

    #[inline]
    pub fn session_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.index.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn pending_route_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.pending_index.lookup_pending_by_tuple(local, remote)
    }

    #[inline]
    pub fn session_id_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        self.index.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub fn forget_session(&mut self, session_id: SessionId) {
        self.index.forget_session(session_id);
    }

    #[inline]
    pub fn forget_pending_open(&mut self, id: SessionId) {
        self.pending_index.forget_pending_open(id);
    }

    pub fn next_initial_sequence(&mut self, local: SocketAddr, remote: SocketAddr) -> u32 {
        let mut value = self.next_iss;
        value ^= u32::from(local.port()) << 16 | u32::from(remote.port());
        value ^= match (local.ip(), remote.ip()) {
            (std::net::IpAddr::V4(local), std::net::IpAddr::V4(remote)) => {
                u32::from(local) ^ u32::from(remote).rotate_left(13)
            }
            (std::net::IpAddr::V6(local), std::net::IpAddr::V6(remote)) => {
                let local = u128::from(local);
                let remote = u128::from(remote);
                (local as u32) ^ ((local >> 64) as u32) ^ (remote as u32).rotate_left(7)
            }
            _ => 0x9e37_79b9,
        };
        self.next_iss = self.next_iss.wrapping_add(64_099);
        value.max(1)
    }

    #[inline]
    pub fn mark_session_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
        session_id: SessionId,
    ) {
        context.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_retransmit_timer(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
        session_id: SessionId,
        ticks: u64,
    ) -> CoreResult<()> {
        Self::arm_tcp_timer_ticks(
            context,
            session_id,
            TcpConnectionTimerKind::RETRANSMIT,
            ticks,
        )
    }

    #[inline]
    pub fn cancel_retransmit_timer(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
        session_id: SessionId,
    ) -> bool {
        Self::cancel_tcp_timer(context, session_id, TcpConnectionTimerKind::RETRANSMIT)
    }

    #[inline]
    pub fn timer_token(kind: TcpConnectionTimerKind) -> Option<SessionTimerToken> {
        let bits = kind.bits();
        if bits == 0 || bits.count_ones() != 1 {
            return None;
        }
        Some(SessionTimerToken::new(bits.trailing_zeros() + 1))
    }

    #[inline]
    pub fn timer_kind(token: SessionTimerToken) -> Option<TcpConnectionTimerKind> {
        let ordinal = token.get();
        if ordinal == 0 || ordinal > u16::BITS {
            return None;
        }
        TcpConnectionTimerKind::from_timer_bit(1u16 << (ordinal - 1))
    }

    #[inline]
    pub fn arm_tcp_timer_ticks(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        ticks: u64,
    ) -> CoreResult<()> {
        let Some(token) = Self::timer_token(kind) else {
            return Ok(());
        };
        context.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_tcp_timer(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
    ) -> bool {
        let Some(token) = Self::timer_token(kind) else {
            return false;
        };
        context.cancel_timer(session_id, token)
    }

    #[inline]
    pub fn take_drained_sends(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
    ) -> hammer_infra::vec::Vec<SessionAppSendSubmission> {
        context.app_mut().take_drained_sends()
    }

    #[inline]
    pub fn take_drained_closes(
        context: &mut SessionProtocolContext<'_, TcpServiceConnectionState>,
    ) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        context.app_mut().take_drained_closes()
    }

    #[inline]
    pub fn register_queue(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
    ) -> CoreResult<SessionQueueHandle> {
        register_session_queue(&TCP_SESSION_QUEUES, TcpSessionQueue::new(worker, buffers))
    }

    #[inline]
    pub fn connect(
        handle: SessionQueueHandle,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> CoreResult<SessionId> {
        Self::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue.connect(local, remote)
        })
    }

    #[inline]
    pub fn session_queue_dispatch_fn() -> SessionQueueDispatchFn {
        tcp_session_queue_dispatch
    }

    #[inline]
    pub(crate) fn with_queue<R, F>(handle: SessionQueueHandle, f: F) -> CoreResult<R>
    where
        F: crate::session::node::SessionQueueAccess<TcpSessionQueue, R>,
    {
        with_session_queue(&TCP_SESSION_QUEUES, handle, f)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_for_test(
        queue: TcpSessionQueue,
    ) -> CoreResult<SessionQueueHandle> {
        register_session_queue(&TCP_SESSION_QUEUES, queue)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_with_connection_for_test(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        connection: TcpServiceConnectionState,
    ) -> CoreResult<SessionQueueHandle> {
        let mut queue = TcpSessionQueue::new(worker, buffers);
        let session_id = queue.insert_session(connection);
        let connection: TcpConnection<Established, TcpServiceController> =
            queue.take_connection(session_id)?;
        queue.protocol.remember_session(
            session_id,
            connection.connection_id(),
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        Self::register_queue_for_test(queue)
    }
}

fn tcp_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    handle: SessionQueueHandle,
    output_next: SessionQueueNext,
    now: Instant,
) -> CoreResult<()> {
    TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
        queue.dispatch_once_at(runtime, now, output_next)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeProcessFn, NodeResult,
        NodeRuntimeData,
    };
    use hammer_core::error::CoreError;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpConnectionId, TcpSegmentFlags, TcpSegmentView, tcp_options_from_bytes,
    };
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppRingHandle, AppSqe, AppUserData};

    use super::*;
    use crate::session::SessionQueueNode;
    use std::sync::{Arc, Mutex, OnceLock};

    const ACTIVE_OPEN_ISS: u32 = 81_000;

    #[inline]
    const fn unused_output_next() -> SessionQueueNext {
        SessionQueueNext::from_node(NodeId::new(0))
    }

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            _frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must use descriptor process",
            ))
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {}

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let state = {
            let states = capture_states()
                .lock()
                .map_err(|_| CoreError::internal("capture registry poisoned"))?;
            Arc::clone(
                states
                    .get(data.usize_word(0)?)
                    .ok_or_else(|| CoreError::internal("capture state missing"))?,
            )
        };
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            state
                .lock()
                .map_err(|_| CoreError::internal("capture poisoned"))?
                .packets
                .push(packet.into_iter().collect());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    fn tcp_connection() -> TcpServiceConnectionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        TcpServiceConnectionState::established_for_test(
            Some(TcpConnectionId::new(7001)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    fn syn_sent_connection(
        worker: DataWorkerId,
        local: SocketAddr,
        remote: SocketAddr,
        iss: u32,
        capabilities: TcpCapabilities,
    ) -> TcpServiceConnectionState {
        let closed: TcpConnection<Closed, TcpServiceController> =
            TcpConnection::new(None, worker, local.port(), Some(local), remote);
        let mut syn_sent = closed.connect_state(iss);
        syn_sent.set_local_capabilities(capabilities);
        syn_sent.into()
    }

    fn remember_established(queue: &mut TcpSessionQueue, session_id: SessionId) -> CoreResult<()> {
        let connection: TcpConnection<Established, TcpServiceController> =
            queue.take_connection(session_id)?;
        queue.protocol.remember_session(
            session_id,
            connection.connection_id(),
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        Ok(())
    }

    fn remember_pending(queue: &mut TcpSessionQueue, session_id: SessionId) -> CoreResult<()> {
        let connection: TcpConnection<SynSent, TcpServiceController> =
            queue.take_connection(session_id)?;
        queue.protocol.remember_pending_open(
            session_id,
            connection.local(),
            connection.remote(),
            connection.owner_worker(),
            connection.next_node(),
        );
        queue
            .driver
            .replace_session_state(session_id, connection.into());
        Ok(())
    }

    fn install_connecting_session(
        queue: &mut TcpSessionQueue,
        connection: TcpServiceConnectionState,
    ) -> CoreResult<SessionId> {
        let session_id = queue.insert_session(connection);
        remember_pending(queue, session_id)?;
        queue.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        queue.mark_session_ready(session_id);
        Ok(session_id)
    }

    #[test]
    fn tcp_active_open_creates_syn_sent_session_and_emits_syn() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let handle = TcpSessionProtocol::register_queue_for_test(TcpSessionQueue::new(
            worker,
            runtime.packet_buffers().clone(),
        ))
        .expect("register queue");
        let local: SocketAddr = "192.0.2.10:50001".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(output),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        let session_id = TcpSessionProtocol::connect(handle, local, remote).expect("active open");
        let active_open_iss =
            TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
                let connection: TcpConnection<SynSent, TcpServiceController> =
                    queue.take_connection(session_id)?;
                let iss = connection.iss();
                queue
                    .driver
                    .replace_session_state(session_id, connection.into());
                Ok(iss)
            })
            .expect("read active-open iss");

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            active_open_iss,
            TcpCapabilities::default(),
        );

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            let connection: TcpConnection<SynSent, TcpServiceController> =
                queue.take_connection(session_id)?;
            assert_eq!(connection.snd_una(), active_open_iss);
            assert_eq!(connection.snd_nxt(), active_open_iss + 1);
            assert_eq!(connection.rcv_nxt(), 0);
            assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            assert_eq!(queue.session_route_by_tuple(local, remote), None);
            assert_eq!(
                queue.pending_route_by_tuple(local, remote),
                Some((session_id, worker, TcpInputNext::SynSent))
            );
            Ok(())
        })
        .expect("inspect active open");
    }

    #[test]
    fn tcp_active_open_retransmit_timer_reemits_syn() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let handle =
            TcpSessionProtocol::register_queue_for_test(TcpSessionQueue::with_timer_clock(
                worker,
                runtime.packet_buffers().clone(),
                Duration::from_millis(10),
                Instant::now(),
            ))
            .expect("register queue");
        let local: SocketAddr = "192.0.2.10:50002".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(output),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        let session_id = TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            install_connecting_session(
                queue,
                syn_sent_connection(
                    worker,
                    local,
                    remote,
                    ACTIVE_OPEN_ISS,
                    TcpCapabilities::default(),
                ),
            )
        })
        .expect("active open");

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(runtime.run_ready_nodes().expect("run first syn"), 2);
        capture.lock().unwrap().packets.clear();

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue
                .expire_timers_for_test(1)
                .expect("expire before timer");
            Ok(())
        })
        .expect("expire before timer");
        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule no retransmit");
        assert_eq!(runtime.run_ready_nodes().expect("run empty"), 1);
        assert!(capture.lock().unwrap().packets.is_empty());

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue.expire_timers_for_test(1).expect("expire retransmit");
            Ok(())
        })
        .expect("expire retransmit");
        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule retransmit");
        assert_eq!(runtime.run_ready_nodes().expect("run retransmit"), 2);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            ACTIVE_OPEN_ISS,
            TcpCapabilities::default(),
        );
        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            let connection: TcpConnection<SynSent, TcpServiceController> =
                queue.take_connection(session_id)?;
            assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            Ok(())
        })
        .expect("inspect retransmit session");
    }

    #[test]
    fn tcp_session_queue_indexes_pool_session_ids_and_drains_ready_app_close() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let op = AppOpId::new(7001);
        let session_id = queue.insert_session(tcp_connection());

        remember_established(&mut queue, session_id).expect("index test session");
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));
        queue.mark_session_ready(session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(11)), op))
            .expect("push close sqe");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch queue");

        let closes = queue.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, session_id);
        assert_eq!(closes[0].op, op);
    }

    #[test]
    fn tcp_session_queue_resolves_app_submission_session_from_descriptor_op() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let first_op = AppOpId::new(7001);
        let second_op = AppOpId::new(7002);
        let first_session_id = queue.insert_session(tcp_connection());
        let second_session_id = queue.insert_session(tcp_connection());

        assert!(queue.bind_session_app_ring(first_session_id, first_op, ring.clone()));
        assert!(queue.bind_session_app_ring(second_session_id, second_op, ring.clone()));
        queue.mark_session_ready(first_session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(22)), second_op))
            .expect("push second close sqe");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch queue");

        let closes = queue.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, second_session_id);
        assert_eq!(closes[0].op, second_op);
    }

    #[test]
    fn tcp_session_queue_delivers_payload_to_pending_recv_cqe() {
        let worker = DataWorkerId::new(0);
        let buffers = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, buffers.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let op = AppOpId::new(7_101);
        let session_id = queue.insert_session(tcp_connection());
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));
        ring.try_push_submission(AppSqe::recv(Some(AppUserData::new(33)), op, 32))
            .expect("push recv sqe");
        let buffer = buffers
            .alloc_index_with_bytes(Default::default(), b"tcp:hello")
            .expect("recv buffer");
        buffers.advance(buffer, 4).expect("advance to payload");

        queue
            .enqueue_rx(session_id, buffer, false)
            .expect("enqueue rx");

        let completion = ring.pop_completion().expect("recv completion");
        assert_eq!(completion.user_data(), Some(AppUserData::new(33)));
        match completion.kind() {
            AppCqeKind::Recv {
                op: completed_op,
                fin,
                ..
            } => {
                assert_eq!(*completed_op, op);
                assert!(!fin);
            }
            other => panic!("expected recv completion, got {other:?}"),
        }
        let recv = completion.into_recv().expect("recv cqe");
        assert_eq!(
            recv.copy_current().expect("recv payload"),
            b"hello".to_vec()
        );
        recv.release();
    }

    #[test]
    fn tcp_session_queue_retransmit_timer_can_be_armed_and_cancelled() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection());

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("arm retransmit timer");
        queue
            .dispatch_for_ticks(&runtime, 1, unused_output_next())
            .expect("expire timer");
        assert!(queue.session_state(session_id).is_some());

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("rearm retransmit timer");
        assert!(queue.cancel_retransmit_timer(session_id));
        queue
            .dispatch_for_ticks(&runtime, 1, unused_output_next())
            .expect("dispatch cancelled timer");
        assert!(queue.session_state(session_id).is_some());
    }

    #[test]
    fn tcp_session_queue_can_register_rack_tlp_and_pacing_timers() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection());

        queue
            .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::RACK, 1)
            .expect("arm rack timer");
        queue
            .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::TLP, 2)
            .expect("arm tlp timer");
        queue
            .arm_tcp_timer_ticks(session_id, TcpConnectionTimerKind::PACING, 3)
            .expect("arm pacing timer");

        assert_eq!(queue.expire_timers_for_test(1).expect("expire rack"), 1);
        assert_eq!(queue.expire_timers_for_test(1).expect("expire tlp"), 1);
        assert_eq!(queue.expire_timers_for_test(1).expect("expire pacing"), 1);
    }

    #[test]
    fn tcp_session_queue_rearmed_retransmit_timer_keeps_new_active_timer() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection());

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("arm first retransmit timer");

        queue.expire_timers_for_test(1).expect("expire first timer");
        queue
            .arm_retransmit_timer(session_id, 4)
            .expect("rearm retransmit timer before expiry dispatch");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch stale expiry");

        assert!(queue.session_state(session_id).is_some());
    }

    fn assert_tcp_syn(
        packet: &[u8],
        local: SocketAddr,
        remote: SocketAddr,
        sequence: u32,
        expected: TcpCapabilities,
    ) {
        let segment = TcpSegmentView::parse(packet).expect("tcp segment");
        assert_eq!(segment.source_port(), local.port());
        assert_eq!(segment.destination_port(), remote.port());
        assert_eq!(segment.sequence_number(), sequence);
        assert!(segment.flags().contains(TcpSegmentFlags::SYN));
        let options = tcp_options_from_bytes(segment.options());
        assert_eq!(options.capabilities, expected);
    }
}
