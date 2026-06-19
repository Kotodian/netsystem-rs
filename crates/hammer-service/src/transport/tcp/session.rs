use std::net::SocketAddr;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpSackBlock, TcpSegmentFlags};
use hammer_infra::map::FlatHashTable;
use hammer_infra::pool::{Index as PoolIndex, Pool};
#[cfg(test)]
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::transport::congestion::CongestionController;

use super::connection::TcpConnection;
use super::segment::{TcpPacket, TcpSegment};
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
};
use crate::session::runtime::{
    SessionDriverRuntime, SessionEntry, SessionQueueProtocol, SessionStateFactory,
    dispatch_session_queue_once_at,
};
#[cfg(test)]
use crate::session::runtime::{SessionQueueStep, dispatch_session_queue_for_ticks};
use crate::session::{SessionId, SessionProtocolContext, SessionTimerExpiry, SessionTimerToken};

const TCP_ACTIVE_OPEN_TIMER_TICKS: u64 = 2;

pub(crate) type TcpSessionQueueHandle<C> = SessionQueueHandle<TcpSessionQueue<C>>;

#[derive(Debug, Default)]
struct TcpSessionRx {
    entries: hammer_infra::vec::Vec<(u32, BufferIndex, bool)>,
}

impl TcpSessionRx {
    fn release(self, buffers: &DataPlaneBuffers) {
        for (_, index, _) in self.entries {
            buffers.free_index(index);
        }
    }

    fn insert(&mut self, sequence: u32, index: BufferIndex, fin: bool) {
        self.entries.push((sequence, index, fin));
        let mut position = self.entries.len().saturating_sub(1);
        while position > 0 && self.entries[position - 1].0 > self.entries[position].0 {
            self.entries.swap(position - 1, position);
            position -= 1;
        }
    }

    fn end_sequence(
        buffers: &DataPlaneBuffers,
        sequence: u32,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<u32> {
        let payload_len = buffers.get_buffer(index)?.current_len() as u32;
        Ok(sequence.wrapping_add(payload_len + u32::from(fin)))
    }

    fn first_overlap(
        &self,
        buffers: &DataPlaneBuffers,
        ready_through: u32,
        start: u32,
        end: u32,
    ) -> CoreResult<Option<(u32, u32)>> {
        for (sequence, index, fin) in self.entries.iter().copied() {
            if sequence < ready_through {
                continue;
            }
            let retained_end = Self::end_sequence(buffers, sequence, index, fin)?;
            let overlap_start = start.max(sequence);
            let overlap_end = end.min(retained_end);
            if overlap_start < overlap_end {
                return Ok(Some((overlap_start, overlap_end)));
            }
            if sequence > end {
                break;
            }
        }
        Ok(None)
    }

    fn advance_connection<C>(
        &self,
        buffers: &DataPlaneBuffers,
        connection: &mut TcpConnection<Established, C>,
    ) -> CoreResult<()>
    where
        C: CongestionController + 'static,
    {
        loop {
            let mut next = None;
            for (sequence, index, fin) in self.entries.iter().copied() {
                if sequence >= connection.rcv_nxt() {
                    next = Some((sequence, index, fin));
                    break;
                }
            }
            let Some((sequence, index, _)) = next else {
                return Ok(());
            };
            if sequence != connection.rcv_nxt() {
                return Ok(());
            }
            let payload_len = buffers.get_buffer(index)?.current_len();
            let packet = TcpPacket {
                local: connection.local().unwrap_or(connection.remote()),
                remote: connection.remote(),
                sequence,
                acknowledgment: None,
                advertised_window: 0,
                flags: TcpSegmentFlags::ACK,
                capabilities: TcpCapabilities::default(),
                sack_blocks: Vec::new(),
                payload_offset: 0,
                payload_len,
            };
            if connection.accept_payload(&packet).is_none() {
                return Ok(());
            }
        }
    }

    fn deliver_ready<F>(&mut self, ready_through: u32, mut complete: F) -> CoreResult<()>
    where
        F: FnMut(BufferIndex, bool) -> CoreResult<bool>,
    {
        loop {
            let Some((sequence, index, fin)) = self.entries.first().copied() else {
                return Ok(());
            };
            if sequence >= ready_through {
                return Ok(());
            }
            if !complete(index, fin)? {
                return Ok(());
            }
            let _ = self.entries.remove(0);
        }
    }

    fn sack_blocks(
        &self,
        buffers: &DataPlaneBuffers,
        ready_through: u32,
    ) -> CoreResult<([TcpSackBlock; 4], usize)> {
        let mut blocks = [TcpSackBlock {
            left_edge: 0,
            right_edge: 0,
        }; 4];
        let mut count = 0usize;
        let mut current: Option<TcpSackBlock> = None;
        for (sequence, index, fin) in self.entries.iter().copied() {
            if sequence < ready_through {
                continue;
            }
            let right_edge = Self::end_sequence(buffers, sequence, index, fin)?;
            match current.as_mut() {
                Some(block) if sequence <= block.right_edge => {
                    block.right_edge = block.right_edge.max(right_edge);
                }
                Some(block) => {
                    if count == blocks.len() {
                        break;
                    }
                    blocks[count] = *block;
                    count += 1;
                    current = Some(TcpSackBlock {
                        left_edge: sequence,
                        right_edge,
                    });
                }
                None => {
                    current = Some(TcpSackBlock {
                        left_edge: sequence,
                        right_edge,
                    });
                }
            }
        }
        if let Some(block) = current
            && count < blocks.len()
        {
            blocks[count] = block;
            count += 1;
        }
        Ok((blocks, count))
    }
}

pub(crate) struct TcpSessionQueue<C>
where
    C: CongestionController + 'static,
{
    driver: SessionDriverRuntime<TcpConnectionState<C>>,
    pub(super) protocol: TcpSessionProtocol,
}

impl<C> TcpSessionQueue<C>
where
    C: CongestionController + 'static,
{
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
    pub(crate) fn insert_session(&mut self, connection: TcpConnectionState<C>) -> SessionId {
        self.driver.insert_session(connection)
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> SessionId
    where
        F: SessionStateFactory<TcpConnectionState<C>>,
    {
        self.driver.insert_session_with_id(f)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn session_state(
        &self,
        session_id: SessionId,
    ) -> Option<&TcpConnectionState<C>> {
        self.driver.session_state(session_id)
    }

    pub(crate) fn take_connection<S>(
        &mut self,
        session_id: SessionId,
    ) -> CoreResult<TcpConnection<S, C>>
    where
        TcpConnection<S, C>: TryFrom<TcpConnectionState<C>, Error = CoreError>,
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
    ) -> CoreResult<Option<SessionEntry<TcpConnectionState<C>>>> {
        let closed = self.driver.close_session(session_id)?;
        if closed.is_some() {
            self.protocol.release_rx(self.driver.buffers(), session_id);
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
        let connection: TcpConnection<Closed, _> =
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
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        dispatch_session_queue_once_at(
            runtime,
            &mut self.driver,
            &mut self.protocol,
            now,
            output_next,
            output,
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

impl<C> TcpConnection<Closed, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn connect(
        self,
        queue: &mut TcpSessionQueue<C>,
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

impl<C> TcpConnection<Listen, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_syn(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
        if !packet.flags.contains(TcpSegmentFlags::SYN)
            || packet
                .flags
                .intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
        {
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(None);
        }
        let (connection, segment) = self.accept_syn(packet);
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
        Ok(Some(segment))
    }
}

impl<C> TcpConnection<SynSent, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_open_reply(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
            && self.unacceptable_ack(acknowledgment)
        {
            if packet.flags.contains(TcpSegmentFlags::RST) {
                queue.driver.replace_session_state(session_id, self.into());
                return Ok(None);
            }
            let segment = self.control_segment(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(Some(segment));
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
            let (mut connection, segment) = self.accept_syn_ack(packet, acknowledgment);
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
            return Ok(Some(segment));
        }

        let (connection, segment) = self.accept_simultaneous_syn(packet);
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
        Ok(Some(segment))
    }
}

impl<C> TcpConnection<SynRcvd, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_final_ack(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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
            let segment = self.control_segment(packet, TcpSegmentFlags::RST, Some(acknowledgment));
            queue.driver.replace_session_state(session_id, self.into());
            return Ok(Some(segment));
        }
        let (mut connection, segment) = self.accept_final_ack(packet, acknowledgment);
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
        Ok(Some(segment))
    }
}

impl<C> TcpConnection<Established, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_data(
        mut self,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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
            self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
        }

        let mut ack = false;
        if packet.payload_len != 0 {
            ack = true;
            let buffers = runtime.packet_buffers();
            let mut sequence = packet.sequence;
            let mut payload_len = packet.payload_len;
            let fin = packet.flags.contains(TcpSegmentFlags::FIN);
            let ready_through = self.rcv_nxt();
            let mut dsack = None;

            buffers.advance(index, packet.payload_offset)?;

            if sequence < ready_through {
                let duplicate_end = sequence.wrapping_add(payload_len as u32).min(ready_through);
                if duplicate_end > sequence {
                    dsack = Some(TcpSackBlock {
                        left_edge: sequence,
                        right_edge: duplicate_end,
                    });
                }
                let trim = ready_through.saturating_sub(sequence) as usize;
                if trim >= payload_len {
                    payload_len = 0;
                } else {
                    buffers.advance(index, trim)?;
                    sequence = ready_through;
                    payload_len -= trim;
                }
            }

            if payload_len != 0 {
                let end = sequence.wrapping_add(payload_len as u32);
                if let Some((overlap_start, overlap_end)) = queue
                    .protocol
                    .rx(session_id)
                    .map(|rx| rx.first_overlap(buffers, ready_through, sequence, end))
                    .transpose()?
                    .flatten()
                {
                    dsack.get_or_insert(TcpSackBlock {
                        left_edge: overlap_start,
                        right_edge: overlap_end,
                    });
                    if overlap_start == sequence {
                        let trim = overlap_end.saturating_sub(sequence) as usize;
                        if trim >= payload_len {
                            payload_len = 0;
                        } else {
                            buffers.advance(index, trim)?;
                            sequence = overlap_end;
                            payload_len -= trim;
                        }
                    } else {
                        payload_len = overlap_start.saturating_sub(sequence) as usize;
                    }
                }
            }

            if payload_len != 0 {
                runtime.truncate_chain(index, payload_len)?;
                let rx = queue.protocol.rx_mut_or_alloc(session_id);
                rx.insert(sequence, index, fin);
                rx.advance_connection(buffers, &mut self)?;
            } else {
                runtime.free_index(index);
            }

            let (sack_blocks, sack_block_count) = queue
                .protocol
                .rx(session_id)
                .map(|rx| rx.sack_blocks(buffers, self.rcv_nxt()))
                .transpose()?
                .unwrap_or((
                    [TcpSackBlock {
                        left_edge: 0,
                        right_edge: 0,
                    }; 4],
                    0,
                ));
            self.update_ack_sack_blocks(&sack_blocks[..sack_block_count], dsack);
            let driver = &queue.driver;
            let ready_through = self.rcv_nxt();
            if let Some(rx) = queue.protocol.rx_index.lookup(&session_id.get()) {
                let rx = queue
                    .protocol
                    .rx
                    .get_mut(rx)
                    .expect("tcp rx index is valid");
                rx.deliver_ready(ready_through, |buffer, fin| {
                    driver.enqueue_rx(session_id, buffer, fin)
                })?;
            }
        }

        if packet.flags.contains(TcpSegmentFlags::FIN)
            && packet.sequence.wrapping_add(packet.payload_len as u32) == self.rcv_nxt()
        {
            let (connection, segment) = self.accept_fin(packet);
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
            return Ok(Some(segment));
        }

        let segment = ack.then(|| self.control_segment(packet, TcpSegmentFlags::ACK, None));
        queue.protocol.remember_session(
            session_id,
            self.connection_id(),
            self.local(),
            self.remote(),
            self.owner_worker(),
            self.next_node(),
        );
        queue.driver.replace_session_state(session_id, self.into());
        Ok(segment)
    }
}

impl<C> TcpConnection<CloseWait, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_close_wait(
        mut self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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
            self.receive_ack(
                acknowledgment,
                packet.advertised_window,
                packet.sack_blocks.as_slice(),
            );
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            let (connection, segment) = self.accept_repeated_fin(packet);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            return Ok(Some(segment));
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl<C> TcpConnection<FinWait1, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_fin_wait1(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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
                let (connection, segment) = self.accept_fin_ack(packet, acknowledgment);
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
                Ok(Some(segment))
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
                let (connection, segment) = self.accept_fin(packet);
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
                Ok(Some(segment))
            }
            _ => {
                queue.driver.replace_session_state(session_id, self.into());
                Ok(None)
            }
        }
    }
}

impl<C> TcpConnection<FinWait2, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_fin_wait2(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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
            let (connection, segment) = self.accept_fin(packet);
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
            return Ok(Some(segment));
        }
        queue.driver.replace_session_state(session_id, self.into());
        Ok(None)
    }
}

impl<C> TcpConnection<Closing, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_closing(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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

impl<C> TcpConnection<LastAck, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_last_ack(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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

impl<C> TcpConnection<TimeWait, C>
where
    C: CongestionController + 'static,
{
    pub(crate) fn receive_time_wait(
        self,
        queue: &mut TcpSessionQueue<C>,
        session_id: SessionId,
        packet: &super::segment::TcpPacket,
    ) -> CoreResult<Option<TcpSegment>> {
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

impl<C> SessionQueueProtocol<TcpConnectionState<C>> for TcpSessionProtocol
where
    C: CongestionController + 'static,
{
    fn handle_timer_expiry(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<
            '_,
            TcpConnectionState<C>,
        >,
        expiry: SessionTimerExpiry,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        let Some(kind) = Self::timer_kind(expiry.token()) else {
            return Ok(());
        };
        let timer_output = context
            .session_state_mut(expiry.session_id())
            .and_then(|connection| {
                connection.tcp_timer_expire(kind);
                connection.on_tcp_timer(kind)
            });
        if let Some((kind, segment)) = timer_output {
            let index = context.buffers().alloc_index(Default::default())?;
            if let Err(error) = self.insert_segment(index, segment) {
                context.buffers().free_index(index);
                return Err(error);
            }
            let token = TcpSessionProtocol::timer_token(kind);
            if let Some(token) = token {
                if let Err(error) =
                    context.arm_timer_ticks(expiry.session_id(), token, TCP_ACTIVE_OPEN_TIMER_TICKS)
                {
                    self.remove_segment(index);
                    context.buffers().free_index(index);
                    return Err(error);
                }
            }
            if let Err(error) = output.enqueue(runtime, output_next.node(), index) {
                if let Some(token) = token {
                    context.cancel_timer(expiry.session_id(), token);
                }
                self.remove_segment(index);
                context.buffers().free_index(index);
                return Err(error);
            }
        }
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<
            '_,
            TcpConnectionState<C>,
        >,
        session_id: SessionId,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        if let Some(TcpConnectionState::Established(connection)) =
            context.session_state_mut(session_id)
        {
            let ready_through = connection.rcv_nxt();
            if let Some(index) = self.rx_index.lookup(&session_id.get()) {
                let rx = self.rx.get_mut(index).expect("tcp rx index is valid");
                rx.deliver_ready(ready_through, |buffer, fin| {
                    context.enqueue_rx(session_id, buffer, fin)
                })?;
            }
        }
        let timer_output = context
            .session_state_mut(session_id)
            .and_then(|connection| connection.on_tcp_ready());
        if let Some(segment) = timer_output {
            let index = context.buffers().alloc_index(Default::default())?;
            if let Err(error) = self.insert_segment(index, segment) {
                context.buffers().free_index(index);
                return Err(error);
            }
            if let Err(error) = output.enqueue(runtime, output_next.node(), index) {
                self.remove_segment(index);
                context.buffers().free_index(index);
                return Err(error);
            }
        }
        Ok(())
    }

    fn tx_payload_len(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<
            '_,
            TcpConnectionState<C>,
        >,
        session_id: SessionId,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<usize> {
        let Some(TcpConnectionState::Established(connection)) = context.session_state(session_id)
        else {
            return Ok(0);
        };
        Ok(connection.tx_payload_len(pending_len, now))
    }

    fn prepare_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<
            '_,
            TcpConnectionState<C>,
        >,
        session_id: SessionId,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let Some(TcpConnectionState::Established(connection)) =
            context.session_state_mut(session_id)
        else {
            return Err(CoreError::internal(
                "tcp tx prepare requires established connection",
            ));
        };
        let budget = connection.tx_payload_len(payload_len, now);
        if payload_len > budget {
            return Err(CoreError::internal("tcp tx payload exceeds send budget"));
        }
        let segment = connection.tx_segment(payload_len)?;
        self.insert_segment(index, segment)?;
        Ok(())
    }

    fn cancel_tx(&mut self, index: BufferIndex) {
        self.remove_segment(index);
    }

    fn commit_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<
            '_,
            TcpConnectionState<C>,
        >,
        session_id: SessionId,
        _: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()> {
        let Some(state) = context.session_state_mut(session_id) else {
            return Err(CoreError::internal("tcp tx commit state is missing"));
        };
        let mut connection: TcpConnection<Established, _> =
            state.clone().try_into()?;
        let timers = connection.commit_payload_tx(payload_len, now)?;
        let rack_ticks = timers
            .contains(TcpConnectionTimerKind::RACK)
            .then(|| connection.tcp_timer_ticks(TcpConnectionTimerKind::RACK))
            .flatten();
        let tlp_ticks = timers
            .contains(TcpConnectionTimerKind::TLP)
            .then(|| connection.tcp_timer_ticks(TcpConnectionTimerKind::TLP))
            .flatten();
        *state = connection.into();
        if let Some(ticks) = rack_ticks
            && let Some(token) = Self::timer_token(TcpConnectionTimerKind::RACK)
        {
            context.arm_timer_ticks(session_id, token, ticks)?;
        }
        if let Some(ticks) = tlp_ticks
            && let Some(token) = Self::timer_token(TcpConnectionTimerKind::TLP)
        {
            context.arm_timer_ticks(session_id, token, ticks)?;
        }
        Ok(())
    }
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    index: TcpSessionConnectionIndex,
    pending_index: TcpPendingIndex,
    segments: Pool<TcpSegment>,
    segment_index: FlatHashTable<u128, PoolIndex>,
    rx: Pool<TcpSessionRx>,
    rx_index: FlatHashTable<u64, PoolIndex>,
    next_iss: u32,
}

impl TcpSessionProtocol {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            index: TcpSessionConnectionIndex::empty(),
            pending_index: TcpPendingIndex::empty(),
            segments: Pool::with_capacity(1024),
            segment_index: FlatHashTable::new(),
            rx: Pool::with_capacity(1024),
            rx_index: FlatHashTable::new(),
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

    pub(crate) fn insert_segment(
        &mut self,
        index: BufferIndex,
        segment: TcpSegment,
    ) -> CoreResult<()> {
        let key = buffer_index_key(index);
        if self.segment_index.lookup(&key).is_some() {
            return Err(CoreError::internal("tcp segment already exists"));
        }
        let segment_index = self
            .segments
            .insert(segment)
            .ok_or_else(|| CoreError::internal("tcp segment pool exhausted"))?;
        self.segment_index.insert(key, segment_index);
        Ok(())
    }

    pub(crate) fn remove_segment(&mut self, index: BufferIndex) -> Option<TcpSegment> {
        let key = buffer_index_key(index);
        let segment_index = self.segment_index.remove(&key)?;
        self.segments.remove(segment_index)
    }

    #[inline]
    pub(crate) fn take_segment(&mut self, index: BufferIndex) -> Option<TcpSegment> {
        self.remove_segment(index)
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

    fn release_rx(&mut self, buffers: &DataPlaneBuffers, session_id: SessionId) {
        let Some(index) = self.rx_index.remove(&session_id.get()) else {
            return;
        };
        let Some(rx) = self.rx.remove(index) else {
            return;
        };
        rx.release(buffers);
    }

    fn rx(&self, session_id: SessionId) -> Option<&TcpSessionRx> {
        let index = self.rx_index.lookup(&session_id.get())?;
        self.rx.get(index)
    }

    fn rx_mut_or_alloc(&mut self, session_id: SessionId) -> &mut TcpSessionRx {
        let key = session_id.get();
        let index = match self.rx_index.lookup(&key) {
            Some(index) => index,
            None => {
                let index = self
                    .rx
                    .insert(TcpSessionRx::default())
                    .expect("tcp rx pool exhausted");
                self.rx_index.insert(key, index);
                index
            }
        };
        self.rx.get_mut(index).expect("tcp rx index is valid")
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
    pub fn mark_session_ready<C>(
        &mut self,
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
    )
    where
        C: CongestionController + 'static,
    {
        context.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_retransmit_timer<C>(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
        ticks: u64,
    ) -> CoreResult<()>
    where
        C: CongestionController + 'static,
    {
        Self::arm_tcp_timer_ticks(
            context,
            session_id,
            TcpConnectionTimerKind::RETRANSMIT,
            ticks,
        )
    }

    #[inline]
    pub fn cancel_retransmit_timer<C>(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
    ) -> bool
    where
        C: CongestionController + 'static,
    {
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
    pub fn arm_tcp_timer_ticks<C>(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
        ticks: u64,
    ) -> CoreResult<()>
    where
        C: CongestionController + 'static,
    {
        let Some(token) = Self::timer_token(kind) else {
            return Ok(());
        };
        context.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_tcp_timer<C>(
        context: &mut SessionProtocolContext<'_, TcpConnectionState<C>>,
        session_id: SessionId,
        kind: TcpConnectionTimerKind,
    ) -> bool
    where
        C: CongestionController + 'static,
    {
        let Some(token) = Self::timer_token(kind) else {
            return false;
        };
        context.cancel_timer(session_id, token)
    }

    #[inline]
    pub(crate) fn register_queue<C>(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
    ) -> CoreResult<TcpSessionQueueHandle<C>>
    where
        C: CongestionController + 'static,
    {
        crate::session::node::register_session_queue(TcpSessionQueue::<C>::new(worker, buffers))
    }

    #[inline]
    pub(crate) fn connect<C>(
        handle: TcpSessionQueueHandle<C>,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> CoreResult<SessionId>
    where
        C: CongestionController + 'static,
    {
        Self::with_queue(handle, |queue: &mut TcpSessionQueue<C>| {
            queue.connect(local, remote)
        })
    }

    #[inline]
    pub(crate) fn session_queue_dispatch_fn<C>() -> SessionQueueDispatchFn
    where
        C: CongestionController + 'static,
    {
        tcp_session_queue_dispatch::<C>
    }

    #[inline]
    pub(crate) fn with_queue<C, R, F>(
        handle: TcpSessionQueueHandle<C>,
        f: F,
    ) -> CoreResult<R>
    where
        C: CongestionController + 'static,
        F: crate::session::node::SessionQueueAccess<TcpSessionQueue<C>, R>,
    {
        crate::session::node::with_session_queue(handle, f)
    }

    #[inline]
    pub(crate) fn take_segment_for_buffer<C>(
        handle: TcpSessionQueueHandle<C>,
        index: BufferIndex,
    ) -> CoreResult<Option<TcpSegment>>
    where
        C: CongestionController + 'static,
    {
        Self::with_queue(handle, |queue: &mut TcpSessionQueue<C>| {
            Ok(queue.protocol.take_segment(index))
        })
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_for_test<C>(
        queue: TcpSessionQueue<C>,
    ) -> CoreResult<TcpSessionQueueHandle<C>>
    where
        C: CongestionController + 'static,
    {
        crate::session::node::register_session_queue(queue)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_with_connection_for_test<C>(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        connection: TcpConnectionState<C>,
    ) -> CoreResult<TcpSessionQueueHandle<C>>
    where
        C: CongestionController + 'static,
    {
        let mut queue = TcpSessionQueue::<C>::new(worker, buffers);
        let session_id = queue.insert_session(connection);
        let connection: TcpConnection<Established, C> = queue.take_connection(session_id)?;
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

fn tcp_session_queue_dispatch<C>(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    output: &mut SessionQueueOutput,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    TcpSessionProtocol::with_queue(
        SessionQueueHandle::<TcpSessionQueue<C>>::new(data),
        |queue: &mut TcpSessionQueue<C>| {
            queue.dispatch_once_at(runtime, now, output_next, output)?;
            Ok(())
        },
    )
}

fn buffer_index_key(index: BufferIndex) -> u128 {
    (u128::from(index.pool_id()) << 64)
        | (u128::from(index.slot()) << 32)
        | u128::from(index.generation())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeProcessFn, NodeResult,
        NodeRuntimeData, RouteMetadata,
    };
    use hammer_core::error::CoreError;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpConnectionId, TcpSackBlock, TcpSegmentFlags, TcpSegmentView,
        tcp_options_from_bytes,
    };
    use crate::transport::congestion::BbrController;
    use hammer_runtime::app::{
        AppCqeKind, AppOpId, AppRingHandle, AppSendData, AppSqe, AppUserData,
    };

    use super::*;
    use crate::session::SessionQueueNode;
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};
    use crate::transport::tcp::segment::TcpPacket;
    use std::sync::{Arc, Mutex, OnceLock};

    const ACTIVE_OPEN_ISS: u32 = 81_000;

    #[inline]
    const fn unused_output_next() -> SessionQueueNext {
        SessionQueueNext::from_node(NodeId::new(0))
    }

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
        indices: std::vec::Vec<BufferIndex>,
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
            let mut state = state
                .lock()
                .map_err(|_| CoreError::internal("capture poisoned"))?;
            state.indices.push(index);
            state.packets.push(packet.into_iter().collect());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    type TestTcpConnectionState = TcpConnectionState<BbrController>;
    type TestTcpSessionQueue = TcpSessionQueue<BbrController>;

    fn tcp_connection() -> TestTcpConnectionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        TestTcpConnectionState::established_for_test(
            Some(TcpConnectionId::new(7001)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    fn tcp_connection_with_sack() -> TestTcpConnectionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        let mut listen: TcpConnection<Listen, BbrController> = TcpConnection::new(
            Some(TcpConnectionId::new(7002)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        );
        listen.set_local_capabilities(TcpCapabilities {
            sack: true,
            ..TcpCapabilities::default()
        });
        let syn = TcpPacket {
            local: remote,
            remote: local,
            flags: TcpSegmentFlags::SYN,
            sequence: 7_000,
            acknowledgment: None,
            advertised_window: u16::MAX,
            payload_offset: 0,
            payload_len: 0,
            capabilities: TcpCapabilities {
                sack: true,
                ..TcpCapabilities::default()
            },
            sack_blocks: Vec::new(),
        };
        let (syn_rcvd, _) = listen.accept_syn(&syn);
        let final_ack = TcpPacket {
            acknowledgment: Some(syn_rcvd.snd_nxt()),
            flags: TcpSegmentFlags::ACK,
            ..syn
        };
        let (established, _) =
            syn_rcvd.accept_final_ack(&final_ack, final_ack.acknowledgment.expect("ack"));
        established.into()
    }

    fn syn_sent_connection(
        worker: DataWorkerId,
        local: SocketAddr,
        remote: SocketAddr,
        iss: u32,
        capabilities: TcpCapabilities,
    ) -> TestTcpConnectionState {
        let closed: TcpConnection<Closed, BbrController> =
            TcpConnection::new(None, worker, local.port(), Some(local), remote);
        let mut syn_sent = closed.connect_state(iss);
        syn_sent.set_local_capabilities(capabilities);
        syn_sent.into()
    }

    fn remember_established(
        queue: &mut TestTcpSessionQueue,
        session_id: SessionId,
    ) -> CoreResult<()> {
        let connection: TcpConnection<Established, BbrController> =
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

    fn remember_pending(queue: &mut TestTcpSessionQueue, session_id: SessionId) -> CoreResult<()> {
        let connection: TcpConnection<SynSent, BbrController> =
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
        queue: &mut TestTcpSessionQueue,
        connection: TestTcpConnectionState,
    ) -> CoreResult<SessionId> {
        let session_id = queue.insert_session(connection);
        remember_pending(queue, session_id)?;
        queue.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        queue.mark_session_ready(session_id);
        Ok(session_id)
    }

    fn segment_sack_blocks(segment: &TcpSegment) -> std::vec::Vec<TcpSackBlock> {
        let mut header = [0u8; 64];
        let written = segment.write_header(&mut header).expect("write header");
        tcp_options_from_bytes(&header[20..written]).sack_blocks
    }

    #[test]
    fn tcp_session_protocol_takes_segment_once() {
        let mut protocol = TcpSessionProtocol::new(DataWorkerId::new(0));
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let index = runtime
            .packet_buffers()
            .alloc_index(RouteMetadata::default())
            .expect("buffer");
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
        let segment = TcpSegment::new(
            local,
            remote,
            1,
            2,
            4096,
            TcpSegmentFlags::ACK,
            TcpCapabilities::default(),
            None,
            0,
        );

        protocol.insert_segment(index, segment).expect("insert");
        assert!(protocol.take_segment(index).is_some());
        assert!(protocol.take_segment(index).is_none());
    }

    #[test]
    fn tcp_session_protocol_rejects_duplicate_segment_for_buffer() {
        let mut protocol = TcpSessionProtocol::new(DataWorkerId::new(0));
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let index = runtime
            .packet_buffers()
            .alloc_index(RouteMetadata::default())
            .expect("buffer");
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
        let segment = TcpSegment::new(
            local,
            remote,
            1,
            2,
            4096,
            TcpSegmentFlags::ACK,
            TcpCapabilities::default(),
            None,
            0,
        );

        protocol
            .insert_segment(index, segment)
            .expect("first insert");
        assert!(protocol.insert_segment(index, segment).is_err());
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
        let capture_node = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let drop = runtime
            .nodes()
            .register_internal(crate::data_plane::DropNode::new());
        let output = runtime.nodes().register_internal(TcpOutputNode::new(
            TcpOutputNext::nodes(drop, capture_node),
            handle,
        ));
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
            TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
                let connection: TcpConnection<SynSent, BbrController> =
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
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 3);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            active_open_iss,
            TcpCapabilities::default(),
        );

        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            let connection: TcpConnection<SynSent, BbrController> =
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
        let capture_node = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let drop = runtime
            .nodes()
            .register_internal(crate::data_plane::DropNode::new());
        let output = runtime.nodes().register_internal(TcpOutputNode::new(
            TcpOutputNext::nodes(drop, capture_node),
            handle,
        ));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(output),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        let session_id = TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
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
        assert_eq!(runtime.run_ready_nodes().expect("run first syn"), 3);
        capture.lock().unwrap().packets.clear();

        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
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

        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            queue.expire_timers_for_test(1).expect("expire retransmit");
            Ok(())
        })
        .expect("expire retransmit");
        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule retransmit");
        assert_eq!(runtime.run_ready_nodes().expect("run retransmit"), 3);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            ACTIVE_OPEN_ISS,
            TcpCapabilities::default(),
        );
        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            let connection: TcpConnection<SynSent, BbrController> =
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
        assert_eq!(closes[0].session_id(), session_id);
        assert_eq!(closes[0].op(), op);
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
        assert_eq!(closes[0].session_id(), second_session_id);
        assert_eq!(closes[0].op(), second_op);
    }

    #[test]
    fn established_app_send_reaches_session_output() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let local: SocketAddr = "127.0.0.1:10000".parse().expect("local");
        let remote: SocketAddr = "127.0.0.1:20000".parse().expect("remote");
        let connection = TestTcpConnectionState::established_for_test(
            Some(TcpConnectionId::new(7)),
            worker,
            local.port(),
            Some(local),
            remote,
        );
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(connection);
        remember_established(&mut queue, session_id).expect("remember established");
        let start_snd_nxt = {
            let connection: TcpConnection<Established, BbrController> =
                queue.take_connection(session_id).expect("established");
            let snd_nxt = connection.snd_nxt();
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            snd_nxt
        };

        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"hello").expect("data"))
            .try_into()
            .expect("transfer");
        queue.driver.app_mut().push_pending_send(session_id, send);
        queue.mark_session_ready(session_id);

        let handle = TcpSessionProtocol::register_queue_for_test(queue).expect("register queue");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(lookup),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(
            runtime
                .run_ready_nodes()
                .expect("run session and output capture"),
            2
        );

        let index = {
            let capture = capture.lock().unwrap();
            assert_eq!(capture.packets.len(), 1);
            assert_eq!(capture.packets[0].as_slice(), b"hello");
            capture.indices[0]
        };
        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            let segment = queue.protocol.take_segment(index).expect("segment");
            assert_eq!(segment.payload_len(), 5);
            assert!(queue.protocol.take_segment(index).is_none());
            let connection: TcpConnection<Established, BbrController> =
                queue.take_connection(session_id)?;
            assert_eq!(connection.snd_nxt(), start_snd_nxt + 5);
            assert_eq!(connection.recovery().bytes_in_flight(), 5);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            Ok(())
        })
        .expect("inspect tx intent");
    }

    #[test]
    fn established_app_send_stays_pending_until_cumulative_ack_arrives() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let local: SocketAddr = "127.0.0.1:10000".parse().expect("local");
        let remote: SocketAddr = "127.0.0.1:20000".parse().expect("remote");
        let connection = TestTcpConnectionState::established_for_test(
            Some(TcpConnectionId::new(8)),
            worker,
            local.port(),
            Some(local),
            remote,
        );
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(connection);
        remember_established(&mut queue, session_id).expect("remember established");

        let (start_snd_nxt, local, remote) = {
            let connection: TcpConnection<Established, BbrController> =
                queue.take_connection(session_id).expect("established");
            let local = connection.local().expect("local");
            let remote = connection.remote();
            let snd_nxt = connection.snd_nxt();
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            (snd_nxt, local, remote)
        };

        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"hello").expect("data"))
            .try_into()
            .expect("transfer");
        queue.driver.app_mut().push_pending_send(session_id, send);
        queue.mark_session_ready(session_id);

        let handle = TcpSessionProtocol::register_queue_for_test(queue).expect("register queue");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(lookup),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(
            runtime
                .run_ready_nodes()
                .expect("run session and output capture"),
            2
        );

        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            assert!(
                queue.driver.app().has_pending_send(session_id),
                "pending send must stay retained until ack cleanup"
            );
            let connection: TcpConnection<Established, BbrController> =
                queue.take_connection(session_id)?;
            assert_eq!(connection.snd_nxt(), start_snd_nxt + 5);
            queue
                .driver
                .replace_session_state(session_id, connection.into());
            Ok(())
        })
        .expect("inspect retained send");

        let packet = runtime
            .alloc_index_with_bytes(Default::default(), &[])
            .expect("ack packet buffer");
        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            let connection: TcpConnection<Established, BbrController> =
                queue.take_connection(session_id)?;
            let sequence = connection.rcv_nxt();
            let _ = connection.receive_data(
                &runtime,
                packet,
                queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence,
                    acknowledgment: Some(start_snd_nxt + 5),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 0,
                },
            )?;
            Ok(())
        })
        .expect("receive cumulative ack");

        TcpSessionProtocol::with_queue(handle, |queue: &mut _| {
            assert!(
                !queue.driver.app().has_pending_send(session_id),
                "cumulative ack must release retained send"
            );
            Ok(())
        })
        .expect("inspect ack cleanup");
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
            .driver
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
    fn established_receive_out_of_order_payload_emits_sack_and_keeps_state_open() {
        let runtime = DataPlaneRuntime::with_capacities(256, 8, 4, 4);
        let mut queue =
            TcpSessionQueue::new(DataWorkerId::new(0), runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection_with_sack());
        remember_established(&mut queue, session_id).expect("remember established");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let local = connection.local().expect("local");
        let remote = connection.remote();
        let rcv_nxt = connection.rcv_nxt();
        let snd_nxt = connection.snd_nxt();
        queue
            .driver
            .replace_session_state(session_id, connection.into());

        let buffer = runtime
            .alloc_index_with_bytes(Default::default(), b"later")
            .expect("payload");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let segment = connection
            .receive_data(
                &runtime,
                buffer,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: rcv_nxt + 5,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive data")
            .expect("ack segment");

        assert_eq!(
            segment_sack_blocks(&segment),
            vec![TcpSackBlock {
                left_edge: rcv_nxt + 5,
                right_edge: rcv_nxt + 10,
            }]
        );

        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        assert_eq!(connection.rcv_nxt(), rcv_nxt);
        queue
            .driver
            .replace_session_state(session_id, connection.into());
    }

    #[test]
    fn established_gap_fill_advances_rcv_nxt_and_delivers_only_first_ready_payload() {
        let runtime = DataPlaneRuntime::with_capacities(256, 8, 4, 4);
        let mut queue =
            TcpSessionQueue::new(DataWorkerId::new(0), runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(8, 8);
        let op = AppOpId::new(7_102);
        let session_id = queue.insert_session(tcp_connection_with_sack());
        remember_established(&mut queue, session_id).expect("remember established");
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));

        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let local = connection.local().expect("local");
        let remote = connection.remote();
        let start_rcv_nxt = connection.rcv_nxt();
        let snd_nxt = connection.snd_nxt();
        queue
            .driver
            .replace_session_state(session_id, connection.into());

        let later = runtime
            .alloc_index_with_bytes(Default::default(), b"later")
            .expect("later");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        connection
            .receive_data(
                &runtime,
                later,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt + 5,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive later");

        ring.try_push_submission(AppSqe::recv(Some(AppUserData::new(44)), op, 32))
            .expect("push recv");
        let hello = runtime
            .alloc_index_with_bytes(Default::default(), b"hello")
            .expect("hello");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let segment = connection
            .receive_data(
                &runtime,
                hello,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive hello")
            .expect("ack segment");

        assert!(segment_sack_blocks(&segment).is_empty());

        let completion = ring.pop_completion().expect("recv completion");
        let recv = completion.into_recv().expect("recv data");
        assert_eq!(
            recv.copy_current().expect("recv payload"),
            b"hello".to_vec()
        );
        recv.release();
        assert!(ring.pop_completion().is_none());

        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        assert_eq!(connection.rcv_nxt(), start_rcv_nxt + 10);
        queue
            .driver
            .replace_session_state(session_id, connection.into());
    }

    #[test]
    fn recv_submission_retries_delivery_of_retained_payload() {
        let runtime = DataPlaneRuntime::with_capacities(256, 8, 4, 4);
        let mut queue =
            TcpSessionQueue::new(DataWorkerId::new(0), runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(8, 8);
        let op = AppOpId::new(7_103);
        let session_id = queue.insert_session(tcp_connection_with_sack());
        remember_established(&mut queue, session_id).expect("remember established");
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));

        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let local = connection.local().expect("local");
        let remote = connection.remote();
        let start_rcv_nxt = connection.rcv_nxt();
        let snd_nxt = connection.snd_nxt();
        queue
            .driver
            .replace_session_state(session_id, connection.into());

        let later = runtime
            .alloc_index_with_bytes(Default::default(), b"later")
            .expect("later");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        connection
            .receive_data(
                &runtime,
                later,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt + 5,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive later");

        let hello = runtime
            .alloc_index_with_bytes(Default::default(), b"hello")
            .expect("hello");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        connection
            .receive_data(
                &runtime,
                hello,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive hello");

        ring.try_push_submission(AppSqe::recv(Some(AppUserData::new(55)), op, 32))
            .expect("push first recv");
        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch first recv");
        let first = ring.pop_completion().expect("first completion");
        let first_recv = first.into_recv().expect("first recv");
        assert_eq!(
            first_recv.copy_current().expect("first payload"),
            b"hello".to_vec()
        );
        first_recv.release();

        ring.try_push_submission(AppSqe::recv(Some(AppUserData::new(56)), op, 32))
            .expect("push second recv");
        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch second recv");
        let second = ring.pop_completion().expect("second completion");
        let second_recv = second.into_recv().expect("second recv");
        assert_eq!(
            second_recv.copy_current().expect("second payload"),
            b"later".to_vec()
        );
        second_recv.release();
    }

    #[test]
    fn established_duplicate_payload_emits_dsack_block() {
        let runtime = DataPlaneRuntime::with_capacities(256, 8, 4, 4);
        let mut queue =
            TcpSessionQueue::new(DataWorkerId::new(0), runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection_with_sack());
        remember_established(&mut queue, session_id).expect("remember established");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let local = connection.local().expect("local");
        let remote = connection.remote();
        let start_rcv_nxt = connection.rcv_nxt();
        let snd_nxt = connection.snd_nxt();
        queue
            .driver
            .replace_session_state(session_id, connection.into());

        let first = runtime
            .alloc_index_with_bytes(Default::default(), b"hello")
            .expect("first");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        connection
            .receive_data(
                &runtime,
                first,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive first");

        let duplicate = runtime
            .alloc_index_with_bytes(Default::default(), b"hello")
            .expect("duplicate");
        let connection: TcpConnection<Established, BbrController> =
            queue.take_connection(session_id).expect("established");
        let segment = connection
            .receive_data(
                &runtime,
                duplicate,
                &mut queue,
                session_id,
                &TcpPacket {
                    local,
                    remote,
                    sequence: start_rcv_nxt,
                    acknowledgment: Some(snd_nxt),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: Vec::new(),
                    payload_offset: 0,
                    payload_len: 5,
                },
            )
            .expect("receive duplicate")
            .expect("ack segment");

        assert_eq!(
            segment_sack_blocks(&segment),
            vec![TcpSackBlock {
                left_edge: start_rcv_nxt,
                right_edge: start_rcv_nxt + 5,
            }]
        );
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
