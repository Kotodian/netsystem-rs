use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use super::{TCP_TIMER_DELAYED_ACK, TCP_TIMER_PERSIST, TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TLP, TcpQueueHandle, refresh_tcp_timers_for_session};
use super::segment::parse_tcp_packet;
use crate::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode<C: CongestionController + 'static> {
    session_queue: TcpQueueHandle<C>,
}

impl<C> Node for TcpEstablishedNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_established_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_established_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpEstablishedNode::<C>::runtime_nexts(runtime)?;
    tcp_established_frame::<C>(runtime, frame, TcpQueueHandle::new(data), next)
}

fn tcp_established_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueueHandle<C>,
    next: [NodeId; TcpEstablishedNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpEstablishedNext::Output as usize];
    let drop_next = next[TcpEstablishedNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_established_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
            Ok(()) => Ok(None),
            Err(_) => {
                next_frames.enqueue(runtime, drop_next, index)?;
                Ok(None)
            }
        }
    })?;
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

fn tcp_established_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueueHandle<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut release_input = true;
    let mut tx_segment = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let (session_id, _, _) = queue
            .session_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
        let (control, ack_advanced, acked_tx_len, accept_payload, immediate_ack) = {
            let connection = queue
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
            let previous_snd_una = connection.snd_una();
            let (control, _) = connection.receive_established(&packet)?;
            let acked_tx_len = connection.take_acked_tx_len(previous_snd_una);
            (
                control,
                connection.snd_una() != previous_snd_una,
                acked_tx_len,
                connection.accept_payload(&packet),
                false,
            )
        };
        if acked_tx_len != 0 {
            queue.release_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            queue.mark_ready(session_id);
        }
        let mut immediate_ack = immediate_ack;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim);
            runtime
                .packet_buffers()
                .advance(index, packet.payload_offset.saturating_add(trim))?;
            runtime
                .packet_buffers()
                .truncate_chain(index, accepted_len)?;
            let enqueue = queue.enqueue_rx(session_id, index, offset, false)?;
            {
                let connection = queue
                    .session_mut(session_id)
                    .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
                connection.receive_payload(
                    packet.sequence,
                    trim as u32,
                    enqueue.delivered_len,
                    enqueue.newest_ooo_start,
                    enqueue.newest_ooo_len,
                );
                let clean_in_order = trim == 0
                    && offset == 0
                    && enqueue.delivered_len == accepted_len as u32
                    && enqueue.newest_ooo_start.is_none();
                immediate_ack = if clean_in_order {
                    connection.on_clean_in_order_payload()
                } else {
                    true
                };
            }
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            release_input = false;
        } else if packet.payload_len != 0 {
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            let connection = queue
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }
        if immediate_ack {
            let connection = queue
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
            tx_segment = Some(connection.control_segment(
                &packet,
                hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
                None,
            ));
        }
        refresh_tcp_timers_for_session(
            &mut queue,
            session_id,
            (1u16 << TCP_TIMER_RETRANSMIT)
                | (1u16 << TCP_TIMER_RACK)
                | (1u16 << TCP_TIMER_TLP)
                | (1u16 << TCP_TIMER_DELAYED_ACK)
                | (1u16 << TCP_TIMER_PERSIST),
        )?;
        queue.refresh_session_route(session_id)?;
        if tx_segment.is_none() {
            tx_segment = control;
        }
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }
    if let Some(segment) = tx_segment.take() {
        let allocated = runtime.packet_buffers().alloc_index()?;
        if let Err(error) = segment.write_to_buffer(runtime.packet_buffers(), allocated) {
            runtime.free_index(allocated);
            return Err(error);
        }
        if let Err(error) = next_frames.enqueue(runtime, tcp_output, allocated) {
            runtime.free_index(allocated);
            return Err(error);
        }
    }
    if release_input {
        runtime.free_index(index);
    }
    Ok(())
}
