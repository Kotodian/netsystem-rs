use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::transport::congestion::CongestionController;

use super::{TCP_TIMER_DELAYED_ACK, TCP_TIMER_PERSIST, TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TIME_WAIT, TCP_TIMER_TLP, TcpQueueHandle, refresh_tcp_timers_for_session};
use super::segment::parse_tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode<C: CongestionController + 'static> {
    session_queue: TcpQueueHandle<C>,
}

impl<C> Node for TcpRcvProcessNode<C>
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
        tcp_rcv_process_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_rcv_process_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_rcv_process_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpRcvProcessNode::<C>::runtime_nexts(runtime)?;
    tcp_rcv_process_frame::<C>(runtime, frame, TcpQueueHandle::new(data), next)
}

fn tcp_rcv_process_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueueHandle<C>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpRcvProcessNext::Output as usize];
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_rcv_process_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
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

fn tcp_rcv_process_index<C>(
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
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let (session_id, _, _) = queue
            .session_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp rcv-process session is missing"))?;
        let (control, established_with_payload) = {
            let connection = queue
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("tcp rcv-process session is missing"))?;
            let previous_state = connection.state();
            let control = connection.receive_close_side(&packet)?;
            (
                control,
                previous_state == crate::transport::tcp::TcpState::SynRcvd
                    && connection.state() == crate::transport::tcp::TcpState::Established
                    && packet.payload_len != 0,
            )
        };
        if established_with_payload {
            {
                let mut buffer = runtime.packet_buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset)?;
                buffer.truncate_chain(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            release_input = false;
        }
        refresh_tcp_timers_for_session(
            &mut queue,
            session_id,
            (1u16 << TCP_TIMER_RETRANSMIT)
                | (1u16 << TCP_TIMER_RACK)
                | (1u16 << TCP_TIMER_TLP)
                | (1u16 << TCP_TIMER_DELAYED_ACK)
                | (1u16 << TCP_TIMER_PERSIST)
                | (1u16 << TCP_TIMER_TIME_WAIT),
        )?;
        queue.refresh_session_route(session_id)?;
        Ok(control)
    };
    let control = result?;
    if let Some(segment) = control {
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
