use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpSegmentFlags, TcpState};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::output::{TCP_FLAG_ACK, tcp_output_packet_flags};
use super::segment::parse_tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Output,
    Drop,
}

pub struct TcpRcvProcessControlPlane {
    next: [NodeId; TcpRcvProcessNext::COUNT],
}

impl TcpRcvProcessControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpRcvProcessNext::COUNT]) -> Self {
        Self { next }
    }

    #[inline]
    pub fn node(&self) -> TcpRcvProcessNode {
        TcpRcvProcessNode::new(self.next)
    }
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpRcvProcessNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpRcvProcessNode {
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
        tcp_rcv_process_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp rcv-process node missing session queue"))
    }
}

fn tcp_rcv_process_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpRcvProcessNode::runtime_nexts(runtime)?;
    tcp_rcv_process_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

fn tcp_rcv_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp rcv-process node missing session queue"))?;
    let output = next[TcpRcvProcessNext::Output as usize];
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_rcv_process_index(runtime, index, session_queue, output, &mut next_frames) {
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

fn tcp_rcv_process_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    let mut output_index = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
        let session_id = queue
            .session_id_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp rcv-process session is missing"))?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            queue.close_session(session_id)?;
            return Ok(());
        }
        let connection = queue
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("tcp rcv-process state is missing"))?;
        if connection.state() == TcpState::SynRcvd {
            let acknowledgment = packet
                .acknowledgment
                .ok_or_else(|| CoreError::internal("syn-rcvd packet missing ACK"))?;
            if !packet.flags.contains(TcpSegmentFlags::ACK)
                || !connection.accepts_ack(acknowledgment)
            {
                return Err(CoreError::internal("syn-rcvd ACK is unacceptable"));
            }
            connection.apply_ack(acknowledgment, packet.advertised_window);
            connection.set_state(TcpState::Established);
            let record = tcp_output_packet_flags(connection, packet.local, &[], TCP_FLAG_ACK)?;
            let allocated = record.alloc_finalized_header_buffer(runtime)?;
            output_index = Some(allocated);
            let indexed = connection.clone();
            queue.index_session(session_id, &indexed);
            queue.cancel_retransmit_timer(session_id);
        } else {
            if let Some(acknowledgment) = packet.acknowledgment
                && packet.flags.contains(TcpSegmentFlags::ACK)
            {
                connection.apply_ack(acknowledgment, packet.advertised_window);
            }
            if packet.flags.contains(TcpSegmentFlags::FIN) {
                if !connection.accept_in_order_payload(packet.sequence, 1) {
                    return Err(CoreError::internal("tcp FIN sequence is unacceptable"));
                }
                connection.set_state(TcpState::CloseWait);
                let record = tcp_output_packet_flags(connection, packet.local, &[], TCP_FLAG_ACK)?;
                let allocated = record.alloc_finalized_header_buffer(runtime)?;
                output_index = Some(allocated);
                let indexed = connection.clone();
                queue.index_session(session_id, &indexed);
            }
        }
        Ok(())
    });
    if let Err(error) = result {
        if let Some(output_index) = output_index.take() {
            runtime.free_index(output_index);
        }
        return Err(error);
    }
    if let Some(output_index) = output_index.take()
        && let Err(error) = next_frames.enqueue(runtime, output, output_index)
    {
        runtime.free_index(output_index);
        return Err(error);
    }
    runtime.free_index(index);
    Ok(())
}
