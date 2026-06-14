use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpSegmentFlags, TcpState};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::segment::{alloc_tcp_segment_for_connection, parse_tcp_packet};

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpEstablishedNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpEstablishedNode {
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
        tcp_established_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp established node missing session queue"))
    }
}

fn tcp_established_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpEstablishedNode::runtime_nexts(runtime)?;
    tcp_established_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

fn tcp_established_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpEstablishedNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp established node missing session queue"))?;
    let output = next[TcpEstablishedNext::Output as usize];
    let drop_next = next[TcpEstablishedNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_established_index(runtime, index, session_queue, output, &mut next_frames) {
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

fn tcp_established_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    let mut output_index = None;
    let mut remove_session = None;
    let mut deliver_payload = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
        let session_id = queue
            .session_id_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
        if packet.flags.contains(TcpSegmentFlags::RST) {
            remove_session = Some(session_id);
            return Ok(());
        }
        let connection = queue
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("tcp established state is missing"))?;
        if let Some(acknowledgment) = packet.acknowledgment
            && packet.flags.contains(TcpSegmentFlags::ACK)
        {
            connection.apply_ack(acknowledgment, packet.advertised_window);
        }
        let mut ack = false;
        if packet.has_payload() {
            ack = true;
            if connection.accept_in_order_payload(packet.sequence, packet.payload_len) {
                deliver_payload = Some((session_id, packet.payload_offset, packet.payload_len));
            }
        }
        if packet.flags.contains(TcpSegmentFlags::FIN) {
            ack = true;
            let fin_sequence = packet.sequence.wrapping_add(packet.payload_len as u32);
            if !connection.accept_in_order_payload(fin_sequence, 1) {
                return Err(CoreError::internal(
                    "tcp established FIN sequence is unacceptable",
                ));
            }
            connection.set_state(TcpState::CloseWait);
        }
        if ack {
            let (allocated, _, _) = alloc_tcp_segment_for_connection(
                runtime.packet_buffers(),
                connection,
                packet.local,
                TcpSegmentFlags::ACK,
                0,
            )?;
            output_index = Some(allocated);
        }
        let indexed = connection.clone();
        queue.index_session(session_id, &indexed);
        Ok(())
    });
    if let Err(error) = result {
        if let Some(output_index) = output_index.take() {
            runtime.free_index(output_index);
        }
        return Err(error);
    }
    let mut input_consumed = false;
    if let Some((session_id, payload_offset, payload_len)) = deliver_payload {
        if let Err(error) = runtime
            .advance(index, payload_offset)
            .and_then(|()| runtime.truncate_chain(index, payload_len))
        {
            if let Some(output_index) = output_index.take() {
                runtime.free_index(output_index);
            }
            return Err(error);
        }
        let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
            queue.enqueue_rx(session_id, index, false)?;
            Ok(())
        });
        input_consumed = true;
        if let Err(error) = result {
            if let Some(output_index) = output_index.take() {
                runtime.free_index(output_index);
            }
            return Err(error);
        }
    }
    if let Some(session_id) = remove_session {
        let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
            queue.close_session(session_id)?;
            Ok(())
        });
        if let Err(error) = result {
            if let Some(output_index) = output_index.take() {
                runtime.free_index(output_index);
            }
            return Err(error);
        }
    }
    if let Some(output_index) = output_index.take()
        && let Err(error) = next_frames.enqueue(runtime, output, output_index)
    {
        runtime.free_index(output_index);
        return Err(error);
    }
    if !input_consumed {
        runtime.free_index(index);
    }
    Ok(())
}
