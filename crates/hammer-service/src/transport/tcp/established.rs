use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpState;

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::connection::TcpConnection;
use super::segment::{alloc_tcp_segment, parse_tcp_packet, tcp_segment_metadata};
use super::session::TcpSessionQueue;
use super::state_machine::Established;

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
    let result = TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
        let session_id = queue
            .session_id_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp established session is missing"))?;
        let connection: TcpConnection<Established> = queue.take_connection(session_id)?;
        let (next, control, accepted_payload_len, fin): (
            super::connection::TcpConnectionState,
            _,
            _,
            _,
        ) = connection.receive_data(&packet);
        let _next_node = next.next_node();
        if next.state() == TcpState::Closed {
            remove_session = Some(session_id);
            return Ok(());
        }
        if let Some(payload_len) = accepted_payload_len {
            deliver_payload = Some((session_id, packet.payload_offset, payload_len, fin));
        }
        if let Some(header) = control {
            let allocated = alloc_tcp_segment(
                runtime.packet_buffers(),
                tcp_segment_metadata(packet.local, packet.remote),
                header,
            )?;
            output_index = Some(allocated);
        }
        queue.put_connection(session_id, next);
        let indexed = queue
            .session_state(session_id)
            .ok_or_else(|| CoreError::internal("updated tcp established session is missing"))?
            .clone();
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
    if let Some((session_id, payload_offset, payload_len, fin)) = deliver_payload {
        if let Err(error) = runtime
            .advance(index, payload_offset)
            .and_then(|()| runtime.truncate_chain(index, payload_len))
        {
            if let Some(output_index) = output_index.take() {
                runtime.free_index(output_index);
            }
            return Err(error);
        }
        let result =
            TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
                queue.enqueue_rx(session_id, index, fin)?;
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
        let result =
            TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
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
