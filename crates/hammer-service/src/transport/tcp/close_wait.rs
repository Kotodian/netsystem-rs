use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::connection::TcpConnection;
use super::segment::{alloc_tcp_segment, parse_tcp_packet, tcp_segment_metadata};
use super::session::TcpSessionQueue;
use super::state_machine::CloseWait;

#[hammer_component_macros::node_next]
pub enum TcpCloseWaitNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpCloseWaitNext)]
pub struct TcpCloseWaitNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpCloseWaitNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpCloseWaitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_close_wait_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_close_wait_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp close-wait node missing session queue"))
    }
}

fn tcp_close_wait_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpCloseWaitNode::runtime_nexts(runtime)?;
    tcp_close_wait_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

fn tcp_close_wait_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpCloseWaitNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp close-wait node missing session queue"))?;
    let output = next[TcpCloseWaitNext::Output as usize];
    let drop_next = next[TcpCloseWaitNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_close_wait_index(runtime, index, session_queue, output, &mut next_frames) {
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

fn tcp_close_wait_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    let mut output_index = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
        let (session_id, _, _) = queue
            .session_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp close-wait session is missing"))?;
        let connection: TcpConnection<CloseWait> = queue.take_connection(session_id)?;
        let control = connection.receive_close_wait(queue, session_id, &packet)?;
        if let Some(header) = control {
            let allocated = alloc_tcp_segment(
                runtime.packet_buffers(),
                tcp_segment_metadata(packet.local, packet.remote),
                header,
            )?;
            output_index = Some(allocated);
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
