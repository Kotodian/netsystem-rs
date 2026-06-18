use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::connection::TcpConnection;
use super::segment::parse_tcp_packet;
use super::session::{TcpServiceController, TcpSessionQueue};
use super::state_machine::SynRcvd;

#[hammer_component_macros::node_next]
pub enum TcpSynRcvdNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpSynRcvdNext)]
pub struct TcpSynRcvdNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpSynRcvdNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpSynRcvdNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_syn_rcvd_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_syn_rcvd_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp syn-rcvd node missing session queue"))
    }
}

fn tcp_syn_rcvd_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpSynRcvdNode::runtime_nexts(runtime)?;
    tcp_syn_rcvd_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

fn tcp_syn_rcvd_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpSynRcvdNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp syn-rcvd node missing session queue"))?;
    let tcp_output = next[TcpSynRcvdNext::Output as usize];
    let drop_next = next[TcpSynRcvdNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_syn_rcvd_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
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

fn tcp_syn_rcvd_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    let mut tx_index = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
        let (session_id, _, _) = queue
            .session_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp syn-rcvd session is missing"))?;
        let connection: TcpConnection<SynRcvd, TcpServiceController> =
            queue.take_connection(session_id)?;
        let control = connection.receive_final_ack(queue, session_id, &packet)?;
        if let Some(segment) = control {
            let allocated = runtime.packet_buffers().alloc_index(Default::default())?;
            if let Err(error) = queue.protocol.insert_segment(allocated, segment) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    });
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
                queue.protocol.remove_segment(tx_index);
                Ok(())
            })?;
            runtime.free_index(tx_index);
        }
        return Err(error);
    }
    if let Some(tx_index) = tx_index.take()
        && let Err(error) = next_frames.enqueue(runtime, tcp_output, tx_index)
    {
        TcpSessionProtocol::with_queue(session_queue, |queue: &mut TcpSessionQueue| {
            queue.protocol.remove_segment(tx_index);
            Ok(())
        })?;
        runtime.free_index(tx_index);
        return Err(error);
    }
    runtime.free_index(index);
    Ok(())
}
