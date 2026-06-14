use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpSegmentFlags, TcpState};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::connection::TcpConnectionState;
use super::output::{TCP_FLAG_ACK, TCP_FLAG_SYN, tcp_output_packet_flags};
use super::segment::parse_tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpListenNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpListenNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_listen_process_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp listen node missing session queue"))
    }
}

fn tcp_listen_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpListenNode::runtime_nexts(runtime)?;
    tcp_listen_process_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

#[inline]
fn tcp_listen_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpListenNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp listen node missing session queue"))?;
    let output = next[TcpListenNext::Output as usize];
    let drop_next = next[TcpListenNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_listen_index(runtime, index, session_queue, output, &mut next_frames) {
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

fn tcp_listen_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    if !packet.flags.contains(TcpSegmentFlags::SYN)
        || packet
            .flags
            .intersects(TcpSegmentFlags::ACK | TcpSegmentFlags::RST)
    {
        return Err(CoreError::internal("tcp listen received non-SYN packet"));
    }

    let mut output_index = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
        let mut connection = TcpConnectionState::new(
            None,
            queue.worker(),
            TcpState::SynRcvd,
            packet.local.port(),
            Some(packet.local),
            packet.remote,
        );
        connection.apply_peer_handshake_capabilities(packet.capabilities);
        connection.initialize_passive_open(1, packet.sequence, packet.advertised_window);
        let session_id = queue.insert_session(connection);
        let connection_id = TcpConnectionId::new(session_id.get());
        let connection = queue
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("inserted tcp session is missing"))?;
        connection.set_connection_id(connection_id);
        let record =
            tcp_output_packet_flags(connection, packet.local, &[], TCP_FLAG_SYN | TCP_FLAG_ACK)?;
        connection.retransmit_queue_mut().track_output(&record);
        connection.set_send_state(
            connection.snd_una(),
            record.next_send_sequence(),
            connection.snd_wnd(),
        );
        let allocated = record.alloc_finalized_header_buffer(runtime)?;
        output_index = Some(allocated);
        let indexed = connection.clone();
        queue.index_session(session_id, &indexed);
        queue.arm_retransmit_timer(session_id, 1)?;
        queue.mark_session_ready(session_id);
        Ok(())
    });
    if let Err(error) = result {
        if let Some(output_index) = output_index.take() {
            runtime.free_index(output_index);
        }
        return Err(error);
    }

    if let Some(output_index) = output_index.take() {
        if let Err(error) = next_frames.enqueue(runtime, output, output_index) {
            runtime.free_index(output_index);
            return Err(error);
        }
    }
    runtime.free_index(index);
    Ok(())
}
