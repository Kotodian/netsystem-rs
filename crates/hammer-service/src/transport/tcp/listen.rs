use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpConnectionId, TcpSegmentFlags, TcpSegmentHeader, TcpSeq, TcpState,
};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::connection::TcpConnectionState;
use super::output::{
    tcp_output_acknowledgment, tcp_output_next_sequence, tcp_output_sequence,
    tcp_output_sequence_len,
};
use super::segment::{alloc_tcp_segment, parse_tcp_packet, tcp_segment_metadata};

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
        let iss = 1;
        connection.set_sequence_state(
            iss,
            packet.sequence,
            iss,
            TcpSeq::new(iss).advance(1).raw(),
            connection.effective_send_window(u32::from(packet.advertised_window)),
            TcpSeq::new(packet.sequence).advance(1).raw(),
            connection.rcv_wnd(),
        );
        connection.set_state(TcpState::SynRcvd);
        let session_id = queue.insert_session(connection);
        let connection_id = TcpConnectionId::new(session_id.get());
        let connection = queue
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("inserted tcp session is missing"))?;
        connection.set_connection_id(connection_id);
        let flags = TcpSegmentFlags::SYN | TcpSegmentFlags::ACK;
        let flags_bits = flags.bits();
        let sequence = tcp_output_sequence(connection, flags_bits);
        let sequence_len = tcp_output_sequence_len(flags_bits, 0);
        let next_sequence = tcp_output_next_sequence(sequence, sequence_len);
        let allocated = alloc_tcp_segment(
            runtime.packet_buffers(),
            tcp_segment_metadata(packet.local, connection.remote()),
            TcpSegmentHeader {
                source_port: packet.local.port(),
                destination_port: connection.remote().port(),
                sequence_number: sequence,
                acknowledgment_number: tcp_output_acknowledgment(connection, flags_bits),
                flags,
                advertised_window: connection.advertised_receive_window(connection.rcv_wnd()),
                capabilities: connection.local_capabilities(),
            },
        )?;
        connection.set_send_state(connection.snd_una(), next_sequence, connection.snd_wnd());
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
