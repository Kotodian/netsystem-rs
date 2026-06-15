use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeProcessFn,
    NodeRegistration, NodeResult, NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpSeq;

pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[hammer_component_macros::node_next]
pub enum TcpOutputNext {
    Drop,
    Lookup,
}

#[derive(Debug, Clone, Copy)]
pub struct TcpOutputNode {
    next: [NodeId; TcpOutputNext::COUNT],
    cached_next: Option<NodeId>,
}

impl TcpOutputNode {
    pub const NODE_NAME: &'static str = "tcp-output-node";

    #[inline]
    pub fn new(next: [NodeId; TcpOutputNext::COUNT]) -> Self {
        Self {
            next,
            cached_next: None,
        }
    }
}

impl Node for TcpOutputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let (result, cached_next) = tcp_output_node_process_frame(
            runtime,
            frame,
            self.next[TcpOutputNext::Lookup as usize],
            self.next[TcpOutputNext::Drop as usize],
            self.cached_next,
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_output_node_process
    }
}

impl InternalNode for TcpOutputNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, TcpOutputNext::COUNT)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

fn tcp_output_node_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let current = runtime
        .current_node()
        .ok_or_else(|| CoreError::internal("tcp output missing current node"))?;
    let next = [
        runtime
            .nodes()
            .node_next_slot(current, TcpOutputNext::Drop as usize)?,
        runtime
            .nodes()
            .node_next_slot(current, TcpOutputNext::Lookup as usize)?,
    ];
    let (result, _) = tcp_output_node_process_frame(
        runtime,
        frame,
        next[TcpOutputNext::Lookup as usize],
        next[TcpOutputNext::Drop as usize],
        None,
    )?;
    Ok(result)
}

fn tcp_output_node_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    lookup: NodeId,
    drop: NodeId,
    cached_next: Option<NodeId>,
) -> CoreResult<(NodeResult, Option<NodeId>)> {
    NodeVectorDispatch::new(cached_next).route_frame_index(runtime, frame, |index| {
        Ok(Some(tcp_output_next_for_index(
            runtime, index, lookup, drop,
        )?))
    })
}

fn tcp_output_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    lookup: NodeId,
    drop: NodeId,
) -> CoreResult<NodeId> {
    let buffer = runtime.get_buffer(index)?;
    let metadata = buffer.metadata();
    if metadata.network != Network::Tcp
        || metadata.source.is_none()
        || metadata.destination.is_none()
        || buffer.current_len() == 0
    {
        return Ok(drop);
    }
    Ok(lookup)
}

#[inline]
pub const fn tcp_effective_output_payload_len(peer_max_segment_size: Option<u16>) -> usize {
    match peer_max_segment_size {
        Some(max_segment_size) if max_segment_size != 0 => {
            let max_segment_size = max_segment_size as usize;
            if max_segment_size < DEFAULT_TCP_OUTPUT_PAYLOAD_LEN {
                max_segment_size
            } else {
                DEFAULT_TCP_OUTPUT_PAYLOAD_LEN
            }
        }
        _ => DEFAULT_TCP_OUTPUT_PAYLOAD_LEN,
    }
}

#[inline]
pub fn tcp_available_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
) -> u32 {
    snd_wnd
        .min(congestion_window)
        .saturating_sub(tcp_inflight_sequence_len(snd_una, snd_nxt))
}

#[inline]
pub fn tcp_payload_len_in_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
    requested_payload_len: usize,
    control_len: u32,
) -> usize {
    let available_payload_len =
        tcp_available_send_window(snd_una, snd_nxt, snd_wnd, congestion_window)
            .saturating_sub(control_len) as usize;
    available_payload_len.min(requested_payload_len)
}

#[inline]
pub const fn tcp_output_sequence_len(flags: u8, payload_len: usize) -> u32 {
    let control_len = ((flags & TCP_FLAG_SYN != 0) as u32) + ((flags & TCP_FLAG_FIN != 0) as u32);
    payload_len as u32 + control_len
}

#[inline]
pub fn tcp_output_next_sequence(sequence: u32, sequence_len: u32) -> u32 {
    TcpSeq::new(sequence).advance(sequence_len).raw()
}

#[inline]
fn tcp_inflight_sequence_len(snd_una: u32, snd_nxt: u32) -> u32 {
    if snd_una != 0 && snd_nxt != 0 {
        TcpSeq::new(snd_una).distance_to(TcpSeq::new(snd_nxt))
    } else {
        0
    }
}
