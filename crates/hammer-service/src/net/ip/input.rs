use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeResult, SocksAddr, for_each_buffer_frame_index,
};
use hammer_core::error::CoreResult;

use crate::net::ip::{IpInputTarget, parse_ip_packet};

#[hammer_component_macros::node_next]
pub enum IpInputNext {
    Lookup,
    Reassembly,
}

pub struct IpInputNode {
    next: [NodeId; IpInputNext::COUNT],
}

impl IpInputNode {
    #[inline]
    pub fn new(next: [NodeId; IpInputNext::COUNT]) -> Self {
        Self { next }
    }
}

impl<G> Node<G> for IpInputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        dispatch_ip_input_frame(runtime, frame, self.next)
    }
}

impl<G> InternalNode<G> for IpInputNode {}

#[inline]
fn dispatch_ip_input_frame<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &mut BufferFrame,
    next: [NodeId; IpInputNext::COUNT],
) -> CoreResult<NodeResult> {
    let mut next_frames = NodeNextFrames::default();
    for_each_buffer_frame_index!(runtime, frame, |index| {
        let node = next_node_for_index(runtime, index, next)?;
        next_frames.enqueue_optional(runtime, index, node)
    })?;
    frame.clear();
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

#[inline(always)]
fn next_node_for_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    next: [NodeId; IpInputNext::COUNT],
) -> CoreResult<Option<NodeId>> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let parsed = match parse_ip_packet(buffer.current()) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };
    buffer.set_packet_cursor(parsed.cursor);
    let metadata = buffer.metadata_mut();
    metadata.source = Some(SocksAddr::ip(parsed.source, 0));
    metadata.destination = Some(SocksAddr::ip(parsed.destination, 0));
    match parsed.input_target {
        IpInputTarget::Lookup => Ok(Some(next[IpInputNext::Lookup.slot()])),
        IpInputTarget::Options => return Ok(None),
        IpInputTarget::Reassembly => Ok(Some(next[IpInputNext::Reassembly.slot()])),
    }
}
