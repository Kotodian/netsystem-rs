use hammer_adapter::{
    BufferFrame, BufferIndex, BufferNodeError, BufferRefMut, DataPlaneRuntime, InternalNode, Node,
    NodeId, NodeNextFrames, NodeResult, SocksAddr, for_each_buffer_frame_index,
};
use hammer_core::error::CoreResult;

use crate::net::ip::{IpInputError, IpInputTarget, parse_ip_packet_with_chain_len};

#[hammer_component_macros::node_next]
pub enum IpInputNext {
    Drop,
    Punt,
    Options,
    Lookup,
    LookupMulticast,
    IcmpError,
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
        let mut next_frames = NodeNextFrames::default();
        for_each_buffer_frame_index!(runtime, frame, |index| {
            let node = next_node_for_index(runtime, index, self.next)?;
            next_frames.enqueue(runtime, node, index)
        })?;
        frame.clear();
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for IpInputNode {}

#[inline(always)]
fn next_node_for_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    next: [NodeId; IpInputNext::COUNT],
) -> CoreResult<NodeId> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let parsed = match parse_ip_packet_with_chain_len(
        buffer.current(),
        buffer.total_len_not_including_first(),
    ) {
        Ok(parsed) => parsed,
        Err(_) => {
            set_node_error(&mut buffer, IpInputError::BadLength);
            return Ok(next[IpInputNext::Drop.slot()]);
        }
    };
    buffer.set_packet_cursor(parsed.cursor);
    if parsed.input_error == IpInputError::None {
        buffer.clear_node_error();
    } else {
        set_node_error(&mut buffer, parsed.input_error);
    }
    let metadata = buffer.metadata_mut();
    metadata.source = Some(SocksAddr::ip(parsed.source, 0));
    metadata.destination = Some(SocksAddr::ip(parsed.destination, 0));
    match parsed.input_target {
        IpInputTarget::Drop => Ok(next[IpInputNext::Drop.slot()]),
        IpInputTarget::Punt => Ok(next[IpInputNext::Punt.slot()]),
        IpInputTarget::Options => Ok(next[IpInputNext::Options.slot()]),
        IpInputTarget::Lookup => Ok(next[IpInputNext::Lookup.slot()]),
        IpInputTarget::LookupMulticast => Ok(next[IpInputNext::LookupMulticast.slot()]),
        IpInputTarget::IcmpError => Ok(next[IpInputNext::IcmpError.slot()]),
        IpInputTarget::Reassembly => Ok(next[IpInputNext::Reassembly.slot()]),
    }
}

#[inline(always)]
fn set_node_error(buffer: &mut BufferRefMut<'_>, error: IpInputError) {
    buffer.set_node_error(BufferNodeError::new(error.code()));
}
