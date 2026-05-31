use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeNextTable, NodeResult, SocksAddr, define_node_next, for_each_buffer_frame_index,
};
use hammer_core::error::CoreResult;

use crate::net::ip::{IpInputTarget, parse_ip_packet};

define_node_next! {
    pub enum IpInputNext {
        Lookup,
        Reassembly,
    }
}

pub struct IpInputNode {
    next: NodeNextTable<IpInputNext>,
}

impl IpInputNode {
    #[inline]
    pub fn new(lookup_next: NodeId, reassembly_next: NodeId) -> Self {
        Self {
            next: NodeNextTable::new(lookup_next).with(IpInputNext::Reassembly, reassembly_next),
        }
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
    next: NodeNextTable<IpInputNext>,
) -> CoreResult<NodeResult> {
    let mut next_frames = NodeNextFrames::default();
    for_each_buffer_frame_index!(runtime, frame, |index| {
        enqueue_index(runtime, &mut next_frames, index, next)
    })?;
    frame.clear();
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

#[inline(always)]
fn enqueue_index<G>(
    runtime: &DataPlaneRuntime<G>,
    next_frames: &mut NodeNextFrames,
    index: BufferIndex,
    next: NodeNextTable<IpInputNext>,
) -> CoreResult<()> {
    let Some(node) = next_node_for_index(runtime, index, next)? else {
        runtime.free_index(index);
        return Ok(());
    };
    next_frames.enqueue(runtime, node, index)?;
    Ok(())
}

#[inline(always)]
fn next_node_for_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    next_table: NodeNextTable<IpInputNext>,
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
    let next = match parsed.input_target {
        IpInputTarget::Lookup => IpInputNext::Lookup,
        IpInputTarget::Options => return Ok(None),
        IpInputTarget::Reassembly => IpInputNext::Reassembly,
    };
    Ok(Some(next_table.node(next)))
}
