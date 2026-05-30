use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeNextTable, NodeResult, SocksAddr, define_node_next,
};
use hammer_core::error::CoreResult;

use crate::net::ip::{IpInputTarget, parse_ip_packet};

define_node_next! {
    pub enum IpInputNext {
        Lookup,
        Options,
        Reassembly,
    }
}

pub struct IpInputNode {
    next: NodeNextTable<IpInputNext>,
}

impl IpInputNode {
    #[inline]
    pub fn new(lookup_next: NodeId) -> Self {
        Self {
            next: NodeNextTable::new(lookup_next),
        }
    }

    #[inline]
    pub fn with_next(mut self, next: IpInputNext, node: NodeId) -> Self {
        self.next = self.next.with(next, node);
        self
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
    let indices = frame.pending_indices().to_vec();
    frame.clear();
    let mut cursor = indices.as_slice().chunks_exact(2);
    for batch in cursor.by_ref() {
        for index in batch.iter().copied() {
            enqueue_index(runtime, &mut next_frames, index, next)?;
        }
    }
    for index in cursor.remainder().iter().copied() {
        enqueue_index(runtime, &mut next_frames, index, next)?;
    }
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

#[inline]
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

#[inline]
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
        IpInputTarget::Options => IpInputNext::Options,
        IpInputTarget::Reassembly => IpInputNext::Reassembly,
    };
    Ok(Some(next_table.node(next)))
}
