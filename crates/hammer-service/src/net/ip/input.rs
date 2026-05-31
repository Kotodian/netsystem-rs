use hammer_adapter::{
    BufferFrame, BufferFramePairBatch, BufferFrameQuadBatch, BufferIndex, DataPlaneRuntime,
    FrameBatchWidth, InternalNode, Node, NodeId, NodeNextFrames, NodeNextTable, NodeResult,
    SocksAddr, define_node_next,
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
    match runtime.preferred_frame_batch_width() {
        FrameBatchWidth::Quad => {
            dispatch_ip_input_frame_quad(runtime, frame, next, &mut next_frames)?
        }
        FrameBatchWidth::Pair => {
            dispatch_ip_input_frame_pair(runtime, frame, next, &mut next_frames)?
        }
    }
    frame.clear();
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

#[inline]
fn dispatch_ip_input_frame_quad<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &BufferFrame,
    next: NodeNextTable<IpInputNext>,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let mut cursor = frame.quad_batch_cursor();
    cursor.prefetch_next_quad(runtime);
    while let Some(batch) = cursor.next() {
        cursor.prefetch_next_quad(runtime);
        match batch {
            BufferFrameQuadBatch::Quad(indices) => {
                enqueue_index(runtime, next_frames, indices[0], next)?;
                enqueue_index(runtime, next_frames, indices[1], next)?;
                enqueue_index(runtime, next_frames, indices[2], next)?;
                enqueue_index(runtime, next_frames, indices[3], next)?;
            }
            BufferFrameQuadBatch::Pair(indices) => {
                enqueue_index(runtime, next_frames, indices[0], next)?;
                enqueue_index(runtime, next_frames, indices[1], next)?;
            }
            BufferFrameQuadBatch::Single(index) => {
                enqueue_index(runtime, next_frames, index, next)?;
            }
        }
    }
    Ok(())
}

#[inline]
fn dispatch_ip_input_frame_pair<G>(
    runtime: &DataPlaneRuntime<G>,
    frame: &BufferFrame,
    next: NodeNextTable<IpInputNext>,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let mut cursor = frame.pair_batch_cursor();
    cursor.prefetch_next_pair(runtime);
    while let Some(batch) = cursor.next() {
        cursor.prefetch_next_pair(runtime);
        match batch {
            BufferFramePairBatch::Pair(indices) => {
                enqueue_index(runtime, next_frames, indices[0], next)?;
                enqueue_index(runtime, next_frames, indices[1], next)?;
            }
            BufferFramePairBatch::Single(index) => {
                enqueue_index(runtime, next_frames, index, next)?;
            }
        }
    }
    Ok(())
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
