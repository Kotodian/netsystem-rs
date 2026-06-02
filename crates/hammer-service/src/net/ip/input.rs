use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, InternalNode,
    Node, NodeId, NodeNextEnqueue, NodeResult, SocksAddr,
};
use hammer_core::error::CoreResult;

use crate::data_plane::{FeatureArc, FeatureArcSpec, set_buffer_node_error_code};
use crate::net::ip::{
    IpInputError, IpInputTarget, network_for_protocol, parse_ip_packet_with_chain_len,
};

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpUnicastArc {}

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

#[hammer_component_macros::node(next = IpInputNext, start_arc = A)]
pub struct IpInputNode<A: FeatureArcSpec = IpUnicastArc>;

impl<A, G> Node<G> for IpInputNode<A>
where
    A: FeatureArcSpec,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let next = self.next;
        let feature_arc = self.feature_arc.as_ref();
        let indices = frame.pending_indices();
        let width = frame_batch_width(runtime);
        let speculative = {
            let mut batch = runtime.buffer_batch_mut();
            prefetch_range_with_batch(&mut batch, indices, 0, width);
            next_node_for_index_with_batch(runtime, &mut batch, first, next, feature_arc)?
        };
        let mut first_chunk = true;
        NodeNextEnqueue::new(speculative).validate_frame_with_buffer_batch_chunks(
            runtime,
            frame,
            |batch, indices| {
                prefetch_indices_with_batch(batch, indices);
            },
            |batch, indices, nexts| {
                let start_offset = if first_chunk {
                    first_chunk = false;
                    nexts[0] = speculative;
                    1
                } else {
                    0
                };
                next_nodes_for_indices_with_batch(
                    runtime,
                    batch,
                    indices,
                    nexts,
                    start_offset,
                    next,
                    feature_arc,
                )
            },
        )
    }
}

impl<A, G> InternalNode<G> for IpInputNode<A> where A: FeatureArcSpec {}

#[inline(always)]
fn prefetch_index_with_batch(batch: &mut BufferBatchMut<'_>, index: BufferIndex) {
    batch.prefetch_read(index);
}

#[inline(always)]
fn prefetch_range_with_batch(
    batch: &mut BufferBatchMut<'_>,
    indices: &[BufferIndex],
    offset: usize,
    width: usize,
) {
    if offset >= indices.len() {
        return;
    }
    let end = (offset + width).min(indices.len());
    for index in indices[offset..end].iter().copied() {
        prefetch_index_with_batch(batch, index);
    }
}

#[inline(always)]
fn prefetch_indices_with_batch(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices.iter().copied() {
        prefetch_index_with_batch(batch, index);
    }
}

#[inline(always)]
fn frame_batch_width<G>(runtime: &DataPlaneRuntime<G>) -> usize {
    match runtime.preferred_frame_batch_width() {
        hammer_adapter::FrameBatchWidth::Quad => 4,
        hammer_adapter::FrameBatchWidth::Pair => 2,
    }
}

#[inline(always)]
fn next_node_for_index_with_batch<A, G>(
    runtime: &DataPlaneRuntime<G>,
    batch: &mut BufferBatchMut<'_>,
    index: BufferIndex,
    next: [NodeId; IpInputNext::COUNT],
    feature_arc: Option<&FeatureArc<A>>,
) -> CoreResult<NodeId>
where
    A: FeatureArcSpec,
{
    let buffer = batch.buffer_mut(index)?;
    let parsed = match parse_ip_packet_with_chain_len(
        buffer.current(),
        buffer.total_len_not_including_first(),
    ) {
        Ok(parsed) => parsed,
        Err(_) => {
            set_buffer_node_error_code(runtime, buffer, IpInputError::BadLength.code())?;
            return Ok(next[IpInputNext::Drop.slot()]);
        }
    };
    if parsed.input_error == IpInputError::None {
        buffer.clear_node_error();
    } else {
        set_buffer_node_error_code(runtime, buffer, parsed.input_error.code())?;
    }
    let network = network_for_protocol(parsed.protocol);
    let cursor = if network.is_some() {
        BufferPacketCursor::new()
            .with_packet_len(parsed.packet_len)
            .with_network_header(parsed.network_header_offset, parsed.network_header_len)
            .with_transport_header(parsed.transport_header_offset, parsed.transport_header_len)
            .with_transport_payload_offset(
                parsed.transport_header_offset + parsed.transport_header_len,
            )
    } else {
        BufferPacketCursor::new()
    };
    *buffer.packet_cursor_mut() = cursor;
    let metadata = buffer.metadata_mut();
    if let Some(network) = network {
        metadata.network = network;
    }
    metadata.source = Some(SocksAddr::ip(parsed.source, 0));
    metadata.destination = Some(SocksAddr::ip(parsed.destination, 0));
    match parsed.input_target {
        IpInputTarget::Drop => Ok(next[IpInputNext::Drop.slot()]),
        IpInputTarget::Punt => Ok(next[IpInputNext::Punt.slot()]),
        IpInputTarget::Options => Ok(next[IpInputNext::Options.slot()]),
        IpInputTarget::Lookup => Ok(feature_arc.map_or(next[IpInputNext::Lookup.slot()], |arc| {
            arc.start_or(metadata, next[IpInputNext::Lookup.slot()])
        })),
        IpInputTarget::LookupMulticast => Ok(next[IpInputNext::LookupMulticast.slot()]),
        IpInputTarget::IcmpError => Ok(next[IpInputNext::IcmpError.slot()]),
        IpInputTarget::Reassembly => Ok(next[IpInputNext::Reassembly.slot()]),
    }
}

#[inline(always)]
fn next_nodes_for_indices_with_batch<A, G>(
    runtime: &DataPlaneRuntime<G>,
    batch: &mut BufferBatchMut<'_>,
    indices: &[BufferIndex],
    nexts: &mut [NodeId; 4],
    start_offset: usize,
    next: [NodeId; IpInputNext::COUNT],
    feature_arc: Option<&FeatureArc<A>>,
) -> CoreResult<()>
where
    A: FeatureArcSpec,
{
    for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
        nexts[offset] = next_node_for_index_with_batch(runtime, batch, index, next, feature_arc)?;
    }
    Ok(())
}
