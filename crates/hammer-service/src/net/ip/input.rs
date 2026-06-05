use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, Node, NodeId,
    NodeNextEnqueue, NodeNextStorage, NodeResult, PacketTrace, SocksAddr, TraceFormatter,
    add_packet_trace, unlikely,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::icmp::IcmpErrorMetadata;

use crate::data_plane::{FeatureArc, FeatureArcSpec, set_buffer_node_error_code};
use crate::net::ip::{
    IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket, network_for_protocol,
    parse_ip_packet_with_chain_len,
};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_input_error, put_option_ip_input_target,
    put_option_ip_protocol, put_option_ip_version, put_usize,
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

#[hammer_component_macros::node(role = internal, next = IpInputNext, start_arc = A)]
pub struct IpInputNode<A: FeatureArcSpec = IpUnicastArc> {
    #[node(default)]
    cached_next: Option<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub input_target: Option<IpInputTarget>,
    pub input_error: Option<IpInputError>,
    pub packet_len: usize,
    pub next: NodeId,
}

impl IpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            input_target: cursor.read_option_ip_input_target()?,
            input_error: cursor.read_option_ip_input_error()?,
            packet_len: cursor.read_usize()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpInputTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_ip_input_target(out, self.input_target);
        put_option_ip_input_error(out, self.input_error);
        put_usize(out, self.packet_len);
        put_node(out, self.next);
    }
}

fn format_ip_input_trace(bytes: &[u8]) -> String {
    match IpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IpInputTrace invalid={bytes:?}"),
    }
}

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
        let next = Self::runtime_nexts(runtime)?;
        let feature_arc = self.feature_arc.as_ref();
        let indices = frame.pending_indices();
        let width = frame_batch_width(runtime);
        let mut last_next = None;
        let cached_next = self.cached_next;
        let mut traces = std::vec::Vec::new();
        let speculative = if let Some(cached_next) = cached_next {
            cached_next
        } else {
            let mut batch = runtime.buffer_batch_mut();
            prefetch_range_with_batch(&mut batch, indices, 0, width);
            let first_next = next_node_for_index_with_batch(
                runtime,
                &mut batch,
                first,
                next,
                feature_arc,
                &mut traces,
            )?;
            last_next = Some(first_next);
            first_next
        };
        let mut first_chunk = true;
        let result = NodeNextEnqueue::new(speculative).validate_frame_with_buffer_batch_chunks(
            runtime,
            frame,
            |batch, indices| {
                prefetch_indices_with_batch(batch, indices);
            },
            |batch, indices, nexts| {
                let start_offset = if first_chunk {
                    first_chunk = false;
                    if cached_next.is_none() {
                        nexts[0] = speculative;
                        1
                    } else {
                        0
                    }
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
                    &mut last_next,
                    &mut traces,
                )
            },
        )?;
        for (index, trace) in traces {
            add_packet_trace!(runtime, index, trace)?;
        }
        if let Some(node) = last_next {
            self.cached_next = Some(node);
        }
        Ok(result)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_input_trace)
    }
}

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
    traces: &mut std::vec::Vec<(BufferIndex, IpInputTrace)>,
) -> CoreResult<NodeId>
where
    A: FeatureArcSpec,
{
    let (trace, resolved) = {
        let buffer = batch.buffer_mut(index)?;
        let traced = buffer.trace_mark().is_some();
        match parse_ip_packet_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) {
            Err(_) => {
                set_buffer_node_error_code(runtime, buffer, IpInputError::BadLength.code())?;
                let resolved = NodeNextStorage::next(&next, IpInputNext::Drop);
                (
                    if unlikely(traced) {
                        Some(IpInputTrace {
                            version: None,
                            protocol: None,
                            input_target: None,
                            input_error: Some(IpInputError::BadLength),
                            packet_len: 0,
                            next: resolved,
                        })
                    } else {
                        None
                    },
                    resolved,
                )
            }
            Ok(parsed) => {
                if parsed.input_error == IpInputError::None {
                    buffer.clear_node_error();
                } else {
                    set_buffer_node_error_code(runtime, buffer, parsed.input_error.code())?;
                }
                let network = network_for_protocol(parsed.protocol);
                let cursor = if network.is_some() {
                    BufferPacketCursor::new()
                        .with_packet_len(parsed.packet_len)
                        .with_network_header(
                            parsed.network_header_offset,
                            parsed.network_header_len,
                        )
                        .with_transport_header(
                            parsed.transport_header_offset,
                            parsed.transport_header_len,
                        )
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
                metadata.icmp_error = icmp_error_metadata_for_input(&parsed);
                let resolved = match parsed.input_target {
                    IpInputTarget::Drop => NodeNextStorage::next(&next, IpInputNext::Drop),
                    IpInputTarget::Punt => NodeNextStorage::next(&next, IpInputNext::Punt),
                    IpInputTarget::Options => NodeNextStorage::next(&next, IpInputNext::Options),
                    IpInputTarget::Lookup => feature_arc.map_or(
                        NodeNextStorage::next(&next, IpInputNext::Lookup),
                        |arc| {
                            arc.start_or(
                                metadata,
                                NodeNextStorage::next(&next, IpInputNext::Lookup),
                            )
                        },
                    ),
                    IpInputTarget::LookupMulticast => {
                        NodeNextStorage::next(&next, IpInputNext::LookupMulticast)
                    }
                    IpInputTarget::IcmpError => {
                        NodeNextStorage::next(&next, IpInputNext::IcmpError)
                    }
                    IpInputTarget::Reassembly => {
                        NodeNextStorage::next(&next, IpInputNext::Reassembly)
                    }
                };
                (
                    if unlikely(traced) {
                        Some(IpInputTrace {
                            version: Some(parsed.version),
                            protocol: Some(parsed.protocol),
                            input_target: Some(parsed.input_target),
                            input_error: Some(parsed.input_error),
                            packet_len: parsed.packet_len,
                            next: resolved,
                        })
                    } else {
                        None
                    },
                    resolved,
                )
            }
        }
    };
    if let Some(trace) = trace {
        traces.push((index, trace));
    }
    Ok(resolved)
}

#[inline(always)]
fn icmp_error_metadata_for_input(parsed: &ParsedIpPacket) -> Option<IcmpErrorMetadata> {
    if parsed.input_target != IpInputTarget::IcmpError {
        return None;
    }
    match (parsed.version, parsed.input_error) {
        (IpVersion::V4, IpInputError::TimeExpired) => Some(IcmpErrorMetadata::ipv4_time_exceeded()),
        (IpVersion::V6, IpInputError::TimeExpired) => Some(IcmpErrorMetadata::ipv6_time_exceeded()),
        _ => None,
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
    last_next: &mut Option<NodeId>,
    traces: &mut std::vec::Vec<(BufferIndex, IpInputTrace)>,
) -> CoreResult<()>
where
    A: FeatureArcSpec,
{
    for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
        let node =
            next_node_for_index_with_batch(runtime, batch, index, next, feature_arc, traces)?;
        nexts[offset] = node;
        *last_next = Some(node);
    }
    Ok(())
}
