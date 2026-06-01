use hammer_adapter::{
    Buffer, BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime,
    InternalNode, Network, Node, NodeId, NodeNextEnqueue, NodeResult, SocksAddr,
};
use hammer_core::error::CoreResult;

use crate::net::ip::{IpInputError, IpInputTarget, IpProtocol, parse_ip_packet_with_chain_len};

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
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let speculative = {
            let mut batch = runtime.buffer_batch_mut();
            next_node_for_index_with_batch(runtime, &mut batch, first, self.next)?
        };
        NodeNextEnqueue::new(speculative).validate_frame_with_first_next_and_buffer_batch_prefetch(
            runtime,
            frame,
            first,
            speculative,
            |batch, index| batch.prefetch_read(index),
            |batch, index| next_node_for_index_with_batch(runtime, batch, index, self.next),
        )
    }
}

impl<G> InternalNode<G> for IpInputNode {}

#[inline(always)]
fn next_node_for_index_with_batch<G>(
    runtime: &DataPlaneRuntime<G>,
    batch: &mut BufferBatchMut<'_>,
    index: BufferIndex,
    next: [NodeId; IpInputNext::COUNT],
) -> CoreResult<NodeId> {
    batch.with_buffer_mut(index, |buffer| {
        let parsed = match parse_ip_packet_with_chain_len(
            buffer.current(),
            buffer.total_len_not_including_first(),
        ) {
            Ok(parsed) => parsed,
            Err(_) => {
                set_node_error(runtime, buffer, IpInputError::BadLength)?;
                return Ok(next[IpInputNext::Drop.slot()]);
            }
        };
        if parsed.input_error == IpInputError::None {
            buffer.clear_node_error();
        } else {
            set_node_error(runtime, buffer, parsed.input_error)?;
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
            IpInputTarget::Lookup => Ok(next[IpInputNext::Lookup.slot()]),
            IpInputTarget::LookupMulticast => Ok(next[IpInputNext::LookupMulticast.slot()]),
            IpInputTarget::IcmpError => Ok(next[IpInputNext::IcmpError.slot()]),
            IpInputTarget::Reassembly => Ok(next[IpInputNext::Reassembly.slot()]),
        }
    })?
}

#[inline(always)]
fn network_for_protocol(protocol: IpProtocol) -> Option<Network> {
    match protocol {
        IpProtocol::Tcp => Some(Network::Tcp),
        IpProtocol::Udp => Some(Network::Udp),
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => Some(Network::Icmp),
        IpProtocol::Other(_) => None,
    }
}

#[inline(always)]
fn set_node_error<G>(
    runtime: &DataPlaneRuntime<G>,
    buffer: &mut Buffer,
    error: IpInputError,
) -> CoreResult<()> {
    let error = runtime.record_current_node_error(error.code())?;
    buffer.set_node_error(error);
    Ok(())
}
