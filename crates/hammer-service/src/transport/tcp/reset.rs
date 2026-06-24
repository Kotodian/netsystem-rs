use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, Node, NodeId,
    NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    tcp_reset_network_header_len, tcp_reset_reply_from_current_packet, TcpResetPacketCursor,
};

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
    Lookup,
}

#[derive(Clone, Copy)]
#[hammer_component_macros::node(role = internal, next = TcpResetNext)]
pub struct TcpResetNode {
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for TcpResetNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let (result, cached_next) = tcp_reset_process_frame(
            runtime,
            frame,
            next[TcpResetNext::Drop as usize],
            next[TcpResetNext::Lookup as usize],
            self.cached_next,
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_reset_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn tcp_reset_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpResetNode::runtime_nexts(runtime)?;
    let (result, _) = tcp_reset_process_frame(
        runtime,
        frame,
        next[TcpResetNext::Drop as usize],
        next[TcpResetNext::Lookup as usize],
        None,
    )?;
    Ok(result)
}

fn tcp_reset_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    drop_next: NodeId,
    lookup_next: NodeId,
    cached_next: Option<NodeId>,
) -> CoreResult<(NodeResult, Option<NodeId>)> {
    NodeVectorDispatch::new(cached_next).route_frame(
        runtime,
        frame,
        prefetch_tcp_reset,
        |_, indices, nexts| {
            for (offset, index) in indices.iter().copied().enumerate() {
                nexts[offset] = Some(tcp_reset_next_for_index(
                    runtime,
                    index,
                    drop_next,
                    lookup_next,
                )?);
            }
            Ok(())
        },
    )
}

#[inline(always)]
fn prefetch_tcp_reset(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices.iter().copied() {
        batch.prefetch_read(index);
    }
}

fn tcp_reset_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    lookup_next: NodeId,
) -> CoreResult<NodeId> {
    let packet = runtime.copy_current_chain(index)?;
    let cursor = tcp_reset_packet_cursor(runtime.get_buffer(index)?.packet_cursor());
    let Some(reply) = tcp_reset_reply_from_current_packet(&packet, cursor) else {
        return Ok(drop_next);
    };
    replace_current_chain(runtime, index, &reply.packet)?;
    refresh_reset_metadata(runtime, index, &reply.packet)?;
    Ok(lookup_next)
}

#[inline]
fn tcp_reset_packet_cursor(cursor: BufferPacketCursor) -> TcpResetPacketCursor {
    TcpResetPacketCursor {
        packet_len: cursor.packet_len(),
        network_header_offset: cursor.network_header_offset(),
        network_header_len: cursor.network_header_len(),
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
        transport_payload_offset: cursor.transport_payload_offset(),
    }
}

#[inline(always)]
fn replace_current_chain(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.truncate_chain(0)?;
    buffer.append(packet)
}

fn refresh_reset_metadata(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    const TCP_HEADER_LEN: usize = 20;

    let network_header_len = tcp_reset_network_header_len(packet)
        .ok_or_else(|| CoreError::internal("tcp reset reply uses unsupported IP version"))?;

    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    buffer.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet.len())
            .with_network_header(0, network_header_len)
            .with_transport_header(network_header_len, TCP_HEADER_LEN)
            .with_transport_payload_offset(network_header_len + TCP_HEADER_LEN),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

    use hammer_adapter::{BufferNodeError, InternalNode, NodeRegistration};

    use super::*;

    #[derive(Clone, Copy)]
    struct BlackholeNode;

    impl Node for BlackholeNode {
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            frame.drain_pending();
            Ok(NodeResult::drop())
        }

        fn node_process(&self) -> NodeProcessFn {
            blackhole_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(NodeRuntimeData::default())
        }
    }

    impl InternalNode for BlackholeNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::next("tcp-reset-test-blackhole", 0)
        }
    }

    fn blackhole_process(
        _: &DataPlaneRuntime,
        _: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.drain_pending();
        Ok(NodeResult::drop())
    }

    #[test]
    fn tcp_reset_node_rewrites_buffer_and_routes_to_lookup() {
        let runtime = DataPlaneRuntime::with_capacities(512, 8, 4, 4);
        let drop = runtime.nodes().register_internal(BlackholeNode);
        let lookup = runtime.nodes().register_internal(BlackholeNode);
        let reset = runtime
            .nodes()
            .register_internal(TcpResetNode::new(TcpResetNext::nodes(drop, lookup)));
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let index = runtime
            .alloc_index_with_bytes(&ipv4_tcp_packet(0x10, 1_000, 9_000, &[]))
            .expect("alloc packet");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
            buffer.set_packet_cursor(buffer_packet_cursor(40));
            let error = runtime
                .record_current_node_error(7)
                .expect("record current node error");
            buffer.set_node_error(BufferNodeError::new(NodeId::new(0), error));
        }
        runtime
            .get_frame_mut(frame)
            .expect("frame")
            .push_index(index)
            .expect("push index");

        assert!(runtime
            .schedule_frame(reset, frame)
            .expect("schedule reset"));
        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

        let packet = runtime.copy_current_chain(index).expect("rewritten packet");
        let cursor = runtime.get_buffer(index).expect("buffer").packet_cursor();
        let reply_tcp = etherparse::TcpSlice::from_slice(&packet[20..]).expect("parse reply");
        assert_eq!(cursor.packet_len(), packet.len());
        assert_eq!(cursor.network_header_offset(), 0);
        assert_eq!(cursor.transport_header_offset(), 20);
        assert_eq!(cursor.transport_payload_offset(), 40);
        assert!(runtime
            .get_buffer(index)
            .expect("buffer")
            .node_error()
            .is_none());
        assert!(reply_tcp.rst());
        assert_eq!(reply_tcp.sequence_number(), 9_000);
    }

    fn buffer_packet_cursor(packet_len: usize) -> BufferPacketCursor {
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, 20)
            .with_transport_header(20, 20)
            .with_transport_payload_offset(40)
    }

    fn ipv4_tcp_packet(flags: u8, sequence: u32, acknowledgment: u32, payload: &[u8]) -> Vec<u8> {
        let packet_len = 20 + 20 + payload.len();
        let total_len = u16::try_from(packet_len).expect("packet length fits");
        let mut packet = vec![0u8; packet_len];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&total_len.to_be_bytes());
        packet[8] = 64;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
        packet[16..20].copy_from_slice(&[198, 51, 100, 2]);
        packet[20..22].copy_from_slice(&50_000u16.to_be_bytes());
        packet[22..24].copy_from_slice(&80u16.to_be_bytes());
        packet[24..28].copy_from_slice(&sequence.to_be_bytes());
        packet[28..32].copy_from_slice(&acknowledgment.to_be_bytes());
        packet[32] = 0x50;
        packet[33] = flags;
        packet[34..36].copy_from_slice(&4096u16.to_be_bytes());
        if !payload.is_empty() {
            packet[40..40 + payload.len()].copy_from_slice(payload);
        }
        let tcp_checksum = ipv4_l4_checksum([192, 0, 2, 1], [198, 51, 100, 2], 6, &packet[20..]);
        packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
        let ip_checksum = internet_checksum(&packet[..20]);
        packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        packet
    }

    fn ipv4_l4_checksum(
        source: [u8; 4],
        destination: [u8; 4],
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        internet_checksum_parts(&[
            &source,
            &destination,
            &[0, protocol],
            &(segment.len() as u16).to_be_bytes(),
            segment,
        ])
    }
}
