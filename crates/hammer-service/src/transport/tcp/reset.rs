use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::{TcpError, TcpSegmentFlags, tcp_header};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
#[cfg(test)]
use hammer_infra::vec::Vec;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
    #[next("ip-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::transport::tcp::reset::register_tcp_reset,
    next = TcpResetNext,
    role = internal,
)]
#[derive(Clone, Copy)]
pub struct TcpResetNode {
    #[node(default)]
    cached_next: Option<NodeId>,
}

pub fn register_tcp_reset(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        TcpResetNode::new([NodeId::new(0); TcpResetNext::COUNT]),
        &TcpResetNext::NEXT_NAMES,
    )
}

impl Node for TcpResetNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        tcp_reset_process_frame(runtime, frame)
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
) -> NodeResult {
    tcp_reset_process_frame(runtime, frame)
}

fn tcp_reset_process_frame(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        tcp_reset_next_for_index(runtime, index).unwrap_or(TcpResetNext::Drop)
    })
}

#[inline(always)]
fn tcp_reset_next_for_index(runtime: &DataPlaneRuntime, index: Index) -> CoreResult<TcpResetNext> {
    let reset = {
        let buffer = runtime.get_buffer(index)?;
        tcp_reset_prepare_from_current(
            buffer.current(),
            unsafe { std::mem::transmute::<_, &crate::net::NetworkOpaque>(buffer.opaque()) }
                .packet_cursor(),
        )
    };
    let Some(reply_len) = tcp_reset_write_reply(runtime, index, reset)? else {
        return Ok(TcpResetNext::Drop);
    };
    refresh_reset_metadata(runtime, index, reply_len)?;
    Ok(TcpResetNext::Lookup)
}

#[inline(always)]
fn tcp_reset_write_reply(
    runtime: &DataPlaneRuntime,
    index: Index,
    reset: Option<([u8; 16], [u8; 16], u16, u16, u32, u32, u8, u8)>,
) -> CoreResult<Option<usize>> {
    let Some((
        source,
        destination,
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        version,
    )) = reset
    else {
        return Ok(None);
    };
    let reply_len = {
        let mut buffer = runtime.get_buffer_mut(index)?;
        buffer.truncate(0)?;
        let writable = buffer.writable_tail_mut();
        let Some(reply_len) = tcp_reset_write_current_reply(
            writable,
            source,
            destination,
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            flags,
            version,
        ) else {
            return Ok(None);
        };
        buffer.commit_writable_tail(reply_len)?;
        reply_len
    };
    Ok(Some(reply_len))
}

#[inline(always)]
fn tcp_reset_prepare_from_current(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Option<([u8; 16], [u8; 16], u16, u16, u32, u32, u8, u8)> {
    let available_len = packet.len().min(cursor.packet_len());
    if cursor.transport_header_offset() > available_len
        || cursor.transport_payload_offset() > available_len
        || cursor.network_header_offset() > available_len
    {
        return None;
    }
    let version = packet.get(cursor.network_header_offset()).copied()? >> 4;
    let tcp_bytes = packet.get(cursor.transport_header_offset()..available_len)?;
    let tcp = tcp_header(tcp_bytes).ok()?;
    let tcp_header_len = tcp.header_len();
    if cursor.transport_payload_offset()
        != cursor
            .transport_header_offset()
            .checked_add(tcp_header_len)?
    {
        return None;
    }
    let flags = tcp.flags();
    if flags.contains(TcpSegmentFlags::RST) {
        return None;
    }
    let payload_len = available_len.checked_sub(cursor.transport_payload_offset())?;
    let sequence_len = u32::try_from(
        payload_len
            .checked_add(usize::from(flags.contains(TcpSegmentFlags::SYN)))?
            .checked_add(usize::from(flags.contains(TcpSegmentFlags::FIN)))?,
    )
    .ok()?;
    let (response_sequence, response_acknowledgment, response_flags) =
        if flags.contains(TcpSegmentFlags::ACK) {
            (tcp.acknowledgment_number(), 0, 0x04)
        } else {
            (
                0,
                tcp.sequence_number().wrapping_add(sequence_len),
                0x04 | 0x10,
            )
        };
    let mut source = [0u8; 16];
    let mut destination = [0u8; 16];
    match version {
        4 => {
            if cursor.network_header_len() < 20
                || cursor
                    .network_header_offset()
                    .checked_add(cursor.network_header_len())?
                    > available_len
                || usize::from(packet.get(cursor.network_header_offset())? & 0x0f) * 4 < 20
            {
                return None;
            }
            read_bytes(
                packet.get(
                    cursor.network_header_offset() + 16..cursor.network_header_offset() + 20,
                )?,
                &mut source[..4],
            );
            read_bytes(
                packet.get(
                    cursor.network_header_offset() + 12..cursor.network_header_offset() + 16,
                )?,
                &mut destination[..4],
            );
        }
        6 => {
            if cursor.network_header_len() < 40
                || cursor
                    .network_header_offset()
                    .checked_add(cursor.network_header_len())?
                    > available_len
            {
                return None;
            }
            read_bytes(
                packet.get(
                    cursor.network_header_offset() + 24..cursor.network_header_offset() + 40,
                )?,
                &mut source,
            );
            read_bytes(
                packet
                    .get(cursor.network_header_offset() + 8..cursor.network_header_offset() + 24)?,
                &mut destination,
            );
        }
        _ => return None,
    }
    Some((
        source,
        destination,
        tcp.destination_port(),
        tcp.source_port(),
        response_sequence,
        response_acknowledgment,
        response_flags,
        version,
    ))
}

#[inline(always)]
fn tcp_reset_write_current_reply(
    output: &mut [u8],
    source: [u8; 16],
    destination: [u8; 16],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    version: u8,
) -> Option<usize> {
    match version {
        4 => tcp_reset_write_ipv4_reply(
            output,
            source,
            destination,
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            flags,
        ),
        6 => tcp_reset_write_ipv6_reply(
            output,
            source,
            destination,
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            flags,
        ),
        _ => None,
    }
}

fn tcp_reset_write_ipv4_reply(
    output: &mut [u8],
    source: [u8; 16],
    destination: [u8; 16],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
) -> Option<usize> {
    const IPV4_HEADER_LEN: usize = 20;
    const TCP_HEADER_LEN: usize = 20;
    let total_len = IPV4_HEADER_LEN + TCP_HEADER_LEN;
    let reset = output.get_mut(..total_len)?;
    reset.fill(0);
    reset[0] = 0x45;
    write_be_u16(reset, 2, total_len as u16);
    if crate::transport::active_tcp_policy().pmtu_enabled {
        hammer_core::protocol::ip::apply_ipv4_dont_fragment(reset, true);
    }
    reset[8] = 64;
    reset[9] = 6;
    write_bytes(reset, 12, &source[..4]);
    write_bytes(reset, 16, &destination[..4]);
    tcp_reset_write_tcp_header(
        &mut reset[IPV4_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
    )?;
    let tcp_len_bytes = be_u16(TCP_HEADER_LEN as u16);
    let tcp_checksum = internet_checksum_parts(&[
        &source[..4],
        &destination[..4],
        &[0, 6],
        &tcp_len_bytes,
        &reset[IPV4_HEADER_LEN..],
    ]);
    write_be_u16(reset, 36, tcp_checksum);
    let ip_checksum = internet_checksum(&reset[..IPV4_HEADER_LEN]);
    write_be_u16(reset, 10, ip_checksum);
    Some(total_len)
}

fn tcp_reset_write_ipv6_reply(
    output: &mut [u8],
    source: [u8; 16],
    destination: [u8; 16],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
) -> Option<usize> {
    const IPV6_HEADER_LEN: usize = 40;
    const TCP_HEADER_LEN: usize = 20;
    let total_len = IPV6_HEADER_LEN + TCP_HEADER_LEN;
    let reset = output.get_mut(..total_len)?;
    reset.fill(0);
    reset[0] = 0x60;
    write_be_u16(reset, 4, TCP_HEADER_LEN as u16);
    reset[6] = 6;
    reset[7] = 64;
    write_bytes(reset, 8, &source);
    write_bytes(reset, 24, &destination);
    tcp_reset_write_tcp_header(
        &mut reset[IPV6_HEADER_LEN..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
    )?;
    let tcp_len_bytes = be_u32(TCP_HEADER_LEN as u32);
    let tcp_checksum = internet_checksum_parts(&[
        &source,
        &destination,
        &tcp_len_bytes,
        &[0, 0, 0, 6],
        &reset[IPV6_HEADER_LEN..],
    ]);
    write_be_u16(reset, 56, tcp_checksum);
    Some(total_len)
}

#[inline(always)]
fn tcp_reset_write_tcp_header(
    output: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
) -> Option<()> {
    let header = output.get_mut(..20)?;
    write_be_u16(header, 0, source_port);
    write_be_u16(header, 2, destination_port);
    write_be_u32(header, 4, sequence);
    write_be_u32(header, 8, acknowledgment);
    header[12] = 0x50;
    header[13] = flags;
    Some(())
}

#[inline(always)]
fn write_bytes(output: &mut [u8], offset: usize, bytes: &[u8]) {
    let mut index = 0usize;
    while index < bytes.len() {
        output[offset + index] = bytes[index];
        index += 1;
    }
}

#[inline(always)]
fn read_bytes(input: &[u8], output: &mut [u8]) {
    let mut index = 0usize;
    while index < input.len() {
        output[index] = input[index];
        index += 1;
    }
}

#[inline(always)]
fn write_be_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset] = (value >> 8) as u8;
    output[offset + 1] = value as u8;
}

#[inline(always)]
fn write_be_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset] = (value >> 24) as u8;
    output[offset + 1] = (value >> 16) as u8;
    output[offset + 2] = (value >> 8) as u8;
    output[offset + 3] = value as u8;
}

#[inline(always)]
fn be_u16(value: u16) -> [u8; 2] {
    [(value >> 8) as u8, value as u8]
}

#[inline(always)]
fn be_u32(value: u32) -> [u8; 4] {
    [
        (value >> 24) as u8,
        (value >> 16) as u8,
        (value >> 8) as u8,
        value as u8,
    ]
}

fn refresh_reset_metadata(
    runtime: &DataPlaneRuntime,
    index: Index,
    packet_len: usize,
) -> CoreResult<()> {
    const TCP_HEADER_LEN: usize = 20;

    let mut buffer = runtime.get_buffer_mut(index)?;
    let network_header_len = match buffer.current().first().copied().map(|byte| byte >> 4) {
        Some(4) => 20,
        Some(6) => 40,
        _ => return Err(TcpError::SegmentInvalid.into()),
    };
    buffer.clear_node_error();
    unsafe { std::mem::transmute::<_, &mut crate::net::NetworkOpaque>(buffer.opaque_mut()) }
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(network_header_len, TCP_HEADER_LEN)
                .with_transport_payload_offset(network_header_len + TCP_HEADER_LEN),
        );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

    use hammer_core::data_plane::BufferNodeError;
    use hammer_runtime::InternalNode;

    use super::*;
    use crate::transport::tcp::TcpResetError;

    #[derive(Default)]
    struct CaptureState {
        packets: Vec<Vec<u8>>,
        cursors: Vec<hammer_core::data_plane::BufferPacketCursor>,
        node_errors: Vec<Option<BufferNodeError>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(&mut self, _runtime: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {}

    fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let state = {
            let states = capture_states().lock().expect("capture registry");
            Arc::clone(
                states
                    .get(data.usize_word(0).expect("capture slot"))
                    .expect("capture slot is invalid"),
            )
        };
        let mut state = state.lock().expect("capture state");
        for index in frame.pending_indices().iter().copied() {
            let buffer = runtime.get_buffer(index).expect("capture buffer");
            let cursor =
                unsafe { std::mem::transmute::<_, &crate::net::NetworkOpaque>(buffer.opaque()) }
                    .packet_cursor();
            state.packets.push(buffer.current().to_vec().into());
            state.cursors.push(cursor);
            state
                .node_errors
                .push(runtime.node_error(index).expect("node error"));
        }
        NodeResult::drop()
    }

    #[test]
    fn tcp_reset_node_rewrites_buffer_and_routes_to_lookup() {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                    buffer_slot_capacity: 512,
                    buffer_slots: 8,
                    frame_slots: 4,
                    ..hammer_core::data_plane::DataPlaneBufferConfig::default()
                },
            });
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_node = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
        let reset = runtime
            .nodes()
            .register_internal(TcpResetNode::new(TcpResetNext::nodes(drop_node, lookup)));
        let mut frame = runtime
            .buffers()
            .get_next_frame(reset)
            .expect("alloc frame");
        let index = runtime
            .alloc_index_with_bytes(&ipv4_tcp_packet(0x10, 1_000, 9_000, &[]))
            .expect("alloc packet");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
            unsafe {
                std::mem::transmute::<_, &mut crate::net::NetworkOpaque>(buffer.opaque_mut())
            }
            .set_packet_cursor(buffer_packet_cursor(40));
            let code = TcpResetError::BadTcpHeader.code();
            buffer.set_node_error(BufferNodeError::new(NodeId::new(0), code));
            let _ = runtime.record_current_node_error(code);
        }
        frame.push_index(index).expect("push index");

        runtime.put_next_frame(frame).expect("put reset");
        assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

        let drop_packets = drop_state.lock().expect("drop capture");
        assert!(drop_packets.packets.is_empty());
        std::mem::drop(drop_packets);

        let lookup_packets = lookup_state.lock().expect("lookup capture");
        assert_eq!(lookup_packets.packets.len(), 1);
        let packet = &lookup_packets.packets[0];
        let cursor = lookup_packets.cursors[0];
        let reply_tcp = etherparse::TcpSlice::from_slice(&packet[20..]).expect("parse reply");
        assert_eq!(cursor.packet_len(), packet.len());
        assert_eq!(cursor.network_header_offset(), 0);
        assert_eq!(cursor.transport_header_offset(), 20);
        assert_eq!(cursor.transport_payload_offset(), 40);
        assert!(lookup_packets.node_errors[0].is_none());
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
        let mut packet = hammer_infra::vec![0u8; packet_len];
        packet[0] = 0x45;
        write_be_u16(&mut packet, 2, total_len);
        packet[8] = 64;
        packet[9] = 6;
        write_bytes(&mut packet, 12, &[192, 0, 2, 1]);
        write_bytes(&mut packet, 16, &[198, 51, 100, 2]);
        write_be_u16(&mut packet, 20, 50_000);
        write_be_u16(&mut packet, 22, 80);
        write_be_u32(&mut packet, 24, sequence);
        write_be_u32(&mut packet, 28, acknowledgment);
        packet[32] = 0x50;
        packet[33] = flags;
        write_be_u16(&mut packet, 34, 4096);
        if !payload.is_empty() {
            write_bytes(&mut packet, 40, payload);
        }
        let tcp_checksum = ipv4_l4_checksum([192, 0, 2, 1], [198, 51, 100, 2], 6, &packet[20..]);
        write_be_u16(&mut packet, 36, tcp_checksum);
        let ip_checksum = internet_checksum(&packet[..20]);
        write_be_u16(&mut packet, 10, ip_checksum);
        packet
    }

    fn ipv4_l4_checksum(
        source: [u8; 4],
        destination: [u8; 4],
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let segment_len = be_u16(segment.len() as u16);
        internet_checksum_parts(&[&source, &destination, &[0, protocol], &segment_len, segment])
    }
}
