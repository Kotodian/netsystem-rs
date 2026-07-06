use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};

use hammer_core::error::{CoreError, CoreResult};

use super::publish_tcp_connection;
use super::segment::tcp_packet;
use super::{TCP_MAIN, TcpNodeError, TcpQueue, ensure_tcp_session_queue, read_session_id};
use super::{tcp_worker_state, tcp_worker_state_mut};
use crate::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::transport::tcp::syn_sent::register_tcp_syn_sent,
    name = "tcp-syn-sent",
    next = TcpSynSentNext,
    role = internal,
)]
pub struct TcpSynSentNode<C: CongestionController + 'static> {
    session_queue: TcpQueue<C>,
}

pub fn register_tcp_syn_sent(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
        let queue = TcpQueue::<C>::new(queue_data);
        TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| CoreError::internal("tcp main not initialized"))?;
        runtime.nodes().try_register_internal_with_next_names(
            TcpSynSentNode::<C>::new(queue, [NodeId::new(0); TcpSynSentNext::COUNT]),
            &TcpSynSentNext::NEXT_NAMES,
        )
    })
}

impl<C> Node for TcpSynSentNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let next = match Self::runtime_nexts(runtime) {
            Ok(next) => next,
            Err(_) => return NodeResult::drop(),
        };
        tcp_syn_sent_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_syn_sent_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_syn_sent_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let next = match TcpSynSentNode::<C>::runtime_nexts(runtime) {
        Ok(next) => next,
        Err(_) => return NodeResult::drop(),
    };
    tcp_syn_sent_frame::<C>(runtime, frame, TcpQueue::<C>::new(data), next)
}

fn tcp_syn_sent_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueue<C>,
    next: [NodeId; TcpSynSentNext::COUNT],
) -> NodeResult
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpSynSentNext::Output as usize];
    let drop_next = next[TcpSynSentNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    let indices = frame.pending_indices();
    let len = indices.len();
    let mut read = 0usize;
    while read + 4 <= len {
        prefetch_tcp_syn_sent(runtime, &indices[read..read + 4]);
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read + 1],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read + 2],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read + 3],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        read += 4;
    }
    if read + 2 <= len {
        prefetch_tcp_syn_sent(runtime, &indices[read..read + 2]);
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        tcp_syn_sent_enqueue_index(
            runtime,
            indices[read + 1],
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        read += 2;
    }
    while read < len {
        let index = indices[read];
        prefetch_tcp_syn_sent(runtime, &indices[read..read + 1]);
        tcp_syn_sent_enqueue_index(
            runtime,
            index,
            session_queue,
            tcp_output,
            drop_next,
            &mut next_frames,
        );
        read += 1;
    }
    next_frames.finish(runtime, frame)
}

fn tcp_syn_sent_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = tcp_packet(runtime, index)?;
    let mut release_input = true;
    let mut tx_index = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ =
                runtime.record_current_node_error(TcpNodeError::SynSentSessionRouteMissing.code());
            TcpNodeError::SynSentSessionRouteMissing
        })?;
        let (control, acked_tx_len, established, established_with_payload) = {
            let local_capabilities = tcp_worker_state()
                .pending_open_capabilities(session_id)
                .unwrap_or_default();
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ =
                    runtime.record_current_node_error(TcpNodeError::SynSentSessionMissing.code());
                TcpNodeError::SynSentSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let previous_state = connection.state();
            let control = connection.receive_open_reply(&packet, local_capabilities)?;
            let established = connection.state() == crate::transport::tcp::TcpState::Established;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                established,
                previous_state == crate::transport::tcp::TcpState::SynSent
                    && established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            queue.release_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if let Some(cookie) = packet.fast_open_cookie.filter(|cookie| !cookie.is_empty()) {
            tcp_worker_state_mut().remember_fast_open_cookie(
                packet.local,
                packet.remote,
                cookie,
                packet.capabilities.max_segment_size,
            );
        }
        if established_with_payload {
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset as isize)?;
                buffer.truncate(packet.payload_len)?;
            }
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            release_input = false;
        };
        publish_tcp_connection(&mut queue, session_id)?;
        if established {
            queue.app().connected(session_id)?;
        }
        if let Some(segment) = control {
            let allocated = runtime.buffers().alloc_index()?;
            if let Err(error) = segment.write_to_buffer(runtime.buffers(), allocated) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    };
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            runtime.free_index(tx_index);
        }
        return Err(error);
    }
    if let Some(tx_index) = tx_index.take() {
        next_frames.enqueue(runtime, tcp_output, tx_index);
    }
    if release_input {
        runtime.free_index(index);
    }
    Ok(())
}

#[inline(always)]
fn prefetch_tcp_syn_sent(runtime: &DataPlaneRuntime, indices: &[BufferIndex]) {
    let mut read = 0usize;
    while read < indices.len() {
        runtime.prefetch_header(indices[read]);
        read += 1;
    }
}

#[inline(always)]
fn tcp_syn_sent_enqueue_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueue<C>,
    tcp_output: NodeId,
    drop_next: NodeId,
    next_frames: &mut NodeNextFrames,
) where
    C: CongestionController + 'static,
{
    if tcp_syn_sent_index(runtime, index, session_queue, tcp_output, next_frames).is_err() {
        hammer_adapter::validate_buffer_enqueue_x1!(runtime, next_frames, drop_next, index);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::data_plane::DropNode;
    use crate::net::NetworkOpaque;
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::input::TcpInputControlPlane;
    use crate::transport::tcp::lookup::TcpLookupSnapshot;
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};
    use crate::transport::tcp::{
        TcpInputNext, TcpQueue, TcpSessionDriver, TcpWorkerOwnedState, connect_tcp_session,
        set_tcp_worker_state,
    };
    use hammer_adapter::{BufferPacketCursor, DataWorkerId, InternalNode, NodeId};
    use hammer_core::error::CoreError;

    use super::*;

    const LOCAL: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 50_001;
    const REMOTE_PORT: u16 = 443;
    const SERVER_ISN: u32 = 11_000;
    const SYN: u8 = 0x02;
    const RST: u8 = 0x04;
    const ACK: u8 = 0x10;

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
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
        fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
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

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let state = match capture_states().lock() {
            Ok(states) => {
                let slot = match data.usize_word(0) {
                    Ok(slot) => slot,
                    Err(_) => return NodeResult::drop(),
                };
                match states.get(slot) {
                    Some(state) => Arc::clone(state),
                    None => return NodeResult::drop(),
                }
            }
            Err(_) => return NodeResult::drop(),
        };
        let mut pending = frame.drain_pending();
        while let Some(index) = pending.next() {
            let packet = match runtime.get_buffer(index) {
                Ok(buffer) => buffer.current().to_vec(),
                Err(_) => continue,
            };
            match state.lock() {
                Ok(mut state) => state.packets.push(packet),
                Err(_) => continue,
            }
            runtime.free_index(index);
        }
        NodeResult::drop()
    }

    fn open_client_session(handle: TcpQueue<BbrController>) -> (crate::session::SessionId, u32) {
        let mut queue = handle.borrow_mut().expect("tcp queue");
        let session_id =
            connect_tcp_session(&mut queue, local_addr(), remote_addr()).expect("connect session");
        let connection = queue.session(session_id).expect("tcp session is missing");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynSent);
        let initial_sequence = connection.iss();
        (session_id, initial_sequence)
    }

    fn local_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(LOCAL), LOCAL_PORT)
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(REMOTE), REMOTE_PORT)
    }

    fn tcp_input_node(
        runtime: &DataPlaneRuntime,
        handle: TcpQueue<BbrController>,
        syn_sent: NodeId,
        drop: NodeId,
    ) -> NodeId {
        let control = TcpInputControlPlane::new();
        control
            .publish_lookup(TcpLookupSnapshot::default())
            .expect("publish lookup");
        runtime.nodes().register_internal(control.node(
            TcpInputNext::nodes(drop, drop, drop, drop, syn_sent, drop, drop),
            Some(handle),
            None,
        ))
    }

    fn send_packet(runtime: &DataPlaneRuntime, node: NodeId, packet: std::vec::Vec<u8>) {
        let mut frame = runtime.alloc_frame().expect("frame");
        let buffer = runtime.alloc_index_with_bytes(&packet).expect("packet");
        stamp_tcp_cursor(runtime, buffer, &packet);
        frame.push_index(buffer).expect("push packet");
        runtime.submit_frame(frame, node).expect("submit");
    }

    fn stamp_tcp_cursor(
        runtime: &DataPlaneRuntime,
        buffer: hammer_adapter::BufferIndex,
        packet: &[u8],
    ) {
        let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
        let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        let tcp_header_len = ((packet[header_len + 12] >> 4) as usize) * 4;
        let mut buffer = runtime.get_buffer_mut(buffer).expect("buffer mut");
        unsafe { std::mem::transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
            .set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, header_len)
                    .with_transport_header(header_len, tcp_header_len)
                    .with_transport_payload_offset(header_len + tcp_header_len),
            );
    }

    fn assert_tcp_packet(
        packet: &[u8],
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        flags: u8,
    ) {
        let _ = source;
        let _ = destination;
        assert_eq!(u16::from_be_bytes([packet[0], packet[1]]), source_port);
        assert_eq!(u16::from_be_bytes([packet[2], packet[3]]), destination_port);
        assert_eq!(
            u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]),
            sequence
        );
        assert_eq!(
            u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]),
            acknowledgment
        );
        assert_eq!(packet[13] & flags, flags);
    }

    fn tcp_packet(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        flags: u8,
    ) -> std::vec::Vec<u8> {
        let mut packet = ipv4_packet(source, destination, 6, 20);
        write_tcp_segment(
            &mut packet[20..],
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            flags,
        );
        let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        write_be_u16(&mut packet, 36, checksum);
        update_ipv4_header_checksum(&mut packet);
        packet
    }

    fn ipv4_packet(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        payload_len: usize,
    ) -> std::vec::Vec<u8> {
        let total_len = 20 + payload_len;
        let mut packet = vec![0u8; total_len];
        packet[0] = 0x45;
        write_be_u16(&mut packet, 2, total_len as u16);
        packet[8] = 64;
        packet[9] = protocol;
        write_bytes(&mut packet, 12, &source.octets());
        write_bytes(&mut packet, 16, &destination.octets());
        packet
    }

    fn write_tcp_segment(
        segment: &mut [u8],
        source_port: u16,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        flags: u8,
    ) {
        write_be_u16(segment, 0, source_port);
        write_be_u16(segment, 2, destination_port);
        write_be_u32(segment, 4, sequence);
        write_be_u32(segment, 8, acknowledgment);
        segment[12] = 0x50;
        segment[13] = flags;
        write_be_u16(segment, 14, u16::MAX);
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let segment_len = be_u16(segment.len() as u16);
        internet_checksum_parts(&[
            &source.octets(),
            &destination.octets(),
            &[0, protocol],
            &segment_len,
            segment,
        ])
    }

    fn update_ipv4_header_checksum(packet: &mut [u8]) {
        packet[10] = 0;
        packet[11] = 0;
        let checksum = internet_checksum(&packet[..20]);
        write_be_u16(packet, 10, checksum);
    }

    fn write_bytes(output: &mut [u8], offset: usize, bytes: &[u8]) {
        let mut index = 0usize;
        while index < bytes.len() {
            output[offset + index] = bytes[index];
            index += 1;
        }
    }

    fn write_be_u16(output: &mut [u8], offset: usize, value: u16) {
        output[offset] = (value >> 8) as u8;
        output[offset + 1] = value as u8;
    }

    fn write_be_u32(output: &mut [u8], offset: usize, value: u32) {
        output[offset] = (value >> 24) as u8;
        output[offset + 1] = (value >> 16) as u8;
        output[offset + 2] = (value >> 8) as u8;
        output[offset + 3] = value as u8;
    }

    fn be_u16(value: u16) -> [u8; 2] {
        [(value >> 8) as u8, value as u8]
    }

    fn internet_checksum(bytes: &[u8]) -> u16 {
        let mut sum = 0u32;
        for chunk in bytes.chunks(2) {
            let word = match chunk {
                [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
                [hi] => u16::from_be_bytes([*hi, 0]) as u32,
                _ => unreachable!(),
            };
            sum += word;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }

    fn internet_checksum_parts(parts: &[&[u8]]) -> u16 {
        let mut sum = 0u32;
        let mut high = None;
        for part in parts {
            let mut index = 0usize;
            if let Some(hi) = high.take() {
                if let Some(&lo) = part.first() {
                    sum += u16::from_be_bytes([hi, lo]) as u32;
                    while sum > 0xffff {
                        sum = (sum & 0xffff) + (sum >> 16);
                    }
                    index = 1;
                } else {
                    high = Some(hi);
                    continue;
                }
            }
            let mut chunks = part[index..].chunks_exact(2);
            for chunk in &mut chunks {
                sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
                while sum > 0xffff {
                    sum = (sum & 0xffff) + (sum >> 16);
                }
            }
            if let [hi] = chunks.remainder() {
                high = Some(*hi);
            }
        }
        if let Some(hi) = high {
            sum += u16::from_be_bytes([hi, 0]) as u32;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }
}
