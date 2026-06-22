use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpSeq;

use super::segment::TcpSegment;

pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[hammer_component_macros::node_next]
pub enum TcpOutputNext {
    Drop,
    Lookup,
}

#[derive(Clone, Copy)]
pub struct TcpOutputNode {
    next: [NodeId; TcpOutputNext::COUNT],
    cached_next: Option<NodeId>,
}

impl TcpOutputNode {
    pub const NODE_NAME: &'static str = "tcp-output-node";

    #[inline]
    pub fn new(next: [NodeId; TcpOutputNext::COUNT]) -> Self {
        Self {
            next,
            cached_next: None,
        }
    }
}

impl Node for TcpOutputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let (result, cached_next) = tcp_output_node_process_frame(
            runtime,
            frame,
            self.next[TcpOutputNext::Lookup as usize],
            self.next[TcpOutputNext::Drop as usize],
            self.cached_next,
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_output_node_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

impl InternalNode for TcpOutputNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, TcpOutputNext::COUNT)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

fn tcp_output_node_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let current = runtime
        .current_node()
        .ok_or_else(|| CoreError::internal("tcp output missing current node"))?;
    let next = [
        runtime
            .nodes()
            .node_next_slot(current, TcpOutputNext::Drop as usize)?,
        runtime
            .nodes()
            .node_next_slot(current, TcpOutputNext::Lookup as usize)?,
    ];
    let (result, _) = tcp_output_node_process_frame(
        runtime,
        frame,
        next[TcpOutputNext::Lookup as usize],
        next[TcpOutputNext::Drop as usize],
        None,
    )?;
    Ok(result)
}

fn tcp_output_node_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    lookup: NodeId,
    drop: NodeId,
    cached_next: Option<NodeId>,
) -> CoreResult<(NodeResult, Option<NodeId>)> {
    NodeVectorDispatch::new(cached_next).route_frame(
        runtime,
        frame,
        prefetch_tcp_output,
        |_, indices, nexts| {
            for (offset, index) in indices.iter().copied().enumerate() {
                nexts[offset] = Some(tcp_output_next_for_index(runtime, index, lookup, drop)?);
            }
            Ok(())
        },
    )
}

#[inline(always)]
fn prefetch_tcp_output(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
    for index in indices.iter().copied() {
        batch.prefetch_read(index);
    }
}

fn tcp_output_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    lookup: NodeId,
    drop: NodeId,
) -> CoreResult<NodeId> {
    let segment = TcpSegment::read_from_buffer(runtime, index)?;
    let mut header = [0u8; 64];
    let header_len = segment.write_header(&mut header)?;
    let header = &header[..header_len];
    if runtime.packet_buffers().prepend(index, header).is_err() {
        return Ok(drop);
    }
    Ok(lookup)
}

#[inline]
pub const fn tcp_effective_output_payload_len(peer_max_segment_size: Option<u16>) -> usize {
    match peer_max_segment_size {
        Some(max_segment_size) if max_segment_size != 0 => {
            let max_segment_size = max_segment_size as usize;
            if max_segment_size < DEFAULT_TCP_OUTPUT_PAYLOAD_LEN {
                max_segment_size
            } else {
                DEFAULT_TCP_OUTPUT_PAYLOAD_LEN
            }
        }
        _ => DEFAULT_TCP_OUTPUT_PAYLOAD_LEN,
    }
}

#[inline]
pub fn tcp_available_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
) -> u32 {
    snd_wnd
        .min(congestion_window)
        .saturating_sub(tcp_inflight_sequence_len(snd_una, snd_nxt))
}

#[inline]
pub fn tcp_payload_len_in_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
    requested_payload_len: usize,
    control_len: u32,
) -> usize {
    let available_payload_len =
        tcp_available_send_window(snd_una, snd_nxt, snd_wnd, congestion_window)
            .saturating_sub(control_len) as usize;
    available_payload_len.min(requested_payload_len)
}

#[inline]
pub const fn tcp_output_sequence_len(flags: u8, payload_len: usize) -> u32 {
    let control_len = ((flags & TCP_FLAG_SYN != 0) as u32) + ((flags & TCP_FLAG_FIN != 0) as u32);
    payload_len as u32 + control_len
}

#[inline]
pub fn tcp_output_next_sequence(sequence: u32, sequence_len: u32) -> u32 {
    TcpSeq::from(sequence).advance(sequence_len).raw()
}

#[inline]
fn tcp_inflight_sequence_len(snd_una: u32, snd_nxt: u32) -> u32 {
    if snd_una != 0 && snd_nxt != 0 {
        TcpSeq::from(snd_una).distance_to(TcpSeq::from(snd_nxt))
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{BufferFrame, NodeProcessFn};
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpSegmentFlags};

    use super::*;
    use crate::transport::tcp::segment::TcpSegment;

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
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must use descriptor process",
            ))
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
    ) -> CoreResult<NodeResult> {
        let state = {
            let states = capture_states()
                .lock()
                .map_err(|_| CoreError::internal("capture registry poisoned"))?;
            Arc::clone(
                states
                    .get(data.usize_word(0)?)
                    .ok_or_else(|| CoreError::internal("capture state missing"))?,
            )
        };
        for index in frame.drain_pending() {
            let packet = runtime.get_buffer(index)?.current().to_vec();
            state
                .lock()
                .map_err(|_| CoreError::internal("capture poisoned"))?
                .packets
                .push(packet);
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    fn output_graph() -> (
        DataPlaneRuntime,
        Arc<Mutex<CaptureState>>,
        Arc<Mutex<CaptureState>>,
        NodeId,
    ) {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
        let drop = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, lookup)));
        (runtime, lookup_state, drop_state, output)
    }

    fn send_to_output(runtime: &DataPlaneRuntime, output: NodeId, index: BufferIndex) {
        let frame = runtime.alloc_frame_index().expect("frame");
        runtime
            .get_frame_mut(frame)
            .expect("frame mut")
            .push_index(index)
            .expect("push index");
        assert!(runtime.schedule_frame(output, frame).expect("schedule"));
    }

    fn test_segment(payload_len: usize) -> TcpSegment {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
        TcpSegment::new(
            local,
            remote,
            100,
            200,
            4096,
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            TcpCapabilities::default(),
            None,
            None,
            None,
            payload_len,
        )
    }

    #[test]
    fn tcp_output_prepends_segment_header_and_routes_lookup() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index().expect("buffer");
        runtime.append(index, b"hello").expect("payload");
        test_segment(5)
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(drop_state.lock().unwrap().packets.is_empty());
        let packets = &lookup_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        assert_eq!(&packet[0..2], &50000u16.to_be_bytes());
        assert_eq!(&packet[2..4], &443u16.to_be_bytes());
        assert_eq!(&packet[4..8], &100u32.to_be_bytes());
        assert_eq!(&packet[8..12], &200u32.to_be_bytes());
        assert_eq!(packet[12] >> 4, 5);
        assert_eq!(packet[13] & TCP_FLAG_ACK, TCP_FLAG_ACK);
        assert_eq!(packet[13] & TCP_FLAG_PSH, TCP_FLAG_PSH);
        assert_eq!(&packet[14..16], &4096u16.to_be_bytes());
        assert_eq!(&packet[20..], b"hello");
    }

    #[test]
    fn tcp_output_missing_intent_routes_drop() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index_with_bytes(b"hello").expect("buffer");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(lookup_state.lock().unwrap().packets.is_empty());
        assert_eq!(drop_state.lock().unwrap().packets.len(), 1);
    }

    #[test]
    fn tcp_output_routes_drop_when_prepend_fails() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index_with_bytes(b"hello").expect("buffer");
        let current_data = runtime.current_data(index).expect("current data");
        runtime
            .packet_buffers()
            .advance(index, current_data)
            .expect("consume headroom");
        test_segment(5)
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(lookup_state.lock().unwrap().packets.is_empty());
        assert_eq!(drop_state.lock().unwrap().packets.len(), 1);
    }
}
