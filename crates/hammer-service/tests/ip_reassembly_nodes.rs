use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferFrame, BufferNodeError, DataPlaneHandoff, DataPlaneRuntime, DataWorkerId, InternalNode,
    Network, Node, NodeHandle, NodeProcessFn, NodeResult, NodeRuntimeData, RouteMetadata,
    SocksAddr, TraceControlPlane, TraceInputPolicy, TracePolicy,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_service::data_plane::DropNode;
use hammer_service::net::{
    IcmpErrorNext, IcmpErrorNode, IcmpErrorSourceTable, IpInputError, IpInputNext, IpInputNode,
    IpInputTarget, IpInputTrace, IpProtocol, IpReassemblyDirectory, IpReassemblyHandoff,
    IpReassemblyNext, IpReassemblyNode, IpReassemblyTrace, IpReassemblyTraceAction, IpUnicastArc,
};

type PacketVec = hammer_infra::vec::Vec<u8>;

struct SinkNode {
    runtime_data: NodeRuntimeData,
}

#[derive(Default)]
struct SinkState {
    packets: Vec<PacketVec>,
    metadata: Vec<RouteMetadata>,
    node_errors: Vec<Option<BufferNodeError>>,
    buffer_indices: Vec<hammer_adapter::BufferIndex>,
    chained: Vec<bool>,
}

impl SinkNode {
    fn new(state: Arc<Mutex<SinkState>>) -> Self {
        let mut states = sink_states().lock().expect("sink state registry poisoned");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("sink state slot"),
        }
    }
}

impl Node for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "sink node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        sink_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for SinkNode {}

#[derive(Clone)]
struct SinkCapture {
    state: Arc<Mutex<SinkState>>,
}

impl SinkCapture {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SinkState::default())),
        }
    }

    fn node(&self) -> SinkNode {
        SinkNode::new(Arc::clone(&self.state))
    }

    fn len(&self) -> usize {
        self.state.lock().unwrap().packets.len()
    }

    fn packets(&self) -> Vec<PacketVec> {
        self.state.lock().unwrap().packets.clone()
    }

    fn metadata(&self) -> Vec<RouteMetadata> {
        self.state.lock().unwrap().metadata.clone()
    }

    fn node_errors(&self) -> Vec<Option<BufferNodeError>> {
        self.state.lock().unwrap().node_errors.clone()
    }

    fn buffer_indices(&self) -> Vec<hammer_adapter::BufferIndex> {
        self.state.lock().unwrap().buffer_indices.clone()
    }

    fn chained(&self) -> Vec<bool> {
        self.state.lock().unwrap().chained.clone()
    }
}

fn sink_states() -> &'static Mutex<Vec<Arc<Mutex<SinkState>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<SinkState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn sink_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = {
        let states = sink_states()
            .lock()
            .map_err(|_| CoreError::internal("sink state registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("sink state slot is invalid"))?,
        )
    };
    for index in frame.drain_pending() {
        let chained = runtime.is_chained(index)?;
        let packet = runtime.copy_current_chain(index)?;
        let metadata = runtime.metadata(index)?;
        let node_error = runtime.node_error(index)?;
        let mut state = state
            .lock()
            .map_err(|_| CoreError::internal("sink state poisoned"))?;
        state.buffer_indices.push(index);
        state.chained.push(chained);
        state.packets.push(packet);
        state.metadata.push(metadata);
        state.node_errors.push(node_error);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode,
{
    let _ = node;
}

#[test]
fn ip_input_routes_ipv4_options_to_options_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let lookup_capture = SinkCapture::new();
    let options_capture = SinkCapture::new();
    let lookup = runtime.nodes().register_internal(lookup_capture.node());
    let options = runtime.nodes().register_internal(options_capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(lookup, drop)));
    let input = IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
        lookup, lookup, options, lookup, lookup, lookup, reassembly,
    ));
    assert_internal_node(&input);
    let input = runtime.nodes().register_internal(input);
    let control = TraceControlPlane::new(4);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: input,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 2);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_options_packet();
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &packet)
        .expect("alloc packet");
    runtime.try_mark_trace(input, buffer).expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(options_capture.packets(), vec![packet]);
    assert_eq!(
        options_capture.node_errors(),
        vec![Some(BufferNodeError::new(
            input,
            IpInputError::Options.code()
        ))]
    );
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::Options.code())
            .expect("options counter"),
        1
    );
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    let trace = IpInputTrace::decode(&records[0].entries[0].payload_bytes).expect("ip input trace");
    assert_eq!(trace.version, Some(hammer_service::net::IpVersion::V4));
    assert_eq!(trace.protocol, Some(IpProtocol::Udp));
    assert_eq!(trace.input_target, Some(IpInputTarget::Options));
    assert_eq!(trace.input_error, Some(IpInputError::Options));
    assert_eq!(trace.next, options);
    assert_eq!(lookup_capture.len(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_input_matches_vpp_ipv4_validation_nexts() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 16);
    let ingress_interface = 7;
    let local_source = [198, 51, 100, 1];
    let lookup_capture = SinkCapture::new();
    let mcast_capture = SinkCapture::new();
    let icmp_capture = SinkCapture::new();
    let reassembly_capture = SinkCapture::new();
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = runtime.nodes().register_internal(lookup_capture.node());
    let mcast = runtime.nodes().register_internal(mcast_capture.node());
    let icmp_lookup = runtime.nodes().register_internal(icmp_capture.node());
    let error_sources = IcmpErrorSourceTable::from_sources([(
        ingress_interface,
        IpAddr::V4(Ipv4Addr::from(local_source)),
    )]);
    let icmp = runtime.nodes().register_internal(
        IcmpErrorNode::new(IcmpErrorNext::nodes(drop, icmp_lookup))
            .with_source_table(error_sources.handle()),
    );
    let reassembly_sink = runtime.nodes().register_internal(reassembly_capture.node());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(
            reassembly_sink,
            drop,
        )));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, mcast, icmp, reassembly,
        )));
    let unknown_protocol = ipv4_protocol_packet([10, 0, 0, 2], [198, 51, 100, 7], 143, b"opaque");
    let multicast = ipv4_udp_packet([10, 0, 0, 3], 12_346, [224, 0, 0, 1], 53, b"mcast");
    let ttl_expired = ipv4_with_ttl(
        ipv4_udp_packet([10, 0, 0, 4], 12_347, [198, 51, 100, 8], 53, b"ttl"),
        0,
    );
    let bad_checksum = ipv4_bad_checksum(ipv4_udp_packet(
        [10, 0, 0, 5],
        12_348,
        [198, 51, 100, 9],
        53,
        b"bad",
    ));
    let fragment_offset_one = ipv4_fragment_offset_one(ipv4_udp_packet(
        [10, 0, 0, 6],
        12_349,
        [198, 51, 100, 10],
        53,
        b"frag-offset-one",
    ));
    let fragmented = ipv4_fragment(
        &ipv4_udp_packet([10, 0, 0, 7], 12_350, [198, 51, 100, 11], 53, b"fragmented"),
        104,
        0,
        8,
        true,
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet_on_interface(&runtime, frame, &unknown_protocol, ingress_interface);
    push_packet_on_interface(&runtime, frame, &multicast, ingress_interface);
    push_packet_on_interface(&runtime, frame, &ttl_expired, ingress_interface);
    push_packet_on_interface(&runtime, frame, &bad_checksum, ingress_interface);
    push_packet_on_interface(&runtime, frame, &fragment_offset_one, ingress_interface);
    push_packet_on_interface(&runtime, frame, &fragmented, ingress_interface);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 7);
    assert_eq!(lookup_capture.packets(), vec![unknown_protocol]);
    assert_eq!(mcast_capture.packets(), vec![multicast]);
    let icmp_packets = icmp_capture.packets();
    assert_eq!(icmp_packets.len(), 1);
    assert_ipv4_time_exceeded(&icmp_packets[0], &ttl_expired, local_source);
    assert_eq!(icmp_capture.node_errors(), vec![None]);
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::TimeExpired.code())
            .expect("ttl counter"),
        1
    );
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::BadChecksum.code())
            .expect("checksum counter"),
        1
    );
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::FragmentOffsetOne.code())
            .expect("fragment offset counter"),
        1
    );
    assert_eq!(
        runtime
            .node_error_count(drop, IpInputError::BadChecksum.code())
            .expect("drop counter"),
        0
    );
    assert_eq!(reassembly_capture.len(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 1);
}

#[test]
fn ip_input_matches_vpp_ipv6_validation_nexts() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 24, 8, 16);
    let ingress_interface = 9;
    let local_source = Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 5);
    let lookup_capture = SinkCapture::new();
    let mcast_capture = SinkCapture::new();
    let icmp_capture = SinkCapture::new();
    let reassembly_capture = SinkCapture::new();
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = runtime.nodes().register_internal(lookup_capture.node());
    let mcast = runtime.nodes().register_internal(mcast_capture.node());
    let icmp_lookup = runtime.nodes().register_internal(icmp_capture.node());
    let error_sources =
        IcmpErrorSourceTable::from_sources([(ingress_interface, IpAddr::V6(local_source))]);
    let icmp = runtime.nodes().register_internal(
        IcmpErrorNode::new(IcmpErrorNext::nodes(drop, icmp_lookup))
            .with_source_table(error_sources.handle()),
    );
    let reassembly_sink = runtime.nodes().register_internal(reassembly_capture.node());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(
            reassembly_sink,
            drop,
        )));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, mcast, icmp, reassembly,
        )));
    let lookup_packet = ipv6_protocol_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 1),
        Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 2),
        143,
        64,
        b"opaque",
    );
    let multicast = ipv6_udp_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 3),
        12_345,
        Ipv6Addr::from([0xff, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
        53,
        b"mcast",
    );
    let hop_expired = ipv6_protocol_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 4),
        Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 5),
        143,
        0,
        b"expired",
    );
    let too_short = vec![0x60, 0, 0, 0, 0, 0, 17, 64];
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet_on_interface(&runtime, frame, &lookup_packet, ingress_interface);
    push_packet_on_interface(&runtime, frame, &multicast, ingress_interface);
    push_packet_on_interface(&runtime, frame, &hop_expired, ingress_interface);
    push_packet_on_interface(&runtime, frame, &too_short, ingress_interface);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 6);
    assert_eq!(lookup_capture.packets(), vec![lookup_packet]);
    assert_eq!(mcast_capture.packets(), vec![multicast]);
    let icmp_packets = icmp_capture.packets();
    assert_eq!(icmp_packets.len(), 1);
    assert_ipv6_time_exceeded(&icmp_packets[0], &hop_expired, local_source);
    assert_eq!(icmp_capture.node_errors(), vec![None]);
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::TimeExpired.code())
            .expect("hop counter"),
        1
    );
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::BadLength.code())
            .expect("bad length counter"),
        1
    );
    assert_eq!(reassembly_capture.len(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_input_validates_chain_length_like_vpp_current_chain() {
    let runtime = DataPlaneRuntime::with_capacities(24, 16, 8, 8);
    let lookup_capture = SinkCapture::new();
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = runtime.nodes().register_internal(lookup_capture.node());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(lookup, drop)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, lookup, drop, reassembly,
        )));
    let packet = ipv4_protocol_packet(
        [10, 0, 0, 8],
        [198, 51, 100, 12],
        143,
        b"payload-spans-more-than-one-buffer",
    );
    assert!(packet.len() > 24);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(lookup_capture.packets(), vec![packet]);
    assert_eq!(
        runtime
            .node_error_count(input, IpInputError::BadLength.code())
            .expect("bad length counter"),
        0
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_emits_complete_packet_after_out_of_order_fragments() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)));
    let trace = TraceControlPlane::new(8);
    trace.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: reassembly,
            count: 2,
        }],
    });
    runtime.set_trace_control(Some(trace.handle()), 4);
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 2],
        12_345,
        [198, 51, 100, 7],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 100, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_packet(&runtime, frame, reassembly, &fragments[1]);
    let first_fragment = push_marked_packet(&runtime, frame, reassembly, &fragments[0]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        &capture.packets(),
        &[ipv4_reassembled_packet(&original, 100)]
    );
    let metadata = capture.metadata();
    assert_eq!(metadata[0].network, Network::Udp);
    assert_eq!(
        metadata[0].source,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 0))
    );
    assert_eq!(
        metadata[0].destination,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 0))
    );
    assert_eq!(&capture.buffer_indices(), &[first_fragment]);
    assert_eq!(&capture.chained(), &[true]);
    assert_eq!(trace.drain_completed(), 2);
    let records = trace.take_records();
    assert_eq!(records.len(), 2);
    let traces = records
        .iter()
        .flat_map(|record| record.entries.iter())
        .filter(|entry| entry.node_name == Some("ip-reassembly-node"))
        .map(|entry| IpReassemblyTrace::decode(&entry.payload_bytes).expect("ip reassembly trace"))
        .collect::<Vec<_>>();
    assert_eq!(traces.len(), 2);
    let key = traces[0].key.expect("trace key");
    assert!(traces.iter().all(|trace| trace.key == Some(key)));
    assert!(
        traces
            .iter()
            .all(|trace| trace.current_worker == DataWorkerId::new(0))
    );
    assert!(
        traces
            .iter()
            .all(|trace| trace.owner_worker == Some(DataWorkerId::new(0)))
    );
    assert!(
        traces.iter().any(|trace| {
            trace.action == IpReassemblyTraceAction::Pending && trace.next.is_none()
        })
    );
    assert!(traces.iter().any(|trace| {
        trace.action == IpReassemblyTraceAction::Reassembled && trace.next == Some(sink)
    }));
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_reuses_current_frame_for_complete_packet() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 1);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 22],
        12_365,
        [198, 51, 100, 27],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 110, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[1]);
    push_packet(&runtime, frame, &fragments[0]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        capture.packets(),
        vec![ipv4_reassembled_packet(&original, 110)]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_accepts_more_than_three_fragments_by_default() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 12],
        12_355,
        [198, 51, 100, 17],
        53,
        b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ012345",
    );
    let fragments = ipv4_fragments_by_payload_lengths(&original, 105, &[16, 16, 16, 28]);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[2]);
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[3]);
    push_packet(&runtime, frame, &fragments[1]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        &capture.packets(),
        &[ipv4_reassembled_packet(&original, 105)]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_drops_context_when_fragment_limit_is_exceeded() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime.nodes().register_internal(
        IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)).with_max_fragments_per_reassembly(2),
    );
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 13],
        12_356,
        [198, 51, 100, 18],
        53,
        b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ012345",
    );
    let fragments = ipv4_fragments_by_payload_lengths(&original, 106, &[16, 16, 16, 28]);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[1]);
    push_packet(&runtime, frame, &fragments[2]);
    push_packet(&runtime, frame, &fragments[3]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert!(capture.packets().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_ignores_duplicate_covered_fragment() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 3],
        12_346,
        [198, 51, 100, 8],
        443,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 101, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[1]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(
        &capture.packets(),
        &[ipv4_reassembled_packet(&original, 101)]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_drops_context_on_partial_overlap() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let drop = runtime.nodes().register_internal(DropNode::new());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, drop)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 4],
        12_347,
        [198, 51, 100, 9],
        853,
        b"abcdefghijklmnopqrstuvwxyz012345",
    );
    let first = ipv4_fragment(&original, 102, 0, 24, true);
    let overlap = ipv4_fragment(&original, 102, 16, 16, false);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &first);
    push_packet(&runtime, frame, &overlap);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert!(capture.packets().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv6_reassembly_emits_complete_packet_after_out_of_order_fragments() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(ip_reassembly_nexts(sink, sink)));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            sink, reassembly,
        )));
    let original = ipv6_udp_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1),
        20_000,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2),
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv6_fragments(&original, 0x0102_0304, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[1]);
    let first_fragment = push_packet(&runtime, frame, &fragments[0]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(&capture.packets(), &[original]);
    let metadata = capture.metadata();
    assert_eq!(metadata[0].network, Network::Udp);
    assert_eq!(
        metadata[0].source,
        Some(SocksAddr::ip(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1)),
            0
        ))
    );
    assert_eq!(
        metadata[0].destination,
        Some(SocksAddr::ip(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2)),
            0
        ))
    );
    assert_eq!(&capture.buffer_indices(), &[first_fragment]);
    assert_eq!(&capture.chained(), &[true]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn reassembly_expire_frees_incomplete_fragments() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 4);
    let capture = SinkCapture::new();
    let sink = runtime.nodes().register_internal(capture.node());
    let mut reassembly_node = IpReassemblyNode::new(ip_reassembly_nexts(sink, sink))
        .with_timeout(Duration::from_millis(100));
    let reassembly = runtime.nodes().register_internal(reassembly_node.clone());
    let packet = ipv4_udp_packet(
        [10, 0, 0, 5],
        12_348,
        [198, 51, 100, 10],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&packet, 103, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &fragments[0])
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");

    let started = Instant::now();
    assert!(
        runtime
            .schedule_frame(reassembly, frame)
            .expect("schedule fragment")
    );
    assert_eq!(runtime.run_ready_nodes().expect("process fragment"), 1);

    assert_eq!(runtime.in_use_buffers(), 1);
    assert_eq!(
        reassembly_node.expire(&runtime, started + Duration::from_secs(1)),
        1
    );
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_handoffs_fragments_to_owner_worker_without_copying_payload_storage() {
    const REASSEMBLY_HANDLE: NodeHandle = NodeHandle::new(1);
    const SINK_HANDLE: NodeHandle = NodeHandle::new(2);
    let handoff = DataPlaneHandoff::new(2, 8);
    let directory = IpReassemblyDirectory::default();
    let first_runtime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let capture = SinkCapture::new();
    let first_sink = first_runtime
        .nodes()
        .register_internal_with_handle(SINK_HANDLE, capture.node())
        .expect("register first sink");
    let first_reassembly = first_runtime
        .nodes()
        .register_internal_with_handle(
            REASSEMBLY_HANDLE,
            IpReassemblyNode::new(ip_reassembly_nexts(first_sink, first_sink)).with_handoff(
                IpReassemblyHandoff::new(
                    REASSEMBLY_HANDLE,
                    SINK_HANDLE,
                    DataWorkerId::new(0),
                    directory.clone(),
                ),
            ),
        )
        .expect("register first reassembly");
    let first_input = first_runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            first_sink,
            first_reassembly,
        )));
    let second_sink = second_runtime
        .nodes()
        .register_internal_with_handle(SINK_HANDLE, capture.node())
        .expect("register second sink");
    let second_reassembly = second_runtime
        .nodes()
        .register_internal_with_handle(
            REASSEMBLY_HANDLE,
            IpReassemblyNode::new(ip_reassembly_nexts(second_sink, second_sink)).with_handoff(
                IpReassemblyHandoff::new(
                    REASSEMBLY_HANDLE,
                    SINK_HANDLE,
                    DataWorkerId::new(1),
                    directory.clone(),
                ),
            ),
        )
        .expect("register second reassembly");
    let second_input = second_runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(ip_input_nexts(
            second_sink,
            second_reassembly,
        )));
    let original = ipv4_udp_packet(
        [10, 0, 0, 9],
        40_000,
        [198, 51, 100, 13],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 104, 16);
    let first_frame = first_runtime
        .alloc_frame_index()
        .expect("alloc first frame");
    let first_fragment = first_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &fragments[0])
        .expect("alloc first fragment");
    let first_fragment_ptr = first_runtime
        .current_ptr(first_fragment)
        .expect("first ptr") as usize;
    first_runtime
        .get_frame_mut(first_frame)
        .expect("mutate first frame")
        .push_index(first_fragment)
        .expect("push first fragment");
    let second_frame = second_runtime
        .alloc_frame_index()
        .expect("alloc second frame");
    let second_fragment = second_runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &fragments[1])
        .expect("alloc second fragment");
    second_runtime
        .get_frame_mut(second_frame)
        .expect("mutate second frame")
        .push_index(second_fragment)
        .expect("push second fragment");

    assert!(
        second_runtime
            .schedule_frame(second_input, second_frame)
            .expect("schedule second")
    );
    assert_eq!(second_runtime.run_ready_nodes().expect("run second"), 2);
    assert!(
        first_runtime
            .schedule_frame(first_input, first_frame)
            .expect("schedule first")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run first"), 2);
    assert_eq!(second_runtime.run_ready_nodes().expect("drain owner"), 1);
    assert_eq!(first_runtime.run_ready_nodes().expect("drain return"), 1);
    assert_eq!(
        &capture.packets(),
        &[ipv4_reassembled_packet(&original, 104)]
    );
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
    assert_ne!(first_fragment_ptr, 0);
}

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
) -> hammer_adapter::BufferIndex {
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
    index
}

fn push_marked_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    trace_input: hammer_adapter::NodeId,
    packet: &[u8],
) -> hammer_adapter::BufferIndex {
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), packet)
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_input, index)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
    index
}

fn push_packet_on_interface(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    interface_index: u32,
) -> hammer_adapter::BufferIndex {
    let index = runtime
        .alloc_index_with_bytes(
            RouteMetadata {
                ingress_interface: Some(interface_index),
                ..RouteMetadata::default()
            },
            packet,
        )
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
    index
}

fn ip_input_nexts(
    default: hammer_adapter::NodeId,
    reassembly: hammer_adapter::NodeId,
) -> [hammer_adapter::NodeId; IpInputNext::COUNT] {
    IpInputNext::nodes(
        default, default, default, default, default, default, reassembly,
    )
}

fn ip_reassembly_nexts(
    lookup: hammer_adapter::NodeId,
    drop: hammer_adapter::NodeId,
) -> [hammer_adapter::NodeId; IpReassemblyNext::COUNT] {
    IpReassemblyNext::nodes(lookup, drop)
}

fn assert_ipv4_time_exceeded(packet: &[u8], original: &[u8], local_source: [u8; 4]) {
    assert_eq!(packet[0], 0x45);
    assert_eq!(packet[8], 64);
    assert_eq!(packet[9], 1);
    assert_eq!(&packet[12..16], &local_source);
    assert_eq!(&packet[16..20], &original[12..16]);
    assert_eq!(packet[20], 11);
    assert_eq!(packet[21], 0);
    assert_eq!(&packet[24..28], &[0, 0, 0, 0]);
    assert_eq!(&packet[28..], original);
    assert_eq!(internet_checksum(&packet[..20]), 0);
    assert_eq!(internet_checksum(&packet[20..]), 0);
}

fn assert_ipv6_time_exceeded(packet: &[u8], original: &[u8], local_source: Ipv6Addr) {
    assert_eq!(packet[0], 0x60);
    assert_eq!(packet[6], 58);
    assert_eq!(packet[7], 64);
    assert_eq!(&packet[8..24], &local_source.octets());
    assert_eq!(&packet[24..40], &original[8..24]);
    assert_eq!(packet[40], 3);
    assert_eq!(packet[41], 0);
    assert_eq!(&packet[44..48], &[0, 0, 0, 0]);
    assert_eq!(&packet[48..], original);
    let destination = Ipv6Addr::from(<[u8; 16]>::try_from(&packet[24..40]).expect("destination"));
    assert_eq!(
        ipv6_l4_checksum(local_source, destination, 58, &packet[40..]),
        0
    );
}

fn ipv4_options_packet() -> Vec<u8> {
    let payload = b"option";
    let total_len = 24 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x46;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[198, 51, 100, 7]);
    packet[20..24].copy_from_slice(&[1, 1, 1, 0]);
    packet[24..].copy_from_slice(payload);
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_udp_packet(
    source: [u8; 4],
    source_port: u16,
    destination: [u8; 4],
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_protocol_packet(
    source: [u8; 4],
    destination: [u8; 4],
    protocol: u8,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = protocol;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..].copy_from_slice(payload);
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_with_ttl(mut packet: Vec<u8>, ttl: u8) -> Vec<u8> {
    packet[8] = ttl;
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_bad_checksum(mut packet: Vec<u8>) -> Vec<u8> {
    packet[10] ^= 0xff;
    packet
}

fn ipv4_fragment_offset_one(mut packet: Vec<u8>) -> Vec<u8> {
    packet[6..8].copy_from_slice(&1u16.to_be_bytes());
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_fragments(packet: &[u8], identification: u16, split: usize) -> Vec<Vec<u8>> {
    let payload_len = packet.len() - 20;
    vec![
        ipv4_fragment(packet, identification, 0, split, true),
        ipv4_fragment(packet, identification, split, payload_len - split, false),
    ]
}

fn ipv4_fragments_by_payload_lengths(
    packet: &[u8],
    identification: u16,
    lengths: &[usize],
) -> Vec<Vec<u8>> {
    let payload_len = packet.len() - 20;
    assert_eq!(lengths.iter().sum::<usize>(), payload_len);
    let mut offset = 0usize;
    let last = lengths.len().saturating_sub(1);
    lengths
        .iter()
        .enumerate()
        .map(|(index, len)| {
            let fragment = ipv4_fragment(packet, identification, offset, *len, index != last);
            offset += *len;
            fragment
        })
        .collect()
}

fn ipv4_reassembled_packet(packet: &[u8], identification: u16) -> Vec<u8> {
    let mut packet = packet.to_vec();
    packet[4..6].copy_from_slice(&identification.to_be_bytes());
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_fragment(
    packet: &[u8],
    identification: u16,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    assert_eq!(payload_offset % 8, 0);
    let mut fragment = Vec::with_capacity(20 + payload_len);
    fragment.extend_from_slice(&packet[..20]);
    fragment.extend_from_slice(&packet[20 + payload_offset..20 + payload_offset + payload_len]);
    let fragment_len = fragment.len() as u16;
    fragment[2..4].copy_from_slice(&fragment_len.to_be_bytes());
    fragment[4..6].copy_from_slice(&identification.to_be_bytes());
    let flags_offset = ((payload_offset / 8) as u16) | if more_fragments { 0x2000 } else { 0 };
    fragment[6..8].copy_from_slice(&flags_offset.to_be_bytes());
    update_ipv4_checksum(&mut fragment);
    fragment
}

fn ipv6_udp_packet(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let payload_len = 8 + payload.len();
    let mut packet = vec![0; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 17;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40..42].copy_from_slice(&source_port.to_be_bytes());
    packet[42..44].copy_from_slice(&destination_port.to_be_bytes());
    packet[44..46].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[48..].copy_from_slice(payload);
    packet
}

fn ipv6_protocol_packet(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    protocol: u8,
    hop_limit: u8,
    payload: &[u8],
) -> Vec<u8> {
    let payload_len = payload.len();
    let mut packet = vec![0; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = protocol;
    packet[7] = hop_limit;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40..].copy_from_slice(payload);
    packet
}

fn ipv6_fragments(packet: &[u8], identification: u32, split: usize) -> Vec<Vec<u8>> {
    let payload_len = packet.len() - 40;
    vec![
        ipv6_fragment(packet, identification, 0, split, true),
        ipv6_fragment(packet, identification, split, payload_len - split, false),
    ]
}

fn ipv6_fragment(
    packet: &[u8],
    identification: u32,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    assert_eq!(payload_offset % 8, 0);
    let mut fragment = Vec::with_capacity(40 + 8 + payload_len);
    fragment.extend_from_slice(&packet[..40]);
    fragment[6] = 44;
    fragment.extend_from_slice(&[
        packet[6],
        0,
        0,
        0,
        (identification >> 24) as u8,
        (identification >> 16) as u8,
        (identification >> 8) as u8,
        identification as u8,
    ]);
    let mut offset_more = ((payload_offset / 8) as u16) << 3;
    if more_fragments {
        offset_more |= 1;
    }
    fragment[42..44].copy_from_slice(&offset_more.to_be_bytes());
    fragment.extend_from_slice(&packet[40 + payload_offset..40 + payload_offset + payload_len]);
    let fragment_payload_len = fragment.len() - 40;
    fragment[4..6].copy_from_slice(&(fragment_payload_len as u16).to_be_bytes());
    fragment
}

fn update_ipv4_checksum(packet: &mut [u8]) {
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..ihl]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}
