use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, ForwardingMetadata, InternalNode, NetworkOpaque, Node,
    NodeProcessFn, NodeResult, NodeRuntimeData, SecondaryOpaque, TraceControlPlane,
    TraceInputPolicy, TracePolicy,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::forwarding::AdjacencyRewrite;
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{
    AdjacencyRewriteNode, AdjacencyRewriteTrace, Dpo, DpoId, DpoProto, DpoType, FibTableBuilder,
    IpInputNext, IpInputNode, IpLocalControlPlane, IpLocalNext, IpLookupControlPlane,
    IpLookupTrace, IpUnicastArc,
};
use ipnet::{Ipv4Net, Ipv6Net};
use std::mem::transmute;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LookupTestOpaque {
    _tap_ethernet: Option<hammer_adapter::TapEthernetMetadata>,
    _icmp_error: Option<IcmpErrorMetadata>,
    forwarding: Option<ForwardingMetadata>,
}

const _: () =
    assert!(core::mem::size_of::<LookupTestOpaque>() == core::mem::size_of::<SecondaryOpaque>());

#[derive(Default)]
struct SinkState {
    payloads: Vec<hammer_infra::vec::Vec<u8>>,
    forwarding: Vec<Option<ForwardingMetadata>>,
    egress_interfaces: Vec<Option<u32>>,
    frame_lens: Vec<usize>,
}

struct SinkNode {
    runtime_data: NodeRuntimeData,
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

struct CorruptCurrentHeaderNode {
    runtime_data: NodeRuntimeData,
}

impl CorruptCurrentHeaderNode {
    fn new(next: hammer_adapter::NodeId) -> Self {
        Self {
            runtime_data: NodeRuntimeData::from_words([u64::from(next.slot()), 0, 0, 0]),
        }
    }
}

impl Node for CorruptCurrentHeaderNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "corrupt current header node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        corrupt_current_header_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CorruptCurrentHeaderNode {}

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
    state
        .lock()
        .map_err(|_| CoreError::internal("sink state poisoned"))?
        .frame_lens
        .push(frame.pending_len());
    for buffer in frame.drain_pending() {
        let payload = runtime.copy_packet(buffer)?;
        let (egress_interface, forwarding) = {
            let buffer_ref = runtime.get_buffer(buffer)?;
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer_ref.opaque()) };
            let opaque = unsafe { transmute::<_, &LookupTestOpaque>(buffer_ref.opaque2()) };
            (
                Some(network.sw_if_index[1]).filter(|value| *value != 0),
                opaque.forwarding,
            )
        };
        runtime.free_index(buffer);
        let mut state = state
            .lock()
            .map_err(|_| CoreError::internal("sink state poisoned"))?;
        state.forwarding.push(forwarding);
        state.egress_interfaces.push(egress_interface);
        state.payloads.push(payload);
    }
    Ok(NodeResult::drop())
}

fn corrupt_current_header_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    for index in frame.pending_indices().iter().copied() {
        runtime.get_buffer_mut(index)?.current_mut()[0] = 0;
    }
    let slot = u32::try_from(data.word(0))
        .map_err(|_| CoreError::internal("corrupt next node id overflow"))?;
    Ok(NodeResult::next_current(hammer_adapter::NodeId::new(slot)))
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode,
{
    let _ = node;
}

#[test]
fn ip_lookup_node_uses_ipv4_mtrie_longest_prefix_match() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let default_state = Arc::new(Mutex::new(SinkState::default()));
    let specific_state = Arc::new(Mutex::new(SinkState::default()));
    let host_state = Arc::new(Mutex::new(SinkState::default()));
    let default = register_sink(&runtime, &default_state);
    let specific = register_sink(&runtime, &specific_state);
    let host = register_sink(&runtime, &host_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut builder = FibTableBuilder::new(drop);
    let default_lb = add_single_path(&mut builder, DpoProto::IP4, default);
    let specific_lb = add_single_path(&mut builder, DpoProto::IP4, specific);
    let host_lb = add_single_path(&mut builder, DpoProto::IP4, host);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        default_lb,
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("specific route"),
        specific_lb,
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 42), 32).expect("host route"),
        host_lb,
    );
    let control = IpLookupControlPlane::new(builder.build());
    let lookup = control.node();
    assert_internal_node(&lookup);
    let lookup = runtime.nodes().register_internal(lookup);
    let trace = TraceControlPlane::new(8);
    trace.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: lookup,
            count: 1,
        }]
        .into(),
    });
    runtime.set_trace_control(Some(trace.handle()), 4);

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_marked_packet(
        &runtime,
        frame,
        lookup,
        &ipv4_udp_packet([10, 0, 0, 1], 10_001, [203, 0, 113, 7], 53, b"default"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_002, [198, 51, 100, 7], 53, b"specific"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_003, [198, 51, 100, 42], 53, b"host"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_payloads(&default_state, &[b"default".as_slice()]);
    assert_payloads(&specific_state, &[b"specific".as_slice()]);
    assert_payloads(&host_state, &[b"host".as_slice()]);
    assert_frame_lens(&default_state, &[1]);
    assert_frame_lens(&specific_state, &[1]);
    assert_frame_lens(&host_state, &[1]);
    assert_forwarding(&host_state, host_lb.get());
    let default_forwarding =
        default_state.lock().unwrap().forwarding[0].expect("default forwarding");
    assert_eq!(trace.drain_completed(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, lookup);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].node_name, Some("ip-lookup"));
    assert_eq!(
        IpLookupTrace::decode(&records[0].entries[0].payload_bytes).expect("ip lookup trace"),
        IpLookupTrace {
            fib_index: 0,
            route_dpo_type: Some(default_forwarding.route_dpo_type),
            route_dpo_index: Some(default_forwarding.route_dpo_index),
            load_balance_index: Some(default_forwarding.load_balance_index),
            bucket_index: Some(default_forwarding.bucket_index),
            dpo_type: Some(default_forwarding.dpo_type),
            dpo_index: Some(default_forwarding.dpo_index),
            next: default,
        }
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_vector_enqueue_batches_same_next_in_one_output_frame() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop_node);
    let lb = add_single_path(&mut builder, DpoProto::IP4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_011, [203, 0, 113, 1], 53, b"one"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_012, [203, 0, 113, 2], 53, b"two"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_013, [203, 0, 113, 3], 53, b"three"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(
        &state,
        &[b"one".as_slice(), b"two".as_slice(), b"three".as_slice()],
    );
    assert_frame_lens(&state, &[3]);
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_vector_enqueue_reuses_current_frame_for_same_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 1);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop_node);
    let lb = add_single_path(&mut builder, DpoProto::IP4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_021, [203, 0, 113, 1], 53, b"one"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_022, [203, 0, 113, 2], 53, b"two"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(&state, &[b"one".as_slice(), b"two".as_slice()]);
    assert_frame_lens(&state, &[2]);
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_uses_ipv6_hash_prefix_order() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let default_state = Arc::new(Mutex::new(SinkState::default()));
    let subnet_state = Arc::new(Mutex::new(SinkState::default()));
    let host_state = Arc::new(Mutex::new(SinkState::default()));
    let default = register_sink(&runtime, &default_state);
    let subnet = register_sink(&runtime, &subnet_state);
    let host = register_sink(&runtime, &host_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut builder = FibTableBuilder::new(drop);
    let default_lb = add_single_path(&mut builder, DpoProto::IP6, default);
    let subnet_lb = add_single_path(&mut builder, DpoProto::IP6, subnet);
    let host_lb = add_single_path(&mut builder, DpoProto::IP6, host);
    builder.add_ip6_route(
        Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).expect("default route"),
        default_lb,
    );
    builder.add_ip6_route(
        Ipv6Net::new("2001:db8:64::".parse().expect("subnet"), 64).expect("subnet route"),
        subnet_lb,
    );
    builder.add_ip6_route(
        Ipv6Net::new("2001:db8:64::42".parse().expect("host"), 128).expect("host route"),
        host_lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_001, "2001:db8:ffff::1", 53, b"default"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_002, "2001:db8:64::7", 53, b"subnet"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_003, "2001:db8:64::42", 53, b"host"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_payloads(&default_state, &[b"default".as_slice()]);
    assert_payloads(&subnet_state, &[b"subnet".as_slice()]);
    assert_payloads(&host_state, &[b"host".as_slice()]);
    assert_forwarding(&host_state, host_lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_sends_miss_to_drop_dpo() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(FibTableBuilder::new(drop).build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_001, [198, 51, 100, 7], 53, b"miss"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_routes_receive_dpo_to_local_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let drop = runtime.nodes().register_internal(DropNode::new());
    let udp = register_sink(&runtime, &state);
    let local_control =
        IpLocalControlPlane::new(IpLocalNext::nodes(drop, drop, drop, udp, drop, drop));
    runtime.nodes().register_internal(local_control.node());
    let receive = runtime
        .nodes()
        .register_internal(local_control.receive_node());
    let mut builder = FibTableBuilder::new(drop);
    let receive_lb =
        builder.add_load_balance(DpoProto::IP4, [DpoId::receive(DpoProto::IP4, receive)]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 10), 32).expect("receive route"),
        receive_lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_011, [192, 0, 2, 10], 53, b"receive"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_payloads(&state, &[b"receive".as_slice()]);
    let forwarding = state.lock().unwrap().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.route_dpo_type, DpoType::LOAD_BALANCE);
    assert_eq!(forwarding.route_dpo_index, receive_lb.get());
    assert_eq!(forwarding.dpo_type, DpoType::RECEIVE);
    assert_eq!(forwarding.dpo_index, 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_routes_direct_receive_dpo_to_local_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let drop = runtime.nodes().register_internal(DropNode::new());
    let udp = register_sink(&runtime, &state);
    let local_control =
        IpLocalControlPlane::new(IpLocalNext::nodes(drop, drop, drop, udp, drop, drop));
    runtime.nodes().register_internal(local_control.node());
    let receive = runtime
        .nodes()
        .register_internal(local_control.receive_node());
    let mut builder = FibTableBuilder::new(drop);
    builder.add_ip4_route_dpo(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 11), 32).expect("direct receive route"),
        DpoId::receive(DpoProto::IP4, receive),
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_012, [192, 0, 2, 11], 53, b"direct"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_payloads(&state, &[b"direct".as_slice()]);
    let forwarding = state.lock().unwrap().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.load_balance_index, u32::MAX);
    assert_eq!(forwarding.bucket_index, u16::MAX);
    assert_eq!(forwarding.route_dpo_type, DpoType::RECEIVE);
    assert_eq!(forwarding.route_dpo_index, 0);
    assert_eq!(forwarding.dpo_type, DpoType::RECEIVE);
    assert_eq!(forwarding.dpo_index, 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_routes_stacked_dpo_with_parent_identity() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let receive = register_sink(&runtime, &state);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop_node);
    let parent = Dpo::receive(DpoProto::IP4, drop_node);
    let stacked = Dpo::stack(parent, receive);
    let stack_lb = builder.add_load_balance(DpoProto::IP4, [stacked]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 20), 32).expect("stack route"),
        stack_lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_012, [192, 0, 2, 20], 53, b"stack"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(&state, &[b"stack".as_slice()]);
    let forwarding = state.lock().unwrap().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.dpo_type, DpoType::RECEIVE);
    assert_eq!(forwarding.dpo_index, 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_routes_custom_dpo_to_custom_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let custom = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let custom_type = DpoType::new(7);
    let custom_index = 11;
    let mut builder = FibTableBuilder::new(drop);
    let custom_lb = builder.add_load_balance(
        DpoProto::IP4,
        [Dpo::new(DpoProto::IP4, custom_type, custom_index, custom)],
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 30), 32).expect("custom route"),
        custom_lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_013, [192, 0, 2, 30], 53, b"custom"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(&state, &[b"custom".as_slice()]);
    let forwarding = state.lock().unwrap().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.dpo_type, custom_type);
    assert_eq!(forwarding.dpo_index, custom_index);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn adjacency_rewrite_node_prepends_rewrite_and_sets_egress_interface() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop_node);
    let rewrite = [0xaa, 0xbb, 0xcc, 0xdd];
    let dpo = builder.add_interface_adjacency_dpo(
        DpoProto::IP4,
        12,
        AdjacencyRewrite::try_new(&rewrite).expect("rewrite fits"),
        drop_node,
        sink,
    );
    let adjacency = dpo.adjacency_index().expect("adjacency index");
    let control = IpLookupControlPlane::new(builder.build());
    let rewrite_node = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(control.table_handle()));
    let trace = TraceControlPlane::new(8);
    trace.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: rewrite_node,
            count: 1,
        }]
        .into(),
    });
    runtime.set_trace_control(Some(trace.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_udp_packet([10, 0, 0, 1], 30_014, [192, 0, 2, 30], 53, b"rewrite");
    let index = alloc_packet_with_headroom(&runtime, &packet);
    runtime
        .try_mark_trace(rewrite_node, index)
        .expect("mark packet");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
        let opaque = unsafe { transmute::<_, &mut LookupTestOpaque>(buffer.opaque2_mut()) };
        opaque.forwarding = Some(ForwardingMetadata {
            fib_index: 0,
            route_dpo_type: DpoType::ADJACENCY,
            route_dpo_index: adjacency.get(),
            load_balance_index: u32::MAX,
            bucket_index: u16::MAX,
            dpo_type: DpoType::ADJACENCY,
            dpo_index: adjacency.get(),
        });
        buffer.set_packet_cursor(
            hammer_adapter::BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, 20)
                .with_transport_header(20, 8)
                .with_transport_payload_offset(28),
        );
    }
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(rewrite_node, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    let state_ref = state.lock().unwrap();
    assert_eq!(state_ref.payloads.len(), 1);
    assert_eq!(&state_ref.payloads[0][..rewrite.len()], &rewrite);
    assert_eq!(&state_ref.payloads[0][rewrite.len()..], packet.as_slice());
    let metadata = state_ref.forwarding[0].expect("forwarding metadata");
    assert_eq!(metadata.dpo_type, DpoType::ADJACENCY);
    assert_eq!(metadata.dpo_index, adjacency.get());
    assert_eq!(state_ref.egress_interfaces[0], Some(12));
    std::mem::drop(state_ref);
    assert_eq!(trace.drain_completed(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, rewrite_node);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].node_name, Some("adjacency-rewrite"));
    assert_eq!(
        AdjacencyRewriteTrace::decode(&records[0].entries[0].payload_bytes)
            .expect("adjacency rewrite trace"),
        AdjacencyRewriteTrace {
            dpo_index: Some(adjacency.get()),
            egress_interface: Some(12),
            rewrite_len: rewrite.len(),
            error: None,
            next: Some(sink),
        }
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn adjacency_rewrite_node_prepends_rewrite_when_packet_has_headroom() {
    let runtime = DataPlaneRuntime::with_capacities(256, 16, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop_node = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop_node);
    let rewrite = [0xaa, 0xbb, 0xcc, 0xdd];
    let dpo = builder.add_interface_adjacency_dpo(
        DpoProto::IP4,
        9,
        AdjacencyRewrite::try_new(&rewrite).expect("rewrite fits"),
        drop_node,
        sink,
    );
    let adjacency = dpo.adjacency_index().expect("adjacency index");
    let control = IpLookupControlPlane::new(builder.build());
    let rewrite_node = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(control.table_handle()));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_udp_packet(
        [10, 0, 0, 1],
        30_114,
        [192, 0, 2, 130],
        53,
        b"rewrite-chained-payload",
    );
    let index = alloc_packet_with_headroom(&runtime, &packet);
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
        let opaque = unsafe { transmute::<_, &mut LookupTestOpaque>(buffer.opaque2_mut()) };
        opaque.forwarding = Some(ForwardingMetadata {
            fib_index: 0,
            route_dpo_type: DpoType::ADJACENCY,
            route_dpo_index: adjacency.get(),
            load_balance_index: u32::MAX,
            bucket_index: u16::MAX,
            dpo_type: DpoType::ADJACENCY,
            dpo_index: adjacency.get(),
        });
        buffer.set_packet_cursor(
            hammer_adapter::BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, 20)
                .with_transport_header(20, 8)
                .with_transport_payload_offset(28),
        );
    }
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(rewrite_node, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    let state_ref = state.lock().unwrap();
    assert_eq!(state_ref.payloads.len(), 1);
    assert_eq!(&state_ref.payloads[0][..rewrite.len()], &rewrite);
    assert_eq!(&state_ref.payloads[0][rewrite.len()..], packet.as_slice());
    assert_eq!(state_ref.egress_interfaces[0], Some(9));
    std::mem::drop(state_ref);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn adjacency_rewrite_node_drops_missing_forwarding_and_missing_adjacency() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let control = IpLookupControlPlane::new(FibTableBuilder::new(drop).build());
    let rewrite_node = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(control.table_handle()));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_015, [192, 0, 2, 31], 53, b"missing-meta"),
    );
    let missing_adjacency = runtime
        .alloc_index_with_bytes(&ipv4_udp_packet(
            [10, 0, 0, 1],
            30_016,
            [192, 0, 2, 32],
            53,
            b"missing-adj",
        ))
        .expect("alloc missing adjacency");
    {
        let mut buffer = runtime
            .get_buffer_mut(missing_adjacency)
            .expect("missing adjacency");
        let opaque = unsafe { transmute::<_, &mut LookupTestOpaque>(buffer.opaque2_mut()) };
        opaque.forwarding = Some(ForwardingMetadata {
            fib_index: 0,
            route_dpo_type: DpoType::ADJACENCY,
            route_dpo_index: 99,
            load_balance_index: u32::MAX,
            bucket_index: u16::MAX,
            dpo_type: DpoType::ADJACENCY,
            dpo_index: 99,
        });
    }
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(missing_adjacency)
        .expect("push missing adjacency");

    assert!(
        runtime
            .schedule_frame(rewrite_node, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_control_plane_publish_replaces_forwarding_table() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let first_state = Arc::new(Mutex::new(SinkState::default()));
    let second_state = Arc::new(Mutex::new(SinkState::default()));
    let first = register_sink(&runtime, &first_state);
    let second = register_sink(&runtime, &second_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut first_builder = FibTableBuilder::new(drop);
    let first_lb = add_single_path(&mut first_builder, DpoProto::IP4, first);
    first_builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("first default"),
        first_lb,
    );
    let control = IpLookupControlPlane::new(first_builder.build());
    let lookup = runtime.nodes().register_internal(control.node());

    let first_frame = runtime.alloc_frame_index().expect("alloc first frame");
    push_packet(
        &runtime,
        first_frame,
        &ipv4_udp_packet([10, 0, 0, 1], 50_001, [203, 0, 113, 1], 53, b"first"),
    );
    assert!(
        runtime
            .schedule_frame(lookup, first_frame)
            .expect("schedule")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run first"), 2);
    assert_payloads(&first_state, &[b"first".as_slice()]);

    let mut second_builder = FibTableBuilder::new(drop);
    let second_lb = add_single_path(&mut second_builder, DpoProto::IP4, second);
    second_builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("second default"),
        second_lb,
    );
    control.publish(second_builder.build()).expect("publish");

    let second_frame = runtime.alloc_frame_index().expect("alloc second frame");
    push_packet(
        &runtime,
        second_frame,
        &ipv4_udp_packet([10, 0, 0, 1], 50_002, [203, 0, 113, 2], 53, b"second"),
    );
    assert!(
        runtime
            .schedule_frame(lookup, second_frame)
            .expect("schedule")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run second"), 2);
    assert_payloads(&second_state, &[b"second".as_slice()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_control_plane_publish_runs_through_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "ip-lookup-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let control =
        IpLookupControlPlane::new(FibTableBuilder::new(drop).build()).with_barrier(barrier.clone());

    control
        .publish(FibTableBuilder::new(drop).build())
        .expect("publish");

    assert_eq!(barrier.sync_count(), 1);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn ip_input_to_lookup_graph_routes_packet_by_fib() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop);
    let lb = add_single_path(&mut builder, DpoProto::IP4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, drop, drop, drop,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 40_001, [198, 51, 100, 17], 853, b"graph"),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_payloads(&state, &[b"graph".as_slice()]);
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_input_batches_lookup_packets_into_one_scheduled_next_frame() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, sink, drop, drop, drop,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 41_001, [198, 51, 100, 17], 853, b"one"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 41_002, [198, 51, 100, 18], 853, b"two"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 41_003, [198, 51, 100, 19], 853, b"three"),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(
        &state,
        &[b"one".as_slice(), b"two".as_slice(), b"three".as_slice()],
    );
    assert_frame_lens(&state, &[3]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_uses_ip_input_cursor_without_reparsing_current_header() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let state = Arc::new(Mutex::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibTableBuilder::new(drop);
    let lb = add_single_path(&mut builder, DpoProto::IP4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let corrupt = runtime
        .nodes()
        .register_internal(CorruptCurrentHeaderNode::new(lookup));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, corrupt, drop, drop, drop,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 40_011, [198, 51, 100, 17], 853, b"cursor"),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    {
        let state = state.lock().unwrap();
        assert_eq!(state.payloads.len(), 1);
        assert_eq!(&state.payloads[0][28..], b"cursor");
    }
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn register_sink(
    runtime: &DataPlaneRuntime,
    state: &Arc<Mutex<SinkState>>,
) -> hammer_adapter::NodeId {
    runtime
        .nodes()
        .register_internal(SinkNode::new(Arc::clone(state)))
}

fn add_single_path(
    builder: &mut FibTableBuilder,
    proto: DpoProto,
    node: hammer_adapter::NodeId,
) -> hammer_service::net::LoadBalanceIndex {
    builder.add_single_path_load_balance(proto, node)
}

fn push_packet(runtime: &DataPlaneRuntime, frame: hammer_adapter::FrameIndex, packet: &[u8]) {
    let index = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
}

fn alloc_packet_with_headroom(
    runtime: &DataPlaneRuntime,
    packet: &[u8],
) -> hammer_adapter::BufferIndex {
    let index = runtime.alloc_index().expect("alloc packet with headroom");
    runtime.append(index, packet).expect("append packet bytes");
    index
}

fn push_marked_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    trace_input: hammer_adapter::NodeId,
    packet: &[u8],
) {
    let index = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_input, index)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
}

fn assert_payloads(state: &Arc<Mutex<SinkState>>, expected_payloads: &[&[u8]]) {
    let payloads = state
        .lock()
        .unwrap()
        .payloads
        .iter()
        .map(|packet| udp_payload(packet).to_vec())
        .collect::<Vec<_>>();
    let expected = expected_payloads
        .iter()
        .map(|payload| payload.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(payloads, expected);
}

fn assert_frame_lens(state: &Arc<Mutex<SinkState>>, expected_lens: &[usize]) {
    assert_eq!(&*state.lock().unwrap().frame_lens, expected_lens);
}

fn assert_forwarding(state: &Arc<Mutex<SinkState>>, load_balance_index: u32) {
    let forwarding = state.lock().unwrap().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.load_balance_index, load_balance_index);
    assert_eq!(forwarding.dpo_type, DpoType::ADJACENCY);
}

fn udp_payload(packet: &[u8]) -> &[u8] {
    match packet[0] >> 4 {
        4 => {
            let ihl = ((packet[0] & 0x0f) as usize) * 4;
            &packet[ihl + 8..]
        }
        6 => &packet[40 + 8..],
        _ => panic!("test packet is not IP"),
    }
}

fn ipv4_udp_packet(
    source: [u8; 4],
    source_port: u16,
    destination: [u8; 4],
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    let checksum = ipv4_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    packet
}

fn ipv6_udp_packet(
    source: &str,
    source_port: u16,
    destination: &str,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let source = source.parse::<Ipv6Addr>().expect("source addr");
    let destination = destination.parse::<Ipv6Addr>().expect("destination addr");
    let payload_len = 8 + payload.len();
    let mut packet = vec![0u8; 40 + payload_len];
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

fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
