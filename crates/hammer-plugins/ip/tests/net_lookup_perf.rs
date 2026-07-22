use std::hint::black_box;
use std::mem::transmute;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, SecondaryOpaque,
};
use hammer_plugin_ip::forwarding::{DpoProto, FibTableBuilder, ForwardingMetadata};
use hammer_plugin_ip::{
    IpInputNext, IpInputNode, IpLookupControlPlane, IpLookupNext, IpUnicastArc,
};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneInstructionSet, DataPlaneRuntime, DataPlaneRuntimeConfig,
    InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_service::data_plane::DropNode;
use hammer_service::opaque::{NetworkOpaque, TapEthernetMetadata};
use ipnet::{Ipv4Net, Ipv6Net};

const FRAME_PACKETS: usize = 128;
const FRAME_ROUNDS: usize = 512;
const SAMPLE_COUNT: usize = 5;
static PERF_PROBE_LOCK: Mutex<()> = Mutex::new(());

fn test_runtime_configured_instruction_set(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_slots: usize,
    instruction_set: DataPlaneInstructionSet,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new_with_instruction_set(config, instruction_set)
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LookupPerfOpaque {
    tap_ethernet: Option<TapEthernetMetadata>,
    icmp_error: Option<hammer_plugin_ip::protocol::icmp::IcmpErrorMetadata>,
    forwarding: Option<ForwardingMetadata>,
}

const _: () =
    assert!(core::mem::size_of::<LookupPerfOpaque>() == core::mem::size_of::<SecondaryOpaque>());

struct SinkNode {
    runtime_data: NodeRuntimeData,
}

#[derive(Default)]
struct SinkCounters {
    packets: AtomicUsize,
    checksum: AtomicU64,
}

impl SinkNode {
    fn new(counters: Arc<SinkCounters>) -> Self {
        let mut states = sink_states().lock().expect("sink state registry poisoned");
        let slot = states.len();
        states.push(counters);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("sink state slot"),
        }
    }
}

impl Node for SinkNode {
    #[inline(always)]
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        sink_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for SinkNode {}

fn sink_states() -> &'static Mutex<Vec<Arc<SinkCounters>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<SinkCounters>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn sink_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let counters = {
        let states = sink_states().lock().expect("sink state registry poisoned");
        Arc::clone(
            states
                .get(data.usize_word(0).expect("usize word 0"))
                .expect("sink state slot is invalid"),
        )
    };
    let mut packets = 0usize;
    let mut checksum = 0u64;
    for index in frame.pending_indices().iter().copied() {
        let buffer = runtime.get_buffer(index).expect("get buffer");
        let opaque = unsafe { transmute::<_, &LookupPerfOpaque>(buffer.opaque2()) };
        if let Some(forwarding) = opaque.forwarding {
            checksum = checksum.wrapping_add(u64::from(forwarding.load_balance_index));
            checksum = checksum.wrapping_add(u64::from(forwarding.bucket_index));
        }
        packets += 1;
    }
    counters.packets.fetch_add(packets, Ordering::Relaxed);
    counters
        .checksum
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.wrapping_add(checksum))
        })
        .ok();
    NodeResult::drop()
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Ipv4SameNext,
    Ipv4MixedNext,
    Ipv6SameNext,
}

#[test]
#[ignore = "performance probe; run with `make verify-dataplane-performance`"]
fn ip_lookup_frame_batch_perf_probe() {
    let performance_test_guard = PERF_PROBE_LOCK.lock().expect("performance test lock");
    let scenarios = [
        Scenario::Ipv4SameNext,
        Scenario::Ipv4MixedNext,
        Scenario::Ipv6SameNext,
    ];
    for scenario in scenarios {
        let packet_scalar = measure_packet_scalar_samples(scenario);
        let frame_pair = measure_lookup_samples(scenario, DataPlaneInstructionSet::Scalar);
        let frame_native = measure_lookup_samples(scenario, DataPlaneInstructionSet::native());
        assert_eq!(packet_scalar.best.packets, FRAME_PACKETS * FRAME_ROUNDS);
        assert_eq!(frame_pair.best.packets, packet_scalar.best.packets);
        assert_eq!(frame_native.best.packets, packet_scalar.best.packets);
        assert_eq!(frame_pair.best.checksum, packet_scalar.best.checksum);
        assert_eq!(frame_native.best.checksum, packet_scalar.best.checksum);

        eprintln!(
            "{scenario:?}: samples={SAMPLE_COUNT} rounds={FRAME_ROUNDS} frame_packets={FRAME_PACKETS} packet_scalar_best={:.2} packet_scalar_median={:.2} ns/packet frame_pair_best={:.2} frame_pair_median={:.2} ns/packet frame_native({:?})_best={:.2} frame_native_median={:.2} ns/packet vector_vs_packet_best_ratio={:.3} native_vs_pair_best_ratio={:.3} checksum={} / {} / {}",
            packet_scalar.best.ns_per_packet(),
            packet_scalar.median.ns_per_packet(),
            frame_pair.best.ns_per_packet(),
            frame_pair.median.ns_per_packet(),
            DataPlaneInstructionSet::native(),
            frame_native.best.ns_per_packet(),
            frame_native.median.ns_per_packet(),
            frame_native.best.ns_per_packet() / packet_scalar.best.ns_per_packet(),
            frame_native.best.ns_per_packet() / frame_pair.best.ns_per_packet(),
            packet_scalar.best.checksum,
            frame_pair.best.checksum,
            frame_native.best.checksum,
        );
    }
    drop(performance_test_guard);
}

#[test]
#[ignore = "performance probe; run with `make verify-dataplane-performance`"]
fn ip_input_lookup_frame_batch_perf_probe() {
    let performance_test_guard = PERF_PROBE_LOCK.lock().expect("performance test lock");
    let scenarios = [
        Scenario::Ipv4SameNext,
        Scenario::Ipv4MixedNext,
        Scenario::Ipv6SameNext,
    ];
    for scenario in scenarios {
        let pipeline_pair = measure_input_lookup_samples(scenario, DataPlaneInstructionSet::Scalar);
        let pipeline_native =
            measure_input_lookup_samples(scenario, DataPlaneInstructionSet::native());
        assert_eq!(pipeline_pair.best.packets, FRAME_PACKETS * FRAME_ROUNDS);
        assert_eq!(pipeline_native.best.packets, pipeline_pair.best.packets);
        assert_eq!(pipeline_native.best.checksum, pipeline_pair.best.checksum);

        eprintln!(
            "InputLookup::{scenario:?}: samples={SAMPLE_COUNT} rounds={FRAME_ROUNDS} frame_packets={FRAME_PACKETS} pipeline_pair_best={:.2} pipeline_pair_median={:.2} ns/packet pipeline_native({:?})_best={:.2} pipeline_native_median={:.2} ns/packet native_vs_pair_best_ratio={:.3} checksum={} / {}",
            pipeline_pair.best.ns_per_packet(),
            pipeline_pair.median.ns_per_packet(),
            DataPlaneInstructionSet::native(),
            pipeline_native.best.ns_per_packet(),
            pipeline_native.median.ns_per_packet(),
            pipeline_native.best.ns_per_packet() / pipeline_pair.best.ns_per_packet(),
            pipeline_pair.best.checksum,
            pipeline_native.best.checksum,
        );
    }
    drop(performance_test_guard);
}

fn measure_packet_scalar_samples(scenario: Scenario) -> ProbeSummary {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(measure_packet_scalar(scenario));
    }
    samples.sort_by(|left, right| {
        left.ns_per_packet()
            .partial_cmp(&right.ns_per_packet())
            .expect("finite ns/packet")
    });
    ProbeSummary {
        best: samples[0],
        median: samples[SAMPLE_COUNT / 2],
    }
}

fn measure_lookup_samples(
    scenario: Scenario,
    instruction_set: DataPlaneInstructionSet,
) -> ProbeSummary {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(measure_lookup(scenario, instruction_set));
    }
    samples.sort_by(|left, right| {
        left.ns_per_packet()
            .partial_cmp(&right.ns_per_packet())
            .expect("finite ns/packet")
    });
    ProbeSummary {
        best: samples[0],
        median: samples[SAMPLE_COUNT / 2],
    }
}

fn measure_input_lookup_samples(
    scenario: Scenario,
    instruction_set: DataPlaneInstructionSet,
) -> ProbeSummary {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(measure_input_lookup(scenario, instruction_set));
    }
    samples.sort_by(|left, right| {
        left.ns_per_packet()
            .partial_cmp(&right.ns_per_packet())
            .expect("finite ns/packet")
    });
    ProbeSummary {
        best: samples[0],
        median: samples[SAMPLE_COUNT / 2],
    }
}

fn measure_packet_scalar(scenario: Scenario) -> ProbeStats {
    let runtime =
        test_runtime_configured_instruction_set(2048, 1, 2, DataPlaneInstructionSet::Scalar);
    let counters = Arc::new(SinkCounters::default());
    let lookup = build_lookup(&runtime, scenario, &counters);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        for packet in packets_by_frame.iter() {
            let mut frame = runtime
                .buffers()
                .get_next_frame(lookup)
                .expect("alloc frame");
            let index = allocate_lookup_packet(&runtime, packet);
            frame.push_index(index).expect("push packet");
            runtime.put_next_frame(frame).expect("schedule");
            black_box(runtime.run_ready_nodes().expect("run nodes"));
            debug_assert_eq!(runtime.in_use_buffers(), 0);
        }
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(counters.packets.load(Ordering::Relaxed)),
        checksum: black_box(counters.checksum.load(Ordering::Relaxed)),
    }
}

fn measure_lookup(scenario: Scenario, instruction_set: DataPlaneInstructionSet) -> ProbeStats {
    let runtime = test_runtime_configured_instruction_set(2048, FRAME_PACKETS, 32, instruction_set);
    let counters = Arc::new(SinkCounters::default());
    let lookup = build_lookup(&runtime, scenario, &counters);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        let mut frame = runtime
            .buffers()
            .get_next_frame(lookup)
            .expect("alloc frame");
        for packet in packets_by_frame.iter() {
            let index = allocate_lookup_packet(&runtime, packet);
            frame.push_index(index).expect("push packet");
        }
        runtime.put_next_frame(frame).expect("schedule");
        black_box(runtime.run_ready_nodes().expect("run nodes"));
        debug_assert_eq!(runtime.in_use_buffers(), 0);
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(counters.packets.load(Ordering::Relaxed)),
        checksum: black_box(counters.checksum.load(Ordering::Relaxed)),
    }
}

fn measure_input_lookup(
    scenario: Scenario,
    instruction_set: DataPlaneInstructionSet,
) -> ProbeStats {
    let runtime = test_runtime_configured_instruction_set(2048, FRAME_PACKETS, 32, instruction_set);
    let counters = Arc::new(SinkCounters::default());
    let input = build_input_lookup(&runtime, scenario, &counters);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        let mut frame = runtime
            .buffers()
            .get_next_frame(input)
            .expect("alloc frame");
        for packet in packets_by_frame.iter() {
            let index = runtime
                .alloc_index_with_bytes(packet)
                .expect("alloc packet");
            frame.push_index(index).expect("push packet");
        }
        runtime.put_next_frame(frame).expect("schedule");
        black_box(runtime.run_ready_nodes().expect("run nodes"));
        debug_assert_eq!(runtime.in_use_buffers(), 0);
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(counters.packets.load(Ordering::Relaxed)),
        checksum: black_box(counters.checksum.load(Ordering::Relaxed)),
    }
}

fn build_lookup(
    runtime: &DataPlaneRuntime,
    scenario: Scenario,
    counters: &Arc<SinkCounters>,
) -> hammer_core::data_plane::NodeId {
    let drop = runtime.nodes().register_internal(DropNode::new());
    build_lookup_with_drop(runtime, scenario, counters, drop)
}

fn build_lookup_with_drop(
    runtime: &DataPlaneRuntime,
    scenario: Scenario,
    counters: &Arc<SinkCounters>,
    drop: hammer_core::data_plane::NodeId,
) -> hammer_core::data_plane::NodeId {
    let control = IpLookupControlPlane::new(FibTableBuilder::new(u16::MAX).build());
    let lookup = runtime
        .nodes()
        .register_internal(control.node(IpLookupNext::nodes(drop)));
    let drop_slot = runtime
        .nodes()
        .add_node_next_slot(lookup, drop)
        .expect("drop next");
    let mut builder = FibTableBuilder::new(drop_slot);
    match scenario {
        Scenario::Ipv4SameNext => {
            let sink = register_sink(runtime, counters);
            let sink_slot = runtime
                .nodes()
                .add_node_next_slot(lookup, sink)
                .expect("sink next");
            let lb = add_single_path(&mut builder, DpoProto::IP4, sink_slot);
            builder.add_ip4_route(
                Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
                lb,
            );
        }
        Scenario::Ipv4MixedNext => {
            for route in 1..=4 {
                let sink = register_sink(runtime, counters);
                let sink_slot = runtime
                    .nodes()
                    .add_node_next_slot(lookup, sink)
                    .expect("sink next");
                let lb = add_single_path(&mut builder, DpoProto::IP4, sink_slot);
                builder.add_ip4_route(
                    Ipv4Net::new(Ipv4Addr::new(198, 51, route, 0), 24).expect("mixed route"),
                    lb,
                );
            }
        }
        Scenario::Ipv6SameNext => {
            let sink = register_sink(runtime, counters);
            let sink_slot = runtime
                .nodes()
                .add_node_next_slot(lookup, sink)
                .expect("sink next");
            let lb = add_single_path(&mut builder, DpoProto::IP6, sink_slot);
            builder.add_ip6_route(
                Ipv6Net::new(Ipv6Addr::new(0x2001, 0x0db8, 0x0064, 0, 0, 0, 0, 0), 64)
                    .expect("ipv6 route"),
                lb,
            );
        }
    }
    control.publish(builder.build()).expect("publish fib");
    lookup
}

fn build_input_lookup(
    runtime: &DataPlaneRuntime,
    scenario: Scenario,
    counters: &Arc<SinkCounters>,
) -> hammer_core::data_plane::NodeId {
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = build_lookup_with_drop(runtime, scenario, counters, drop);
    runtime
        .nodes()
        .register_internal(IpInputNode::<IpUnicastArc>::new(IpInputNext::nodes(
            drop, drop, drop, lookup, drop, drop, drop,
        )))
}

fn register_sink(
    runtime: &DataPlaneRuntime,
    counters: &Arc<SinkCounters>,
) -> hammer_core::data_plane::NodeId {
    runtime
        .nodes()
        .register_internal(SinkNode::new(Arc::clone(counters)))
}

fn add_single_path(
    builder: &mut FibTableBuilder<u16>,
    proto: DpoProto,
    next: u16,
) -> hammer_plugin_ip::forwarding::LoadBalanceIndex {
    builder.add_single_path_load_balance(proto, next)
}

fn build_packets(scenario: Scenario) -> Vec<Vec<u8>> {
    let mut packets = Vec::with_capacity(FRAME_PACKETS);
    for index in 0..FRAME_PACKETS {
        let packet = match scenario {
            Scenario::Ipv4SameNext => ipv4_udp_packet(
                [10, 0, 0, 1],
                10_000 + index as u16,
                [203, 0, 113, index as u8],
                53,
                b"lookup",
            ),
            Scenario::Ipv4MixedNext => {
                let route = ((index % 4) + 1) as u8;
                ipv4_udp_packet(
                    [10, 0, 0, 1],
                    20_000 + index as u16,
                    [198, 51, route, index as u8],
                    53,
                    b"lookup",
                )
            }
            Scenario::Ipv6SameNext => ipv6_udp_packet(
                Ipv6Addr::LOCALHOST,
                30_000 + index as u16,
                Ipv6Addr::new(0x2001, 0x0db8, 0x0064, 0, 0, 0, 0, index as u16),
                53,
                b"lookup",
            ),
        };
        packets.push(packet);
    }
    packets
}

fn allocate_lookup_packet(runtime: &DataPlaneRuntime, packet: &[u8]) -> Index {
    let index = runtime
        .alloc_index_with_bytes(packet)
        .expect("allocate lookup packet");
    let Some(first_byte) = packet.first().copied() else {
        return index;
    };
    let (ip_version, ip_protocol, network_header_len) = match first_byte >> 4 {
        4 => (4, packet[9], usize::from(first_byte & 0x0f) * 4),
        6 => (6, packet[6], 40),
        _ => return index,
    };
    let cursor = BufferPacketCursor::new()
        .with_packet_len(packet.len())
        .with_network_header(0, network_header_len)
        .with_transport_header(network_header_len, 8)
        .with_transport_payload_offset(network_header_len + 8);
    let mut buffer = runtime.get_buffer_mut(index).expect("lookup packet buffer");
    // SAFETY: `NetworkOpaque` is the declared primary-opaque layout and its
    // compile-time size/alignment checks fit the buffer's opaque storage.
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
    network.set_packet_cursor(cursor);
    let ip = network.ip_mut();
    ip.set_ip_version(Some(ip_version));
    ip.set_ip_protocol(Some(ip_protocol));
    index
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
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
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

#[derive(Debug, Clone, Copy)]
struct ProbeStats {
    elapsed: Duration,
    packets: usize,
    checksum: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProbeSummary {
    best: ProbeStats,
    median: ProbeStats,
}

impl ProbeStats {
    fn ns_per_packet(self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000_000_000.0 / self.packets as f64
    }
}
