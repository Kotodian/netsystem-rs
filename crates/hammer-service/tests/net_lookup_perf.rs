use std::cell::Cell;
use std::hint::black_box;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferFrame, DataPlaneInstructionSet, DataPlaneRuntime, InternalNode, Node, NodeResult,
    RouteMetadata,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::ip::IpVersion;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{
    DpoId, FibSnapshotBuilder, IpInputNext, IpInputNode, IpLookupControlPlane, IpLookupNode,
};
use ipnet::{Ipv4Net, Ipv6Net};

const FRAME_PACKETS: usize = 128;
const FRAME_ROUNDS: usize = 512;
const SAMPLE_COUNT: usize = 5;
static PERF_PROBE_LOCK: Mutex<()> = Mutex::new(());

struct SinkNode {
    packets: Rc<Cell<usize>>,
    checksum: Rc<Cell<u64>>,
}

impl Node<ProbeNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<ProbeNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut packets = self.packets.get();
        let mut checksum = self.checksum.get();
        for index in frame.drain_pending() {
            if let Some(forwarding) = runtime.metadata(index)?.forwarding {
                checksum = checksum.wrapping_add(u64::from(forwarding.load_balance_index));
                checksum = checksum.wrapping_add(u64::from(forwarding.bucket_index));
            }
            packets += 1;
            runtime.free_index(index);
        }
        self.packets.set(packets);
        self.checksum.set(checksum);
        Ok(NodeResult::drop())
    }
}

impl InternalNode<ProbeNode> for SinkNode {}

enum ProbeNode {
    Sink(SinkNode),
    Drop(DropNode),
    Input(IpInputNode),
    Lookup(IpLookupNode),
}

impl From<SinkNode> for ProbeNode {
    fn from(node: SinkNode) -> Self {
        Self::Sink(node)
    }
}

impl From<DropNode> for ProbeNode {
    fn from(node: DropNode) -> Self {
        Self::Drop(node)
    }
}

impl From<IpInputNode> for ProbeNode {
    fn from(node: IpInputNode) -> Self {
        Self::Input(node)
    }
}

impl From<IpLookupNode> for ProbeNode {
    fn from(node: IpLookupNode) -> Self {
        Self::Lookup(node)
    }
}

impl Node<ProbeNode> for ProbeNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<ProbeNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Sink(node) => node.process(runtime, frame),
            Self::Drop(node) => node.process(runtime, frame),
            Self::Input(node) => node.process(runtime, frame),
            Self::Lookup(node) => node.process(runtime, frame),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    Ipv4SameNext,
    Ipv4MixedNext,
    Ipv6SameNext,
}

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-service --release --test net_lookup_perf -- --ignored --nocapture --test-threads=1`"]
fn ip_lookup_frame_batch_perf_probe() {
    let _guard = PERF_PROBE_LOCK.lock().expect("perf probe lock");
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
}

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-service --release --test net_lookup_perf -- --ignored --nocapture --test-threads=1`"]
fn ip_input_lookup_frame_batch_perf_probe() {
    let _guard = PERF_PROBE_LOCK.lock().expect("perf probe lock");
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
    let runtime = DataPlaneRuntime::<ProbeNode>::with_capacities_and_instruction_set(
        2048,
        1,
        1,
        2,
        DataPlaneInstructionSet::Scalar,
    );
    let packets = Rc::new(Cell::new(0));
    let checksum = Rc::new(Cell::new(0));
    let lookup = build_lookup(&runtime, scenario, &packets, &checksum);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        for packet in packets_by_frame.iter() {
            let frame = runtime.alloc_frame_index().expect("alloc frame");
            {
                let mut frame_ref = runtime.get_frame_mut(frame).expect("mutate frame");
                let index = runtime
                    .alloc_index_with_bytes(RouteMetadata::default(), packet)
                    .expect("alloc packet");
                frame_ref.push_index(index).expect("push packet");
            }
            assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));
            black_box(runtime.run_ready_nodes().expect("run nodes"));
            debug_assert_eq!(runtime.in_use_buffers(), 0);
        }
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(packets.get()),
        checksum: black_box(checksum.get()),
    }
}

fn measure_lookup(scenario: Scenario, instruction_set: DataPlaneInstructionSet) -> ProbeStats {
    let runtime = DataPlaneRuntime::<ProbeNode>::with_capacities_and_instruction_set(
        2048,
        FRAME_PACKETS,
        FRAME_PACKETS,
        32,
        instruction_set,
    );
    let packets = Rc::new(Cell::new(0));
    let checksum = Rc::new(Cell::new(0));
    let lookup = build_lookup(&runtime, scenario, &packets, &checksum);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        {
            let mut frame_ref = runtime.get_frame_mut(frame).expect("mutate frame");
            for packet in packets_by_frame.iter() {
                let index = runtime
                    .alloc_index_with_bytes(RouteMetadata::default(), packet)
                    .expect("alloc packet");
                frame_ref.push_index(index).expect("push packet");
            }
        }
        assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));
        black_box(runtime.run_ready_nodes().expect("run nodes"));
        debug_assert_eq!(runtime.in_use_buffers(), 0);
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(packets.get()),
        checksum: black_box(checksum.get()),
    }
}

fn measure_input_lookup(
    scenario: Scenario,
    instruction_set: DataPlaneInstructionSet,
) -> ProbeStats {
    let runtime = DataPlaneRuntime::<ProbeNode>::with_capacities_and_instruction_set(
        2048,
        FRAME_PACKETS,
        FRAME_PACKETS,
        32,
        instruction_set,
    );
    let packets = Rc::new(Cell::new(0));
    let checksum = Rc::new(Cell::new(0));
    let input = build_input_lookup(&runtime, scenario, &packets, &checksum);
    let packets_by_frame = build_packets(scenario);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        {
            let mut frame_ref = runtime.get_frame_mut(frame).expect("mutate frame");
            for packet in packets_by_frame.iter() {
                let index = runtime
                    .alloc_index_with_bytes(RouteMetadata::default(), packet)
                    .expect("alloc packet");
                frame_ref.push_index(index).expect("push packet");
            }
        }
        assert!(runtime.schedule_frame(input, frame).expect("schedule"));
        black_box(runtime.run_ready_nodes().expect("run nodes"));
        debug_assert_eq!(runtime.in_use_buffers(), 0);
    }
    let elapsed = started.elapsed();

    ProbeStats {
        elapsed,
        packets: black_box(packets.get()),
        checksum: black_box(checksum.get()),
    }
}

fn build_lookup(
    runtime: &DataPlaneRuntime<ProbeNode>,
    scenario: Scenario,
    packets: &Rc<Cell<usize>>,
    checksum: &Rc<Cell<u64>>,
) -> hammer_adapter::NodeId {
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibSnapshotBuilder::new(drop);
    match scenario {
        Scenario::Ipv4SameNext => {
            let sink = register_sink(runtime, packets, checksum);
            let lb = add_single_path(&mut builder, IpVersion::V4, sink);
            builder.add_ip4_route(
                Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
                lb,
            );
        }
        Scenario::Ipv4MixedNext => {
            for route in 1..=4 {
                let sink = register_sink(runtime, packets, checksum);
                let lb = add_single_path(&mut builder, IpVersion::V4, sink);
                builder.add_ip4_route(
                    Ipv4Net::new(Ipv4Addr::new(198, 51, route, 0), 24).expect("mixed route"),
                    lb,
                );
            }
        }
        Scenario::Ipv6SameNext => {
            let sink = register_sink(runtime, packets, checksum);
            let lb = add_single_path(&mut builder, IpVersion::V6, sink);
            builder.add_ip6_route(
                Ipv6Net::new(Ipv6Addr::new(0x2001, 0x0db8, 0x0064, 0, 0, 0, 0, 0), 64)
                    .expect("ipv6 route"),
                lb,
            );
        }
    }
    runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node())
}

fn build_input_lookup(
    runtime: &DataPlaneRuntime<ProbeNode>,
    scenario: Scenario,
    packets: &Rc<Cell<usize>>,
    checksum: &Rc<Cell<u64>>,
) -> hammer_adapter::NodeId {
    let lookup = build_lookup(runtime, scenario, packets, checksum);
    let drop = runtime.nodes().register_internal(DropNode::new());
    runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            drop, drop, drop, lookup, drop, drop, drop,
        )))
}

fn register_sink(
    runtime: &DataPlaneRuntime<ProbeNode>,
    packets: &Rc<Cell<usize>>,
    checksum: &Rc<Cell<u64>>,
) -> hammer_adapter::NodeId {
    runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(packets),
        checksum: Rc::clone(checksum),
    })
}

fn add_single_path(
    builder: &mut FibSnapshotBuilder,
    version: IpVersion,
    node: hammer_adapter::NodeId,
) -> hammer_service::net::LoadBalanceIndex {
    let adjacency = builder.add_adjacency(version, node);
    builder.add_load_balance(version, [DpoId::adjacency(version, adjacency, node)])
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
