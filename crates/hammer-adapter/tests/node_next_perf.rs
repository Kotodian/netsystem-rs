use std::cell::Cell;
use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneInstructionSet, DataPlaneRuntime, DriverNode, InternalNode,
    Node, NodeId, NodeNextEnqueue, NodeResult,
};
use hammer_core::error::CoreResult;

const FRAME_PACKETS: usize = 128;
const FRAME_ROUNDS: usize = 4096;
const SAMPLE_COUNT: usize = 5;

struct SplitNode {
    speculative: NodeId,
    alternate: NodeId,
    mixed_next: bool,
}

impl Node for SplitNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        NodeNextEnqueue::new(self.speculative).validate_frame(runtime, frame, |index| {
            if self.mixed_next && index.slot() % 4 == 0 {
                Ok(self.alternate)
            } else {
                Ok(self.speculative)
            }
        })
    }
}

impl InternalNode for SplitNode {}

struct SinkNode {
    packets: Rc<Cell<usize>>,
    checksum: Rc<Cell<u64>>,
}

impl Node for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut packets = self.packets.get();
        let mut checksum = self.checksum.get();
        for index in frame.drain_pending() {
            packets += 1;
            checksum = checksum.wrapping_add(index.slot() as u64);
        }
        self.packets.set(packets);
        self.checksum.set(checksum);
        Ok(NodeResult::drop())
    }
}

impl DriverNode for SinkNode {}

#[test]
#[ignore = "performance probe; run with `cargo test -p hammer-adapter --release --test node_next_perf -- --ignored --nocapture`"]
fn speculative_next_enqueue_batch_probe() {
    for mixed_next in [false, true] {
        let scalar = measure_enqueue_samples(mixed_next, DataPlaneInstructionSet::Scalar);
        let native = measure_enqueue_samples(mixed_next, DataPlaneInstructionSet::native());
        assert_eq!(scalar.best.packets, FRAME_PACKETS * FRAME_ROUNDS);
        assert_eq!(native.best.packets, scalar.best.packets);
        assert_eq!(native.best.checksum, scalar.best.checksum);
        eprintln!(
            "speculative_enqueue mixed_next={mixed_next}: samples={SAMPLE_COUNT} frames={FRAME_ROUNDS} frame_packets={FRAME_PACKETS} scalar_best={:.2} scalar_median={:.2} ns/packet native({:?})_best={:.2} native_median={:.2} ns/packet best_ratio={:.3} checksum={}",
            scalar.best.ns_per_packet(),
            scalar.median.ns_per_packet(),
            DataPlaneInstructionSet::native(),
            native.best.ns_per_packet(),
            native.median.ns_per_packet(),
            native.best.ns_per_packet() / scalar.best.ns_per_packet(),
            scalar.best.checksum,
        );
    }
}

fn measure_enqueue_samples(
    mixed_next: bool,
    instruction_set: DataPlaneInstructionSet,
) -> ProbeSummary {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(measure_enqueue(mixed_next, instruction_set));
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

fn measure_enqueue(mixed_next: bool, instruction_set: DataPlaneInstructionSet) -> ProbeStats {
    let runtime = DataPlaneRuntime::with_capacities_and_instruction_set(
        64,
        FRAME_PACKETS,
        FRAME_PACKETS,
        32,
        instruction_set,
    );
    let packets = Rc::new(Cell::new(0));
    let checksum = Rc::new(Cell::new(0));
    let default = runtime.nodes().register_driver(SinkNode {
        packets: Rc::clone(&packets),
        checksum: Rc::clone(&checksum),
    });
    let alternate = runtime.nodes().register_driver(SinkNode {
        packets: Rc::clone(&packets),
        checksum: Rc::clone(&checksum),
    });
    let split = runtime.nodes().register_internal(SplitNode {
        speculative: default,
        alternate,
        mixed_next,
    });
    let indices = alloc_indices(&runtime);

    let started = Instant::now();
    for _ in 0..FRAME_ROUNDS {
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        {
            let mut frame = runtime.get_frame_mut(frame).expect("mutate frame");
            for index in indices.iter().copied() {
                frame.push_index(index).expect("push frame index");
            }
        }
        assert!(runtime.schedule_frame(split, frame).expect("schedule"));
        black_box(runtime.run_ready_nodes().expect("run nodes"));
    }
    ProbeStats {
        elapsed: started.elapsed(),
        packets: black_box(packets.get()),
        checksum: black_box(checksum.get()),
    }
}

fn alloc_indices(runtime: &DataPlaneRuntime) -> Vec<BufferIndex> {
    let mut indices = Vec::with_capacity(FRAME_PACKETS);
    for index in 0..FRAME_PACKETS {
        let buffer = runtime
            .alloc_index_with_bytes(&[index as u8])
            .expect("alloc packet");
        indices.push(buffer);
    }
    indices
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
    #[inline]
    fn ns_per_packet(self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000_000_000.0 / self.packets as f64
    }
}
