//! Graph Fanout 256-packet benchmarks (#54).
//!
//! Fixture construction, graph registration, and initial allocation stay outside
//! the measured warmed grouping/transfer section. Scalar and architecture
//! mask-compare paths are reported as separate groups.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Frame, Index, Next, NodeId, NodeKind,
    NodeRegistration,
};
use hammer_infra::mask_compare::{
    mask_compare_u16_arch, mask_compare_u16_scalar, mask_compare_u16_words,
};
use hammer_runtime::RuntimeResult;
use hammer_runtime::node::{NodeDescriptor, NodeResult, NodeRuntimeData};
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig,
};

fn test_runtime(frame_slots: usize, buffer_slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    })
}

fn register_sink(runtime: &DataPlaneRuntime, name: &'static str) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            |_, _, frame: &mut BufferFrame| {
                frame.discard_prefix(frame.len());
                NodeResult::drop()
            },
            NodeRuntimeData::empty(),
            NodeRegistration::next(name, 0),
            &[],
            None,
        ),
    )
}

fn register_owner(runtime: &DataPlaneRuntime, nexts: &[NodeId]) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_descriptor(
        NodeKind::Internal,
        NodeDescriptor::new(
            |_, _, _| NodeResult::drop(),
            NodeRuntimeData::empty(),
            NodeRegistration::next("fanout-owner", nexts.len()),
            nexts,
            None,
        ),
    )
}

struct FanoutFixture {
    runtime: DataPlaneRuntime,
    owner: NodeId,
    frame: Frame<Next>,
    nexts: [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    _indices: Vec<Index>,
}

fn build_fixture(pattern: FanoutPattern) -> FanoutFixture {
    let runtime = test_runtime(128, DEFAULT_BUFFER_FRAME_CAPACITY * 4);
    let sinks = [
        register_sink(&runtime, "s0").expect("s0"),
        register_sink(&runtime, "s1").expect("s1"),
        register_sink(&runtime, "s2").expect("s2"),
        register_sink(&runtime, "s3").expect("s3"),
    ];
    let owner = register_owner(&runtime, &sinks).expect("owner");
    let mut indices = Vec::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut frame = runtime.buffers().get_next_frame(owner).expect("frame");
    for offset in 0..DEFAULT_BUFFER_FRAME_CAPACITY {
        let index = runtime
            .alloc_index_with_bytes(&[(offset % 256) as u8])
            .expect("alloc");
        frame.push_index(index).expect("push");
        indices.push(index);
    }
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    match pattern {
        FanoutPattern::Single => {}
        FanoutPattern::Alternating => {
            for (slot, next) in nexts.iter_mut().enumerate() {
                *next = (slot % 2) as u16;
            }
        }
        FanoutPattern::Multi => {
            for (slot, next) in nexts.iter_mut().enumerate() {
                *next = (slot % 4) as u16;
            }
        }
    }

    // Warm grouping/transfer once so measured iterations start from a steady state.
    runtime.with_current_node(owner, || {
        runtime.enqueue_to_next(&mut frame, &nexts);
    });
    let _ = runtime.run_ready_nodes();

    let mut frame = runtime
        .buffers()
        .get_next_frame(owner)
        .expect("measured frame");
    let mut indices = Vec::with_capacity(DEFAULT_BUFFER_FRAME_CAPACITY);
    for offset in 0..DEFAULT_BUFFER_FRAME_CAPACITY {
        let index = runtime
            .alloc_index_with_bytes(&[(offset % 256) as u8])
            .expect("alloc");
        frame.push_index(index).expect("push");
        indices.push(index);
    }

    FanoutFixture {
        runtime,
        owner,
        frame,
        nexts,
        _indices: indices,
    }
}

#[derive(Clone, Copy)]
enum FanoutPattern {
    Single,
    Alternating,
    Multi,
}

fn bench_fanout_256(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_256/native");
    for (name, pattern) in [
        ("single_next", FanoutPattern::Single),
        ("alternating_two_next", FanoutPattern::Alternating),
        ("multi_next", FanoutPattern::Multi),
    ] {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter_batched(
                || build_fixture(pattern),
                |mut fixture| {
                    fixture.runtime.with_current_node(fixture.owner, || {
                        fixture
                            .runtime
                            .enqueue_to_next(&mut fixture.frame, &fixture.nexts);
                    });
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_mask_compare_paths(c: &mut Criterion) {
    let values = {
        let mut values = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
        for (slot, value) in values.iter_mut().enumerate() {
            *value = (slot % 4) as u16;
        }
        values
    };
    let words = mask_compare_u16_words(values.len());

    let mut group = c.benchmark_group("mask_compare_256");
    group.bench_function("scalar", |b| {
        b.iter(|| {
            let mut masks = [0u64; 4];
            assert!(masks.len() >= words);
            std::hint::black_box(mask_compare_u16_scalar(1, &values, &mut masks))
        });
    });
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    group.bench_function("arch", |b| {
        b.iter(|| {
            let mut masks = [0u64; 4];
            assert!(masks.len() >= words);
            std::hint::black_box(mask_compare_u16_arch(1, &values, &mut masks))
        });
    });
    group.finish();
}

criterion_group!(benches, bench_fanout_256, bench_mask_compare_paths);
criterion_main!(benches);
