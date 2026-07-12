use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hammer_core::data_plane::{Index, BufferPool, DataPlaneBufferConfig, NodeId};
use hammer_infra::vec::Vec;
use hammer_runtime::{DataPlaneHandoff, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId};

fn test_runtime(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_slots: usize,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

fn cleanup_runtime_for_pool(pool: &BufferPool, queue_capacity: usize) -> DataPlaneRuntime {
    let handoff = DataPlaneHandoff::new_shared_buffer_arena(1, queue_capacity.max(1), pool.arena());
    DataPlaneRuntime::attach_handoff_worker(
        test_runtime(1, 1, 1),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    )
}

fn drop_owned_index(runtime: &DataPlaneRuntime, index: Index) {
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("cleanup frame");
    frame.push_index(index).expect("cleanup push index");
}

fn drop_owned_indices(runtime: &DataPlaneRuntime, indices: Vec<Index>) {
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("cleanup frame");
    frame
        .push_indices(indices)
        .expect("cleanup push batch indices");
}

/// Allocate and free a single empty buffer, one pair per iteration. This is
/// the per-packet cost on the hot path.
fn bench_alloc_free_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_free_single");
    group.bench_function("empty", |b| {
        b.iter_batched(
            || {
                let pool = BufferPool::with_capacity(2048, 4096);
                let cleanup = cleanup_runtime_for_pool(&pool, 1);
                (pool, cleanup)
            },
            |(pool, cleanup)| {
                let index = pool.alloc_index().expect("alloc");
                drop_owned_index(&cleanup, index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("with_bytes_1500", |b| {
        let payload = [0u8; 1500];
        b.iter_batched(
            || {
                let pool = BufferPool::with_capacity(2048, 4096);
                let cleanup = cleanup_runtime_for_pool(&pool, 1);
                (pool, cleanup)
            },
            |(pool, cleanup)| {
                let index = pool.alloc_index_with_bytes(&payload).expect("alloc");
                drop_owned_index(&cleanup, index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Batched alloc/free of 256 buffers per iteration. Exercises the batched
/// thread-cache refill/return and the prefetch-ahead behaviour, which is the
/// realistic per-frame cost during node processing.
fn bench_alloc_free_batch256(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_free_batch");
    for &batch in &[64usize, 256, 1024] {
        group.bench_with_input(BenchmarkId::new("empty", batch), &batch, |b, &batch| {
            b.iter_batched(
                || {
                    let pool = BufferPool::with_capacity(2048, batch.max(4096));
                    let cleanup = cleanup_runtime_for_pool(&pool, batch);
                    (pool, cleanup)
                },
                |(pool, cleanup)| {
                    let mut indices = Vec::with_capacity(batch);
                    for _ in 0..batch {
                        indices.push(pool.alloc_index().expect("alloc"));
                    }
                    drop_owned_indices(&cleanup, indices);
                },
                criterion::BatchSize::SmallInput,
            );
        });
        group.bench_with_input(
            BenchmarkId::new("with_bytes_1500", batch),
            &batch,
            |b, &batch| {
                let payload = [0u8; 1500];
                b.iter_batched(
                    || {
                        let pool = BufferPool::with_capacity(2048, batch.max(4096));
                        let cleanup = cleanup_runtime_for_pool(&pool, batch);
                        (pool, cleanup)
                    },
                    |(pool, cleanup)| {
                        let mut indices = Vec::with_capacity(batch);
                        for _ in 0..batch {
                            indices.push(pool.alloc_index_with_bytes(&payload).expect("alloc"));
                        }
                        drop_owned_indices(&cleanup, indices);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Chain alloc/free: a 9000-byte payload spans multiple slots, exercising the
/// chain alloc + free_chain path that TCP output uses for large segments.
fn bench_chain_alloc_free(c: &mut Criterion) {
    let payload = [0u8; 9000];
    let mut group = c.benchmark_group("chain_alloc_free");
    group.bench_function("9000B", |b| {
        b.iter_batched(
            || {
                let pool = BufferPool::with_capacity(2048, 4096);
                let cleanup = cleanup_runtime_for_pool(&pool, 1);
                (pool, cleanup)
            },
            |(pool, cleanup)| {
                let index = pool.alloc_index_with_bytes(&payload).expect("chain alloc");
                drop_owned_index(&cleanup, index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// End-to-end runtime alloc/free (includes frame pool + instruction set),
/// closer to what a node actually pays.
fn bench_runtime_alloc_free(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_alloc_free");
    group.bench_function("single", |b| {
        b.iter_batched(
            || test_runtime(2048, 4096, 256, 64),
            |runtime| {
                let index = runtime.alloc_index().expect("alloc");
                drop_owned_index(&runtime, index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("batch_256", |b| {
        b.iter_batched(
            || test_runtime(2048, 4096, 256, 64),
            |runtime| {
                let mut indices = Vec::with_capacity(256);
                for _ in 0..256 {
                    indices.push(runtime.alloc_index().expect("alloc"));
                }
                drop_owned_indices(&runtime, indices);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_alloc_free_single,
    bench_alloc_free_batch256,
    bench_chain_alloc_free,
    bench_runtime_alloc_free,
);
criterion_main!(benches);
