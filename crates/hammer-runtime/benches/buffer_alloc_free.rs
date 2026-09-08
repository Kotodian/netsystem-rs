use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use hammer_core::data_plane::BufferMain;
use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain};

fn test_runtime(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_slots: usize,
) -> DataPlaneMain {
    BUFFER_MAIN_INIT.call_once(|| {
        hammer_infra::main_heap::init_default().unwrap();
        BufferMain::new(2048, 4096, &[0], 0, hammer_infra::PageSize::Default).unwrap();
    });
    let config = DataPlaneBufferConfig {
        buffer_slot_capacity,
        buffer_slots,
        frame_slots,
        ..DataPlaneBufferConfig::default()
    };
    DataPlaneMain::new(config)
}

/// Allocate and free a single empty buffer, one pair per iteration. This is
/// the per-packet cost on the hot path.
fn bench_alloc_free_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_free_single");
    group.bench_function("empty", |b| {
        b.iter_batched(
            || test_runtime(2048, 4096, 1),
            |mut buffers| {
                let mut index = 0;
                assert_eq!(buffers.buffer_alloc(core::slice::from_mut(&mut index)), 1);
                buffers.buffer_free_one(index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("with_bytes_1500", |b| {
        let payload = [0u8; 1500];
        b.iter_batched(
            || test_runtime(2048, 4096, 1),
            |mut buffers| {
                let mut index = u32::MAX;
                assert_eq!(buffers.buffer_add_data(&mut index, &payload), payload.len());
                buffers.buffer_free_one(index);
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
                || test_runtime(2048, batch.max(4096), 1),
                |mut buffers| {
                    let mut indices = vec![0; batch];
                    assert_eq!(buffers.buffer_alloc(&mut indices), indices.len());
                    buffers.buffer_free(&indices);
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
                    || test_runtime(2048, batch.max(4096), 1),
                    |mut buffers| {
                        let mut indices = vec![0; batch];
                        assert_eq!(buffers.buffer_alloc(&mut indices), indices.len());
                        for &index in &indices {
                            buffers
                                .buffer_mut(index)
                                .put_uninit(payload.len() as u16)
                                .copy_from_slice(&payload);
                        }
                        buffers.buffer_free(&indices);
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
            || test_runtime(2048, 4096, 1),
            |mut buffers| {
                let mut index = u32::MAX;
                assert_eq!(buffers.buffer_add_data(&mut index, &payload), payload.len());
                buffers.buffer_free_one(index);
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
            || test_runtime(2048, 4096, 256),
            |mut runtime| {
                let mut index = 0;
                assert_eq!(runtime.buffer_alloc(core::slice::from_mut(&mut index)), 1);
                runtime.buffer_free_one(index);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("batch_256", |b| {
        b.iter_batched(
            || test_runtime(2048, 4096, 256),
            |mut runtime| {
                let mut indices = vec![0; 256];
                assert_eq!(runtime.buffer_alloc(&mut indices), indices.len());
                runtime.buffer_free(&indices);
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

static BUFFER_MAIN_INIT: std::sync::Once = std::sync::Once::new();
