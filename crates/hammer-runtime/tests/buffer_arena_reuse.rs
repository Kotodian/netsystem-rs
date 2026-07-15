use hammer_core::data_plane::{DataPlaneBufferConfig, Index, NodeId};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

const MAX_RETAINED_MAIN_HEAP_ALLOCATIONS: usize = 1_000_000;
const BUFFER_REUSE_ROUNDS: usize = 8192;

#[test]
fn warmed_buffer_arena_keeps_working_after_main_heap_exhaustion() {
    let requested = hammer_infra::main_heap::minimum_capacity().max(64 << 20);
    hammer_infra::main_heap::init(requested).expect("initialize fixed main heap");

    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 4096,
            frame_slots: 32,
            ..DataPlaneBufferConfig::default()
        },
    });
    assert!(
        allocate_and_recycle_buffer(&runtime),
        "warm the buffer arena allocation path"
    );

    let mut retained_allocations = Vec::with_capacity(MAX_RETAINED_MAIN_HEAP_ALLOCATIONS);
    let mut retained_pointer_limit_reached = false;

    'allocation_sizes: for size in [1 << 20, 64 << 10, 4 << 10, 256, 16, 1] {
        loop {
            if retained_allocations.len() == retained_allocations.capacity() {
                retained_pointer_limit_reached = true;
                break 'allocation_sizes;
            }
            // SAFETY: each successful pointer is retained without dereference
            // and released exactly once after the buffer-reuse check.
            let pointer = unsafe { libc::malloc(size) };
            if pointer.is_null() {
                break;
            }
            retained_allocations.push(pointer);
        }
    }

    // The last size class is exhausted before this check. If warmed buffer
    // allocation/free performs an ordinary allocation, the fixed Main Heap
    // cannot satisfy it and this subprocess fails instead of using System.
    // SAFETY: a non-null result is retained for the common cleanup below.
    let one_byte_allocation = unsafe { libc::malloc(1) };
    let main_heap_exhausted = one_byte_allocation.is_null();
    if !one_byte_allocation.is_null() {
        retained_allocations.push(one_byte_allocation);
    }

    let mut every_buffer_was_recycled = true;
    for _ in 0..BUFFER_REUSE_ROUNDS {
        if !allocate_and_recycle_buffer(&runtime) {
            every_buffer_was_recycled = false;
            break;
        }
    }

    // SAFETY: every retained pointer is live and appears exactly once.
    unsafe {
        for pointer in retained_allocations {
            libc::free(pointer);
        }
    }

    assert!(!retained_pointer_limit_reached);
    assert!(main_heap_exhausted);
    assert!(every_buffer_was_recycled);
}

fn allocate_and_recycle_buffer(runtime: &DataPlaneRuntime) -> bool {
    let Ok(index) = runtime.alloc_index() else {
        return false;
    };
    release_owned_index(runtime, index)
}

fn release_owned_index(runtime: &DataPlaneRuntime, index: Index) -> bool {
    let Ok(mut frame) = runtime.buffers().get_next_frame(NodeId::new(0)) else {
        return false;
    };
    if frame.push_index(index).is_err() {
        return false;
    }
    drop(frame);
    true
}
