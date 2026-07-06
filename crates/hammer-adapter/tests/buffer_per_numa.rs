use std::sync::Arc;

use hammer_adapter::buffer::{BufferPool, BufferPoolArena};
use hammer_adapter::{DataPlaneHandoff, DataPlaneInstructionSet, DataPlaneRuntime, DataWorkerId};
use hammer_infra::heap::Heap;

trait CleanupOwner {
    fn drop_index_owned(&self, index: hammer_adapter::BufferIndex);
}

impl CleanupOwner for BufferPool {
    fn drop_index_owned(&self, index: hammer_adapter::BufferIndex) {
        let buffers = hammer_adapter::DataPlaneBuffers::with_buffer_arena_and_frame_capacity(
            self.arena(),
            1,
            1,
            DataPlaneInstructionSet::native(),
        );
        let mut frame = buffers.alloc_frame().expect("cleanup frame");
        frame.push_index(index).expect("cleanup push index");
    }
}

impl CleanupOwner for DataPlaneRuntime {
    fn drop_index_owned(&self, index: hammer_adapter::BufferIndex) {
        let mut frame = self.alloc_frame().expect("cleanup frame");
        frame.push_index(index).expect("cleanup push index");
    }
}

macro_rules! drop_owned_index {
    ($owner:expr, $index:expr) => {{
        ($owner).drop_index_owned($index);
    }};
}

#[test]
fn arenas_keep_their_heap_numa_identity() {
    let a0 = BufferPoolArena::with_capacity_in(1024, 32, Arc::new(Heap::local(0)));
    let a1 = BufferPoolArena::with_capacity_in(1024, 32, Arc::new(Heap::local(1)));
    let p0 = BufferPool::with_arena(a0);
    let p1 = BufferPool::with_arena(a1);

    let i0 = p0.alloc_index().expect("numa0 alloc");
    let i1 = p1.alloc_index().expect("numa1 alloc");

    assert_eq!(p0.heap_numa_node(), 0);
    assert_eq!(p1.heap_numa_node(), 1);
    assert_ne!(p0.pool_id(), p1.pool_id());
    assert_ne!(p0.buffer_raw_ptr(i0.slot()), p1.buffer_raw_ptr(i1.slot()));
}

#[test]
fn ordinary_buffer_pool_clone_shares_thread_cache_on_same_thread() {
    let source = include_str!("../src/buffer.rs");

    assert!(
        source.contains("thread_cache: Rc::clone(&self.thread_cache)"),
        "BufferPool::clone must share thread_cache on same-thread clones"
    );
}

#[test]
fn empty_numa_configuration_defaults_to_numa_zero() {
    let runtime = DataPlaneRuntime::with_numa_buffer_capacity(1024, 16, &[]);
    let index = runtime.alloc_index().expect("fallback alloc");

    assert_eq!(runtime.active_numa_node(), 0);
    assert_eq!(runtime.buffers().active_numa_node(), 0);
    assert_eq!(runtime.buffers().buffers().heap_numa_node(), 0);

    drop_owned_index!(&runtime, index);
}

#[test]
fn legacy_with_buffer_arena_constructor_keeps_active_numa_zero() {
    let arena = BufferPoolArena::with_capacity_in(1024, 16, Arc::new(Heap::local(3)));
    let buffers = hammer_adapter::DataPlaneBuffers::with_buffer_arena_and_frame_capacity(
        arena.clone(),
        4,
        4,
        DataPlaneInstructionSet::Scalar,
    );
    let runtime = DataPlaneRuntime::with_buffer_arena_and_frame_capacity(
        arena,
        4,
        4,
        DataPlaneInstructionSet::Scalar,
    );

    assert_eq!(buffers.active_numa_node(), 0);
    assert_eq!(buffers.buffers().heap_numa_node(), 3);
    assert_eq!(runtime.active_numa_node(), 0);
    assert_eq!(runtime.buffers().active_numa_node(), 0);
    assert_eq!(runtime.buffers().buffers().heap_numa_node(), 3);
}

#[test]
fn handoff_capacities_preserve_handoff_arena_numa_identity() {
    let handoff = DataPlaneHandoff::with_buffer_arena(
        2,
        4,
        BufferPoolArena::with_capacity_in(1024, 16, Arc::new(Heap::local(3))),
    );
    let runtime = DataPlaneRuntime::with_handoff_capacities(
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
        4,
        4,
        DataPlaneInstructionSet::Scalar,
    );

    assert_eq!(runtime.active_numa_node(), 3);
    assert_eq!(runtime.buffers().active_numa_node(), 3);
    assert_eq!(runtime.buffers().buffers().heap_numa_node(), 3);
}

#[test]
fn handoff_worker_clone_falls_back_to_configured_nonzero_numa_arena() {
    let handoff = DataPlaneHandoff::with_buffer_arena(
        2,
        4,
        BufferPoolArena::with_capacity_in(1024, 16, Arc::new(Heap::local(3))),
    );
    let runtime = DataPlaneRuntime::with_handoff_capacities(
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
        4,
        4,
        DataPlaneInstructionSet::Scalar,
    );

    let worker = runtime.clone_for_worker(1, 1);
    let main_index = runtime.alloc_index().expect("main handoff alloc");
    let worker_index = worker.alloc_index().expect("worker handoff alloc");

    assert_eq!(worker.active_numa_node(), 3);
    assert_eq!(worker.buffers().active_numa_node(), 3);
    assert_eq!(worker.buffers().buffers().heap_numa_node(), 3);
    assert_eq!(main_index.pool_id(), worker_index.pool_id());

    drop_owned_index!(&worker, worker_index);
    drop_owned_index!(&runtime, main_index);
}

#[test]
fn same_numa_worker_clone_shares_arena_but_not_thread_cache() {
    let runtime = DataPlaneRuntime::with_numa_buffer_capacity(1024, 4, &[0]);
    let worker = runtime.clone_for_worker(1, 0);

    let main_index = runtime.alloc_index().expect("main alloc");
    drop_owned_index!(&runtime, main_index);

    assert!(runtime.cached_free_buffers() > 0);
    assert_eq!(worker.cached_free_buffers(), 0);

    let worker_index = worker.alloc_index().expect("worker alloc");

    assert_eq!(worker_index.pool_id(), main_index.pool_id());
    assert_ne!(worker_index.slot(), main_index.slot());
    assert_eq!(worker.cached_free_buffers(), 0);

    drop_owned_index!(&worker, worker_index);
}

#[test]
fn handoff_source_does_not_use_lock_backed_lazy_arena_init() {
    let source = include_str!("../src/handoff.rs");

    assert!(
        !source.contains("Mutex"),
        "handoff.rs must not contain Mutex-backed arena initialization"
    );
    assert!(
        !source.contains("set_or_get_buffer_arena"),
        "handoff.rs must not contain lazy arena setter/getter"
    );
}
