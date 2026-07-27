use hammer_core::data_plane::{BufferPoolArena, DataPlaneBuffers, NodeId};
use hammer_infra::PageSize;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneHandoff, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId,
};

fn runtime_config(
    slot_capacity: usize,
    slots: usize,
    frame_slots: usize,
) -> DataPlaneRuntimeConfig {
    DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: slot_capacity,
            buffer_slots: slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    }
}

fn runtime_with_numa(
    slot_capacity: usize,
    slots: usize,
    numa_nodes: &'static [u32],
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: slot_capacity,
            buffer_slots: slots,
            numa_nodes,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

fn runtime_with_handoff_arena(arena: BufferPoolArena, frame_slots: usize) -> DataPlaneRuntime {
    let handoff = DataPlaneHandoff::new_shared_buffer_arena(1, frame_slots.max(1), arena);
    DataPlaneRuntime::attach_handoff_worker(
        DataPlaneRuntime::new(runtime_config(1, 1, frame_slots)),
        handoff.worker(DataWorkerId::new(0)),
    )
}

trait CleanupOwner {
    fn drop_index_owned(&self, index: hammer_core::data_plane::Index);
}

impl CleanupOwner for DataPlaneBuffers {
    fn drop_index_owned(&self, index: hammer_core::data_plane::Index) {
        let mut frame = self.get_next_frame(NodeId::new(0)).expect("cleanup frame");
        frame.push_index(index).expect("cleanup push index");
    }
}

impl CleanupOwner for DataPlaneRuntime {
    fn drop_index_owned(&self, index: hammer_core::data_plane::Index) {
        let mut frame = self
            .buffers()
            .get_next_frame(NodeId::new(0))
            .expect("cleanup frame");
        frame.push_index(index).expect("cleanup push index");
    }
}

macro_rules! drop_owned_index {
    ($owner:expr, $index:expr) => {{
        ($owner).drop_index_owned($index);
    }};
}

#[test]
fn arenas_keep_their_arena_numa_identity() {
    let a0 = BufferPoolArena::with_capacity_on_numa(1024, 32, PageSize::Default, 0)
        .expect("create NUMA 0 arena");
    let a1 = BufferPoolArena::with_capacity_on_numa(1024, 32, PageSize::Default, 1)
        .expect("create NUMA 1 arena");
    let b0 = DataPlaneBuffers::from_arenas([a0.clone()], 1, 0, 0);
    let b1 = DataPlaneBuffers::from_arenas([a1.clone()], 1, 0, 1);

    let i0 = b0.alloc_index().expect("numa0 alloc");
    let i1 = b1.alloc_index().expect("numa1 alloc");

    assert_eq!(a0.numa_node(), 0);
    assert_eq!(a1.numa_node(), 1);
    assert_eq!(i0.pool_id(), a0.pool_id());
    assert_eq!(i1.pool_id(), a1.pool_id());
    assert_ne!(i0.pool_id(), i1.pool_id());

    drop_owned_index!(&b0, i0);
    drop_owned_index!(&b1, i1);
}

#[test]
fn default_numa_configuration_uses_numa_zero() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 1024,
            buffer_slots: 16,
            ..Default::default()
        },
    });
    let index = runtime.alloc_index().expect("default alloc");

    assert_eq!(runtime.active_numa_node(), 0);
    assert_eq!(runtime.buffers().active_numa_node(), 0);

    drop_owned_index!(&runtime, index);
}

#[test]
fn config_constructor_resolves_active_numa_to_configured_node() {
    let config = DataPlaneBufferConfig {
        buffer_slot_capacity: 1024,
        buffer_slots: 16,
        frame_slots: 4,
        numa_nodes: &[3],
        active_numa_node: 0,
        ..DataPlaneBufferConfig::default()
    };
    let buffers = DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers: config })
        .buffers()
        .clone();
    let runtime = runtime_with_numa(1024, 16, &[3]);

    assert_eq!(buffers.active_numa_node(), 3);
    assert_eq!(runtime.active_numa_node(), 3);
    assert_eq!(runtime.buffers().active_numa_node(), 3);
}

#[test]
fn handoff_arena_constructor_uses_arena_numa_identity() {
    let arena = BufferPoolArena::with_capacity_on_numa(1024, 16, PageSize::Default, 3)
        .expect("create NUMA 3 arena");
    let runtime = runtime_with_handoff_arena(arena, 4);
    let buffers = runtime.buffers();

    assert_eq!(buffers.active_numa_node(), 3);
    assert_eq!(runtime.active_numa_node(), 3);
    assert_eq!(runtime.buffers().active_numa_node(), 3);
}

#[test]
fn handoff_capacities_preserve_handoff_arena_numa_identity() {
    let handoff = DataPlaneHandoff::new_shared_buffer_arena(
        2,
        4,
        BufferPoolArena::with_capacity_on_numa(1024, 16, PageSize::Default, 3)
            .expect("create NUMA 3 arena"),
    );
    let runtime = DataPlaneRuntime::attach_handoff_worker(
        DataPlaneRuntime::new(runtime_config(1, 1, 4)),
        handoff.worker(DataWorkerId::new(0)),
    );

    assert_eq!(runtime.active_numa_node(), 3);
    assert_eq!(runtime.buffers().active_numa_node(), 3);
}

#[test]
fn handoff_worker_runtime_falls_back_to_configured_nonzero_numa_arena() {
    let handoff = DataPlaneHandoff::new_shared_buffer_arena(
        2,
        4,
        BufferPoolArena::with_capacity_on_numa(1024, 16, PageSize::Default, 3)
            .expect("create NUMA 3 arena"),
    );
    let runtime = DataPlaneRuntime::attach_handoff_worker(
        DataPlaneRuntime::new(runtime_config(1, 1, 4)),
        handoff.worker(DataWorkerId::new(0)),
    );

    let worker = runtime.for_worker(1, 1).expect("worker runtime fork");
    let main_index = runtime.alloc_index().expect("main handoff alloc");
    let worker_index = worker.alloc_index().expect("worker handoff alloc");

    assert_eq!(worker.active_numa_node(), 3);
    assert_eq!(worker.buffers().active_numa_node(), 3);
    assert_eq!(main_index.pool_id(), worker_index.pool_id());

    drop_owned_index!(&worker, worker_index);
    drop_owned_index!(&runtime, main_index);
}

#[test]
fn same_numa_worker_runtime_shares_arena_but_not_thread_cache() {
    let runtime = runtime_with_numa(1024, 4, &[0]);
    let worker = runtime.for_worker(1, 0).expect("worker runtime fork");

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
