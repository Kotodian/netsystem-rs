use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::Engine;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};

fn test_runtime() -> DataPlaneRuntime {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: 2048,
        buffer_slots: 64,
        frame_slots: 64,
        numa_nodes: &[0, 1],
        active_numa_node: 0,
        ..DataPlaneBufferConfig::default()
    };
    DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers })
}

#[test]
fn spawned_engine_uses_worker_numa_runtime_view() {
    let runtime = test_runtime();
    let mut main = Engine::new(runtime, RuntimeRegistry::new());
    main.numa_node = 0;

    let worker = main.spawn_on_numa(3, 1).expect("spawn worker on NUMA node");

    let main_index = main.runtime.alloc_index().expect("main alloc");
    let worker_index = worker.runtime.alloc_index().expect("worker alloc");

    assert_eq!(main.thread_index, 0);
    assert_eq!(worker.thread_index, 3);
    assert_eq!(worker.numa_node, 1);
    assert_ne!(main_index.pool_id(), worker_index.pool_id());
    assert_eq!(main.runtime.active_numa_node(), 0);
    assert_eq!(worker.runtime.active_numa_node(), 1);
}
