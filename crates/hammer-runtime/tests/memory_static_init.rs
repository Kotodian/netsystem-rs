use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::Worker;
use hammer_runtime::{DataPlaneInstructionSet, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

fn runtime_config(numa_nodes: &'static [u32], active_numa_node: u32) -> DataPlaneRuntimeConfig {
    DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 64,
            frame_slots: 32,
            numa_nodes,
            active_numa_node,
            ..DataPlaneBufferConfig::default()
        },
    }
}

#[test]
fn memory_init_materializes_the_configured_buffer_and_instruction_set_policy() {
    let mut expected_worker = Worker::default();
    expected_worker.buffer.slot_bytes = 4096;
    expected_worker.buffer.slots_per_numa = 7;
    expected_worker.buffer.frame_pool_size = 5;
    expected_worker.instruction_set = "scalar".to_owned();
    let expected =
        hammer_runtime::new_worker_runtime(&expected_worker).expect("configured runtime");
    let expected_stride = expected
        .buffers()
        .try_buffers()
        .expect("configured buffer pool")
        .slot_stride();
    let registry = RuntimeRegistry::new();
    let mut engine = Engine::new(DataPlaneRuntime::new(runtime_config(&[0], 0)), registry);

    engine
        .configure_early(
            r#"
[worker]
instruction_set = "scalar"

[worker.buffer]
slot_bytes = 4096
slots_per_numa = 7
frame_pool_size = 5
"#,
        )
        .expect("configured worker config dispatch");

    assert_eq!(
        engine.runtime.instruction_set(),
        DataPlaneInstructionSet::Scalar
    );
    assert_eq!(engine.runtime.buffers().frame_slots(), 5);
    assert_eq!(
        engine
            .runtime
            .buffers()
            .try_buffers()
            .unwrap()
            .slot_stride(),
        expected_stride
    );
    for _ in 0..7 {
        engine
            .runtime
            .alloc_index()
            .expect("configured buffer slot");
    }
    assert!(engine.runtime.alloc_index().is_err());
}

#[test]
fn engine_spawn_uses_initialized_runtime_view_for_inherited_numa() {
    let main_runtime = DataPlaneRuntime::new(runtime_config(&[0], 0));
    let main = hammer_runtime::Engine::new(main_runtime, hammer_runtime::RuntimeRegistry::new());

    let worker = main.spawn(3).expect("spawn worker");

    assert_eq!(worker.thread_index, 3);
    assert_eq!(worker.numa_node, 0);
    assert_eq!(worker.runtime.active_numa_node(), 0);
    assert_eq!(
        main.runtime.alloc_index().expect("main alloc").pool_id(),
        worker
            .runtime
            .alloc_index()
            .expect("worker alloc")
            .pool_id(),
        "worker runtime on inherited NUMA must share the initialized runtime arena"
    );
}

#[test]
fn runtime_config_builds_per_numa_worker_views_without_global_lookup() {
    let runtime = DataPlaneRuntime::new(runtime_config(&[0, 1], 0));
    let main = runtime.for_worker(0, 0);
    let worker_same_numa = runtime.for_worker(3, 0);
    let worker_other_numa = runtime.for_worker(4, 1);

    assert_eq!(main.active_numa_node(), 0);
    assert_eq!(worker_same_numa.active_numa_node(), 0);
    assert_eq!(worker_other_numa.active_numa_node(), 1);
    assert_eq!(
        main.alloc_index().expect("main alloc").pool_id(),
        worker_same_numa
            .alloc_index()
            .expect("same numa worker alloc")
            .pool_id(),
        "workers on the same NUMA node must share the startup-created buffer pool"
    );
    assert_ne!(
        main.alloc_index().expect("main alloc").pool_id(),
        worker_other_numa
            .alloc_index()
            .expect("other numa worker alloc")
            .pool_id(),
        "different NUMA nodes use distinct buffer pools"
    );
}
