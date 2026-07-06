use hammer_adapter::DataPlaneRuntime;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::Engine;

#[test]
fn spawned_engine_uses_worker_numa_runtime_view() {
    let runtime = DataPlaneRuntime::with_numa_buffer_capacity(2048, 64, &[0, 1]);
    let mut main = Engine::new(runtime, RuntimeRegistry::new());
    main.numa_node = 0;

    let worker = main.spawn_on_numa(3, 1);

    let main_index = main.runtime.alloc_index().expect("main alloc");
    let worker_index = worker.runtime.alloc_index().expect("worker alloc");

    assert_eq!(main.thread_index, 0);
    assert_eq!(worker.thread_index, 3);
    assert_eq!(worker.numa_node, 1);
    assert_ne!(main_index.pool_id(), worker_index.pool_id());
    assert_eq!(main.runtime.active_numa_node(), 0);
    assert_eq!(worker.runtime.active_numa_node(), 1);
}

#[test]
fn start_workers_applies_setup_before_numa_probe_and_runtime_clone() {
    let start_workers = include_str!("../src/start_workers.rs");
    let setup_pos = start_workers
        .find("apply_worker_thread_setup(&worker_config, worker_index);")
        .expect("worker setup call");
    let numa_pos = start_workers
        .find("current_numa_node().unwrap_or(0)")
        .expect("numa probe");
    let spawn_pos = start_workers
        .find("worker_seed.spawn_on_numa(idx, worker_numa_node)")
        .expect("worker runtime clone");

    assert!(
        start_workers.contains(".get::<hammer_core::config::Config>()"),
        "start_workers must read worker config from the runtime registry"
    );
    assert!(
        setup_pos < numa_pos,
        "worker thread setup must happen before probing the worker NUMA node"
    );
    assert!(
        numa_pos < spawn_pos,
        "worker runtime clone must happen after the worker NUMA node is known"
    );
}
