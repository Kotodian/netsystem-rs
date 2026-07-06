use hammer_adapter::memory::{MemoryConfig, MemoryMain};
use hammer_runtime::init::{INIT_FUNCTIONS, topological_order};

#[test]
fn memory_init_sources_are_static_no_lock() {
    let adapter_memory = include_str!("../../hammer-adapter/src/memory.rs");
    let runtime_memory = include_str!("../src/memory.rs");
    for (path, src) in [
        ("crates/hammer-adapter/src/memory.rs", adapter_memory),
        ("crates/hammer-runtime/src/memory.rs", runtime_memory),
    ] {
        for forbidden in [
            "Mutex",
            "RwLock",
            "OnceLock",
            "LazyLock",
            "get_or_init",
            "thread_local!",
            "HashMap",
            "BTreeMap",
        ] {
            assert!(
                !src.contains(forbidden),
                "{path} must not use {forbidden} for memory initialization"
            );
        }
    }
}

#[test]
fn memory_init_is_registered_before_workers() {
    let memory = INIT_FUNCTIONS
        .iter()
        .find(|f| f.name == "memory_init")
        .expect("memory_init registration");
    assert!(
        memory.runs_before.contains(&"start_workers"),
        "memory_init must run before worker threads are spawned"
    );

    let order = topological_order(&INIT_FUNCTIONS).expect("init order");
    let memory_pos = order
        .iter()
        .position(|idx| INIT_FUNCTIONS[*idx].name == "memory_init")
        .expect("memory_init in order");
    let workers_pos = order
        .iter()
        .position(|idx| INIT_FUNCTIONS[*idx].name == "start_workers")
        .expect("start_workers in order");
    assert!(memory_pos < workers_pos);
}

#[test]
fn start_workers_uses_static_memory_runtime_path() {
    let start_workers = include_str!("../src/start_workers.rs");
    assert!(
        !start_workers.contains("new_worker_runtime"),
        "worker startup must not bypass static memory initialization"
    );
    assert!(
        !start_workers.contains("MemoryMain::from_static_config"),
        "worker startup must not re-materialize static memory inside worker threads"
    );
    assert!(
        start_workers.contains("worker_seed")
            && !start_workers.contains("DataPlaneRuntime::with_buffer_arena_and_frame_capacity"),
        "worker startup must derive worker runtimes from the initialized main runtime view"
    );
}

#[test]
fn task_4_visibility_surface_stays_narrow() {
    let buffer_src = include_str!("../../hammer-adapter/src/buffer.rs");
    assert!(
        !buffer_src.contains("pub struct DataPlaneRuntimeWorkerSeed"),
        "worker seed concrete type must not be publicly nameable from hammer_adapter::buffer"
    );
    assert!(
        !buffer_src.contains("pub fn with_static_buffer_arena"),
        "static arena constructor helper must stay adapter-crate-visible only"
    );
}

#[test]
fn engine_spawn_uses_initialized_runtime_view_for_inherited_numa() {
    let config = MemoryConfig {
        numa_nodes: &[0],
        buffer_slot_capacity: 2048,
        buffer_slots_per_numa: 64,
        frame_capacity: 256,
        frame_slots: 32,
    };
    let memory = MemoryMain::from_static_config(config).expect("memory");
    let main_runtime = memory.runtime(0, 0).expect("main runtime");
    let main =
        hammer_runtime::Engine::new(main_runtime, hammer_core::registry::RuntimeRegistry::new());

    let worker = main.spawn(3);

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
        "worker clone on inherited NUMA must share the initialized runtime arena"
    );
}

#[test]
fn memory_main_builds_per_numa_runtimes_without_global_lookup() {
    let config = MemoryConfig {
        numa_nodes: &[0, 1],
        buffer_slot_capacity: 2048,
        buffer_slots_per_numa: 64,
        frame_capacity: 256,
        frame_slots: 32,
    };
    let memory = MemoryMain::from_static_config(config).expect("memory");

    let main = memory.runtime(0, 0).expect("numa0 runtime");
    let worker_same_numa = memory.runtime(3, 0).expect("numa0 worker runtime");
    let worker_other_numa = memory.runtime(4, 1).expect("numa1 worker runtime");

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
