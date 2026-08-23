use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, EnginePool};

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [__INIT_FN_OBSERVE_MAIN_LOOP_EXIT];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [];
    session_transports = [];
    session_apps = [];
    binary_api_methods = [];
);

static EXIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[hammer_component_macros::main_loop_exit_function]
fn observe_main_loop_exit(_: &mut Engine) -> RuntimeResult<()> {
    EXIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn engine_pool() -> EnginePool {
    let buffers = DataPlaneBufferConfig {
        buffer_slot_capacity: 64,
        buffer_slots: 4,
        frame_slots: 4,
        ..DataPlaneBufferConfig::default()
    };
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers });
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    EnginePool::new(engine).expect("engine pool")
}

#[test]
fn engine_pool_close_dispatches_main_loop_exit_hooks_once() {
    EXIT_CALLS.store(0, Ordering::Relaxed);
    let mut pool = engine_pool();

    pool.close().expect("close engine pool");
    pool.close().expect("repeat close is idempotent");

    assert_eq!(EXIT_CALLS.load(Ordering::Relaxed), 1);
}
