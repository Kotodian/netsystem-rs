use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, EnginePool};

hammer_runtime::__declare_registration_image!();

static EXIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[hammer_component_macros::main_loop_exit_function]
fn observe_main_loop_exit(_: &mut Engine) -> HammerResult<()> {
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
    EnginePool::new(Engine::new(runtime, RuntimeRegistry::new()))
}

#[test]
fn engine_pool_close_dispatches_main_loop_exit_hooks_once() {
    EXIT_CALLS.store(0, Ordering::Relaxed);
    let mut pool = engine_pool();

    pool.close().expect("close engine pool");
    pool.close().expect("repeat close is idempotent");

    assert_eq!(EXIT_CALLS.load(Ordering::Relaxed), 1);
}
