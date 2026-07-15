use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::config::Config;
use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

hammer_runtime::__declare_registration_image!();

static INIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[hammer_component_macros::init_function(name = "incremental_lifecycle_probe")]
fn incremental_lifecycle_probe() -> HammerResult<()> {
    INIT_CALLS.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn test_engine() -> Engine {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 4,
            frame_slots: 4,
            ..DataPlaneBufferConfig::default()
        },
    });
    let registry = RuntimeRegistry::new();
    registry.set(Arc::new(Config::default()));
    Engine::new(runtime, registry)
}

#[test]
fn init_lifecycle_executes_each_constructor_record_once_per_engine() {
    INIT_CALLS.store(0, Ordering::Relaxed);
    let mut engine = test_engine();

    hammer_runtime::init::run_init_functions(&mut engine).expect("first init dispatch");
    hammer_runtime::init::run_init_functions(&mut engine).expect("repeated init dispatch");

    assert_eq!(INIT_CALLS.load(Ordering::Relaxed), 1);
}
