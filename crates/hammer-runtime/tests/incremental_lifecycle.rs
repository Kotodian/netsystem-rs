use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

hammer_runtime::__declare_registration_image!();

static INIT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[hammer_component_macros::init_function(name = "incremental_lifecycle_probe")]
fn incremental_lifecycle_probe() -> RuntimeResult<()> {
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
    Engine::new(runtime, RuntimeRegistry::new())
}

#[test]
fn init_lifecycle_executes_each_constructor_record_once_per_engine() {
    INIT_CALLS.store(0, Ordering::Relaxed);
    let mut engine = test_engine();

    hammer_runtime::init::run_init_functions(&mut engine).expect("first init dispatch");
    hammer_runtime::init::run_init_functions(&mut engine).expect("repeated init dispatch");

    assert_eq!(INIT_CALLS.load(Ordering::Relaxed), 1);
}
