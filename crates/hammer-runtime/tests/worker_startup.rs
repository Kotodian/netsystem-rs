use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::config::Config;
use hammer_core::data_plane::{
    BufferFrame, NodeHandle, NodeId, NodeKind, NodeRegistration, NodeState,
};
use hammer_core::error::{CoreError, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::start_workers::start_workers;
use hammer_runtime::{
    DataPlaneRuntime, Engine, EnginePool, NodeDescriptor, NodeResult, NodeRuntimeData,
    new_worker_runtime,
};

hammer_runtime::__declare_registration_image!();

const READY: usize = 0;
const INIT_FAILURE: usize = 1;
const PANIC: usize = 2;

static CASE: AtomicUsize = AtomicUsize::new(READY);
static INITIALIZED: AtomicUsize = AtomicUsize::new(0);

fn startup_node_process(
    _: &DataPlaneRuntime,
    _: NodeRuntimeData,
    _: &mut BufferFrame,
) -> NodeResult {
    NodeResult::drop()
}

#[hammer_component_macros::worker_init_function(name = "verify_worker_startup_contract")]
fn verify_worker_startup_contract(engine: &mut Engine) -> HammerResult<()> {
    let worker = engine.data_worker_id()?;
    let node = engine
        .runtime
        .node_by_name("startup-node")
        .ok_or_else(|| CoreError::internal("worker clone is missing startup-node"))?;
    assert_eq!(node, NodeId::new(0));
    engine
        .runtime
        .nodes()
        .set_node_state(node, NodeState::Polling)?;
    engine
        .runtime
        .handoff_indices(worker, NodeHandle::new(0), std::iter::empty())?;

    match CASE.load(Ordering::Acquire) {
        INIT_FAILURE if worker.slot() == 1 => {
            return Err(CoreError::internal("injected worker initialization failure").into());
        }
        PANIC if worker.slot() == 1 => panic!("injected worker initialization panic"),
        _ => {}
    }

    INITIALIZED.fetch_or(1 << worker.slot(), Ordering::Release);
    Ok(())
}

fn engine_pool() -> EnginePool {
    let mut config = Config::default();
    config.worker.count = 2;
    config.worker.buffer.slot_bytes = 2048;
    config.worker.buffer.slots_per_numa = 64;
    config.worker.buffer.frame_pool_size = 64;
    config.worker.instruction_set = "scalar".to_owned();
    let runtime = new_worker_runtime(&config).expect("configured runtime");
    let node = runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                startup_node_process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("startup-node", 0),
                &[],
                None,
            ),
        )
        .expect("canonical startup graph");
    runtime
        .nodes()
        .set_node_state(node, NodeState::Disabled)
        .expect("canonical startup node state");
    let registry = RuntimeRegistry::new();
    registry.set(std::sync::Arc::new(config));
    EnginePool::new(Engine::new(runtime, registry))
}

fn stop_workers(pool: &mut EnginePool) {
    EnginePool::main_loop_exit(pool.main_engine());
    pool.close().expect("close worker pool");
}

#[test]
fn data_worker_startup_is_transactional() {
    CASE.store(READY, Ordering::Release);
    INITIALIZED.store(0, Ordering::Release);
    let mut pool = engine_pool();
    start_workers(pool.main_engine_mut()).expect("transactional worker startup");
    assert_eq!(INITIALIZED.load(Ordering::Acquire), 0b11);
    assert_eq!(
        pool.main_engine()
            .runtime
            .nodes()
            .node_state(NodeId::new(0))
            .expect("main node state"),
        NodeState::Disabled
    );
    stop_workers(&mut pool);

    CASE.store(INIT_FAILURE, Ordering::Release);
    INITIALIZED.store(0, Ordering::Release);
    let mut pool = engine_pool();
    let error = start_workers(pool.main_engine_mut()).expect_err("worker init must fail startup");
    assert!(
        error
            .to_string()
            .contains("injected worker initialization failure")
    );
    assert_eq!(INITIALIZED.load(Ordering::Acquire), 0b01);
    stop_workers(&mut pool);

    CASE.store(PANIC, Ordering::Release);
    INITIALIZED.store(0, Ordering::Release);
    let mut pool = engine_pool();
    let error = start_workers(pool.main_engine_mut()).expect_err("worker panic must fail startup");
    assert!(
        error
            .to_string()
            .contains("init function `verify_worker_startup_contract` panicked")
    );
    stop_workers(&mut pool);
}
