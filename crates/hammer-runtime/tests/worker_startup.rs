use core::hint::spin_loop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use hammer_core::data_plane::{
    BufferFrame, NodeHandle, NodeId, NodeKind, NodeRegistration, NodeState,
};
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::Worker;
use hammer_runtime::start_workers::start_workers;
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, EnginePool, InternalNode, Node, NodeDescriptor,
    NodeProcessFn, NodeResult, NodeRuntimeData, RuntimeError, RuntimeResult,
};

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [__INIT_FN_VERIFY_WORKER_STARTUP_CONTRACT];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [];
    session_transports = [];
    session_apps = [];
    binary_api_methods = [];
);

const READY: usize = 0;
const INIT_FAILURE: usize = 1;
const PANIC: usize = 2;
const EARLY_EXIT: usize = 3;
const STARTUP_NODE_HANDLE: NodeHandle = NodeHandle::new(41);

/// Both tests drive worker startup through the shared CASE/INITIALIZED/
/// DISPATCHED statics; the parallel test harness must not interleave them.
static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialize_test() -> std::sync::MutexGuard<'static, ()> {
    TEST_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

static CASE: AtomicUsize = AtomicUsize::new(READY);
static INITIALIZED: AtomicUsize = AtomicUsize::new(0);
static DISPATCHED: AtomicUsize = AtomicUsize::new(0);
static ABORT_OBSERVED: AtomicUsize = AtomicUsize::new(0);

fn startup_node_process(
    _: &DataPlaneRuntime,
    data: NodeRuntimeData,
    _: &mut BufferFrame,
) -> NodeResult {
    let worker = usize::try_from(data.word(0) - 1).expect("worker runtime data fits usize");
    DISPATCHED.fetch_or(1 << worker, Ordering::Release);
    NodeResult::drop()
}

struct StartupNode {
    next: [NodeId; 1],
}

impl Node for StartupNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        startup_node_process
    }

    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("startup-node", 1)
    }

    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

impl InternalNode for StartupNode {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("startup-node", 1)
    }

    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

#[hammer_component_macros::worker_init_function(name = "verify_worker_startup_contract")]
fn verify_worker_startup_contract(engine: &mut Engine) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let sink = engine
        .runtime
        .node_by_name("startup-sink")
        .expect("worker clone contains startup-sink");
    let node = engine
        .runtime
        .node_by_name("startup-node")
        .expect("worker clone contains startup-node");
    let sibling = engine
        .runtime
        .node_by_name("startup-sibling")
        .expect("worker clone contains startup-sibling");

    assert_eq!(sink, NodeId::new(0));
    assert_eq!(node, NodeId::new(1));
    assert_eq!(sibling, NodeId::new(2));
    assert_eq!(engine.runtime.nodes().node_next_slot(node, 0)?, sink);
    assert_eq!(engine.runtime.nodes().node_next_slot(sibling, 0)?, sink);
    assert_eq!(engine.runtime.nodes().node_siblings(node)?, vec![sibling]);
    assert_eq!(
        engine.runtime.nodes().node_state(node)?,
        NodeState::Disabled
    );
    assert_eq!(engine.runtime.buffers().frame_slots(), 5);

    let topology_error = engine
        .runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                startup_node_process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("worker-added-node", 0),
                &[],
                None,
            ),
        )
        .expect_err("worker init must not mutate graph topology");
    assert!(matches!(
        &topology_error,
        RuntimeError::GraphTopologyMutationFromWorker
    ));

    engine.set_worker_node_runtime_data(
        node,
        NodeRuntimeData::from_words([
            u64::try_from(worker.slot()).expect("worker slot fits u64") + 1,
            0,
            0,
            0,
        ]),
    )?;
    engine
        .runtime
        .nodes()
        .set_node_state(node, NodeState::Polling)?;
    let index = engine.runtime.alloc_index_with_bytes(&[0; 128])?;
    let mut handoff_frame = BufferFrame::with_capacity(1);
    handoff_frame.push_index(index)?;
    engine
        .runtime
        .handoff_indices(worker, STARTUP_NODE_HANDLE, &mut handoff_frame)?;
    assert_eq!(engine.runtime.run_ready_nodes()?, 1);

    let case = CASE.load(Ordering::Acquire);
    if case != READY && worker.slot() == 0 {
        while !engine.main_loop_exit_now.load(Ordering::Acquire) {
            spin_loop();
        }
        ABORT_OBSERVED.fetch_or(1, Ordering::Release);
    } else {
        match case {
            INIT_FAILURE if worker.slot() == 1 => return Err(topology_error),
            PANIC if worker.slot() == 1 => panic!("injected worker initialization panic"),
            EARLY_EXIT if worker.slot() == 1 => {
                engine.main_loop_exit_now.store(true, Ordering::Release);
                return Ok(());
            }
            _ => {}
        }
    }

    INITIALIZED.fetch_or(1 << worker.slot(), Ordering::Release);
    Ok(())
}

fn engine_pool() -> (EnginePool, tokio::runtime::Runtime) {
    let mut worker = Worker::default();
    worker.count = 2;
    worker.buffer.slot_bytes = 128;
    worker.buffer.slots_per_numa = 64;
    worker.buffer.frame_pool_size = 5;
    worker.buffer.page_size = Some(hammer_infra::PageSize::Default);
    worker.control.queue_capacity = 1;
    let mut engine =
        Engine::new_configured(RuntimeRegistry::new(), worker).expect("configured main engine");
    let sink = engine
        .runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                startup_node_process,
                NodeRuntimeData::empty(),
                NodeRegistration::next("startup-sink", 0),
                &[],
                None,
            ),
        )
        .expect("canonical startup sink");
    let node = engine
        .runtime
        .nodes()
        .register_internal_with_handle(STARTUP_NODE_HANDLE, StartupNode { next: [sink] })
        .expect("canonical startup node");
    let sibling = engine
        .runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                startup_node_process,
                NodeRuntimeData::empty(),
                NodeRegistration::sibling_of("startup-sibling", "startup-node"),
                &[],
                None,
            ),
        )
        .expect("canonical startup sibling");
    assert_eq!(sink, NodeId::new(0));
    assert_eq!(node, NodeId::new(1));
    assert_eq!(sibling, NodeId::new(2));
    engine
        .runtime
        .nodes()
        .set_node_state(node, NodeState::Disabled)
        .expect("canonical startup node state");

    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("control runtime");
    let pool = EnginePool::new(engine, &runtime).expect("engine pool");
    (pool, runtime)
}

fn reset(case: usize) {
    CASE.store(case, Ordering::Release);
    INITIALIZED.store(0, Ordering::Release);
    DISPATCHED.store(0, Ordering::Release);
    ABORT_OBSERVED.store(0, Ordering::Release);
}

fn stop_workers(pool: &mut EnginePool) {
    EnginePool::main_loop_exit(pool.main_engine());
    pool.close().expect("close worker pool");
}

#[test]
fn data_worker_startup_is_transactional() {
    let _serial = serialize_test();
    reset(READY);
    let (mut pool, _runtime) = engine_pool();
    start_workers(pool.main_engine_mut()).expect("transactional worker startup");
    assert_eq!(INITIALIZED.load(Ordering::Acquire), 0b11);
    assert_eq!(DISPATCHED.load(Ordering::Acquire), 0b11);
    assert_eq!(
        pool.main_engine()
            .runtime
            .nodes()
            .node_state(NodeId::new(1))
            .expect("main node state"),
        NodeState::Disabled
    );
    stop_workers(&mut pool);

    reset(INIT_FAILURE);
    let (mut pool, _runtime) = engine_pool();
    let error = start_workers(pool.main_engine_mut()).expect_err("worker init must fail startup");
    assert!(matches!(
        error,
        RuntimeError::WorkerExitedBeforeStartupBarrier { phase: "main-loop" }
    ));
    assert_eq!(INITIALIZED.load(Ordering::Acquire), 0b01);
    assert_eq!(ABORT_OBSERVED.load(Ordering::Acquire), 1);
    stop_workers(&mut pool);

    reset(PANIC);
    let (mut pool, _runtime) = engine_pool();
    let panic = catch_unwind(AssertUnwindSafe(|| start_workers(pool.main_engine_mut())))
        .expect_err("worker panic must unwind startup");
    assert_eq!(
        panic.downcast_ref::<&str>().copied(),
        Some("injected worker initialization panic")
    );
    assert_eq!(ABORT_OBSERVED.load(Ordering::Acquire), 1);
    stop_workers(&mut pool);

    reset(EARLY_EXIT);
    let (mut pool, _runtime) = engine_pool();
    let error = start_workers(pool.main_engine_mut()).expect_err("early exit must fail startup");
    assert!(matches!(
        error,
        RuntimeError::WorkerExitedBeforeStartupBarrier { phase: "main-loop" }
    ));
    assert_eq!(ABORT_OBSERVED.load(Ordering::Acquire), 1);
    stop_workers(&mut pool);
}

#[test]
fn runtime_main_loop_enter_catalog_starts_workers() {
    let _serial = serialize_test();
    reset(READY);
    let (mut pool, _runtime) = engine_pool();

    hammer_runtime::init::run_main_loop_enter(pool.main_engine_mut())
        .expect("run runtime main-loop-enter catalog");

    assert_eq!(INITIALIZED.load(Ordering::Acquire), 0b11);
    assert_eq!(DISPATCHED.load(Ordering::Acquire), 0b11);
    stop_workers(&mut pool);
}

#[test]
fn main_engine_schedules_bounded_control_work_on_the_selected_worker() {
    let _serial = serialize_test();
    reset(READY);
    let (mut pool, _runtime) = engine_pool();
    let error = pool
        .main_engine()
        .schedule_on_worker(DataWorkerId::new(0), || {})
        .expect_err("reject control work before worker startup");
    assert!(matches!(
        error,
        RuntimeError::WorkerControlUnavailable { worker } if worker == DataWorkerId::new(0)
    ));
    start_workers(pool.main_engine_mut()).expect("start data workers");

    let error = pool
        .main_engine()
        .schedule_on_worker(DataWorkerId::new(2), || {})
        .expect_err("reject control work for an unconfigured worker");
    assert!(matches!(
        error,
        RuntimeError::DataWorkerIndexOutOfRange {
            worker: 2,
            worker_count: 2,
        }
    ));

    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    pool.main_engine()
        .schedule_on_worker(DataWorkerId::new(0), move || {
            let thread = std::thread::current();
            started_tx
                .send(thread.name().expect("named Data Worker").to_owned())
                .expect("report selected worker");
            release_rx.recv().expect("release blocked worker task");
        })
        .expect("schedule worker control task");

    assert_eq!(
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("selected worker started control task"),
        "hammer-worker-1"
    );

    pool.main_engine()
        .schedule_on_worker(DataWorkerId::new(0), || {})
        .expect("fill bounded worker control queue");
    let error = pool
        .main_engine()
        .schedule_on_worker(DataWorkerId::new(0), || {})
        .expect_err("reject control work beyond configured capacity");
    assert!(matches!(
        error,
        RuntimeError::WorkerControlQueueFull {
            worker,
            capacity: 1,
        } if worker == DataWorkerId::new(0)
    ));

    release_tx.send(()).expect("release blocked worker task");
    EnginePool::main_loop_exit(pool.main_engine());
    pool.close().expect("close worker pool");
    let error = pool
        .main_engine()
        .schedule_on_worker(DataWorkerId::new(0), || {})
        .expect_err("reject control work after worker exit");
    assert!(matches!(
        error,
        RuntimeError::WorkerControlClosed { worker } if worker == DataWorkerId::new(0)
    ));
}
