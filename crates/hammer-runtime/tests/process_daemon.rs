//! Daemon-shaped Process Node driving: the daemon's service futures sleep
//! while the Engine drives registered Process Nodes on the main-thread
//! LocalSet.
//!
//! Each test owns disjoint atomic state; the two tests never touch a shared
//! static, so parallel execution cannot race.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::ThreadId;
use std::time::Duration;

use hammer_runtime::__private::RegistrationImage;
use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, ProcessContext, ProcessWake,
};

/// Test 1: clock wakes while the daemon service future is dormant.
static DAEMON_CLOCK_WAKES: AtomicU32 = AtomicU32::new(0);
/// Test 1: event wakes, and whether they resumed on the main thread.
static DAEMON_EVENT_WAKES: AtomicU32 = AtomicU32::new(0);
static DAEMON_EVENT_ON_MAIN: AtomicBool = AtomicBool::new(false);
static DAEMON_MAIN_THREAD: OnceLock<ThreadId> = OnceLock::new();
/// Test 2: the base node keeps its clock cadence after a late registration.
static STILL_DRIVEN_CLOCK_WAKES: AtomicU32 = AtomicU32::new(0);
/// Test 2: the late-added node is driven in the same LocalSet.
static LATE_ADDED_CLOCK_WAKES: AtomicU32 = AtomicU32::new(0);

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [__PROCESS_NODE_DAEMON_WAIT_TEST];
    session_transports = [];
    session_apps = [];
    binary_api_methods = [];
);

/// An image carrying only Process Nodes, expanding to the same empty-slice
/// layout `__declare_registration_image!` produces, so the positional
/// `RegistrationImage::new` shape exists in exactly one place in this test.
macro_rules! process_image {
    ($($node:path),+ $(,)?) => {
        RegistrationImage::new(
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            &[$($node),+],
            &[],
            &[],
            &[],
        )
    };
}

/// Test 2's base image: its own node, so its clock-wake counter is disjoint
/// from test 1's daemon-wait-test node.
static STILL_DRIVEN_IMAGE: RegistrationImage = process_image!(__PROCESS_NODE_STILL_DRIVEN_TEST);

/// A later registration image carrying one additional Process Node, mimicking
/// a runtime hot-add generation.
static LATE_ADDED_IMAGE: RegistrationImage =
    process_image!(__PROCESS_NODE_LATE_ADDED_PROCESS_RUNTIME_TEST);

#[hammer_component_macros::process_node(name = "late-added-process-runtime-test")]
async fn late_added_process_runtime_test(mut context: ProcessContext) -> RuntimeResult<()> {
    loop {
        match context
            .wait_for_event_or_clock(Duration::from_millis(1))
            .await
        {
            ProcessWake::Clock => {
                LATE_ADDED_CLOCK_WAKES.fetch_add(1, Ordering::Release);
            }
            ProcessWake::Event(_) => {}
        }
    }
}

#[hammer_component_macros::process_node(name = "daemon-wait-test")]
async fn daemon_wait_test(mut context: ProcessContext) -> RuntimeResult<()> {
    loop {
        match context
            .wait_for_event_or_clock(Duration::from_millis(1))
            .await
        {
            ProcessWake::Clock => {
                DAEMON_CLOCK_WAKES.fetch_add(1, Ordering::Release);
            }
            ProcessWake::Event(_) => {
                DAEMON_EVENT_WAKES.fetch_add(1, Ordering::Release);
                if DAEMON_MAIN_THREAD
                    .get()
                    .is_some_and(|main| std::thread::current().id() == *main)
                {
                    DAEMON_EVENT_ON_MAIN.store(true, Ordering::Release);
                }
            }
        }
    }
}

#[hammer_component_macros::process_node(name = "still-driven-test")]
async fn still_driven_test(mut context: ProcessContext) -> RuntimeResult<()> {
    loop {
        match context
            .wait_for_event_or_clock(Duration::from_millis(1))
            .await
        {
            ProcessWake::Clock => {
                STILL_DRIVEN_CLOCK_WAKES.fetch_add(1, Ordering::Release);
            }
            ProcessWake::Event(_) => {}
        }
    }
}

fn test_engine() -> Engine {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 16,
            frame_slots: 16,
            ..DataPlaneBufferConfig::default()
        },
        ..DataPlaneRuntimeConfig::default()
    });
    Engine::new(runtime, RuntimeRegistry::new())
}

fn main_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("main runtime")
}

#[test]
fn daemon_dormant_services_still_drive_registered_process_nodes() {
    DAEMON_CLOCK_WAKES.store(0, Ordering::Relaxed);
    DAEMON_EVENT_WAKES.store(0, Ordering::Relaxed);
    DAEMON_EVENT_ON_MAIN.store(false, Ordering::Relaxed);
    let _ = DAEMON_MAIN_THREAD.set(std::thread::current().id());

    let mut engine = test_engine();
    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    engine.start_process_nodes().expect("start process nodes");
    let process = engine
        .process_handle("daemon-wait-test")
        .expect("registered process handle");
    let runtime = main_runtime();

    // The daemon service future sleeps; the LocalSet must still poll the node
    // on its clock wakes.
    engine.run_processes_until(&runtime, async {
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    assert!(
        DAEMON_CLOCK_WAKES.load(Ordering::Acquire) >= 3,
        "Process Node clock wakes while the service future is dormant"
    );

    process.signal(7, 42).expect("signal typed event");
    engine.run_processes_until(&runtime, async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    assert_eq!(
        DAEMON_EVENT_WAKES.load(Ordering::Acquire),
        1,
        "signal was delivered as an event wake"
    );
    assert!(
        DAEMON_EVENT_ON_MAIN.load(Ordering::Acquire),
        "event resumed on the main thread"
    );

    engine
        .shutdown_process_nodes(&runtime)
        .expect("shutdown process nodes");
    assert!(
        process.signal(7, 42).is_err(),
        "shutdown aborts and joins, leaving no running node"
    );
}

#[test]
fn runtime_added_registration_process_node_executes_in_same_local_set() {
    STILL_DRIVEN_CLOCK_WAKES.store(0, Ordering::Relaxed);
    LATE_ADDED_CLOCK_WAKES.store(0, Ordering::Relaxed);

    let mut engine = test_engine();
    engine
        .plugin_main_mut()
        .register_builtin_image(&STILL_DRIVEN_IMAGE);
    engine.start_process_nodes().expect("start process nodes");
    let runtime = main_runtime();

    // Hot-add: a later registration image adds a Process Node, and the
    // idempotent start re-enters to spawn it alongside the running set.
    engine
        .plugin_main_mut()
        .register_builtin_image(&LATE_ADDED_IMAGE);
    engine.start_process_nodes().expect("start late-added node");
    let late = engine
        .process_handle("late-added-process-runtime-test")
        .expect("late-added process handle");

    engine.run_processes_until(&runtime, async {
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    assert!(
        LATE_ADDED_CLOCK_WAKES.load(Ordering::Acquire) >= 1,
        "late-added node was driven in the running LocalSet"
    );
    assert!(
        STILL_DRIVEN_CLOCK_WAKES.load(Ordering::Acquire) >= 1,
        "original node stays driven in the same LocalSet"
    );

    engine
        .shutdown_process_nodes(&runtime)
        .expect("shutdown process nodes");
    assert!(
        late.signal(1, 0).is_err(),
        "late-added node aborted and joined on shutdown"
    );
}
