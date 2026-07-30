use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;
use std::time::Duration;

use hammer_runtime::DataPlaneBufferConfig;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, ProcessContext, ProcessWake,
};

hammer_runtime::__declare_registration_image!(
    init_functions = [];
    config_functions = [];
    early_config_functions = [];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [
        __PROCESS_NODE_PANICKING_PROCESS_RUNTIME_TEST,
        __PROCESS_NODE_PROCESS_RUNTIME_TEST,
    ];
    session_transports = [];
    app_session_protocols = [];
    binary_api_methods = [];
);

static OBSERVED_THREADS: OnceLock<Mutex<Vec<ThreadId>>> = OnceLock::new();
static PANICKING_PROCESS_RAN: AtomicBool = AtomicBool::new(false);
static CLOCK_OBSERVED: AtomicBool = AtomicBool::new(false);

#[hammer_component_macros::process_node(name = "panicking-process-runtime-test")]
async fn panicking_process_runtime_test(_: ProcessContext) -> RuntimeResult<()> {
    PANICKING_PROCESS_RAN.store(true, Ordering::Release);
    panic!("injected Process Node panic");
}

#[hammer_component_macros::process_node(name = "process-runtime-test")]
async fn process_runtime_test(mut context: ProcessContext) -> RuntimeResult<()> {
    assert_eq!(
        context
            .wait_for_event_or_clock(Duration::from_millis(1))
            .await,
        ProcessWake::Clock
    );
    OBSERVED_THREADS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("observed thread registry")
        .push(std::thread::current().id());
    CLOCK_OBSERVED.store(true, Ordering::Release);

    let wake = context
        .wait_for_event_or_clock(Duration::from_secs(1))
        .await;
    assert_eq!(wake.event_type(), Some(7));
    assert_eq!(wake.event_data(), &[11, 13]);
    OBSERVED_THREADS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("observed thread registry")
        .push(std::thread::current().id());
    Ok(())
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
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    engine
        .plugin_main_mut()
        .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
    engine
}

#[test]
fn process_clock_and_events_run_only_on_main_thread() {
    OBSERVED_THREADS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("observed thread registry")
        .clear();
    PANICKING_PROCESS_RAN.store(false, Ordering::Relaxed);
    CLOCK_OBSERVED.store(false, Ordering::Relaxed);
    let main_thread = std::thread::current().id();
    let mut engine = test_engine();
    engine.start_process_nodes().expect("start process nodes");
    engine
        .start_process_nodes()
        .expect("starting the same Process Node inventory is idempotent");
    let process = engine
        .process_handle("process-runtime-test")
        .expect("registered process handle");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("main runtime");

    engine.run_processes_until(&runtime, async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !CLOCK_OBSERVED.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Process Node clock wake");
        process.signal(7, 11).expect("signal first event datum");
        process.signal(7, 13).expect("signal second event datum");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observed = OBSERVED_THREADS
                    .get()
                    .expect("observed thread registry")
                    .lock()
                    .expect("observed thread registry")
                    .len();
                if observed == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Process Node event wake");
    });
    engine
        .shutdown_process_nodes(&runtime)
        .expect("join process nodes");

    let observed = OBSERVED_THREADS
        .get()
        .expect("observed thread registry")
        .lock()
        .expect("observed thread registry")
        .clone();
    assert_eq!(observed, vec![main_thread, main_thread]);
    assert!(PANICKING_PROCESS_RAN.load(Ordering::Acquire));
}
