use std::cell::RefCell;
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, NodeId, NodeState};
use hammer_infra::pool::Index;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::app::AppSessionConfig;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, InternalNode, Node, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::ApplicationMain;
use hammer_service::session::node::{
    SessionQueueNext, SessionQueueNode, SessionQueueOutput, register_app_session_input_node,
    register_session_queue_node,
};
use hammer_service::session::runtime::{
    SessionMain, SessionTransport, SessionTransportId, SessionWorker, TransportInternalTransport,
    TransportInternalTx, dispatch_session_queue_events, install_session_worker,
};

#[derive(Default)]
struct BlackholeNode;

impl Node for BlackholeNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        |_, _, _| NodeResult::drop()
    }
}

impl InternalNode for BlackholeNode {}

fn descriptor_identity(fd: RawFd) -> std::io::Result<(libc::dev_t, libc::ino_t)> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `fstat` initializes all fields before the value is read.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } == 0 {
        // SAFETY: the successful `fstat` call initialized `status`.
        let status = unsafe { status.assume_init() };
        Ok((status.st_dev, status.st_ino))
    } else {
        Err(std::io::Error::last_os_error())
    }
}

struct RecordingTransport(Arc<Mutex<Vec<&'static str>>>);

impl SessionTransport<Index> for RecordingTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.0.lock().expect("events").push("time");
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.0.lock().expect("events").push("close");
        Ok(())
    }
}

impl TransportInternalTransport<Index> for RecordingTransport {
    fn internal_tx(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: hammer_service::session::SessionId,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

struct TestWorker {
    sessions: SessionWorker<Index>,
    transport: RecordingTransport,
}

thread_local! {
    static TEST_WORKER: RefCell<Option<TestWorker>> = const { RefCell::new(None) };
}

fn install_test_worker(worker: TestWorker) {
    TEST_WORKER.with(|slot| *slot.borrow_mut() = Some(worker));
}

fn with_test_worker_mut<R>(
    f: impl FnOnce(&mut TestWorker) -> RuntimeResult<R>,
) -> RuntimeResult<R> {
    TEST_WORKER.with(|slot| {
        let mut slot = slot.borrow_mut();
        f(slot.as_mut().ok_or_else(RuntimeError::service_closed)?)
    })
}

fn take_test_worker() -> TestWorker {
    TEST_WORKER.with(|slot| slot.borrow_mut().take().expect("test worker"))
}

fn update_test_worker(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    with_test_worker_mut(|worker| {
        worker.transport.update_time(
            &mut worker.sessions,
            runtime,
            output_next,
            frame,
            output,
            now,
        )
    })
}

fn dispatch_test_worker(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    with_test_worker_mut(|worker| {
        dispatch_session_queue_events(
            runtime,
            &mut worker.sessions,
            &mut worker.transport,
            output_next,
            frame,
            output,
            now,
        )
        .map(|_| ())
    })
}

fn worker_engine() -> (Engine, NodeId, NodeRuntimeData, SessionQueueNext) {
    let main = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    let session_queue = register_session_queue_node(&main.runtime).expect("register session queue");
    let app_session_input =
        register_app_session_input_node(&main.runtime).expect("register app session input");
    let sink = main.runtime.nodes().register_internal(BlackholeNode);
    SessionQueueNode::compile_output_next(&main.runtime, session_queue, sink)
        .expect("compile session queue transport edge");
    main.runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)
        .expect("disable main session queue");

    let mut worker = main.spawn(1).expect("spawn data worker");
    let session_main = Arc::new(SessionMain::new(1, ApplicationMain::new(1)));
    let worker_id = worker.data_worker_id().expect("data worker id");
    let sessions = SessionWorker::<Index>::new(worker_id).expect("session worker for test");
    install_session_worker(
        &session_main,
        &mut worker,
        app_session_input,
        session_queue,
        sessions,
    )
    .expect("install Session worker");
    let node_data = worker
        .runtime
        .nodes()
        .node_runtime_data(session_queue)
        .expect("worker session queue data");
    let output_next = SessionQueueNode::existing_output_next(&worker.runtime, session_queue, sink)
        .expect("resolve worker session queue transport edge");
    (worker, session_queue, node_data, output_next)
}

#[test]
fn session_worker_readiness_is_idle_before_signal() {
    let (mut engine, session_queue, _, _) = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let mut sessions = SessionWorker::<Index>::new(worker).expect("session worker for test");

    sessions
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install queue readiness");
    let graph = engine.runtime.nodes().clone();

    assert_eq!(
        engine
            .file_main_mut()
            .poll(&graph)
            .expect("poll idle session runtime"),
        0
    );
}

#[test]
fn svm_readiness_marks_session_queue_before_main_loop_dispatch() {
    let (mut engine, session_queue, node_data, output_next) = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let events = Arc::new(Mutex::new(Vec::new()));
    SessionQueueNode::install_worker_attachment(
        &engine.runtime,
        node_data,
        output_next,
        update_test_worker,
        dispatch_test_worker,
    )
    .expect("install session queue transport dispatch");

    let mut sessions =
        SessionWorker::<Index>::with_app_session_config(worker, AppSessionConfig::default())
            .expect("session worker for test");
    let session_id = sessions.insert_session_for_test(RecordingTransport::ID, Index::new(7, 1));
    sessions.schedule_disconnect(session_id);
    sessions
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install SVM queue readiness");
    install_test_worker(TestWorker {
        sessions,
        transport: RecordingTransport(Arc::clone(&events)),
    });
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Interrupt)
        .expect("enable session queue interrupts");

    with_test_worker_mut(|worker| {
        worker.sessions.signal_queue();
        Ok(())
    })
    .expect("queue close event");

    let graph = engine.runtime.nodes().clone();
    let callbacks = engine
        .file_main_mut()
        .poll(&graph)
        .expect("poll SVM queue readiness");

    assert_eq!(callbacks, 1);
    assert!(!engine.runtime.nodes().has_pending());
    assert!(
        !engine
            .runtime
            .nodes()
            .mark_interrupt_pending(session_queue)
            .expect("readiness interrupt coalesces")
    );
    assert!(events.lock().expect("events").is_empty());
    engine
        .runtime
        .schedule_empty_frame(session_queue)
        .expect("main loop schedules marked session queue");
    assert!(engine.runtime.nodes().has_pending());
    assert!(engine.runtime.run_ready_nodes().expect("run session queue") >= 1);
    assert_eq!(*events.lock().expect("events"), vec!["time", "close"]);

    let mut worker = take_test_worker();
    worker
        .sessions
        .remove_queue_readiness(&mut engine)
        .expect("remove SVM queue readiness");
}

#[test]
fn replacing_svm_session_worker_removes_the_old_file_before_queue_release() {
    let (mut engine, session_queue, _, _) = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let mut first =
        SessionWorker::<Index>::with_app_session_config(worker, AppSessionConfig::default())
            .expect("session worker for test");
    first.signal_queue();
    first
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install first readiness");
    first
        .remove_queue_readiness(&mut engine)
        .expect("remove first readiness before queue release");
    drop(first);

    let mut second =
        SessionWorker::<Index>::with_app_session_config(worker, AppSessionConfig::default())
            .expect("session worker for test");
    second
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install replacement readiness");

    let graph = engine.runtime.nodes().clone();
    assert_eq!(
        engine
            .file_main_mut()
            .poll(&graph)
            .expect("old readiness must be removed"),
        0
    );
    second.signal_queue();
    assert_eq!(
        engine
            .file_main_mut()
            .poll(&graph)
            .expect("poll replacement readiness"),
        1
    );
    second
        .remove_queue_readiness(&mut engine)
        .expect("remove replacement readiness");
}

#[test]
fn worker_teardown_closes_session_queue_and_file_descriptors() {
    let (descriptor, identity) = thread::spawn(|| {
        let (mut engine, session_queue, _, _) = worker_engine();
        let worker = engine.data_worker_id().expect("data worker id");
        let mut sessions =
            SessionWorker::<Index>::with_app_session_config(worker, AppSessionConfig::default())
                .expect("session worker for test");
        let descriptor = sessions
            .queue_signal_descriptor()
            .expect("SVM queue read descriptor");
        let identity = descriptor_identity(descriptor).expect("queue descriptor identity");
        sessions
            .install_queue_readiness(&mut engine, session_queue)
            .expect("install queue readiness");
        (descriptor, identity)
    })
    .join()
    .expect("session worker thread");

    match descriptor_identity(descriptor) {
        Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EBADF)),
        Ok(reused) => assert_ne!(reused, identity, "queue descriptor remains open"),
    }
}
