use std::cell::RefCell;
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, NodeState};
use hammer_infra::pool::Index;
use hammer_infra::segment::{Local, Svm};
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::app::{AppSessionConfig, SessionEventQueue, SessionEvt, SessionEvtType};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, InternalNode, Node, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::session::node::{SessionQueueNext, SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionTransportId, SessionWorker, TransportInternalTransport,
    TransportInternalTx, dispatch_session_queue_pending,
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

impl SessionTransport<Index, Svm> for RecordingTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index, Svm>,
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
        _: &mut SessionWorker<Index, Svm>,
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

impl TransportInternalTransport<Index, Svm> for RecordingTransport {
    fn internal_tx(
        &mut self,
        _: &mut SessionWorker<Index, Svm>,
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
    sessions: SessionWorker<Index, Svm>,
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

fn dispatch_test_worker(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    with_test_worker_mut(|worker| {
        dispatch_session_queue_pending(
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

fn worker_engine() -> Engine {
    let main = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    main.spawn(1).expect("spawn data worker")
}

fn register_session_queue(
    engine: &mut Engine,
) -> (hammer_core::data_plane::NodeId, NodeRuntimeData) {
    let node = SessionQueueNode::new().expect("session queue node");
    let data = node.node_runtime_data().expect("session queue data");
    let id = engine
        .runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue node");
    (id, data)
}

#[test]
fn local_session_worker_does_not_register_file_readiness() {
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let (session_queue, _) = register_session_queue(&mut engine);
    let mut sessions = SessionWorker::<Index, Local>::new(worker, engine.runtime.buffers().clone());

    sessions
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install local queue readiness");
    let graph = engine.runtime.nodes().clone();

    assert_eq!(
        engine
            .file_main_mut()
            .poll(&graph)
            .expect("poll local session runtime"),
        0
    );
}

#[test]
fn svm_readiness_schedules_session_queue_before_the_node_drains_events() {
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let events = Arc::new(Mutex::new(Vec::new()));
    let (session_queue, node_data) = register_session_queue(&mut engine);
    let sink = engine.runtime.nodes().register_internal(BlackholeNode);
    let output_next = SessionQueueNode::compile_output_next(&engine.runtime, session_queue, sink)
        .expect("compile session queue transport edge");
    SessionQueueNode::install_worker_attachment(node_data, output_next, dispatch_test_worker)
        .expect("install session queue transport dispatch");

    let mut sessions = SessionWorker::<Index, Svm>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        AppSessionConfig::default(),
    );
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
        .set_node_state(session_queue, NodeState::Polling)
        .expect("poll session queue");

    with_test_worker_mut(|worker| {
        worker
            .sessions
            .app()
            .tx_evt_q()
            .enqueue_ctrl(SessionEvt::ctrl(
                session_id.pool_index().slot(),
                worker.sessions.worker().slot() as u32,
                SessionEvtType::Close,
            ))
            .expect("app writes close event");
        Ok(())
    })
    .expect("queue close event");

    let graph = engine.runtime.nodes().clone();
    let callbacks = engine
        .file_main_mut()
        .poll(&graph)
        .expect("poll SVM queue readiness");

    assert_eq!(callbacks, 1);
    assert!(engine.runtime.nodes().has_pending());
    assert!(events.lock().expect("events").is_empty());
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
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let (session_queue, _) = register_session_queue(&mut engine);
    let mut first = SessionWorker::<Index, Svm>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        AppSessionConfig::default(),
    );
    first
        .app()
        .tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            1,
            worker.slot() as u32,
            SessionEvtType::Close,
        ))
        .expect("signal first worker");
    first
        .install_queue_readiness(&mut engine, session_queue)
        .expect("install first readiness");
    first
        .remove_queue_readiness(&mut engine)
        .expect("remove first readiness before queue release");
    drop(first);

    let mut second = SessionWorker::<Index, Svm>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        AppSessionConfig::default(),
    );
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
    second
        .app()
        .tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            2,
            worker.slot() as u32,
            SessionEvtType::Close,
        ))
        .expect("signal replacement worker");
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
        let mut engine = worker_engine();
        let worker = engine.data_worker_id().expect("data worker id");
        let (session_queue, _) = register_session_queue(&mut engine);
        let mut sessions = SessionWorker::<Index, Svm>::new_svm(
            worker,
            engine.runtime.buffers().clone(),
            AppSessionConfig::default(),
        );
        let descriptor = sessions
            .app()
            .tx_evt_q()
            .read_fd()
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
