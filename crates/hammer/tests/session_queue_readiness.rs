use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, NodeState};
use hammer_core::error::CoreResult;
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::pool::Index;
use hammer_infra::segment::{Local, Svm};
use hammer_runtime::app::{AppSessionConfig, SessionEventQueue, SessionEvt, SessionEvtType};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, InternalNode, Node, NodeProcessFn, NodeResult,
};
use hammer_service::session::node::{
    SessionQueueNext, SessionQueueNode, SessionQueueOutput, register_session_queue,
};
use hammer_service::session::runtime::{
    SessionDriverRuntime, SessionTransport, SessionTransportId, SessionWorker,
    TransportInternalTransport, TransportInternalTx, dispatch_registered_session_queue_once_at,
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
    // SAFETY: status is writable storage for one stat, and a successful fstat
    // initializes it completely before assume_init below.
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } == 0 {
        // SAFETY: fstat succeeded and initialized the complete stat value.
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
    ) -> CoreResult<()> {
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
    ) -> CoreResult<()> {
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
    ) -> CoreResult<()> {
        Ok(())
    }
}

fn worker_engine() -> Engine {
    let main = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    main.spawn(1).expect("spawn data worker")
}

#[test]
fn local_session_runtime_does_not_register_file_readiness() {
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let node = SessionQueueNode::new().expect("session queue node");
    let node_data = node
        .node_runtime_data()
        .expect("session queue runtime data");
    let session_queue = engine
        .runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue node");
    let driver =
        SessionDriverRuntime::<(), Local, Index>::new(worker, engine.runtime.buffers().clone(), ());

    let _ = register_session_queue(&mut engine, session_queue, node_data, driver)
        .expect("bind local session runtime");

    assert_eq!(
        engine
            .file_main_mut()
            .expect("worker FileMain")
            .poll()
            .expect("poll local session runtime"),
        0
    );
}

#[test]
fn svm_readiness_schedules_session_queue_before_the_node_drains_events() {
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut driver = SessionDriverRuntime::<_, Svm, Index>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        (RecordingTransport(Arc::clone(&events)), ()),
        AppSessionConfig::default(),
    );
    let session_id = driver
        .insert_session_with_transport(RecordingTransport::ID, |_, _| Ok(Index::new(7, 1)))
        .expect("create session");
    let node = SessionQueueNode::new().expect("session queue node");
    let node_data = node
        .node_runtime_data()
        .expect("session queue runtime data");
    let session_queue = engine
        .runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue node");
    let sink = engine.runtime.nodes().register_internal(BlackholeNode);
    let handle = register_session_queue(&mut engine, session_queue, node_data, driver)
        .expect("bind SVM session runtime");
    SessionQueueNode::attach_queue_by_runtime_data(
        &engine.runtime,
        session_queue,
        node_data,
        handle,
        sink,
        dispatch_registered_session_queue_once_at::<(RecordingTransport, ()), Svm, Index>,
    )
    .expect("attach session queue");
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)
        .expect("poll session queue");
    handle
        .borrow_mut()
        .expect("session runtime")
        .app()
        .tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            worker.slot() as u32,
            SessionEvtType::Close,
        ))
        .expect("app writes close event");

    engine.install_current();
    let callbacks = engine
        .file_main_mut()
        .expect("worker FileMain")
        .poll()
        .expect("poll SVM queue readiness");
    Engine::uninstall_current();

    assert_eq!(callbacks, 1);
    assert!(engine.runtime.nodes().has_pending());
    assert!(events.lock().expect("events").is_empty());
    assert!(engine.runtime.run_ready_nodes().expect("run session queue") >= 1);
    assert_eq!(*events.lock().expect("events"), vec!["time", "close"]);
}

#[test]
fn replacing_svm_session_runtime_removes_the_old_file_before_queue_release() {
    let mut engine = worker_engine();
    let worker = engine.data_worker_id().expect("data worker id");
    let node = SessionQueueNode::new().expect("session queue node");
    let node_data = node
        .node_runtime_data()
        .expect("session queue runtime data");
    let session_queue = engine
        .runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue node");
    let first = SessionDriverRuntime::<(), Svm, Index>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        (),
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
        .expect("signal first runtime");
    let _ = register_session_queue(&mut engine, session_queue, node_data, first)
        .expect("bind first SVM session runtime");

    let second = SessionDriverRuntime::<(), Svm, Index>::new_svm(
        worker,
        engine.runtime.buffers().clone(),
        (),
        AppSessionConfig::default(),
    );
    let handle = register_session_queue(&mut engine, session_queue, node_data, second)
        .expect("replace SVM session runtime");

    engine.install_current();
    assert_eq!(
        engine
            .file_main_mut()
            .expect("worker FileMain")
            .poll()
            .expect("old readiness must be removed"),
        0
    );
    handle
        .borrow_mut()
        .expect("replacement runtime")
        .app()
        .tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            2,
            worker.slot() as u32,
            SessionEvtType::Close,
        ))
        .expect("signal replacement runtime");
    assert_eq!(
        engine
            .file_main_mut()
            .expect("worker FileMain")
            .poll()
            .expect("poll replacement readiness"),
        1
    );
    Engine::uninstall_current();
}

#[test]
fn worker_teardown_closes_session_queue_and_file_descriptors() {
    let (descriptor, identity) = thread::spawn(|| {
        let mut engine = worker_engine();
        let worker = engine.data_worker_id().expect("data worker id");
        let node = SessionQueueNode::new().expect("session queue node");
        let node_data = node
            .node_runtime_data()
            .expect("session queue runtime data");
        let session_queue = engine
            .runtime
            .nodes()
            .try_register_driver(node)
            .expect("register session queue node");
        let driver = SessionDriverRuntime::<(), Svm, Index>::new_svm(
            worker,
            engine.runtime.buffers().clone(),
            (),
            AppSessionConfig::default(),
        );
        let descriptor = driver
            .app()
            .tx_evt_q()
            .read_fd()
            .expect("SVM queue read descriptor");
        let identity = descriptor_identity(descriptor).expect("queue descriptor identity");
        let _ = register_session_queue(&mut engine, session_queue, node_data, driver)
            .expect("bind SVM session runtime");
        (descriptor, identity)
    })
    .join()
    .expect("session worker thread");

    match descriptor_identity(descriptor) {
        Err(error) => assert_eq!(error.raw_os_error(), Some(libc::EBADF)),
        Ok(reused) => assert_ne!(reused, identity, "queue descriptor remains open"),
    }
}
