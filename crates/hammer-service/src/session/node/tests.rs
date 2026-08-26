use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, DataPlaneBuffers, NodeRegistration};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, InternalNode, Node, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use super::{SessionQueueNext, SessionQueueNode};
use crate::session::ApplicationMain;
use crate::session::error::SessionError;
use crate::session::runtime::{
    SessionPacketizedTransport, SessionPacketizedTx, SessionTransport, SessionTransportId,
    SessionWorker, TransportInternalTransport, TransportInternalTx, TransportSendFlags,
    TransportSendParams, TxBatchBuffer, dispatch_session_queue_once,
};
use hammer_runtime::app::AppSession;
use hammer_runtime::app::{SessionEvt, SessionEvtType};

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

fn test_session_queue_next(
    runtime: &DataPlaneRuntime,
    output: hammer_core::data_plane::NodeId,
) -> (hammer_core::data_plane::NodeId, SessionQueueNext) {
    let node = SessionQueueNode::new().expect("session queue node");
    let owner = runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue");
    let slot = runtime
        .nodes()
        .add_node_next_slot(owner, output)
        .expect("session queue next");
    (owner, SessionQueueNext::from_slot(slot))
}

#[derive(Default)]
struct RecordingTransport {
    events: Vec<&'static str>,
    sampled_times: Vec<Instant>,
    tx_indexes: Vec<u32>,
}

impl SessionTransport<u32> for RecordingTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    fn app_rx_evt(
        &mut self,
        _: u32,
        _: usize,
        _: usize,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        self.events.push("app_rx");
        Ok(false)
    }

    fn update_time(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.events.push("transport_time");
        self.sampled_times.push(now);
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.events.push("control");
        Ok(())
    }

    fn reset(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.events.push("reset");
        Ok(())
    }
}

fn session_worker_for_test() -> SessionWorker<u32> {
    SessionWorker::new(
        DataWorkerId::new(0),
        1,
        hammer_runtime::app::AppSessionConfig::default(),
        1024,
        ApplicationMain::new(1024),
        None,
    )
    .expect("session worker for test")
}

impl TransportInternalTransport<u32> for RecordingTransport {
    fn internal_tx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: crate::session::SessionId,
        index: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.events.push("io");
        self.tx_indexes.push(index);
        Ok(())
    }
}

#[derive(Default)]
struct TcpTransport {
    tx_actions: Vec<u32>,
}

impl SessionTransport<u32> for TcpTransport {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(10);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl SessionPacketizedTransport<u32> for TcpTransport {
    fn control_tx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: usize,
        _: Instant,
    ) -> RuntimeResult<TransportSendParams> {
        Ok(TransportSendParams {
            snd_space: 1,
            tx_offset: 0,
            send_goal_size: 1,
            flags: TransportSendFlags::default(),
        })
    }

    fn tx_action(
        &mut self,
        index: u32,
        _: &[TxBatchBuffer],
        _: &DataPlaneBuffers,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.tx_actions.push(index);
        Ok(())
    }
}

#[test]
fn app_reset_dispatch_invokes_transport_reset_distinct_from_close() -> RuntimeResult<()> {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut sessions = session_worker_for_test();
    let mut transport = RecordingTransport::default();
    let session_id = sessions.insert_session_for_test(RecordingTransport::ID, 4u32);
    let app = attach_local_app_session(&mut sessions, session_id);
    app.app_rx_mq()
        .enqueue_ctrl(SessionEvt::ctrl(
            app.session_index(),
            0,
            SessionEvtType::Reset,
        ))
        .expect("enqueue app reset");
    sessions.poll_app().expect("stage app reset");
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch app reset");

    assert!(transport.events.iter().any(|event| *event == "reset"));
    assert!(!transport.events.iter().any(|event| *event == "control"));
    Ok(())
}

#[test]
fn session_queue_updates_transport_before_control_and_io_and_without_events() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let mut sessions = session_worker_for_test();
    let mut transport = RecordingTransport::default();
    let session_id = sessions.insert_session_for_test(RecordingTransport::ID, 9u32);
    let app = attach_local_app_session(&mut sessions, session_id);
    let session_rx_fifo = Arc::clone(sessions.session_fifos(session_id).expect("Session FIFOs").0);
    assert_eq!(session_rx_fifo.enqueue(b"x"), 1);
    session_rx_fifo.want_deq_notification();
    assert_eq!(session_rx_fifo.dequeue_drop(1), 1);
    app.app_rx_mq()
        .enqueue_io(SessionEvt::io(app.session_index(), SessionEvtType::RxDeq))
        .expect("enqueue rx dequeue");
    sessions.schedule_disconnect(session_id);
    sessions.mark_ready(session_id);
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let step = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch queue");

    assert_eq!(step.scheduled_sessions, 1);
    assert_eq!(
        transport.events,
        vec!["transport_time", "control", "io", "app_rx"]
    );
    assert_eq!(transport.sampled_times.len(), 1);

    transport.events.clear();
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch empty queue");
    assert_eq!(transport.events, vec!["transport_time"]);
    assert_eq!(transport.sampled_times.len(), 2);
}

#[test]
fn session_queue_dispatches_new_io_before_old_io() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut sessions = session_worker_for_test();
    let mut transport = TcpTransport::default();
    let old_index = 1u32;
    let new_index = 2u32;
    let old_session = sessions.insert_session_for_test(TcpTransport::ID, old_index);
    let new_session = sessions.insert_session_for_test(TcpTransport::ID, new_index);
    let old_app = attach_local_app_session(&mut sessions, old_session);
    let new_app = attach_local_app_session(&mut sessions, new_session);
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    old_app.send_bytes(b"old").expect("enqueue old TX bytes");
    sessions.poll_app().expect("stage old TX event");
    sessions.new_io_events.clear();
    sessions.old_io_events.clear();
    sessions.old_io_events.push_back(SessionEvt::io(
        old_session.pool_index(),
        SessionEvtType::TxEnq,
    ));
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch old IO once");
    new_app.send_bytes(b"n").expect("enqueue new TX byte");
    sessions.poll_app().expect("stage new TX event");
    sessions.new_io_events.clear();
    sessions.new_io_events.push_back(SessionEvt::io(
        new_session.pool_index(),
        SessionEvtType::TxEnq,
    ));
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch new before old IO");

    assert_eq!(transport.tx_actions, vec![old_index, new_index, old_index]);
}

#[test]
fn default_connection_index_returns_exact_transport_index() {
    let index = 2u32;
    let transport = TcpTransport::default();

    assert_eq!(
        transport
            .connection_index(index)
            .expect("exact transport index"),
        index
    );
}

fn attach_local_app_session(
    sessions: &mut SessionWorker<u32>,
    session_id: crate::session::SessionId,
) -> Arc<AppSession> {
    let app = sessions
        .local_app()
        .app_session(session_id)
        .cloned()
        .expect("application App Session");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(app.poll_events(&mut events), 1);
    assert_eq!(events[0].evt_type, SessionEvtType::Connect);
    app
}

#[test]
fn session_queue_connects_transport_session_directly_to_application() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut sessions = SessionWorker::<u32>::new(
        DataWorkerId::new(0),
        1,
        hammer_runtime::app::AppSessionConfig::new(8, 16),
        1024,
        ApplicationMain::new(1024),
        None,
    )
    .expect("session worker");
    let mut transport = RecordingTransport::default();
    let session_id = sessions.insert_session_for_test(RecordingTransport::ID, 10u32);
    let application = attach_local_app_session(&mut sessions, session_id);
    let (session_rx_fifo, session_tx_fifo) = {
        let (rx_fifo, tx_fifo) = sessions.session_fifos(session_id).expect("Session FIFOs");
        (Arc::clone(rx_fifo), Arc::clone(tx_fifo))
    };
    assert!(Arc::ptr_eq(application.rx_fifo(), &session_rx_fifo));
    assert!(Arc::ptr_eq(application.tx_fifo(), &session_tx_fifo));
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let ingress = runtime
        .alloc_index_with_bytes(b"request")
        .expect("transport RX buffer");
    sessions
        .enqueue_rx(runtime.buffers(), session_id, ingress, 0, false)
        .expect("transport RX enqueue");
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("ingress dispatch");

    let mut received = [0_u8; 8];
    assert_eq!(application.recv_bytes(&mut received), 7);
    assert_eq!(&received[..7], b"request");
    assert_eq!(session_rx_fifo.max_dequeue(), 7);
    assert_eq!(application.consume_rx(7), 7);

    application.send_bytes(b"reply").expect("application TX");
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("egress dispatch");
    let mut transmitted = [0_u8; 5];
    assert_eq!(
        session_tx_fifo.peek(0, transmitted.len(), &mut transmitted),
        5
    );
    assert_eq!(&transmitted, b"reply");
    assert_eq!(application.tx_fifo().max_dequeue(), 5);
}

#[test]
fn direct_session_notifies_application_after_tx_space_opens() {
    let mut sessions = SessionWorker::<u32>::new(
        DataWorkerId::new(0),
        1,
        hammer_runtime::app::AppSessionConfig::new(8, 16),
        1024,
        ApplicationMain::new(1024),
        None,
    )
    .expect("session worker");
    let session_id = sessions.insert_session_for_test(RecordingTransport::ID, 11u32);
    let application = attach_local_app_session(&mut sessions, session_id);
    let session_tx_fifo = Arc::clone(sessions.session_fifos(session_id).expect("Session FIFOs").1);

    assert_eq!(
        application.send_bytes(b"12345678").expect("fill TX FIFO"),
        8
    );
    application.want_tx_notification();
    assert_eq!(application.send_bytes(b"cd").expect("full TX FIFO"), 0);

    sessions
        .ack_tx_up_to(session_id, 8)
        .expect("transport TX dequeue");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(application.poll_events(&mut events), 1);
    assert_eq!(events[0].evt_type, SessionEvtType::TxDeq);
    assert_eq!(application.send_bytes(b"cd").expect("retry TX"), 2);
    let mut transmitted = [0_u8; 2];
    assert_eq!(
        session_tx_fifo.peek(0, transmitted.len(), &mut transmitted),
        2
    );
    assert_eq!(&transmitted, b"cd");
}

struct PayloadCaptureNode {
    runtime_data: NodeRuntimeData,
}

impl PayloadCaptureNode {
    fn new(packets: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        let mut states = payload_capture_states()
            .lock()
            .expect("payload capture states");
        let slot = states.len();
        states.push(packets);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("payload capture slot"),
        }
    }
}

impl Node for PayloadCaptureNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        payload_capture_process
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for PayloadCaptureNode {
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }
}

fn payload_capture_states() -> &'static Mutex<Vec<Arc<Mutex<Vec<Vec<u8>>>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<Vec<Vec<u8>>>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn payload_capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let Ok(slot) = data.usize_word(0) else {
        return NodeResult::drop();
    };
    let packets = {
        let states = payload_capture_states()
            .lock()
            .expect("payload capture states");
        let Some(packets) = states.get(slot) else {
            return NodeResult::drop();
        };
        Arc::clone(packets)
    };
    let mut packets = packets.lock().expect("captured payloads");
    if frame
        .pending_indices()
        .iter()
        .try_for_each(|&index| {
            let buffer = runtime.get_buffer(index).map_err(|_| ())?;
            packets.push(buffer.current().to_vec());
            Ok::<(), ()>(())
        })
        .is_err()
    {
        return NodeResult::drop();
    }
    NodeResult::drop()
}

struct QuicShapedTransport {
    observed_fifo_lengths: Arc<Mutex<Vec<usize>>>,
}

impl SessionTransport<u32> for QuicShapedTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(7);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl TransportInternalTransport<u32> for QuicShapedTransport {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<u32>,
        session_id: crate::session::SessionId,
        index: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        let len = sessions.pending_send_len(session_id)?.unwrap_or(0);
        self.observed_fifo_lengths
            .lock()
            .expect("observed lengths")
            .push(len);
        if len != 0 {
            let buffer = runtime.buffers().alloc_index()?;
            sessions.copy_tx_to_buffer(runtime.buffers(), session_id, 0, len, buffer)?;
            if output.try_enqueue_io(frame, output_next, buffer)? {
                sessions.notify_transport_closed(session_id, index)?;
            }
        }
        Ok(())
    }
}

#[test]
fn internal_tx_dispatches_each_session_sharing_a_transport_connection_once() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let observed_fifo_lengths = Arc::new(Mutex::new(Vec::new()));
    let captured_payloads = Arc::new(Mutex::new(Vec::new()));
    let output = runtime
        .nodes()
        .register_internal(PayloadCaptureNode::new(Arc::clone(&captured_payloads)));
    let transport_index = 12u32;
    let mut sessions = session_worker_for_test();
    let mut transport = QuicShapedTransport {
        observed_fifo_lengths: Arc::clone(&observed_fifo_lengths),
    };
    let first = sessions.insert_session_for_test(QuicShapedTransport::ID, transport_index);
    let second = sessions.insert_session_for_test(QuicShapedTransport::ID, transport_index);
    let first_app = attach_local_app_session(&mut sessions, first);
    let second_app = attach_local_app_session(&mut sessions, second);
    first_app.send_bytes(b"one").expect("first stream send");
    second_app.send_bytes(b"four").expect("second stream send");

    let (owner, next) = {
        let node = SessionQueueNode::new().expect("session queue node");
        let owner = runtime
            .nodes()
            .try_register_driver(node)
            .expect("register sq");
        let slot = runtime
            .nodes()
            .add_node_next_slot(owner, output)
            .expect("next");
        (owner, SessionQueueNext::from_slot(slot))
    };
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("internal tx");
    let _ = runtime.run_ready_nodes().expect("capture internal TX");

    assert_eq!(
        *observed_fifo_lengths.lock().expect("observed lengths"),
        vec![3, 4]
    );
    assert_eq!(
        *captured_payloads.lock().expect("captured payloads"),
        vec![b"one".to_vec(), b"four".to_vec()]
    );
    [first_app, second_app].into_iter().for_each(|app| {
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
        assert_eq!(app.poll_events(&mut events), 1);
        assert_eq!(events[0].evt_type, SessionEvtType::TransportClosed);
    });
}

struct FailingPacketizedTransport;

impl SessionTransport<u32> for FailingPacketizedTransport {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(8);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl SessionPacketizedTransport<u32> for FailingPacketizedTransport {
    fn control_tx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        pending_len: usize,
        _: Instant,
    ) -> RuntimeResult<TransportSendParams> {
        Ok(TransportSendParams {
            snd_space: pending_len,
            tx_offset: 0,
            send_goal_size: pending_len,
            flags: TransportSendFlags::default(),
        })
    }

    fn tx_action(
        &mut self,
        _: u32,
        _: &[TxBatchBuffer],
        _: &DataPlaneBuffers,
        _: Instant,
    ) -> RuntimeResult<()> {
        Err(SessionError::TxOffsetOutOfRange {
            session_id: crate::session::SessionId::from(2u32),
            tx_offset: 1,
            available: 0,
        }
        .into())
    }
}

struct RecordingPacketizedTransport {
    runtime: DataPlaneRuntime,
    events: Arc<Mutex<Vec<&'static str>>>,
    params: TransportSendParams,
    send_params_calls: usize,
    tx_action_calls: usize,
    batches: Vec<Vec<(usize, usize)>>,
}

impl SessionTransport<u32> for RecordingPacketizedTransport {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(9);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl SessionPacketizedTransport<u32> for RecordingPacketizedTransport {
    fn control_tx(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn send_params(
        &mut self,
        _: &mut SessionWorker<u32>,
        _: u32,
        _: usize,
        _: Instant,
    ) -> RuntimeResult<TransportSendParams> {
        self.send_params_calls += 1;
        Ok(self.params)
    }

    fn tx_action(
        &mut self,
        _: u32,
        batch: &[TxBatchBuffer],
        _: &DataPlaneBuffers,
        _: Instant,
    ) -> RuntimeResult<()> {
        let _ = self.runtime.run_ready_nodes()?;
        self.events.lock().expect("events").push("transport_commit");
        self.tx_action_calls += 1;
        self.batches.push(
            batch
                .iter()
                .map(|entry| (entry.tx_offset, entry.payload_len))
                .collect(),
        );
        Ok(())
    }
}

struct VisibilityCaptureNode {
    runtime_data: NodeRuntimeData,
}

impl VisibilityCaptureNode {
    fn new(events: Arc<Mutex<Vec<&'static str>>>) -> Self {
        let mut states = visibility_capture_states()
            .lock()
            .expect("visibility capture states");
        let slot = states.len();
        states.push(events);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("visibility capture slot"),
        }
    }
}

impl Node for VisibilityCaptureNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        visibility_capture_process
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for VisibilityCaptureNode {
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }
}

fn visibility_capture_states() -> &'static Mutex<Vec<Arc<Mutex<Vec<&'static str>>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<Vec<&'static str>>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn visibility_capture_process(
    _: &DataPlaneRuntime,
    data: NodeRuntimeData,
    _: &mut BufferFrame,
) -> NodeResult {
    let Ok(slot) = data.usize_word(0) else {
        return NodeResult::drop();
    };
    let events = {
        let states = visibility_capture_states()
            .lock()
            .expect("visibility capture states");
        let Some(events) = states.get(slot) else {
            return NodeResult::drop();
        };
        Arc::clone(events)
    };
    events.lock().expect("events").push("graph_visible");
    NodeResult::drop()
}

#[test]
fn session_tx_dispatch_commits_batch_before_graph_visibility() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let transport = RecordingPacketizedTransport {
        runtime: runtime.clone(),
        events: Arc::clone(&events),
        params: TransportSendParams {
            snd_space: 16,
            tx_offset: 0,
            send_goal_size: 4,
            flags: TransportSendFlags::default(),
        },
        send_params_calls: 0,
        tx_action_calls: 0,
        batches: Vec::new(),
    };
    let mut sessions = session_worker_for_test();
    let mut transport = transport;
    let session_id = sessions.insert_session_for_test(RecordingPacketizedTransport::ID, 5u32);
    let app = attach_local_app_session(&mut sessions, session_id);
    app.send_bytes(&[0xab; 16]).expect("send bytes");
    let output_node = runtime
        .nodes()
        .register_internal(VisibilityCaptureNode::new(Arc::clone(&events)));
    let node = SessionQueueNode::new().expect("session queue node");
    let owner = runtime
        .nodes()
        .try_register_driver(node)
        .expect("register sq");
    let next = SessionQueueNext::from_slot(
        runtime
            .nodes()
            .add_node_next_slot(owner, output_node)
            .expect("next"),
    );
    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch session queue");
    let _ = runtime.run_ready_nodes().expect("run capture node");

    assert_eq!(transport.send_params_calls, 1);
    assert_eq!(transport.tx_action_calls, 1);
    assert_eq!(
        transport.batches,
        vec![vec![(0, 4), (4, 4), (8, 4), (12, 4)]]
    );
    assert_eq!(
        *events.lock().expect("events"),
        vec!["transport_commit", "graph_visible"]
    );
}

#[test]
fn session_tx_deschedules_without_tx_action_when_send_space_is_zero() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let transport = RecordingPacketizedTransport {
        runtime: runtime.clone(),
        events: Arc::new(Mutex::new(Vec::new())),
        params: TransportSendParams {
            snd_space: 0,
            tx_offset: 0,
            send_goal_size: 4,
            flags: TransportSendFlags::DESCHED,
        },
        send_params_calls: 0,
        tx_action_calls: 0,
        batches: Vec::new(),
    };
    let mut sessions = session_worker_for_test();
    let mut transport = transport;
    let session_id = sessions.insert_session_for_test(RecordingPacketizedTransport::ID, 6u32);
    let app = attach_local_app_session(&mut sessions, session_id);
    app.send_bytes(&[0xab; 8]).expect("send bytes");
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let first = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("first dispatch");
    let second = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("second dispatch");

    assert_eq!(first.scheduled_sessions, 1);
    assert_eq!(second.scheduled_sessions, 0);
    assert_eq!(transport.send_params_calls, 1);
    assert_eq!(transport.tx_action_calls, 0);
    assert_eq!(
        sessions
            .pending_send_len(session_id)
            .expect("pending length"),
        Some(8)
    );
}

#[test]
fn failed_session_packetized_tx_action_keeps_fifo_and_graph_unchanged() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut sessions = session_worker_for_test();
    let mut transport = FailingPacketizedTransport;
    let session_id = sessions.insert_session_for_test(FailingPacketizedTransport::ID, 2u32);
    let app = attach_local_app_session(&mut sessions, session_id);
    app.send_bytes(b"stay").expect("send bytes");
    sessions.mark_ready(session_id);
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let error = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect_err("tx action must fail");

    assert!(matches!(
        &error,
        RuntimeError::Subsystem { subsystem, .. } if *subsystem == "session"
    ));
    assert_eq!(
        sessions
            .pending_send_len(session_id)
            .expect("pending length"),
        Some(4)
    );
    assert_eq!(runtime.run_ready_nodes().expect("capture output"), 0);
}

#[test]
fn transport_deleted_then_queued_app_close_releases_the_session_slot() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let transport_index = 3u32;
    let mut sessions = session_worker_for_test();
    let mut transport = FailingPacketizedTransport;
    let session_id =
        sessions.insert_session_for_test(FailingPacketizedTransport::ID, transport_index);
    let app = attach_local_app_session(&mut sessions, session_id);
    sessions
        .notify_transport_deleted(session_id, transport_index)
        .expect("delete transport Session");
    app.app_rx_mq()
        .enqueue_ctrl(SessionEvt::ctrl(
            app.session_index(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app close");
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch app close");

    assert!(!sessions.has_session(session_id));
    let replacement = sessions.insert_session_for_test(FailingPacketizedTransport::ID, 4u32);
    assert_eq!(replacement.pool_index(), session_id.pool_index());
}

#[test]
fn session_queue_io_budget_caps_normal_and_custom_tx_at_128() {
    use super::SESSION_QUEUE_IO_BUDGET;
    let mut output = super::SessionQueueOutput::default();
    let mut frame = BufferFrame::with_capacity(SESSION_QUEUE_IO_BUDGET + 8);
    let next = SessionQueueNext::from_slot(0);
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    (0..SESSION_QUEUE_IO_BUDGET).for_each(|_| {
        let index = runtime.alloc_index().expect("alloc");
        assert!(
            output
                .try_enqueue_io(&mut frame, next, index)
                .expect("enqueue within budget")
        );
    });
    assert_eq!(output.remaining_io_budget(), 0);
    assert_eq!(output.io_count(), SESSION_QUEUE_IO_BUDGET);
    let overflow = runtime.alloc_index().expect("overflow alloc");
    assert!(
        !output
            .try_enqueue_io(&mut frame, next, overflow)
            .expect("enqueue at cap")
    );
    assert_eq!(frame.len(), SESSION_QUEUE_IO_BUDGET);
}
