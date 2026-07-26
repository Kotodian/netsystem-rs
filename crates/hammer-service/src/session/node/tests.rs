use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, DataPlaneBuffers, NodeRegistration};
use hammer_infra::pool::Index;
use hammer_infra::segment::Segment;
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, InternalNode, Node, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};

use super::{SessionQueueNext, SessionQueueNode};
use crate::session::error::SessionQueueError;
use crate::session::runtime::{
    SessionPacketizedTransport, SessionPacketizedTx, SessionTransport, SessionTransportId,
    SessionWorker, TransportInternalTransport, TransportInternalTx, TransportSendFlags,
    TransportSendParams, TxBatchBuffer, dispatch_session_queue_once,
};
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle};
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

#[derive(Clone)]
struct RecordingState {
    events: Arc<Mutex<Vec<&'static str>>>,
    sampled_times: Arc<Mutex<Vec<Instant>>>,
}

#[derive(Clone)]
struct TcpRecordingTransport(RecordingState);

impl SessionTransport<Index> for TcpRecordingTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(1);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.0.events.lock().expect("events").push("tcp_time");
        self.0.sampled_times.lock().expect("times").push(now);
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.0.events.lock().expect("events").push("control");
        Ok(())
    }
}

impl TransportInternalTransport<Index> for TcpRecordingTransport {
    fn internal_tx(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        self.0.events.lock().expect("events").push("io");
        Ok(())
    }
}

#[test]
fn session_queue_updates_transport_before_control_and_io() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let events = Arc::new(Mutex::new(Vec::new()));
    let sampled_times = Arc::new(Mutex::new(Vec::new()));
    let mut sessions = SessionWorker::<Index>::new(worker);
    let mut transport = TcpRecordingTransport(RecordingState {
        events: Arc::clone(&events),
        sampled_times: Arc::clone(&sampled_times),
    });
    let session_id = sessions.insert_session_for_test(TcpRecordingTransport::ID, Index::new(9, 3));
    sessions.schedule_disconnect(session_id);
    sessions.mark_ready(session_id);
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let step = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch queue");

    assert_eq!(step.scheduled_sessions, 1);
    assert_eq!(
        *events.lock().expect("events"),
        vec!["tcp_time", "control", "io"]
    );
    let times = sampled_times.lock().expect("times");
    assert_eq!(times.len(), 1);
}

fn attach_local_app_session(
    sessions: &mut SessionWorker<Index>,
    session_id: crate::session::SessionId,
) -> Arc<AppSession> {
    let app_session = Arc::new(
        AppSession::new_in_segment(
            Segment::default(),
            AppSessionConfig::new(256, 16),
            SessionHandle::new(session_id.pool_index().slot(), 0),
            sessions.local_app().tx_evt_q().clone(),
        )
        .expect("app session"),
    );
    sessions
        .local_app_mut()
        .attach_session(session_id, Arc::clone(&app_session));
    app_session
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
    for &index in frame.pending_indices() {
        let Ok(buffer) = runtime.get_buffer(index) else {
            return NodeResult::drop();
        };
        packets.push(buffer.current().to_vec());
    }
    NodeResult::drop()
}

struct QuicShapedTransport {
    streams: Arc<Mutex<Vec<crate::session::SessionId>>>,
    observed_fifo_lengths: Arc<Mutex<Vec<usize>>>,
}

impl SessionTransport<Index> for QuicShapedTransport {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(7);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index>,
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
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl TransportInternalTransport<Index> for QuicShapedTransport {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        let streams = self.streams.lock().expect("streams").clone();
        for session_id in streams {
            let len = sessions.pending_send_len(session_id)?.unwrap_or(0);
            self.observed_fifo_lengths
                .lock()
                .expect("observed lengths")
                .push(len);
            if len != 0 {
                let buffer = runtime.buffers().alloc_index()?;
                sessions.copy_tx_to_buffer(runtime.buffers(), session_id, 0, len, buffer)?;
                if !output.try_enqueue_io(frame, output_next, buffer)? {
                    break;
                }
            }
            sessions.notify_transport_closed(session_id, index)?;
        }
        Ok(())
    }
}

#[test]
fn quic_shaped_internal_tx_can_fan_close_out_to_stream_sessions() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let streams = Arc::new(Mutex::new(Vec::new()));
    let observed_fifo_lengths = Arc::new(Mutex::new(Vec::new()));
    let captured_payloads = Arc::new(Mutex::new(Vec::new()));
    let output = runtime
        .nodes()
        .register_internal(PayloadCaptureNode::new(Arc::clone(&captured_payloads)));
    let transport_index = Index::new(12, 4);
    let mut sessions = SessionWorker::<Index>::new(DataWorkerId::new(0));
    let mut transport = QuicShapedTransport {
        streams: Arc::clone(&streams),
        observed_fifo_lengths: Arc::clone(&observed_fifo_lengths),
    };
    let first = sessions.insert_session_for_test(QuicShapedTransport::ID, transport_index);
    let second = sessions.insert_session_for_test(QuicShapedTransport::ID, transport_index);
    *streams.lock().expect("streams") = vec![first, second];
    let first_app = attach_local_app_session(&mut sessions, first);
    let second_app = attach_local_app_session(&mut sessions, second);
    first_app.send_bytes(b"one").expect("first stream send");
    assert_eq!(second_app.tx_fifo().enqueue(b"four"), 4);

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
    for app in [first_app, second_app] {
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
        assert_eq!(app.poll_events(&mut events), 1);
        assert_eq!(events[0].evt_type, SessionEvtType::Close);
    }
}

struct FailingPacketizedTransport;

impl SessionTransport<Index> for FailingPacketizedTransport {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(8);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index>,
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
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl SessionPacketizedTransport<Index> for FailingPacketizedTransport {
    fn control_tx(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: Index,
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
        _: &mut SessionWorker<Index>,
        _: Index,
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
        _: Index,
        _: &[TxBatchBuffer],
        _: &DataPlaneBuffers,
        _: Instant,
    ) -> RuntimeResult<()> {
        Err(SessionQueueError::DispatchFailed.into())
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

impl SessionTransport<Index> for RecordingPacketizedTransport {
    type Tx = SessionPacketizedTx;

    const ID: SessionTransportId = SessionTransportId::new(9);

    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index>,
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
        _: &mut SessionWorker<Index>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut super::SessionQueueOutput,
        _: Instant,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

impl SessionPacketizedTransport<Index> for RecordingPacketizedTransport {
    fn control_tx(
        &mut self,
        _: &mut SessionWorker<Index>,
        _: Index,
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
        _: &mut SessionWorker<Index>,
        _: Index,
        _: usize,
        _: Instant,
    ) -> RuntimeResult<TransportSendParams> {
        self.send_params_calls += 1;
        Ok(self.params)
    }

    fn tx_action(
        &mut self,
        _: Index,
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
    let mut sessions = SessionWorker::<Index>::new(DataWorkerId::new(0));
    let mut transport = transport;
    let session_id =
        sessions.insert_session_for_test(RecordingPacketizedTransport::ID, Index::new(5, 1));
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
    let mut sessions = SessionWorker::<Index>::new(DataWorkerId::new(0));
    let mut transport = transport;
    let session_id =
        sessions.insert_session_for_test(RecordingPacketizedTransport::ID, Index::new(6, 1));
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
    let mut sessions = SessionWorker::<Index>::new(DataWorkerId::new(0));
    let mut transport = FailingPacketizedTransport;
    let session_id =
        sessions.insert_session_for_test(FailingPacketizedTransport::ID, Index::new(2, 1));
    let app = attach_local_app_session(&mut sessions, session_id);
    app.send_bytes(b"stay").expect("send bytes");
    sessions.mark_ready(session_id);
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    let error = dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect_err("tx action must fail");

    assert!(matches!(
        &error,
        RuntimeError::Subsystem { subsystem, .. } if *subsystem == "session queue"
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
    let transport_index = Index::new(3, 9);
    let mut sessions = SessionWorker::<Index>::new(DataWorkerId::new(0));
    let mut transport = FailingPacketizedTransport;
    let session_id =
        sessions.insert_session_for_test(FailingPacketizedTransport::ID, transport_index);
    let app = attach_local_app_session(&mut sessions, session_id);
    sessions.notify_transport_deleted(session_id, transport_index);
    app.tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app close");
    let sink = runtime.nodes().register_internal(BlackholeNode);
    let (owner, next) = test_session_queue_next(&runtime, sink);

    dispatch_session_queue_once(&runtime, owner, &mut sessions, &mut transport, next)
        .expect("dispatch app close");

    assert!(!sessions.has_session(session_id));
    let replacement =
        sessions.insert_session_for_test(FailingPacketizedTransport::ID, Index::new(4, 10));
    assert_eq!(
        replacement.pool_index().slot(),
        session_id.pool_index().slot()
    );
    assert_ne!(
        replacement.pool_index().generation(),
        session_id.pool_index().generation()
    );
}

#[test]
fn session_queue_io_budget_caps_normal_and_custom_tx_at_128() {
    use super::SESSION_QUEUE_IO_BUDGET;
    let mut output = super::SessionQueueOutput::default();
    let mut frame = BufferFrame::with_capacity(SESSION_QUEUE_IO_BUDGET + 8);
    let next = SessionQueueNext::from_slot(0);
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    for _ in 0..SESSION_QUEUE_IO_BUDGET {
        let index = runtime.alloc_index().expect("alloc");
        assert!(
            output
                .try_enqueue_io(&mut frame, next, index)
                .expect("enqueue within budget")
        );
    }
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
