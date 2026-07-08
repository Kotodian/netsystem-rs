use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use hammer_adapter::{
    BufferFrame, DataPlaneBufferConfig, DataPlaneBuffers, DataPlaneRuntime, DataPlaneRuntimeConfig,
    DataWorkerId, InternalNode, Node, NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData,
};
use hammer_core::error::CoreResult;
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppSession, AppSessionConfig, SessionHandle};
use hammer_service::session::SessionQueueNext;
use hammer_service::session::protocol::SessionQueueControlContext;
use hammer_service::session::runtime::{
    SessionDriverRuntime, SessionQueueProtocol, TransportSendFlags, TransportSendParams,
    TxBatchBuffer, dispatch_session_queue_for_ticks,
};

fn test_runtime_configured(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_capacity: usize,
    frame_slots: usize,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_capacity,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

struct TestTxProtocol {
    offset: usize,
    send_params_calls: usize,
    push_header_calls: usize,
    pushed_batches: std::vec::Vec<std::vec::Vec<(usize, usize)>>,
    snd_space: Option<usize>,
    send_goal_size: usize,
    flags: TransportSendFlags,
    runtime: DataPlaneRuntime,
    events: Arc<Mutex<std::vec::Vec<&'static str>>>,
}

impl Default for TestTxProtocol {
    fn default() -> Self {
        Self {
            offset: 0,
            send_params_calls: 0,
            push_header_calls: 0,
            pushed_batches: std::vec::Vec::new(),
            snd_space: None,
            send_goal_size: 4,
            flags: TransportSendFlags::default(),
            runtime: test_runtime_configured(2048, 64, 64, 8),
            events: Arc::new(Mutex::new(std::vec::Vec::new())),
        }
    }
}

impl SessionQueueProtocol for TestTxProtocol {
    fn handle_expired_timer(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: u32,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
    ) -> CoreResult<bool> {
        Ok(false)
    }

    fn handle_ready_session(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: bool,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
    ) -> CoreResult<bool> {
        Ok(false)
    }

    fn send_params(
        &mut self,
        _: &mut SessionQueueControlContext,
        pending_len: usize,
        _: Instant,
    ) -> CoreResult<TransportSendParams> {
        self.send_params_calls += 1;
        Ok(TransportSendParams {
            snd_space: self.snd_space.unwrap_or(pending_len),
            tx_offset: self.offset,
            send_goal_size: self.send_goal_size,
            flags: self.flags,
        })
    }

    fn push_header(
        &mut self,
        _: &mut SessionQueueControlContext,
        batch: &[TxBatchBuffer],
        _: Instant,
    ) -> CoreResult<()> {
        let _ = self.runtime.run_ready_nodes()?;
        self.events.lock().expect("events").push("transport_commit");
        self.push_header_calls += 1;
        self.pushed_batches.push(
            batch
                .iter()
                .map(|entry| (entry.tx_offset, entry.payload_len))
                .collect(),
        );
        self.offset = batch
            .last()
            .map(|entry| entry.tx_offset + entry.payload_len)
            .unwrap_or(self.offset);
        Ok(())
    }

    fn custom_tx(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut SessionQueueControlContext,
        _: SessionQueueNext,
        _: &mut hammer_service::session::node::SessionQueueOutput,
        _: usize,
        _: Instant,
    ) -> CoreResult<usize> {
        Ok(0)
    }

    fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
}

#[derive(Default)]
struct CaptureState {
    packets: std::vec::Vec<std::vec::Vec<u8>>,
    events: Arc<Mutex<std::vec::Vec<&'static str>>>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states().lock().expect("capture registry");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
        }
    }
}

impl Node for CaptureNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }
}

fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
    static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
}

fn chain_bytes(
    buffers: &DataPlaneBuffers,
    index: hammer_adapter::BufferIndex,
) -> CoreResult<hammer_infra::vec::Vec<u8>> {
    let mut bytes = hammer_infra::vec::Vec::new();
    for buffer in buffers.chain(index) {
        bytes.extend_from_slice(buffer?.current());
    }
    Ok(bytes)
}

fn capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let slot = match data.usize_word(0) {
        Ok(s) => s,
        Err(_) => return NodeResult::drop(),
    };
    let state = {
        let states = capture_states().lock().expect("capture registry");
        match states.get(slot) {
            Some(s) => Arc::clone(s),
            None => return NodeResult::drop(),
        }
    };
    let mut state = state.lock().expect("capture state");
    state.events.lock().expect("events").push("graph_visible");
    for &index in frame.pending_indices() {
        let packet = match chain_bytes(runtime.buffers(), index) {
            Ok(bytes) => bytes,
            Err(_) => return NodeResult::drop(),
        };
        state.packets.push(packet.to_vec());
    }
    NodeResult::drop()
}

#[test]
fn session_tx_dispatch_commits_batch_before_graph_visibility() {
    // frame_capacity must be >= DEFAULT_TX_DISPATCH_BUDGET (64) so that
    // output.schedule can push all indices into one frame.
    let runtime = test_runtime_configured(2048, 64, 64, 8);
    let buffers = runtime.buffers();
    let events = Arc::new(Mutex::new(std::vec::Vec::new()));
    let capture_state = Arc::new(Mutex::new(CaptureState {
        packets: std::vec::Vec::new(),
        events: Arc::clone(&events),
    }));
    let mut driver =
        SessionDriverRuntime::<TestTxProtocol, Local>::new(DataWorkerId::new(0), buffers.clone());
    let session_id = driver.insert_session(TestTxProtocol {
        runtime: runtime.clone(),
        events: Arc::clone(&events),
        ..TestTxProtocol::default()
    });

    let app_session = Arc::new(
        AppSession::<Local>::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 64),
            SessionHandle::new(session_id.pool_index().slot() as u32, 0),
            driver.app().tx_evt_q().clone(),
        )
        .expect("create app session"),
    );

    let tx_data = [0xABu8; 16];
    app_session.send_bytes(&tx_data).expect("send bytes");

    driver.app_mut().attach_session(session_id, app_session);

    let next: SessionQueueNext = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture_state)))
        .into();
    dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next)
        .expect("dispatch session queue");
    let _ = runtime.run_ready_nodes().expect("run capture node");

    let protocol = driver.session(session_id).expect("protocol state");
    assert_eq!(protocol.send_params_calls, 1);
    assert_eq!(protocol.push_header_calls, 1);
    assert_eq!(
        protocol.pushed_batches,
        vec![vec![(0, 4), (4, 4), (8, 4), (12, 4)]]
    );
    let capture = capture_state.lock().expect("capture state");
    assert_eq!(capture.packets.len(), 4);
    assert_eq!(
        *events.lock().expect("events"),
        vec!["transport_commit", "graph_visible"]
    );
}

#[test]
fn session_tx_deschedules_without_push_header_when_send_space_is_zero() {
    let runtime = test_runtime_configured(2048, 64, 64, 8);
    let buffers = runtime.buffers();
    let capture_state = Arc::new(Mutex::new(CaptureState::default()));
    let mut driver =
        SessionDriverRuntime::<TestTxProtocol, Local>::new(DataWorkerId::new(0), buffers.clone());
    let session_id = driver.insert_session(TestTxProtocol {
        runtime: runtime.clone(),
        snd_space: Some(0),
        flags: TransportSendFlags::DESCHED,
        ..TestTxProtocol::default()
    });

    let app_session = Arc::new(
        AppSession::<Local>::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 64),
            SessionHandle::new(session_id.pool_index().slot() as u32, 0),
            driver.app().tx_evt_q().clone(),
        )
        .expect("create app session"),
    );

    app_session
        .send_bytes(&[0xABu8; 8])
        .expect("send pending payload");

    driver.app_mut().attach_session(session_id, app_session);

    let next: SessionQueueNext = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture_state)))
        .into();
    dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next)
        .expect("dispatch session queue");
    let _ = runtime.run_ready_nodes().expect("run capture node");

    let protocol = driver.session(session_id).expect("protocol state");
    assert_eq!(protocol.send_params_calls, 1);
    assert_eq!(protocol.push_header_calls, 0);
    let capture = capture_state.lock().expect("capture state");
    assert!(capture.packets.is_empty());
}

#[test]
fn session_tx_packetizes_by_send_goal_size_without_gso_metadata() {
    let runtime = test_runtime_configured(2048, 64, 64, 8);
    let buffers = runtime.buffers();
    let capture_state = Arc::new(Mutex::new(CaptureState::default()));
    let mut driver =
        SessionDriverRuntime::<TestTxProtocol, Local>::new(DataWorkerId::new(0), buffers.clone());
    let session_id = driver.insert_session(TestTxProtocol {
        runtime: runtime.clone(),
        send_goal_size: 12,
        ..TestTxProtocol::default()
    });

    let app_session = Arc::new(
        AppSession::<Local>::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 64),
            SessionHandle::new(session_id.pool_index().slot() as u32, 0),
            driver.app().tx_evt_q().clone(),
        )
        .expect("create app session"),
    );

    app_session
        .send_bytes(&[0xABu8; 24])
        .expect("send pending payload");

    driver.app_mut().attach_session(session_id, app_session);

    let next: SessionQueueNext = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture_state)))
        .into();
    dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next)
        .expect("dispatch session queue");
    let _ = runtime.run_ready_nodes().expect("run capture node");

    let protocol = driver.session(session_id).expect("protocol state");
    assert_eq!(protocol.send_params_calls, 1);
    assert_eq!(protocol.push_header_calls, 1);
    assert_eq!(protocol.pushed_batches, vec![vec![(0, 12), (12, 12)]]);
    let capture = capture_state.lock().expect("capture state");
    assert_eq!(capture.packets.len(), 2);
    assert_eq!(capture.packets[0].len(), 12);
    assert_eq!(capture.packets[1].len(), 12);
}
