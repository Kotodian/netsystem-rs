//! TCP/session lifecycle tests through the adjacent worker state.

use std::sync::Arc;

use crate::{TcpCapabilities, TcpConnectionId, TcpPacket, TcpSegmentFlags, TcpState};
use hammer_core::data_plane::NodeId;
use hammer_infra::pool::Index;
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppContext, AppSession, AppSessionConfig, SessionHandle};
use hammer_runtime::app::{SessionEvt, SessionEvtType};
use hammer_runtime::spawn::DataRuntimeContext;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId};

use hammer_service::data_plane::DropNode;
use hammer_service::session::node::{SessionQueueNext, SessionQueueNode};
use hammer_service::session::runtime::dispatch_session_queue_once;
use hammer_service::transport::congestion::{BbrController, CongestionController};

use crate::{TcpConnection, TcpWorker, TcpWorkerState, insert_tcp_session, rollback_tcp_session};
use hammer_service::session::SessionId;

fn tcp_session<C>(
    state: &TcpWorkerState<C, Local>,
    session_id: SessionId,
) -> Option<&TcpConnection<C>>
where
    C: CongestionController + 'static,
{
    let (_, index) = state.sessions.session_transport(session_id)?;
    state.tcp.connections.get(index)
}

fn established_connection<C>(session_id: SessionId) -> TcpConnection<C>
where
    C: CongestionController,
{
    let local = "192.0.2.10:443".parse().expect("local address");
    let remote = "198.51.100.20:50001".parse().expect("remote address");
    TcpConnection::established_with_sack_for_test(
        Some(TcpConnectionId::new(session_id.get())),
        DataWorkerId::new(0),
        443,
        Some(local),
        remote,
    )
}

fn worker_state<C>(runtime: &DataPlaneRuntime) -> TcpWorkerState<C, Local>
where
    C: CongestionController + 'static,
{
    let worker = DataWorkerId::new(0);
    TcpWorkerState::new(
        hammer_service::session::SessionWorker::new(worker, runtime.buffers().clone()),
        TcpWorker::new(worker),
    )
}

fn attach_app<C>(
    state: &mut TcpWorkerState<C, Local>,
    session_id: SessionId,
) -> Arc<AppSession<Local>>
where
    C: CongestionController + 'static,
{
    let app = Arc::new(
        AppSession::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 16),
            SessionHandle::new(session_id.pool_index().slot(), 0),
            state.sessions.app().tx_evt_q().clone(),
        )
        .expect("app session"),
    );
    state
        .sessions
        .app_mut()
        .attach_session(session_id, Arc::clone(&app));
    app
}

fn session_queue(runtime: &DataPlaneRuntime) -> (NodeId, SessionQueueNext) {
    let output = runtime.nodes().register_internal(DropNode::new());
    let node = SessionQueueNode::new().expect("session queue node");
    let owner = runtime
        .nodes()
        .try_register_driver(node)
        .expect("register session queue node");
    let slot = runtime
        .nodes()
        .add_node_next_slot(owner, output)
        .expect("session queue output");
    (owner, SessionQueueNext::from_slot(slot))
}

fn dispatch_session_queue<C>(
    runtime: &DataPlaneRuntime,
    state: &mut TcpWorkerState<C, Local>,
    owner: NodeId,
    output_next: SessionQueueNext,
) where
    C: CongestionController + 'static,
{
    dispatch_session_queue_once(
        runtime,
        owner,
        &mut state.sessions,
        &mut state.tcp,
        output_next,
    )
    .expect("dispatch session queue");
}

fn poll_app_events(app: &AppSession<Local>) -> Vec<SessionEvtType> {
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
    let count = app.poll_events(&mut events);
    events[..count].iter().map(|event| event.evt_type).collect()
}

#[test]
fn with_app_context_retains_custom_app_session_config() {
    let data_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("data runtime");
    let config = AppSessionConfig::new(512, 8);
    let app_context = AppContext::new(
        DataRuntimeContext::new(data_runtime.handle().clone()),
        config,
    );
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let sessions = hammer_service::session::SessionWorker::with_app_context(
        worker,
        runtime.buffers().clone(),
        app_context,
    );
    let state = TcpWorkerState::new(sessions, TcpWorker::<BbrController>::new(worker));

    assert_eq!(
        (
            state.sessions.app_session_config(),
            state
                .sessions
                .app_context()
                .map(AppContext::app_session_config),
        ),
        (config, Some(config))
    );
}

#[test]
fn app_close_is_recorded_before_tcp_disconnect() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut state = worker_state::<BbrController>(&runtime);
    let session_id =
        insert_tcp_session(&mut state, established_connection).expect("insert TCP session");
    let app = attach_app(&mut state, session_id);
    app.tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app close");

    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut state, owner, output_next);

    assert!(state.sessions.has_session(session_id));
    assert_eq!(
        tcp_session(&state, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::FinWait1
    );
    assert!(poll_app_events(&app).is_empty());
}

#[test]
fn tcp_closed_publication_notifies_app_once_before_cleanup() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut state = worker_state::<BbrController>(&runtime);
    let session_id =
        insert_tcp_session(&mut state, established_connection).expect("insert TCP session");
    let app = attach_app(&mut state, session_id);
    let reset = {
        let connection = tcp_session(&state, session_id).expect("TCP connection");
        TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local address"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: None,
            advertised_window: 0,
            flags: TcpSegmentFlags::RST,
            capabilities: TcpCapabilities::default(),
            sack_blocks: Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        }
    };
    let (_, connection_index) = state
        .sessions
        .session_transport(session_id)
        .expect("session transport");
    state
        .tcp
        .receive_close_side_for_test(connection_index, &reset)
        .expect("receive reset");
    assert_eq!(
        tcp_session(&state, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::Closed
    );
    state.sessions.mark_ready(session_id);

    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut state, owner, output_next);

    assert!(state.sessions.has_session(session_id));
    assert!(tcp_session(&state, session_id).is_none());
    assert_eq!(poll_app_events(&app), vec![SessionEvtType::Close]);

    app.tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app cleanup");
    dispatch_session_queue(&runtime, &mut state, owner, output_next);

    assert!(!state.sessions.has_session(session_id));
    assert!(poll_app_events(&app).is_empty());
}

#[test]
fn rollback_discards_unpublished_session_without_close_notification() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut state = worker_state::<BbrController>(&runtime);
    let session_id =
        insert_tcp_session(&mut state, established_connection).expect("insert TCP session");
    let app = attach_app(&mut state, session_id);

    assert!(rollback_tcp_session(&mut state, session_id).expect("rollback session"));

    assert!(!state.sessions.has_session(session_id));
    assert!(tcp_session(&state, session_id).is_none());
    assert!(poll_app_events(&app).is_empty());
}
