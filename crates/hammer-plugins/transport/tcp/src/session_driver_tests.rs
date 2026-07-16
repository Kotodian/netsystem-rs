//! TCP session-driver lifecycle tests (needs crate-private helpers).

use std::sync::Arc;

use hammer_core::data_plane::NodeState;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionId, TcpPacket, TcpSegmentFlags, TcpState,
};
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::pool::Index;
use hammer_infra::segment::Local;
use hammer_runtime::app::{AppContext, AppSession, AppSessionConfig, SessionHandle};
use hammer_runtime::app::{SessionEvt, SessionEvtType};
use hammer_runtime::spawn::DataRuntimeContext;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine};

use hammer_service::data_plane::DropNode;
use hammer_service::session::SessionId;
use hammer_service::session::node::{
    SessionQueueHandle, SessionQueueNode, register_session_queue, register_session_queue_node,
};
use hammer_service::session::runtime::{
    SessionDriverRuntime, dispatch_registered_session_queue_once_at,
};
use hammer_service::transport::congestion::{BbrController, CongestionController};

use crate::{TcpConnection, TcpWorker, insert_tcp_session, rollback_tcp_session, tcp_session};

type TcpDriver<C> = SessionDriverRuntime<(TcpWorker<C>, ()), Local, Index>;

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

fn attach_app<C>(driver: &mut TcpDriver<C>, session_id: SessionId) -> Arc<AppSession<Local>>
where
    C: CongestionController + 'static,
{
    let app = Arc::new(
        AppSession::new_in_segment(
            Local::default(),
            AppSessionConfig::new(256, 16),
            SessionHandle::new(session_id.pool_index().slot(), 0),
            driver.app().tx_evt_q().clone(),
        )
        .expect("app session"),
    );
    driver
        .app_mut()
        .attach_session(session_id, Arc::clone(&app));
    app
}

fn attach_driver_to_node<C>(
    runtime: &DataPlaneRuntime,
    driver: TcpDriver<C>,
) -> SessionQueueHandle<TcpDriver<C>>
where
    C: CongestionController + 'static,
{
    let output = runtime.nodes().register_internal(DropNode::new());
    let node = register_session_queue_node(runtime, 0).expect("session queue node");
    let runtime_data = SessionQueueNode::registered_runtime_data().expect("session queue data");
    let mut engine = Engine::new(runtime.clone(), RuntimeRegistry::new());
    let handle = register_session_queue(&mut engine, node, runtime_data, driver)
        .expect("session queue handle");
    SessionQueueNode::attach_queue_by_runtime_data(
        runtime,
        node,
        runtime_data,
        handle,
        output,
        dispatch_registered_session_queue_once_at::<(TcpWorker<C>, ()), Local, Index>,
    )
    .expect("attach session queue");
    runtime
        .nodes()
        .set_node_state(node, NodeState::Polling)
        .expect("poll session queue");
    handle
}

fn run_session_queue(runtime: &DataPlaneRuntime) {
    assert_eq!(
        runtime
            .schedule_polling_driver_nodes()
            .expect("schedule session queue"),
        1
    );
    assert!(runtime.run_ready_nodes().expect("run session queue") >= 1);
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

    let driver = TcpDriver::<BbrController>::with_app_context(
        worker,
        runtime.buffers().clone(),
        (TcpWorker::new(worker), ()),
        app_context,
    );

    assert_eq!(
        (
            driver.app_session_config(),
            driver.app_context().map(AppContext::app_session_config),
        ),
        (config, Some(config))
    );
}

#[test]
fn app_close_is_recorded_before_tcp_disconnect() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let mut driver = TcpDriver::<BbrController>::new(
        worker,
        runtime.buffers().clone(),
        (TcpWorker::new(worker), ()),
    );
    let session_id =
        insert_tcp_session(&mut driver, established_connection).expect("insert TCP session");
    let app = attach_app(&mut driver, session_id);
    app.tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app close");
    let handle = attach_driver_to_node(&runtime, driver);

    run_session_queue(&runtime);

    let driver = handle.borrow_mut().expect("TCP session queue");
    assert!(driver.sessions().has_session(session_id));
    assert_eq!(
        tcp_session(&*driver, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::FinWait1
    );
    drop(driver);
    assert!(poll_app_events(&app).is_empty());
}

#[test]
fn tcp_closed_publication_notifies_app_once_before_cleanup() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let mut driver = TcpDriver::<BbrController>::new(
        worker,
        runtime.buffers().clone(),
        (TcpWorker::new(worker), ()),
    );
    let session_id =
        insert_tcp_session(&mut driver, established_connection).expect("insert TCP session");
    let app = attach_app(&mut driver, session_id);
    let reset = {
        let connection = tcp_session(&driver, session_id).expect("TCP connection");
        TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local address"),
            sequence: connection.rcv_nxt().into(),
            acknowledgment: None,
            advertised_window: 0,
            flags: TcpSegmentFlags::RST,
            capabilities: TcpCapabilities::default(),
            sack_blocks: std::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        }
    };
    let (_, transport_index) = driver
        .sessions()
        .session_transport(session_id)
        .expect("session transport");
    driver
        .transports_mut()
        .0
        .receive_close_side_for_test(transport_index, &reset)
        .expect("receive reset");
    assert_eq!(
        tcp_session(&driver, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::Closed
    );
    driver.schedule_session_work_for_test(session_id);
    let handle = attach_driver_to_node(&runtime, driver);

    run_session_queue(&runtime);

    {
        let driver = handle.borrow_mut().expect("TCP session queue");
        assert!(driver.sessions().has_session(session_id));
        assert!(tcp_session(&*driver, session_id).is_none());
    }
    assert_eq!(poll_app_events(&app), vec![SessionEvtType::Close]);

    app.tx_evt_q()
        .enqueue_ctrl(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            0,
            SessionEvtType::Close,
        ))
        .expect("queue app cleanup");
    run_session_queue(&runtime);

    let driver = handle.borrow_mut().expect("TCP session queue");
    assert!(!driver.sessions().has_session(session_id));
    drop(driver);
    assert!(poll_app_events(&app).is_empty());
}

#[test]
fn rollback_discards_unpublished_session_without_close_notification() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let worker = DataWorkerId::new(0);
    let mut driver = TcpDriver::<BbrController>::new(
        worker,
        runtime.buffers().clone(),
        (TcpWorker::new(worker), ()),
    );
    let session_id =
        insert_tcp_session(&mut driver, established_connection).expect("insert TCP session");
    let app = attach_app(&mut driver, session_id);

    assert!(rollback_tcp_session(&mut driver, session_id).expect("rollback session"));

    assert!(!driver.sessions().has_session(session_id));
    assert!(tcp_session(&driver, session_id).is_none());
    assert!(poll_app_events(&app).is_empty());
}
