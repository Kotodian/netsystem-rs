//! TCP/session lifecycle tests through the adjacent worker state.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{
    TcpCapabilities, TcpPacket, TcpSegmentFlags, TcpSeq, TcpState, publish_tcp_connection,
};
use hammer_core::data_plane::{BufferFrame, NodeId};
use hammer_infra::pool::Index;
use hammer_runtime::app::{
    AppSessionConfig, AppSessionError, AppSessionProtocol, AppSessionProtocolEntry,
    AppSessionProtocolRole, AppSessionProtocolSelection, ApplicationId, SessionEvt, SessionEvtType,
};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, RuntimeError};

use hammer_service::data_plane::DropNode;
use hammer_service::session::node::{SessionQueueNext, SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionPacketizedTransport, SessionTransport, SessionWorker, TransportSendFlags,
    dispatch_session_queue_once, dispatch_session_queue_pending,
};
use hammer_service::session::{ApplicationMain, SessionId};

use crate::timers::{TcpTimerKind, TcpTimers};
use crate::{TcpConnection, TcpWorker};

#[hammer_component_macros::app_session_protocol(name = "tls")]
struct TlsProtocol;

impl AppSessionProtocol for TlsProtocol {
    fn create(
        _: Option<ApplicationId>,
        _: AppSessionProtocolRole,
        _: Option<u64>,
        _: Option<&str>,
    ) -> hammer_runtime::RuntimeResult<Self> {
        Ok(Self)
    }

    fn ingress(
        &mut self,
        _: &hammer_infra::fifo::Fifo,
        _: &hammer_infra::fifo::Fifo,
    ) -> hammer_runtime::RuntimeResult<(usize, usize)> {
        Ok((0, 0))
    }

    fn egress(
        &mut self,
        _: &hammer_infra::fifo::Fifo,
        _: &hammer_infra::fifo::Fifo,
    ) -> hammer_runtime::RuntimeResult<(usize, usize)> {
        Err(AppSessionError::EventQueueFull {
            session: 0,
            event: SessionEvtType::Connect,
        }
        .into())
    }
}

#[hammer_component_macros::app_session_protocol(name = "http")]
struct HttpProtocol;

impl AppSessionProtocol for HttpProtocol {
    fn create(
        _: Option<ApplicationId>,
        _: AppSessionProtocolRole,
        _: Option<u64>,
        _: Option<&str>,
    ) -> hammer_runtime::RuntimeResult<Self> {
        Ok(Self)
    }

    fn ingress(
        &mut self,
        _: &hammer_infra::fifo::Fifo,
        _: &hammer_infra::fifo::Fifo,
    ) -> hammer_runtime::RuntimeResult<(usize, usize)> {
        Ok((0, 0))
    }

    fn egress(
        &mut self,
        _: &hammer_infra::fifo::Fifo,
        _: &hammer_infra::fifo::Fifo,
    ) -> hammer_runtime::RuntimeResult<(usize, usize)> {
        Err(AppSessionError::EventQueueFull {
            session: 0,
            event: SessionEvtType::Connect,
        }
        .into())
    }
}

fn tcp_session<'a>(
    sessions: &SessionWorker<Index>,
    tcp: &'a TcpWorker,
    session_id: SessionId,
) -> Option<&'a TcpConnection> {
    let (_, index) = sessions.session_transport(session_id)?;
    tcp.connections.get(index)
}

fn established_connection() -> TcpConnection {
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local address");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote address");
    TcpConnection::established_with_sack_for_test(
        None,
        DataWorkerId::new(0),
        443,
        Some(local),
        remote,
    )
}

fn worker_state() -> (SessionWorker<Index>, TcpWorker, Arc<ApplicationMain>) {
    let worker = DataWorkerId::new(0);
    let applications = ApplicationMain::with_protocols(
        1024,
        [
            __APP_SESSION_PROTOCOL_TLS_PROTOCOL,
            __APP_SESSION_PROTOCOL_HTTP_PROTOCOL,
        ],
    );
    let sessions = hammer_service::session::SessionWorker::new(
        worker,
        1,
        AppSessionConfig::default(),
        1024,
        Arc::clone(&applications),
        None,
    )
    .expect("session worker for test");
    (sessions, TcpWorker::new(worker), applications)
}

fn attach_protocol_session(
    sessions: &mut SessionWorker<Index>,
    applications: &Arc<ApplicationMain>,
    tcp: &mut TcpWorker,
    connection_index: Index,
    protocol: AppSessionProtocolEntry,
) -> SessionId {
    let application = applications.attach().expect("attach test Application");
    sessions
        .install_application_mq_for_test(application)
        .expect("install test Application MQ");
    let policy = hammer_runtime::app::AppSessionPolicy::new(
        hammer_runtime::app::APP_SESSION_POLICY_VERSION,
        [AppSessionProtocolSelection::new(
            protocol.registration().name(),
        )],
    )
    .expect("App Session protocol policy is valid");
    let application_listener = applications
        .register_listener(application, &policy)
        .expect("register protocol Application listener");
    let session_main = std::sync::Arc::new(hammer_service::session::runtime::SessionMain::new(
        1,
        Arc::clone(applications),
    ));
    sessions.set_listener_main(std::sync::Arc::clone(&session_main));
    let listener = session_main
        .listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "test-session",
                Some(|_, _| Ok(())),
                Some(|_| Ok(())),
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test endpoint"),
                sessions.worker(),
            ),
        )
        .expect("register protocol Session listener");
    let session_id = sessions
        .stream_accept(TcpWorker::ID, connection_index, listener)
        .expect("construct protocol Session");
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach protocol Session");
    session_id
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

fn dispatch_session_queue(
    runtime: &DataPlaneRuntime,
    sessions: &mut SessionWorker<Index>,
    tcp: &mut TcpWorker,
    owner: NodeId,
    output_next: SessionQueueNext,
) {
    dispatch_session_queue_once(runtime, owner, sessions, tcp, output_next)
        .expect("dispatch session queue");
}

#[test]
fn session_queue_dispatch_advances_tcp_timers_without_session_events() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let now = Instant::now();
    let resolution = Duration::from_millis(10);
    tcp.timers = TcpTimers::new(now, resolution);
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    {
        let TcpWorker {
            connections,
            timers,
            ..
        } = &mut tcp;
        let timer_state = connections
            .get_mut(connection_index)
            .expect("TCP connection")
            .timer_state_mut();
        timers
            .set(
                connection_index,
                timer_state,
                TcpTimerKind::DelayedAck,
                resolution,
            )
            .expect("arm delayed ACK timer");
    }
    let mut frame = BufferFrame::with_capacity(1);
    let mut output = SessionQueueOutput::default();

    dispatch_session_queue_pending(
        &runtime,
        &mut sessions,
        &mut tcp,
        SessionQueueNext::from_slot(0),
        &mut frame,
        &mut output,
        now + resolution,
    )
    .expect("dispatch session queue");

    assert!(
        !tcp.connection(connection_index)
            .expect("TCP connection")
            .timer_state()
            .is_active(TcpTimerKind::DelayedAck)
    );
}

#[test]
fn app_close_is_recorded_before_tcp_disconnect() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    sessions.schedule_disconnect(session_id);

    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    assert!(sessions.has_session(session_id));
    assert_eq!(
        tcp_session(&sessions, &tcp, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::FinWait1
    );
}

#[test]
fn tcp_closed_publication_notifies_app_once_before_cleanup() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let reset = {
        let connection = tcp_session(&sessions, &tcp, session_id).expect("TCP connection");
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
    let (_, connection_index) = sessions
        .session_transport(session_id)
        .expect("session transport");
    tcp.receive_close_side_for_test(connection_index, &reset)
        .expect("receive reset");
    assert_eq!(
        tcp_session(&sessions, &tcp, session_id)
            .expect("TCP connection")
            .state(),
        TcpState::Closed
    );
    sessions.mark_ready(session_id);

    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    assert!(sessions.has_session(session_id));
    assert!(tcp_session(&sessions, &tcp, session_id).is_none());

    sessions.schedule_disconnect(session_id);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    assert!(!sessions.has_session(session_id));
}

#[test]
fn rollback_discards_unpublished_session_without_close_notification() {
    let (mut sessions, mut tcp, applications) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let application = applications.attach().expect("attach test Application");
    sessions
        .install_application_mq_for_test(application)
        .expect("install test Application MQ");
    let policy = hammer_runtime::app::AppSessionPolicy::new(
        hammer_runtime::app::APP_SESSION_POLICY_VERSION,
        [],
    )
    .expect("direct App Session policy is valid");
    let application_listener = applications
        .register_listener(application, &policy)
        .expect("register test Application listener");
    let session_main = std::sync::Arc::new(hammer_service::session::runtime::SessionMain::new(
        1,
        std::sync::Arc::clone(&applications),
    ));
    sessions.set_listener_main(std::sync::Arc::clone(&session_main));
    let listener = session_main
        .listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "test-session",
                Some(|_, _| Ok(())),
                Some(|_| Ok(())),
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test endpoint"),
                sessions.worker(),
            ),
        )
        .expect("register test Session listener");
    let session_id = sessions
        .stream_accept(
            <TcpWorker as SessionTransport<Index>>::ID,
            connection_index,
            listener,
        )
        .expect("accept stream session");
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let connection_index = sessions
        .rollback_session_creation(session_id)
        .expect("rollback session")
        .expect("TCP connection index");
    let removed = tcp.remove_connection(connection_index);

    assert!(!sessions.has_session(session_id));
    assert!(removed.is_some());
    assert!(tcp.connection(connection_index).is_none());
}

#[test]
fn active_open_commits_session_after_full_connection_publication() {
    let (mut sessions, mut tcp, applications) = worker_state();
    let worker = DataWorkerId::new(0);
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local address");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote address");
    let mut connection = TcpConnection::new(None, worker, local.port(), Some(local), remote);
    connection.connect_state(100);
    let connection_index = tcp
        .insert_connection(connection)
        .expect("insert active-open TCP connection");
    let application = applications.attach().expect("attach test Application");
    sessions
        .install_application_mq_for_test(application)
        .expect("install test Application MQ");
    let policy = hammer_runtime::app::AppSessionPolicy::new(
        hammer_runtime::app::APP_SESSION_POLICY_VERSION,
        [],
    )
    .expect("direct App Session policy is valid");
    let application_listener = applications
        .register_listener(application, &policy)
        .expect("register test Application listener");
    let session_main = std::sync::Arc::new(hammer_service::session::runtime::SessionMain::new(
        1,
        std::sync::Arc::clone(&applications),
    ));
    sessions.set_listener_main(std::sync::Arc::clone(&session_main));
    let listener = session_main
        .listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "test-session",
                Some(|_, _| Ok(())),
                Some(|_| Ok(())),
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test endpoint"),
                worker,
            ),
        )
        .expect("register test Session listener");
    let session_id = sessions
        .stream_accept(TcpWorker::ID, connection_index, listener)
        .expect("construct active-open Session");
    tcp.connection_mut(connection_index)
        .expect("active-open TCP connection")
        .attach_session(session_id)
        .expect("attach active-open Session");

    publish_tcp_connection(&mut sessions, &mut tcp, session_id)
        .expect("publish active half-open lookup");
    assert_eq!(
        tcp.lookup
            .pending_route_by_tuple(local, remote)
            .map(|route| route.0),
        Some(session_id)
    );

    let mut established = established_connection();
    established
        .attach_session(session_id)
        .expect("attach established Session");
    *tcp.connection_mut(connection_index)
        .expect("active-open TCP connection remains installed") = established;
    publish_tcp_connection(&mut sessions, &mut tcp, session_id)
        .expect("publish established TCP connection and notify App");

    assert!(tcp.lookup.pending_route_by_tuple(local, remote).is_none());
    assert_eq!(
        tcp.lookup
            .session_route_by_tuple(local, remote)
            .map(|route| route.0),
        Some(session_id)
    );
    assert!(sessions.rollback_session_creation(session_id).is_err());
}

#[test]
fn passive_open_app_notification_failure_rolls_back_all_owner_state() {
    let (mut sessions, mut tcp, applications) = worker_state();
    let connection = established_connection();
    let local = connection.local().expect("local address");
    let remote = connection.remote();
    let connection_index = tcp
        .insert_connection(connection)
        .expect("insert passive-open TCP connection");
    let session_id = attach_protocol_session(
        &mut sessions,
        &applications,
        &mut tcp,
        connection_index,
        __APP_SESSION_PROTOCOL_TLS_PROTOCOL,
    );

    let error = publish_tcp_connection(&mut sessions, &mut tcp, session_id)
        .expect_err("full App event queue rejects passive-open notification");

    assert!(matches!(
        error,
        RuntimeError::AppSession(AppSessionError::EventQueueFull {
            event: SessionEvtType::Connect,
            ..
        })
    ));
    assert!(tcp.lookup.session_route_by_tuple(local, remote).is_none());
    assert!(tcp.lookup.pending_route_by_tuple(local, remote).is_none());
    assert!(tcp.connection(connection_index).is_none());
    assert!(!sessions.has_session(session_id));
}

#[test]
fn active_open_app_notification_failure_rolls_back_all_owner_state() {
    let (mut sessions, mut tcp, applications) = worker_state();
    let worker = DataWorkerId::new(0);
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local address");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote address");
    let mut connection = TcpConnection::new(None, worker, local.port(), Some(local), remote);
    connection.connect_state(100);
    let connection_index = tcp
        .insert_connection(connection)
        .expect("insert active-open TCP connection");
    let session_id = attach_protocol_session(
        &mut sessions,
        &applications,
        &mut tcp,
        connection_index,
        __APP_SESSION_PROTOCOL_HTTP_PROTOCOL,
    );
    publish_tcp_connection(&mut sessions, &mut tcp, session_id)
        .expect("publish active half-open lookup");

    let mut established = established_connection();
    established
        .attach_session(session_id)
        .expect("attach established Session");
    *tcp.connection_mut(connection_index)
        .expect("active-open TCP connection remains installed") = established;
    let error = publish_tcp_connection(&mut sessions, &mut tcp, session_id)
        .expect_err("full App event queue rejects active-open notification");

    assert!(matches!(
        error,
        RuntimeError::AppSession(AppSessionError::EventQueueFull {
            event: SessionEvtType::Connect,
            ..
        })
    ));
    assert!(tcp.lookup.session_route_by_tuple(local, remote).is_none());
    assert!(tcp.lookup.pending_route_by_tuple(local, remote).is_none());
    assert!(tcp.connection(connection_index).is_none());
    assert!(!sessions.has_session(session_id));
}

#[test]
fn app_rx_event_refreshes_window_before_zero_window_is_advertised() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (_, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .set_rcv_wnd(0);
    let mut frame = BufferFrame::with_capacity(1);
    let mut output = SessionQueueOutput::default();

    let request_notification = <TcpWorker as SessionTransport<Index>>::app_rx_evt(
        &mut tcp,
        connection_index,
        8 << 10,
        64 << 10,
        &runtime,
        SessionQueueNext::from_slot(0),
        &mut frame,
        &mut output,
    )
    .expect("process app RX event");

    assert!(!request_notification);
    assert_eq!(
        tcp.connection(connection_index)
            .expect("TCP connection")
            .rcv_wnd(),
        8 << 10
    );
}

#[test]
fn app_rx_event_refreshes_window_before_rearming_dequeue_notification() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (_, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let connection = tcp
        .connection_mut(connection_index)
        .expect("TCP connection");
    connection.set_rcv_wnd(0);
    let local = connection.local().expect("local address");
    let remote = connection.remote();
    connection.control_segment(
        local,
        remote,
        TcpSegmentFlags::ACK,
        None,
        TcpCapabilities::default(),
    );
    let mut frame = BufferFrame::with_capacity(1);
    let mut output = SessionQueueOutput::default();

    let request_notification = <TcpWorker as SessionTransport<Index>>::app_rx_evt(
        &mut tcp,
        connection_index,
        1 << 10,
        64 << 10,
        &runtime,
        SessionQueueNext::from_slot(0),
        &mut frame,
        &mut output,
    )
    .expect("process app RX event");

    assert!(request_notification);
    assert_eq!(
        tcp.connection(connection_index)
            .expect("TCP connection")
            .rcv_wnd(),
        1 << 10
    );
}

#[test]
fn fin_with_payload_is_processed_after_rx_enqueue_and_notifies_app_once() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let app = sessions
        .app_session(session_id)
        .cloned()
        .expect("application App Session");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(app.poll_events(&mut events), 1);

    let (local, remote, rcv_nxt, snd_nxt) = {
        let connection = tcp.connection(connection_index).expect("TCP connection");
        (
            connection.local().expect("local address"),
            connection.remote(),
            connection.rcv_nxt(),
            connection.snd_nxt(),
        )
    };
    let packet = TcpPacket {
        local,
        remote,
        sequence: TcpSeq::from(rcv_nxt),
        acknowledgment: Some(TcpSeq::from(snd_nxt)),
        advertised_window: u16::MAX,
        flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
        capabilities: TcpCapabilities::default(),
        sack_blocks: Vec::new(),
        timestamp: None,
        fast_open_cookie: None,
        ip_ecn: None,
        payload_offset: 0,
        payload_len: 5,
    };
    let (_, index) = sessions
        .session_transport(session_id)
        .expect("session transport");
    let now = Instant::now();
    {
        let TcpWorker {
            connections,
            timers,
            ..
        } = &mut tcp;
        let connection = connections.get_mut(index).expect("TCP connection");
        let _ = connection
            .receive_established_with_timers(index, timers, &packet, now)
            .expect("process established segment");
        assert_eq!(connection.accept_payload(&packet), Some((0, 0)));
        let ingress = runtime
            .alloc_index_with_bytes(b"hello")
            .expect("ingress buffer");
        let delivery = sessions
            .enqueue_rx(runtime.buffers(), session_id, ingress, 0, false)
            .expect("enqueue payload");
        connection.receive_payload(packet.sequence, 0, delivery);
        let fin = connection
            .process_fin_after_payload(&packet)
            .expect("process FIN after payload");
        assert!(fin.is_some(), "FIN+payload must emit an ACK");
    }
    sessions
        .notify_transport_closing(&runtime, session_id, index)
        .expect("notify app close");

    assert_eq!(
        tcp.connection(index).expect("TCP connection").state(),
        TcpState::CloseWait
    );
    assert_eq!(
        tcp.connection(index).expect("TCP connection").rcv_nxt(),
        rcv_nxt.wrapping_add(6)
    );
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 8];
    assert_eq!(app.poll_events(&mut events), 2);
    assert_eq!(events[0].evt_type, SessionEvtType::RxEnq);
    assert_eq!(events[1].evt_type, SessionEvtType::Disconnected);
}

#[test]
fn app_half_close_sends_fin_without_closing_session_state() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let app = sessions
        .app_session(session_id)
        .cloned()
        .expect("application App Session");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(app.poll_events(&mut events), 1);

    app.half_close().expect("request half close");
    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    assert_eq!(
        tcp.connection(connection_index)
            .expect("TCP connection")
            .state(),
        TcpState::FinWait1
    );
    assert!(sessions.has_session(session_id));
}

#[test]
fn app_full_close_sends_fin_and_keeps_session_until_transport_cleanup() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let app = sessions
        .app_session(session_id)
        .cloned()
        .expect("application App Session");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(app.poll_events(&mut events), 1);

    app.close().expect("request full close");
    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    assert_eq!(
        tcp.connection(connection_index)
            .expect("TCP connection")
            .state(),
        TcpState::FinWait1
    );
    assert!(sessions.has_session(session_id));
}

#[test]
fn tcp_reset_delivers_distinct_reset_event_to_app() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp, _) = worker_state();
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    let app = sessions
        .app_session(session_id)
        .cloned()
        .expect("application App Session");
    let mut events = [SessionEvt::io(0, SessionEvtType::Connect)];
    assert_eq!(app.poll_events(&mut events), 1);

    let reset = {
        let connection = tcp.connection(connection_index).expect("TCP connection");
        TcpPacket {
            local: connection.remote(),
            remote: connection.local().expect("local address"),
            sequence: TcpSeq::from(connection.rcv_nxt()),
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
    tcp.receive_close_side_for_test(connection_index, &reset)
        .expect("receive reset");
    sessions.mark_ready(session_id);
    let (owner, output_next) = session_queue(&runtime);
    dispatch_session_queue(&runtime, &mut sessions, &mut tcp, owner, output_next);

    let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
    assert_eq!(app.poll_events(&mut events), 1);
    assert_eq!(events[0].evt_type, SessionEvtType::Reset);
}

#[test]
fn tcp_send_params_deschedules_when_peer_window_is_zero() {
    let (mut sessions, mut tcp, _) = worker_state();
    let mut connection = established_connection();
    connection.set_peer_window_for_test(0);
    let connection_index = tcp
        .insert_connection(connection)
        .expect("insert TCP connection");
    let session_id = sessions
        .insert_session_for_test(<TcpWorker as SessionTransport<Index>>::ID, connection_index);
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");

    let params = <TcpWorker as SessionPacketizedTransport<Index>>::send_params(
        &mut tcp,
        &mut sessions,
        connection_index,
        8,
        Instant::now(),
    )
    .expect("send params");

    assert_eq!(params.snd_space, 0);
    assert!(params.flags.contains(TransportSendFlags::DESCHED));
}
