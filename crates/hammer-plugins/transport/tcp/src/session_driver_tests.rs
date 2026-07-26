//! TCP/session lifecycle tests through the adjacent worker state.

use crate::{TcpCapabilities, TcpPacket, TcpSegmentFlags, TcpState};
use hammer_core::data_plane::NodeId;
use hammer_infra::pool::Index;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId};

use hammer_service::data_plane::DropNode;
use hammer_service::session::SessionId;
use hammer_service::session::node::{SessionQueueNext, SessionQueueNode};
use hammer_service::session::runtime::{
    SessionTransport, SessionWorker, dispatch_session_queue_once,
};

use crate::{TcpConnection, TcpWorker};

fn tcp_session<'a>(
    sessions: &SessionWorker<Index>,
    tcp: &'a TcpWorker,
    session_id: SessionId,
) -> Option<&'a TcpConnection> {
    let (_, index) = sessions.session_transport(session_id)?;
    tcp.connections.get(index)
}

fn established_connection() -> TcpConnection {
    let local = "192.0.2.10:443".parse().expect("local address");
    let remote = "198.51.100.20:50001".parse().expect("remote address");
    TcpConnection::established_with_sack_for_test(
        None,
        DataWorkerId::new(0),
        443,
        Some(local),
        remote,
    )
}

fn worker_state(runtime: &DataPlaneRuntime) -> (SessionWorker<Index>, TcpWorker) {
    let worker = DataWorkerId::new(0);
    (
        hammer_service::session::SessionWorker::new(worker).expect("session worker for test"),
        TcpWorker::new(worker),
    )
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
fn app_close_is_recorded_before_tcp_disconnect() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp) = worker_state(&runtime);
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
    let (mut sessions, mut tcp) = worker_state(&runtime);
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
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let (mut sessions, mut tcp) = worker_state(&runtime);
    let connection_index = tcp
        .insert_connection(established_connection())
        .expect("insert TCP connection");
    let session_id = sessions
        .stream_accept(
            <TcpWorker as SessionTransport<Index>>::ID,
            connection_index,
            0,
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
