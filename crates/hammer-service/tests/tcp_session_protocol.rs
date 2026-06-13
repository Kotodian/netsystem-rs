use std::net::SocketAddr;

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_runtime::app::{AppObjectRef, AppOpcode, AppSqeData, AppSqeDescriptor, AppUserData};
use hammer_service::session::protocol::tcp::TcpSessionProtocol;
use hammer_service::session::{
    AppSessionClose, AppSessionId, AppSessionSubmission, AppSessionTimerExpiry,
    SessionProtocolContext, SessionProtocolOps, WorkerSessionRuntime,
};
use hammer_service::transport::tcp::{TcpDataPlaneConnection, TcpLookupId};

fn connection(
    lookup_id: TcpLookupId,
    connection_id: TcpConnectionId,
    worker: DataWorkerId,
) -> TcpDataPlaneConnection {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpDataPlaneConnection::new(
        lookup_id,
        Some(connection_id),
        worker,
        TcpState::Established,
        local.port(),
        Some(local),
        remote,
    )
}

#[test]
fn tcp_session_protocol_owns_connections_by_lookup_and_session_id() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(7);
    let connection_id = TcpConnectionId::new(7);
    let mut protocol = TcpSessionProtocol::new(worker);

    protocol
        .install_connection(connection(11, connection_id, worker))
        .expect("install connection");
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    assert!(protocol.connection(connection_id).is_some());
    assert!(protocol.lookup_connection(11).is_some());
    assert_eq!(
        protocol.connection_for_session(session_id),
        Some(connection_id)
    );
    assert_eq!(
        protocol.session_for_connection(connection_id),
        Some(session_id)
    );
    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert!(protocol.take_ready_connections().is_empty());
}

#[test]
fn tcp_session_protocol_marks_connection_ready_from_app_submission() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(21);
    let connection_id = TcpConnectionId::new(21);
    let mut protocol = TcpSessionProtocol::new(worker);
    let mut sessions = WorkerSessionRuntime::new(worker);
    protocol
        .install_connection(connection(21, connection_id, worker))
        .expect("install connection");
    protocol.take_ready_connections();
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Close,
        AppUserData::new(21),
        AppObjectRef::Flow(hammer_runtime::app::AppFlowId::new(21)),
        AppSqeData::Close,
    );
    let mut context = SessionProtocolContext::new(worker, &mut sessions);
    protocol
        .handle_submission(
            &mut context,
            AppSessionSubmission::Close(AppSessionClose::new(session_id, descriptor)),
        )
        .expect("handle close submission");

    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert_eq!(
        protocol.take_pending_app_submissions_for_test(),
        vec![(connection_id, AppUserData::new(21))]
    );
}

#[test]
fn tcp_session_protocol_dispatches_timer_expiry() {
    let worker = DataWorkerId::new(0);
    let session_id = AppSessionId::new(22);
    let connection_id = TcpConnectionId::new(22);
    let token = TcpSessionProtocol::retransmit_timer_token();
    let mut protocol = TcpSessionProtocol::new(worker);
    let mut sessions = WorkerSessionRuntime::new(worker);
    protocol
        .install_connection(connection(22, connection_id, worker))
        .expect("install connection");
    protocol.take_ready_connections();
    protocol
        .bind_app_session(session_id, connection_id)
        .expect("bind app session");

    let mut context = SessionProtocolContext::new(worker, &mut sessions);
    protocol
        .handle_timer_expiry(&mut context, AppSessionTimerExpiry::new(session_id, token))
        .expect("handle timer expiry");

    assert_eq!(protocol.take_ready_connections(), vec![connection_id]);
    assert_eq!(
        protocol.take_pending_timers_for_test(),
        vec![(connection_id, token)]
    );
}
