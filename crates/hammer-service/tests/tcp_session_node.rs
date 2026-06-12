use std::net::{Shutdown, SocketAddr};
use std::time::{Duration, Instant};

use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_runtime::app::{
    AppBackend, AppBufferLease, AppObjectRef, AppOpcode, AppRegisteredBuffer, AppSqeData,
    AppSqeDescriptor, AppSubmissionEntry, AppTcpShutdown, AppUserData,
};
use hammer_runtime::spawn::with_data_plane_buffers;
use hammer_service::transport::tcp::{
    TcpAppCommand, TcpDataPlaneConnection, TcpLookupId, TcpSessionNode, TcpSessionRuntime,
    TcpSessionTimerKind,
};

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
fn tcp_session_runtime_owns_connections_by_lookup_and_connection_id() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(7);
    let mut runtime = TcpSessionRuntime::new(worker);

    runtime
        .install_connection(connection(11, connection_id, worker))
        .expect("install connection");

    assert!(runtime.connection(connection_id).is_some());
    assert!(runtime.lookup_connection(11).is_some());
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);
    assert!(runtime.take_ready_connections().is_empty());
}

#[test]
fn tcp_session_runtime_expires_timer_wheel_entries_into_ready_connections() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(9);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(12, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::Retransmit, 3)
        .expect("arm retransmit timer");

    assert_eq!(runtime.expire_timers(2).expect("expire before deadline"), 0);
    assert!(runtime.take_ready_connections().is_empty());

    assert_eq!(runtime.expire_timers(1).expect("expire at deadline"), 1);
    assert_eq!(
        runtime.dispatch_pending_timers_for_test(),
        vec![(connection_id, TcpSessionTimerKind::Retransmit)]
    );
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);
}

#[test]
fn tcp_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(10);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(13, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::Retransmit, 2)
        .expect("arm first retransmit timer");
    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::Retransmit, 5)
        .expect("rearm retransmit timer");

    assert_eq!(runtime.expire_timers(2).expect("expire stale timer"), 0);
    assert!(runtime.dispatch_pending_timers_for_test().is_empty());
    assert!(runtime.take_ready_connections().is_empty());

    assert_eq!(runtime.expire_timers(3).expect("expire rearmed timer"), 1);
    assert_eq!(
        runtime.dispatch_pending_timers_for_test(),
        vec![(connection_id, TcpSessionTimerKind::Retransmit)]
    );
}

#[test]
fn tcp_session_runtime_cancel_timer_suppresses_expiry() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(11);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(14, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::Persist, 2)
        .expect("arm persist timer");
    assert!(runtime.cancel_timer(connection_id, TcpSessionTimerKind::Persist));

    assert_eq!(runtime.expire_timers(2).expect("expire canceled timer"), 0);
    assert!(runtime.dispatch_pending_timers_for_test().is_empty());
    assert!(runtime.take_ready_connections().is_empty());
}

#[test]
fn tcp_session_node_runs_as_empty_frame_driver_without_shared_inbox() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let driver = runtime
        .nodes()
        .register_driver(TcpSessionNode::new(DataWorkerId::new(0)));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule session node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
}

#[test]
fn tcp_session_runtime_polls_app_send_submission_into_ready_connection() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(21);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(21, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(connection_id, backend.clone())
        .expect("attach app backend");

    let buffers = with_data_plane_buffers(Clone::clone);
    let index = buffers
        .alloc_index_with_bytes(Default::default(), b"tcp-session-app-send")
        .expect("alloc app send buffer");
    let registered = AppRegisteredBuffer::from_lease(AppBufferLease::from_buffer(buffers, index))
        .expect("registered buffer");
    let descriptor = AppSqeDescriptor::new(
        AppOpcode::Send,
        AppUserData::new(21),
        AppObjectRef::Flow(flow),
        AppSqeData::Send {
            buffer: registered.index(),
        },
    );

    backend
        .try_push_submission_entry(AppSubmissionEntry::with_attachment(descriptor, registered))
        .expect("push app send entry");

    assert_eq!(runtime.poll_app_rings().expect("poll app rings"), 1);
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);

    let commands = runtime.take_app_commands();
    assert_eq!(commands.len(), 1);
    match &commands[0] {
        TcpAppCommand::Send(send) => {
            assert_eq!(send.connection_id(), connection_id);
            assert_eq!(send.descriptor().user_data(), AppUserData::new(21));
            assert_eq!(
                send.registered()
                    .lease()
                    .copy_current()
                    .expect("copy app send payload"),
                b"tcp-session-app-send"
            );
        }
        other => panic!("unexpected app command: {other:?}"),
    }
}

#[test]
fn tcp_session_runtime_polls_app_shutdown_into_ready_connection() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(22);
    let mut runtime = TcpSessionRuntime::new(worker);
    runtime
        .install_connection(connection(22, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();

    let backend = AppBackend::new(4);
    let flow = backend.flow();
    runtime
        .attach_app_backend(connection_id, backend.clone())
        .expect("attach app backend");

    backend
        .try_push_tcp_shutdown(AppTcpShutdown::new(flow, Shutdown::Write))
        .expect("push app shutdown");

    assert_eq!(runtime.poll_app_rings().expect("poll app rings"), 1);
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);

    let commands = runtime.take_app_commands();
    assert_eq!(commands.len(), 1);
    match &commands[0] {
        TcpAppCommand::Shutdown(shutdown) => {
            assert_eq!(shutdown.connection_id(), connection_id);
            assert_eq!(shutdown.shutdown().flow(), flow);
            assert_eq!(shutdown.shutdown().how(), Shutdown::Write);
        }
        other => panic!("unexpected app command: {other:?}"),
    }
}

#[test]
fn tcp_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
    let worker = DataWorkerId::new(0);
    let connection_id = TcpConnectionId::new(23);
    let start = Instant::now();
    let mut runtime = TcpSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);
    runtime
        .install_connection(connection(23, connection_id, worker))
        .expect("install connection");
    runtime.take_ready_connections();
    runtime
        .arm_timer_ticks(connection_id, TcpSessionTimerKind::OutputPacing, 2)
        .expect("arm output pacing timer");

    let first = runtime
        .poll_once_at(start + Duration::from_millis(10))
        .expect("first poll");
    assert_eq!(first.expired_timers, 0);
    assert!(runtime.take_ready_connections().is_empty());

    let second = runtime
        .poll_once_at(start + Duration::from_millis(20))
        .expect("second poll");
    assert_eq!(second.expired_timers, 1);
    assert_eq!(
        runtime.dispatch_pending_timers_for_test(),
        vec![(connection_id, TcpSessionTimerKind::OutputPacing)]
    );
    assert_eq!(runtime.take_ready_connections(), vec![connection_id]);
}
