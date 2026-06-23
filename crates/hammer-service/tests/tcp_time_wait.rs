use hammer_service::transport::tcp::{TcpState, time_wait_session_for_test};

#[test]
fn tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry() {
    let (mut harness, session_id, local, remote) =
        time_wait_session_for_test::<hammer_service::transport::congestion::BbrController>();

    harness
        .drive_fin_ack_to_time_wait(session_id)
        .expect("drive");

    let state = harness.session(session_id).expect("session");
    assert_eq!(state.state(), TcpState::TimeWait);
    assert!(harness.session_route_by_tuple(local, remote).is_some());
}
