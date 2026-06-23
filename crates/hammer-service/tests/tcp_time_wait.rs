use hammer_service::transport::tcp::{TCP_TIMER_TIME_WAIT, TcpState, closing_session_for_test};
#[test]
fn tcp_fin_path_enters_time_wait_and_retains_tuple_until_expiry() {
    let (mut harness, session_id, local, remote) =
        closing_session_for_test::<hammer_service::transport::congestion::BbrController>();

    harness
        .drive_fin_ack_to_time_wait(session_id)
        .expect("drive");

    let state = harness.session(session_id).expect("session");
    assert_eq!(state.state(), TcpState::TimeWait);
    assert!(harness.session_route_by_tuple(local, remote).is_some());
}

#[test]
fn tcp_time_wait_duplicate_fin_reacks_and_rearms_timer() {
    let (mut harness, session_id, _, _) =
        closing_session_for_test::<hammer_service::transport::congestion::BbrController>();

    harness
        .drive_fin_ack_to_time_wait(session_id)
        .expect("drive");

    let segment = harness
        .receive_duplicate_fin(session_id)
        .expect("receive")
        .expect("ack segment");
    let mut header = [0u8; 64];
    let header_len = segment.write_header(&mut header).expect("write header");
    let parsed =
        etherparse::TcpSlice::from_slice(&header[..header_len]).expect("parse tcp header");

    assert!(parsed.ack());
    let state = harness.session(session_id).expect("session");
    assert_eq!(state.state(), TcpState::TimeWait);
    assert!(state.timer_is_active(TCP_TIMER_TIME_WAIT));
}

#[test]
fn tcp_time_wait_expiry_closes_session_and_releases_tuple() {
    let (mut harness, session_id, local, remote) =
        closing_session_for_test::<hammer_service::transport::congestion::BbrController>();

    harness
        .drive_fin_ack_to_time_wait(session_id)
        .expect("drive");
    harness.expire_time_wait(session_id).expect("expire");

    assert!(harness.session(session_id).is_none());
    assert!(harness.session_route_by_tuple(local, remote).is_none());
}
