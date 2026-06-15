use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::session::SessionId;
use hammer_service::transport::tcp::congestion::TcpCongestionAckSample;
use hammer_service::transport::tcp::connection::{TcpConnection, TcpConnectionState};
use hammer_service::transport::tcp::state_machine::Closed;
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpConnectionTimerKind, TcpSessionConnectionIndex,
};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection(connection_id: TcpConnectionId, local_port: u16) -> TcpConnectionState {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    let connection: TcpConnection<Closed> = TcpConnection::new(
        Some(connection_id),
        DataWorkerId::new(0),
        local_port,
        Some(local),
        remote,
    );
    connection.connect(1).into()
}

fn acknowledge(connection: &mut TcpConnectionState, now: Instant) {
    connection.observe_congestion_ack(TcpCongestionAckSample {
        bytes_acked: TEST_SEGMENT_LEN,
        rtt: Duration::from_millis(20),
        now,
        bytes_in_flight: TEST_SEGMENT_LEN,
    });
}

#[test]
fn tcp_congestion_state_uses_connection_max_segment_size_for_initial_windows() {
    let small_mss = 1_200;
    let large_mss = 1_460;
    let mut small = hammer_service::transport::tcp::TcpCongestionState::new(small_mss);
    let large = hammer_service::transport::tcp::TcpCongestionState::new(large_mss);

    assert_eq!(small.max_segment_size(), small_mss);
    assert_eq!(large.max_segment_size(), large_mss);
    assert_eq!(small.congestion_window(), 10 * small_mss);
    assert_eq!(large.congestion_window(), 10 * large_mss);

    small.on_loss(u32::MAX);

    assert_eq!(small.congestion_window(), 4 * small_mss);
}

#[test]
fn tcp_connection_states_own_independent_congestion_state() {
    let now = Instant::now();
    let mut first = connection(TcpConnectionId::new(1), 50_001);
    let second = connection(TcpConnectionId::new(2), 50_002);

    first.observe_congestion_ack(TcpCongestionAckSample {
        bytes_acked: TEST_SEGMENT_LEN,
        rtt: Duration::from_millis(20),
        now,
        bytes_in_flight: TEST_SEGMENT_LEN,
    });

    assert_ne!(
        first.congestion().delivered(),
        second.congestion().delivered()
    );
    assert!(first.congestion().congestion_window() > second.congestion().congestion_window());
}

#[test]
fn tcp_connection_state_exposes_owned_congestion_control() {
    let now = Instant::now();
    let mut connection = connection(TcpConnectionId::new(3), 50_003);

    acknowledge(&mut connection, now);

    assert_eq!(
        connection.congestion().delivered(),
        u64::from(TEST_SEGMENT_LEN)
    );
    assert!(connection.congestion().congestion_window() > TEST_SEGMENT_LEN);
}

#[test]
fn tcp_connection_state_keeps_owned_congestion_window() {
    let mut connection = connection(TcpConnectionId::new(4), 50_004);
    let initial_congestion_window = connection.congestion().congestion_window();
    connection.observe_congestion_loss(u32::MAX);

    assert!(connection.congestion().congestion_window() < initial_congestion_window);
}

#[test]
fn tcp_connection_index_resolves_connection_id_and_tuple_to_session_id() {
    let first = connection(TcpConnectionId::new(101), 50_011);
    let second = connection(TcpConnectionId::new(102), 50_012);
    let first_session = SessionId::new(1_011);
    let second_session = SessionId::new(1_012);
    let first_local = first.local().expect("first local socket");
    let first_remote = first.remote();
    let second_local = second.local().expect("second local socket");
    let second_remote = second.remote();
    let mut index = TcpSessionConnectionIndex::empty();

    index.insert(first_session, &first);
    index.insert(second_session, &second);

    assert_eq!(
        index
            .lookup_by_connection_id(TcpConnectionId::new(102))
            .expect("id connection"),
        second_session
    );
    assert_eq!(
        index
            .lookup_by_tuple(first_local, first_remote)
            .expect("tuple connection"),
        first_session
    );

    index.remove_session(first_session);
    assert!(
        index
            .lookup_by_connection_id(TcpConnectionId::new(101))
            .is_none()
    );
    assert!(index.lookup_by_tuple(first_local, first_remote).is_none());
    assert_eq!(
        index
            .lookup_by_tuple(second_local, second_remote)
            .expect("second tuple remains"),
        second_session
    );
}

#[test]
fn tcp_connection_index_resolves_ipv6_tuple_without_compressing_key() {
    let local = SocketAddr::new(
        IpAddr::V6("2001:db8:200::10".parse::<Ipv6Addr>().expect("local")),
        50_123,
    );
    let remote = SocketAddr::new(
        IpAddr::V6("2001:db8:100::20".parse::<Ipv6Addr>().expect("remote")),
        443,
    );
    let connection: TcpConnection<Closed> = TcpConnection::new(
        Some(TcpConnectionId::new(202)),
        DataWorkerId::new(0),
        local.port(),
        Some(local),
        remote,
    );
    let connection: TcpConnectionState = connection.connect(1).into();
    let session_id = SessionId::new(2_022);
    let mut index = TcpSessionConnectionIndex::empty();

    index.insert(session_id, &connection);

    assert_eq!(
        index
            .lookup_by_tuple(local, remote)
            .expect("IPv6 tuple lookup"),
        session_id
    );
    assert!(
        index
            .lookup_by_tuple(
                SocketAddr::new(
                    IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 10)),
                    local.port()
                ),
                remote
            )
            .is_none()
    );
}

#[test]
fn tcp_connection_state_owns_timer_active_and_pending_bits() {
    let mut connection = connection(TcpConnectionId::new(303), 50_303);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));

    connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);

    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));

    connection.tcp_timer_expire(TcpConnectionTimerKind::Retransmit);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
    assert!(connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));
    assert!(connection.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));

    assert!(connection.tcp_timer_take_pending(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_is_live(TcpConnectionTimerKind::Retransmit));
}

#[test]
fn tcp_connection_timer_dispatch_skips_rearmed_pending_timer() {
    let mut connection = connection(TcpConnectionId::new(304), 50_304);

    connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);
    connection.tcp_timer_expire(TcpConnectionTimerKind::Retransmit);
    connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);

    assert!(!connection.tcp_timer_dispatch_pending(TcpConnectionTimerKind::Retransmit));
    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));
}

#[test]
fn tcp_connection_timer_reset_clears_pending_dispatch() {
    let mut connection = connection(TcpConnectionId::new(305), 50_305);

    connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);
    connection.tcp_timer_expire(TcpConnectionTimerKind::Retransmit);
    connection.tcp_timer_reset(TcpConnectionTimerKind::Retransmit);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));
    assert!(!connection.tcp_timer_take_pending(TcpConnectionTimerKind::Retransmit));
}
