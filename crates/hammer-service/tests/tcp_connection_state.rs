use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::session::SessionId;
use hammer_service::transport::congestion::{BbrController, CongestionController};
use hammer_service::transport::tcp::connection::{TcpConnection, TcpConnectionState};
use hammer_service::transport::tcp::state_machine::{Closed, Listen, SynSent};
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpConnectionTimerKind, TcpInputNext, TcpSessionConnectionIndex,
};

fn connection(
    connection_id: TcpConnectionId,
    local_port: u16,
) -> TcpConnection<SynSent, BbrController> {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    let connection: TcpConnection<Closed, BbrController> = TcpConnection::new(
        Some(connection_id),
        DataWorkerId::new(0),
        local_port,
        Some(local),
        remote,
    );
    connection.connect_state(1)
}

#[test]
fn tcp_congestion_state_uses_connection_max_segment_size_for_initial_windows() {
    let small_mss = 1_200;
    let large_mss = 1_460;
    let small = BbrController::new(small_mss);
    let large = BbrController::new(large_mss);

    assert_eq!(small.max_datagram_size(), small_mss);
    assert_eq!(large.max_datagram_size(), large_mss);
    assert_eq!(small.congestion_window(), 10 * small_mss);
    assert_eq!(large.congestion_window(), 10 * large_mss);

    assert_ne!(small.congestion_window(), large.congestion_window());
}

#[test]
fn tcp_typed_connections_own_independent_congestion_state() {
    let first = connection(TcpConnectionId::new(1), 50_001);
    let second = connection(TcpConnectionId::new(2), 50_002);

    assert_ne!(first.connection_id(), second.connection_id());
    assert_eq!(
        first.congestion().delivered(),
        second.congestion().delivered()
    );
    assert_eq!(
        first.congestion().congestion_window(),
        second.congestion().congestion_window()
    );
}

#[test]
fn tcp_typed_connection_exposes_owned_congestion_control() {
    let connection = connection(TcpConnectionId::new(3), 50_003);

    assert_eq!(
        connection.congestion().max_datagram_size(),
        DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32
    );
    assert_eq!(connection.congestion().delivered(), 0);
}

#[test]
fn tcp_listen_connection_uses_left_hand_state_constructor() {
    let local: SocketAddr = "192.0.2.10:50004".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    let connection: TcpConnection<Listen, BbrController> = TcpConnection::new(
        Some(TcpConnectionId::new(4)),
        DataWorkerId::new(0),
        local.port(),
        Some(local),
        remote,
    );

    assert_eq!(connection.local(), Some(local));
    assert_eq!(connection.remote(), remote);
    assert_eq!(connection.next_node(), TcpInputNext::Listen);
    assert_eq!(
        connection.congestion().max_datagram_size(),
        DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32
    );
}

#[test]
fn tcp_connection_does_not_keep_private_pacing_deadline() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp/state_machine.rs"),
    )
    .expect("read tcp state machine");

    assert!(!source.contains("next_output_at"));
    assert!(!source.contains("schedule_next_output"));
}

#[test]
fn tcp_connection_state_erases_and_restores_typed_state() {
    let connection = connection(TcpConnectionId::new(5), 50_005);
    let state: TcpConnectionState<BbrController> = connection.clone().into();
    let restored: TcpConnection<SynSent, BbrController> =
        state.try_into().expect("restore syn-sent");

    assert_eq!(restored.connection_id(), connection.connection_id());
    assert_eq!(restored.snd_nxt(), connection.snd_nxt());
    assert_eq!(restored.next_node(), TcpInputNext::SynSent);
}

#[test]
fn tcp_connection_index_resolves_connection_id_and_tuple_to_route() {
    let first = connection(TcpConnectionId::new(101), 50_011);
    let second = connection(TcpConnectionId::new(102), 50_012);
    let first_session = SessionId::new(1_011);
    let second_session = SessionId::new(1_012);
    let first_local = first.local().expect("first local socket");
    let first_remote = first.remote();
    let second_local = second.local().expect("second local socket");
    let second_remote = second.remote();
    let mut index = TcpSessionConnectionIndex::empty();

    index.remember_session(
        first_session,
        first.connection_id(),
        first.local(),
        first.remote(),
        first.owner_worker(),
        first.next_node(),
    );
    index.remember_session(
        second_session,
        second.connection_id(),
        second.local(),
        second.remote(),
        second.owner_worker(),
        second.next_node(),
    );

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
        (first_session, DataWorkerId::new(0), TcpInputNext::SynSent)
    );

    index.forget_session(first_session);
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
        (second_session, DataWorkerId::new(0), TcpInputNext::SynSent)
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
    let connection: TcpConnection<Closed, BbrController> = TcpConnection::new(
        Some(TcpConnectionId::new(202)),
        DataWorkerId::new(0),
        local.port(),
        Some(local),
        remote,
    );
    let connection = connection.connect_state(1);
    let session_id = SessionId::new(2_022);
    let mut index = TcpSessionConnectionIndex::empty();

    index.remember_session(
        session_id,
        connection.connection_id(),
        connection.local(),
        connection.remote(),
        connection.owner_worker(),
        connection.next_node(),
    );

    assert_eq!(
        index
            .lookup_by_tuple(local, remote)
            .expect("IPv6 tuple lookup"),
        (session_id, DataWorkerId::new(0), TcpInputNext::SynSent)
    );
    assert!(
        index
            .lookup_by_tuple(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), local.port()),
                remote
            )
            .is_none()
    );
}

#[test]
fn tcp_connection_owns_timer_active_and_pending_bits() {
    let mut connection = connection(TcpConnectionId::new(303), 50_303);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::RETRANSMIT));

    connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);

    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::RETRANSMIT));

    connection.tcp_timer_expire(TcpConnectionTimerKind::RETRANSMIT);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
    assert!(connection.tcp_timer_is_pending(TcpConnectionTimerKind::RETRANSMIT));
    assert!(connection.tcp_timer_is_live(TcpConnectionTimerKind::RETRANSMIT));

    assert!(connection.tcp_timer_take_pending(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_is_live(TcpConnectionTimerKind::RETRANSMIT));
}

#[test]
fn tcp_connection_timer_dispatch_skips_rearmed_pending_timer() {
    let mut connection = connection(TcpConnectionId::new(304), 50_304);

    connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);
    connection.tcp_timer_expire(TcpConnectionTimerKind::RETRANSMIT);
    connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);

    assert!(!connection.tcp_timer_dispatch_pending(TcpConnectionTimerKind::RETRANSMIT));
    assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::RETRANSMIT));
}

#[test]
fn tcp_connection_timer_reset_clears_pending_dispatch() {
    let mut connection = connection(TcpConnectionId::new(305), 50_305);

    connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);
    connection.tcp_timer_expire(TcpConnectionTimerKind::RETRANSMIT);
    connection.tcp_timer_reset(TcpConnectionTimerKind::RETRANSMIT);

    assert!(!connection.tcp_timer_is_active(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::RETRANSMIT));
    assert!(!connection.tcp_timer_take_pending(TcpConnectionTimerKind::RETRANSMIT));
}
