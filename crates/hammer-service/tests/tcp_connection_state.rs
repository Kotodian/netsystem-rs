use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpState};
use hammer_service::session::SessionId;
use hammer_service::transport::tcp::congestion::TcpCongestionAckSample;
use hammer_service::transport::tcp::connection::TcpConnectionState;
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpConnectionTimerKind, TcpSessionConnectionIndex,
};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection(connection_id: TcpConnectionId, local_port: u16) -> TcpConnectionState {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpConnectionState::new(
        Some(connection_id),
        DataWorkerId::new(0),
        TcpState::Established,
        local_port,
        Some(local),
        remote,
    )
}

fn acknowledge(control: &mut hammer_service::transport::tcp::TcpCongestionState, now: Instant) {
    control.on_ack(TcpCongestionAckSample {
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

    first.congestion_mut().on_ack(TcpCongestionAckSample {
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

    acknowledge(connection.congestion_mut(), now);

    assert_eq!(
        connection.congestion().delivered(),
        u64::from(TEST_SEGMENT_LEN)
    );
    assert!(connection.congestion().congestion_window() > TEST_SEGMENT_LEN);
}

#[test]
fn tcp_connection_state_output_view_uses_owned_congestion_window() {
    let mut connection = connection(TcpConnectionId::new(4), 50_004);
    connection.set_send_state(1_000, 2_000, 65_535);
    connection.congestion_mut().on_loss(u32::MAX);

    let view = connection.output_send_view();

    assert_eq!(view.snd_una, 1_000);
    assert_eq!(view.snd_nxt, 2_000);
    assert_eq!(view.snd_wnd, 65_535);
    assert_eq!(
        view.congestion_window,
        connection.congestion().congestion_window()
    );
}

#[test]
fn tcp_connection_states_negotiate_tcp_options_independently() {
    let mut first = connection(TcpConnectionId::new(5), 50_005);
    let mut second = connection(TcpConnectionId::new(6), 50_006);

    first.set_local_capabilities(TcpCapabilities {
        window_scale: Some(4),
        sack: true,
        timestamps: true,
        ecn: true,
        ..TcpCapabilities::default()
    });
    second.set_local_capabilities(TcpCapabilities {
        window_scale: Some(1),
        sack: false,
        timestamps: true,
        ecn: true,
        ..TcpCapabilities::default()
    });

    let first_remote = TcpCapabilities {
        window_scale: Some(6),
        sack: true,
        timestamps: true,
        ecn: false,
        ..TcpCapabilities::default()
    };
    let second_remote = TcpCapabilities {
        window_scale: Some(12),
        sack: true,
        timestamps: false,
        ecn: true,
        ..TcpCapabilities::default()
    };

    let first_negotiated = first.apply_peer_handshake_capabilities(first_remote);
    let second_negotiated = second.apply_peer_handshake_capabilities(second_remote);

    assert_eq!(first.remote_capabilities(), Some(first_remote));
    assert_eq!(second.remote_capabilities(), Some(second_remote));
    assert_eq!(first.effective_send_window_scale(), 6);
    assert_eq!(first.effective_receive_window_scale(), 4);
    assert_eq!(second.effective_send_window_scale(), 12);
    assert_eq!(second.effective_receive_window_scale(), 1);
    assert!(first_negotiated.sack);
    assert!(first_negotiated.timestamps);
    assert!(!first_negotiated.ecn);
    assert!(!second_negotiated.sack);
    assert!(!second_negotiated.timestamps);
    assert!(second_negotiated.ecn);
    assert_eq!(first.negotiated_options(), first_negotiated);
    assert_eq!(second.negotiated_options(), second_negotiated);
}

#[test]
fn tcp_connection_state_scales_advertised_windows_safely() {
    let mut connection = connection(TcpConnectionId::new(7), 50_007);
    connection.set_local_capabilities(TcpCapabilities {
        window_scale: Some(20),
        ..TcpCapabilities::default()
    });
    connection.apply_peer_handshake_capabilities(TcpCapabilities {
        window_scale: Some(20),
        ..TcpCapabilities::default()
    });

    assert_eq!(connection.effective_send_window_scale(), 14);
    assert_eq!(connection.effective_receive_window_scale(), 14);
    assert_eq!(
        connection.effective_send_window(u32::from(u16::MAX)),
        u32::from(u16::MAX) << 14
    );
    assert_eq!(connection.effective_send_window(u32::MAX), u32::MAX);
    assert_eq!(
        connection.advertised_receive_window(u32::from(u16::MAX) << 14),
        u16::MAX
    );
    assert_eq!(connection.advertised_receive_window(u32::MAX), u16::MAX);
}

#[test]
fn tcp_connection_state_scales_peer_window_into_send_view() {
    let mut connection = connection(TcpConnectionId::new(8), 50_008);
    connection.set_local_capabilities(TcpCapabilities {
        window_scale: Some(2),
        ..TcpCapabilities::default()
    });
    connection.apply_peer_handshake_capabilities(TcpCapabilities {
        window_scale: Some(5),
        ..TcpCapabilities::default()
    });

    connection.set_send_state(1_000, 1_200, 32);

    assert_eq!(connection.snd_wnd(), 32 << 5);
    assert_eq!(connection.output_send_view().snd_wnd, 32 << 5);
}

#[test]
fn tcp_connection_state_output_state_advertises_scaled_receive_window() {
    let local: SocketAddr = "192.0.2.10:50008".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    let mut connection = TcpConnectionState::new(
        Some(TcpConnectionId::new(9)),
        DataWorkerId::new(0),
        TcpState::Established,
        local.port(),
        Some(local),
        remote,
    );
    connection.set_local_capabilities(TcpCapabilities {
        window_scale: Some(4),
        ..TcpCapabilities::default()
    });
    connection.apply_peer_handshake_capabilities(TcpCapabilities {
        window_scale: Some(2),
        ..TcpCapabilities::default()
    });
    connection.set_receive_state(9_000, 8_192);

    assert_eq!(
        connection.advertised_receive_window(connection.rcv_wnd()),
        512
    );
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
    let connection = TcpConnectionState::new(
        Some(TcpConnectionId::new(202)),
        DataWorkerId::new(0),
        TcpState::Established,
        local.port(),
        Some(local),
        remote,
    );
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
