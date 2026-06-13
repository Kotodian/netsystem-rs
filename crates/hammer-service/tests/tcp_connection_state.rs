use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::{DataWorkerId, RouteMetadata};
use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpState};
use hammer_service::session::protocol::tcp::state::{TcpSessionState, TcpSessionTable};
use hammer_service::transport::tcp::congestion::TcpCongestionAckSample;
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_PSH, TcpLookupId, TcpOutputRecord,
    tcp_output_packet,
};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection(
    lookup_id: TcpLookupId,
    connection_id: TcpConnectionId,
    local_port: u16,
) -> TcpSessionState {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpSessionState::new(
        lookup_id,
        Some(connection_id),
        DataWorkerId::new(0),
        TcpState::Established,
        local_port,
        Some(local),
        remote,
    )
}

fn output_record(lookup_id: TcpLookupId, connection_id: TcpConnectionId) -> TcpOutputRecord {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpOutputRecord {
        lookup_id,
        connection_id,
        local,
        remote,
        sequence: 1_000,
        acknowledgment: 2_000,
        flags: TCP_FLAG_ACK | TCP_FLAG_PSH,
        advertised_window: 4_096,
        payload_len: DEFAULT_TCP_OUTPUT_PAYLOAD_LEN,
        metadata: RouteMetadata::default(),
    }
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
fn tcp_session_states_own_independent_congestion_state() {
    let now = Instant::now();
    let mut first = connection(1, TcpConnectionId::new(1), 50_001);
    let second = connection(2, TcpConnectionId::new(2), 50_002);

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
fn tcp_session_state_exposes_owned_congestion_control() {
    let now = Instant::now();
    let mut connection = connection(3, TcpConnectionId::new(3), 50_003);

    acknowledge(connection.congestion_mut(), now);

    assert_eq!(
        connection.congestion().delivered(),
        u64::from(TEST_SEGMENT_LEN)
    );
    assert!(connection.congestion().congestion_window() > TEST_SEGMENT_LEN);
}

#[test]
fn tcp_session_state_output_view_uses_owned_congestion_window() {
    let mut connection = connection(4, TcpConnectionId::new(4), 50_004);
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
fn tcp_session_states_negotiate_tcp_options_independently() {
    let mut first = connection(5, TcpConnectionId::new(5), 50_005);
    let mut second = connection(6, TcpConnectionId::new(6), 50_006);

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
fn tcp_session_state_scales_advertised_windows_safely() {
    let mut connection = connection(7, TcpConnectionId::new(7), 50_007);
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
fn tcp_session_state_scales_peer_window_into_send_view() {
    let mut connection = connection(8, TcpConnectionId::new(8), 50_008);
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
fn tcp_session_state_output_state_advertises_scaled_receive_window() {
    let local: SocketAddr = "192.0.2.10:50008".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    let mut connection = TcpSessionState::new(
        9,
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

    let record = tcp_output_packet(&connection, local, &[]).expect("output packet");

    assert_eq!(record.advertised_window, 512);
}

#[test]
fn tcp_connection_table_resolves_by_lookup_and_connection_id() {
    let first = connection(11, TcpConnectionId::new(101), 50_011);
    let second = connection(12, TcpConnectionId::new(102), 50_012);
    let mut table = TcpSessionTable::empty();

    table.insert(first);
    table.insert(second);

    assert_eq!(
        table
            .lookup_by_lookup_id(11)
            .expect("lookup connection")
            .local_port(),
        50_011
    );
    assert_eq!(
        table
            .lookup_by_connection_id(TcpConnectionId::new(102))
            .expect("id connection")
            .local_port(),
        50_012
    );

    let next_output_at = Instant::now() + Duration::from_millis(10);
    let record = output_record(11, TcpConnectionId::new(101));
    let by_lookup = table
        .lookup_by_lookup_id_mut(11)
        .expect("mutable lookup connection");
    by_lookup.set_send_state(1_000, 2_440, 32_768);
    by_lookup.set_next_output_at(Some(next_output_at));
    by_lookup.retransmit_queue_mut().track_output(&record);

    let by_id = table
        .lookup_by_connection_id_mut(TcpConnectionId::new(102))
        .expect("mutable id connection");
    by_id.set_send_state(2_000, 3_000, 16_384);

    let resolved = table
        .lookup_by_lookup_id(11)
        .expect("updated lookup connection");
    assert_eq!(resolved.lookup_id(), 11);
    assert_eq!(resolved.connection_id(), Some(TcpConnectionId::new(101)));
    assert_eq!(resolved.snd_una(), 1_000);
    assert_eq!(resolved.snd_nxt(), 2_440);
    assert_eq!(resolved.snd_wnd(), 32_768);
    assert_eq!(resolved.next_output_at(), Some(next_output_at));
    assert_eq!(resolved.retransmit_queue().len(), 1);

    let resolved_by_id = table
        .lookup_by_connection_id(TcpConnectionId::new(102))
        .expect("updated id connection");
    assert_eq!(resolved_by_id.snd_una(), 2_000);
    assert_eq!(resolved_by_id.snd_nxt(), 3_000);
    assert_eq!(resolved_by_id.snd_wnd(), 16_384);
}
