use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::{DataWorkerId, RouteMetadata};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_service::transport::tcp::congestion::{TcpCongestionAckSample, TcpCongestionControl};
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_PSH, TcpConnectionTable,
    TcpDataPlaneConnection, TcpLookupId, TcpOutputSegment,
};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection(
    lookup_id: TcpLookupId,
    connection_id: TcpConnectionId,
    local_port: u16,
) -> TcpDataPlaneConnection {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpDataPlaneConnection::new(
        lookup_id,
        Some(connection_id),
        DataWorkerId::new(0),
        TcpState::Established,
        local_port,
        Some(local),
        remote,
    )
}

fn output_segment(lookup_id: TcpLookupId, connection_id: TcpConnectionId) -> TcpOutputSegment {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    TcpOutputSegment {
        lookup_id,
        connection_id,
        local,
        remote,
        sequence: 1_000,
        acknowledgment: 2_000,
        flags: TCP_FLAG_ACK | TCP_FLAG_PSH,
        advertised_window: 4_096,
        payload: vec![1; DEFAULT_TCP_OUTPUT_PAYLOAD_LEN],
        metadata: RouteMetadata::default(),
        packet: vec![1; DEFAULT_TCP_OUTPUT_PAYLOAD_LEN],
    }
}

fn acknowledge<C: TcpCongestionControl>(control: &mut C, now: Instant) {
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
fn tcp_data_plane_connections_own_independent_congestion_state() {
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
fn tcp_data_plane_connection_exposes_trait_backed_congestion_control() {
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
fn tcp_data_plane_connection_output_view_uses_owned_congestion_window() {
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
fn tcp_connection_table_resolves_by_lookup_and_connection_id() {
    let first = connection(11, TcpConnectionId::new(101), 50_011);
    let second = connection(12, TcpConnectionId::new(102), 50_012);
    let mut table = TcpConnectionTable::empty();

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
    let segment = output_segment(11, TcpConnectionId::new(101));
    let by_lookup = table
        .lookup_by_lookup_id_mut(11)
        .expect("mutable lookup connection");
    by_lookup.set_send_state(1_000, 2_440, 32_768);
    by_lookup.set_next_output_at(Some(next_output_at));
    by_lookup.retransmit_queue_mut().track_segment(&segment);

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
