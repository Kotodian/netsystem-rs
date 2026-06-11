use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::{DataWorkerId, RouteMetadata};
use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_service::transport::tcp::congestion::TcpCongestionAckSample;
use hammer_service::transport::tcp::congestion_control::{
    TcpCongestionAckObservation, TcpCongestionControlNode, TcpCongestionLossObservation,
    TcpCongestionSendObservation,
};
use hammer_service::transport::tcp::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_PSH, TcpConnectionTable,
    TcpDataPlaneConnection, TcpOutputSegment,
};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection() -> TcpDataPlaneConnection {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    let mut connection = TcpDataPlaneConnection::new(
        7,
        Some(TcpConnectionId::new(7001)),
        DataWorkerId::new(0),
        TcpState::Established,
        50_000,
        Some(local),
        remote,
    );
    connection.set_send_state(1000, 1000, 65_535);
    connection
}

fn segment(sequence: u32, payload: &[u8]) -> TcpOutputSegment {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    TcpOutputSegment {
        lookup_id: 7,
        connection_id: TcpConnectionId::new(7001),
        local,
        remote,
        sequence,
        acknowledgment: 2000,
        flags: TCP_FLAG_ACK | TCP_FLAG_PSH,
        advertised_window: 4096,
        payload: payload.to_vec(),
        metadata: RouteMetadata::default(),
        packet: payload.to_vec(),
    }
}

#[test]
fn tcp_congestion_node_updates_one_connection_from_ack_sample() {
    let now = Instant::now();
    let mut table = TcpConnectionTable::empty();
    let mut connection = connection();
    let sent = segment(1000, &[1; DEFAULT_TCP_OUTPUT_PAYLOAD_LEN]);
    connection
        .retransmit_queue_mut()
        .track_segment_with_sent_at(&sent, now - Duration::from_millis(20));
    connection.set_send_state(1000, sent.next_send_sequence(), 65_535);
    table.insert(connection);

    let before = table
        .lookup_by_lookup_id(7)
        .expect("connection")
        .congestion()
        .congestion_window();

    TcpCongestionControlNode::observe_ack(
        &mut table,
        TcpCongestionAckObservation {
            lookup_id: 7,
            accepted_acknowledgment: sent.next_send_sequence(),
            now,
        },
    )
    .expect("ack observation");

    let after = table
        .lookup_by_lookup_id(7)
        .expect("connection")
        .congestion()
        .congestion_window();

    assert!(after > before);
}

#[test]
fn tcp_congestion_node_send_observation_sets_pacing_deadline_on_connection() {
    let now = Instant::now();
    let mut table = TcpConnectionTable::empty();
    let mut connection = connection();
    connection.congestion_mut().on_ack(TcpCongestionAckSample {
        bytes_acked: TEST_SEGMENT_LEN,
        rtt: Duration::from_millis(20),
        now,
        bytes_in_flight: TEST_SEGMENT_LEN,
    });
    table.insert(connection);

    TcpCongestionControlNode::observe_send(
        &mut table,
        TcpCongestionSendObservation {
            lookup_id: 7,
            bytes_sent: TEST_SEGMENT_LEN,
            bytes_in_flight: 0,
            now,
        },
    )
    .expect("send observation");

    assert!(
        table
            .lookup_by_lookup_id(7)
            .expect("connection")
            .next_output_at()
            .is_some()
    );
}

#[test]
fn tcp_congestion_node_loss_reduces_only_target_connection() {
    let mut table = TcpConnectionTable::empty();
    table.insert(connection());
    let before = table
        .lookup_by_lookup_id(7)
        .expect("connection")
        .congestion()
        .congestion_window();

    TcpCongestionControlNode::observe_loss(
        &mut table,
        TcpCongestionLossObservation {
            lookup_id: 7,
            bytes_lost: TEST_SEGMENT_LEN,
        },
    )
    .expect("loss observation");

    let after = table
        .lookup_by_lookup_id(7)
        .expect("connection")
        .congestion()
        .congestion_window();

    assert!(after < before);
}
