use std::net::SocketAddr;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_service::transport::tcp::DEFAULT_TCP_OUTPUT_PAYLOAD_LEN;
use hammer_service::transport::tcp::congestion_control::{
    TcpCongestionAckObservation, TcpCongestionControlNode, TcpCongestionLossObservation,
    TcpCongestionSendObservation,
};
use hammer_service::transport::tcp::connection::TcpConnection;
use hammer_service::transport::tcp::state_machine::{Closed, SynSent};

const TEST_SEGMENT_LEN: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

fn connection() -> TcpConnection<SynSent> {
    let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    let connection: TcpConnection<Closed> = TcpConnection::new(
        Some(TcpConnectionId::new(7001)),
        DataWorkerId::new(0),
        50_000,
        Some(local),
        remote,
    );
    connection.connect_state(1)
}

#[test]
fn tcp_congestion_control_updates_one_connection_from_ack_sample() {
    let now = Instant::now();
    let mut connection = connection();

    let before = connection.congestion().congestion_window();

    TcpCongestionControlNode::observe_ack(
        &mut connection,
        TcpCongestionAckObservation {
            accepted_acknowledgment: TEST_SEGMENT_LEN,
            bytes_acked: TEST_SEGMENT_LEN,
            rtt: Duration::from_millis(20),
            now,
        },
    )
    .expect("ack observation");

    let after = connection.congestion().congestion_window();

    assert!(after > before);
}

#[test]
fn tcp_congestion_control_send_observation_sets_pacing_deadline_on_connection() {
    let now = Instant::now();
    let mut connection = connection();
    connection.observe_congestion_ack(
        hammer_service::transport::tcp::congestion::TcpCongestionAckSample {
            bytes_acked: TEST_SEGMENT_LEN,
            rtt: Duration::from_millis(20),
            now,
            bytes_in_flight: TEST_SEGMENT_LEN,
        },
    );

    TcpCongestionControlNode::observe_send(
        &mut connection,
        TcpCongestionSendObservation {
            bytes_sent: TEST_SEGMENT_LEN,
            bytes_in_flight: 0,
            now,
        },
    )
    .expect("send observation");

    assert!(connection.next_output_at().is_some());
}

#[test]
fn tcp_congestion_control_loss_reduces_only_target_connection() {
    let mut connection = connection();
    let before = connection.congestion().congestion_window();

    TcpCongestionControlNode::observe_loss(
        &mut connection,
        TcpCongestionLossObservation {
            bytes_lost: TEST_SEGMENT_LEN,
        },
    )
    .expect("loss observation");

    let after = connection.congestion().congestion_window();

    assert!(after < before);
}
