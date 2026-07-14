use std::net::SocketAddr;

use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
use hammer_plugin_tcp::connection::TcpConnection;
use hammer_plugin_tcp::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpInputNext};
use hammer_runtime::DataWorkerId;
use hammer_service::transport::congestion::{BbrController, CongestionController};

fn connection(connection_id: TcpConnectionId, local_port: u16) -> TcpConnection<BbrController> {
    let local: SocketAddr = format!("192.0.2.10:{local_port}")
        .parse()
        .expect("test local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("test remote");
    let mut connection = TcpConnection::new(
        Some(connection_id),
        DataWorkerId::new(0),
        local_port,
        Some(local),
        remote,
    );
    connection.connect_state(1);
    connection
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
fn tcp_connections_own_independent_congestion_state() {
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
fn tcp_connection_exposes_owned_congestion_control() {
    let connection = connection(TcpConnectionId::new(3), 50_003);

    assert_eq!(
        connection.congestion().max_datagram_size(),
        DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32
    );
    assert_eq!(connection.congestion().delivered(), 0);
}

#[test]
fn tcp_connection_starts_closed_then_connects_to_syn_sent() {
    let local: SocketAddr = "192.0.2.10:50004".parse().expect("local");
    let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
    let mut connection: TcpConnection<BbrController> = TcpConnection::new(
        Some(TcpConnectionId::new(4)),
        DataWorkerId::new(0),
        local.port(),
        Some(local),
        remote,
    );

    assert_eq!(connection.state(), TcpState::Closed);
    assert_eq!(connection.next_node(), TcpInputNext::Drop);
    connection.connect_state(1);
    assert_eq!(connection.state(), TcpState::SynSent);
    assert_eq!(connection.next_node(), TcpInputNext::SynSent);
}
