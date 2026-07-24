use std::time::{Duration, Instant};

use hammer_plugin_tcp::DEFAULT_TCP_OUTPUT_PAYLOAD_LEN;
use hammer_plugin_tcp::connection::TcpConnection;
use hammer_runtime::DataWorkerId;
use hammer_service::transport::congestion::{
    AckedPacket, BbrController, BbrMode, CongestionController, RttSample,
};

const MSS: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

#[test]
fn bbr_controller_implements_transport_congestion_controller() {
    let now = Instant::now();
    let mut controller = BbrController::new(MSS);

    controller.on_packet_sent(1, MSS, 0, now);
    controller.on_ack(
        now + Duration::from_millis(20),
        AckedPacket {
            packet_number: 1,
            bytes: MSS,
            sent_at: now,
            app_limited: false,
            ecn_ce_count: 0,
        },
        RttSample {
            latest: Duration::from_millis(20),
            min: Duration::from_millis(20),
        },
        MSS,
    );
    controller.on_end_acks(now + Duration::from_millis(20), MSS, false, 1);

    assert_eq!(controller.bbr_mode(), BbrMode::Startup);
    assert_eq!(controller.delivered(), u64::from(MSS));
    assert_eq!(controller.min_rtt(), Some(Duration::from_millis(20)));
    assert!(controller.pacing_rate_bytes_per_second().is_some());
}

#[test]
fn bbr_controller_reduces_cwnd_when_ack_reports_ce_feedback() {
    let now = Instant::now();
    let mut controller = BbrController::new(MSS);
    let initial_cwnd = controller.congestion_window();

    controller.on_packet_sent(1, MSS * 4, 0, now);
    controller.on_ack(
        now + Duration::from_millis(20),
        AckedPacket {
            packet_number: 1,
            bytes: MSS * 4,
            sent_at: now,
            app_limited: false,
            ecn_ce_count: 2,
        },
        RttSample {
            latest: Duration::from_millis(20),
            min: Duration::from_millis(20),
        },
        MSS * 4,
    );

    assert!(controller.congestion_window() < initial_cwnd + MSS * 4);
}

#[test]
fn tcp_connection_reports_congestion_without_exposing_controller_type() {
    let remote = "127.0.0.1:443".parse().expect("remote");
    let connection = TcpConnection::new(None, DataWorkerId::new(0), 10_000, None, remote);

    assert_eq!(connection.congestion_metrics().congestion_window, MSS * 10);
}
