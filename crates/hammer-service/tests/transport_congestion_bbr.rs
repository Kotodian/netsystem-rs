use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use hammer_adapter::DataWorkerId;
use hammer_service::transport::congestion::{
    AckedPacket, BbrController, BbrMode, CongestionController, CongestionMetrics, LostPacket,
    PacketNumber, RttSample,
};
use hammer_service::transport::tcp::DEFAULT_TCP_OUTPUT_PAYLOAD_LEN;
use hammer_service::transport::tcp::connection::TcpConnection;

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
            ecn_ce: false,
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

#[derive(Clone, Debug)]
struct TestController(BbrController);

impl CongestionController for TestController {
    fn new(max_datagram_size: u32) -> Self {
        Self(BbrController::new(max_datagram_size))
    }

    fn metrics(&self) -> CongestionMetrics {
        self.0.metrics()
    }

    fn max_datagram_size(&self) -> u32 {
        self.0.max_datagram_size()
    }

    fn congestion_window(&self) -> u32 {
        self.0.congestion_window()
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        self.0.pacing_rate_bytes_per_second()
    }

    fn delivered(&self) -> u64 {
        self.0.delivered()
    }

    fn min_rtt(&self) -> Option<Duration> {
        self.0.min_rtt()
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        self.0.max_bandwidth_bytes_per_second()
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        self.0
            .on_packet_sent(packet_number, bytes_sent, bytes_in_flight, now);
    }

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32) {
        self.0.on_ack(now, acked, rtt, bytes_in_flight);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        self.0
            .on_end_acks(now, bytes_in_flight, app_limited, largest_acked_packet);
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool) {
        self.0.on_loss(now, lost, persistent_congestion);
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        self.0.on_mtu_update(max_datagram_size);
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        self.0.next_send_delay(pending_bytes)
    }
}

#[test]
fn tcp_connection_uses_left_hand_congestion_controller_type() {
    let remote = "127.0.0.1:443".parse().expect("remote");
    let connection: TcpConnection<TestController> =
        TcpConnection::new(None, DataWorkerId::new(0), 10_000, None, remote);

    assert_eq!(connection.congestion().max_datagram_size(), MSS);
}

#[test]
fn shared_congestion_has_no_tcp_session_app_or_quic_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/congestion");
    let mut combined = String::new();
    for entry in fs::read_dir(root).expect("read congestion dir") {
        let entry = entry.expect("dir entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("rs") {
            combined.push_str(&fs::read_to_string(entry.path()).expect("read source"));
        }
    }

    for forbidden in [
        "TcpSeq",
        "TcpConnection",
        "TcpConnectionState",
        "TcpConnection",
        "SessionId",
        "SessionQueue",
        "AppRing",
        "AppOp",
        "TcpSegment",
        "TcpPacket",
        "QuicConnection",
        "QuicSession",
        "QuicPacket",
        "QuicStream",
    ] {
        assert!(
            !combined.contains(forbidden),
            "shared congestion leaked transport/session/app type: {forbidden}"
        );
    }
}
