use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::TcpSackBlock;
use hammer_infra::vec::Vec;
use hammer_service::transport::congestion::{
    AckedPacket, CongestionController, CongestionMetrics, LostPacket, PacketNumber, RttSample,
};
use hammer_service::transport::tcp::recovery::{TcpRecoveryAck, TcpRecoveryState, TcpSentSegment};

#[derive(Clone, Debug, Default)]
struct RecordingController {
    acked: Vec<AckedPacket>,
    lost: Vec<LostPacket>,
    sent: Vec<PacketNumber>,
    end_acks: u32,
    last_rtt: Option<RttSample>,
    last_event_at: Option<Instant>,
    last_sent_bytes: u32,
    last_bytes_in_flight: u32,
    app_limited_end: bool,
    largest_acked_packet: PacketNumber,
    mtu: u32,
    pending_send_delay: Option<Duration>,
}

impl CongestionController for RecordingController {
    fn new(max_datagram_size: u32) -> Self {
        Self {
            mtu: max_datagram_size,
            ..Self::default()
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        CongestionMetrics {
            congestion_window: 0,
            pacing_rate_bytes_per_second: None,
            delivered: self.acked.len() as u64,
            max_bandwidth_bytes_per_second: 0,
            min_rtt: None,
        }
    }

    fn max_datagram_size(&self) -> u32 {
        self.mtu
    }

    fn congestion_window(&self) -> u32 {
        0
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        None
    }

    fn delivered(&self) -> u64 {
        self.acked.len() as u64
    }

    fn min_rtt(&self) -> Option<Duration> {
        None
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        0
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        self.sent.push(packet_number);
        self.last_event_at = Some(now);
        self.last_sent_bytes = bytes_sent;
        self.last_bytes_in_flight = bytes_in_flight.saturating_add(bytes_sent);
        self.pending_send_delay = Some(Duration::ZERO);
    }

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32) {
        self.pending_send_delay = Some(now.saturating_duration_since(acked.sent_at));
        self.last_event_at = Some(now);
        self.last_rtt = Some(rtt);
        self.last_bytes_in_flight = bytes_in_flight;
        self.acked.push(acked);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        self.last_event_at = Some(now);
        self.pending_send_delay = Some(Duration::ZERO);
        self.last_bytes_in_flight = bytes_in_flight;
        self.app_limited_end = app_limited;
        self.largest_acked_packet = largest_acked_packet;
        self.end_acks += 1;
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool) {
        self.last_event_at = Some(now);
        self.pending_send_delay = Some(now.saturating_duration_since(lost.sent_at));
        if persistent_congestion {
            self.last_bytes_in_flight = 0;
        }
        self.lost.push(lost);
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        self.mtu = max_datagram_size;
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        if pending_bytes == 0 {
            None
        } else {
            self.pending_send_delay
        }
    }
}

fn segment(packet_number: u64, sequence: u32, len: u32, sent_at: Instant) -> TcpSentSegment {
    TcpSentSegment {
        packet_number,
        sequence,
        end_sequence: sequence.wrapping_add(len),
        bytes: len,
        sent_at,
        retransmitted: false,
        is_probe: false,
    }
}

fn ack(acknowledgment: u32, now: Instant, rtt_ms: u64) -> TcpRecoveryAck {
    TcpRecoveryAck {
        acknowledgment,
        now,
        latest_rtt: Duration::from_millis(rtt_ms),
        min_rtt: Duration::from_millis(rtt_ms),
        app_limited: false,
        ecn_ce: false,
    }
}

#[test]
fn rack_cumulative_ack_feeds_controller_ack_sample() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));

    recovery.on_ack(
        ack(2_000, now + Duration::from_millis(40), 40),
        &mut controller,
    );

    assert_eq!(controller.acked.len(), 1);
    assert_eq!(controller.acked[0].packet_number, 1);
    assert_eq!(controller.acked[0].bytes, 1_000);
    assert_eq!(controller.end_acks, 1);
    assert_eq!(recovery.bytes_in_flight(), 1_000);
}

#[test]
fn rack_marks_older_unacked_segment_lost_after_later_sack() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));

    recovery.on_sack_blocks(
        ack(1_000, now + Duration::from_millis(30), 40),
        &[TcpSackBlock {
            left_edge: 2_000,
            right_edge: 3_000,
        }],
        &mut controller,
    );
    recovery.on_rack_timeout(now + Duration::from_millis(56), &mut controller);

    assert_eq!(controller.lost.len(), 1);
    assert_eq!(controller.lost[0].packet_number, 1);
}

#[test]
fn tlp_selects_newest_outstanding_segment_as_probe() {
    let now = Instant::now();
    let mut recovery = TcpRecoveryState::new();
    let mut controller = RecordingController::new(1_460);
    recovery.record_sent(segment(1, 1_000, 1_000, now));
    recovery.record_sent(segment(2, 2_000, 1_000, now + Duration::from_millis(1)));
    recovery.on_ack(
        ack(2_000, now + Duration::from_millis(40), 40),
        &mut controller,
    );

    let probe = recovery.next_tlp_probe().expect("tlp probe");

    assert_eq!(probe.packet_number, 2);
    assert_eq!(probe.sequence, 2_000);
    assert!(probe.is_probe);
}

#[test]
fn tcp_recovery_uses_infra_vec_and_has_no_session_app_or_bbr_types() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport/tcp/recovery.rs"),
    )
    .expect("read recovery source");

    assert!(source.contains("hammer_infra::vec::Vec"));
    for forbidden in [
        "TcpConnectionState",
        "TcpSession",
        "SessionId",
        "SessionQueue",
        "AppRing",
        "AppOp",
        "BbrController",
        "BbrMode",
    ] {
        assert!(
            !source.contains(forbidden),
            "tcp recovery leaked unrelated type: {forbidden}"
        );
    }
}
