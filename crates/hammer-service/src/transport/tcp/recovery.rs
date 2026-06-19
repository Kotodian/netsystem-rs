use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::TcpSackBlock;
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, PacketNumber, RttSample,
};

const DEFAULT_RACK_TIMEOUT_TICKS: u64 = 6;
const DEFAULT_TLP_TIMEOUT_TICKS: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OutstandingSegment {
    packet_number: PacketNumber,
    sequence: u32,
    end_sequence: u32,
    bytes: u32,
    sent_at: Instant,
    retransmitted: bool,
    is_probe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpRecoveryAck {
    pub acknowledgment: u32,
    pub now: Instant,
    pub latest_rtt: Duration,
    pub min_rtt: Duration,
    pub app_limited: bool,
    pub ecn_ce: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TcpRecoveryState {
    next_packet_number: PacketNumber,
    sent: Vec<OutstandingSegment>,
    rack_pending_loss: Vec<OutstandingSegment>,
    rack_timer_armed: bool,
    tlp_timer_armed: bool,
}

impl TcpRecoveryState {
    pub fn new() -> Self {
        Self {
            next_packet_number: 1,
            sent: Vec::new(),
            rack_pending_loss: Vec::new(),
            rack_timer_armed: false,
            tlp_timer_armed: false,
        }
    }

    pub fn next_packet_number(&mut self) -> PacketNumber {
        let packet_number = self.next_packet_number;
        self.next_packet_number = self.next_packet_number.saturating_add(1);
        packet_number
    }

    pub fn record_sent(
        &mut self,
        packet_number: PacketNumber,
        sequence: u32,
        end_sequence: u32,
        bytes: u32,
        sent_at: Instant,
        retransmitted: bool,
        is_probe: bool,
    ) {
        self.sent.push(OutstandingSegment {
            packet_number,
            sequence,
            end_sequence,
            bytes,
            sent_at,
            retransmitted,
            is_probe,
        });
        self.tlp_timer_armed = true;
    }

    pub fn bytes_in_flight(&self) -> u32 {
        self.sent
            .iter()
            .fold(0u32, |total, segment| total.saturating_add(segment.bytes))
    }

    pub fn has_unacked_data(&self) -> bool {
        !self.sent.is_empty()
    }

    pub fn rack_timeout_ticks(&self) -> Option<u64> {
        self.rack_timer_armed.then_some(DEFAULT_RACK_TIMEOUT_TICKS)
    }

    pub fn tlp_timeout_ticks(&self) -> Option<u64> {
        (self.tlp_timer_armed && self.has_unacked_data()).then_some(DEFAULT_TLP_TIMEOUT_TICKS)
    }

    pub fn on_ack<C: CongestionController>(&mut self, ack: TcpRecoveryAck, congestion: &mut C) {
        let acked = self.take_acked_segments(|segment| {
            seq_before_or_equal(segment.end_sequence, ack.acknowledgment)
        });
        self.deliver_acked_segments(ack, acked, congestion);
        self.tlp_timer_armed = self.has_unacked_data();
    }

    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    ) {
        let mut acked = self.take_acked_segments(|segment| {
            seq_before_or_equal(segment.end_sequence, ack.acknowledgment)
        });
        let mut highest_sacked_right = ack.acknowledgment;
        for block in blocks {
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            acked.extend(self.take_acked_segments(|segment| {
                seq_before_or_equal(block.left_edge, segment.sequence)
                    && seq_before_or_equal(segment.end_sequence, block.right_edge)
            }));
        }
        self.deliver_acked_segments(ack, acked, congestion);
        if highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(highest_sacked_right);
        }
        self.tlp_timer_armed = self.has_unacked_data();
    }

    pub fn on_rack_timeout<C: CongestionController>(&mut self, now: Instant, congestion: &mut C) {
        while let Some(segment) = self.rack_pending_loss.pop() {
            self.remove_outstanding(segment.packet_number);
            congestion.on_loss(
                now,
                LostPacket {
                    packet_number: segment.packet_number,
                    bytes: segment.bytes,
                    sent_at: segment.sent_at,
                },
                false,
            );
        }
        self.rack_timer_armed = false;
        self.tlp_timer_armed = self.has_unacked_data();
    }

    #[cfg(test)]
    fn next_tlp_probe(&mut self) -> Option<OutstandingSegment> {
        let mut segment = *self.sent.iter().max_by_key(|segment| segment.sent_at)?;
        segment.is_probe = true;
        self.tlp_timer_armed = false;
        Some(segment)
    }

    fn mark_rack_candidates(&mut self, highest_sacked_right: u32) {
        for segment in self.sent.iter().copied() {
            if seq_before(segment.end_sequence, highest_sacked_right)
                && !self
                    .rack_pending_loss
                    .iter()
                    .any(|pending| pending.packet_number == segment.packet_number)
            {
                self.rack_pending_loss.push(segment);
            }
        }
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
    }

    fn remove_outstanding(&mut self, packet_number: PacketNumber) {
        if let Some(index) = self
            .sent
            .iter()
            .position(|segment| segment.packet_number == packet_number)
        {
            self.sent.remove(index);
        }
    }

    fn take_acked_segments(
        &mut self,
        mut is_acked: impl FnMut(OutstandingSegment) -> bool,
    ) -> Vec<OutstandingSegment> {
        let mut acked = Vec::new();
        let mut index = 0;
        while index < self.sent.len() {
            let segment = self.sent[index];
            if is_acked(segment) {
                acked.push(self.sent.remove(index));
            } else {
                index += 1;
            }
        }
        acked
    }

    fn deliver_acked_segments<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        acked: Vec<OutstandingSegment>,
        congestion: &mut C,
    ) {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut bytes_in_flight_after_ack = self.bytes_in_flight();
        let total_acked_bytes = acked
            .iter()
            .fold(0u32, |total, segment| total.saturating_add(segment.bytes));
        let mut bytes_in_flight_before_next_ack =
            bytes_in_flight_after_ack.saturating_add(total_acked_bytes);
        for segment in acked {
            largest_acked = largest_acked.max(segment.packet_number);
            any_acked = true;
            bytes_in_flight_before_next_ack =
                bytes_in_flight_before_next_ack.saturating_sub(segment.bytes);
            bytes_in_flight_after_ack = bytes_in_flight_before_next_ack;
            deliver_acked_segment(bytes_in_flight_after_ack, ack, segment, congestion);
        }
        if any_acked {
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
    }
}

impl Default for OutstandingSegment {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            packet_number: 0,
            sequence: 0,
            end_sequence: 0,
            bytes: 0,
            sent_at: now,
            retransmitted: false,
            is_probe: false,
        }
    }
}

#[inline]
fn seq_before(left: u32, right: u32) -> bool {
    left.wrapping_sub(right) > (1 << 31)
}

#[inline]
fn seq_before_or_equal(left: u32, right: u32) -> bool {
    left == right || seq_before(left, right)
}

fn deliver_acked_segment<C: CongestionController>(
    bytes_in_flight: u32,
    ack: TcpRecoveryAck,
    segment: OutstandingSegment,
    congestion: &mut C,
) {
    congestion.on_ack(
        ack.now,
        AckedPacket {
            packet_number: segment.packet_number,
            bytes: segment.bytes,
            sent_at: segment.sent_at,
            app_limited: ack.app_limited,
            ecn_ce: ack.ecn_ce,
        },
        RttSample {
            latest: ack.latest_rtt,
            min: ack.min_rtt,
        },
        bytes_in_flight,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Default)]
    struct RecordingController {
        acked: Vec<AckedPacket>,
        acked_bytes_in_flight: Vec<u32>,
        lost: Vec<LostPacket>,
        end_acks: u32,
        mtu: u32,
    }

    impl CongestionController for RecordingController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                mtu: max_datagram_size,
                ..Self::default()
            }
        }

        fn metrics(&self) -> crate::transport::congestion::CongestionMetrics {
            crate::transport::congestion::CongestionMetrics {
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

        fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}

        fn on_ack(&mut self, _: Instant, acked: AckedPacket, _: RttSample, bytes_in_flight: u32) {
            self.acked_bytes_in_flight.push(bytes_in_flight);
            self.acked.push(acked);
        }

        fn on_end_acks(&mut self, _: Instant, _: u32, _: bool, _: PacketNumber) {
            self.end_acks += 1;
        }

        fn on_loss(&mut self, _: Instant, lost: LostPacket, _: bool) {
            self.lost.push(lost);
        }

        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.mtu = max_datagram_size;
        }

        fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
            (pending_bytes != 0).then_some(Duration::ZERO)
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

    fn record_sent_for_test(
        recovery: &mut TcpRecoveryState,
        packet_number: PacketNumber,
        sequence: u32,
        end_sequence: u32,
        bytes: u32,
        sent_at: Instant,
    ) {
        recovery.record_sent(
            packet_number,
            sequence,
            end_sequence,
            bytes,
            sent_at,
            false,
            false,
        );
    }

    #[test]
    fn on_ack_acknowledges_cumulative_range() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_460);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );

        recovery.on_ack(
            ack(2_000, now + Duration::from_millis(40), 40),
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 1);
        assert_eq!(controller.acked[0].packet_number, 1);
        assert_eq!(controller.acked_bytes_in_flight.as_slice(), &[1_000]);
        assert_eq!(recovery.bytes_in_flight(), 1_000);
    }

    #[test]
    fn on_ack_reports_per_acked_segment_bytes_in_flight() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_460);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );

        recovery.on_ack(
            ack(3_000, now + Duration::from_millis(30), 40),
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 2);
        assert_eq!(controller.acked[0].packet_number, 1);
        assert_eq!(controller.acked[1].packet_number, 2);
        assert_eq!(controller.acked_bytes_in_flight.as_slice(), &[1_000, 0]);
    }

    #[test]
    fn on_sack_blocks_marks_older_segment_lost_after_later_sack() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_460);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );

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
    fn next_tlp_probe_selects_newest_outstanding_segment() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );

        let probe = recovery.next_tlp_probe().expect("tlp probe");

        assert_eq!(probe.packet_number, 2);
        assert!(probe.is_probe);
    }

    #[test]
    fn recovery_module_does_not_depend_on_session_app_or_bbr_layers() {
        let source = include_str!("recovery.rs");
        let tests_start = source.find("#[cfg(test)]").expect("tests module");
        let module_body = &source[..tests_start];
        let forbidden = [
            "crate::session::",
            "hammer_runtime::app::",
            "BbrController",
            "TcpCongestionController",
            "AppRingHandle",
            "SessionId",
        ];

        for pattern in forbidden {
            assert!(
                !module_body.contains(pattern),
                "recovery.rs unexpectedly depends on forbidden layer symbol: {pattern}"
            );
        }
    }
}
