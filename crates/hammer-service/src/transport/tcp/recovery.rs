use std::time::{Duration, Instant};

use hammer_adapter::BufferIndex;
use hammer_core::protocol::tcp::TcpSackBlock;
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, PacketNumber, RttSample,
};

const DEFAULT_RACK_TIMEOUT_TICKS: u64 = 6;
const DEFAULT_TLP_TIMEOUT_TICKS: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpRecoveryAck {
    pub acknowledgment: u32,
    pub now: Instant,
    pub latest_rtt: Duration,
    pub min_rtt: Duration,
    pub app_limited: bool,
    pub ecn_ce_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct TcpRecoveryState {
    next_packet_number: PacketNumber,
    sent_packet_numbers: Vec<PacketNumber>,
    sent_sequences: Vec<u32>,
    sent_end_sequences: Vec<u32>,
    sent_bytes: Vec<u32>,
    sent_payloads: Vec<Option<BufferIndex>>,
    sent_at: Vec<Instant>,
    sent_retransmitted: Vec<bool>,
    sent_probes: Vec<bool>,
    rack_pending_loss: Vec<PacketNumber>,
    rack_timer_armed: bool,
    tlp_timer_armed: bool,
    recovery_active: bool,
    recovery_window: u32,
    recovery_prev_window: u32,
    recovery_delivered: u32,
    recovery_retransmitted: u32,
    recovery_new_data: u32,
    recovery_end_sequence: u32,
}

impl TcpRecoveryState {
    pub fn new() -> Self {
        Self {
            next_packet_number: 1,
            sent_packet_numbers: Vec::new(),
            sent_sequences: Vec::new(),
            sent_end_sequences: Vec::new(),
            sent_bytes: Vec::new(),
            sent_payloads: Vec::new(),
            sent_at: Vec::new(),
            sent_retransmitted: Vec::new(),
            sent_probes: Vec::new(),
            rack_pending_loss: Vec::new(),
            rack_timer_armed: false,
            tlp_timer_armed: false,
            recovery_active: false,
            recovery_window: 0,
            recovery_prev_window: 0,
            recovery_delivered: 0,
            recovery_retransmitted: 0,
            recovery_new_data: 0,
            recovery_end_sequence: 0,
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
        payload: Option<BufferIndex>,
        sent_at: Instant,
        retransmitted: bool,
        is_probe: bool,
    ) {
        self.sent_packet_numbers.push(packet_number);
        self.sent_sequences.push(sequence);
        self.sent_end_sequences.push(end_sequence);
        self.sent_bytes.push(bytes);
        self.sent_payloads.push(payload);
        self.sent_at.push(sent_at);
        self.sent_retransmitted.push(retransmitted);
        self.sent_probes.push(is_probe);
        self.tlp_timer_armed = true;
    }

    pub fn bytes_in_flight(&self) -> u32 {
        self.sent_bytes
            .iter()
            .fold(0u32, |total, bytes| total.saturating_add(*bytes))
    }

    pub fn has_unacked_data(&self) -> bool {
        !self.sent_packet_numbers.is_empty()
    }

    #[inline]
    pub fn in_recovery(&self) -> bool {
        self.recovery_active
    }

    pub fn rack_timeout_ticks(&self) -> Option<u64> {
        self.rack_timer_armed.then_some(DEFAULT_RACK_TIMEOUT_TICKS)
    }

    pub fn tlp_timeout_ticks(&self) -> Option<u64> {
        (self.tlp_timer_armed && self.has_unacked_data()).then_some(DEFAULT_TLP_TIMEOUT_TICKS)
    }

    pub fn on_ack<C: CongestionController>(&mut self, ack: TcpRecoveryAck, congestion: &mut C) {
        let acked =
            self.take_acked_segments(|_, end_sequence| seq_before_or_equal(end_sequence, ack.acknowledgment));
        let advanced = !acked.is_empty();
        self.deliver_acked_segments(ack, acked, congestion);
        self.maybe_finish_recovery(ack.acknowledgment);
        if self.recovery_active && advanced {
            if let Some(packet_number) = self.sent_packet_numbers.first().copied()
                && !self
                    .rack_pending_loss
                    .iter()
                    .any(|pending| *pending == packet_number)
            {
                self.rack_pending_loss.push(packet_number);
                self.rack_timer_armed = true;
            }
        }
        self.tlp_timer_armed = self.has_unacked_data();
    }

    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    ) {
        let mut acked =
            self.take_acked_segments(|_, end_sequence| seq_before_or_equal(end_sequence, ack.acknowledgment));
        let mut highest_sacked_right = ack.acknowledgment;
        for block in blocks {
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            acked.extend(self.take_acked_segments(|sequence, end_sequence| {
                seq_before_or_equal(block.left_edge, sequence)
                    && seq_before_or_equal(end_sequence, block.right_edge)
            }));
        }
        self.deliver_acked_segments(ack, acked, congestion);
        self.maybe_finish_recovery(ack.acknowledgment);
        if highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(highest_sacked_right);
        }
        self.tlp_timer_armed = self.has_unacked_data();
    }

    pub fn on_rack_timeout<C: CongestionController>(
        &mut self,
        now: Instant,
        snd_nxt: u32,
        congestion: &mut C,
    ) {
        let recovery_prev_window = congestion.congestion_window();
        let recovery_started = !self.recovery_active && !self.rack_pending_loss.is_empty();
        for packet_number in self.rack_pending_loss.iter().copied() {
            let Some(index) = self.find_sent(packet_number) else {
                continue;
            };
            congestion.on_loss(
                now,
                LostPacket {
                    packet_number,
                    bytes: self.sent_bytes[index],
                    sent_at: self.sent_at[index],
                },
                false,
            );
        }
        if recovery_started {
            self.recovery_active = true;
            self.recovery_prev_window = recovery_prev_window.max(1);
            self.recovery_window = congestion.congestion_window();
            self.recovery_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = snd_nxt;
        } else if self.recovery_active {
            self.recovery_window = self.recovery_window.min(congestion.congestion_window());
        }
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
        self.tlp_timer_armed = self.has_unacked_data();
    }

    #[inline]
    pub fn on_retransmit_sent(&mut self, bytes: u32) {
        if !self.recovery_active || bytes == 0 {
            return;
        }
        self.recovery_retransmitted = self.recovery_retransmitted.saturating_add(bytes);
    }

    #[inline]
    pub fn on_new_data_sent(&mut self, bytes: u32) {
        if !self.recovery_active || bytes == 0 {
            return;
        }
        self.recovery_new_data = self.recovery_new_data.saturating_add(bytes);
    }

    pub fn recovery_send_space(&self, bytes_in_flight: u32, max_datagram_size: u32) -> Option<u32> {
        if !self.recovery_active {
            return None;
        }
        let max_datagram_size = max_datagram_size.max(1);
        let prr_out = self
            .recovery_retransmitted
            .saturating_add(self.recovery_new_data);
        let mut space = if bytes_in_flight > self.recovery_window {
            let delivered = u128::from(self.recovery_delivered);
            let window = u128::from(self.recovery_window);
            let prev_window = u128::from(self.recovery_prev_window.max(1));
            let allowed = delivered.saturating_mul(window) / prev_window;
            let allowed = allowed.min(u128::from(u32::MAX)) as u32;
            allowed.saturating_sub(prr_out)
        } else {
            let limit = self
                .recovery_delivered
                .saturating_sub(prr_out)
                .saturating_add(max_datagram_size);
            self.recovery_window
                .saturating_sub(bytes_in_flight)
                .min(limit)
        };
        if prr_out == 0 {
            space = space.max(max_datagram_size);
        }
        Some(space)
    }

    pub fn take_rack_retransmit(&mut self) -> Option<(u32, u32, Option<BufferIndex>)> {
        let packet_number = self.rack_pending_loss.pop()?;
        let index = self.find_sent(packet_number)?;
        self.sent_retransmitted[index] = true;
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
        Some((
            self.sent_sequences[index],
            self.sent_bytes[index],
            self.sent_payloads[index],
        ))
    }

    pub fn take_tlp_probe(&mut self) -> Option<(PacketNumber, u32, u32, Option<BufferIndex>)> {
        let index = self
            .sent_at
            .iter()
            .enumerate()
            .max_by_key(|(_, sent_at)| *sent_at)
            .map(|(index, _)| index)?;
        self.sent_probes[index] = true;
        self.tlp_timer_armed = false;
        Some((
            self.sent_packet_numbers[index],
            self.sent_sequences[index],
            self.sent_bytes[index],
            self.sent_payloads[index],
        ))
    }

    fn mark_rack_candidates(&mut self, highest_sacked_right: u32) {
        for index in 0..self.sent_packet_numbers.len() {
            let packet_number = self.sent_packet_numbers[index];
            if seq_before(self.sent_end_sequences[index], highest_sacked_right)
                && !self
                    .rack_pending_loss
                    .iter()
                    .any(|pending| *pending == packet_number)
            {
                self.rack_pending_loss.push(packet_number);
            }
        }
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
    }

    fn take_acked_segments(
        &mut self,
        mut is_acked: impl FnMut(u32, u32) -> bool,
    ) -> Vec<(PacketNumber, u32, Instant)> {
        let mut acked = Vec::new();
        let mut index = 0;
        while index < self.sent_packet_numbers.len() {
            let sequence = self.sent_sequences[index];
            let end_sequence = self.sent_end_sequences[index];
            if is_acked(sequence, end_sequence) {
                acked.push(self.take_sent(index));
            } else {
                index += 1;
            }
        }
        acked
    }

    fn deliver_acked_segments<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        acked: Vec<(PacketNumber, u32, Instant)>,
        congestion: &mut C,
    ) {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut bytes_in_flight_after_ack = self.bytes_in_flight();
        let total_acked_bytes = acked
            .iter()
            .fold(0u32, |total, segment| total.saturating_add(segment.1));
        if self.recovery_active && total_acked_bytes != 0 {
            self.recovery_delivered = self.recovery_delivered.saturating_add(total_acked_bytes);
        }
        let mut bytes_in_flight_before_next_ack =
            bytes_in_flight_after_ack.saturating_add(total_acked_bytes);
        for segment in acked {
            largest_acked = largest_acked.max(segment.0);
            any_acked = true;
            bytes_in_flight_before_next_ack =
                bytes_in_flight_before_next_ack.saturating_sub(segment.1);
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

    fn find_sent(&self, packet_number: PacketNumber) -> Option<usize> {
        self.sent_packet_numbers
            .iter()
            .position(|current| *current == packet_number)
    }

    fn maybe_finish_recovery(&mut self, acknowledgment: u32) {
        if !self.recovery_active {
            return;
        }
        if !seq_before(acknowledgment, self.recovery_end_sequence) {
            self.recovery_active = false;
            self.recovery_window = 0;
            self.recovery_prev_window = 0;
            self.recovery_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = 0;
        }
    }

    fn take_sent(&mut self, index: usize) -> (PacketNumber, u32, Instant) {
        let packet_number = self.sent_packet_numbers[index];
        let mut pending_index = 0;
        while pending_index < self.rack_pending_loss.len() {
            if self.rack_pending_loss[pending_index] == packet_number {
                let _ = self.rack_pending_loss.remove(pending_index);
                continue;
            }
            pending_index += 1;
        }
        let packet_number = self.sent_packet_numbers.remove(index);
        let _ = self.sent_sequences.remove(index);
        let _ = self.sent_end_sequences.remove(index);
        let bytes = self.sent_bytes.remove(index);
        let _ = self.sent_payloads.remove(index);
        let sent_at = self.sent_at.remove(index);
        let _ = self.sent_retransmitted.remove(index);
        let _ = self.sent_probes.remove(index);
        (packet_number, bytes, sent_at)
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
    segment: (PacketNumber, u32, Instant),
    congestion: &mut C,
) {
    congestion.on_ack(
        ack.now,
        AckedPacket {
            packet_number: segment.0,
            bytes: segment.1,
            sent_at: segment.2,
            app_limited: ack.app_limited,
            ecn_ce_count: ack.ecn_ce_count,
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
        congestion_window: u32,
    }

    impl CongestionController for RecordingController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                mtu: max_datagram_size,
                congestion_window: max_datagram_size.saturating_mul(4),
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
            self.congestion_window
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
            self.congestion_window = self
                .congestion_window
                .saturating_sub(lost.bytes.max(self.mtu))
                .max(self.mtu);
        }

        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.mtu = max_datagram_size;
            self.congestion_window = self.congestion_window.max(max_datagram_size);
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
            ecn_ce_count: 0,
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
            None,
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
        recovery.on_rack_timeout(now + Duration::from_millis(56), 3_000, &mut controller);

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

        let probe = recovery.take_tlp_probe().expect("tlp probe");

        assert_eq!(probe.0, 2);
    }

    #[test]
    fn recovery_send_space_allows_one_mss_when_recovery_starts() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(&mut recovery, 2, 2_000, 3_000, 1_000, now + Duration::from_millis(1));
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: 2_000,
                right_edge: 3_000,
            }],
            &mut controller,
        );

        recovery.on_rack_timeout(now + Duration::from_millis(56), 3_000, &mut controller);

        assert_eq!(recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000), Some(1_000));
    }

    #[test]
    fn recovery_send_space_tracks_prr_delivery_and_send_accounting() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );
        record_sent_for_test(
            &mut recovery,
            3,
            3_000,
            4_000,
            1_000,
            now + Duration::from_millis(2),
        );
        record_sent_for_test(
            &mut recovery,
            4,
            4_000,
            5_000,
            1_000,
            now + Duration::from_millis(3),
        );
        record_sent_for_test(
            &mut recovery,
            5,
            5_000,
            6_000,
            1_000,
            now + Duration::from_millis(4),
        );
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: 2_000,
                right_edge: 3_000,
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(now + Duration::from_millis(56), 6_000, &mut controller);

        assert_eq!(recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000), Some(1_000));

        recovery.on_retransmit_sent(1_000);

        assert_eq!(recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000), Some(0));

        recovery.on_ack(ack(3_000, now + Duration::from_millis(90), 40), &mut controller);

        assert_eq!(recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000), Some(0));

        recovery.on_ack(ack(4_000, now + Duration::from_millis(120), 40), &mut controller);

        assert_eq!(recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000), Some(1_000));
    }

    #[test]
    fn recovery_partial_ack_marks_new_head_for_retransmit_without_sack() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            2_000,
            3_000,
            1_000,
            now + Duration::from_millis(1),
        );
        record_sent_for_test(
            &mut recovery,
            3,
            3_000,
            4_000,
            1_000,
            now + Duration::from_millis(2),
        );
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: 2_000,
                right_edge: 3_000,
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(now + Duration::from_millis(56), 4_000, &mut controller);

        let first = recovery
            .take_rack_retransmit()
            .expect("first recovery retransmit");
        assert_eq!(first.0, 1_000);

        recovery.on_retransmit_sent(1_000);
        recovery.on_ack(ack(2_000, now + Duration::from_millis(90), 40), &mut controller);

        let second = recovery
            .take_rack_retransmit()
            .expect("partial ack should schedule new head");
        assert_eq!(second.0, 3_000);
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
