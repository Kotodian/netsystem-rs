use std::time::{Duration, Instant};

use hammer_adapter::BufferIndex;
use hammer_core::protocol::tcp::{TcpSackBlock, TcpSeq};
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::rbtree::RbTree;
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, PacketNumber, RttSample,
};

const DEFAULT_RACK_TIMEOUT_TICKS: u64 = 6;
const DEFAULT_TLP_TIMEOUT_TICKS: u64 = 20;

#[derive(Clone, Copy, Debug)]
pub(crate) struct TcpSentSample {
    pub(crate) packet_number: PacketNumber,
    pub(crate) sequence: TcpSeq,
    pub(crate) end_sequence: TcpSeq,
    pub(crate) bytes: u32,
    pub(crate) payload: Option<BufferIndex>,
    pub(crate) payload_offset: u32,
    pub(crate) payload_len: u32,
    pub(crate) retransmitted: bool,
    pub(crate) sent_at: Instant,
    pub(crate) prev: Option<PoolIndex>,
    pub(crate) next: Option<PoolIndex>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpRecoveryAck {
    pub acknowledgment: TcpSeq,
    pub now: Instant,
    pub app_limited: bool,
    pub ecn_ce_count: u64,
}

#[derive(Debug)]
pub struct TcpRecoveryState {
    next_packet_number: PacketNumber,
    sent_samples: Pool<TcpSentSample>,
    sample_lookup: RbTree<TcpSeq, PoolIndex>,
    sample_head: Option<PoolIndex>,
    sample_tail: Option<PoolIndex>,
    rack_pending_loss: Vec<PoolIndex>,
    rack_timer_armed: bool,
    tlp_timer_armed: bool,
    recovery_active: bool,
    recovery_window: u32,
    recovery_prev_window: u32,
    recovery_delivered: u32,
    recovery_retransmitted: u32,
    recovery_new_data: u32,
    recovery_end_sequence: TcpSeq,
}

impl TcpRecoveryState {
    pub fn new() -> Self {
        Self {
            next_packet_number: 1,
            sent_samples: Pool::with_capacity(32),
            sample_lookup: RbTree::with_capacity(32),
            sample_head: None,
            sample_tail: None,
            rack_pending_loss: Vec::new(),
            rack_timer_armed: false,
            tlp_timer_armed: false,
            recovery_active: false,
            recovery_window: 0,
            recovery_prev_window: 0,
            recovery_delivered: 0,
            recovery_retransmitted: 0,
            recovery_new_data: 0,
            recovery_end_sequence: TcpSeq::from(0),
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
        sequence: TcpSeq,
        end_sequence: TcpSeq,
        bytes: u32,
        payload: Option<BufferIndex>,
        payload_offset: u32,
        payload_len: u32,
        sent_at: Instant,
    ) {
        let prev = self.sample_tail;
        let sample_index = self
            .sent_samples
            .insert(TcpSentSample {
                packet_number,
                sequence,
                end_sequence,
                bytes,
                payload,
                payload_offset,
                payload_len,
                retransmitted: false,
                sent_at,
                prev,
                next: None,
            })
            .expect("tcp recovery sample pool exhausted");
        if let Some(prev_index) = prev {
            self.sent_sample_mut(prev_index).next = Some(sample_index);
        } else {
            self.sample_head = Some(sample_index);
        }
        self.sample_tail = Some(sample_index);
        let replaced = self.sample_lookup.insert(sequence, sample_index);
        debug_assert!(
            replaced.is_none(),
            "tcp recovery sample lookup key should remain unique"
        );
        self.tlp_timer_armed = true;
    }

    pub fn bytes_in_flight(&self) -> u32 {
        self.sent_samples.iter().fold(0u32, |total, (_, sample)| {
            total.saturating_add(sample.bytes)
        })
    }

    pub fn has_unacked_data(&self) -> bool {
        !self.sent_samples.is_empty()
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

    pub fn on_ack<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        congestion: &mut C,
    ) -> Option<Duration> {
        let acked = self.take_acked_segments(|_, end_sequence| {
            end_sequence <= ack.acknowledgment
        });
        let advanced = !acked.is_empty();
        let latest_rtt = self.deliver_acked_segments(ack, acked, congestion);
        self.maybe_finish_recovery(ack.acknowledgment);
        if self.recovery_active && advanced {
            if let Some(head) = self.sample_head
                && !self
                    .rack_pending_loss
                    .iter()
                    .any(|pending| *pending == head)
            {
                self.rack_pending_loss.push(head);
                self.rack_timer_armed = true;
            }
        }
        self.tlp_timer_armed = self.has_unacked_data();
        latest_rtt
    }

    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    ) -> Option<Duration> {
        let mut acked = self.take_acked_segments(|_, end_sequence| {
            end_sequence <= ack.acknowledgment
        });
        let mut highest_sacked_right = ack.acknowledgment;
        for block in blocks {
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            acked.extend(self.take_sacked_segments(*block));
        }
        let latest_rtt = self.deliver_acked_segments(ack, acked, congestion);
        self.maybe_finish_recovery(ack.acknowledgment);
        if highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(highest_sacked_right);
        }
        self.tlp_timer_armed = self.has_unacked_data();
        latest_rtt
    }

    pub fn on_rack_timeout<C: CongestionController>(
        &mut self,
        now: Instant,
        snd_nxt: TcpSeq,
        congestion: &mut C,
    ) {
        let recovery_prev_window = congestion.congestion_window();
        let recovery_started = !self.recovery_active && !self.rack_pending_loss.is_empty();
        for sample_index in self.rack_pending_loss.iter().copied() {
            let Some(sample) = self.sent_samples.get(sample_index).copied() else {
                continue;
            };
            congestion.on_loss(
                now,
                LostPacket {
                    packet_number: sample.packet_number,
                    bytes: sample.bytes,
                    sent_at: sample.sent_at,
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

    pub(crate) fn take_rack_retransmit(&mut self) -> Option<TcpSentSample> {
        let sample_index = self.rack_pending_loss.pop()?;
        let sample = self.sent_samples.get_mut(sample_index)?;
        sample.retransmitted = true;
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
        Some(*sample)
    }

    pub(crate) fn take_tlp_probe(&mut self) -> Option<TcpSentSample> {
        let index = self.sample_tail?;
        let sample = self.sent_samples.get_mut(index)?;
        sample.retransmitted = true;
        self.tlp_timer_armed = false;
        Some(*sample)
    }

    pub(crate) fn oldest_unacked_sample(&self) -> Option<TcpSentSample> {
        let index = self.sample_head?;
        Some(self.sent_sample(index))
    }

    fn mark_rack_candidates(&mut self, highest_sacked_right: TcpSeq) {
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if sample.end_sequence < highest_sacked_right
                && !self
                    .rack_pending_loss
                    .iter()
                    .any(|pending| *pending == index)
            {
                self.rack_pending_loss.push(index);
            }
            cursor = sample.next;
        }
        self.rack_timer_armed = !self.rack_pending_loss.is_empty();
    }

    fn take_acked_segments(
        &mut self,
        mut is_acked: impl FnMut(TcpSeq, TcpSeq) -> bool,
    ) -> Vec<TcpSentSample> {
        let mut acked = Vec::new();
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            let next = sample.next;
            let sequence = sample.sequence;
            let end_sequence = sample.end_sequence;
            if is_acked(sequence, end_sequence) {
                acked.push(self.take_sent(index));
            }
            cursor = next;
        }
        acked
    }

    fn take_sacked_segments(
        &mut self,
        block: TcpSackBlock,
    ) -> Vec<TcpSentSample> {
        let mut matched = Vec::new();
        let left_edge = block.left_edge;
        let right_edge = block.right_edge;
        let mut cursor = self.first_sample_at_or_after(left_edge);
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if right_edge < sample.sequence {
                break;
            }
            cursor = self.next_sample_after(sample.sequence);
            if left_edge <= sample.sequence
                && sample.end_sequence <= right_edge
            {
                matched.push(index);
            }
        }
        matched
            .into_iter()
            .map(|index| self.take_sent(index))
            .collect()
    }

    fn deliver_acked_segments<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        acked: Vec<TcpSentSample>,
        congestion: &mut C,
    ) -> Option<Duration> {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut latest_rtt = None;
        let mut bytes_in_flight_after_ack = self.bytes_in_flight();
        let total_acked_bytes = acked
            .iter()
            .fold(0u32, |total, segment| total.saturating_add(segment.bytes));
        if self.recovery_active && total_acked_bytes != 0 {
            self.recovery_delivered = self.recovery_delivered.saturating_add(total_acked_bytes);
        }
        let mut bytes_in_flight_before_next_ack =
            bytes_in_flight_after_ack.saturating_add(total_acked_bytes);
        for segment in acked {
            largest_acked = largest_acked.max(segment.packet_number);
            any_acked = true;
            bytes_in_flight_before_next_ack =
                bytes_in_flight_before_next_ack.saturating_sub(segment.bytes);
            bytes_in_flight_after_ack = bytes_in_flight_before_next_ack;
            latest_rtt = deliver_acked_segment(
                bytes_in_flight_after_ack,
                ack,
                segment,
                congestion,
            )
            .or(latest_rtt);
        }
        if any_acked {
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        latest_rtt
    }

    fn sent_sample(&self, index: PoolIndex) -> TcpSentSample {
        self.sent_samples
            .get(index)
            .copied()
            .expect("tcp recovery sample index is valid")
    }

    fn sent_sample_mut(&mut self, index: PoolIndex) -> &mut TcpSentSample {
        self.sent_samples
            .get_mut(index)
            .expect("tcp recovery sample index is valid")
    }

    fn first_sample_at_or_after(&self, sequence: TcpSeq) -> Option<PoolIndex> {
        if let Some(index) = self.sample_lookup.get(&sequence).copied() {
            return Some(index);
        }
        self.sample_lookup
            .successor(&sequence)
            .map(|(_, index)| *index)
    }

    fn next_sample_after(&self, sequence: TcpSeq) -> Option<PoolIndex> {
        self.sample_lookup
            .successor(&sequence)
            .map(|(_, index)| *index)
    }

    fn maybe_finish_recovery(&mut self, acknowledgment: TcpSeq) {
        if !self.recovery_active {
            return;
        }
        if acknowledgment >= self.recovery_end_sequence {
            self.recovery_active = false;
            self.recovery_window = 0;
            self.recovery_prev_window = 0;
            self.recovery_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = TcpSeq::from(0);
        }
    }

    fn take_sent(&mut self, index: PoolIndex) -> TcpSentSample {
        let sample = self.sent_sample(index);
        let mut pending_index = 0;
        while pending_index < self.rack_pending_loss.len() {
            if self.rack_pending_loss[pending_index] == index {
                let _ = self.rack_pending_loss.remove(pending_index);
                continue;
            }
            pending_index += 1;
        }
        let _ = self
            .sample_lookup
            .remove(&sample.sequence)
            .expect("tcp recovery sample lookup key should exist");
        if let Some(prev) = sample.prev {
            self.sent_sample_mut(prev).next = sample.next;
        } else {
            self.sample_head = sample.next;
        }
        if let Some(next) = sample.next {
            self.sent_sample_mut(next).prev = sample.prev;
        } else {
            self.sample_tail = sample.prev;
        }
        self.sent_samples
            .remove(index)
            .expect("tcp recovery sample index is valid")
    }
}

impl Clone for TcpRecoveryState {
    fn clone(&self) -> Self {
        let mut cloned = Self::new();
        cloned.next_packet_number = self.next_packet_number;
        cloned.recovery_active = self.recovery_active;
        cloned.recovery_window = self.recovery_window;
        cloned.recovery_prev_window = self.recovery_prev_window;
        cloned.recovery_delivered = self.recovery_delivered;
        cloned.recovery_retransmitted = self.recovery_retransmitted;
        cloned.recovery_new_data = self.recovery_new_data;
        cloned.recovery_end_sequence = self.recovery_end_sequence;

        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            cloned.record_sent(
                sample.packet_number,
                sample.sequence,
                sample.end_sequence,
                sample.bytes,
                sample.payload,
                sample.payload_offset,
                sample.payload_len,
                sample.sent_at,
            );
            cursor = sample.next;
        }

        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            let next = sample.next;
            if self
                .rack_pending_loss
                .iter()
                .any(|pending| *pending == index)
            {
                let cloned_index = cloned
                    .sample_lookup
                    .get(&sample.sequence)
                    .copied()
                    .expect("cloned sample lookup should contain copied sample");
                cloned.rack_pending_loss.push(cloned_index);
            }
            cursor = next;
        }
        cloned.rack_timer_armed = self.rack_timer_armed;
        cloned.tlp_timer_armed = self.tlp_timer_armed;
        cloned
    }
}

impl Default for TcpRecoveryState {
    fn default() -> Self {
        Self::new()
    }
}

fn deliver_acked_segment<C: CongestionController>(
    bytes_in_flight: u32,
    ack: TcpRecoveryAck,
    segment: TcpSentSample,
    congestion: &mut C,
) -> Option<Duration> {
    let latest_rtt = ack.now.saturating_duration_since(segment.sent_at);
    let rtt_sample = if segment.retransmitted {
        Duration::ZERO
    } else {
        latest_rtt
    };
    congestion.on_ack(
        ack.now,
        AckedPacket {
            packet_number: segment.packet_number,
            bytes: segment.bytes,
            sent_at: segment.sent_at,
            app_limited: ack.app_limited,
            ecn_ce_count: ack.ecn_ce_count,
        },
        RttSample {
            latest: rtt_sample,
            min: rtt_sample,
        },
        bytes_in_flight,
    );
    (!segment.retransmitted).then_some(latest_rtt)
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
        let _ = rtt_ms;
        TcpRecoveryAck {
            acknowledgment: TcpSeq::from(acknowledgment),
            now,
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
            TcpSeq::from(sequence),
            TcpSeq::from(end_sequence),
            bytes,
            None,
            0,
            0,
            sent_at,
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
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(56),
            TcpSeq::from(3_000),
            &mut controller,
        );

        assert_eq!(controller.lost.len(), 1);
        assert_eq!(controller.lost[0].packet_number, 1);
    }

    #[test]
    fn on_sack_blocks_acks_successor_sample_when_left_edge_has_no_exact_match() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_460);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);
        record_sent_for_test(
            &mut recovery,
            2,
            3_000,
            4_000,
            1_000,
            now + Duration::from_millis(1),
        );

        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_500),
                right_edge: TcpSeq::from(4_000),
            }],
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 1);
        assert_eq!(controller.acked[0].packet_number, 2);
        assert_eq!(recovery.bytes_in_flight(), 1_000);
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

        assert_eq!(probe.packet_number, 2);
    }

    #[test]
    fn next_tlp_probe_falls_back_to_previous_tail_after_sack_removes_newest_segment() {
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
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );

        let probe = recovery
            .take_tlp_probe()
            .expect("tlp probe should use remaining tail");

        assert_eq!(probe.packet_number, 1);
        assert_eq!(probe.sequence, TcpSeq::from(1_000));
    }

    #[test]
    fn recovery_send_space_allows_one_mss_when_recovery_starts() {
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
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );

        recovery.on_rack_timeout(
            now + Duration::from_millis(56),
            TcpSeq::from(3_000),
            &mut controller,
        );

        assert_eq!(
            recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000),
            Some(1_000)
        );
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
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(56),
            TcpSeq::from(6_000),
            &mut controller,
        );

        assert_eq!(
            recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000),
            Some(1_000)
        );

        recovery.on_retransmit_sent(1_000);

        assert_eq!(
            recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000),
            Some(0)
        );

        recovery.on_ack(
            ack(3_000, now + Duration::from_millis(90), 40),
            &mut controller,
        );

        assert_eq!(
            recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000),
            Some(0)
        );

        recovery.on_ack(
            ack(4_000, now + Duration::from_millis(120), 40),
            &mut controller,
        );

        assert_eq!(
            recovery.recovery_send_space(recovery.bytes_in_flight(), 1_000),
            Some(1_000)
        );
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
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(56),
            TcpSeq::from(4_000),
            &mut controller,
        );

        let first = recovery
            .take_rack_retransmit()
            .expect("first recovery retransmit");
        assert_eq!(first.sequence, TcpSeq::from(1_000));

        recovery.on_retransmit_sent(1_000);
        recovery.on_ack(
            ack(2_000, now + Duration::from_millis(90), 40),
            &mut controller,
        );

        let second = recovery
            .take_rack_retransmit()
            .expect("partial ack should schedule new head");
        assert_eq!(second.sequence, TcpSeq::from(3_000));
    }

    #[test]
    fn clone_preserves_distinct_pending_rack_candidates() {
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
                left_edge: TcpSeq::from(3_000),
                right_edge: TcpSeq::from(4_000),
            }],
            &mut controller,
        );

        let mut cloned = recovery.clone();
        let first = cloned
            .take_rack_retransmit()
            .expect("first pending retransmit");
        let second = cloned
            .take_rack_retransmit()
            .expect("second pending retransmit");

        assert_eq!(
            (first.sequence, second.sequence),
            (TcpSeq::from(2_000), TcpSeq::from(1_000))
        );
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
