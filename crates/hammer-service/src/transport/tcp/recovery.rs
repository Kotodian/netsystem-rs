use std::cmp::min;
use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::{TcpSackBlock, TcpSeq};
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::rbtree::RbTree;
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, PacketNumber, RttSample,
};

const TCP_MIN_TLP_TIMEOUT: Duration = Duration::from_millis(10);
const TCP_DUPACK_THRESHOLD: u32 = 3;

#[derive(Clone, Copy, Debug)]
pub(crate) struct TcpSentSample {
    pub(crate) packet_number: PacketNumber,
    pub(crate) sequence: TcpSeq,
    pub(crate) end_sequence: TcpSeq,
    pub(crate) bytes: u32,
    pub(crate) payload_len: u32,
    pub(crate) retransmitted: bool,
    pub(crate) rack_deadline: Option<Instant>,
    pub(crate) sent_at: Instant,
    pub(crate) prev: Option<PoolIndex>,
    pub(crate) next: Option<PoolIndex>,
}

impl TcpSentSample {
    #[inline]
    fn covers(self, sequence: TcpSeq) -> bool {
        self.sequence <= sequence && self.end_sequence > sequence
    }

    #[inline]
    fn overlaps(self, start: TcpSeq, end: TcpSeq) -> bool {
        self.end_sequence > start && self.sequence < end
    }

    fn split(self, at: TcpSeq) -> (TcpSentSample, TcpSentSample) {
        debug_assert!(at > self.sequence);
        debug_assert!(at < self.end_sequence);
        let left_bytes = self.sequence.distance_to(at);
        let left_payload_len = proportional_payload_len(self.bytes, self.payload_len, left_bytes);
        let right_bytes = self.bytes.saturating_sub(left_bytes);
        let right_payload_len = self.payload_len.saturating_sub(left_payload_len);
        (
            TcpSentSample {
                end_sequence: at,
                bytes: left_bytes,
                payload_len: left_payload_len,
                ..self
            },
            TcpSentSample {
                sequence: at,
                bytes: right_bytes,
                payload_len: right_payload_len,
                ..self
            },
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpRecoveryAck {
    pub acknowledgment: TcpSeq,
    pub now: Instant,
    pub app_limited: bool,
    pub ecn_ce_count: u64,
    pub reordering_window: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TcpScoreboardHole {
    end: TcpSeq,
    lost: bool,
}

#[derive(Debug)]
struct TcpScoreboard {
    holes: RbTree<TcpSeq, TcpScoreboardHole>,
    high_sacked: TcpSeq,
    high_rxt: TcpSeq,
    lost_bytes: u32,
    reorder: u32,
}

impl Clone for TcpScoreboard {
    fn clone(&self) -> Self {
        let mut holes = RbTree::with_capacity(self.holes.len().max(1));
        for (start, hole) in self.holes.iter() {
            let _ = holes.insert(*start, *hole);
        }
        Self {
            holes,
            high_sacked: self.high_sacked,
            high_rxt: self.high_rxt,
            lost_bytes: self.lost_bytes,
            reorder: self.reorder,
        }
    }
}

impl TcpScoreboard {
    #[inline]
    fn new() -> Self {
        Self {
            holes: RbTree::with_capacity(32),
            high_sacked: 0u32.into(),
            high_rxt: 0u32.into(),
            lost_bytes: 0,
            reorder: TCP_DUPACK_THRESHOLD,
        }
    }

    #[inline]
    fn clear(&mut self) {
        self.holes = RbTree::with_capacity(self.holes.len().max(1));
        self.high_sacked = 0u32.into();
        self.high_rxt = 0u32.into();
        self.lost_bytes = 0;
        self.reorder = TCP_DUPACK_THRESHOLD;
    }
}

#[derive(Debug)]
pub struct TcpRecoveryState {
    next_packet_number: PacketNumber,
    sent_samples: Pool<TcpSentSample>,
    sample_lookup: RbTree<TcpSeq, PoolIndex>,
    sample_head: Option<PoolIndex>,
    sample_tail: Option<PoolIndex>,
    bytes_in_flight: u32,
    ack_floor: TcpSeq,
    scoreboard: TcpScoreboard,
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
            bytes_in_flight: 0,
            ack_floor: 0u32.into(),
            scoreboard: TcpScoreboard::new(),
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
                payload_len,
                retransmitted: false,
                rack_deadline: None,
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
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(bytes);
        let replaced = self.sample_lookup.insert(sequence, sample_index);
        debug_assert!(
            replaced.is_none(),
            "tcp recovery sample lookup key should remain unique"
        );
        self.tlp_timer_armed = true;
    }

    pub fn bytes_in_flight(&self) -> u32 {
        self.bytes_in_flight
    }

    pub fn has_unacked_data(&self) -> bool {
        self.bytes_in_flight != 0
    }

    #[inline]
    pub fn in_recovery(&self) -> bool {
        self.recovery_active
    }

    pub fn rack_timeout(&self, now: Instant) -> Option<Duration> {
        let mut deadline = None;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if !self.sample_is_lost(sample)
                && let Some(sample_deadline) = sample.rack_deadline
            {
                deadline = Some(match deadline {
                    Some(current) => min(current, sample_deadline),
                    None => sample_deadline,
                });
            }
            cursor = sample.next;
        }
        deadline.map(|deadline| deadline.saturating_duration_since(now))
    }

    pub fn tlp_timeout(&self, srtt: Option<Duration>, rto: Duration) -> Option<Duration> {
        if !self.tlp_timer_armed || !self.has_unacked_data() {
            return None;
        }
        let srtt = srtt.unwrap_or(rto);
        let timeout = srtt.checked_mul(2).unwrap_or(rto).max(TCP_MIN_TLP_TIMEOUT);
        Some(timeout.min(rto))
    }

    pub fn on_ack<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        congestion: &mut C,
    ) -> Option<Duration> {
        self.ack_floor = ack.acknowledgment;
        let (acked, acked_bytes) = self.take_acked_segments(ack.acknowledgment);
        let advanced = !acked.is_empty();
        let latest_rtt = self.deliver_acked_segments(ack, acked, acked_bytes, congestion);
        self.rebuild_scoreboard(
            ack.acknowledgment,
            self.scoreboard.high_sacked,
            congestion.max_datagram_size(),
        );
        self.maybe_finish_recovery(ack.acknowledgment);
        if self.recovery_active && advanced {
            self.queue_recovery_head(ack.now, ack.reordering_window);
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
        self.ack_floor = ack.acknowledgment;
        let (mut acked, mut acked_bytes) = self.take_acked_segments(ack.acknowledgment);
        acked.reserve(blocks.len());
        let mut highest_sacked_right = ack.acknowledgment;
        for block in blocks {
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            let (sacked, sacked_bytes) = self.take_sacked_segments(*block);
            acked_bytes = acked_bytes.saturating_add(sacked_bytes);
            acked.extend(sacked);
        }
        let latest_rtt = self.deliver_acked_segments(ack, acked, acked_bytes, congestion);
        self.rebuild_scoreboard(
            ack.acknowledgment,
            highest_sacked_right,
            congestion.max_datagram_size(),
        );
        self.maybe_finish_recovery(ack.acknowledgment);
        if highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(highest_sacked_right, ack.now, ack.reordering_window);
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
        let mut recovery_started = false;
        let mut cursor = self.sample_head;
        while let Some(sample_index) = cursor {
            let sample = self.sent_sample(sample_index);
            cursor = sample.next;
            if self.sample_is_lost(sample)
                || sample.rack_deadline.is_none_or(|deadline| deadline > now)
            {
                continue;
            }
            congestion.on_loss(
                now,
                LostPacket {
                    packet_number: sample.packet_number,
                    bytes: sample.bytes,
                    sent_at: sample.sent_at,
                },
                false,
            );
            let current = self.sent_sample_mut(sample_index);
            current.rack_deadline = None;
            self.mark_hole_lost(sample.sequence);
            recovery_started |= !self.recovery_active;
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
        self.refresh_lost_bytes();
        self.rack_timer_armed = self.has_pending_rack_deadline();
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
        let (hole_start, hole_end) = self.next_lost_hole()?;
        let sample_index = self.sample_at_or_after(hole_start, true)?;
        let sample = self.sent_sample(sample_index);
        let start = hole_start.max(sample.sequence);
        let end = hole_end.min(sample.end_sequence);
        if end <= start {
            return None;
        }
        let bytes = start.distance_to(end);
        let payload_len = proportional_payload_len(sample.bytes, sample.payload_len, bytes);
        {
            let current = self.sent_sample_mut(sample_index);
            current.retransmitted = true;
            current.rack_deadline = None;
        }
        self.scoreboard.high_rxt = end;
        self.rack_timer_armed = self.has_pending_rack_deadline();
        Some(TcpSentSample {
            packet_number: sample.packet_number,
            sequence: start,
            end_sequence: end,
            bytes,
            payload_len,
            retransmitted: true,
            rack_deadline: None,
            sent_at: sample.sent_at,
            prev: sample.prev,
            next: sample.next,
        })
    }

    pub(crate) fn take_tlp_probe(&mut self) -> Option<TcpSentSample> {
        let index = self.sample_tail?;
        let sample = self.sent_samples.get_mut(index)?;
        sample.retransmitted = true;
        self.tlp_timer_armed = false;
        Some(*sample)
    }

    pub(crate) fn on_retransmission_timeout<C: CongestionController>(
        &mut self,
        now: Instant,
        snd_nxt: TcpSeq,
        congestion: &mut C,
    ) -> Option<TcpSentSample> {
        let head = self.sample_head?;
        let sample = self.sent_sample(head);
        let recovery_prev_window = congestion.congestion_window();
        congestion.on_loss(
            now,
            LostPacket {
                packet_number: sample.packet_number,
                bytes: sample.bytes,
                sent_at: sample.sent_at,
            },
            true,
        );
        if !self.recovery_active {
            self.recovery_active = true;
            self.recovery_prev_window = recovery_prev_window.max(1);
            self.recovery_window = congestion.congestion_window();
            self.recovery_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = snd_nxt;
        } else {
            self.recovery_window = self.recovery_window.min(congestion.congestion_window());
            if snd_nxt > self.recovery_end_sequence {
                self.recovery_end_sequence = snd_nxt;
            }
        }
        let current = self.sent_sample_mut(head);
        current.retransmitted = true;
        current.rack_deadline = None;
        self.rack_timer_armed = false;
        self.tlp_timer_armed = self.has_unacked_data();
        Some(sample)
    }

    #[cfg(test)]
    pub(crate) fn oldest_unacked_sample(&self) -> Option<TcpSentSample> {
        let index = self.sample_head?;
        Some(self.sent_sample(index))
    }

    fn mark_rack_candidates(
        &mut self,
        highest_sacked_right: TcpSeq,
        now: Instant,
        reordering_window: Duration,
    ) {
        let mut cursor = self.sample_head;
        let deadline = now + reordering_window;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if sample.end_sequence < highest_sacked_right
                && !self.sample_is_lost(sample)
                && sample.rack_deadline.is_none()
            {
                self.sent_sample_mut(index).rack_deadline = Some(deadline);
            }
            cursor = sample.next;
        }
        self.rack_timer_armed = self.has_pending_rack_deadline();
    }

    fn take_acked_segments(&mut self, acknowledgment: TcpSeq) -> (Vec<TcpSentSample>, u32) {
        let mut acked = Vec::with_capacity(4);
        let mut total_bytes = 0u32;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            let next = sample.next;
            if acknowledgment <= sample.sequence {
                break;
            }
            if acknowledgment >= sample.end_sequence {
                let sample = self.take_sent(index);
                total_bytes = total_bytes.saturating_add(sample.bytes);
                acked.push(sample);
            } else {
                let sample = self.take_sample_prefix(index, acknowledgment);
                total_bytes = total_bytes.saturating_add(sample.bytes);
                acked.push(sample);
                break;
            }
            cursor = next;
        }
        (acked, total_bytes)
    }

    fn take_sacked_segments(&mut self, block: TcpSackBlock) -> (Vec<TcpSentSample>, u32) {
        let mut matched = Vec::with_capacity(4);
        let mut total_bytes = 0u32;
        let left_edge = block.left_edge;
        let right_edge = block.right_edge;
        let mut cursor = self.sample_at_or_after(left_edge, true);
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if right_edge < sample.sequence {
                break;
            }
            cursor = self.next_sample(sample.sequence);
            if !sample.overlaps(left_edge, right_edge) {
                continue;
            }
            let ack_start = sample.sequence.max(left_edge);
            let ack_end = sample.end_sequence.min(right_edge);
            if ack_start > sample.sequence {
                self.split_sample(index, ack_start);
            }
            let Some(current_index) = self.sample_at_or_after(ack_start, false) else {
                continue;
            };
            let current = self.sent_sample(current_index);
            if ack_end < current.end_sequence {
                let sample = self.take_sample_prefix(current_index, ack_end);
                total_bytes = total_bytes.saturating_add(sample.bytes);
                matched.push(sample);
            } else {
                let sample = self.take_sent(current_index);
                total_bytes = total_bytes.saturating_add(sample.bytes);
                matched.push(sample);
            }
        }
        (matched, total_bytes)
    }

    fn deliver_acked_segments<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        acked: Vec<TcpSentSample>,
        total_acked_bytes: u32,
        congestion: &mut C,
    ) -> Option<Duration> {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut latest_rtt = None;
        let mut bytes_in_flight_after_ack = self.bytes_in_flight();
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
            latest_rtt = deliver_acked_segment(bytes_in_flight_after_ack, ack, segment, congestion)
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

    fn sample_at_or_after(&self, sequence: TcpSeq, include_covering: bool) -> Option<PoolIndex> {
        if let Some(index) = self.sample_lookup.get(&sequence).copied() {
            return Some(index);
        }
        let successor = self
            .sample_lookup
            .successor(&sequence)
            .map(|(_, index)| *index);
        if !include_covering {
            return successor;
        }
        if let Some((_, predecessor_index)) = self.sample_lookup.predecessor(&sequence) {
            let predecessor = self.sent_sample(*predecessor_index);
            if predecessor.covers(sequence) {
                return Some(*predecessor_index);
            }
        }
        successor
    }

    fn next_sample(&self, sequence: TcpSeq) -> Option<PoolIndex> {
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
        let sample = self
            .sent_samples
            .remove(index)
            .expect("tcp recovery sample index is valid");
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sample.bytes);
        sample
    }

    fn split_sample(&mut self, index: PoolIndex, split_start: TcpSeq) {
        let sample = self.sent_sample(index);
        let (prefix, suffix) = sample.split(split_start);

        {
            let current = self.sent_sample_mut(index);
            current.sequence = suffix.sequence;
            current.bytes = suffix.bytes;
            current.payload_len = suffix.payload_len;
            current.rack_deadline = sample.rack_deadline;
        }
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(prefix.bytes);
        let _ = self
            .sample_lookup
            .remove(&sample.sequence)
            .expect("tcp recovery sample lookup key should exist");
        let replaced = self.sample_lookup.insert(suffix.sequence, index);
        debug_assert!(replaced.is_none());

        let prefix_index = self.insert_sample_before(index, prefix);
        if sample.rack_deadline.is_some() && !self.sample_is_lost(sample) {
            self.sent_sample_mut(prefix_index).rack_deadline = sample.rack_deadline;
        }
    }

    fn take_sample_prefix(&mut self, index: PoolIndex, split_end: TcpSeq) -> TcpSentSample {
        let sample = self.sent_sample(index);
        let (prefix, suffix) = sample.split(split_end);

        {
            let current = self.sent_sample_mut(index);
            current.sequence = suffix.sequence;
            current.bytes = suffix.bytes;
            current.payload_len = suffix.payload_len;
            current.rack_deadline = sample.rack_deadline;
        }
        let _ = self
            .sample_lookup
            .remove(&sample.sequence)
            .expect("tcp recovery sample lookup key should exist");
        let replaced = self.sample_lookup.insert(suffix.sequence, index);
        debug_assert!(replaced.is_none());

        prefix
    }

    fn insert_sample_before(
        &mut self,
        next_index: PoolIndex,
        mut sample: TcpSentSample,
    ) -> PoolIndex {
        let next = self.sent_sample(next_index);
        sample.prev = next.prev;
        sample.next = Some(next_index);
        let sample_index = self
            .sent_samples
            .insert(sample)
            .expect("tcp recovery sample pool exhausted");
        if let Some(prev_index) = next.prev {
            self.sent_sample_mut(prev_index).next = Some(sample_index);
        } else {
            self.sample_head = Some(sample_index);
        }
        self.sent_sample_mut(next_index).prev = Some(sample_index);
        let replaced = self.sample_lookup.insert(sample.sequence, sample_index);
        debug_assert!(replaced.is_none());
        sample_index
    }

    fn rebuild_scoreboard(
        &mut self,
        acknowledgment: TcpSeq,
        high_sacked: TcpSeq,
        max_datagram_size: u32,
    ) {
        self.scoreboard.clear();
        self.scoreboard.high_sacked = high_sacked.max(acknowledgment);
        self.scoreboard.high_rxt = acknowledgment;
        if self.sample_head.is_none() || self.scoreboard.high_sacked <= acknowledgment {
            return;
        }

        let mut cursor = self.sample_head;
        let mut hole_start = acknowledgment;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            cursor = sample.next;
            if sample.end_sequence <= acknowledgment {
                continue;
            }
            if sample.sequence > hole_start {
                let start = hole_start;
                let end = sample.sequence.min(self.scoreboard.high_sacked);
                if end > start {
                    let _ = self
                        .scoreboard
                        .holes
                        .insert(start, TcpScoreboardHole { end, lost: false });
                }
            }
            hole_start = hole_start.max(sample.end_sequence);
            if hole_start >= self.scoreboard.high_sacked {
                break;
            }
        }

        if hole_start < self.scoreboard.high_sacked {
            let _ = self.scoreboard.holes.insert(
                hole_start,
                TcpScoreboardHole {
                    end: self.scoreboard.high_sacked,
                    lost: false,
                },
            );
        }
        self.update_scoreboard_loss(max_datagram_size.max(1));
    }

    fn update_scoreboard_loss(&mut self, max_datagram_size: u32) {
        self.scoreboard.lost_bytes = 0;
        let reorder_limit = self.scoreboard.reorder.max(TCP_DUPACK_THRESHOLD);
        let mut cursor = self.scoreboard.holes.first().map(|(start, _)| *start);
        while let Some(start) = cursor {
            cursor = self
                .scoreboard
                .holes
                .successor(&start)
                .map(|(next_start, _)| *next_start);
            let should_mark_lost =
                self.should_mark_hole_lost(start, max_datagram_size, reorder_limit);
            let Some(hole) = self.scoreboard.holes.get_mut(&start) else {
                continue;
            };
            if should_mark_lost {
                let hole_bytes = start.distance_to(hole.end);
                self.scoreboard.lost_bytes = self.scoreboard.lost_bytes.saturating_add(hole_bytes);
                hole.lost = true;
            } else {
                hole.lost = false;
            }
        }
    }

    fn should_mark_hole_lost(
        &self,
        start: TcpSeq,
        max_datagram_size: u32,
        reorder_limit: u32,
    ) -> bool {
        let Some(hole) = self.scoreboard.holes.get(&start).copied() else {
            return false;
        };
        let mut sacked = 0u32;
        let mut blocks = 0u32;
        let mut previous_end = hole.end;
        let mut cursor = self
            .scoreboard
            .holes
            .successor(&start)
            .map(|(next_start, _)| *next_start);
        while let Some(next_start) = cursor {
            sacked = sacked.saturating_add(previous_end.distance_to(next_start));
            blocks = blocks.saturating_add(1);
            if blocks >= reorder_limit
                || sacked
                    > reorder_limit
                        .saturating_sub(1)
                        .saturating_mul(max_datagram_size.max(1))
            {
                return true;
            }
            let Some(next_hole) = self.scoreboard.holes.get(&next_start).copied() else {
                break;
            };
            previous_end = next_hole.end;
            cursor = self
                .scoreboard
                .holes
                .successor(&next_start)
                .map(|(after_start, _)| *after_start);
        }
        false
    }

    fn queue_recovery_head(&mut self, _: Instant, _: Duration) {
        let Some(head) = self.sample_head else {
            return;
        };
        let sample = self.sent_sample(head);
        if sample.rack_deadline.is_some() || self.sample_is_lost(sample) {
            return;
        }
        let current = self.sent_sample_mut(head);
        current.rack_deadline = None;
        self.mark_hole_lost(sample.sequence);
        self.refresh_lost_bytes();
        self.rack_timer_armed = self.has_pending_rack_deadline();
    }

    fn next_lost_hole(&self) -> Option<(TcpSeq, TcpSeq)> {
        let mut cursor = self.scoreboard.high_rxt.max(self.ack_floor);
        if let Some((start, hole)) = self.hole_covering_or_after(cursor) {
            if hole.lost && start < self.scoreboard.high_sacked {
                let begin = cursor.max(start);
                if hole.end > begin {
                    return Some((begin, hole.end));
                }
            }
            if start > self.scoreboard.high_rxt {
                cursor = start;
            }
        }
        for (start, hole) in self.scoreboard.holes.iter() {
            if *start < cursor || !hole.lost || *start >= self.scoreboard.high_sacked {
                continue;
            }
            let begin = (*start).max(cursor);
            if hole.end > begin {
                return Some((begin, hole.end));
            }
        }
        None
    }

    fn hole_covering_or_after(&self, sequence: TcpSeq) -> Option<(TcpSeq, TcpScoreboardHole)> {
        if let Some((start, hole)) = self.scoreboard.holes.predecessor(&sequence)
            && hole.end > sequence
        {
            return Some((*start, *hole));
        }
        if let Some(hole) = self.scoreboard.holes.get(&sequence).copied() {
            return Some((sequence, hole));
        }
        self.scoreboard
            .holes
            .successor(&sequence)
            .map(|(start, hole)| (*start, *hole))
    }

    fn mark_hole_lost(&mut self, sequence: TcpSeq) {
        let Some((start, hole)) = self.hole_covering_or_after(sequence) else {
            return;
        };
        if hole.end <= sequence {
            return;
        }
        if let Some(current) = self.scoreboard.holes.get_mut(&start) {
            current.lost = true;
        }
    }

    fn refresh_lost_bytes(&mut self) {
        self.scoreboard.lost_bytes = 0;
        for (start, hole) in self.scoreboard.holes.iter() {
            if hole.lost {
                self.scoreboard.lost_bytes = self
                    .scoreboard
                    .lost_bytes
                    .saturating_add(start.distance_to(hole.end));
            }
        }
    }

    fn sample_is_lost(&self, sample: TcpSentSample) -> bool {
        self.hole_covering_or_after(sample.sequence)
            .is_some_and(|(start, hole)| {
                hole.lost && start <= sample.sequence && hole.end > sample.sequence
            })
    }

    fn has_pending_rack_deadline(&self) -> bool {
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if !self.sample_is_lost(sample) && sample.rack_deadline.is_some() {
                return true;
            }
            cursor = sample.next;
        }
        false
    }
}

impl Clone for TcpRecoveryState {
    fn clone(&self) -> Self {
        let mut cloned = Self::new();
        cloned.next_packet_number = self.next_packet_number;
        cloned.ack_floor = self.ack_floor;
        cloned.scoreboard = self.scoreboard.clone();
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
                sample.payload_len,
                sample.sent_at,
            );
            cursor = sample.next;
        }

        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            let next = sample.next;
            let cloned_index = cloned
                .sample_lookup
                .get(&sample.sequence)
                .copied()
                .expect("cloned sample lookup should contain copied sample");
            {
                let cloned_sample = cloned.sent_sample_mut(cloned_index);
                cloned_sample.retransmitted = sample.retransmitted;
                cloned_sample.rack_deadline = sample.rack_deadline;
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

#[inline]
fn proportional_payload_len(bytes: u32, payload_len: u32, portion_bytes: u32) -> u32 {
    if bytes == 0 || payload_len == 0 || portion_bytes == 0 {
        return 0;
    }
    let payload = (u64::from(payload_len) * u64::from(portion_bytes)) / u64::from(bytes);
    payload.min(u64::from(payload_len)) as u32
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
        let reordering_window =
            Duration::from_millis(rtt_ms.max(1) / 4).max(Duration::from_millis(1));
        TcpRecoveryAck {
            acknowledgment: TcpSeq::from(acknowledgment),
            now,
            app_limited: false,
            ecn_ce_count: 0,
            reordering_window,
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
    fn rack_waits_for_reordering_window_before_marking_loss() {
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
            ack(1_000, now + Duration::from_millis(30), 80),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(45),
            TcpSeq::from(3_000),
            &mut controller,
        );
        assert!(controller.lost.is_empty());

        recovery.on_rack_timeout(
            now + Duration::from_millis(55),
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
    fn on_ack_splits_sample_when_ack_covers_partial_range() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_460);
        record_sent_for_test(&mut recovery, 1, 1_000, 2_000, 1_000, now);

        recovery.on_ack(
            ack(1_500, now + Duration::from_millis(25), 25),
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 1);
        assert_eq!(controller.acked[0].packet_number, 1);
        assert_eq!(controller.acked[0].bytes, 500);
        let remaining = recovery.oldest_unacked_sample().expect("remaining sample");
        assert_eq!(remaining.sequence, TcpSeq::from(1_500));
        assert_eq!(remaining.end_sequence, TcpSeq::from(2_000));
        assert_eq!(remaining.bytes, 500);
        assert_eq!(recovery.bytes_in_flight(), 500);
    }

    #[test]
    fn on_sack_blocks_split_sample_for_partial_sack_range() {
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
            ack(1_000, now + Duration::from_millis(30), 30),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_400),
                right_edge: TcpSeq::from(2_800),
            }],
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 1);
        assert_eq!(controller.acked[0].packet_number, 2);
        assert_eq!(controller.acked[0].bytes, 400);

        let head = recovery.oldest_unacked_sample().expect("head");
        assert_eq!(head.sequence, TcpSeq::from(1_000));
        assert_eq!(head.end_sequence, TcpSeq::from(2_000));

        let middle = recovery
            .sample_at_or_after(TcpSeq::from(2_000), false)
            .map(|index| recovery.sent_sample(index))
            .expect("middle sample");
        assert_eq!(middle.sequence, TcpSeq::from(2_000));
        assert_eq!(middle.end_sequence, TcpSeq::from(2_400));

        let tail = recovery
            .sample_at_or_after(TcpSeq::from(2_800), false)
            .map(|index| recovery.sent_sample(index))
            .expect("tail sample");
        assert_eq!(tail.sequence, TcpSeq::from(2_800));
        assert_eq!(tail.end_sequence, TcpSeq::from(3_000));
        assert_eq!(recovery.bytes_in_flight(), 1_600);
    }

    #[test]
    fn repeated_sack_blocks_do_not_requeue_same_rack_loss_candidate() {
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

        let block = TcpSackBlock {
            left_edge: TcpSeq::from(3_000),
            right_edge: TcpSeq::from(4_000),
        };
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 30),
            &[block],
            &mut controller,
        );
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(35), 35),
            &[block],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(60),
            TcpSeq::from(4_000),
            &mut controller,
        );

        assert_eq!(controller.lost.len(), 2);
        assert_eq!(controller.lost[0].packet_number, 1);
        assert_eq!(controller.lost[1].packet_number, 2);
    }

    #[test]
    fn overlapping_sack_blocks_do_not_double_ack_same_sample_bytes() {
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
            ack(1_000, now + Duration::from_millis(30), 30),
            &[
                TcpSackBlock {
                    left_edge: TcpSeq::from(2_200),
                    right_edge: TcpSeq::from(2_800),
                },
                TcpSackBlock {
                    left_edge: TcpSeq::from(2_400),
                    right_edge: TcpSeq::from(3_000),
                },
            ],
            &mut controller,
        );

        assert_eq!(controller.acked.len(), 2);
        assert_eq!(controller.acked[0].bytes, 600);
        assert_eq!(controller.acked[1].bytes, 200);
        assert_eq!(recovery.bytes_in_flight(), 1_200);
    }

    #[test]
    fn sack_ack_of_middle_range_only_marks_lower_unsacked_samples_lost() {
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
            ack(1_000, now + Duration::from_millis(30), 30),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(60),
            TcpSeq::from(4_000),
            &mut controller,
        );

        assert_eq!(controller.lost.len(), 1);
        assert_eq!(controller.lost[0].packet_number, 1);
        let remaining = recovery.oldest_unacked_sample().expect("remaining head");
        assert_eq!(remaining.packet_number, 1);
        let next = recovery
            .sample_at_or_after(TcpSeq::from(3_000), false)
            .map(|index| recovery.sent_sample(index))
            .expect("higher unsacked sample remains outstanding");
        assert_eq!(next.packet_number, 3);
    }

    #[test]
    fn partial_ack_in_recovery_requeues_only_new_unsacked_head_once() {
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
            ack(1_000, now + Duration::from_millis(30), 30),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(3_000),
                right_edge: TcpSeq::from(4_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(60),
            TcpSeq::from(4_000),
            &mut controller,
        );

        let first = recovery.take_rack_retransmit().expect("first retransmit");
        assert_eq!(first.packet_number, 2);

        recovery.on_ack(
            ack(2_500, now + Duration::from_millis(90), 90),
            &mut controller,
        );
        let second = recovery
            .take_rack_retransmit()
            .expect("new unsacked head after partial ack");
        assert_eq!(second.sequence, TcpSeq::from(2_500));
        assert!(recovery.take_rack_retransmit().is_none());
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
        cloned.on_rack_timeout(
            now + Duration::from_millis(60),
            TcpSeq::from(4_000),
            &mut controller,
        );
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
