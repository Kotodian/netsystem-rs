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

/// Inline capacity for per-ACK scoreboard key collection. SACK gap counts are
/// bounded by the number of SACK blocks a receiver reports (RFC 2018 caps a
/// single SACK option at 4 blocks, and pathological reordering rarely exceeds
/// 8 distinct gaps). When this capacity is exceeded the collector falls back to
/// a heap `Vec` via the `#[cold]` overflow path, preserving correctness.
const SCOREBOARD_KEY_INLINE_CAP: usize = 8;

/// Small-stack collector for `TcpSeq` keys used on the ACK hot path, replacing
/// the per-ACK `Vec<TcpSeq>` allocations in `advance_scoreboard_for_ack` and
/// `update_scoreboard_loss`. Holds up to `SCOREBOARD_KEY_INLINE_CAP` entries in
/// a stack array; overflowing callers drain what fits and continue with a
/// `#[cold]` heap fallback for the remainder, so behavior matches the old Vec
/// exactly while keeping the common case allocation-free.
struct ScoreboardKeyCollector {
    inline: [TcpSeq; SCOREBOARD_KEY_INLINE_CAP],
    len: usize,
    overflow: Option<Vec<TcpSeq>>,
}

impl ScoreboardKeyCollector {
    #[inline]
    fn new() -> Self {
        Self {
            inline: [TcpSeq::from(0); SCOREBOARD_KEY_INLINE_CAP],
            len: 0,
            overflow: None,
        }
    }

    #[inline]
    fn push(&mut self, key: TcpSeq) {
        if let Some(buf) = &mut self.overflow {
            buf.push(key);
            return;
        }
        if self.len < SCOREBOARD_KEY_INLINE_CAP {
            self.inline[self.len] = key;
            self.len += 1;
        } else {
            // Spill: move inline contents to a heap Vec and continue there.
            // #[cold] attribution is on `spill_to_overflow`.
            self.spill_to_overflow(key);
        }
    }

    #[cold]
    fn spill_to_overflow(&mut self, key: TcpSeq) {
        let mut buf = Vec::with_capacity(self.len.saturating_add(1).max(SCOREBOARD_KEY_INLINE_CAP));
        for i in 0..self.len {
            buf.push(self.inline[i]);
        }
        buf.push(key);
        // Inline contents have been moved into the heap Vec; reset the inline
        // view so drain operations do not visit them twice.
        self.len = 0;
        self.overflow = Some(buf);
    }

    #[inline]
    fn pop_front(&mut self) -> Option<TcpSeq> {
        if self.len != 0 {
            let key = self.inline[0];
            let mut index = 1usize;
            while index < self.len {
                self.inline[index - 1] = self.inline[index];
                index += 1;
            }
            self.len -= 1;
            return Some(key);
        }
        let buf = self.overflow.as_mut()?;
        if buf.is_empty() {
            self.overflow = None;
            return None;
        }
        Some(buf.remove(0))
    }

    #[inline]
    fn pop_back(&mut self) -> Option<TcpSeq> {
        if let Some(buf) = self.overflow.as_mut() {
            if let Some(key) = buf.pop() {
                if buf.is_empty() {
                    self.overflow = None;
                }
                return Some(key);
            }
            self.overflow = None;
        }
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(self.inline[self.len])
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TcpSentSample {
    pub(crate) packet_number: PacketNumber,
    pub(crate) sequence: TcpSeq,
    pub(crate) end_sequence: TcpSeq,
    pub(crate) bytes: u32,
    pub(crate) payload_len: u32,
    pub(crate) retransmitted: bool,
    pub(crate) lost: bool,
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
    #[cfg(test)]
    clears: u64,
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
            #[cfg(test)]
            clears: self.clears,
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
            #[cfg(test)]
            clears: 0,
        }
    }

    #[inline]
    fn clear(&mut self) {
        // Preserve a minimum capacity so a subsequent rebuild with more holes
        // than the previous one does not exhaust the fixed-size node pool.
        let capacity = self.holes.len().max(32);
        self.holes = RbTree::with_capacity(capacity);
        self.high_sacked = 0u32.into();
        self.high_rxt = 0u32.into();
        self.lost_bytes = 0;
        self.reorder = TCP_DUPACK_THRESHOLD;
        #[cfg(test)]
        {
            self.clears = self.clears.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RackDeadlineIndex {
    earliest: Option<Instant>,
}

/// Test-only snapshot of the scoreboard used by the incremental-vs-full-rebuild
/// oracle test.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ScoreboardSnapshot {
    pub holes: std::vec::Vec<(u32, u32, bool)>,
    pub high_sacked: u32,
    pub high_rxt: u32,
    pub lost_bytes: u32,
    pub clears: u64,
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
    rack_index: RackDeadlineIndex,
    rack_timer_armed: bool,
    tlp_timer_armed: bool,
    recovery_active: bool,
    recovery_window: u32,
    recovery_prev_window: u32,
    recovery_delivered: u32,
    recovery_retransmitted: u32,
    recovery_new_data: u32,
    recovery_end_sequence: TcpSeq,
    /// Test-only: when true, `on_ack` uses the full `rebuild_scoreboard`
    /// (oracle path) instead of the incremental `advance_scoreboard_for_ack`.
    #[cfg(test)]
    force_full_rebuild_on_ack: bool,
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
            rack_index: RackDeadlineIndex::default(),
            rack_timer_armed: false,
            tlp_timer_armed: false,
            recovery_active: false,
            recovery_window: 0,
            recovery_prev_window: 0,
            recovery_delivered: 0,
            recovery_retransmitted: 0,
            recovery_new_data: 0,
            recovery_end_sequence: TcpSeq::from(0),
            #[cfg(test)]
            force_full_rebuild_on_ack: false,
        }
    }

    /// Test-only: force `on_ack` to use the full `rebuild_scoreboard` oracle
    /// path instead of the incremental ACK path.
    #[cfg(test)]
    pub(crate) fn set_full_rebuild_ack_for_test(&mut self, on: bool) {
        self.force_full_rebuild_on_ack = on;
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
                lost: false,
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
        self.rack_index
            .earliest
            .map(|deadline| deadline.saturating_duration_since(now))
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
        let (advanced, latest_rtt) = self.process_ack(ack, congestion);
        #[cfg(test)]
        if self.force_full_rebuild_on_ack {
            self.rebuild_scoreboard(
                ack.acknowledgment,
                self.scoreboard.high_sacked,
                congestion.max_datagram_size(),
            );
        } else {
            self.advance_scoreboard_for_ack(ack.acknowledgment, congestion.max_datagram_size());
        }
        #[cfg(not(test))]
        self.advance_scoreboard_for_ack(ack.acknowledgment, congestion.max_datagram_size());
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
            current.lost = true;
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
        self.rack_rescan_earliest();
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

    /// Retransmit the lowest lost, not-yet-retransmitted sample.
    ///
    /// Walks `sample_head` ascending (samples are linked in increasing sequence
    /// order) and retransmits the first sample with `lost && !retransmitted`.
    /// Ascending order matches VPP `scoreboard_next_rxt_hole` (walks forward from
    /// the first hole) and RFC 6675 §4.3 ("retransmit the segment starting with
    /// HighACK + 1"). Per-sample `lost` is the authoritative RACK-loss signal and
    /// is also set by `update_scoreboard_loss` for SACK-gap-driven loss, giving a
    /// single unified retransmit path.
    pub(crate) fn take_rack_retransmit(&mut self) -> Option<TcpSentSample> {
        let mut cursor = self.sample_head;
        while let Some(sample_index) = cursor {
            let sample = self.sent_sample(sample_index);
            cursor = sample.next;
            if !sample.lost || sample.retransmitted {
                continue;
            }
            let start = sample.sequence;
            let end = sample.end_sequence;
            let bytes = sample.bytes;
            let payload_len = sample.payload_len;
            let cleared = sample.rack_deadline;
            {
                let current = self.sent_sample_mut(sample_index);
                current.retransmitted = true;
                current.rack_deadline = None;
            }
            self.rack_invalidate_cleared(cleared);
            self.scoreboard.high_rxt = end;
            self.rack_timer_armed = self.has_pending_rack_deadline();
            return Some(TcpSentSample {
                packet_number: sample.packet_number,
                sequence: start,
                end_sequence: end,
                bytes,
                payload_len,
                retransmitted: true,
                lost: false,
                rack_deadline: None,
                sent_at: sample.sent_at,
                prev: sample.prev,
                next: sample.next,
            });
        }
        None
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
        let cleared = current.rack_deadline;
        current.rack_deadline = None;
        self.rack_invalidate_cleared(cleared);
        self.rack_timer_armed = false;
        self.tlp_timer_armed = self.has_unacked_data();
        Some(sample)
    }

    #[cfg(test)]
    pub(crate) fn oldest_unacked_sample(&self) -> Option<TcpSentSample> {
        let index = self.sample_head?;
        Some(self.sent_sample(index))
    }

    /// Test accessor: snapshot of the scoreboard holes (start, end, lost) in
    /// ascending order, plus `high_sacked`, `high_rxt` and `lost_bytes`.
    #[cfg(test)]
    pub(crate) fn scoreboard_snapshot(&self) -> ScoreboardSnapshot {
        let mut holes: std::vec::Vec<(u32, u32, bool)> = std::vec::Vec::new();
        for (start, hole) in self.scoreboard.holes.iter() {
            holes.push((u32::from(*start), u32::from(hole.end), hole.lost));
        }
        ScoreboardSnapshot {
            holes,
            high_sacked: u32::from(self.scoreboard.high_sacked),
            high_rxt: u32::from(self.scoreboard.high_rxt),
            lost_bytes: self.scoreboard.lost_bytes,
            clears: self.scoreboard.clears,
        }
    }

    /// Test accessor: every outstanding sample as (sequence, end, bytes, lost,
    /// retransmitted) in ascending sequence order.
    #[cfg(test)]
    pub(crate) fn sample_snapshot(&self) -> std::vec::Vec<(u32, u32, u32, bool, bool)> {
        let mut out = std::vec::Vec::new();
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let s = self.sent_sample(index);
            out.push((
                u32::from(s.sequence),
                u32::from(s.end_sequence),
                s.bytes,
                s.lost,
                s.retransmitted,
            ));
            cursor = s.next;
        }
        out
    }

    fn mark_rack_candidates(
        &mut self,
        highest_sacked_right: TcpSeq,
        now: Instant,
        reordering_window: Duration,
    ) {
        let mut cursor = self.sample_head;
        let deadline = now + reordering_window;
        let mut any_marked = false;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if sample.end_sequence < highest_sacked_right
                && !self.sample_is_lost(sample)
                && sample.rack_deadline.is_none()
            {
                self.sent_sample_mut(index).rack_deadline = Some(deadline);
                any_marked = true;
            }
            cursor = sample.next;
        }
        if any_marked {
            self.rack_note_deadline(deadline);
        }
        self.rack_timer_armed = self.has_pending_rack_deadline();
    }

    /// Fused ACK-path replacement for `take_acked_segments` + `deliver_acked_segments`.
    ///
    /// Walks `sample_head` once, taking each acked segment (full or partial
    /// prefix) and feeding it straight to `deliver_acked_segment` inline, so no
    /// intermediate `Vec<TcpSentSample>` is allocated on the ACK hot path.
    ///
    /// Equivalence with the pre-fuse path: `take_sent`/`take_sample_prefix`
    /// decrement `bytes_in_flight` by `segment.bytes` before the inline
    /// `deliver_acked_segment` call, so `self.bytes_in_flight()` read after the
    /// take equals exactly `bif_before_ack - sum(seg[0..=i].bytes)`, which is the
    /// `bytes_in_flight_after_ack` value the pre-fuse `deliver_acked_segments`
    /// computed via its rollback (`bif_after_all + total - sum`). `recovery_delivered`
    /// is credited by the accumulated acked bytes once at the end, matching the
    /// pre-fuse one-shot `+= total_acked_bytes`. `on_end_acks` fires exactly once
    /// when any segment was acked, matching the pre-fuse `any_acked` guard.
    ///
    /// Returns `(advanced, latest_rtt)` where `advanced` is true iff any segment
    /// was acked (preserving the pre-fuse `!acked.is_empty()` signal) and
    /// `latest_rtt` is the most recent non-retransmitted RTT sample.
    fn process_ack<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        congestion: &mut C,
    ) -> (bool, Option<Duration>) {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut latest_rtt = None;
        let mut total_acked_bytes = 0u32;
        let mut cursor = self.sample_head;
        let mut done = false;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            let next = sample.next;
            if ack.acknowledgment <= sample.sequence {
                break;
            }
            let segment = if ack.acknowledgment >= sample.end_sequence {
                let taken = self.take_sent(index);
                total_acked_bytes = total_acked_bytes.saturating_add(taken.bytes);
                taken
            } else {
                // Partial prefix: the remaining suffix stays outstanding at
                // `sequence == acknowledgment`, so no later sample can be acked
                // by this cumulative ACK either. Match `take_acked_segments`
                // which breaks after taking the prefix.
                let prefix = self.take_sample_prefix(index, ack.acknowledgment);
                total_acked_bytes = total_acked_bytes.saturating_add(prefix.bytes);
                done = true;
                prefix
            };
            largest_acked = largest_acked.max(segment.packet_number);
            any_acked = true;
            let bytes_in_flight_after_ack = self.bytes_in_flight();
            latest_rtt = deliver_acked_segment(bytes_in_flight_after_ack, ack, segment, congestion)
                .or(latest_rtt);
            if done {
                break;
            }
            cursor = next;
        }
        if self.recovery_active && total_acked_bytes != 0 {
            self.recovery_delivered = self.recovery_delivered.saturating_add(total_acked_bytes);
        }
        if any_acked {
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        (any_acked, latest_rtt)
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
        let cleared = sample.rack_deadline;
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
        self.rack_invalidate_cleared(cleared);
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
        let _ = self
            .sample_lookup
            .remove(&sample.sequence)
            .expect("tcp recovery sample lookup key should exist");
        let replaced = self.sample_lookup.insert(suffix.sequence, index);
        debug_assert!(replaced.is_none());

        let prefix_index = self.insert_sample_before(index, prefix);
        if !self.sample_is_lost(sample)
            && let Some(deadline) = sample.rack_deadline
        {
            self.sent_sample_mut(prefix_index).rack_deadline = Some(deadline);
            self.rack_note_deadline(deadline);
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

        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(prefix.bytes);
        self.rack_invalidate_cleared(sample.rack_deadline);
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
            // No SACK-gap holes remain, but RACK-lost samples above high_sacked
            // still count toward lost_bytes — recompute from per-sample flags
            // instead of leaving the zero from clear().
            self.refresh_lost_bytes();
            self.rack_rescan_earliest();
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
        self.rack_rescan_earliest();
    }

    /// Incremental scoreboard update for an ACK advancing `snd_una`.
    ///
    /// Unlike `rebuild_scoreboard`, this does NOT `holes.clear()` and rebuild
    /// from the sample list. SACK-gap holes above `snd_una` are unchanged; only
    /// holes that fell below the new cumulative ACK are removed or trimmed. This
    /// matches the full rebuild because between two SACK ops the only sample
    /// mutations an ACK causes are removals/shrinks at or below `snd_una`, which
    /// never create holes above `snd_una` — so trimming at `acknowledgment` is
    /// equivalent to a full rebuild. SACK ops still call `rebuild_scoreboard`
    /// (which is where sacked-sample removal creates/merges holes).
    fn advance_scoreboard_for_ack(&mut self, acknowledgment: TcpSeq, max_datagram_size: u32) {
        self.scoreboard.high_sacked = self.scoreboard.high_sacked.max(acknowledgment);
        // Match rebuild_scoreboard, which resets high_rxt to the cumulative ACK
        // on every scoreboard update (take_rack_retransmit raises it again when
        // it retransmits).
        self.scoreboard.high_rxt = acknowledgment;
        if self.sample_head.is_none() || self.scoreboard.high_sacked <= acknowledgment {
            // No outstanding samples or everything up to high_sacked is now
            // acknowledged: no SACK-gap holes remain. Drop holes without a full
            // clear() of unrelated scoreboard state, then recompute lost_bytes
            // from per-sample flags (RACK-lost samples above high_sacked still
            // count).
            let mut keys = ScoreboardKeyCollector::new();
            for (start, _) in self.scoreboard.holes.iter() {
                keys.push(*start);
            }
            while let Some(start) = keys.pop_front() {
                self.scoreboard.holes.remove(&start);
            }
            self.refresh_lost_bytes();
            self.rack_rescan_earliest();
            return;
        }

        // Remove holes fully below the new ACK and trim the one it crosses.
        // Collect first to avoid mutating while iterating.
        let mut to_remove = ScoreboardKeyCollector::new();
        let mut to_trim: Option<(TcpSeq, TcpSeq, bool)> = None;
        for (start, hole) in self.scoreboard.holes.iter() {
            if hole.end <= acknowledgment {
                to_remove.push(*start);
            } else if *start < acknowledgment {
                to_trim = Some((*start, hole.end, hole.lost));
            }
        }
        while let Some(start) = to_remove.pop_front() {
            self.scoreboard.holes.remove(&start);
        }
        if let Some((old_start, end, lost)) = to_trim {
            self.scoreboard.holes.remove(&old_start);
            let _ = self
                .scoreboard
                .holes
                .insert(acknowledgment, TcpScoreboardHole { end, lost });
        }

        // Ensure the leading SACK-gap hole exists. Full rebuild creates a hole
        // `[ack, min(first_sample.seq, high_sacked))` whenever the lowest
        // outstanding sample starts above snd_una. The trim above only shrinks
        // existing holes; it cannot create this leading hole when the previous
        // scoreboard had none (e.g. the last SACK ran while no samples were
        // outstanding and early-returned, then a new sample was recorded above
        // high_sacked). Recreate it from the first sample to match full rebuild.
        if let Some(head) = self.sample_head {
            let first = self.sent_sample(head);
            if first.sequence > acknowledgment && acknowledgment < self.scoreboard.high_sacked {
                let leading_end = first.sequence.min(self.scoreboard.high_sacked);
                match self.scoreboard.holes.get_mut(&acknowledgment) {
                    Some(hole) => {
                        if hole.end < leading_end {
                            hole.end = leading_end;
                        }
                    }
                    None => {
                        let _ = self.scoreboard.holes.insert(
                            acknowledgment,
                            TcpScoreboardHole {
                                end: leading_end,
                                lost: false,
                            },
                        );
                    }
                }
            }
        }

        self.update_scoreboard_loss(max_datagram_size.max(1));
        self.rack_rescan_earliest();
    }
    ///
    /// Replaces the original O(holes^2) `should_mark_hole_lost` (which rescanned
    /// all successor holes per hole). A hole is declared lost when enough sacked
    /// bytes or sacked blocks accumulate in the holes ABOVE it. Walking holes
    /// descending and accumulating `sacked_ahead` / `blocks_ahead` computes the
    /// same decision for every hole in one pass.
    ///
    /// `sacked_ahead(hi)` = sum of gaps between consecutive holes from hi upward
    /// (= `sum_{k>i} hk.start - h(k-1).end`); `blocks_ahead(hi)` = number of
    /// holes above hi. These match the original per-hole successor scan exactly.
    fn update_scoreboard_loss(&mut self, max_datagram_size: u32) {
        let reorder_limit = self.scoreboard.reorder.max(TCP_DUPACK_THRESHOLD);
        let mss = max_datagram_size.max(1);
        let byte_threshold = reorder_limit.saturating_sub(1).saturating_mul(mss);

        // Collect holes ascending, then decide descending so each hole sees the
        // sacked bytes/blocks accumulated above it.
        let mut hole_starts = ScoreboardKeyCollector::new();
        let mut cursor = self.scoreboard.holes.first().map(|(start, _)| *start);
        while let Some(start) = cursor {
            hole_starts.push(start);
            cursor = self
                .scoreboard
                .holes
                .successor(&start)
                .map(|(next_start, _)| *next_start);
        }

        let mut sacked_ahead: u32 = 0;
        let mut blocks_ahead: u32 = 0;
        let mut higher_start: Option<TcpSeq> = None;
        while let Some(start) = hole_starts.pop_back() {
            let Some(hole) = self.scoreboard.holes.get(&start).copied() else {
                continue;
            };
            let should_mark_lost = blocks_ahead >= reorder_limit || sacked_ahead > byte_threshold;
            let hole_end = hole.end;
            // Apply the decision without holding the borrow across sample mutation.
            if let Some(h) = self.scoreboard.holes.get_mut(&start) {
                h.lost = should_mark_lost;
            }
            if should_mark_lost {
                // Unify: a SACK-gap hole declared lost also marks the samples it
                // covers as lost so the single per-sample retransmit walk in
                // take_rack_retransmit reaches them. RACK-driven loss sets
                // sample.lost directly in on_rack_timeout / queue_recovery_head.
                self.mark_samples_in_range_lost(start, hole_end);
            }
            // Accumulate the gap between this hole and the next-higher hole for
            // the lower holes still to be decided.
            if let Some(high_start) = higher_start {
                sacked_ahead = sacked_ahead.saturating_add(hole_end.distance_to(high_start));
                blocks_ahead = blocks_ahead.saturating_add(1);
            }
            higher_start = Some(start);
        }
        self.refresh_lost_bytes();
    }

    fn mark_samples_in_range_lost(&mut self, range_start: TcpSeq, range_end: TcpSeq) {
        if range_end <= range_start {
            return;
        }
        let mut cursor = self.sample_at_or_after(range_start, true);
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if sample.sequence >= range_end {
                break;
            }
            cursor = sample.next;
            if sample.end_sequence > range_start && sample.sequence < range_end {
                self.sent_sample_mut(index).lost = true;
            }
        }
    }

    fn queue_recovery_head(&mut self, _: Instant, _: Duration) {
        let Some(head) = self.sample_head else {
            return;
        };
        let sample = self.sent_sample(head);
        if sample.rack_deadline.is_some() || self.sample_is_lost(sample) {
            return;
        }
        {
            let current = self.sent_sample_mut(head);
            current.rack_deadline = None;
            current.lost = true;
        }
        self.refresh_lost_bytes();
        self.rack_rescan_earliest();
        self.rack_timer_armed = self.has_pending_rack_deadline();
    }

    fn refresh_lost_bytes(&mut self) {
        let mut lost_bytes = 0u32;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if sample.lost {
                lost_bytes = lost_bytes.saturating_add(sample.bytes);
            }
            cursor = sample.next;
        }
        self.scoreboard.lost_bytes = lost_bytes;
    }

    fn sample_is_lost(&self, sample: TcpSentSample) -> bool {
        sample.lost
    }

    fn has_pending_rack_deadline(&self) -> bool {
        self.rack_index.earliest.is_some()
    }

    #[inline]
    fn rack_note_deadline(&mut self, deadline: Instant) {
        self.rack_index.earliest = Some(match self.rack_index.earliest {
            None => deadline,
            Some(current) => current.min(deadline),
        });
    }

    fn rack_invalidate_cleared(&mut self, cleared: Option<Instant>) {
        let Some(cleared) = cleared else {
            return;
        };
        if self.rack_index.earliest != Some(cleared) {
            return;
        }
        self.rack_rescan_earliest();
    }

    #[cold]
    fn rack_rescan_earliest(&mut self) {
        let mut earliest = None;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if !self.sample_is_lost(sample)
                && let Some(deadline) = sample.rack_deadline
                && earliest.is_none_or(|current| deadline < current)
            {
                earliest = Some(deadline);
            }
            cursor = sample.next;
        }
        self.rack_index.earliest = earliest;
    }

    #[cfg(test)]
    pub(crate) fn rack_earliest_full_scan(&self) -> Option<Instant> {
        let mut earliest = None;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index);
            if !self.sample_is_lost(sample)
                && let Some(deadline) = sample.rack_deadline
                && earliest.is_none_or(|current| deadline < current)
            {
                earliest = Some(deadline);
            }
            cursor = sample.next;
        }
        earliest
    }

    /// Test accessor: total bytes credited to the PRR `recovery_delivered`
    /// counter since recovery started. Used by the fuse-equivalence test to
    /// verify the inlined ACK path accumulates the same delivered bytes as the
    /// pre-fuse `deliver_acked_segments` path.
    #[cfg(test)]
    pub(crate) fn recovery_delivered_for_test(&self) -> u32 {
        self.recovery_delivered
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
        #[cfg(test)]
        {
            cloned.force_full_rebuild_on_ack = self.force_full_rebuild_on_ack;
        }
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
                cloned_sample.lost = sample.lost;
                cloned_sample.rack_deadline = sample.rack_deadline;
            }
            cursor = next;
        }
        cloned.rack_timer_armed = self.rack_timer_armed;
        cloned.tlp_timer_armed = self.tlp_timer_armed;
        cloned.rack_index = RackDeadlineIndex::default();
        cloned.rack_rescan_earliest();
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

    /// Congestion controller with fixed `congestion_window` and `max_datagram_size`
    /// that ignores all callbacks. Used by the incremental-vs-full-rebuild oracle
    /// test so the two parallel recovery instances see identical congestion
    /// feedback and diverge only via the scoreboard update path.
    #[derive(Clone, Debug, Default)]
    struct FixedController {
        mtu: u32,
        congestion_window: u32,
    }

    impl CongestionController for FixedController {
        fn new(max_datagram_size: u32) -> Self {
            Self {
                mtu: max_datagram_size,
                congestion_window: max_datagram_size.saturating_mul(8),
            }
        }
        fn metrics(&self) -> crate::transport::congestion::CongestionMetrics {
            crate::transport::congestion::CongestionMetrics {
                congestion_window: 0,
                pacing_rate_bytes_per_second: None,
                delivered: 0,
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
            0
        }
        fn min_rtt(&self) -> Option<Duration> {
            None
        }
        fn max_bandwidth_bytes_per_second(&self) -> u64 {
            0
        }
        fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}
        fn on_ack(&mut self, _: Instant, _: AckedPacket, _: RttSample, _: u32) {}
        fn on_end_acks(&mut self, _: Instant, _: u32, _: bool, _: PacketNumber) {}
        fn on_loss(&mut self, _: Instant, _: LostPacket, _: bool) {}
        fn on_mtu_update(&mut self, max_datagram_size: u32) {
            self.mtu = max_datagram_size;
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
        // Bottom-up retransmit order per RFC 6675 §4.3 / VPP scoreboard_next_rxt_hole:
        // the lowest lost sequence (sample 1) is retransmitted first.
        assert_eq!(first.packet_number, 1);

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

        // Bottom-up retransmit order per RFC 6675 §4.3 / VPP scoreboard_next_rxt_hole:
        // lowest lost sequence (sample 1, seq 1000) first, then sample 2 (seq 2000).
        assert_eq!(
            (first.sequence, second.sequence),
            (TcpSeq::from(1_000), TcpSeq::from(2_000))
        );
    }

    #[test]
    fn mark_rack_candidates_does_not_lower_earliest_when_no_new_sample_marked() {
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

        // First SACK: rtt=80ms -> reordering_window=20ms -> deadline = (now+30ms)+20ms = now+50ms.
        // Sample 1 (1k..2k) qualifies (end 2k < high_sacked 3k), gets rack_deadline = now+50ms.
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 80),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        let first_earliest = recovery.rack_earliest_full_scan();
        assert_eq!(
            recovery
                .rack_timeout(now + Duration::from_millis(30))
                .map(|d| d.as_millis()),
            first_earliest.and_then(|d| {
                Some(
                    d.saturating_duration_since(now + Duration::from_millis(30))
                        .as_millis(),
                )
            }),
        );

        // Second SACK: rtt=10ms -> reordering_window=2ms -> deadline = (now+31ms)+2ms = now+33ms.
        // Sample 1 already has rack_deadline (is_some()), so the guard skips it -> any_marked=false.
        // A smaller deadline (33ms < 50ms) must NOT pull earliest down to 33ms (no sample holds it).
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(31), 10),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );

        // earliest must still reflect sample 1's real deadline (now+50ms), not the phantom now+33ms.
        let expected =
            first_earliest.map(|d| d.saturating_duration_since(now + Duration::from_millis(31)));
        assert_eq!(
            recovery.rack_timeout(now + Duration::from_millis(31)),
            expected,
        );
        // And it must match the oracle full scan.
        assert_eq!(recovery.rack_earliest_full_scan(), first_earliest);
    }

    #[test]
    fn rack_earliest_deadline_is_o1_after_record_and_take() {
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
        assert_eq!(recovery.rack_timeout(now), None);

        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(2_000),
                right_edge: TcpSeq::from(3_000),
            }],
            &mut controller,
        );
        // reordering_window = 40/4 = 10ms; deadline = (now+30ms)+10ms = now+40ms
        let rto = recovery
            .rack_timeout(now + Duration::from_millis(30))
            .expect("rack deadline");
        assert_eq!(rto, Duration::from_millis(10));

        recovery.on_ack(
            ack(2_000, now + Duration::from_millis(50), 40),
            &mut controller,
        );
        assert_eq!(recovery.rack_timeout(now + Duration::from_millis(50)), None);
    }

    #[test]
    fn rack_index_matches_full_scan_after_mixed_ops() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);

        fn assert_rack_matches_scan(recovery: &TcpRecoveryState, at: Instant, label: &str) {
            let indexed = recovery.rack_timeout(at);
            let scanned = recovery
                .rack_earliest_full_scan()
                .map(|deadline| deadline.saturating_duration_since(at));
            assert_eq!(indexed, scanned, "{label}: rack index mismatch");
        }

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
        assert_rack_matches_scan(&recovery, now, "after record");

        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(3_000),
                right_edge: TcpSeq::from(4_000),
            }],
            &mut controller,
        );
        assert_rack_matches_scan(&recovery, now + Duration::from_millis(30), "after sack");

        recovery.on_rack_timeout(
            now + Duration::from_millis(60),
            TcpSeq::from(4_000),
            &mut controller,
        );
        assert_rack_matches_scan(
            &recovery,
            now + Duration::from_millis(60),
            "after rack timeout",
        );

        let _ = recovery.take_rack_retransmit();
        assert_rack_matches_scan(
            &recovery,
            now + Duration::from_millis(60),
            "after take rack retransmit",
        );

        recovery.on_ack(
            ack(2_000, now + Duration::from_millis(70), 40),
            &mut controller,
        );
        assert_rack_matches_scan(&recovery, now + Duration::from_millis(70), "after ack");
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
            "AppSession",
            "SessionAppRuntime",
            "SessionId",
        ];

        for pattern in forbidden {
            assert!(
                !module_body.contains(pattern),
                "recovery.rs unexpectedly depends on forbidden layer symbol: {pattern}"
            );
        }
    }

    /// Behavior-equivalence guard for the ACK-path fuse (Task 6 Plan A).
    ///
    /// The pre-fuse `on_ack` did: `take_acked_segments` (collect into a Vec,
    /// decrementing `bytes_in_flight` per taken segment) -> `deliver_acked_segments`
    /// (iterate the Vec, computing `bytes_in_flight_after_ack` per segment as
    /// `bif_after_all_takes + total - sum(seg[0..=i].bytes)`). The fused inline
    /// path must deliver the *exact same* `bytes_in_flight` value to each
    /// `congestion.on_ack` call, fire `on_end_acks` the same number of times,
    /// keep `bytes_in_flight()` identical after the ACK, and credit
    /// `recovery_delivered` by the same total. This test pins those invariants
    /// on a multi-segment cumulative + partial ACK sequence inside recovery,
    /// so regressing the fuse (e.g. reading `bytes_in_flight` before the take,
    /// or dropping the `recovery_delivered` credit) turns it RED.
    #[test]
    fn on_ack_fused_path_matches_pre_fuse_delivery_semantics() {
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);

        // Four full segments at 1000-byte boundaries; total in-flight = 4000.
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
        assert_eq!(recovery.bytes_in_flight(), 4_000);

        // Drive recovery via a SACK of the top segment + RACK timeout so that
        // `recovery_active` is true and `recovery_delivered` is credited on
        // subsequent ACKs.
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &[TcpSackBlock {
                left_edge: TcpSeq::from(4_000),
                right_edge: TcpSeq::from(5_000),
            }],
            &mut controller,
        );
        recovery.on_rack_timeout(
            now + Duration::from_millis(56),
            TcpSeq::from(5_000),
            &mut controller,
        );
        assert!(recovery.in_recovery());
        let delivered_before = recovery.recovery_delivered_for_test();
        assert_eq!(delivered_before, 0);

        // Cumulative ACK of segments 1 and 2 (ack -> 3000). The pre-fuse path
        // delivered `bytes_in_flight_after_ack` = [2000, 1000] to on_ack for
        // packets 1 then 2, and credited recovery_delivered += 2000.
        controller.acked.clear();
        controller.acked_bytes_in_flight.clear();
        controller.end_acks = 0;
        recovery.on_ack(
            ack(3_000, now + Duration::from_millis(90), 90),
            &mut controller,
        );
        assert_eq!(
            controller
                .acked
                .iter()
                .map(|p| p.packet_number)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "cumulative ack must deliver segments in ascending order"
        );
        assert_eq!(
            controller.acked_bytes_in_flight.as_slice(),
            &[2_000, 1_000],
            "per-segment bytes_in_flight must equal pre-fuse rollback computation"
        );
        assert_eq!(controller.end_acks, 1, "on_end_acks must fire once per ACK");
        // SACK already removed segment 4, so in-flight before this ACK was 3000;
        // taking segments 1 and 2 (2000 bytes) leaves 1000.
        assert_eq!(
            recovery.bytes_in_flight(),
            1_000,
            "bytes_in_flight must reflect taken segments"
        );
        assert_eq!(
            recovery.recovery_delivered_for_test(),
            2_000,
            "recovery_delivered must be credited by total acked bytes"
        );

        // Partial ACK splitting segment 3 (ack -> 3500). Pre-fuse delivered
        // bytes_in_flight_after_ack = [500] for the 500-byte prefix and
        // credited recovery_delivered += 500.
        controller.acked.clear();
        controller.acked_bytes_in_flight.clear();
        controller.end_acks = 0;
        recovery.on_ack(
            ack(3_500, now + Duration::from_millis(120), 120),
            &mut controller,
        );
        assert_eq!(controller.acked.len(), 1);
        assert_eq!(controller.acked[0].packet_number, 3);
        assert_eq!(controller.acked[0].bytes, 500);
        assert_eq!(
            controller.acked_bytes_in_flight.as_slice(),
            &[500],
            "partial ack must deliver post-split bytes_in_flight"
        );
        assert_eq!(controller.end_acks, 1);
        assert_eq!(recovery.bytes_in_flight(), 500);
        assert_eq!(
            recovery.recovery_delivered_for_test(),
            2_500,
            "recovery_delivered must accumulate across ACKs"
        );

        // Empty ACK (no advance): on_end_acks must NOT fire and no per-segment
        // on_ack calls must occur.
        controller.acked.clear();
        controller.acked_bytes_in_flight.clear();
        controller.end_acks = 0;
        recovery.on_ack(
            ack(3_500, now + Duration::from_millis(130), 130),
            &mut controller,
        );
        assert!(controller.acked.is_empty());
        assert!(controller.acked_bytes_in_flight.is_empty());
        assert_eq!(
            controller.end_acks, 0,
            "no-op ack must not fire on_end_acks"
        );
        assert_eq!(recovery.bytes_in_flight(), 500);
        assert_eq!(
            recovery.recovery_delivered_for_test(),
            2_500,
            "no-op ack must not credit recovery_delivered"
        );
    }

    /// Source-level allocation guard for Task 6: the ACK hot path must not
    /// allocate a `Vec` for acked-segment collection. `process_ack` inlines the
    /// take+deliver loop and the ACK path no longer calls
    /// `take_acked_segments`/`deliver_acked_segments` (those remain only for the
    /// low-frequency SACK path). A global-allocator counter is unsafe in the
    /// multi-threaded test harness, so this source-equivalence assertion is the
    /// chosen verification method (per the task brief's fallback). It fails if
    /// someone reintroduces a `Vec` allocation on the ACK path.
    #[test]
    fn on_ack_path_does_not_collect_acked_segments_into_vec() {
        let source = include_str!("recovery.rs");
        // Bound the module body at the `mod tests` block, NOT the first
        // `#[cfg(test)]` (which appears on the `clears` debug field far above).
        let tests_start = source
            .find("#[cfg(test)]\nmod tests")
            .expect("tests module");
        let module_body = &source[..tests_start];

        // The ACK path must route through the fused `process_ack`.
        assert!(
            module_body.contains("fn process_ack<"),
            "ACK path must use the fused process_ack (no per-ACK Vec)"
        );

        // `on_ack` must NOT call `take_acked_segments` or `deliver_acked_segments`
        // (those remain only for the SACK path).
        let on_ack_start = module_body
            .find("pub fn on_ack<")
            .expect("on_ack definition");
        let on_ack_body = &module_body[on_ack_start..];
        let on_ack_end = on_ack_body.find("    }\n").expect("on_ack closing brace");
        let on_ack_body = &on_ack_body[..on_ack_end];
        assert!(
            !on_ack_body.contains("take_acked_segments"),
            "on_ack must not call take_acked_segments (per-ACK Vec alloc)"
        );
        assert!(
            !on_ack_body.contains("deliver_acked_segments"),
            "on_ack must not call deliver_acked_segments (consumes a Vec)"
        );

        // `advance_scoreboard_for_ack` and `update_scoreboard_loss` must collect
        // scoreboard keys via the inline `ScoreboardKeyCollector`, not a bare
        // `Vec<TcpSeq>` allocation.
        assert!(
            module_body.contains("struct ScoreboardKeyCollector"),
            "scoreboard key collection must use the inline ScoreboardKeyCollector"
        );
        for fn_name in ["fn advance_scoreboard_for_ack", "fn update_scoreboard_loss"] {
            let fn_start = module_body
                .find(fn_name)
                .unwrap_or_else(|| panic!("{fn_name} not found in recovery.rs module body"));
            let fn_body = &module_body[fn_start..];
            // Find the next top-level `    fn ` or `}` at column 4 to bound the body.
            let fn_end = fn_body[1..]
                .find("\n    fn ")
                .map(|i| i + 1)
                .unwrap_or(fn_body.len());
            let fn_body = &fn_body[..fn_end];
            assert!(
                !fn_body.contains("Vec<TcpSeq>"),
                "{fn_name} must not allocate Vec<TcpSeq> (use ScoreboardKeyCollector)"
            );
            assert!(
                !fn_body.contains("Vec::with_capacity"),
                "{fn_name} must not call Vec::with_capacity"
            );
        }
    }

    /// `ScoreboardKeyCollector` preserves insertion order up to its inline cap
    /// and spills to a heap Vec on overflow without losing or reordering keys.
    #[test]
    fn scoreboard_key_collector_preserves_order_and_overflow() {
        // Below cap: inline only, insertion order preserved.
        let mut c = ScoreboardKeyCollector::new();
        for i in 0..5u32 {
            c.push(TcpSeq::from(1_000 + i));
        }
        let mut out = std::vec::Vec::new();
        while let Some(k) = c.pop_front() {
            out.push(u32::from(k));
        }
        assert_eq!(out, vec![1_000, 1_001, 1_002, 1_003, 1_004]);

        // At cap exactly: still inline.
        let mut c = ScoreboardKeyCollector::new();
        for i in 0..SCOREBOARD_KEY_INLINE_CAP as u32 {
            c.push(TcpSeq::from(2_000 + i));
        }
        let mut out = std::vec::Vec::new();
        while let Some(k) = c.pop_front() {
            out.push(u32::from(k));
        }
        assert_eq!(out.len(), SCOREBOARD_KEY_INLINE_CAP);
        assert_eq!(out[0], 2_000);
        assert_eq!(
            out[SCOREBOARD_KEY_INLINE_CAP - 1],
            2_000 + (SCOREBOARD_KEY_INLINE_CAP - 1) as u32
        );

        // Overflow: spilled keys appear after inline keys, in insertion order.
        let mut c = ScoreboardKeyCollector::new();
        for i in 0..(SCOREBOARD_KEY_INLINE_CAP + 4) as u32 {
            c.push(TcpSeq::from(3_000 + i));
        }
        let mut out = std::vec::Vec::new();
        while let Some(k) = c.pop_front() {
            out.push(u32::from(k));
        }
        assert_eq!(out.len(), SCOREBOARD_KEY_INLINE_CAP + 4);
        for i in 0..out.len() {
            assert_eq!(
                out[i],
                3_000 + i as u32,
                "overflow must preserve order at {i}"
            );
        }

        // Reverse iteration honors overflow-then-inline with correct ordering.
        let mut c = ScoreboardKeyCollector::new();
        for i in 0..(SCOREBOARD_KEY_INLINE_CAP + 2) as u32 {
            c.push(TcpSeq::from(4_000 + i));
        }
        let mut out = std::vec::Vec::new();
        while let Some(k) = c.pop_back() {
            out.push(u32::from(k));
        }
        let total = SCOREBOARD_KEY_INLINE_CAP + 2;
        assert_eq!(out.len(), total);
        for i in 0..total {
            assert_eq!(out[i], 4_000 + (total - 1 - i) as u32, "rev order at {i}");
        }
    }

    /// Drives `update_scoreboard_loss` and `advance_scoreboard_for_ack` past the
    /// inline cap (8 holes) so the `#[cold]` heap fallback is exercised on the
    /// scoreboard key-collection path. Verifies the overflow path produces the
    /// same loss decisions as a hand-computed expectation, guarding the fallback
    /// against silent reordering or dropped keys.
    #[test]
    fn scoreboard_loss_correct_when_hole_count_exceeds_inline_cap() {
        // Build >SCOREBOARD_KEY_INLINE_CAP disjoint SACK gaps by recording many
        // non-contiguous samples and SACKing every other one, then triggering
        // RACK loss so `update_scoreboard_loss` runs with >8 holes.
        let now = Instant::now();
        let mut recovery = TcpRecoveryState::new();
        let mut controller = RecordingController::new(1_000);
        const SEG: u32 = 1_000;
        const N: u32 = (SCOREBOARD_KEY_INLINE_CAP as u32) * 2 + 2; // 18 samples
        for i in 0..N {
            let seq = 1_000 + i * SEG;
            record_sent_for_test(
                &mut recovery,
                i as PacketNumber + 1,
                seq,
                seq + SEG,
                SEG,
                now + Duration::from_millis(i as u64),
            );
        }

        // SACK every even-indexed segment to create gaps; ack_floor stays at 1000.
        let mut blocks: std::vec::Vec<TcpSackBlock> = std::vec::Vec::new();
        for i in 0..N {
            if i % 2 == 1 {
                let seq = 1_000 + i * SEG;
                blocks.push(TcpSackBlock {
                    left_edge: TcpSeq::from(seq),
                    right_edge: TcpSeq::from(seq + SEG),
                });
            }
        }
        recovery.on_sack_blocks(
            ack(1_000, now + Duration::from_millis(30), 40),
            &blocks,
            &mut controller,
        );
        // Hole count = number of unsacked (even-indexed) segments > 8.
        let snap = recovery.scoreboard_snapshot();
        assert!(
            snap.holes.len() > SCOREBOARD_KEY_INLINE_CAP,
            "test must exceed inline cap; got {} holes",
            snap.holes.len()
        );

        // Trigger RACK so update_scoreboard_loss runs on the oversized hole set.
        recovery.on_rack_timeout(
            now + Duration::from_millis(80),
            TcpSeq::from(1_000 + N * SEG),
            &mut controller,
        );

        // The lowest `reorder_limit` (=3) holes above a given hole accumulate
        // enough blocks_ahead to be marked lost; with N/2 sacked blocks above,
        // every non-top hole should be lost (blocks_ahead >= 3 for all but the
        // top 3 holes). Verify lost_bytes reflects lost sample bytes by checking
        // that lost_bytes > 0 and matches a full sample scan.
        let snap = recovery.scoreboard_snapshot();
        assert!(
            snap.lost_bytes > 0,
            "loss marking must fire on oversized hole set"
        );

        // Cross-check: scoreboard `lost_bytes` must equal the sum of outstanding
        // sample bytes flagged `lost` (the unified per-sample loss path). This
        // guards that the overflowed key collection did not drop or reorder
        // holes such that `mark_samples_in_range_lost` missed any sample.
        let samples = recovery.sample_snapshot();
        let scanned_lost: u32 = samples
            .iter()
            .filter(|(_, _, _, lost, _)| *lost)
            .map(|(_, _, bytes, _, _)| *bytes)
            .sum();
        assert_eq!(
            snap.lost_bytes, scanned_lost,
            "lost_bytes must match the per-sample lost scan on oversized hole sets"
        );

        // At least one hole must be declared lost (the overflow path must still
        // propagate loss decisions, not silently no-op).
        assert!(
            snap.holes.iter().any(|(_, _, lost)| *lost),
            "at least one hole must be marked lost past the inline cap"
        );
    }

    // Deterministic LCG pseudo-random generator (fixed seed) so the oracle test
    // is reproducible across runs.
    struct Rng(u64);
    impl Rng {
        fn next_u32(&mut self, bound: u32) -> u32 {
            // xorshift64*
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            ((x >> 32) as u32) % bound.max(1)
        }
    }

    #[test]
    fn incremental_scoreboard_matches_full_rebuild_on_random_sack_sequences() {
        // Verifies the incremental ACK scoreboard path produces identical state
        // to the full per-ACK rebuild oracle on random record/ack/sack/rack
        // sequences. Two parallel `TcpRecoveryState` instances run the same op
        // sequence: `live` uses the incremental `advance_scoreboard_for_ack`;
        // `oracle` forces the full `rebuild_scoreboard` on every ACK. After each
        // op we compare holes (start/end/lost), high_sacked, high_rxt,
        // lost_bytes, and every sample's (sequence/end/lost/retransmitted).
        //
        // RED before Part A: the `clears <= sack_ops + 1` contract fails because
        //   the old code calls `TcpScoreboard::clear()` on every ACK.
        // GREEN after Part A: the incremental path only clears on SACK, and all
        //   scoreboard + per-sample state matches the full-rebuild oracle.
        const MSS: u32 = 1_000;
        const SEG: u32 = 1_000;
        const MAX_SEGMENTS: u32 = 5;
        let now = Instant::now();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);

        for sequence in 0..400u64 {
            let mut live = TcpRecoveryState::new();
            let mut oracle = TcpRecoveryState::new();
            oracle.set_full_rebuild_ack_for_test(true);
            let mut live_cc = FixedController::new(MSS);
            let mut oracle_cc = FixedController::new(MSS);
            let mut next_seq: u32 = 1_000;
            let mut cum_ack: u32 = 1_000;
            let mut sack_ops: u64 = 0;
            let mut step = 0u64;
            let steps = 8 + rng.next_u32(14) as u64;
            while step < steps {
                step += 1;
                let op = rng.next_u32(4);
                match op {
                    0 => {
                        if next_seq >= 1_000 + MAX_SEGMENTS * SEG {
                            continue;
                        }
                        let seq = next_seq;
                        next_seq = next_seq.saturating_add(SEG);
                        let pn = ((seq / SEG) as u64).max(1) as PacketNumber;
                        record_sent_for_test(
                            &mut live,
                            pn,
                            seq,
                            seq + SEG,
                            SEG,
                            now + Duration::from_millis(step),
                        );
                        record_sent_for_test(
                            &mut oracle,
                            pn,
                            seq,
                            seq + SEG,
                            SEG,
                            now + Duration::from_millis(step),
                        );
                    }
                    1 => {
                        // Cumulative TCP ACK is monotonic non-decreasing: only
                        // advance it within [cum_ack, next_seq].
                        let hi = next_seq.max(cum_ack);
                        let ack_seq = cum_ack + rng.next_u32(hi.saturating_sub(cum_ack).max(1));
                        cum_ack = ack_seq;
                        let a = ack(ack_seq, now + Duration::from_millis(50 + step), 40);
                        live.on_ack(a, &mut live_cc);
                        oracle.on_ack(a, &mut oracle_cc);
                    }
                    2 => {
                        if next_seq < 2 * SEG {
                            continue;
                        }
                        let hi = next_seq.max(cum_ack);
                        let ack_seq = cum_ack + rng.next_u32(hi.saturating_sub(cum_ack).max(1));
                        cum_ack = ack_seq;
                        let mut blocks: std::vec::Vec<TcpSackBlock> = std::vec::Vec::new();
                        let nblocks = 1 + rng.next_u32(2) as usize;
                        for _ in 0..nblocks {
                            let span = next_seq.saturating_sub(ack_seq).max(1);
                            let lo = ack_seq + rng.next_u32(span);
                            let hi = (lo + 1 + rng.next_u32(span)).min(next_seq);
                            if hi > lo {
                                blocks.push(TcpSackBlock {
                                    left_edge: TcpSeq::from(lo),
                                    right_edge: TcpSeq::from(hi),
                                });
                            }
                        }
                        if blocks.is_empty() {
                            continue;
                        }
                        sack_ops += 1;
                        let a = ack(ack_seq, now + Duration::from_millis(50 + step), 40);
                        live.on_sack_blocks(a, &blocks, &mut live_cc);
                        oracle.on_sack_blocks(a, &blocks, &mut oracle_cc);
                    }
                    _ => {
                        let t = now + Duration::from_millis(200 + step);
                        live.on_rack_timeout(t, TcpSeq::from(next_seq), &mut live_cc);
                        oracle.on_rack_timeout(t, TcpSeq::from(next_seq), &mut oracle_cc);
                        while live.take_rack_retransmit().is_some() {}
                        while oracle.take_rack_retransmit().is_some() {}
                    }
                }

                let live_snap = live.scoreboard_snapshot();
                let oracle_snap = oracle.scoreboard_snapshot();
                let live_samples = live.sample_snapshot();
                let oracle_samples = oracle.sample_snapshot();
                // Compare scoreboard state (holes/high_sacked/high_rxt/lost_bytes).
                // `clears` intentionally differs: the incremental path must clear
                // less often than the full-rebuild oracle.
                assert_eq!(
                    live_snap.holes,
                    oracle_snap.holes,
                    "seq {sequence} step {step}: holes diverged\n\
                     live={:?}\noracle={:?}\n\
                     live_samples={live_samples:?}\noracle_samples={oracle_samples:?}\n\
                     ack_floor={} high_sacked_live={} high_sacked_oracle={}",
                    live_snap.holes,
                    oracle_snap.holes,
                    u32::from(live.ack_floor),
                    live_snap.high_sacked,
                    oracle_snap.high_sacked,
                );
                assert_eq!(
                    live_snap.high_sacked, oracle_snap.high_sacked,
                    "seq {sequence} step {step}: high_sacked diverged",
                );
                assert_eq!(
                    live_snap.high_rxt, oracle_snap.high_rxt,
                    "seq {sequence} step {step}: high_rxt diverged",
                );
                assert_eq!(
                    live_snap.lost_bytes,
                    oracle_snap.lost_bytes,
                    "seq {sequence} step {step}: lost_bytes diverged (live={} oracle={})\n\
                     live_samples={live_samples:?}\noracle_samples={oracle_samples:?}\n\
                     live_holes={:?}\noracle_holes={:?}",
                    live_snap.lost_bytes,
                    oracle_snap.lost_bytes,
                    live_snap.holes,
                    oracle_snap.holes,
                );
                assert_eq!(
                    live_samples, oracle_samples,
                    "seq {sequence} step {step}: per-sample state diverged\n\
                     live={live_samples:?}\noracle={oracle_samples:?}",
                );

                // Incremental contract: clear() must NOT run on every ACK. SACK
                // ops may full-rebuild (clear), so allow clears up to sack_ops+1.
                // Before Part A this fails because every ACK clears.
                assert!(
                    live_snap.clears <= sack_ops + 1,
                    "seq {sequence} step {step}: too many scoreboard clears \
                     (clears={}, sack_ops={sack_ops}); ACK path must be incremental",
                    live_snap.clears,
                );
            }
            let _ = sequence;
        }
    }
}
