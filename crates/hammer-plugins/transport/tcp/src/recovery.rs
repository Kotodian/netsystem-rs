use std::time::{Duration, Instant};

use crate::{TcpSackBlock, TcpSeq};
use hammer_infra::pool::Pool;
use hammer_infra::rbtree::RbTree;

use hammer_service::transport::congestion::{
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
    pub(crate) prev: Option<u32>,
    pub(crate) next: Option<u32>,
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

    fn split(self, at: TcpSeq) -> Option<(TcpSentSample, TcpSentSample)> {
        if at <= self.sequence || at >= self.end_sequence {
            return None;
        }
        let left_bytes = self.sequence.distance_to(at);
        let left_payload_len = proportional_payload_len(self.bytes, self.payload_len, left_bytes);
        let right_bytes = self.bytes.saturating_sub(left_bytes);
        let right_payload_len = self.payload_len.saturating_sub(left_payload_len);
        Some((
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
        ))
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
        Self {
            holes: self.holes.clone(),
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
        // Preserve the grown capacity so a subsequent rebuild with as many
        // holes as the previous one does not have to grow again.
        let capacity = self.holes.capacity().max(32);
        self.holes = RbTree::with_capacity(capacity);
        self.high_sacked = 0u32.into();
        self.high_rxt = 0u32.into();
        self.lost_bytes = 0;
        self.reorder = TCP_DUPACK_THRESHOLD;
    }
}

/// Test-only snapshot of the scoreboard used by the incremental-vs-full-rebuild
/// oracle test.

#[derive(Debug)]
pub struct TcpRecoveryState {
    next_packet_number: PacketNumber,
    sent_samples: Pool<TcpSentSample>,
    sample_lookup: RbTree<TcpSeq, u32>,
    sample_head: Option<u32>,
    sample_tail: Option<u32>,
    bytes_in_flight: u32,
    ack_floor: TcpSeq,
    scoreboard: TcpScoreboard,
    rack_deadline: Option<Instant>,
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
            rack_deadline: None,
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

    /// Test-only: force `on_ack` to use the full `rebuild_scoreboard` oracle
    /// path instead of the incremental ACK path.

    pub fn next_packet_number(&mut self) -> PacketNumber {
        let packet_number = self.next_packet_number;
        self.next_packet_number = self.next_packet_number.saturating_add(1);
        packet_number
    }

    /// Records one outstanding transmitted sample.
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
        let sample_index = self.sent_samples.insert(TcpSentSample {
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
        });
        let _ = self.sample_lookup.insert(sequence, sample_index);
        if let Some(prev_index) = prev {
            if let Some(previous) = self.sent_samples.get_mut(prev_index) {
                previous.next = Some(sample_index);
            }
        } else {
            self.sample_head = Some(sample_index);
        }
        self.sample_tail = Some(sample_index);
        self.bytes_in_flight = self.bytes_in_flight.saturating_add(bytes);
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
        self.rack_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    pub fn tlp_timeout(&self, srtt: Option<Duration>, rto: Duration) -> Option<Duration> {
        // A SACK-confirmed gap has a concrete RACK deadline. TLP remains the
        // fallback only while there is no stronger loss signal to service.
        if !self.tlp_timer_armed || !self.has_unacked_data() || self.has_pending_rack_deadline() {
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
            let Some(sample) = self.sent_sample(sample_index) else {
                break;
            };
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
            let Some(current) = self.sent_sample_mut(sample_index) else {
                break;
            };
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
            let sample = self.sent_sample(sample_index)?;
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
                let current = self.sent_sample_mut(sample_index)?;
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
        let sample = self.sent_sample(head)?;
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
        let current = self.sent_sample_mut(head)?;
        current.retransmitted = true;
        let cleared = current.rack_deadline;
        current.rack_deadline = None;
        self.rack_invalidate_cleared(cleared);
        self.rack_timer_armed = false;
        self.tlp_timer_armed = self.has_unacked_data();
        Some(sample)
    }

    /// Test accessor: snapshot of the scoreboard holes (start, end, lost) in
    /// ascending order, plus `high_sacked`, `high_rxt` and `lost_bytes`.

    /// Test accessor: every outstanding sample as (sequence, end, bytes, lost,
    /// retransmitted) in ascending sequence order.

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
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if sample.end_sequence < highest_sacked_right
                && !self.sample_is_lost(sample)
                && sample.rack_deadline.is_none()
            {
                if let Some(current) = self.sent_sample_mut(index) {
                    current.rack_deadline = Some(deadline);
                }
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
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            let next = sample.next;
            if ack.acknowledgment <= sample.sequence {
                break;
            }
            let segment = if ack.acknowledgment >= sample.end_sequence {
                let Some(taken) = self.take_sent(index) else {
                    break;
                };
                total_acked_bytes = total_acked_bytes.saturating_add(taken.bytes);
                taken
            } else {
                // Partial prefix: the remaining suffix stays outstanding at
                // `sequence == acknowledgment`, so no later sample can be acked
                // by this cumulative ACK either. Match `take_acked_segments`
                // which breaks after taking the prefix.
                let Some(prefix) = self.take_sample_prefix(index, ack.acknowledgment) else {
                    break;
                };
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
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            let next = sample.next;
            if acknowledgment <= sample.sequence {
                break;
            }
            if acknowledgment >= sample.end_sequence {
                let Some(sample) = self.take_sent(index) else {
                    break;
                };
                total_bytes = total_bytes.saturating_add(sample.bytes);
                acked.push(sample);
            } else {
                let Some(sample) = self.take_sample_prefix(index, acknowledgment) else {
                    break;
                };
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
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if right_edge < sample.sequence {
                break;
            }
            cursor = self.next_sample(sample.sequence);
            if !sample.overlaps(left_edge, right_edge) {
                continue;
            }
            let ack_start = sample.sequence.max(left_edge);
            let ack_end = sample.end_sequence.min(right_edge);
            if ack_start > sample.sequence && self.split_sample(index, ack_start).is_none() {
                continue;
            }
            let Some(current_index) = self.sample_at_or_after(ack_start, false) else {
                continue;
            };
            let Some(current) = self.sent_sample(current_index) else {
                continue;
            };
            if ack_end < current.end_sequence {
                let Some(sample) = self.take_sample_prefix(current_index, ack_end) else {
                    continue;
                };
                total_bytes = total_bytes.saturating_add(sample.bytes);
                matched.push(sample);
            } else {
                let Some(sample) = self.take_sent(current_index) else {
                    continue;
                };
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

    fn sent_sample(&self, index: u32) -> Option<TcpSentSample> {
        self.sent_samples.get(index).copied()
    }

    fn sent_sample_mut(&mut self, index: u32) -> Option<&mut TcpSentSample> {
        self.sent_samples.get_mut(index)
    }

    fn sample_at_or_after(&self, sequence: TcpSeq, include_covering: bool) -> Option<u32> {
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
        if let Some((_, predecessor_index)) = self.sample_lookup.predecessor(&sequence)
            && self
                .sent_sample(*predecessor_index)
                .is_some_and(|predecessor| predecessor.covers(sequence))
        {
            return Some(*predecessor_index);
        }
        successor
    }

    fn next_sample(&self, sequence: TcpSeq) -> Option<u32> {
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

    fn take_sent(&mut self, index: u32) -> Option<TcpSentSample> {
        let sample = self.sent_sample(index)?;
        let cleared = sample.rack_deadline;
        if self.sample_lookup.get(&sample.sequence).copied() != Some(index) {
            return None;
        }
        if sample
            .prev
            .is_some_and(|prev| self.sent_samples.get(prev).is_none())
            || sample
                .next
                .is_some_and(|next| self.sent_samples.get(next).is_none())
        {
            return None;
        }
        self.sample_lookup.remove(&sample.sequence)?;
        if let Some(prev) = sample.prev {
            self.sent_sample_mut(prev)?.next = sample.next;
        } else {
            self.sample_head = sample.next;
        }
        if let Some(next) = sample.next {
            self.sent_sample_mut(next)?.prev = sample.prev;
        } else {
            self.sample_tail = sample.prev;
        }
        let sample = self.sent_samples.remove(index)?;
        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(sample.bytes);
        self.rack_invalidate_cleared(cleared);
        Some(sample)
    }

    fn split_sample(&mut self, index: u32, split_start: TcpSeq) -> Option<()> {
        let sample = self.sent_sample(index)?;
        let (prefix, suffix) = sample.split(split_start)?;
        if self.sample_lookup.get(&sample.sequence).copied() != Some(index) {
            return None;
        }
        self.sample_lookup.remove(&sample.sequence)?;
        {
            let current = self.sent_sample_mut(index)?;
            current.sequence = suffix.sequence;
            current.bytes = suffix.bytes;
            current.payload_len = suffix.payload_len;
            current.rack_deadline = sample.rack_deadline;
        }
        let _ = self.sample_lookup.insert(suffix.sequence, index);

        let prefix_index = self.insert_sample_before(index, prefix)?;
        if !self.sample_is_lost(sample)
            && let Some(deadline) = sample.rack_deadline
        {
            self.sent_sample_mut(prefix_index)?.rack_deadline = Some(deadline);
            self.rack_note_deadline(deadline);
        }
        Some(())
    }

    fn take_sample_prefix(&mut self, index: u32, split_end: TcpSeq) -> Option<TcpSentSample> {
        let sample = self.sent_sample(index)?;
        let (prefix, suffix) = sample.split(split_end)?;
        if self.sample_lookup.get(&sample.sequence).copied() != Some(index) {
            return None;
        }
        self.sample_lookup.remove(&sample.sequence)?;
        {
            let current = self.sent_sample_mut(index)?;
            current.sequence = suffix.sequence;
            current.bytes = suffix.bytes;
            current.payload_len = suffix.payload_len;
            current.rack_deadline = sample.rack_deadline;
        }
        let _ = self.sample_lookup.insert(suffix.sequence, index);

        self.bytes_in_flight = self.bytes_in_flight.saturating_sub(prefix.bytes);
        self.rack_invalidate_cleared(sample.rack_deadline);
        Some(prefix)
    }

    fn insert_sample_before(&mut self, next_index: u32, mut sample: TcpSentSample) -> Option<u32> {
        let next = self.sent_sample(next_index)?;
        if self.sample_lookup.contains_key(&sample.sequence) {
            return None;
        }
        if next
            .prev
            .is_some_and(|prev_index| self.sent_samples.get(prev_index).is_none())
        {
            return None;
        }
        sample.prev = next.prev;
        sample.next = Some(next_index);
        let sample_index = self.sent_samples.insert(sample);
        if self
            .sample_lookup
            .insert(sample.sequence, sample_index)
            .is_some()
        {
            self.sent_samples.remove(sample_index);
            return None;
        }
        if let Some(prev_index) = next.prev {
            self.sent_sample_mut(prev_index)?.next = Some(sample_index);
        } else {
            self.sample_head = Some(sample_index);
        }
        self.sent_sample_mut(next_index)?.prev = Some(sample_index);
        Some(sample_index)
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
            // still count toward lost_bytes - recompute from per-sample flags
            // instead of leaving the zero from clear().
            self.refresh_lost_bytes();
            self.rack_rescan_earliest();
            return;
        }

        let mut cursor = self.sample_head;
        let mut hole_start = acknowledgment;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
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
                let _ = self.scoreboard.holes.remove(&start);
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
            let _ = self.scoreboard.holes.remove(&start);
        }
        if let Some((old_start, end, lost)) = to_trim {
            let _ = self.scoreboard.holes.remove(&old_start);
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
            if let Some(first) = self.sent_sample(head)
                && first.sequence > acknowledgment
                && acknowledgment < self.scoreboard.high_sacked
            {
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
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if sample.sequence >= range_end {
                break;
            }
            cursor = sample.next;
            if sample.end_sequence > range_start && sample.sequence < range_end {
                let Some(current) = self.sent_sample_mut(index) else {
                    break;
                };
                current.lost = true;
            }
        }
    }

    fn queue_recovery_head(&mut self, _: Instant, _: Duration) {
        let Some(head) = self.sample_head else {
            return;
        };
        let Some(sample) = self.sent_sample(head) else {
            return;
        };
        if sample.rack_deadline.is_some() || self.sample_is_lost(sample) {
            return;
        }
        let Some(current) = self.sent_sample_mut(head) else {
            return;
        };
        current.rack_deadline = None;
        current.lost = true;
        self.refresh_lost_bytes();
        self.rack_rescan_earliest();
        self.rack_timer_armed = self.has_pending_rack_deadline();
    }

    fn refresh_lost_bytes(&mut self) {
        let mut lost_bytes = 0u32;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
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
        self.rack_deadline.is_some()
    }

    #[inline]
    fn rack_note_deadline(&mut self, deadline: Instant) {
        self.rack_deadline = Some(match self.rack_deadline {
            None => deadline,
            Some(current) => current.min(deadline),
        });
    }

    fn rack_invalidate_cleared(&mut self, cleared: Option<Instant>) {
        let Some(cleared) = cleared else {
            return;
        };
        if self.rack_deadline != Some(cleared) {
            return;
        }
        self.rack_rescan_earliest();
    }

    #[cold]
    fn rack_rescan_earliest(&mut self) {
        let mut earliest = None;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if !self.sample_is_lost(sample)
                && let Some(deadline) = sample.rack_deadline
                && earliest.is_none_or(|current| deadline < current)
            {
                earliest = Some(deadline);
            }
            cursor = sample.next;
        }
        self.rack_deadline = earliest;
    }
}

impl Clone for TcpRecoveryState {
    fn clone(&self) -> Self {
        Self {
            next_packet_number: self.next_packet_number,
            sent_samples: self.sent_samples.clone(),
            sample_lookup: self.sample_lookup.clone(),
            sample_head: self.sample_head,
            sample_tail: self.sample_tail,
            bytes_in_flight: self.bytes_in_flight,
            ack_floor: self.ack_floor,
            scoreboard: self.scoreboard.clone(),
            rack_deadline: self.rack_deadline,
            rack_timer_armed: self.rack_timer_armed,
            tlp_timer_armed: self.tlp_timer_armed,
            recovery_active: self.recovery_active,
            recovery_window: self.recovery_window,
            recovery_prev_window: self.recovery_prev_window,
            recovery_delivered: self.recovery_delivered,
            recovery_retransmitted: self.recovery_retransmitted,
            recovery_new_data: self.recovery_new_data,
            recovery_end_sequence: self.recovery_end_sequence,
        }
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
