use std::time::{Duration, Instant};

use crate::{TcpSackBlock, TcpSeq};
use hammer_infra::pool::Pool;
use hammer_infra::rbtree::RbTree;

use hammer_service::transport::congestion::{
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
    pub(crate) lost: bool,
    pub(crate) tx_lost: bool,
    pub(crate) delivered: u64,
    pub(crate) delivered_time: Option<Instant>,
    pub(crate) first_tx_time: Instant,
    pub(crate) app_limited: bool,
    pub(crate) tx_in_flight: u64,
    pub(crate) tx_lost_bytes: u64,
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
    delivered: u64,
    delivered_time: Option<Instant>,
    first_tx_time: Option<Instant>,
    lost: u64,
    ack_floor: TcpSeq,
    scoreboard: TcpScoreboard,
    rack_deadline: Option<Instant>,
    rack_reference_sent_at: Option<Instant>,
    rack_reference_end: TcpSeq,
    rack_rtt: Option<Duration>,
    rack_reordered: bool,
    pub(crate) rack_enabled: bool,
    snd_nxt: TcpSeq,
    tlp_timer_armed: bool,
    tlp_probe_end: Option<TcpSeq>,
    tlp_rtt_fresh: bool,
    recovery_active: bool,
    no_sack_first_pending: bool,
    recovery_window: u32,
    recovery_prev_window: u32,
    recovery_delivered: u32,
    recovery_prev_delivered: u32,
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
            delivered: 0,
            delivered_time: None,
            first_tx_time: None,
            lost: 0,
            ack_floor: 0u32.into(),
            scoreboard: TcpScoreboard::new(),
            rack_deadline: None,
            rack_reference_sent_at: None,
            rack_reference_end: TcpSeq::from(0),
            rack_rtt: None,
            rack_reordered: false,
            rack_enabled: false,
            snd_nxt: TcpSeq::from(0),
            tlp_timer_armed: false,
            tlp_probe_end: None,
            tlp_rtt_fresh: false,
            recovery_active: false,
            no_sack_first_pending: false,
            recovery_window: 0,
            recovery_prev_window: 0,
            recovery_delivered: 0,
            recovery_prev_delivered: 0,
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
        bytes_in_flight: u32,
        sent_at: Instant,
        app_limited: bool,
    ) {
        if bytes == 0 {
            return;
        }
        if bytes_in_flight == 0 {
            self.delivered_time = Some(sent_at);
            self.first_tx_time = Some(sent_at);
        }
        let first_tx_time = self.first_tx_time.unwrap_or(sent_at);
        let prev = self.sample_tail;
        let sample_index = self.sent_samples.insert(TcpSentSample {
            packet_number,
            sequence,
            end_sequence,
            bytes,
            payload_len,
            retransmitted: false,
            lost: false,
            tx_lost: false,
            delivered: self.delivered,
            delivered_time: self.delivered_time,
            first_tx_time,
            app_limited,
            tx_in_flight: u64::from(bytes_in_flight).saturating_add(u64::from(bytes)),
            tx_lost_bytes: self.lost,
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
        self.snd_nxt = end_sequence;
        self.tlp_timer_armed = self.tlp_rtt_fresh && self.tlp_probe_end.is_none();
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

    /// VPP tcp_sack.h scoreboard sacked_bytes: bytes SACKed above the
    /// cumulative ACK, excluding the currently tracked missing ranges.
    #[inline]
    pub(crate) fn sacked_bytes(&self) -> u32 {
        let acknowledged_to_high = self.ack_floor.distance_to(self.scoreboard.high_sacked);
        let mut missing = 0u32;
        let mut cursor = self.scoreboard.holes.first().map(|(start, _)| *start);
        while let Some(start) = cursor {
            let hole = self
                .scoreboard
                .holes
                .get(&start)
                .expect("scoreboard traversal retains its current hole");
            missing = missing.saturating_add(start.distance_to(hole.end));
            cursor = self
                .scoreboard
                .holes
                .successor(&start)
                .map(|(next, _)| *next);
        }
        acknowledged_to_high.saturating_sub(missing)
    }

    #[inline]
    pub(crate) fn reorder_threshold(&self) -> u32 {
        self.scoreboard.reorder
    }

    pub fn rack_timeout(&self, now: Instant) -> Option<Duration> {
        self.rack_deadline
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    pub fn tlp_timeout(&self, srtt: Option<Duration>, rto: Duration) -> Option<Duration> {
        // A SACK-confirmed gap has a concrete RACK deadline. TLP remains the
        // fallback only while there is no stronger loss signal to service.
        if !self.tlp_timer_armed
            || !self.tlp_rtt_fresh
            || self.tlp_probe_end.is_some()
            || !self.has_unacked_data()
            || self.has_pending_rack_deadline()
            || self.recovery_active
        {
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
        if self.rack_enabled && self.scoreboard.high_sacked > ack.acknowledgment {
            self.mark_rack_candidates(
                self.scoreboard.high_sacked,
                ack.now,
                ack.reordering_window,
                congestion.max_datagram_size(),
            );
            self.on_rack_timeout(ack.now, self.snd_nxt, congestion);
        }
        if self.recovery_active && advanced {
            self.queue_recovery_head(ack.now, ack.reordering_window);
            self.no_sack_first_pending = true;
        }
        if latest_rtt.is_some() {
            self.tlp_rtt_fresh = true;
        }
        if self
            .tlp_probe_end
            .is_some_and(|end| ack.acknowledgment >= end)
        {
            self.tlp_probe_end = None;
        }
        self.tlp_timer_armed =
            self.has_unacked_data() && self.tlp_rtt_fresh && self.tlp_probe_end.is_none();
        latest_rtt
    }

    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    ) -> Option<Duration> {
        self.ack_floor = ack.acknowledgment;
        let mut latest_rtt = None;
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut acked_bytes = 0u32;
        let mut rate_sample = None;
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if ack.acknowledgment <= sample.sequence {
                break;
            }
            cursor = sample.next;
            let partial = ack.acknowledgment < sample.end_sequence;
            let segment = if partial {
                self.take_sample_prefix(index, ack.acknowledgment)
            } else {
                self.take_sent(index)
            };
            let Some(segment) = segment else {
                break;
            };
            largest_acked = largest_acked.max(segment.packet_number);
            any_acked = true;
            acked_bytes = acked_bytes.saturating_add(segment.bytes);
            latest_rtt = self
                .deliver_sample(ack, segment, &mut rate_sample)
                .or(latest_rtt);
            if partial {
                break;
            }
        }
        let mut highest_sacked_right = self.scoreboard.high_sacked.max(ack.acknowledgment);
        for block in blocks {
            // VPP tcp_sack.c:1137-1149 excludes invalid ordinary SACK blocks.
            if block.left_edge >= block.right_edge
                || block.left_edge <= ack.acknowledgment
                || block.left_edge >= self.snd_nxt
                || block.right_edge > self.snd_nxt
            {
                continue;
            }
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            let mut cursor = self.sample_at_or_after(block.left_edge, true);
            while let Some(index) = cursor {
                let Some(sample) = self.sent_sample(index) else {
                    break;
                };
                if block.right_edge < sample.sequence {
                    break;
                }
                cursor = self.next_sample(sample.sequence);
                if !sample.overlaps(block.left_edge, block.right_edge) {
                    continue;
                }
                let ack_start = sample.sequence.max(block.left_edge);
                let ack_end = sample.end_sequence.min(block.right_edge);
                if ack_start > sample.sequence && self.split_sample(index, ack_start).is_none() {
                    continue;
                }
                let Some(current_index) = self.sample_at_or_after(ack_start, false) else {
                    continue;
                };
                let Some(current) = self.sent_sample(current_index) else {
                    continue;
                };
                let segment = if ack_end < current.end_sequence {
                    self.take_sample_prefix(current_index, ack_end)
                } else {
                    self.take_sent(current_index)
                };
                let Some(segment) = segment else {
                    continue;
                };
                largest_acked = largest_acked.max(segment.packet_number);
                any_acked = true;
                acked_bytes = acked_bytes.saturating_add(segment.bytes);
                latest_rtt = self
                    .deliver_sample(ack, segment, &mut rate_sample)
                    .or(latest_rtt);
            }
        }
        if any_acked {
            self.deliver_ack_batch(ack, acked_bytes, latest_rtt, rate_sample, congestion);
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        self.rebuild_scoreboard(
            ack.acknowledgment,
            highest_sacked_right,
            congestion.max_datagram_size(),
        );
        self.maybe_finish_recovery(ack.acknowledgment);
        if self.rack_enabled && highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(
                highest_sacked_right,
                ack.now,
                ack.reordering_window,
                congestion.max_datagram_size(),
            );
            self.on_rack_timeout(ack.now, self.snd_nxt, congestion);
        }
        if latest_rtt.is_some() {
            self.tlp_rtt_fresh = true;
        }
        if self
            .tlp_probe_end
            .is_some_and(|end| ack.acknowledgment >= end)
        {
            self.tlp_probe_end = None;
        }
        self.tlp_timer_armed =
            self.has_unacked_data() && self.tlp_rtt_fresh && self.tlp_probe_end.is_none();
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
        let mut lost_any = false;
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
            self.lost = self.lost.saturating_add(u64::from(sample.bytes));
            let Some(current) = self.sent_sample_mut(sample_index) else {
                break;
            };
            current.rack_deadline = None;
            current.lost = true;
            current.tx_lost = true;
            lost_any = true;
            recovery_started |= !self.recovery_active;
        }
        if recovery_started {
            self.recovery_active = true;
            self.scoreboard.high_rxt = self
                .scoreboard
                .holes
                .first()
                .map(|(start, _)| (*start).max(self.ack_floor))
                .unwrap_or(self.ack_floor);
            self.no_sack_first_pending = true;
            self.recovery_prev_window = recovery_prev_window.max(1);
            self.recovery_window = congestion.congestion_window();
            self.recovery_delivered = 0;
            self.recovery_prev_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = snd_nxt;
        } else if self.recovery_active {
            self.recovery_window = self.recovery_window.min(congestion.congestion_window());
        }
        self.refresh_lost_bytes();
        self.rack_rescan_earliest();
        if lost_any {
            self.tlp_timer_armed = false;
            self.tlp_probe_end = None;
        }
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

    #[inline]
    pub(crate) fn on_recovery_burst_sent(&mut self) {
        self.recovery_prev_delivered = self.recovery_delivered;
        self.no_sack_first_pending = false;
    }

    #[inline]
    pub(crate) fn no_sack_first_pending(&self) -> bool {
        self.no_sack_first_pending
    }

    #[inline]
    pub(crate) fn recovery_end_sequence(&self) -> TcpSeq {
        self.recovery_end_sequence
    }

    /// VPP tcp_output.c:1929-1932 advances HighRxt only after enqueue.
    #[inline]
    pub(crate) fn advance_high_rxt(&mut self, end: TcpSeq) {
        self.scoreboard.high_rxt = if self.rack_enabled {
            end
        } else {
            self.scoreboard.high_rxt.max(end)
        };
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
            let conserved = self.recovery_delivered.saturating_sub(prr_out);
            let delivered_since_send = self
                .recovery_delivered
                .saturating_sub(self.recovery_prev_delivered);
            let limit = conserved
                .max(delivered_since_send)
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

    /// VPP: tcp_bt.c:1710-1742. Selection does not spend retransmit
    /// eligibility before a Buffer has entered Session pending output.
    pub(crate) fn retransmit_candidate(&self) -> Option<TcpSentSample> {
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index)?;
            cursor = sample.next;
            if sample.tx_lost {
                return Some(sample);
            }
        }
        None
    }

    #[inline]
    pub(crate) fn oldest_unacked(&self) -> Option<TcpSentSample> {
        self.sample_head.and_then(|index| self.sent_sample(index))
    }

    #[inline]
    pub(crate) fn tail_loss_probe_candidate(&self) -> Option<TcpSentSample> {
        self.sample_tail.and_then(|index| self.sent_sample(index))
    }

    pub(crate) fn sample_covering(&self, sequence: TcpSeq) -> Option<TcpSentSample> {
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self.sent_sample(index)?;
            if sample.covers(sequence) {
                return Some(sample);
            }
            cursor = sample.next;
        }
        None
    }

    /// VPP: tcp_output.c:1230-1264. Called only after the final Buffer
    /// contains the retransmitted bytes and has entered Session pending TX.
    pub(crate) fn commit_retransmit(&mut self, sequence: TcpSeq, sent_at: Instant) {
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let sample = self
                .sent_sample(index)
                .expect("retransmit sample remains in the connection pool");
            cursor = sample.next;
            if sample.sequence != sequence {
                continue;
            }
            let deadline = sample.rack_deadline;
            let current = self
                .sent_sample_mut(index)
                .expect("retransmit sample remains in the connection pool");
            current.retransmitted = true;
            current.tx_lost = false;
            current.rack_deadline = None;
            current.sent_at = sent_at;
            self.rack_invalidate_cleared(deadline);
            self.scoreboard.high_rxt = if self.rack_enabled {
                sample.end_sequence
            } else {
                self.scoreboard.high_rxt.max(sample.end_sequence)
            };
            self.refresh_lost_bytes();
            return;
        }
        panic!("retransmitted TCP sequence retains a sent sample");
    }

    /// VPP tcp_tlp.c:15-20,50-68; tcp_output.c:1360-1378. Publish only
    /// after the probe Buffer enters Session pending TX.
    pub(crate) fn record_tlp_probe(
        &mut self,
        end: TcpSeq,
        retransmitted: Option<(TcpSeq, TcpSeq)>,
    ) {
        if let Some((start, retransmit_end)) = retransmitted {
            let mut cursor = self.sample_head;
            while let Some(index) = cursor {
                let sample = self
                    .sent_sample(index)
                    .expect("TLP sample remains allocated during probe publication");
                cursor = sample.next;
                if sample.overlaps(start, retransmit_end) {
                    let current = self
                        .sent_sample_mut(index)
                        .expect("TLP sample remains allocated during probe publication");
                    current.retransmitted = true;
                    current.rack_deadline = None;
                }
            }
            self.rack_rescan_earliest();
        }
        self.tlp_probe_end = Some(end);
        self.tlp_rtt_fresh = false;
        self.tlp_timer_armed = false;
    }

    #[inline]
    pub(crate) fn disarm_tlp(&mut self) {
        self.tlp_timer_armed = false;
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
        if !sample.tx_lost {
            self.lost = self.lost.saturating_add(u64::from(sample.bytes));
        }
        let current = self
            .sent_sample_mut(head)
            .expect("RTO retains the oldest outstanding sample");
        current.tx_lost = true;
        current.lost = true;
        self.refresh_lost_bytes();
        if !self.recovery_active {
            self.recovery_active = true;
            self.scoreboard.high_rxt = self
                .scoreboard
                .holes
                .first()
                .map(|(start, _)| (*start).max(self.ack_floor))
                .unwrap_or(self.ack_floor);
            self.recovery_prev_window = recovery_prev_window.max(1);
            self.recovery_window = congestion.congestion_window();
            self.recovery_delivered = 0;
            self.recovery_prev_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = snd_nxt;
        } else {
            self.recovery_window = self.recovery_window.min(congestion.congestion_window());
            if snd_nxt > self.recovery_end_sequence {
                self.recovery_end_sequence = snd_nxt;
            }
        }
        self.no_sack_first_pending = true;
        self.tlp_timer_armed = false;
        self.tlp_probe_end = None;
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
        max_datagram_size: u32,
    ) {
        let (Some(reference_sent_at), Some(rtt)) = (self.rack_reference_sent_at, self.rack_rtt)
        else {
            return;
        };
        let reordering_window = if !self.rack_reordered
            && (self.recovery_active
                || self.sacked_bytes() >= self.scoreboard.reorder.saturating_mul(max_datagram_size))
        {
            Duration::ZERO
        } else {
            reordering_window
        };
        let mut cursor = self.sample_head;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            if sample.end_sequence <= highest_sacked_right
                && (sample.sent_at < reference_sent_at
                    || (sample.sent_at == reference_sent_at
                        && sample.end_sequence < self.rack_reference_end))
                && !self.sample_is_lost(sample)
                && sample.rack_deadline.is_none()
            {
                let deadline = sample.sent_at + rtt + reordering_window;
                if let Some(current) = self.sent_sample_mut(index) {
                    current.rack_deadline = Some(deadline);
                }
                self.rack_note_deadline(deadline.max(now));
            }
            cursor = sample.next;
        }
    }

    /// Cumulative ACK samples are removed and delivered without staging them.
    fn process_ack<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        congestion: &mut C,
    ) -> (bool, Option<Duration>) {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut acked_bytes = 0u32;
        let mut latest_rtt = None;
        let mut rate_sample = None;
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
                taken
            } else {
                // Partial prefix: the remaining suffix stays outstanding at
                // `sequence == acknowledgment`, so no later sample is acked.
                let Some(prefix) = self.take_sample_prefix(index, ack.acknowledgment) else {
                    break;
                };
                done = true;
                prefix
            };
            largest_acked = largest_acked.max(segment.packet_number);
            any_acked = true;
            acked_bytes = acked_bytes.saturating_add(segment.bytes);
            latest_rtt = self
                .deliver_sample(ack, segment, &mut rate_sample)
                .or(latest_rtt);
            if done {
                break;
            }
            cursor = next;
        }
        if any_acked {
            self.deliver_ack_batch(ack, acked_bytes, latest_rtt, rate_sample, congestion);
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        (any_acked, latest_rtt)
    }

    fn deliver_sample(
        &mut self,
        ack: TcpRecoveryAck,
        segment: TcpSentSample,
        rate_sample: &mut Option<TcpSentSample>,
    ) -> Option<Duration> {
        if self.recovery_active {
            self.recovery_delivered = self.recovery_delivered.saturating_add(segment.bytes);
        }
        self.delivered = self.delivered.saturating_add(u64::from(segment.bytes));
        self.delivered_time = Some(ack.now);
        if rate_sample.is_none_or(|current| {
            segment.sent_at > current.sent_at
                || (segment.sent_at == current.sent_at
                    && segment.end_sequence > current.end_sequence)
        }) {
            *rate_sample = Some(segment);
        }
        if !segment.retransmitted {
            let rtt = ack.now.saturating_duration_since(segment.sent_at);
            if !rtt.is_zero() {
                self.rack_reordered |= segment.end_sequence < self.scoreboard.high_sacked;
                self.rack_rtt = Some(rtt);
                if self.rack_reference_sent_at.is_none_or(|sent_at| {
                    segment.sent_at > sent_at
                        || (segment.sent_at == sent_at
                            && segment.end_sequence > self.rack_reference_end)
                }) {
                    self.rack_reference_sent_at = Some(segment.sent_at);
                    self.rack_reference_end = segment.end_sequence;
                }
            }
        }
        (!segment.retransmitted).then_some(ack.now.saturating_duration_since(segment.sent_at))
    }

    fn deliver_ack_batch<C: CongestionController>(
        &self,
        ack: TcpRecoveryAck,
        acked_bytes: u32,
        latest_rtt: Option<Duration>,
        rate_sample: Option<TcpSentSample>,
        congestion: &mut C,
    ) {
        let Some(segment) = rate_sample else {
            return;
        };
        let prior_time = segment.delivered_time.unwrap_or(segment.first_tx_time);
        let delivered_interval = ack.now.saturating_duration_since(prior_time);
        let tx_interval = segment
            .sent_at
            .saturating_duration_since(segment.first_tx_time);
        let interval = delivered_interval.max(tx_interval);
        congestion.on_ack(
            ack.now,
            AckedPacket {
                packet_number: segment.packet_number,
                bytes: acked_bytes,
                sent_at: segment.sent_at,
                delivered: self.delivered.saturating_sub(segment.delivered),
                prior_delivered: segment.delivered,
                interval,
                tx_in_flight: segment.tx_in_flight,
                tx_lost: segment.tx_lost_bytes,
                app_limited: segment.app_limited,
                ecn_ce_count: ack.ecn_ce_count,
            },
            RttSample {
                latest: latest_rtt.unwrap_or(Duration::ZERO),
                min: latest_rtt.unwrap_or(Duration::ZERO),
            },
            self.bytes_in_flight(),
        );
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
            self.recovery_prev_delivered = 0;
            self.recovery_retransmitted = 0;
            self.recovery_new_data = 0;
            self.recovery_end_sequence = TcpSeq::from(0);
            self.no_sack_first_pending = false;
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
        let high_rxt = self.scoreboard.high_rxt;
        self.scoreboard.clear();
        self.scoreboard.high_sacked = high_sacked.max(acknowledgment);
        self.scoreboard.high_rxt = if self.recovery_active {
            high_rxt.max(acknowledgment)
        } else {
            acknowledgment
        };
        if self.sample_head.is_none() || self.scoreboard.high_sacked <= acknowledgment {
            // No SACK-gap holes remain, but RACK-lost samples above high_sacked
            // still count toward lost_bytes - recompute from per-sample flags
            // instead of leaving the zero from clear().
            self.refresh_lost_bytes();
            self.rack_rescan_earliest();
            return;
        }

        let mut cursor = self.sample_head;
        let mut pending_hole: Option<(TcpSeq, TcpSeq)> = None;
        while let Some(index) = cursor {
            let Some(sample) = self.sent_sample(index) else {
                break;
            };
            cursor = sample.next;
            if sample.end_sequence <= acknowledgment {
                continue;
            }
            if sample.sequence >= self.scoreboard.high_sacked {
                break;
            }
            let start = sample.sequence.max(acknowledgment);
            let end = sample.end_sequence.min(self.scoreboard.high_sacked);
            if start >= end {
                continue;
            }
            match pending_hole {
                Some((hole_start, hole_end)) if start <= hole_end => {
                    pending_hole = Some((hole_start, hole_end.max(end)));
                }
                Some((hole_start, hole_end)) => {
                    let _ = self.scoreboard.holes.insert(
                        hole_start,
                        TcpScoreboardHole {
                            end: hole_end,
                            lost: false,
                        },
                    );
                    pending_hole = Some((start, end));
                }
                None => pending_hole = Some((start, end)),
            }
        }

        if let Some((hole_start, hole_end)) = pending_hole {
            let _ = self.scoreboard.holes.insert(
                hole_start,
                TcpScoreboardHole {
                    end: hole_end,
                    lost: false,
                },
            );
        }
        if self.rack_enabled {
            self.refresh_lost_bytes();
        } else {
            self.update_scoreboard_loss(max_datagram_size.max(1));
        }
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
        // VPP tcp_sack.c:273-288 initializes HighRxt on recovery entry;
        // subsequent ACKs can advance it but cannot undo sent retransmits.
        self.scoreboard.high_rxt = self.scoreboard.high_rxt.max(acknowledgment);
        if self.sample_head.is_none() || self.scoreboard.high_sacked <= acknowledgment {
            // No outstanding samples or everything up to high_sacked is now
            // acknowledged: no SACK-gap holes remain. Drop holes without a full
            // clear() of unrelated scoreboard state, then recompute lost_bytes
            // from per-sample flags (RACK-lost samples above high_sacked still
            // count).
            while let Some(start) = self.scoreboard.holes.first().map(|(start, _)| *start) {
                let _ = self.scoreboard.holes.remove(&start);
            }
            self.refresh_lost_bytes();
            self.rack_rescan_earliest();
            return;
        }

        // Holes are ordered and disjoint: remove the acknowledged prefix,
        // then trim at most one hole crossing the new ACK.
        while let Some((start, hole)) = self
            .scoreboard
            .holes
            .first()
            .map(|(start, hole)| (*start, *hole))
        {
            if hole.end <= acknowledgment {
                let _ = self.scoreboard.holes.remove(&start);
                continue;
            }
            if start < acknowledgment {
                let _ = self.scoreboard.holes.remove(&start);
                let _ = self.scoreboard.holes.insert(
                    acknowledgment,
                    TcpScoreboardHole {
                        end: hole.end,
                        lost: hole.lost,
                    },
                );
            }
            break;
        }

        if self.rack_enabled {
            self.refresh_lost_bytes();
        } else {
            self.update_scoreboard_loss(max_datagram_size.max(1));
        }
        self.rack_rescan_earliest();
    }

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

        // VPP tcp_sack.c:131-139 starts with the SACKed run after the
        // highest unsacked hole; omitting it leaves that hole never lost.
        let trailing_sacked = self
            .scoreboard
            .holes
            .last()
            .map(|(_, hole)| hole.end.distance_to(self.scoreboard.high_sacked))
            .unwrap_or(0);
        let mut sacked_ahead = trailing_sacked;
        let mut blocks_ahead = u32::from(trailing_sacked != 0);
        let mut higher_start: Option<TcpSeq> = None;
        let mut cursor = self.scoreboard.holes.last().map(|(start, _)| *start);
        while let Some(start) = cursor {
            cursor = self
                .scoreboard
                .holes
                .predecessor(&start)
                .map(|(previous, _)| *previous);
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
                // A SACK-gap hole marks its active transmission eligible;
                // RACK uses the same sample bits without the scoreboard path.
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
                current.tx_lost = true;
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
        current.tx_lost = true;
        self.refresh_lost_bytes();
        self.rack_rescan_earliest();
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
        sample.tx_lost
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
            delivered: self.delivered,
            delivered_time: self.delivered_time,
            first_tx_time: self.first_tx_time,
            lost: self.lost,
            ack_floor: self.ack_floor,
            scoreboard: self.scoreboard.clone(),
            rack_deadline: self.rack_deadline,
            rack_reference_sent_at: self.rack_reference_sent_at,
            rack_reference_end: self.rack_reference_end,
            rack_rtt: self.rack_rtt,
            rack_reordered: self.rack_reordered,
            rack_enabled: self.rack_enabled,
            snd_nxt: self.snd_nxt,
            tlp_timer_armed: self.tlp_timer_armed,
            tlp_probe_end: self.tlp_probe_end,
            tlp_rtt_fresh: self.tlp_rtt_fresh,
            recovery_active: self.recovery_active,
            no_sack_first_pending: self.no_sack_first_pending,
            recovery_window: self.recovery_window,
            recovery_prev_window: self.recovery_prev_window,
            recovery_delivered: self.recovery_delivered,
            recovery_prev_delivered: self.recovery_prev_delivered,
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

#[inline]
fn proportional_payload_len(bytes: u32, payload_len: u32, portion_bytes: u32) -> u32 {
    if bytes == 0 || payload_len == 0 || portion_bytes == 0 {
        return 0;
    }
    let payload = (u64::from(payload_len) * u64::from(portion_bytes)) / u64::from(bytes);
    payload.min(u64::from(payload_len)) as u32
}
