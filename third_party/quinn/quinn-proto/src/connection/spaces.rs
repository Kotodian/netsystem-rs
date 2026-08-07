use std::{
    cmp,
    collections::{BTreeMap, VecDeque},
    mem,
    ops::Bound,
};

use rand::Rng;
use rustc_hash::FxHashSet;
use tracing::trace;

use super::{
    assembler::Assembler,
    packet_crypto::{PrevCrypto, ZeroRttCrypto},
};
use crate::{
    connection::StreamsState,
    crypto::{KeyPair, Keys, PacketKey},
    frame,
    packet::SpaceId,
    range_set::ArrayRangeSet,
    shared::IssuedCid,
    Dir, Duration, Instant, SocketAddr, StreamId, TransportError, VarInt,
};

/// Packet-number and recovery state shared by every encryption level.
pub(super) struct PacketNumberSpace<S> {
    pub(super) dedup: Dedup,
    /// Highest received packet number
    pub(super) rx_packet: u64,
    /// Packet numbers to acknowledge
    pub(super) pending_acks: PendingAcks,

    /// The packet number of the next packet that will be sent, if any.
    pub(super) next_packet_number: u64,
    /// The highest-numbered ACK-eliciting packet we've sent
    pub(super) largest_ack_eliciting_sent: u64,
    /// Number of packets in `sent_packets` with numbers above `largest_ack_eliciting_sent`
    pub(super) unacked_non_ack_eliciting_tail: u64,
    /// Number of explicit congestion notification codepoints seen on incoming packets
    pub(super) ecn_counters: frame::EcnCounts,
    pub(super) ping_pending: bool,
    pub(super) loss: Loss<S>,
}

impl<S> PacketNumberSpace<S> {
    pub(super) fn new() -> Self {
        Self {
            dedup: Dedup::new(),
            rx_packet: 0,
            pending_acks: PendingAcks::new(),
            next_packet_number: 0,
            largest_ack_eliciting_sent: 0,
            unacked_non_ack_eliciting_tail: 0,
            ecn_counters: frame::EcnCounts::ZERO,
            ping_pending: false,
            loss: Loss::new(),
        }
    }

    /// Get the next outgoing packet number in this space.
    ///
    /// The caller owns the concrete crypto-space key-count update.
    pub(super) fn get_tx_number(&mut self) -> u64 {
        // TODO: Handle packet number overflow gracefully
        assert!(self.next_packet_number < 2u64.pow(62));
        let x = self.next_packet_number;
        self.next_packet_number += 1;
        x
    }

    /// Verifies sanity of an ECN block and returns whether congestion was encountered.
    pub(super) fn detect_ecn(
        &mut self,
        newly_acked: u64,
        ecn: frame::EcnCounts,
    ) -> Result<bool, &'static str> {
        let ect0_increase = ecn
            .ect0
            .checked_sub(self.loss.ecn_feedback.ect0)
            .ok_or("peer ECT(0) count regression")?;
        let ect1_increase = ecn
            .ect1
            .checked_sub(self.loss.ecn_feedback.ect1)
            .ok_or("peer ECT(1) count regression")?;
        let ce_increase = ecn
            .ce
            .checked_sub(self.loss.ecn_feedback.ce)
            .ok_or("peer CE count regression")?;
        let total_increase = ect0_increase + ect1_increase + ce_increase;
        if total_increase < newly_acked {
            return Err("ECN bleaching");
        }
        if (ect0_increase + ce_increase) < newly_acked || ect1_increase != 0 {
            return Err("ECN corruption");
        }
        self.loss.ecn_feedback = ecn;
        Ok(ce_increase != 0)
    }

    /// Stop tracking sent packet `number`, and return what we knew about it
    pub(super) fn take(&mut self, number: u64) -> Option<SentPacket<S>> {
        let packet = self.loss.sent_packets.remove(&number)?;
        if !packet.ack_eliciting && number > self.largest_ack_eliciting_sent {
            self.unacked_non_ack_eliciting_tail =
                self.unacked_non_ack_eliciting_tail.checked_sub(1).unwrap();
        }
        Some(packet)
    }

    /// May return a packet that should be forgotten
    pub(super) fn sent(&mut self, number: u64, packet: SentPacket<S>) -> Option<SentPacket<S>> {
        // Retain state for at most this many non-ACK-eliciting packets sent after the most recently
        // sent ACK-eliciting packet.
        const MAX_UNACKED_NON_ACK_ELICTING_TAIL: u64 = 1_000;

        let mut forgotten = None;
        if packet.ack_eliciting {
            self.unacked_non_ack_eliciting_tail = 0;
            self.largest_ack_eliciting_sent = number;
        } else if self.unacked_non_ack_eliciting_tail > MAX_UNACKED_NON_ACK_ELICTING_TAIL {
            let oldest_after_ack_eliciting = *self
                .loss
                .sent_packets
                .range((
                    Bound::Excluded(self.largest_ack_eliciting_sent),
                    Bound::Unbounded,
                ))
                .next()
                .unwrap()
                .0;
            let packet = self
                .loss
                .sent_packets
                .remove(&oldest_after_ack_eliciting)
                .unwrap();
            debug_assert!(!packet.ack_eliciting);
            forgotten = Some(packet);
        } else {
            self.unacked_non_ack_eliciting_tail += 1;
        }

        self.loss.sent_packets.insert(number, packet);
        forgotten
    }

    /// Whether any congestion-controlled packets in this space are not yet acknowledged or lost
    pub(super) fn has_in_flight(&self) -> bool {
        self.loss.sent_packets.values().any(|x| x.size != 0)
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn collect_lost_packets(
        &mut self,
        now: Instant,
        due_to_ack: bool,
        loss_delay: Duration,
        packet_threshold: u64,
        in_flight_mtu_probe: Option<u64>,
        congestion_period: Duration,
        first_packet_after_rtt_sample: Option<(SpaceId, u64)>,
        space_id: SpaceId,
    ) -> (Vec<u64>, Option<u64>, u64, bool) {
        let lost_send_time = now.checked_sub(loss_delay).unwrap();
        let largest_acked_packet = self.loss.largest_acked_packet.unwrap();
        let mut lost_packets = Vec::new();
        let mut lost_mtu_probe = None;
        let mut size_of_lost_packets = 0u64;
        let mut persistent_congestion_start = None;
        let mut prev_packet = None;
        let mut in_persistent_congestion = false;

        self.loss.loss_time = None;

        for (&packet, info) in self.loss.sent_packets.range(0..largest_acked_packet) {
            if prev_packet != Some(packet.wrapping_sub(1)) {
                persistent_congestion_start = None;
            }

            if info.time_sent <= lost_send_time || largest_acked_packet >= packet + packet_threshold
            {
                if Some(packet) == in_flight_mtu_probe {
                    lost_mtu_probe = in_flight_mtu_probe;
                } else {
                    lost_packets.push(packet);
                    size_of_lost_packets += info.size as u64;
                    if info.ack_eliciting && due_to_ack {
                        match persistent_congestion_start {
                            Some(start) if info.time_sent - start > congestion_period => {
                                in_persistent_congestion = true;
                            }
                            None if first_packet_after_rtt_sample
                                .is_some_and(|x| x < (space_id, packet)) =>
                            {
                                persistent_congestion_start = Some(info.time_sent);
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                let next_loss_time = info.time_sent + loss_delay;
                self.loss.loss_time = Some(
                    self.loss
                        .loss_time
                        .map_or(next_loss_time, |x| cmp::min(x, next_loss_time)),
                );
                persistent_congestion_start = None;
            }

            prev_packet = Some(packet);
        }

        (
            lost_packets,
            lost_mtu_probe,
            size_of_lost_packets,
            in_persistent_congestion,
        )
    }
}

/// Loss and acknowledgment state for one concrete packet number space.
pub(super) struct Loss<S> {
    /// The largest packet number the remote peer acknowledged in an ACK frame.
    pub(super) largest_acked_packet: Option<u64>,
    /// Transmitted but not acked.
    pub(super) sent_packets: BTreeMap<u64, SentPacket<S>>,
    /// Recent ECN counters sent by the peer in ACK frames.
    pub(super) ecn_feedback: frame::EcnCounts,
    /// The time the most recently sent retransmittable packet was sent.
    pub(super) time_of_last_ack_eliciting_packet: Option<Instant>,
    /// The time at which the earliest sent packet in this space will be considered lost.
    pub(super) loss_time: Option<Instant>,
    /// Number of tail loss probes to send
    pub(super) loss_probes: u32,
}

impl<S> Loss<S> {
    fn new() -> Self {
        Self {
            largest_acked_packet: None,
            sent_packets: BTreeMap::new(),
            ecn_feedback: frame::EcnCounts::ZERO,
            time_of_last_ack_eliciting_packet: None,
            loss_time: None,
            loss_probes: 0,
        }
    }
}

/// Initial or Handshake packet space.
pub(super) struct HandshakeSpace {
    pub(super) packets: PacketNumberSpace<Option<Box<VecDeque<frame::Crypto>>>>,
    pub(super) crypto: Keys,
    pub(super) sent_with_keys: u64,
    /// Incoming cryptographic handshake stream
    pub(super) crypto_stream: Assembler,
    /// Current offset of outgoing cryptographic handshake stream
    pub(super) crypto_offset: u64,
    pub(super) crypto_retransmits: VecDeque<frame::Crypto>,
}

impl HandshakeSpace {
    pub(super) fn new(crypto: Keys) -> Self {
        Self {
            packets: PacketNumberSpace::new(),
            crypto,
            sent_with_keys: 0,
            crypto_stream: Assembler::new(),
            crypto_offset: 0,
            crypto_retransmits: VecDeque::new(),
        }
    }

    pub(super) fn get_tx_number(&mut self) -> u64 {
        self.sent_with_keys += 1;
        self.packets.get_tx_number()
    }

    pub(super) fn can_send(&self) -> SendableFrames {
        SendableFrames {
            acks: self.packets.pending_acks.can_send(),
            other: !self.crypto_retransmits.is_empty() || self.packets.ping_pending,
        }
    }

    /// Queue data for a tail loss probe.
    pub(super) fn maybe_queue_probe(&mut self) {
        if self.packets.loss.loss_probes == 0 {
            return;
        }
        if !self.crypto_retransmits.is_empty() {
            return;
        }

        for packet in self.packets.loss.sent_packets.values_mut() {
            if let Some(frames) = packet.frames.as_mut() {
                if !frames.is_empty() {
                    let mut replacement = mem::take(frames);
                    self.crypto_retransmits.append(&mut replacement);
                    return;
                }
            }
        }

        self.packets.ping_pending = true;
    }
}

/// Application packet space.
pub(super) struct ApplicationSpace {
    pub(super) packets: PacketNumberSpace<ApplicationSentFrames>,
    pub(super) crypto: Option<Keys>,
    pub(super) zero_rtt_crypto: Option<ZeroRttCrypto>,
    pub(super) sent_with_keys: u64,
    pub(super) crypto_stream: Assembler,
    pub(super) crypto_offset: u64,
    pub(super) retransmits: ApplicationRetransmits,
    pub(super) immediate_ack_pending: bool,

    pub(super) key_phase: bool,
    pub(super) key_phase_size: u64,
    pub(super) prev_crypto: Option<PrevCrypto>,
    pub(super) next_crypto: Option<KeyPair<Box<dyn PacketKey>>>,
    pub(super) packet_number_filter: PacketNumberFilter,
}

impl ApplicationSpace {
    pub(super) fn new(rng: &mut (impl Rng + ?Sized)) -> Self {
        Self {
            packets: PacketNumberSpace::new(),
            crypto: None,
            zero_rtt_crypto: None,
            sent_with_keys: 0,
            crypto_stream: Assembler::new(),
            crypto_offset: 0,
            retransmits: ApplicationRetransmits::default(),
            immediate_ack_pending: false,
            key_phase: false,
            key_phase_size: rng.random_range(10..1000),
            prev_crypto: None,
            next_crypto: None,
            packet_number_filter: PacketNumberFilter::new(rng),
        }
    }

    pub(super) fn peek_tx_number(&self) -> u64 {
        self.packets.next_packet_number
    }

    pub(super) fn can_send(&self, streams: &StreamsState) -> SendableFrames {
        SendableFrames {
            acks: self.packets.pending_acks.can_send(),
            other: !self.retransmits.is_empty(streams)
                || self.packets.ping_pending
                || self.immediate_ack_pending,
        }
    }

    /// Queue data for a tail loss probe.
    pub(super) fn maybe_queue_probe(
        &mut self,
        request_immediate_ack: bool,
        streams: &StreamsState,
    ) {
        if self.packets.loss.loss_probes == 0 {
            return;
        }

        if request_immediate_ack {
            self.immediate_ack_pending = true;
        }

        if !self.retransmits.is_empty(streams) {
            return;
        }

        for packet in self.packets.loss.sent_packets.values_mut() {
            if let Some(frames) = packet.frames.retransmits.as_mut() {
                if !frames.is_empty(streams) {
                    self.retransmits |= mem::take(&mut **frames);
                    return;
                }
            }
        }

        if !self.immediate_ack_pending {
            self.packets.ping_pending = true;
        }
    }
}

/// Represents one or more packets subject to retransmission.
#[derive(Debug, Clone)]
pub(super) struct SentPacket<S> {
    /// [`PathData::generation`](super::PathData::generation) of the path on which this packet was sent
    pub(super) path_generation: u64,
    /// The time the packet was sent.
    pub(super) time_sent: Instant,
    /// The number of bytes sent in the packet.
    pub(super) size: u16,
    /// Whether an acknowledgement is expected directly in response to this packet.
    pub(super) ack_eliciting: bool,
    /// The largest packet number acknowledged by this packet
    pub(super) largest_acked: Option<u64>,
    /// Data which needs to be retransmitted in case the packet is lost.
    pub(super) frames: S,
}

/// Retransmittable Application-space data.
#[allow(unreachable_pub)] // fuzzing only
#[derive(Debug, Default, Clone)]
pub(super) struct ApplicationRetransmits {
    pub(super) max_data: bool,
    pub(super) max_stream_id: [bool; 2],
    pub(super) reset_stream: Vec<(StreamId, VarInt)>,
    pub(super) stop_sending: Vec<frame::StopSending>,
    pub(super) max_stream_data: FxHashSet<StreamId>,
    pub(super) crypto: VecDeque<frame::Crypto>,
    pub(super) new_cids: Vec<IssuedCid>,
    pub(super) retire_cids: Vec<u64>,
    pub(super) ack_frequency: bool,
    pub(super) handshake_done: bool,
    pub(super) new_tokens: Vec<SocketAddr>,
}

impl ApplicationRetransmits {
    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        !self.max_data
            && !self.max_stream_id.into_iter().any(|x| x)
            && self.reset_stream.is_empty()
            && self.stop_sending.is_empty()
            && self
                .max_stream_data
                .iter()
                .all(|&id| !streams.can_send_flow_control(id))
            && self.crypto.is_empty()
            && self.new_cids.is_empty()
            && self.retire_cids.is_empty()
            && !self.ack_frequency
            && !self.handshake_done
            && self.new_tokens.is_empty()
    }
}

impl ::std::ops::BitOrAssign for ApplicationRetransmits {
    fn bitor_assign(&mut self, rhs: Self) {
        self.max_data |= rhs.max_data;
        for dir in Dir::iter() {
            self.max_stream_id[dir as usize] |= rhs.max_stream_id[dir as usize];
        }
        self.reset_stream.extend_from_slice(&rhs.reset_stream);
        self.stop_sending.extend_from_slice(&rhs.stop_sending);
        self.max_stream_data.extend(&rhs.max_stream_data);
        for crypto in rhs.crypto.into_iter().rev() {
            self.crypto.push_front(crypto);
        }
        self.new_cids.extend(&rhs.new_cids);
        self.retire_cids.extend(rhs.retire_cids);
        self.ack_frequency |= rhs.ack_frequency;
        self.handshake_done |= rhs.handshake_done;
        self.new_tokens.extend_from_slice(&rhs.new_tokens);
    }
}

impl ::std::iter::FromIterator<Self> for ApplicationRetransmits {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Self>,
    {
        let mut result = Self::default();
        for packet in iter {
            result |= packet;
        }
        result
    }
}

/// Typed frame facts stored for an Application-space sent packet.
#[derive(Debug, Default, Clone)]
pub(super) struct ApplicationSentFrames {
    pub(super) retransmits: Option<Box<ApplicationRetransmits>>,
    pub(super) stream_frames: frame::StreamMetaVec,
}

impl ApplicationSentFrames {
    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        (match &self.retransmits {
            Some(retransmits) => retransmits.is_empty(streams),
            None => true,
        }) && self.stream_frames.is_empty()
    }

    pub(super) fn get_or_create(&mut self) -> &mut ApplicationRetransmits {
        if self.retransmits.is_none() {
            self.retransmits = Some(Box::default());
        }
        self.retransmits.as_deref_mut().unwrap()
    }
}

/// RFC4303-style sliding window packet number deduplicator.
pub(super) struct Dedup {
    window: Window,
    next: u64,
}

type Window = u128;

const WINDOW_SIZE: u64 = 1 + mem::size_of::<Window>() as u64 * 8;

impl Dedup {
    pub(super) fn new() -> Self {
        Self { window: 0, next: 0 }
    }

    fn highest(&self) -> u64 {
        self.next - 1
    }

    pub(super) fn insert(&mut self, packet: u64) -> bool {
        if let Some(diff) = packet.checked_sub(self.next) {
            self.window = ((self.window << 1) | 1)
                .checked_shl(cmp::min(diff, u64::from(u32::MAX)) as u32)
                .unwrap_or(0);
            self.next = packet + 1;
            false
        } else if self.highest() - packet < WINDOW_SIZE {
            if let Some(bit) = (self.highest() - packet).checked_sub(1) {
                let mask = 1 << bit;
                let duplicate = self.window & mask != 0;
                self.window |= mask;
                duplicate
            } else {
                true
            }
        } else {
            true
        }
    }

    fn smallest_missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> Option<u64> {
        debug_assert!(lower_bound <= upper_bound);
        debug_assert!(upper_bound <= self.highest());
        const BITFIELD_SIZE: u64 = (mem::size_of::<Window>() * 8) as u64;

        let lower_bound = lower_bound + 1;
        let upper_bound = upper_bound.saturating_sub(1);
        let start_offset = (self.highest() - upper_bound).max(1) - 1;
        if start_offset >= BITFIELD_SIZE {
            return None;
        }

        let end_offset_exclusive = self.highest().saturating_sub(lower_bound);
        let range_len = end_offset_exclusive
            .saturating_sub(start_offset)
            .min(BITFIELD_SIZE);
        if range_len == 0 {
            return None;
        }

        let mask = if range_len == BITFIELD_SIZE {
            u128::MAX
        } else {
            ((1u128 << range_len) - 1) << start_offset
        };
        let gaps = !self.window & mask;
        let smallest_missing_offset = 128 - gaps.leading_zeros() as u64;
        let smallest_missing_packet = self.highest() - smallest_missing_offset;

        if smallest_missing_packet <= upper_bound {
            Some(smallest_missing_packet)
        } else {
            None
        }
    }

    fn missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> bool {
        self.smallest_missing_in_interval(lower_bound, upper_bound)
            .is_some()
    }
}

/// Indicates which data is available for sending
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct SendableFrames {
    pub(super) acks: bool,
    pub(super) other: bool,
}

impl SendableFrames {
    pub(super) fn empty() -> Self {
        Self {
            acks: false,
            other: false,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.acks && !self.other
    }
}

#[derive(Debug)]
pub(super) struct PendingAcks {
    immediate_ack_required: bool,
    ack_eliciting_since_last_ack_sent: u64,
    non_ack_eliciting_since_last_ack_sent: u64,
    ack_eliciting_threshold: u64,
    reordering_threshold: u64,
    earliest_ack_eliciting_since_last_ack_sent: Option<Instant>,
    ranges: ArrayRangeSet,
    largest_packet: Option<(u64, Instant)>,
    largest_ack_eliciting_packet: Option<u64>,
    largest_acked: Option<u64>,
}

impl PendingAcks {
    fn new() -> Self {
        Self {
            immediate_ack_required: false,
            ack_eliciting_since_last_ack_sent: 0,
            non_ack_eliciting_since_last_ack_sent: 0,
            ack_eliciting_threshold: 1,
            reordering_threshold: 1,
            earliest_ack_eliciting_since_last_ack_sent: None,
            ranges: ArrayRangeSet::default(),
            largest_packet: None,
            largest_ack_eliciting_packet: None,
            largest_acked: None,
        }
    }

    pub(super) fn set_ack_frequency_params(&mut self, frame: &frame::AckFrequency) {
        self.ack_eliciting_threshold = frame.ack_eliciting_threshold.into_inner();
        self.reordering_threshold = frame.reordering_threshold.into_inner();
    }

    pub(super) fn set_immediate_ack_required(&mut self) {
        self.immediate_ack_required = true;
    }

    pub(super) fn on_max_ack_delay_timeout(&mut self) {
        self.immediate_ack_required = self.ack_eliciting_since_last_ack_sent > 0;
    }

    pub(super) fn max_ack_delay_timeout(&self, max_ack_delay: Duration) -> Option<Instant> {
        self.earliest_ack_eliciting_since_last_ack_sent
            .map(|earliest_unacked| earliest_unacked + max_ack_delay)
    }

    pub(super) fn can_send(&self) -> bool {
        self.immediate_ack_required && !self.ranges.is_empty()
    }

    pub(super) fn ack_delay(&self, now: Instant) -> Duration {
        self.largest_packet
            .map_or(Duration::default(), |(_, received)| now - received)
    }

    pub(super) fn packet_received(
        &mut self,
        now: Instant,
        packet_number: u64,
        ack_eliciting: bool,
        dedup: &Dedup,
    ) -> bool {
        if !ack_eliciting {
            self.non_ack_eliciting_since_last_ack_sent += 1;
            return false;
        }

        let prev_largest_ack_eliciting = self.largest_ack_eliciting_packet.unwrap_or(0);
        self.largest_ack_eliciting_packet = self
            .largest_ack_eliciting_packet
            .map(|pn| pn.max(packet_number))
            .or(Some(packet_number));

        self.ack_eliciting_since_last_ack_sent += 1;
        self.immediate_ack_required |=
            self.ack_eliciting_since_last_ack_sent > self.ack_eliciting_threshold;
        self.immediate_ack_required |=
            self.is_out_of_order(packet_number, prev_largest_ack_eliciting, dedup);

        if self.earliest_ack_eliciting_since_last_ack_sent.is_none() && !self.can_send() {
            self.earliest_ack_eliciting_since_last_ack_sent = Some(now);
            return true;
        }

        false
    }

    fn is_out_of_order(
        &self,
        packet_number: u64,
        prev_largest_ack_eliciting: u64,
        dedup: &Dedup,
    ) -> bool {
        match self.reordering_threshold {
            0 => false,
            1 => {
                packet_number < prev_largest_ack_eliciting
                    || dedup.missing_in_interval(prev_largest_ack_eliciting, packet_number)
            }
            _ => {
                let Some((largest_acked, largest_unacked)) =
                    self.largest_acked.zip(self.largest_ack_eliciting_packet)
                else {
                    return false;
                };
                if self.reordering_threshold > largest_acked {
                    return false;
                }
                let largest_reported = largest_acked - self.reordering_threshold + 1;
                let Some(smallest_missing_unreported) =
                    dedup.smallest_missing_in_interval(largest_reported, largest_unacked)
                else {
                    return false;
                };
                largest_unacked - smallest_missing_unreported >= self.reordering_threshold
            }
        }
    }

    pub(super) fn acks_sent(&mut self) {
        self.immediate_ack_required = false;
        self.ack_eliciting_since_last_ack_sent = 0;
        self.non_ack_eliciting_since_last_ack_sent = 0;
        self.earliest_ack_eliciting_since_last_ack_sent = None;
        self.largest_acked = self.largest_ack_eliciting_packet;
    }

    pub(super) fn insert_one(&mut self, packet: u64, now: Instant) {
        self.ranges.insert_one(packet);
        if self.largest_packet.map_or(true, |(pn, _)| packet > pn) {
            self.largest_packet = Some((packet, now));
        }
        if self.ranges.len() > MAX_ACK_BLOCKS {
            self.ranges.pop_min();
        }
    }

    pub(super) fn subtract_below(&mut self, max: u64) {
        self.ranges.remove(0..(max + 1));
    }

    pub(super) fn ranges(&self) -> &ArrayRangeSet {
        &self.ranges
    }

    pub(super) fn maybe_ack_non_eliciting(&mut self) {
        const LAZY_ACK_THRESHOLD: u64 = 10;
        if self.non_ack_eliciting_since_last_ack_sent > LAZY_ACK_THRESHOLD {
            self.immediate_ack_required = true;
        }
    }
}

pub(super) struct PacketNumberFilter {
    next_skipped_packet_number: u64,
    prev_skipped_packet_number: Option<u64>,
    exponent: u32,
}

impl PacketNumberFilter {
    pub(super) fn new(rng: &mut (impl Rng + ?Sized)) -> Self {
        let exponent = 6;
        Self {
            next_skipped_packet_number: rng.random_range(0..2u64.saturating_pow(exponent)),
            prev_skipped_packet_number: None,
            exponent,
        }
    }

    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self {
            next_skipped_packet_number: u64::MAX,
            prev_skipped_packet_number: None,
            exponent: u32::MAX,
        }
    }

    pub(super) fn peek(&self, space: &ApplicationSpace) -> u64 {
        let n = space.peek_tx_number();
        if n != self.next_skipped_packet_number {
            return n;
        }
        n + 1
    }

    pub(super) fn allocate(
        &mut self,
        rng: &mut (impl Rng + ?Sized),
        space: &mut PacketNumberSpace<ApplicationSentFrames>,
        sent_with_keys: &mut u64,
    ) -> u64 {
        let n = space.get_tx_number();
        *sent_with_keys += 1;
        if n != self.next_skipped_packet_number {
            return n;
        }

        trace!("skipping pn {n}");
        self.prev_skipped_packet_number = Some(self.next_skipped_packet_number);
        let next_exponent = self.exponent.saturating_add(1);
        self.next_skipped_packet_number = rng
            .random_range(2u64.saturating_pow(self.exponent)..2u64.saturating_pow(next_exponent));
        self.exponent = next_exponent;

        let skipped = space.get_tx_number();
        *sent_with_keys += 1;
        skipped
    }

    pub(super) fn check_ack(
        &self,
        space_id: SpaceId,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<(), TransportError> {
        if space_id == SpaceId::Data
            && self
                .prev_skipped_packet_number
                .is_some_and(|x| range.contains(&x))
        {
            return Err(TransportError::PROTOCOL_VIOLATION("unsent packet acked"));
        }
        Ok(())
    }
}

const MAX_ACK_BLOCKS: usize = 64;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sanity() {
        let mut dedup = Dedup::new();
        assert!(!dedup.insert(0));
        assert_eq!(dedup.next, 1);
        assert_eq!(dedup.window, 0b1);
        assert!(dedup.insert(0));
        assert_eq!(dedup.next, 1);
        assert_eq!(dedup.window, 0b1);
        assert!(!dedup.insert(1));
        assert_eq!(dedup.next, 2);
        assert_eq!(dedup.window, 0b11);
        assert!(!dedup.insert(2));
        assert_eq!(dedup.next, 3);
        assert_eq!(dedup.window, 0b111);
        assert!(!dedup.insert(4));
        assert_eq!(dedup.next, 5);
        assert_eq!(dedup.window, 0b11110);
        assert!(!dedup.insert(7));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_0100);
        assert!(dedup.insert(4));
        assert!(!dedup.insert(3));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1100);
        assert!(!dedup.insert(6));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1101);
        assert!(!dedup.insert(5));
        assert_eq!(dedup.next, 8);
        assert_eq!(dedup.window, 0b1111_1111);
    }

    #[test]
    fn happypath() {
        let mut dedup = Dedup::new();
        for i in 0..(2 * WINDOW_SIZE) {
            assert!(!dedup.insert(i));
            for j in 0..=i {
                assert!(dedup.insert(j));
            }
        }
    }

    #[test]
    fn jump() {
        let mut dedup = Dedup::new();
        dedup.insert(2 * WINDOW_SIZE);
        assert!(dedup.insert(WINDOW_SIZE));
        assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
        assert_eq!(dedup.window, 0);
        assert!(!dedup.insert(WINDOW_SIZE + 1));
        assert_eq!(dedup.next, 2 * WINDOW_SIZE + 1);
        assert_eq!(dedup.window, 1 << (WINDOW_SIZE - 2));
    }

    #[test]
    fn dedup_has_missing() {
        let mut dedup = Dedup::new();
        dedup.insert(0);
        assert!(!dedup.missing_in_interval(0, 0));
        dedup.insert(1);
        assert!(!dedup.missing_in_interval(0, 1));
        dedup.insert(3);
        assert!(dedup.missing_in_interval(1, 3));
        dedup.insert(4);
        assert!(!dedup.missing_in_interval(3, 4));
        assert!(dedup.missing_in_interval(0, 4));
        dedup.insert(2);
        assert!(!dedup.missing_in_interval(0, 4));
    }

    #[test]
    fn dedup_outside_of_window_has_missing() {
        let mut dedup = Dedup::new();
        for i in 0..140 {
            dedup.insert(i);
        }
        assert!(!dedup.missing_in_interval(0, 4));
        dedup.insert(160);
        assert!(!dedup.missing_in_interval(0, 4));
        assert!(!dedup.missing_in_interval(0, 140));
        assert!(dedup.missing_in_interval(0, 160));
    }

    #[test]
    fn dedup_smallest_missing() {
        let mut dedup = Dedup::new();
        dedup.insert(0);
        assert_eq!(dedup.smallest_missing_in_interval(0, 0), None);
        dedup.insert(1);
        assert_eq!(dedup.smallest_missing_in_interval(0, 1), None);
        dedup.insert(5);
        dedup.insert(7);
        assert_eq!(dedup.smallest_missing_in_interval(0, 7), Some(2));
        assert_eq!(dedup.smallest_missing_in_interval(5, 7), Some(6));
        dedup.insert(2);
        assert_eq!(dedup.smallest_missing_in_interval(1, 7), Some(3));
        dedup.insert(170);
        dedup.insert(172);
        dedup.insert(300);
        assert_eq!(dedup.smallest_missing_in_interval(170, 172), None);
        dedup.insert(500);
        assert_eq!(dedup.smallest_missing_in_interval(0, 500), Some(372));
        assert_eq!(dedup.smallest_missing_in_interval(0, 373), Some(372));
        assert_eq!(dedup.smallest_missing_in_interval(0, 372), None);
    }

    #[test]
    fn pending_acks_first_packet_is_not_considered_reordered() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();
        dedup.insert(0);
        acks.packet_received(Instant::now(), 0, true, &dedup);
        assert!(!acks.immediate_ack_required);
    }

    #[test]
    fn pending_acks_after_immediate_ack_set() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();
        dedup.insert(0);
        let now = Instant::now();
        acks.insert_one(0, now);
        acks.packet_received(now, 0, true, &dedup);
        assert!(!acks.ranges.is_empty());
        assert!(!acks.can_send());
        acks.set_immediate_ack_required();
        assert!(acks.can_send());
    }

    #[test]
    fn pending_acks_ack_delay() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();
        let t1 = Instant::now();
        let t2 = t1 + Duration::from_millis(2);
        let t3 = t2 + Duration::from_millis(5);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(0));
        dedup.insert(0);
        acks.insert_one(0, t1);
        acks.packet_received(t1, 0, true, &dedup);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(2));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(7));
        dedup.insert(3);
        acks.insert_one(3, t2);
        acks.packet_received(t2, 3, true, &dedup);
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));
        dedup.insert(2);
        acks.insert_one(2, t3);
        acks.packet_received(t3, 2, true, &dedup);
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));
    }

    #[test]
    fn sent_packet_size() {
        assert!(size_of::<SentPacket<Option<Box<VecDeque<frame::Crypto>>>>>() <= 128);
        assert!(size_of::<SentPacket<ApplicationSentFrames>>() <= 128);
    }
}
