use std::time::{Duration, Instant};

use hammer_core::protocol::tcp::TcpSackBlock;
use hammer_infra::vec::Vec;

use crate::transport::congestion::{
    AckedPacket, CongestionController, LostPacket, PacketNumber, RttSample,
};

const DEFAULT_RACK_TIMEOUT_TICKS: u64 = 6;
const DEFAULT_TLP_TIMEOUT_TICKS: u64 = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpSentSegment {
    pub packet_number: PacketNumber,
    pub sequence: u32,
    pub end_sequence: u32,
    pub bytes: u32,
    pub sent_at: Instant,
    pub retransmitted: bool,
    pub is_probe: bool,
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
    sent: Vec<TcpSentSegment>,
    rack_pending_loss: Vec<TcpSentSegment>,
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

    pub fn record_sent(&mut self, segment: TcpSentSegment) {
        self.sent.push(segment);
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
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut index = 0;
        while index < self.sent.len() {
            let segment = self.sent[index];
            if seq_before_or_equal(segment.end_sequence, ack.acknowledgment) {
                let segment = self.sent.remove(index);
                largest_acked = largest_acked.max(segment.packet_number);
                any_acked = true;
                deliver_acked_segment(self.bytes_in_flight(), ack, segment, congestion);
            } else {
                index += 1;
            }
        }
        if any_acked {
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        self.tlp_timer_armed = self.has_unacked_data();
    }

    pub fn on_sack_blocks<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        blocks: &[TcpSackBlock],
        congestion: &mut C,
    ) {
        self.on_ack(ack, congestion);
        let mut highest_sacked_right = ack.acknowledgment;
        for block in blocks {
            highest_sacked_right = highest_sacked_right.max(block.right_edge);
            self.ack_sack_block(ack, *block, congestion);
        }
        if highest_sacked_right != ack.acknowledgment {
            self.mark_rack_candidates(highest_sacked_right);
        }
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

    pub fn next_tlp_probe(&mut self) -> Option<TcpSentSegment> {
        let mut segment = *self.sent.iter().max_by_key(|segment| segment.sent_at)?;
        segment.is_probe = true;
        self.tlp_timer_armed = false;
        Some(segment)
    }

    fn ack_sack_block<C: CongestionController>(
        &mut self,
        ack: TcpRecoveryAck,
        block: TcpSackBlock,
        congestion: &mut C,
    ) {
        let mut largest_acked = 0;
        let mut any_acked = false;
        let mut index = 0;
        while index < self.sent.len() {
            let segment = self.sent[index];
            if seq_before_or_equal(block.left_edge, segment.sequence)
                && seq_before_or_equal(segment.end_sequence, block.right_edge)
            {
                let segment = self.sent.remove(index);
                largest_acked = largest_acked.max(segment.packet_number);
                any_acked = true;
                deliver_acked_segment(self.bytes_in_flight(), ack, segment, congestion);
            } else {
                index += 1;
            }
        }
        if any_acked {
            congestion.on_end_acks(
                ack.now,
                self.bytes_in_flight(),
                ack.app_limited,
                largest_acked,
            );
        }
        self.tlp_timer_armed = self.has_unacked_data();
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
}

impl Default for TcpSentSegment {
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
    segment: TcpSentSegment,
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
