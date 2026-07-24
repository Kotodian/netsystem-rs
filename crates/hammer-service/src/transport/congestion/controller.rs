use std::time::{Duration, Instant};

use super::types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};

pub trait CongestionController: Clone + core::fmt::Debug + Send + Sync + 'static {
    fn new(max_datagram_size: u32) -> Self
    where
        Self: Sized;

    fn metrics(&self) -> CongestionMetrics;
    fn max_datagram_size(&self) -> u32;
    fn congestion_window(&self) -> u32;
    fn pacing_rate_bytes_per_second(&self) -> Option<u64>;
    fn delivered(&self) -> u64;
    fn min_rtt(&self) -> Option<Duration>;
    fn max_bandwidth_bytes_per_second(&self) -> u64;

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    );

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, bytes_in_flight: u32);

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    );

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool);
    fn on_mtu_update(&mut self, max_datagram_size: u32);
    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration>;
}
