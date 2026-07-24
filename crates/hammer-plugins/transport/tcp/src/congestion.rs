use std::time::{Duration, Instant};

use hammer_service::transport::congestion::{
    AckedPacket, BbrController, CongestionController, CongestionMetrics, LostPacket, PacketNumber,
    RttSample,
};

use crate::config::CongestionAlgorithm;

#[derive(Debug, Clone)]
pub(crate) enum Controller {
    Bbr(BbrController),
}

impl CongestionController for Controller {
    fn new(max_datagram_size: u32) -> Self {
        match crate::active_tcp_policy().congestion {
            CongestionAlgorithm::Bbr => Self::Bbr(BbrController::new(max_datagram_size)),
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        match self {
            Self::Bbr(controller) => controller.metrics(),
        }
    }

    fn max_datagram_size(&self) -> u32 {
        match self {
            Self::Bbr(controller) => controller.max_datagram_size(),
        }
    }

    fn congestion_window(&self) -> u32 {
        match self {
            Self::Bbr(controller) => controller.congestion_window(),
        }
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        match self {
            Self::Bbr(controller) => controller.pacing_rate_bytes_per_second(),
        }
    }

    fn delivered(&self) -> u64 {
        match self {
            Self::Bbr(controller) => controller.delivered(),
        }
    }

    fn min_rtt(&self) -> Option<Duration> {
        match self {
            Self::Bbr(controller) => controller.min_rtt(),
        }
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        match self {
            Self::Bbr(controller) => controller.max_bandwidth_bytes_per_second(),
        }
    }

    fn on_packet_sent(
        &mut self,
        packet_number: PacketNumber,
        bytes_sent: u32,
        bytes_in_flight: u32,
        now: Instant,
    ) {
        match self {
            Self::Bbr(controller) => {
                controller.on_packet_sent(packet_number, bytes_sent, bytes_in_flight, now);
            }
        }
    }

    fn on_ack(
        &mut self,
        now: Instant,
        acked: AckedPacket,
        rtt: RttSample,
        bytes_in_flight: u32,
    ) {
        match self {
            Self::Bbr(controller) => controller.on_ack(now, acked, rtt, bytes_in_flight),
        }
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        bytes_in_flight: u32,
        app_limited: bool,
        largest_acked_packet: PacketNumber,
    ) {
        match self {
            Self::Bbr(controller) => controller.on_end_acks(
                now,
                bytes_in_flight,
                app_limited,
                largest_acked_packet,
            ),
        }
    }

    fn on_loss(&mut self, now: Instant, lost: LostPacket, persistent_congestion: bool) {
        match self {
            Self::Bbr(controller) => controller.on_loss(now, lost, persistent_congestion),
        }
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        match self {
            Self::Bbr(controller) => controller.on_mtu_update(max_datagram_size),
        }
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        match self {
            Self::Bbr(controller) => controller.next_send_delay(pending_bytes),
        }
    }
}
