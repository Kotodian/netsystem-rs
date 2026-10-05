use std::time::{Duration, Instant};

pub type PacketNumber = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckedPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
    pub sent_at: Instant,
    /// Bytes delivered since the send-time delivery marker used by the
    /// delivery-rate sample.
    pub delivered: u64,
    /// Delivery counter captured when the selected send sample was created.
    pub prior_delivered: u64,
    /// VPP tcp_ack_ctx_t::interval_time for the selected send sample.
    pub interval: Duration,
    /// Bytes in flight immediately after the selected transmission.
    pub tx_in_flight: u64,
    /// Lifetime bytes marked lost when the selected transmission occurred.
    pub tx_lost: u64,
    /// Bytes marked lost since the selected transmission was sent.
    pub lost: u64,
    pub app_limited: bool,
    pub ecn_ce_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LostPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
    /// Bytes in flight at the loss callback, before retransmission output.
    pub bytes_in_flight: u32,
    /// Cumulative bytes marked lost through this loss sample.
    pub lost: u64,
    pub sent_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RttSample {
    pub latest: Duration,
    pub min: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CongestionMetrics {
    pub congestion_window: u32,
    pub pacing_rate_bytes_per_second: Option<u64>,
    pub delivered: u64,
    pub max_bandwidth_bytes_per_second: u64,
    pub min_rtt: Option<Duration>,
}
