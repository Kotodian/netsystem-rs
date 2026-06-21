use std::time::{Duration, Instant};

pub type PacketNumber = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckedPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
    pub sent_at: Instant,
    pub app_limited: bool,
    pub ecn_ce_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LostPacket {
    pub packet_number: PacketNumber,
    pub bytes: u32,
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
