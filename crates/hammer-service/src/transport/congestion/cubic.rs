use std::time::{Duration, Instant};

use super::controller::CongestionController;
use super::types::{AckedPacket, CongestionMetrics, LostPacket, PacketNumber, RttSample};

pub const DEFAULT_CUBIC_MAX_DATAGRAM_SIZE: u32 = 1_460;

const INITIAL_WINDOW_SEGMENTS: u32 = 10;
const MINIMUM_WINDOW_SEGMENTS: u32 = 2;
const CUBIC_C: f64 = 0.4;
const CUBIC_BETA: f64 = 0.7;

#[derive(Clone, Copy, Debug)]
pub struct CubicController {
    max_datagram_size: u32,
    congestion_window: u32,
    slow_start_threshold: u32,
    prior_maximum_window: f64,
    maximum_window: f64,
    origin_window: f64,
    epoch_start: Option<Instant>,
    k: f64,
    acknowledged_bytes: u32,
    delivered: u64,
    min_rtt: Option<Duration>,
    max_bandwidth_bytes_per_second: u64,
    pacing_rate_bytes_per_second: Option<u64>,
}

impl CubicController {
    fn reset_epoch(&mut self) {
        self.epoch_start = None;
        self.acknowledged_bytes = 0;
    }

    fn begin_epoch(&mut self, now: Instant) {
        if self.epoch_start.is_some() {
            return;
        }
        self.epoch_start = Some(now);
        let window_segments = self.window_segments();
        if self.maximum_window <= window_segments {
            self.k = 0.0;
            self.origin_window = window_segments;
        } else {
            self.k = ((self.maximum_window - window_segments) / CUBIC_C).cbrt();
            self.origin_window = self.maximum_window;
        }
    }

    fn window_segments(&self) -> f64 {
        f64::from(self.congestion_window) / f64::from(self.max_datagram_size.max(1))
    }

    fn minimum_window(&self) -> u32 {
        self.max_datagram_size
            .saturating_mul(MINIMUM_WINDOW_SEGMENTS)
    }

    fn update_bandwidth(&mut self, bytes: u32, rtt: Duration) {
        if bytes == 0 || rtt.is_zero() {
            return;
        }
        let sample = (u128::from(bytes) * 1_000_000u128 / rtt.as_micros().max(1))
            .min(u128::from(u64::MAX)) as u64;
        self.max_bandwidth_bytes_per_second = self.max_bandwidth_bytes_per_second.max(sample);
    }

    fn update_pacing_rate(&mut self) {
        let Some(rtt) = self.min_rtt else {
            self.pacing_rate_bytes_per_second = None;
            return;
        };
        let micros = rtt.as_micros().max(1);
        let rate = (u128::from(self.congestion_window) * 1_000_000u128 / micros)
            .clamp(1, u128::from(u64::MAX)) as u64;
        self.pacing_rate_bytes_per_second = Some(rate);
    }

    fn cubic_target(&self, now: Instant) -> u32 {
        let elapsed = self
            .epoch_start
            .map(|epoch| now.saturating_duration_since(epoch))
            .unwrap_or_default()
            + self.min_rtt.unwrap_or_default();
        let t = elapsed.as_secs_f64() - self.k;
        let target_segments = (self.origin_window + CUBIC_C * t * t * t).max(1.0);
        (target_segments * f64::from(self.max_datagram_size))
            .clamp(f64::from(self.minimum_window()), f64::from(u32::MAX)) as u32
    }
}

impl CongestionController for CubicController {
    fn new(max_datagram_size: u32) -> Self {
        let max_datagram_size = max_datagram_size.max(1);
        Self {
            max_datagram_size,
            congestion_window: max_datagram_size.saturating_mul(INITIAL_WINDOW_SEGMENTS),
            slow_start_threshold: u32::MAX,
            prior_maximum_window: 0.0,
            maximum_window: 0.0,
            origin_window: 0.0,
            epoch_start: None,
            k: 0.0,
            acknowledged_bytes: 0,
            delivered: 0,
            min_rtt: None,
            max_bandwidth_bytes_per_second: 0,
            pacing_rate_bytes_per_second: None,
        }
    }

    fn metrics(&self) -> CongestionMetrics {
        CongestionMetrics {
            congestion_window: self.congestion_window(),
            pacing_rate_bytes_per_second: self.pacing_rate_bytes_per_second,
            delivered: self.delivered,
            max_bandwidth_bytes_per_second: self.max_bandwidth_bytes_per_second,
            min_rtt: self.min_rtt,
        }
    }

    fn max_datagram_size(&self) -> u32 {
        self.max_datagram_size
    }

    fn congestion_window(&self) -> u32 {
        self.congestion_window.max(self.minimum_window())
    }

    fn pacing_rate_bytes_per_second(&self) -> Option<u64> {
        self.pacing_rate_bytes_per_second
    }

    fn delivered(&self) -> u64 {
        self.delivered
    }

    fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    fn max_bandwidth_bytes_per_second(&self) -> u64 {
        self.max_bandwidth_bytes_per_second
    }

    fn on_packet_sent(&mut self, _: PacketNumber, _: u32, _: u32, _: Instant) {}

    fn on_ack(&mut self, now: Instant, acked: AckedPacket, rtt: RttSample, _: u32) {
        if acked.bytes == 0 {
            return;
        }
        self.delivered = self.delivered.saturating_add(u64::from(acked.bytes));
        if !rtt.latest.is_zero() {
            self.min_rtt = Some(
                self.min_rtt
                    .map_or(rtt.latest, |value| value.min(rtt.latest)),
            );
            self.update_bandwidth(acked.bytes, rtt.latest);
        }
        if self.congestion_window < self.slow_start_threshold {
            self.congestion_window = self.congestion_window.saturating_add(acked.bytes);
            self.update_pacing_rate();
            return;
        }

        self.begin_epoch(now);
        self.acknowledged_bytes = self.acknowledged_bytes.saturating_add(acked.bytes);
        let target = self.cubic_target(now);
        let reno_increment = u64::from(self.max_datagram_size)
            .saturating_mul(u64::from(acked.bytes))
            / u64::from(self.congestion_window.max(1));
        let cubic_increment = target
            .saturating_sub(self.congestion_window)
            .saturating_mul(acked.bytes)
            / self.congestion_window.max(1);
        let increment = cubic_increment.max(reno_increment as u32).max(1);
        self.congestion_window = self.congestion_window.saturating_add(increment);
        self.update_pacing_rate();
    }

    fn on_end_acks(&mut self, _: Instant, _: u32, _: bool, _: PacketNumber) {}

    fn on_loss(&mut self, _: Instant, lost: LostPacket, _: bool) {
        if lost.bytes == 0 {
            return;
        }
        let window_segments = self.window_segments();
        self.prior_maximum_window = self.maximum_window;
        self.maximum_window = if window_segments < self.prior_maximum_window {
            window_segments * (1.0 + CUBIC_BETA) / 2.0
        } else {
            window_segments
        };
        self.congestion_window = (f64::from(self.congestion_window) * CUBIC_BETA)
            .max(f64::from(self.minimum_window())) as u32;
        self.slow_start_threshold = self.congestion_window;
        self.reset_epoch();
        self.update_pacing_rate();
    }

    fn on_mtu_update(&mut self, max_datagram_size: u32) {
        self.max_datagram_size = max_datagram_size.max(1);
        self.congestion_window = self.congestion_window.max(self.minimum_window());
        self.reset_epoch();
        self.update_pacing_rate();
    }

    fn next_send_delay(&self, pending_bytes: u32) -> Option<Duration> {
        let rate = self.pacing_rate_bytes_per_second?;
        if pending_bytes == 0 || rate == 0 {
            return None;
        }
        let nanos = (u128::from(pending_bytes) * 1_000_000_000u128)
            .div_ceil(u128::from(rate))
            .clamp(1, u128::from(u64::MAX)) as u64;
        Some(Duration::from_nanos(nanos))
    }
}

impl Default for CubicController {
    fn default() -> Self {
        Self::new(DEFAULT_CUBIC_MAX_DATAGRAM_SIZE)
    }
}
