use std::any::Any;
use std::time::{Duration, Instant};

use quinn::congestion::{AckedPacketInfo, Controller, ControllerMetrics, LostPacketInfo};

#[derive(Debug, Clone)]
pub(crate) struct BrutalController {
    mtu: u64,
    ack_rate_milli: u64,
    rtt: Duration,
    start: Instant,
    slots: [PacketSlot; 5],
    debug: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct PacketSlot {
    second: u64,
    acked: u64,
    lost: u64,
}

impl BrutalController {
    pub(crate) fn new(mtu: u16, debug: bool) -> Self {
        Self {
            mtu: u64::from(mtu.max(1200)),
            ack_rate_milli: 1000,
            rtt: Duration::from_millis(100),
            start: Instant::now(),
            slots: [PacketSlot::default(); 5],
            debug,
        }
    }

    pub(crate) fn window_for_bps(&self, bps: u64) -> u64 {
        let rtt_micros = self.rtt.as_micros().max(1) as u64;
        let window = bps
            .saturating_mul(rtt_micros)
            .saturating_mul(2)
            .saturating_mul(1000)
            / 1_000_000
            / self.ack_rate_milli.max(800);
        window.max(2 * self.mtu).max(10_240)
    }

    pub(crate) fn metrics_for_bps(&self, bps: u64) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.window_for_bps(bps);
        metrics.pacing_rate = Some(bps.saturating_mul(8));
        metrics
    }

    fn record_packets(&mut self, now: Instant, acked: usize, lost: usize) {
        let second = now.saturating_duration_since(self.start).as_secs();
        let slot = (second % self.slots.len() as u64) as usize;
        if self.slots[slot].second != second {
            self.slots[slot] = PacketSlot {
                second,
                acked: 0,
                lost: 0,
            };
        }
        self.slots[slot].acked += acked as u64;
        self.slots[slot].lost += lost as u64;
        let min_second = second.saturating_sub(self.slots.len() as u64);
        let (acked, lost) = self
            .slots
            .iter()
            .filter(|slot| slot.second >= min_second)
            .fold((0_u64, 0_u64), |(a, l), slot| {
                (a + slot.acked, l + slot.lost)
            });
        if acked + lost >= 50 {
            self.ack_rate_milli = ((acked * 1000) / (acked + lost)).max(800);
        }
    }
}

impl Controller for BrutalController {
    fn on_sent(&mut self, _now: Instant, _bytes: u64, _last_packet_number: u64) {}

    fn on_ack(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _bytes: u64,
        _app_limited: bool,
        rtt: &quinn::RttEstimator,
    ) {
        self.rtt = rtt.get();
    }

    fn on_end_acks(
        &mut self,
        _now: Instant,
        _in_flight: u64,
        _app_limited: bool,
        _largest_packet_num_acked: Option<u64>,
    ) {
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
    }

    fn on_congestion_event_ex(
        &mut self,
        now: Instant,
        _prior_in_flight: u64,
        acked: &[AckedPacketInfo],
        lost: &[LostPacketInfo],
        _app_limited: bool,
        rtt: &quinn::RttEstimator,
        _largest_packet_num_acked: Option<u64>,
        _is_persistent_congestion: bool,
    ) -> bool {
        self.rtt = rtt.get();
        self.record_packets(now, acked.len(), lost.len());
        let _ = self.debug;
        true
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = u64::from(new_mtu.max(1200));
    }

    fn window(&self) -> u64 {
        self.window_for_bps(0)
    }

    fn metrics(&self) -> ControllerMetrics {
        self.metrics_for_bps(0)
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        (10 * self.mtu).max((2 * self.mtu).max(14_720))
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brutal_controller_uses_sliding_window_relative_to_first_sample() {
        let mut controller = BrutalController::new(1200, false);
        let start = controller.start;

        controller.record_packets(start, 25, 25);
        assert_eq!(controller.ack_rate_milli, 800);

        controller.record_packets(start + Duration::from_secs(6), 50, 0);
        assert_eq!(controller.ack_rate_milli, 1000);
    }

    #[test]
    fn brutal_controller_initial_window_respects_minimum_window() {
        let controller = BrutalController::new(1200, false);

        assert_eq!(controller.initial_window(), 14_720);
    }
}
