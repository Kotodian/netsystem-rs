use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quinn::congestion::{
    AckedPacketInfo, BbrConfig, Controller, ControllerFactory, ControllerMetrics, LostPacketInfo,
};
use quinn::{ClientConfig, TransportConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BbrProfile {
    Standard,
    Conservative,
    Aggressive,
}

impl BbrProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "" | "standard" => Ok(Self::Standard),
            "conservative" => Ok(Self::Conservative),
            "aggressive" => Ok(Self::Aggressive),
            _ => Err(format!("unsupported BBR profile: {value}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Conservative => "conservative",
            Self::Aggressive => "aggressive",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HysteriaBbrConfig {
    profile: BbrProfile,
    initial_mtu: u16,
    dynamic: Option<(CongestionControlHandle, bool)>,
}

impl HysteriaBbrConfig {
    pub fn new(profile: BbrProfile, initial_mtu: u16) -> Self {
        Self {
            profile,
            initial_mtu: initial_mtu.max(1200),
            dynamic: None,
        }
    }

    pub fn new_with_handle(
        profile: BbrProfile,
        initial_mtu: u16,
        handle: CongestionControlHandle,
        brutal_debug: bool,
    ) -> Self {
        Self {
            profile,
            initial_mtu: initial_mtu.max(1200),
            dynamic: Some((handle, brutal_debug)),
        }
    }

    pub fn profile(&self) -> BbrProfile {
        self.profile
    }
}

impl ControllerFactory for HysteriaBbrConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        let mtu = current_mtu.max(self.initial_mtu).max(1200);
        if let Some((handle, brutal_debug)) = &self.dynamic {
            return Box::new(DynamicHysteriaController::new(
                mtu,
                handle.clone(),
                *brutal_debug,
            ));
        }
        let bbr_config: Arc<BbrConfig> = Arc::new(BbrConfig::default());
        bbr_config.build(now, mtu)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CongestionControlHandle {
    brutal_bps: Arc<AtomicU64>,
}

impl CongestionControlHandle {
    pub fn use_brutal(&self, bps: u64) {
        self.brutal_bps.store(bps, Ordering::SeqCst);
    }

    pub fn use_bbr(&self) {
        self.brutal_bps.store(0, Ordering::SeqCst);
    }

    fn brutal_bps(&self) -> u64 {
        self.brutal_bps.load(Ordering::SeqCst)
    }
}

/// Quinn's stock BBRv1 + our BrutalController, switched at runtime by the
/// hysteria2 auth response (server-advertised `rx_auto`). The `profile` knob
/// from the toml is currently advisory only — preserved on `HysteriaBbrConfig`
/// for forward compatibility but not consumed by the underlying BBR.
pub struct DynamicHysteriaController {
    bbr: Box<dyn Controller>,
    brutal: BrutalController,
    handle: CongestionControlHandle,
}

impl DynamicHysteriaController {
    fn new(mtu: u16, handle: CongestionControlHandle, brutal_debug: bool) -> Self {
        Self {
            bbr: build_quinn_bbr(mtu),
            brutal: BrutalController::new(mtu, brutal_debug),
            handle,
        }
    }

    fn using_brutal(&self) -> bool {
        self.handle.brutal_bps() > 0
    }
}

fn build_quinn_bbr(mtu: u16) -> Box<dyn Controller> {
    let config: Arc<BbrConfig> = Arc::new(BbrConfig::default());
    config.build(Instant::now(), mtu)
}

impl Controller for DynamicHysteriaController {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        if self.using_brutal() {
            self.brutal.on_sent(now, bytes, last_packet_number);
        } else {
            self.bbr.on_sent(now, bytes, last_packet_number);
        }
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn::RttEstimator,
    ) {
        if self.using_brutal() {
            self.brutal.on_ack(now, sent, bytes, app_limited, rtt);
        } else {
            self.bbr.on_ack(now, sent, bytes, app_limited, rtt);
        }
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        if self.using_brutal() {
            self.brutal
                .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
        } else {
            self.bbr
                .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
        }
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        if self.using_brutal() {
            self.brutal
                .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
        } else {
            self.bbr
                .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
        }
    }

    fn on_congestion_event_ex(
        &mut self,
        now: Instant,
        prior_in_flight: u64,
        acked: &[AckedPacketInfo],
        lost: &[LostPacketInfo],
        app_limited: bool,
        rtt: &quinn::RttEstimator,
        largest_packet_num_acked: Option<u64>,
        is_persistent_congestion: bool,
    ) -> bool {
        if self.using_brutal() {
            self.brutal.on_congestion_event_ex(
                now,
                prior_in_flight,
                acked,
                lost,
                app_limited,
                rtt,
                largest_packet_num_acked,
                is_persistent_congestion,
            )
        } else {
            self.bbr.on_congestion_event_ex(
                now,
                prior_in_flight,
                acked,
                lost,
                app_limited,
                rtt,
                largest_packet_num_acked,
                is_persistent_congestion,
            )
        }
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.bbr.on_mtu_update(new_mtu);
        self.brutal.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        let bps = self.handle.brutal_bps();
        if bps > 0 {
            self.brutal.window_for_bps(bps)
        } else {
            self.bbr.window()
        }
    }

    fn metrics(&self) -> ControllerMetrics {
        let bps = self.handle.brutal_bps();
        if bps > 0 {
            self.brutal.metrics_for_bps(bps)
        } else {
            self.bbr.metrics()
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            bbr: self.bbr.clone_box(),
            brutal: self.brutal.clone(),
            handle: self.handle.clone(),
        })
    }

    fn initial_window(&self) -> u64 {
        self.bbr.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Clone)]
struct BrutalController {
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
    fn new(mtu: u16, debug: bool) -> Self {
        Self {
            mtu: u64::from(mtu.max(1200)),
            ack_rate_milli: 1000,
            rtt: Duration::from_millis(100),
            start: Instant::now(),
            slots: [PacketSlot::default(); 5],
            debug,
        }
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

    fn window_for_bps(&self, bps: u64) -> u64 {
        let rtt_micros = self.rtt.as_micros().max(1) as u64;
        let window = bps
            .saturating_mul(rtt_micros)
            .saturating_mul(2)
            .saturating_mul(1000)
            / 1_000_000
            / self.ack_rate_milli.max(800);
        window.max(2 * self.mtu).max(10_240)
    }

    fn metrics_for_bps(&self, bps: u64) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.window_for_bps(bps);
        metrics.pacing_rate = Some(bps.saturating_mul(8));
        metrics
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
        // Mirrors the previous shared helper: 10 * MTU, clamped to a
        // minimum window of 14720 bytes, never below 2 * MTU.
        (10 * self.mtu).min((2 * self.mtu).max(14_720))
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub(crate) fn apply_transport_config_with_handle(
    config: &mut ClientConfig,
    transport: TransportConfig,
    profile: BbrProfile,
    initial_packet_size: u16,
    disable_path_mtu_discovery: bool,
    handle: CongestionControlHandle,
    brutal_debug: bool,
) {
    apply_transport_config_with_factory(
        config,
        transport,
        Arc::new(HysteriaBbrConfig::new_with_handle(
            profile,
            initial_packet_size.max(1200),
            handle,
            brutal_debug,
        )),
        initial_packet_size,
        disable_path_mtu_discovery,
    );
}

fn apply_transport_config_with_factory(
    config: &mut ClientConfig,
    mut transport: TransportConfig,
    factory: Arc<HysteriaBbrConfig>,
    initial_packet_size: u16,
    disable_path_mtu_discovery: bool,
) {
    let initial_mtu = initial_packet_size.max(1200);
    transport.initial_mtu(initial_mtu);
    if disable_path_mtu_discovery {
        transport.mtu_discovery_config(None);
    }
    transport.congestion_controller_factory(factory);
    config.transport_config(Arc::new(transport));
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
}
