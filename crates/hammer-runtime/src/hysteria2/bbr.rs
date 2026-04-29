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

    pub fn parameters(self) -> BbrProfileParameters {
        match self {
            Self::Standard => BbrProfileParameters {
                high_gain_milli: 2885,
                high_cwnd_gain_milli: 2885,
                cwnd_gain_milli: 2000,
                startup_growth_rounds: 3,
                bytes_lost_multiplier: 2,
                drain_to_target: false,
                detect_overshooting: false,
            },
            Self::Conservative => BbrProfileParameters {
                high_gain_milli: 2250,
                high_cwnd_gain_milli: 1750,
                cwnd_gain_milli: 1750,
                startup_growth_rounds: 2,
                bytes_lost_multiplier: 1,
                drain_to_target: true,
                detect_overshooting: true,
            },
            Self::Aggressive => BbrProfileParameters {
                high_gain_milli: 3000,
                high_cwnd_gain_milli: 2250,
                cwnd_gain_milli: 2500,
                startup_growth_rounds: 4,
                bytes_lost_multiplier: 2,
                drain_to_target: false,
                detect_overshooting: false,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BbrProfileParameters {
    pub high_gain_milli: u64,
    pub high_cwnd_gain_milli: u64,
    pub cwnd_gain_milli: u64,
    pub startup_growth_rounds: u64,
    pub bytes_lost_multiplier: u64,
    pub drain_to_target: bool,
    pub detect_overshooting: bool,
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
                self.profile,
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

pub struct DynamicHysteriaController {
    /// Heap-allocated controller for the BBR side. Was the in-tree
    /// `HysteriaBbr`; we switched to quinn's stock BBR after measuring our
    /// simplified estimator pin cwnd at the initial window (single-packet
    /// `bytes/rtt` bandwidth samples never let target_cwnd exceed one MTU).
    bbr: Box<dyn Controller>,
    brutal: BrutalController,
    handle: CongestionControlHandle,
    profile: BbrProfile,
}

impl DynamicHysteriaController {
    fn new(
        profile: BbrProfile,
        mtu: u16,
        handle: CongestionControlHandle,
        brutal_debug: bool,
    ) -> Self {
        Self {
            bbr: build_quinn_bbr(mtu),
            brutal: BrutalController::new(mtu, brutal_debug),
            handle,
            profile,
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
            profile: self.profile,
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
            slots: [PacketSlot::default(); 5],
            debug,
        }
    }

    fn record_packets(&mut self, now: Instant, acked: usize, lost: usize) {
        let second = now.elapsed().as_secs();
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
        initial_window(self.mtu)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Clone)]
pub struct HysteriaBbr {
    profile: BbrProfile,
    params: BbrProfileParameters,
    mtu: u64,
    cwnd: u64,
    pacing_rate: Option<u64>,
    max_bandwidth: u64,
    min_rtt: Option<Duration>,
    round_count: u64,
    round_end_packet: u64,
    max_sent_packet: u64,
    full_bandwidth: u64,
    rounds_without_growth: u64,
    mode: BbrMode,
}

impl HysteriaBbr {
    pub fn new(profile: BbrProfile, mtu: u16) -> Self {
        let mtu = u64::from(mtu.max(1200));
        let cwnd = initial_window(mtu);
        Self {
            profile,
            params: profile.parameters(),
            mtu,
            cwnd,
            pacing_rate: None,
            max_bandwidth: 0,
            min_rtt: None,
            round_count: 0,
            round_end_packet: 0,
            max_sent_packet: 0,
            full_bandwidth: 0,
            rounds_without_growth: 0,
            mode: BbrMode::Startup,
        }
    }

    pub fn profile(&self) -> BbrProfile {
        self.profile
    }

    fn on_packets_acked(&mut self, now: Instant, acked: &[AckedPacketInfo]) -> u64 {
        let mut bytes_acked = 0;
        for packet in acked {
            bytes_acked += packet.bytes;
            let rtt = now.saturating_duration_since(packet.sent);
            if !rtt.is_zero() {
                self.min_rtt = Some(self.min_rtt.map_or(rtt, |min| min.min(rtt)));
                let sample = packet.bytes.saturating_mul(1_000_000) / rtt.as_micros().max(1) as u64;
                self.max_bandwidth = self.max_bandwidth.max(sample);
            }
        }
        bytes_acked
    }

    fn on_packets_lost(&mut self, lost: &[LostPacketInfo], persistent: bool) {
        let lost_bytes = lost.iter().map(|packet| packet.bytes).sum::<u64>();
        if persistent {
            self.cwnd = initial_window(self.mtu);
            self.mode = BbrMode::ProbeBw;
            return;
        }
        let penalty = lost_bytes.saturating_mul(self.params.bytes_lost_multiplier);
        self.cwnd = self.cwnd.saturating_sub(penalty).max(2 * self.mtu);
    }

    fn finish_round(
        &mut self,
        now: Instant,
        prior_in_flight: u64,
        bytes_acked: u64,
        largest_packet_num_acked: Option<u64>,
    ) {
        if let Some(largest) = largest_packet_num_acked {
            if largest > self.round_end_packet {
                self.round_count += 1;
                self.round_end_packet = self.max_sent_packet;
                self.update_full_bandwidth();
            }
        }
        self.update_mode(prior_in_flight);
        self.update_pacing_rate();
        self.update_cwnd(bytes_acked);
        if self.min_rtt.is_none() {
            self.min_rtt = Some(now.elapsed().min(Duration::from_millis(100)));
        }
    }

    fn update_full_bandwidth(&mut self) {
        if self.max_bandwidth >= self.full_bandwidth.saturating_mul(125) / 100 {
            self.full_bandwidth = self.max_bandwidth;
            self.rounds_without_growth = 0;
        } else {
            self.rounds_without_growth += 1;
        }
    }

    fn update_mode(&mut self, prior_in_flight: u64) {
        match self.mode {
            BbrMode::Startup if self.rounds_without_growth >= self.params.startup_growth_rounds => {
                self.mode = BbrMode::Drain;
            }
            BbrMode::Drain
                if !self.params.drain_to_target || prior_in_flight <= self.target_cwnd() =>
            {
                self.mode = BbrMode::ProbeBw;
            }
            _ => {}
        }
    }

    fn update_pacing_rate(&mut self) {
        if self.max_bandwidth == 0 {
            return;
        }
        let gain = match self.mode {
            BbrMode::Startup => self.params.high_gain_milli,
            BbrMode::Drain => 1000 * 1000 / self.params.high_gain_milli.max(1),
            BbrMode::ProbeBw => 1000,
        };
        self.pacing_rate = Some(self.max_bandwidth.saturating_mul(gain) / 1000);
    }

    fn update_cwnd(&mut self, bytes_acked: u64) {
        let target = self.target_cwnd();
        let growth = match self.mode {
            BbrMode::Startup => bytes_acked.saturating_mul(self.params.high_cwnd_gain_milli) / 1000,
            _ => bytes_acked,
        };
        self.cwnd = self
            .cwnd
            .saturating_add(growth)
            .min(target.max(initial_window(self.mtu)));
    }

    fn target_cwnd(&self) -> u64 {
        let Some(min_rtt) = self.min_rtt else {
            return initial_window(self.mtu);
        };
        let bdp = self
            .max_bandwidth
            .saturating_mul(min_rtt.as_micros() as u64)
            / 1_000_000;
        bdp.saturating_mul(self.params.cwnd_gain_milli) / 1000
    }
}

impl Controller for HysteriaBbr {
    fn on_sent(&mut self, _now: Instant, _bytes: u64, last_packet_number: u64) {
        self.max_sent_packet = self.max_sent_packet.max(last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        _rtt: &quinn::RttEstimator,
    ) {
        let acked = [AckedPacketInfo {
            packet_number: self.max_sent_packet,
            bytes,
            sent,
            app_limited,
        }];
        let bytes_acked = self.on_packets_acked(now, &acked);
        self.finish_round(now, self.cwnd, bytes_acked, Some(self.max_sent_packet));
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        _app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.finish_round(now, in_flight, 0, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        let lost = [LostPacketInfo {
            packet_number: self.max_sent_packet,
            bytes: lost_bytes,
            sent,
        }];
        self.on_packets_lost(&lost, is_persistent_congestion);
    }

    fn on_congestion_event_ex(
        &mut self,
        now: Instant,
        prior_in_flight: u64,
        acked: &[AckedPacketInfo],
        lost: &[LostPacketInfo],
        _app_limited: bool,
        _rtt: &quinn::RttEstimator,
        largest_packet_num_acked: Option<u64>,
        is_persistent_congestion: bool,
    ) -> bool {
        let bytes_acked = self.on_packets_acked(now, acked);
        self.on_packets_lost(lost, is_persistent_congestion);
        self.finish_round(now, prior_in_flight, bytes_acked, largest_packet_num_acked);
        true
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.mtu = u64::from(new_mtu.max(1200));
        self.cwnd = self.cwnd.max(2 * self.mtu);
    }

    fn window(&self) -> u64 {
        self.cwnd.max(2 * self.mtu)
    }

    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.window();
        metrics.pacing_rate = self.pacing_rate.map(|rate| rate.saturating_mul(8));
        metrics
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        initial_window(self.mtu)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BbrMode {
    Startup,
    Drain,
    ProbeBw,
}

fn initial_window(mtu: u64) -> u64 {
    (10 * mtu).min((2 * mtu).max(14_720))
}

#[allow(dead_code)]
pub(crate) fn apply_transport_config(
    config: &mut ClientConfig,
    transport: TransportConfig,
    profile: BbrProfile,
    initial_packet_size: u16,
    disable_path_mtu_discovery: bool,
) {
    apply_transport_config_with_factory(
        config,
        transport,
        Arc::new(HysteriaBbrConfig::new(
            profile,
            initial_packet_size.max(1200),
        )),
        initial_packet_size,
        disable_path_mtu_discovery,
    );
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
