use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use hammer_core::protocol::congestion::BbrProfile;
use quinn::congestion::{
    AckedPacketInfo, BbrConfig, Controller, ControllerFactory, ControllerMetrics, LostPacketInfo,
};
use quinn::{ClientConfig, TransportConfig};

use super::brutal::BrutalController;

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
            return Box::new(DynamicCongestionController::new(
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

    #[inline]
    fn brutal_bps(&self) -> u64 {
        self.brutal_bps.load(Ordering::SeqCst)
    }
}

/// Quinn's stock BBRv1 + our BrutalController, switched at runtime by the
/// hysteria2 auth response (server-advertised `rx_auto`). The `profile` knob
/// from the toml is currently advisory only — preserved on `HysteriaBbrConfig`
/// for forward compatibility but not consumed by the underlying BBR.
pub struct DynamicCongestionController {
    bbr: Box<dyn Controller>,
    brutal: BrutalController,
    handle: CongestionControlHandle,
}

impl DynamicCongestionController {
    fn new(mtu: u16, handle: CongestionControlHandle, brutal_debug: bool) -> Self {
        Self {
            bbr: build_quinn_bbr(mtu),
            brutal: BrutalController::new(mtu, brutal_debug),
            handle,
        }
    }

    #[inline]
    fn using_brutal(&self) -> bool {
        self.handle.brutal_bps() > 0
    }
}

fn build_quinn_bbr(mtu: u16) -> Box<dyn Controller> {
    let config: Arc<BbrConfig> = Arc::new(BbrConfig::default());
    config.build(Instant::now(), mtu)
}

impl Controller for DynamicCongestionController {
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
