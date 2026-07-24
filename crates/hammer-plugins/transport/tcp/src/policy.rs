//! Config-driven TCP runtime policy published by `tcp_init` from `[plugin.tcp]`.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;

use crate::config::{CongestionAlgorithm, TcpPluginConfig};
use crate::congestion;

/// Snapshot of `[plugin.tcp]` knobs consumed by TCP connections and timers.
#[derive(Debug, Clone, Copy)]
pub struct TcpPolicy {
    pub mss: usize,
    pub receive_window: u32,
    pub(crate) congestion: &'static congestion::Algorithm,
    pub nagle: bool,
    pub time_wait: Duration,
    pub paws_idle: Duration,
    pub retransmit_initial: Duration,
    pub retransmit_min: Duration,
    pub retransmit_max: Duration,
    pub keepalive_idle: Duration,
    pub keepalive_probe_interval: Duration,
    pub keepalive_probe_limit: u8,
    pub pmtu_enabled: bool,
}

impl TcpPolicy {
    pub fn from_plugin_config(tcp: &TcpPluginConfig) -> Self {
        Self {
            mss: tcp.mss,
            receive_window: tcp.receive_window,
            congestion: congestion::resolve(tcp.congestion),
            nagle: tcp.nagle,
            time_wait: tcp.time_wait,
            paws_idle: tcp.paws_idle,
            retransmit_initial: tcp.retransmit.initial,
            retransmit_min: tcp.retransmit.min,
            retransmit_max: tcp.retransmit.max,
            keepalive_idle: tcp.keepalive.idle,
            keepalive_probe_interval: tcp.keepalive.probe_interval,
            keepalive_probe_limit: tcp.keepalive.probe_limit,
            pmtu_enabled: tcp.pmtu.enabled,
        }
    }

    /// Production defaults matching historical TCP constants when no config is
    /// published yet (unit tests that construct connections without init).
    pub const fn production_defaults() -> Self {
        Self {
            mss: 1_440,
            receive_window: u16::MAX as u32,
            congestion: congestion::resolve(CongestionAlgorithm::Bbr),
            nagle: true,
            time_wait: Duration::from_secs(60),
            paws_idle: Duration::from_secs(24 * 60 * 60),
            retransmit_initial: Duration::from_millis(50),
            retransmit_min: Duration::from_millis(50),
            retransmit_max: Duration::from_secs(60),
            keepalive_idle: Duration::from_secs(75),
            keepalive_probe_interval: Duration::from_secs(75),
            keepalive_probe_limit: 8,
            pmtu_enabled: true,
        }
    }
}

impl PartialEq for TcpPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.mss == other.mss
            && self.receive_window == other.receive_window
            && std::ptr::eq(self.congestion, other.congestion)
            && self.nagle == other.nagle
            && self.time_wait == other.time_wait
            && self.paws_idle == other.paws_idle
            && self.retransmit_initial == other.retransmit_initial
            && self.retransmit_min == other.retransmit_min
            && self.retransmit_max == other.retransmit_max
            && self.keepalive_idle == other.keepalive_idle
            && self.keepalive_probe_interval == other.keepalive_probe_interval
            && self.keepalive_probe_limit == other.keepalive_probe_limit
            && self.pmtu_enabled == other.pmtu_enabled
    }
}

impl Eq for TcpPolicy {}

pub static TCP_POLICY: ArcSwapOption<TcpPolicy> = ArcSwapOption::const_empty();

/// Published `[plugin.tcp]` policy, if `tcp_init` has run.
pub fn tcp_policy() -> Option<TcpPolicy> {
    TCP_POLICY.load().as_deref().copied()
}

/// Active TCP policy: published config, or production defaults when unset.
pub fn active_tcp_policy() -> TcpPolicy {
    tcp_policy().unwrap_or_else(TcpPolicy::production_defaults)
}

pub fn publish_tcp_policy(policy: TcpPolicy) {
    TCP_POLICY.store(Some(Arc::new(policy)));
}

pub fn reset_tcp_policy_for_test() {
    TCP_POLICY.store(None);
}
