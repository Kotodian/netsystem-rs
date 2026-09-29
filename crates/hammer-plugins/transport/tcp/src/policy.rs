//! Config-driven TCP runtime policy published by `tcp_init` from `[plugin.tcp]`.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;

use crate::config::TcpPluginConfig;

/// Snapshot of `[plugin.tcp]` knobs consumed by TCP connections and timers.
#[derive(Debug, Clone, Copy)]
pub struct TcpPolicy {
    pub mss: usize,
    pub receive_window: u32,
    pub nagle: bool,
    pub time_wait: Duration,
    pub close_wait: Duration,
    pub fin_wait1: Duration,
    pub fin_wait2: Duration,
    pub last_ack: Duration,
    pub closing: Duration,
    pub paws_idle: Duration,
    pub retransmit_initial: Duration,
    pub retransmit_min: Duration,
    pub retransmit_max: Duration,
    pub allocation_retry: Duration,
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
            nagle: tcp.nagle,
            time_wait: tcp.time_wait,
            close_wait: tcp.close_wait,
            fin_wait1: tcp.fin_wait1,
            fin_wait2: tcp.fin_wait2,
            last_ack: tcp.last_ack,
            closing: tcp.closing,
            paws_idle: tcp.paws_idle,
            retransmit_initial: tcp.retransmit.initial,
            retransmit_min: tcp.retransmit.min,
            retransmit_max: tcp.retransmit.max,
            allocation_retry: tcp.retransmit.allocation_retry,
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
            nagle: true,
            time_wait: Duration::from_secs(10),
            close_wait: Duration::from_secs(2),
            fin_wait1: Duration::from_secs(60),
            fin_wait2: Duration::from_secs(30),
            last_ack: Duration::from_secs(30),
            closing: Duration::from_secs(30),
            paws_idle: Duration::from_secs(24 * 60 * 60),
            retransmit_initial: Duration::from_millis(50),
            retransmit_min: Duration::from_millis(50),
            retransmit_max: Duration::from_secs(60),
            allocation_retry: Duration::from_millis(100),
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
            && self.nagle == other.nagle
            && self.time_wait == other.time_wait
            && self.close_wait == other.close_wait
            && self.fin_wait1 == other.fin_wait1
            && self.fin_wait2 == other.fin_wait2
            && self.last_ack == other.last_ack
            && self.closing == other.closing
            && self.paws_idle == other.paws_idle
            && self.retransmit_initial == other.retransmit_initial
            && self.retransmit_min == other.retransmit_min
            && self.retransmit_max == other.retransmit_max
            && self.allocation_retry == other.allocation_retry
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
