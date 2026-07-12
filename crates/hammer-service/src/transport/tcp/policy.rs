//! Config-driven TCP runtime policy published by `transport_init`.

use std::time::Duration;

use hammer_core::config::network::Tcp as TcpConfig;

/// Snapshot of `[network.tcp]` knobs consumed by TCP connections and timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpPolicy {
    pub mss: usize,
    pub receive_window: u32,
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
    pub fn from_config(tcp: &TcpConfig) -> Self {
        Self {
            mss: tcp.mss,
            receive_window: tcp.receive_window,
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
