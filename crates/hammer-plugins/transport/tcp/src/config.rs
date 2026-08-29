//! TCP plugin config — owned schema under `[plugin.tcp]` (#95).

use std::time::Duration;

use hammer_runtime::{RuntimeError, RuntimeResult};

const TCP_MSS: usize = 1_440;
const TCP_WINDOW: u32 = u16::MAX as u32;
const TCP_INITIAL_RTO: Duration = Duration::from_millis(50);
const TCP_MIN_RTO: Duration = Duration::from_millis(50);
const TCP_MAX_RTO: Duration = Duration::from_secs(60);
const TCP_TIME_WAIT: Duration = Duration::from_secs(600);
const TCP_PAWS_IDLE: Duration = Duration::from_secs(24 * 86_400);
const KEEPALIVE_IDLE: Duration = Duration::from_secs(75);
const KEEPALIVE_PROBE_INTERVAL: Duration = Duration::from_secs(75);
const KEEPALIVE_PROBE_LIMIT: u8 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CongestionAlgorithm {
    Bbr,
    Cubic,
}

impl Default for CongestionAlgorithm {
    fn default() -> Self {
        Self::Bbr
    }
}

/// `[plugin.tcp]` — parsed only inside this plugin.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TcpPluginConfig {
    pub mss: usize,
    pub receive_window: u32,
    pub congestion: CongestionAlgorithm,
    pub nagle: bool,
    #[serde(with = "humantime_serde")]
    pub time_wait: Duration,
    #[serde(with = "humantime_serde")]
    pub paws_idle: Duration,
    pub retransmit: Retransmit,
    pub keepalive: Keepalive,
    pub pmtu: Pmtu,
}

impl Default for TcpPluginConfig {
    fn default() -> Self {
        Self {
            mss: TCP_MSS,
            receive_window: TCP_WINDOW,
            congestion: CongestionAlgorithm::Bbr,
            nagle: true,
            time_wait: TCP_TIME_WAIT,
            paws_idle: TCP_PAWS_IDLE,
            retransmit: Retransmit::default(),
            keepalive: Keepalive::default(),
            pmtu: Pmtu::default(),
        }
    }
}

impl TcpPluginConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.mss == 0 {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.mss must be non-zero",
            ));
        }
        if self.receive_window == 0 {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.receive_window must be non-zero",
            ));
        }
        if self.time_wait.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.time_wait must be non-zero",
            ));
        }
        if self.paws_idle.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.paws_idle must be non-zero",
            ));
        }
        self.retransmit.validate()?;
        self.keepalive.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Retransmit {
    #[serde(with = "humantime_serde")]
    pub initial: Duration,
    #[serde(with = "humantime_serde")]
    pub min: Duration,
    #[serde(with = "humantime_serde")]
    pub max: Duration,
}

impl Default for Retransmit {
    fn default() -> Self {
        Self {
            initial: TCP_INITIAL_RTO,
            min: TCP_MIN_RTO,
            max: TCP_MAX_RTO,
        }
    }
}

impl Retransmit {
    fn validate(&self) -> RuntimeResult<()> {
        if self.initial.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.retransmit.initial must be non-zero",
            ));
        }
        if self.min.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.retransmit.min must be non-zero",
            ));
        }
        if self.max.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.retransmit.max must be non-zero",
            ));
        }
        if self.min > self.initial {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.retransmit.min must not exceed initial",
            ));
        }
        if self.initial > self.max {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.retransmit.initial must not exceed max",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Keepalive {
    #[serde(with = "humantime_serde")]
    pub idle: Duration,
    #[serde(with = "humantime_serde")]
    pub probe_interval: Duration,
    pub probe_limit: u8,
}

impl Default for Keepalive {
    fn default() -> Self {
        Self {
            idle: KEEPALIVE_IDLE,
            probe_interval: KEEPALIVE_PROBE_INTERVAL,
            probe_limit: KEEPALIVE_PROBE_LIMIT,
        }
    }
}

impl Keepalive {
    fn validate(&self) -> RuntimeResult<()> {
        if self.idle.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.keepalive.idle must be non-zero",
            ));
        }
        if self.probe_interval.is_zero() {
            return Err(RuntimeError::config_validation(
                "plugin.tcp.keepalive.probe_interval must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Pmtu {
    pub enabled: bool,
}

impl Default for Pmtu {
    fn default() -> Self {
        Self { enabled: true }
    }
}
