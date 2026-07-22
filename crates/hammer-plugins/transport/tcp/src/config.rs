//! TCP plugin config — owned schema under `[plugin.tcp]` (#95).

use std::net::SocketAddr;
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
pub enum CongestionController {
    Bbr,
}

impl Default for CongestionController {
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
    pub congestion: CongestionController,
    pub nagle: bool,
    #[serde(with = "humantime_serde")]
    pub time_wait: Duration,
    #[serde(with = "humantime_serde")]
    pub paws_idle: Duration,
    pub retransmit: Retransmit,
    pub keepalive: Keepalive,
    pub pmtu: Pmtu,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listen: Vec<TcpListen>,
}

impl Default for TcpPluginConfig {
    fn default() -> Self {
        Self {
            mss: TCP_MSS,
            receive_window: TCP_WINDOW,
            congestion: CongestionController::Bbr,
            nagle: true,
            time_wait: TCP_TIME_WAIT,
            paws_idle: TCP_PAWS_IDLE,
            retransmit: Retransmit::default(),
            keepalive: Keepalive::default(),
            pmtu: Pmtu::default(),
            listen: Vec::new(),
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
        for entry in &self.listen {
            entry.validate()?;
        }
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct TcpListen {
    pub address: SocketAddr,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5_password: Option<String>,
}

impl Default for TcpListen {
    fn default() -> Self {
        Self {
            address: "0.0.0.0:0".parse().expect("wildcard"),
            md5_password: None,
        }
    }
}

impl TcpListen {
    fn validate(&self) -> RuntimeResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;

    use super::*;

    fn parse_tcp(section: &str) -> TcpPluginConfig {
        let tcp: TcpPluginConfig = toml::from_str(section).expect("deserialize plugin.tcp section");
        tcp.validate().expect("validate plugin.tcp");
        tcp
    }

    #[derive(Deserialize)]
    struct LabConfig {
        #[serde(default)]
        plugin: LabPluginConfig,
    }

    #[derive(Default, Deserialize)]
    struct LabPluginConfig {
        #[serde(default)]
        tcp: TcpPluginConfig,
    }

    #[test]
    fn empty_plugin_section_uses_tcp_defaults() {
        let tcp = parse_tcp("");

        assert_eq!(tcp, TcpPluginConfig::default());
    }

    #[test]
    fn tcp_plugin_owns_typed_policy_and_listener_schema() {
        let tcp = parse_tcp(
            r#"
mss = 1200
receive_window = 32768
congestion = "bbr"
nagle = false
time_wait = "30s"
paws_idle = "12h"

[pmtu]
enabled = false

[retransmit]
initial = "100ms"
min = "100ms"
max = "30s"

[keepalive]
idle = "60s"
probe_interval = "30s"
probe_limit = 4

[[listen]]
address = "10.66.77.1:7300"
"#,
        );

        assert_eq!(tcp.mss, 1200);
        assert_eq!(tcp.receive_window, 32768);
        assert_eq!(tcp.congestion, CongestionController::Bbr);
        assert!(!tcp.nagle);
        assert!(!tcp.pmtu.enabled);
        assert_eq!(tcp.time_wait, Duration::from_secs(30));
        assert_eq!(tcp.paws_idle, Duration::from_secs(12 * 3600));
        assert_eq!(tcp.retransmit.initial, Duration::from_millis(100));
        assert_eq!(tcp.keepalive.probe_limit, 4);
        assert_eq!(tcp.listen.len(), 1);
    }

    #[test]
    fn lab_example_parses_through_tcp_plugin_schema() {
        let config: LabConfig =
            toml::from_str(include_str!("../../../../../examples/tun-tcp-echo.toml"))
                .expect("deserialize lab config");
        let tcp = config.plugin.tcp;
        tcp.validate().expect("validate plugin.tcp");

        assert_eq!(tcp.time_wait, Duration::from_secs(2));
        assert_eq!(tcp.keepalive.idle, Duration::from_secs(3));
        assert_eq!(tcp.keepalive.probe_interval, Duration::from_secs(1));
        assert_eq!(tcp.keepalive.probe_limit, 3);
        assert_eq!(tcp.listen[0].address, "10.66.77.1:7300".parse().unwrap());
    }
}
