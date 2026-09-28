use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use thiserror::Error;

const DEFAULT_CONTROL_PORT: u16 = 5_201;
const DEFAULT_DATA_PORT: u16 = 5_202;
const DEFAULT_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Iperf3Config {
    pub enable: bool,
    pub namespace: u32,
    pub control_endpoint: SocketAddr,
    pub data_endpoint: SocketAddr,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
}

impl Default for Iperf3Config {
    fn default() -> Self {
        Self {
            enable: true,
            namespace: 0,
            control_endpoint: SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                DEFAULT_CONTROL_PORT,
            ),
            data_endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_DATA_PORT),
            duration: DEFAULT_DURATION,
        }
    }
}

impl Iperf3Config {
    pub fn validate(&self) -> Result<(), Iperf3ConfigError> {
        if self.control_endpoint.port() == 0 {
            return Err(Iperf3ConfigError::PortZero {
                listener: "control",
            });
        }
        if self.data_endpoint.port() == 0 {
            return Err(Iperf3ConfigError::PortZero { listener: "data" });
        }
        if self.control_endpoint.is_ipv4() != self.data_endpoint.is_ipv4() {
            return Err(Iperf3ConfigError::AddressFamilyMismatch);
        }
        if self.control_endpoint.port() == self.data_endpoint.port() {
            return Err(Iperf3ConfigError::PortCollision {
                port: self.control_endpoint.port(),
            });
        }
        if self.duration.is_zero() {
            return Err(Iperf3ConfigError::DurationZero);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum Iperf3ConfigError {
    #[error("{listener} listener port must be non-zero")]
    PortZero { listener: &'static str },
    #[error("control and data listeners must use the same address family")]
    AddressFamilyMismatch,
    #[error("control and data listeners cannot share port {port}")]
    PortCollision { port: u16 },
    #[error("iperf3 duration must be non-zero")]
    DurationZero,
}
