// Rule / RuleSet surface for the minimal M4 route engine. Full sing-box rule
// sets land later; M4 only consumes the default rules generated from tun.*.

use hammer_core::config::{DomainStrategy, RuleActionKind};

use std::fmt;
use std::net::IpAddr;

use crate::Network;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocksAddr {
    pub host: IpAddr,
    pub port: u16,
    pub domain: Option<String>,
}

impl SocksAddr {
    pub fn ip(host: IpAddr, port: u16) -> Self {
        Self {
            host,
            port,
            domain: None,
        }
    }

    pub fn domain(domain: impl Into<String>, fallback: IpAddr, port: u16) -> Self {
        Self {
            host: fallback,
            port,
            domain: Some(domain.into()),
        }
    }

    pub fn destination_host(&self) -> String {
        self.domain.clone().unwrap_or_else(|| self.host.to_string())
    }
}

impl fmt::Display for SocksAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(domain) = &self.domain {
            return write!(f, "{domain}:{}", self.port);
        }
        match self.host {
            IpAddr::V4(addr) => write!(f, "{addr}:{}", self.port),
            IpAddr::V6(addr) => write!(f, "[{addr}]:{}", self.port),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteMetadata {
    pub inbound: String,
    pub network: Network,
    pub protocol: String,
    pub source: Option<SocksAddr>,
    pub destination: Option<SocksAddr>,
    pub domain: Option<String>,
    pub client: Option<String>,
    pub domain_strategy: Option<DomainStrategy>,
    pub udp_disable_domain_unmapping: bool,
    pub override_destination: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Route { outbound: String },
    HijackDns,
    Reject { method: String },
}

pub trait HeadlessRule: Send + Sync + 'static {}

pub trait Rule: HeadlessRule {
    fn type_name(&self) -> &str;
    fn matches(&self, metadata: &RouteMetadata) -> bool;
    fn action(&self) -> &RuleActionKind;
}
