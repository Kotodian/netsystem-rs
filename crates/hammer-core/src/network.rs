use std::fmt;
use std::net::IpAddr;

/// Mirror of Go's `network` enum used by Outbound.Network() / Listen* paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Network {
    #[default]
    Tcp,
    Udp,
    Icmp,
}

impl Network {
    /// Stable string label suitable for metric tags and diagnostic
    /// formatting; matches the [`Display`] output.
    pub fn as_str(self) -> &'static str {
        match self {
            Network::Tcp => "tcp",
            Network::Udp => "udp",
            Network::Icmp => "icmp",
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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
