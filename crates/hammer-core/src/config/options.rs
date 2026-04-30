use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

#[cfg(feature = "wireguard")]
use super::endpoint::Endpoint;
use super::inbound::Inbound;
use super::log::LogOptions;
use super::outbound::Outbound;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub log: LogOptions,
    pub dns: DnsOptions,
    pub inbounds: Vec<Inbound>,
    pub outbounds: Vec<Outbound>,
    #[cfg(feature = "wireguard")]
    pub endpoints: Vec<Endpoint>,
    pub route: RouteOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsOptions {
    pub servers: Vec<DnsServer>,
    pub final_: String,
    pub strategy: DomainStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsServer {
    pub id: String,
    pub kind: DnsServerKind,
}

impl DnsServer {
    pub fn via(&self) -> &str {
        match &self.kind {
            DnsServerKind::Udp(o) => &o.via,
            DnsServerKind::Tcp(o) => &o.via,
            DnsServerKind::Https(o) => &o.via,
            DnsServerKind::Hosts | DnsServerKind::Local => "",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsServerKind {
    Udp(RemoteDnsServer),
    Tcp(RemoteDnsServer),
    Https(RemoteHttpsDnsServer),
    Hosts,
    Local,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteHttpsDnsServer {
    pub server: String,
    pub server_port: u16,
    pub via: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteOptions {
    pub final_: String,
    pub auto_detect_interface: bool,
    pub rules: Vec<Rule>,
    pub default_domain_resolver: Option<DomainResolveOptions>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DomainResolveOptions {
    pub server: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub default_options: DefaultRule,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefaultRule {
    pub matcher: RuleMatcher,
    pub action: RuleActionKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RuleMatcher {
    #[default]
    Any,
    Inbound(Vec<String>),
    Protocol(Vec<String>),
    Domain(Vec<String>),
    DomainSuffix(Vec<String>),
    DomainKeyword(Vec<String>),
    IpCidr(Vec<IpNet>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleActionKind {
    Sniff(SniffActionOptions),
    HijackDns,
    Reject(RejectActionOptions),
    Resolve(ResolveActionOptions),
    RouteOptions(RouteOptionsActionOptions),
    Route(RouteActionOptions),
}

impl Default for RuleActionKind {
    fn default() -> Self {
        RuleActionKind::HijackDns
    }
}

impl RuleActionKind {
    pub fn name(&self) -> &'static str {
        match self {
            RuleActionKind::Sniff(_) => "sniff",
            RuleActionKind::HijackDns => "hijack-dns",
            RuleActionKind::Reject(_) => "reject",
            RuleActionKind::Resolve(_) => "resolve",
            RuleActionKind::RouteOptions(_) => "route-options",
            RuleActionKind::Route(_) => "route",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteActionOptions {
    pub outbound: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SniffActionOptions {
    pub timeout: Option<Duration>,
    pub override_destination: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RejectActionOptions {
    pub method: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolveActionOptions {
    pub strategy: DomainStrategy,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteOptionsActionOptions {
    pub udp_disable_domain_unmapping: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainStrategy {
    #[default]
    AsIs,
    PreferIpv4,
    PreferIpv6,
    Ipv4Only,
    Ipv6Only,
}

impl DomainStrategy {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    pub fn name(self) -> &'static str {
        match self {
            DomainStrategy::AsIs => "as_is",
            DomainStrategy::PreferIpv4 => "prefer_ipv4",
            DomainStrategy::PreferIpv6 => "prefer_ipv6",
            DomainStrategy::Ipv4Only => "ipv4_only",
            DomainStrategy::Ipv6Only => "ipv6_only",
        }
    }
}
