// Rule / RuleSet surface for the minimal M4 route engine. Full sing-box rule
// sets land later; M4 only consumes the default rules generated from tun.*.

pub use hammer_core::SocksAddr;
use hammer_core::config::{DomainStrategy, RuleActionKind};

use crate::Network;

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
    pub route_decision: Option<RouteDecision>,
    pub forwarding: Option<ForwardingMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardingMetadata {
    pub fib_index: u32,
    pub load_balance_index: u32,
    pub bucket_index: u16,
    pub dpo_type: ForwardingDpoType,
    pub dpo_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardingDpoType {
    Drop,
    Punt,
    Adjacency,
    Receive,
    Custom(u16),
    LoadBalance,
}

/// Where a `RouteDecision::Route` points. Outbounds and endpoints live in
/// parallel namespaces with different data paths — L4 stream (`dial /
/// listen_packet`) for `Outbound`, L3 IP packet (`ip_send / ip_recv`) for
/// `Endpoint`. The router tags the decision so the TUN dispatch layer can
/// pick the right code path without re-querying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    Outbound(String),
    Endpoint(String),
}

impl RouteTarget {
    pub fn name(&self) -> &str {
        match self {
            Self::Outbound(name) | Self::Endpoint(name) => name,
        }
    }

    pub fn is_endpoint(&self) -> bool {
        matches!(self, Self::Endpoint(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Route { target: RouteTarget },
    HijackDns,
    Reject { method: String },
}

pub trait HeadlessRule: Send + Sync + 'static {}

pub trait Rule: HeadlessRule {
    fn type_name(&self) -> &str;
    fn matches(&self, metadata: &RouteMetadata) -> bool;
    fn action(&self) -> &RuleActionKind;
}
