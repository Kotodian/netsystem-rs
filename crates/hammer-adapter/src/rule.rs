// Rule / RuleSet surface for the minimal M4 route engine. Full sing-box rule
// sets land later; M4 only consumes the default rules generated from tun.*.

pub use hammer_core::SocksAddr;
use hammer_core::config::{DomainStrategy, RuleActionKind};
use hammer_core::forwarding::DpoType;
use hammer_core::protocol::icmp::IcmpErrorMetadata;

use crate::{Network, NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeaturePathEntry {
    node: NodeId,
    config: Option<Vec<u8>>,
}

impl FeaturePathEntry {
    #[inline]
    pub fn new(node: NodeId, config: Option<Vec<u8>>) -> Self {
        Self { node, config }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeaturePathMetadata {
    next_entries: Vec<FeaturePathEntry>,
    cursor: usize,
}

impl FeaturePathMetadata {
    #[inline]
    pub fn new(next_entries: Vec<FeaturePathEntry>) -> Self {
        Self {
            next_entries,
            cursor: 0,
        }
    }

    #[inline]
    pub fn pop_next(&mut self) -> Option<FeaturePathEntry> {
        let next = self.next_entries.get(self.cursor).cloned()?;
        self.cursor += 1;
        Some(next)
    }

    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.cursor >= self.next_entries.len()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteMetadata {
    pub inbound: String,
    pub ingress_interface: Option<u32>,
    pub egress_interface: Option<u32>,
    pub ip_ecn: Option<IpEcnCodepoint>,
    pub tap_ethernet: Option<TapEthernetMetadata>,
    pub feature_path: Option<FeaturePathMetadata>,
    pub feature_config: Option<Vec<u8>>,
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
    pub icmp_error: Option<IcmpErrorMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpEcnCodepoint {
    NotEct,
    Ect0,
    Ect1,
    Ce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TapEthernetMetadata {
    pub destination: [u8; 6],
    pub source: [u8; 6],
    pub ethertype: u16,
    pub header_present: bool,
}

impl TapEthernetMetadata {
    #[inline]
    pub const fn new(destination: [u8; 6], source: [u8; 6], ethertype: u16) -> Self {
        Self {
            destination,
            source,
            ethertype,
            header_present: false,
        }
    }

    #[inline]
    pub const fn header_present(destination: [u8; 6], source: [u8; 6], ethertype: u16) -> Self {
        Self {
            destination,
            source,
            ethertype,
            header_present: true,
        }
    }

    #[inline]
    pub fn header(self) -> [u8; 14] {
        let mut header = [0u8; 14];
        header[..6].copy_from_slice(&self.destination);
        header[6..12].copy_from_slice(&self.source);
        header[12..14].copy_from_slice(&self.ethertype.to_be_bytes());
        header
    }
}

impl RouteMetadata {
    #[inline]
    pub fn set_current_feature_config(&mut self, config: Option<Vec<u8>>) {
        self.feature_config = config;
    }

    #[inline]
    pub fn set_feature_path(&mut self, next_entries: Vec<FeaturePathEntry>) {
        self.feature_path = Some(FeaturePathMetadata::new(next_entries));
    }

    #[inline]
    pub fn clear_feature_path(&mut self) {
        self.feature_path = None;
        self.feature_config = None;
    }

    #[inline]
    pub fn pop_feature_next(&mut self) -> Option<NodeId> {
        let path = self.feature_path.as_mut()?;
        let next = path.pop_next();
        if path.is_exhausted() {
            self.feature_path = None;
        }
        next.map(|entry| {
            self.feature_config = entry.config;
            entry.node
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardingMetadata {
    pub fib_index: u32,
    pub route_dpo_type: ForwardingDpoType,
    pub route_dpo_index: u32,
    pub load_balance_index: u32,
    pub bucket_index: u16,
    pub dpo_type: ForwardingDpoType,
    pub dpo_index: u32,
}

pub type ForwardingDpoType = DpoType;

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

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::IcmpErrorMetadata;

    #[test]
    fn icmp_error_metadata_option_stays_word_sized() {
        assert_eq!(size_of::<Option<IcmpErrorMetadata>>(), size_of::<u64>());
    }
}
