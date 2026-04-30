//! `[tun]` inbound config section.
//!
//! Currently the only inbound kind is `tun`, but `Inbound` / `InboundKind`
//! sit at this layer so adding a new inbound (e.g. `socks` for desktop
//! debugging) means dropping a new variant here without touching the rest
//! of the config tree.

use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::error::HammerError;

use super::constants as C;
use super::options::DomainStrategy;
use super::parse::{
    self, deserialize_duration_option, deserialize_ipnet_vec, parse_domain_strategy,
};
use super::raw_struct_with_default_check;

raw_struct_with_default_check! {
    pub struct RawTunConfig {
        /// Inbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Requested TUN interface name.
        pub interface_name: String => "String::is_empty",
        /// TUN MTU.
        pub mtu: u32 => "is_zero_u32",
        /// Packet stack mode, for example `system` or `disabled`.
        #[serde(deserialize_with = "deserialize_tun_stack")]
        pub stack: Option<TunStack> => "Option::is_none",
        /// Local interface addresses in CIDR form.
        #[serde(
            deserialize_with = "deserialize_tun_addresses",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub address: Vec<IpNet> => "Vec::is_empty",
        /// Routes included through the tunnel.
        #[serde(
            deserialize_with = "deserialize_tun_route_addresses",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub route_address: Vec<IpNet> => "Vec::is_empty",
        /// Routes excluded from the tunnel.
        #[serde(
            deserialize_with = "deserialize_tun_route_exclude_addresses",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub route_exclude_address: Vec<IpNet> => "Vec::is_empty",
        /// Whether the platform should install routes automatically.
        pub auto_route: Option<bool> => "Option::is_none",
        /// Whether route installation should use strict routing semantics.
        pub strict_route: bool => "is_false",
        /// UDP idle timeout.
        #[serde(
            deserialize_with = "deserialize_tun_udp_timeout",
            serialize_with = "serialize_duration_option"
        )]
        pub udp_timeout: Option<Duration> => "Option::is_none",
        /// Whether protocol/domain sniffing is enabled.
        pub sniff: bool => "is_false",
        /// Whether DNS packets should be intercepted by the DNS router.
        pub hijack_dns: bool => "is_false",
        /// Whether sniffed destinations replace the original destination.
        pub sniff_override_destination: bool => "is_false",
        /// Sniffing timeout.
        #[serde(
            deserialize_with = "deserialize_tun_sniff_timeout",
            serialize_with = "serialize_duration_option"
        )]
        pub sniff_timeout: Option<Duration> => "Option::is_none",
        /// Domain resolution strategy for sniffed/routed traffic.
        #[serde(deserialize_with = "deserialize_tun_domain_strategy")]
        pub domain_strategy: DomainStrategy => "DomainStrategy::is_default",
        /// Whether UDP domain unmapping is disabled for this inbound.
        pub udp_disable_domain_unmapping: bool => "is_false",
        /// Whether detected QUIC traffic should be rejected.
        pub block_quic: bool => "is_false",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbound {
    pub id: String,
    pub kind: InboundKind,
}

impl Inbound {
    pub fn type_name(&self) -> &'static str {
        match self.kind {
            InboundKind::Tun(_) => "tun",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundKind {
    Tun(TunInboundOptions),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TunInboundOptions {
    pub interface_name: String,
    pub mtu: u32,
    pub address: Vec<IpNet>,
    pub route_address: Vec<IpNet>,
    pub route_exclude_address: Vec<IpNet>,
    pub auto_route: bool,
    pub strict_route: bool,
    pub stack: TunStack,
    pub udp_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TunStack {
    #[default]
    System,
    Disabled,
}

impl TunStack {
    pub fn name(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Disabled => "disabled",
        }
    }
}

/// Build the lone `Inbound` (currently always `tun`) and return the resolved
/// tun id alongside it so the caller can hand it to route-rule derivation.
pub(super) fn build_tun_inbound(raw: &RawTunConfig) -> Result<(Inbound, String), HammerError> {
    let id = if raw.id.is_empty() {
        C::DEFAULT_TUN_ID.to_owned()
    } else {
        raw.id.clone()
    };
    let options = build_tun_options(raw)?;
    Ok((
        Inbound {
            id: id.clone(),
            kind: InboundKind::Tun(options),
        },
        id,
    ))
}

fn build_tun_options(raw: &RawTunConfig) -> Result<TunInboundOptions, HammerError> {
    let mtu = if raw.mtu == 0 {
        C::DEFAULT_TUN_MTU
    } else {
        raw.mtu
    };
    let stack = raw.stack.unwrap_or_default();
    let address = raw.address.clone();
    if address.is_empty() {
        return Err(HammerError::config_validation("tun.address is required"));
    }
    let route_address = raw.route_address.clone();
    let route_exclude_address = raw.route_exclude_address.clone();
    let auto_route = raw.auto_route.unwrap_or(true);
    Ok(TunInboundOptions {
        interface_name: raw.interface_name.clone(),
        mtu,
        address,
        route_address,
        route_exclude_address,
        auto_route,
        strict_route: raw.strict_route,
        stack,
        udp_timeout: raw.udp_timeout,
    })
}

fn deserialize_tun_stack<'de, D>(deserializer: D) -> Result<Option<TunStack>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "" => Ok(None),
        "system" => Ok(Some(TunStack::System)),
        "disabled" => Ok(Some(TunStack::Disabled)),
        other => Err(de::Error::custom(format!(
            "tun.stack: unknown stack {other:?}"
        ))),
    }
}

fn deserialize_tun_domain_strategy<'de, D>(deserializer: D) -> Result<DomainStrategy, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_domain_strategy("tun.domain_strategy", &value).map_err(de::Error::custom)
}

fn deserialize_tun_udp_timeout<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("tun.udp_timeout", deserializer)
}

fn deserialize_tun_sniff_timeout<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("tun.sniff_timeout", deserializer)
}

fn deserialize_tun_addresses<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("tun.address", deserializer)
}

fn deserialize_tun_route_addresses<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("tun.route_address", deserializer)
}

fn deserialize_tun_route_exclude_addresses<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("tun.route_exclude_address", deserializer)
}

fn serialize_ipnet_vec<S>(value: &[IpNet], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    parse::serialize_ipnet_vec(value, serializer)
}

fn serialize_duration_option<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    parse::serialize_duration_option(value, serializer)
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}
