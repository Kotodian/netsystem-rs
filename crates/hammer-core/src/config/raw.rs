use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[cfg(feature = "wireguard")]
use crate::error::HammerError;
use crate::log::Level;

use super::options::{
    DomainStrategy, Hysteria2BbrProfile, Hysteria2Network, Hysteria2ObfsType, RuleMatcher, TunStack,
};
#[cfg(feature = "wireguard")]
use super::parse::parse_base64_key;
use super::parse::{parse_domain_strategy, parse_ipnet, parse_optional_duration};

// Raw config structs are mostly serde field declarations with the same
// `default + skip_serializing_if` pattern. Keep the actual field list explicit,
// but let the macro own the repetitive attributes and derives.
macro_rules! raw_struct {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field:ident : $ty:ty => $skip:literal
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        $vis struct $name {
            $(
                $(#[$field_meta])*
                #[serde(default, skip_serializing_if = $skip)]
                $field_vis $field: $ty,
            )*
        }
    };
}

// Top-level sections are skipped when they are exactly default. This wraps
// `raw_struct!` and adds the small helper serde calls from `RawConfig`.
macro_rules! raw_struct_with_default_check {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field_vis:vis $field:ident : $ty:ty => $skip:literal
            ),* $(,)?
        }
    ) => {
        raw_struct! {
            $(#[$meta])*
            $vis struct $name {
                $(
                    $(#[$field_meta])*
                    $field_vis $field: $ty => $skip,
                )*
            }
        }

        impl $name {
            fn is_default(&self) -> bool {
                *self == $name::default()
            }
        }
    };
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// Optional logging section.
    #[serde(default, skip_serializing_if = "RawLogConfig::is_default")]
    pub log: RawLogConfig,
    /// Optional TUN inbound section.
    #[serde(default, skip_serializing_if = "RawTunConfig::is_default")]
    pub tun: RawTunConfig,
    /// Optional top-level Hysteria2 outbound section.
    #[serde(default, skip_serializing_if = "RawHysteria2Config::is_default")]
    pub hysteria2: RawHysteria2Config,
    /// Optional sing-box style endpoint list.
    #[cfg(feature = "wireguard")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<RawEndpoint>,
    /// Optional DNS transport section.
    #[serde(default, skip_serializing_if = "RawDnsConfig::is_default")]
    pub dns: RawDnsConfig,
    /// Optional route section.
    #[serde(default, skip_serializing_if = "RawRouteConfig::is_default")]
    pub route: RawRouteConfig,
}

raw_struct_with_default_check! {
    pub struct RawLogConfig {
        /// Minimum log level, for example `debug`, `info`, or `warn`.
        #[serde(deserialize_with = "deserialize_log_level")]
        pub level: Option<Level> => "Option::is_none",
        /// Log output target from the user config.
        pub output: String => "String::is_empty",
        /// Whether log lines should include timestamps.
        pub timestamp: bool => "is_false",
        /// Whether logging is disabled entirely.
        pub disabled: bool => "is_false",
    }
}

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

raw_struct_with_default_check! {
    pub struct RawHysteria2Config {
        /// Outbound id used by route rules.
        pub id: String => "String::is_empty",
        /// Hysteria2 server host or IP.
        pub server: String => "String::is_empty",
        /// Single Hysteria2 server port.
        pub server_port: u16 => "is_zero_u16",
        /// Port-hopping range strings from the raw config.
        pub server_ports: Vec<String> => "Vec::is_empty",
        /// Hysteria2 password.
        pub password: String => "String::is_empty",
        /// Upload bandwidth hint in Mbps.
        pub up_mbps: i64 => "is_zero_i64",
        /// Download bandwidth hint in Mbps.
        pub down_mbps: i64 => "is_zero_i64",
        /// TLS SNI override.
        pub sni: String => "String::is_empty",
        /// Whether invalid TLS certificates are accepted.
        pub insecure: bool => "is_false",
        /// Enabled network list from the raw config.
        #[serde(deserialize_with = "deserialize_hysteria2_networks")]
        pub network: Vec<Hysteria2Network> => "Vec::is_empty",
        /// Port-hopping interval.
        #[serde(
            deserialize_with = "deserialize_hysteria2_hop_interval",
            serialize_with = "serialize_duration_option"
        )]
        pub hop_interval: Option<Duration> => "Option::is_none",
        /// Maximum port-hopping interval.
        #[serde(
            deserialize_with = "deserialize_hysteria2_hop_interval_max",
            serialize_with = "serialize_duration_option"
        )]
        pub hop_interval_max: Option<Duration> => "Option::is_none",
        /// QUIC idle timeout.
        #[serde(
            deserialize_with = "deserialize_hysteria2_idle_timeout",
            serialize_with = "serialize_duration_option"
        )]
        pub idle_timeout: Option<Duration> => "Option::is_none",
        /// QUIC keep-alive period.
        #[serde(
            deserialize_with = "deserialize_hysteria2_keep_alive_period",
            serialize_with = "serialize_duration_option"
        )]
        pub keep_alive_period: Option<Duration> => "Option::is_none",
        /// Hysteria2 BBR profile.
        #[serde(deserialize_with = "deserialize_hysteria2_bbr_profile")]
        pub bbr_profile: Option<Hysteria2BbrProfile> => "Option::is_none",
        /// Whether Brutal congestion-control debug output is enabled.
        pub brutal_debug: bool => "is_false",
        /// Whether QUIC path MTU discovery is disabled.
        #[serde(rename = "disable_path_mtu_discovery")]
        pub disable_path_mtu: bool => "is_false",
        /// Initial QUIC datagram size.
        pub initial_packet_size: u16 => "is_zero_u16",
        /// Optional Hysteria2 obfuscation section.
        pub obfs: RawHysteria2Obfs => "RawHysteria2Obfs::is_default",
    }
}

raw_struct_with_default_check! {
    pub struct RawHysteria2Obfs {
        /// Obfuscation type.
        #[serde(rename = "type")]
        #[serde(deserialize_with = "deserialize_hysteria2_obfs_type")]
        pub type_: Option<Hysteria2ObfsType> => "Option::is_none",
        /// Obfuscation password.
        pub password: String => "String::is_empty",
    }
}

/// Outer endpoint variant — sing-box style `[[endpoints]]` entries with a
/// `type` discriminator. Adding a new endpoint protocol (e.g. tailscale) means
/// adding a new variant here without breaking existing TOML files.
#[cfg(feature = "wireguard")]
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", deny_unknown_fields, rename_all = "lowercase")]
pub enum RawEndpoint {
    /// WireGuard endpoint entry.
    Wireguard(RawWireguardEndpoint),
}

#[cfg(feature = "wireguard")]
raw_struct! {
    pub struct RawWireguardEndpoint {
        /// Endpoint id used by route rules and lifecycle managers.
        pub id: String => "String::is_empty",
        /// Base64-encoded WireGuard private key.
        pub private_key: RawBase64Key => "RawBase64Key::is_empty",
        /// Optional UDP listen port.
        pub listen_port: Option<u16> => "Option::is_none",
        /// Optional WireGuard interface MTU.
        pub mtu: Option<u32> => "Option::is_none",
        /// Local WireGuard interface addresses in CIDR form.
        #[serde(
            deserialize_with = "deserialize_wireguard_addresses",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub address: Vec<IpNet> => "Vec::is_empty",
        /// WireGuard peer list.
        pub peers: Vec<RawWireguardPeer> => "Vec::is_empty",
    }
}

#[cfg(feature = "wireguard")]
raw_struct! {
    pub struct RawWireguardPeer {
        /// Base64-encoded peer public key.
        pub public_key: RawBase64Key => "RawBase64Key::is_empty",
        /// Optional base64-encoded pre-shared key.
        pub pre_shared_key: Option<RawBase64Key> => "Option::is_none",
        /// Peer endpoint address; currently must be an IP literal.
        pub address: String => "String::is_empty",
        /// Peer endpoint UDP port.
        pub port: u16 => "is_zero_u16",
        /// Allowed IP prefixes routed to this peer.
        #[serde(
            deserialize_with = "deserialize_wireguard_allowed_ips",
            serialize_with = "serialize_ipnet_vec"
        )]
        pub allowed_ips: Vec<IpNet> => "Vec::is_empty",
        /// Optional persistent keepalive interval in seconds.
        pub persistent_keepalive_interval: Option<u32> => "Option::is_none",
        /// Optional reserved WARP-style header bytes.
        pub reserved: Option<[u8; 3]> => "Option::is_none",
    }
}

#[cfg(feature = "wireguard")]
#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct RawBase64Key(String);

#[cfg(feature = "wireguard")]
impl RawBase64Key {
    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }

    pub fn decode_32(&self, field: &str) -> Result<[u8; 32], HammerError> {
        parse_base64_key(field, self.0.trim())
    }
}

raw_struct_with_default_check! {
    pub struct RawDnsConfig {
        /// DNS transport id.
        pub id: String => "String::is_empty",
        /// Upstream DNS server URL or address.
        pub server: String => "String::is_empty",
        /// DNS answer selection strategy.
        #[serde(deserialize_with = "deserialize_dns_strategy")]
        pub strategy: DomainStrategy => "DomainStrategy::is_default",
        /// Outbound id used to reach the upstream DNS server.
        pub via: String => "String::is_empty",
    }
}

raw_struct_with_default_check! {
    pub struct RawRouteConfig {
        /// Final outbound id.
        #[serde(rename = "final")]
        pub final_: String => "String::is_empty",
        /// Whether to let the runtime detect the platform default interface.
        pub auto_detect_interface: Option<bool> => "Option::is_none",
        /// Ordered user route rules.
        #[serde(
            deserialize_with = "deserialize_route_rules",
            serialize_with = "serialize_route_rules"
        )]
        pub rules: Vec<RawRouteRule> => "Vec::is_empty",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRouteRule {
    /// Single matcher for this route rule.
    pub matcher: RuleMatcher,
    /// Route target outbound id.
    pub outbound: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RawRouteRuleText {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    inbound: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    protocol: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    domain: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    domain_suffix: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    domain_keyword: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_route_ip_cidrs",
        serialize_with = "serialize_ipnet_vec",
        skip_serializing_if = "Vec::is_empty"
    )]
    ip_cidr: Vec<IpNet>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    outbound: String,
}

fn is_false(v: &bool) -> bool {
    !*v
}
fn is_zero_u16(v: &u16) -> bool {
    *v == 0
}
fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn deserialize_log_level<'de, D>(deserializer: D) -> Result<Option<Level>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Ok(None);
    }
    Level::from_name(value.to_ascii_lowercase().as_str())
        .map(Some)
        .ok_or_else(|| de::Error::custom(format!("log.level: unknown log level {value:?}")))
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

fn deserialize_hysteria2_hop_interval<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.hop_interval", deserializer)
}

fn deserialize_hysteria2_hop_interval_max<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.hop_interval_max", deserializer)
}

fn deserialize_hysteria2_idle_timeout<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.idle_timeout", deserializer)
}

fn deserialize_hysteria2_keep_alive_period<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_duration_option("hysteria2.keep_alive_period", deserializer)
}

fn deserialize_duration_option<'de, D>(
    field: &str,
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_optional_duration(field, &value).map_err(de::Error::custom)
}

fn serialize_duration_option<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_str(&format_duration_go_style(*value)),
        None => serializer.serialize_none(),
    }
}

fn format_duration_go_style(value: Duration) -> String {
    const NS: u128 = 1;
    const US: u128 = 1_000 * NS;
    const MS: u128 = 1_000 * US;
    const S: u128 = 1_000 * MS;
    const M: u128 = 60 * S;
    const H: u128 = 60 * M;

    let nanos = value.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    for (unit, scale) in [
        ("h", H),
        ("m", M),
        ("s", S),
        ("ms", MS),
        ("us", US),
        ("ns", NS),
    ] {
        if nanos.is_multiple_of(scale) {
            return format!("{}{}", nanos / scale, unit);
        }
    }
    format!("{nanos}ns")
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

fn deserialize_dns_strategy<'de, D>(deserializer: D) -> Result<DomainStrategy, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_domain_strategy("dns.strategy", &value).map_err(de::Error::custom)
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

#[cfg(feature = "wireguard")]
fn deserialize_wireguard_addresses<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("endpoints.address", deserializer)
}

#[cfg(feature = "wireguard")]
fn deserialize_wireguard_allowed_ips<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("endpoints.peers.allowed_ips", deserializer)
}

fn deserialize_route_ip_cidrs<'de, D>(deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_ipnet_vec("route.rules.ip_cidr", deserializer)
}

fn deserialize_ipnet_vec<'de, D>(field: &str, deserializer: D) -> Result<Vec<IpNet>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|value| parse_ipnet(field, &value).map_err(de::Error::custom))
        .collect()
}

fn serialize_ipnet_vec<S>(value: &[IpNet], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .serialize(serializer)
}

fn deserialize_route_rules<'de, D>(deserializer: D) -> Result<Vec<RawRouteRule>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<RawRouteRuleText>::deserialize(deserializer)?
        .into_iter()
        .enumerate()
        .map(|(idx, raw)| route_rule_from_text(idx, raw).map_err(de::Error::custom))
        .collect()
}

fn route_rule_from_text(idx: usize, raw: RawRouteRuleText) -> Result<RawRouteRule, String> {
    let RawRouteRuleText {
        inbound,
        protocol,
        domain,
        domain_suffix,
        domain_keyword,
        ip_cidr,
        outbound,
    } = raw;
    let mut count = 0;
    let mut matcher = RuleMatcher::Any;
    if !inbound.is_empty() {
        count += 1;
        matcher = RuleMatcher::Inbound(inbound);
    }
    if !protocol.is_empty() {
        count += 1;
        matcher = RuleMatcher::Protocol(protocol);
    }
    if !domain.is_empty() {
        count += 1;
        matcher = RuleMatcher::Domain(domain);
    }
    if !domain_suffix.is_empty() {
        count += 1;
        matcher = RuleMatcher::DomainSuffix(domain_suffix);
    }
    if !domain_keyword.is_empty() {
        count += 1;
        matcher = RuleMatcher::DomainKeyword(domain_keyword);
    }
    if !ip_cidr.is_empty() {
        count += 1;
        matcher = RuleMatcher::IpCidr(ip_cidr);
    }
    match count {
        1 => Ok(RawRouteRule { matcher, outbound }),
        0 => Err(format!(
            "route.rules[{idx}] requires exactly one matcher (inbound/protocol/domain/domain_suffix/domain_keyword/ip_cidr)",
        )),
        _ => Err(format!(
            "route.rules[{idx}] must contain exactly one matcher (inbound/protocol/domain/domain_suffix/domain_keyword/ip_cidr)",
        )),
    }
}

fn route_rule_to_text(raw: &RawRouteRule) -> RawRouteRuleText {
    let mut text = RawRouteRuleText {
        outbound: raw.outbound.clone(),
        ..Default::default()
    };
    match &raw.matcher {
        RuleMatcher::Any => {}
        RuleMatcher::Inbound(values) => text.inbound = values.clone(),
        RuleMatcher::Protocol(values) => text.protocol = values.clone(),
        RuleMatcher::Domain(values) => text.domain = values.clone(),
        RuleMatcher::DomainSuffix(values) => text.domain_suffix = values.clone(),
        RuleMatcher::DomainKeyword(values) => text.domain_keyword = values.clone(),
        RuleMatcher::IpCidr(values) => text.ip_cidr = values.clone(),
    }
    text
}

fn serialize_route_rules<S>(value: &[RawRouteRule], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value
        .iter()
        .map(route_rule_to_text)
        .collect::<Vec<_>>()
        .serialize(serializer)
}

fn deserialize_hysteria2_networks<'de, D>(
    deserializer: D,
) -> Result<Vec<Hysteria2Network>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|value| match value.as_str() {
            "tcp" => Ok(Hysteria2Network::Tcp),
            "udp" => Ok(Hysteria2Network::Udp),
            other => Err(de::Error::custom(format!(
                "hysteria2.network: unknown network {other:?}"
            ))),
        })
        .collect()
}

fn deserialize_hysteria2_bbr_profile<'de, D>(
    deserializer: D,
) -> Result<Option<Hysteria2BbrProfile>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "" => Ok(None),
        "standard" => Ok(Some(Hysteria2BbrProfile::Standard)),
        "conservative" => Ok(Some(Hysteria2BbrProfile::Conservative)),
        "aggressive" => Ok(Some(Hysteria2BbrProfile::Aggressive)),
        other => Err(de::Error::custom(format!(
            "hysteria2.bbr_profile unsupported BBR profile: {other}"
        ))),
    }
}

fn deserialize_hysteria2_obfs_type<'de, D>(
    deserializer: D,
) -> Result<Option<Hysteria2ObfsType>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "" => Ok(None),
        "salamander" => Ok(Some(Hysteria2ObfsType::Salamander)),
        other => Err(de::Error::custom(format!(
            "hysteria2.obfs.type: unknown obfs type {other:?}"
        ))),
    }
}
