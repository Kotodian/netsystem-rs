use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[cfg(feature = "wireguard")]
use super::endpoint::RawEndpoint;
use super::inbound::RawTunConfig;
use super::log::RawLogConfig;
use super::options::{DomainStrategy, RuleMatcher};
use super::outbound::RawHysteria2Config;
use super::parse::parse_ipnet;
use super::raw_struct_with_default_check;

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

fn deserialize_dns_strategy<'de, D>(deserializer: D) -> Result<DomainStrategy, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    super::parse::parse_domain_strategy("dns.strategy", &value).map_err(de::Error::custom)
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
    super::parse::serialize_ipnet_vec(value, serializer)
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
