//! `[route]` config section: rules, matchers, and the action enum.
//!
//! Tun-derived rules (sniff / block-quic / domain-strategy /
//! udp-disable-domain-unmapping) are synthesized from `[tun]` switches by
//! `derive_tun_route_rules` and prepended in front of any user
//! `[[route.rules]]` entries; mod.rs's `build_options` does the splice.

use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::error::HammerError;

use super::constants as C;
use super::inbound::RawTunConfig;
use super::raw_struct_with_default_check;
use super::util::normalize_domain;

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    ip_cidr: Vec<IpNet>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    outbound: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteOptions {
    pub final_: String,
    pub auto_detect_interface: bool,
    pub rules: Vec<Rule>,
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
    Reject(RejectActionOptions),
    Resolve(ResolveActionOptions),
    RouteOptions(RouteOptionsActionOptions),
    Route(RouteActionOptions),
}

impl Default for RuleActionKind {
    fn default() -> Self {
        Self::Resolve(ResolveActionOptions::default())
    }
}

impl RuleActionKind {
    pub fn name(&self) -> &'static str {
        match self {
            RuleActionKind::Sniff(_) => "sniff",
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteOptionsActionOptions {
    pub udp_disable_domain_unmapping: bool,
}

/// Materialise the implicit rules a TUN inbound implies (sniff /
/// block-quic / domain-strategy / udp-disable-domain-unmapping). The user's
/// own `[[route.rules]]` entries land after these.
pub(super) fn derive_tun_route_rules(
    raw_tun: &RawTunConfig,
    tun_id: &str,
) -> Result<Vec<Rule>, HammerError> {
    let sniff_timeout = raw_tun.sniff_timeout;
    let domain_strategy = raw_tun.domain_strategy;
    let sniff = raw_tun.sniff.unwrap_or(false);
    let sniff_override_destination = raw_tun.sniff_override_destination.unwrap_or(false);
    let block_quic = raw_tun.block_quic.unwrap_or(false);
    let udp_disable_domain_unmapping = raw_tun.udp_disable_domain_unmapping.unwrap_or(false);

    if sniff_override_destination && !sniff {
        return Err(HammerError::config_validation(
            "tun.sniff_override_destination requires tun.sniff=true",
        ));
    }
    if block_quic && !sniff {
        return Err(HammerError::config_validation(
            "tun.block_quic requires tun.sniff=true",
        ));
    }
    let mut rules = Vec::new();
    if sniff {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::Sniff(SniffActionOptions {
                timeout: sniff_timeout,
                override_destination: sniff_override_destination,
            }),
        ));
    }
    if domain_strategy != DomainStrategy::AsIs {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::Resolve(ResolveActionOptions {
                strategy: domain_strategy,
            }),
        ));
    }
    if udp_disable_domain_unmapping {
        rules.push(tun_rule(
            tun_id,
            RuleActionKind::RouteOptions(RouteOptionsActionOptions {
                udp_disable_domain_unmapping: true,
            }),
        ));
    }
    if block_quic {
        rules.push(protocol_rule(
            C::PROTOCOL_QUIC,
            RuleActionKind::Reject(RejectActionOptions {
                method: C::REJECT_METHOD_DEFAULT.to_owned(),
            }),
        ));
    }
    Ok(rules)
}

pub(super) fn build_user_rules(raw: &[RawRouteRule]) -> Result<Vec<Rule>, HammerError> {
    raw.iter()
        .enumerate()
        .map(|(idx, raw)| build_user_rule(idx, raw))
        .collect()
}

fn build_user_rule(idx: usize, raw: &RawRouteRule) -> Result<Rule, HammerError> {
    if raw.outbound.is_empty() {
        return Err(HammerError::config_validation(format!(
            "route.rules[{idx}].outbound is required",
        )));
    }
    Ok(Rule {
        default_options: DefaultRule {
            matcher: raw.matcher.clone(),
            action: RuleActionKind::Route(RouteActionOptions {
                outbound: raw.outbound.clone(),
            }),
        },
    })
}

fn tun_rule(tun_id: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            matcher: RuleMatcher::Inbound(vec![tun_id.to_owned()]),
            action,
        },
    }
}

fn protocol_rule(protocol: &str, action: RuleActionKind) -> Rule {
    Rule {
        default_options: DefaultRule {
            matcher: RuleMatcher::Protocol(vec![protocol.to_owned()]),
            action,
        },
    }
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

/// Apply `normalize_domain` across a vec; drops empties so the matcher's
/// "any value matches" loop never has to short-circuit on blanks.
fn normalize_domain_values(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|v| normalize_domain(&v))
        .filter(|v| !v.is_empty())
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
        matcher = RuleMatcher::Domain(normalize_domain_values(domain));
    }
    if !domain_suffix.is_empty() {
        count += 1;
        matcher = RuleMatcher::DomainSuffix(normalize_domain_values(domain_suffix));
    }
    if !domain_keyword.is_empty() {
        count += 1;
        // Keyword is a substring search, not a label match. We still
        // lowercase so case-insensitive substring contains works against
        // sniffed domain values that have already been normalised.
        matcher = RuleMatcher::DomainKeyword(normalize_domain_values(domain_keyword));
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
