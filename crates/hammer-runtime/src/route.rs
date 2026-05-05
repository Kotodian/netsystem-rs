use std::sync::Arc;
use std::time::Duration;

use hammer_adapter::{
    Network, OutboundManager as OutboundManagerTrait, RouteDecision, RouteMetadata,
    Router as RouterTrait, SocksAddr,
};
use hammer_core::config::{RouteOptions, Rule as RuleOptions, RuleActionKind, RuleMatcher};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use ipnet::IpNet;

use crate::OutboundManager;
use crate::impl_logging_lifecycle;

const DEFAULT_SNIFF_TIMEOUT: Duration = Duration::from_millis(300);

/// `route.Router` — matches each connection against the configured rule set and
/// returns a [`RouteDecision`]. Each rule has exactly one matcher; values inside
/// that matcher are OR'd. The first rule that produces a terminal decision wins;
/// non-terminal rules (sniff/resolve/route_options) mutate metadata and fall
/// through.
pub struct Router {
    rules: Vec<RuntimeRule>,
    outbound: Option<Arc<OutboundManager>>,
    default_outbound: String,
}

impl Router {
    pub fn new(_logger: Logger) -> Self {
        Self {
            rules: Vec::new(),
            outbound: None,
            default_outbound: String::new(),
        }
    }

    pub fn from_options(
        _logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
    ) -> Result<Self, HammerError> {
        let rules: Vec<RuntimeRule> = options
            .rules
            .into_iter()
            .map(RuntimeRule::from_options)
            .collect();
        for rule in &rules {
            if let RuleActionKind::Route(action) = &rule.action
                && outbound.get(&action.outbound).is_none()
            {
                return Err(HammerError::config_validation(format!(
                    "route.rules outbound {:?} is not declared",
                    action.outbound,
                )));
            }
        }
        if outbound.get(&options.final_).is_none() {
            return Err(HammerError::config_validation(format!(
                "route.final outbound {:?} is not declared",
                options.final_
            )));
        }
        Ok(Self {
            rules,
            outbound: Some(outbound),
            default_outbound: options.final_,
        })
    }

    pub fn match_route(&self, metadata: &mut RouteMetadata) -> Result<RouteDecision, HammerError> {
        for rule in &self.rules {
            if !rule.matches(metadata) {
                continue;
            }
            match rule.apply(metadata)? {
                RuleApply::Continue => {}
                RuleApply::Decision(decision) => return Ok(decision),
            }
        }
        self.route_to_default(metadata.network)
    }

    #[cfg(feature = "inbound-tun")]
    pub(crate) fn prepare_route_metadata(
        &self,
        metadata: &mut RouteMetadata,
    ) -> Result<(), HammerError> {
        for rule in &self.rules {
            if !rule.matches(metadata) {
                continue;
            }
            match rule.apply(metadata)? {
                RuleApply::Continue => {}
                RuleApply::Decision(_) => return Ok(()),
            }
        }
        Ok(())
    }

    pub fn sniff_timeout(&self, metadata: &RouteMetadata) -> Option<Duration> {
        self.rules.iter().find_map(|rule| {
            if !rule.matches(metadata) {
                return None;
            }
            let RuleActionKind::Sniff(options) = &rule.action else {
                return None;
            };
            Some(options.timeout.unwrap_or(DEFAULT_SNIFF_TIMEOUT))
        })
    }

    pub fn should_sniff(&self, metadata: &RouteMetadata) -> bool {
        self.sniff_timeout(metadata).is_some()
    }

    fn route_to_default(&self, network: Network) -> Result<RouteDecision, HammerError> {
        let Some(outbound) = &self.outbound else {
            return Ok(RouteDecision::Route {
                outbound: self.default_outbound.clone(),
            });
        };
        let Some(default) = outbound.default() else {
            return Err(HammerError::internal("default outbound not found"));
        };
        // ICMP is treated as a soft-supported network: the dispatch
        // path detects an outbound that lacks ICMP via `listen_icmp()`
        // and synthesises a Destination Unreachable back into the tun.
        // Skipping the strict pre-flight check here lets a user keep a
        // simple `final = "hysteria2"` route (which only declares
        // Tcp/Udp) and still receive a clean ICMP reply rather than a
        // hard route engine error every time an app pings.
        if network != Network::Icmp && !default.networks().contains(&network) {
            return Err(HammerError::internal(format!(
                "{network} is not supported by default outbound: {}",
                default.id()
            )));
        }
        Ok(RouteDecision::Route {
            outbound: default.id().to_owned(),
        })
    }
}

impl_logging_lifecycle!(Router, "router");

impl RouterTrait for Router {
    fn reset_network(&self) {
        // No matchers currently depend on network state. When geoip / rule_set
        // caching lands, invalidate them here.
    }
}

struct RuntimeRule {
    matcher: Box<dyn Matcher>,
    action: RuleActionKind,
}

impl RuntimeRule {
    fn from_options(options: RuleOptions) -> Self {
        let default = options.default_options;
        Self {
            matcher: matcher_from_options(default.matcher),
            action: default.action,
        }
    }

    fn matches(&self, metadata: &RouteMetadata) -> bool {
        self.matcher.matches(metadata)
    }

    fn apply(&self, metadata: &mut RouteMetadata) -> Result<RuleApply, HammerError> {
        match &self.action {
            RuleActionKind::Sniff(o) => {
                if o.override_destination {
                    metadata.override_destination = true;
                }
                Ok(RuleApply::Continue)
            }
            RuleActionKind::HijackDns => Ok(RuleApply::Decision(RouteDecision::HijackDns)),
            RuleActionKind::Reject(o) => Ok(RuleApply::Decision(RouteDecision::Reject {
                method: o.method.clone(),
            })),
            RuleActionKind::Resolve(o) => {
                metadata.domain_strategy = Some(o.strategy);
                Ok(RuleApply::Continue)
            }
            RuleActionKind::RouteOptions(o) => {
                if o.udp_disable_domain_unmapping {
                    metadata.udp_disable_domain_unmapping = true;
                }
                Ok(RuleApply::Continue)
            }
            RuleActionKind::Route(o) => Ok(RuleApply::Decision(RouteDecision::Route {
                outbound: o.outbound.clone(),
            })),
        }
    }
}

#[inline]
fn domain_suffix_matches(domain: &str, suffix: &str) -> bool {
    if suffix.is_empty() {
        return false;
    }
    if domain == suffix {
        return true;
    }
    domain.len() > suffix.len()
        && domain.ends_with(suffix)
        && domain.as_bytes()[domain.len() - suffix.len() - 1] == b'.'
}

enum RuleApply {
    Continue,
    Decision(RouteDecision),
}

trait Matcher: Send + Sync {
    fn matches(&self, metadata: &RouteMetadata) -> bool;
}

fn matcher_from_options(matcher: RuleMatcher) -> Box<dyn Matcher> {
    match matcher {
        RuleMatcher::Any => Box::new(AnyMatcher),
        RuleMatcher::Inbound(values) => Box::new(InboundMatcher(values)),
        RuleMatcher::Protocol(values) => Box::new(ProtocolMatcher(values)),
        RuleMatcher::Domain(values) => Box::new(DomainMatcher(values)),
        RuleMatcher::DomainSuffix(values) => Box::new(DomainSuffixMatcher(values)),
        RuleMatcher::DomainKeyword(values) => Box::new(DomainKeywordMatcher(values)),
        RuleMatcher::IpCidr(values) => Box::new(IpCidrMatcher(values)),
    }
}

struct AnyMatcher;

impl Matcher for AnyMatcher {
    fn matches(&self, _metadata: &RouteMetadata) -> bool {
        true
    }
}

struct InboundMatcher(Vec<String>);

impl Matcher for InboundMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.0, &metadata.inbound)
    }
}

struct ProtocolMatcher(Vec<String>);

impl Matcher for ProtocolMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.0, &metadata.protocol)
    }
}

struct DomainMatcher(Vec<String>);

impl Matcher for DomainMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        let Some(domain) = metadata.domain.as_deref() else {
            return false;
        };
        self.0.iter().any(|value| value == domain)
    }
}

struct DomainSuffixMatcher(Vec<String>);

impl Matcher for DomainSuffixMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        let Some(domain) = metadata.domain.as_deref() else {
            return false;
        };
        self.0
            .iter()
            .any(|suffix| domain_suffix_matches(domain, suffix))
    }
}

struct DomainKeywordMatcher(Vec<String>);

impl Matcher for DomainKeywordMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        let Some(domain) = metadata.domain.as_deref() else {
            return false;
        };
        self.0.iter().any(|keyword| domain.contains(keyword))
    }
}

struct IpCidrMatcher(Vec<IpNet>);

impl Matcher for IpCidrMatcher {
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        let Some(SocksAddr { host, .. }) = metadata.destination.as_ref() else {
            return false;
        };
        self.0.iter().any(|net| net.contains(host))
    }
}

#[inline]
fn match_list(values: &[String], actual: &str) -> bool {
    values.is_empty() || values.iter().any(|value| value == actual)
}
