use std::sync::Arc;

use hammer_adapter::{
    Network, OutboundManager as OutboundManagerTrait, RouteDecision, RouteMetadata,
    Router as RouterTrait, SocksAddr,
};
use hammer_core::config::{RouteOptions, Rule as RuleOptions, RuleActionKind};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use ipnet::IpNet;

use crate::OutboundManager;
use crate::impl_logging_lifecycle;

/// `route.Router` — matches each connection against the configured rule set and
/// returns a [`RouteDecision`]. Rule fields within a single rule are AND'd;
/// values inside a single field (e.g. multiple `domain_suffix` entries) are
/// OR'd. The first rule that produces a terminal decision wins; non-terminal
/// rules (sniff/resolve/route_options) mutate metadata and fall through.
pub struct Router {
    logger: Logger,
    rules: Vec<RuntimeRule>,
    outbound: Option<Arc<OutboundManager>>,
    default_outbound: String,
}

impl Router {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            rules: Vec::new(),
            outbound: None,
            default_outbound: String::new(),
        }
    }

    pub fn from_options(
        logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
    ) -> Result<Self, HammerError> {
        let rules: Vec<RuntimeRule> = options
            .rules
            .into_iter()
            .map(RuntimeRule::from_options)
            .collect();
        for rule in &rules {
            if let RuleActionKind::Route(action) = &rule.action {
                if outbound.get(&action.outbound).is_none() {
                    return Err(HammerError::config_validation(format!(
                        "route.rules outbound {:?} is not declared",
                        action.outbound,
                    )));
                }
            }
        }
        Ok(Self {
            logger,
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

    fn route_to_default(&self, network: Network) -> Result<RouteDecision, HammerError> {
        let Some(outbound) = &self.outbound else {
            return Ok(RouteDecision::Route {
                outbound: self.default_outbound.clone(),
            });
        };
        let Some(default) = outbound.default() else {
            return Err(HammerError::internal("default outbound not found"));
        };
        if !default.networks().contains(&network) {
            return Err(HammerError::internal(format!(
                "{network} is not supported by default outbound: {}",
                default.tag()
            )));
        }
        Ok(RouteDecision::Route {
            outbound: default.tag().to_owned(),
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

#[derive(Clone)]
struct RuntimeRule {
    inbound: Vec<String>,
    protocol: Vec<String>,
    domain: Vec<String>,
    domain_suffix: Vec<String>,
    domain_keyword: Vec<String>,
    ip_cidr: Vec<IpNet>,
    action: RuleActionKind,
}

impl RuntimeRule {
    fn from_options(options: RuleOptions) -> Self {
        let default = options.default_options;
        Self {
            inbound: default.inbound,
            protocol: default.protocol,
            domain: default.domain,
            domain_suffix: default.domain_suffix,
            domain_keyword: default.domain_keyword,
            ip_cidr: default.ip_cidr,
            action: default.action,
        }
    }

    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.inbound, &metadata.inbound)
            && match_list(&self.protocol, &metadata.protocol)
            && self.matches_domain(metadata.domain.as_deref())
            && self.matches_ip_cidr(metadata.destination.as_ref())
    }

    fn matches_domain(&self, domain: Option<&str>) -> bool {
        if self.domain.is_empty() && self.domain_suffix.is_empty() && self.domain_keyword.is_empty()
        {
            return true;
        }
        let Some(d) = domain else {
            return false;
        };
        if self.domain.iter().any(|x| x == d) {
            return true;
        }
        if self
            .domain_suffix
            .iter()
            .any(|suffix| domain_suffix_matches(d, suffix))
        {
            return true;
        }
        if self.domain_keyword.iter().any(|kw| d.contains(kw)) {
            return true;
        }
        false
    }

    fn matches_ip_cidr(&self, dest: Option<&SocksAddr>) -> bool {
        if self.ip_cidr.is_empty() {
            return true;
        }
        let Some(addr) = dest else {
            return false;
        };
        self.ip_cidr.iter().any(|net| net.contains(&addr.host))
    }

    fn apply(&self, metadata: &mut RouteMetadata) -> Result<RuleApply, HammerError> {
        match &self.action {
            RuleActionKind::Sniff(_) => Ok(RuleApply::Continue),
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

#[inline]
fn match_list(values: &[String], actual: &str) -> bool {
    values.is_empty() || values.iter().any(|value| value == actual)
}
