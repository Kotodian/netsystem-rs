use std::sync::Arc;

use hammer_adapter::{
    Network, OutboundManager as OutboundManagerTrait, RouteDecision, RouteMetadata,
    Router as RouterTrait,
};
use hammer_core::config::{RouteOptions, Rule as RuleOptions, RuleActionKind};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::OutboundManager;
use crate::impl_logging_lifecycle;

/// `route.Router` skeleton. The rule engine + connection routing hot path
/// arrive in M4.
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
    ) -> Self {
        Self {
            logger,
            rules: options
                .rules
                .into_iter()
                .map(RuntimeRule::from_options)
                .collect(),
            outbound: Some(outbound),
            default_outbound: options.final_,
        }
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
}

impl_logging_lifecycle!(Router, "router");

impl RouterTrait for Router {
    fn reset_network(&self) {
        self.logger.debug("reset_network (M2 stub)");
    }
}

#[derive(Clone)]
struct RuntimeRule {
    inbound: Vec<String>,
    protocol: Vec<String>,
    action: RuleActionKind,
}

impl RuntimeRule {
    fn from_options(options: RuleOptions) -> Self {
        let default = options.default_options;
        Self {
            inbound: default.inbound,
            protocol: default.protocol,
            action: default.action,
        }
    }

    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.inbound, &metadata.inbound)
            && match_list(&self.protocol, &metadata.protocol)
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
        }
    }
}

enum RuleApply {
    Continue,
    Decision(RouteDecision),
}

impl Router {
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

fn match_list(values: &[String], actual: &str) -> bool {
    values.is_empty() || values.iter().any(|value| value == actual)
}
