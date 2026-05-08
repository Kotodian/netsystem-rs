use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hammer_adapter::{
    Network, OutboundManager as OutboundManagerTrait, RouteDecision, RouteMetadata,
    Router as RouterTrait, SocksAddr,
};
use hammer_core::config::{RouteOptions, Rule as RuleOptions, RuleActionKind, RuleMatcher};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hammer_core::metrics::{MetricsRegistry, MetricsScope, NetworkCounters};
use lru::LruCache;

use crate::OutboundManager;
use crate::impl_logging_lifecycle;

const DEFAULT_SNIFF_TIMEOUT: Duration = Duration::from_millis(300);

/// Capacity of the metadata→decision LRU. Sized like `SYSTEM_UDP_FLOW_CAPACITY`
/// (`tun/stack.rs`) so a sustained UDP flow set fits without thrashing while
/// staying below ~100 KiB on the NetExt heap (each entry ≤ ~250 B with the
/// owned `String` keys).
const ROUTER_CACHE_CAPACITY: usize = 256;

/// Subset of `RouteMetadata` that affects rule matching + the default-route
/// fall-through. Anything *mutated* by non-terminal rules (Sniff / Resolve /
/// RouteOptions) is excluded — by the time `match_route` runs, the dispatch
/// path has already invoked `prepare_route_metadata`, so those fields are
/// fully resolved and a cache hit can safely skip the second walk.
///
/// Only fields actually consumed by `matcher_matches` + `route_to_default`
/// land here; src port / src IP / client / domain_strategy / mutation flags
/// are intentionally absent.
#[derive(Hash, PartialEq, Eq, Clone)]
struct MatchKey {
    network: Network,
    inbound: String,
    protocol: String,
    domain: Option<String>,
    destination_host: Option<IpAddr>,
}

impl From<&RouteMetadata> for MatchKey {
    #[inline]
    fn from(metadata: &RouteMetadata) -> Self {
        Self {
            network: metadata.network,
            inbound: metadata.inbound.clone(),
            protocol: metadata.protocol.clone(),
            domain: metadata.domain.clone(),
            destination_host: metadata.destination.as_ref().map(|d| d.host),
        }
    }
}

/// `route.Router` — matches each connection against the configured rule set and
/// returns a [`RouteDecision`]. Each rule has exactly one matcher; values inside
/// that matcher are OR'd. The first rule that produces a terminal decision wins;
/// non-terminal rules (sniff/resolve/route_options) mutate metadata and fall
/// through.
pub struct Router {
    rules: Vec<RuntimeRule>,
    outbound: Option<Arc<OutboundManager>>,
    default_outbound: String,
    metrics: RouterMetrics,
    /// Per-(metadata-snapshot) cache of the terminal decision. UDP/ICMP
    /// dispatch calls `match_route` on every packet; the same 5-tuple
    /// repeats for the lifetime of a flow, so a small LRU collapses N
    /// rule walks into a single hashmap lookup. TCP gets one match per
    /// accepted connection so its hit rate is lower but still positive
    /// for repeated dials to the same target.
    cache: Mutex<LruCache<MatchKey, RouteDecision>>,
}

#[inline]
fn new_router_cache() -> Mutex<LruCache<MatchKey, RouteDecision>> {
    Mutex::new(LruCache::new(
        NonZeroUsize::new(ROUTER_CACHE_CAPACITY)
            .expect("router cache capacity must be non-zero"),
    ))
}

impl Router {
    pub fn new(_logger: Logger) -> Self {
        let metrics = MetricsRegistry::new().scope("route", "router", "default");
        Self {
            rules: Vec::new(),
            outbound: None,
            default_outbound: String::new(),
            metrics: RouterMetrics::new(&metrics, ""),
            cache: new_router_cache(),
        }
    }

    pub fn from_options(
        _logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
    ) -> Result<Self, HammerError> {
        Self::from_options_with_metrics(_logger, options, outbound, MetricsRegistry::new())
    }

    pub fn from_options_with_metrics(
        _logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
        metrics: Arc<MetricsRegistry>,
    ) -> Result<Self, HammerError> {
        let scope = metrics.scope("route", "router", "default");
        let rules: Vec<RuntimeRule> = options
            .rules
            .into_iter()
            .enumerate()
            .map(|(index, rule)| RuntimeRule::from_options(index, rule, &scope))
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
            metrics: RouterMetrics::new(&scope, &options.final_),
            default_outbound: options.final_,
            cache: new_router_cache(),
        })
    }

    pub fn match_route(&self, metadata: &mut RouteMetadata) -> Result<RouteDecision, HammerError> {
        let network = metadata.network;

        // Cache the terminal decision keyed on the subset of metadata that
        // actually drives matcher_matches + route_to_default. The dispatch
        // path runs `prepare_route_metadata` first so all non-terminal
        // mutations (Sniff override_destination, Resolve domain_strategy,
        // RouteOptions udp_disable_domain_unmapping) are already applied
        // by the time we land here — a hit can safely skip the second walk.
        let key = MatchKey::from(&*metadata);
        if let Some(cached) = self
            .cache
            .lock()
            .expect("router cache poisoned")
            .get(&key)
            .cloned()
        {
            self.metrics.cache_hit_total.inc(network);
            return Ok(cached);
        }
        self.metrics.cache_miss_total.inc(network);

        for rule in &self.rules {
            if !rule.matches(metadata) {
                continue;
            }
            match rule.apply(metadata) {
                Ok(RuleApply::Continue) => {}
                Ok(RuleApply::Decision(decision)) => {
                    self.cache
                        .lock()
                        .expect("router cache poisoned")
                        .put(key, decision.clone());
                    return Ok(decision);
                }
                Err(err) => {
                    self.metrics.error_total.inc(network);
                    rule.metrics.error_total.inc(network);
                    return Err(err);
                }
            }
        }
        match self.route_to_default(network) {
            Ok(decision) => {
                self.cache
                    .lock()
                    .expect("router cache poisoned")
                    .put(key, decision.clone());
                Ok(decision)
            }
            Err(err) => {
                self.metrics.error_total.inc(network);
                Err(err)
            }
        }
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
        // Network change can flip reverse-DNS state and (eventually) any
        // network-aware matcher; flush the metadata→decision cache so the
        // next packet picks up the new view. No matcher state needs
        // resetting yet.
        self.cache
            .lock()
            .expect("router cache poisoned")
            .clear();
    }
}

#[derive(Clone)]
struct RouterMetrics {
    error_total: NetworkCounters,
    cache_hit_total: NetworkCounters,
    cache_miss_total: NetworkCounters,
}

impl RouterMetrics {
    fn new(scope: &MetricsScope, _default_outbound: &str) -> Self {
        Self {
            error_total: NetworkCounters::new(scope, "error_total"),
            cache_hit_total: NetworkCounters::new(scope, "cache_hit_total"),
            cache_miss_total: NetworkCounters::new(scope, "cache_miss_total"),
        }
    }
}

#[derive(Clone)]
struct RuntimeRuleMetrics {
    error_total: NetworkCounters,
}

impl RuntimeRuleMetrics {
    fn new(scope: &MetricsScope, index: usize, action: &RuleActionKind) -> Self {
        let mut labels = action_metric_labels(action);
        labels.push(("rule".to_owned(), index.to_string()));
        Self {
            error_total: NetworkCounters::with_labels(scope, "error_total", &labels),
        }
    }
}

fn action_metric_labels(action: &RuleActionKind) -> Vec<(String, String)> {
    let mut labels = vec![("action".to_owned(), route_action_name(action).to_owned())];
    match action {
        RuleActionKind::Route(action) => {
            labels.push(("outbound".to_owned(), action.outbound.clone()));
        }
        RuleActionKind::Reject(action) => {
            labels.push(("method".to_owned(), action.method.clone()));
        }
        _ => {}
    }
    labels
}

fn route_action_name(action: &RuleActionKind) -> &'static str {
    match action {
        RuleActionKind::Sniff(_) => "sniff",
        RuleActionKind::HijackDns => "hijack_dns",
        RuleActionKind::Reject(_) => "reject",
        RuleActionKind::Resolve(_) => "resolve",
        RuleActionKind::RouteOptions(_) => "route_options",
        RuleActionKind::Route(_) => "route",
    }
}

struct RuntimeRule {
    matcher: RuleMatcher,
    action: RuleActionKind,
    metrics: RuntimeRuleMetrics,
}

impl RuntimeRule {
    fn from_options(index: usize, options: RuleOptions, router_scope: &MetricsScope) -> Self {
        let default = options.default_options;
        let action = default.action;
        let metrics = RuntimeRuleMetrics::new(
            &router_scope.child("rule", index.to_string()),
            index,
            &action,
        );
        Self {
            matcher: default.matcher,
            action,
            metrics,
        }
    }

    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        matcher_matches(&self.matcher, metadata)
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

/// Match a `RuleMatcher` against connection metadata. Hot path — called per
/// TCP/UDP/ICMP packet, once per rule walked. Static dispatch on the enum
/// (not `Box<dyn Matcher>`) lets LLVM inline each arm with workspace
/// `lto = "fat"`. The previous trait-object form ate one indirect call per
/// rule; cumulative on every packet × every rule it added up to a real chunk
/// of `Router::match_route` self-time on iOS NetExt.
#[inline]
fn matcher_matches(matcher: &RuleMatcher, metadata: &RouteMetadata) -> bool {
    match matcher {
        RuleMatcher::Any => true,
        RuleMatcher::Inbound(values) => match_list(values, &metadata.inbound),
        RuleMatcher::Protocol(values) => match_list(values, &metadata.protocol),
        RuleMatcher::Domain(values) => match metadata.domain.as_deref() {
            Some(domain) => values.iter().any(|value| value == domain),
            None => false,
        },
        RuleMatcher::DomainSuffix(values) => match metadata.domain.as_deref() {
            Some(domain) => values.iter().any(|suffix| domain_suffix_matches(domain, suffix)),
            None => false,
        },
        RuleMatcher::DomainKeyword(values) => match metadata.domain.as_deref() {
            Some(domain) => values.iter().any(|keyword| domain.contains(keyword.as_str())),
            None => false,
        },
        RuleMatcher::IpCidr(values) => match metadata.destination.as_ref() {
            Some(SocksAddr { host, .. }) => values.iter().any(|net| net.contains(host)),
            None => false,
        },
    }
}

#[inline]
fn match_list(values: &[String], actual: &str) -> bool {
    values.is_empty() || values.iter().any(|value| value == actual)
}
