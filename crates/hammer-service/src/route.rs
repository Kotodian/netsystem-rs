use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use hammer_adapter::{
    Lifecycle, Network, OutboundManager as OutboundManagerTrait, RouteDecision, RouteMetadata,
    RouteTarget, Router as RouterTrait, SocksAddr, StartStage,
};
use hammer_core::config::{RouteOptions, Rule as RuleOptions, RuleActionKind, RuleMatcher};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;
use hammer_core::metrics::{MetricsRegistry, MetricsScope, NetworkCounters};
use ipnet::IpNet;

use crate::OutboundManager;
use crate::component_registry::register_components;

const DEFAULT_SNIFF_TIMEOUT: Duration = Duration::from_millis(300);

/// Capacity of the metadata-to-decision cache. Sized like `SYSTEM_UDP_FLOW_CAPACITY`
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
/// Only fields actually consumed by `RuntimeMatcher::matches` +
/// `route_to_default`
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
#[hammer_component_macros::hammer_component(router, name = "default", builder = build_router)]
pub struct Router {
    rules: Vec<RuntimeRule>,
    outbound: Option<Arc<OutboundManager>>,
    default_target: RouteTarget,
    metrics: RouterMetrics,
    cache: ArcSwap<RouteCacheSnapshot>,
}

#[derive(Clone, Default)]
struct RouteCacheSnapshot {
    decisions: HashMap<MatchKey, RouteDecision>,
}

#[inline]
fn new_router_cache() -> ArcSwap<RouteCacheSnapshot> {
    ArcSwap::from_pointee(RouteCacheSnapshot::default())
}

pub(crate) type RouterBuilder =
    fn(Logger, RouteOptions, Arc<OutboundManager>, Arc<MetricsRegistry>) -> HammerResult<Router>;
pub(crate) type MatcherBuilder = fn(RuleMatcher) -> HammerResult<RuntimeMatcher>;

#[derive(Clone)]
struct RouterFactorySet {
    builders: Arc<HashMap<&'static str, RouterBuilder>>,
}

impl RouterFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_router_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build_default(
        &self,
        logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Router> {
        self.build("default", logger, options, outbound, metrics)
    }

    fn build(
        &self,
        type_name: &str,
        logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Router> {
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown router type: {type_name}"))
        })?;
        builder(logger, options, outbound, metrics)
    }
}

fn register_standard_router_builders(builders: &mut HashMap<&'static str, RouterBuilder>) {
    register_components!(router, builders, [Router]);
}

#[derive(Clone)]
struct RouteMatcherFactorySet {
    builders: Arc<HashMap<&'static str, MatcherBuilder>>,
}

impl RouteMatcherFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_route_matcher_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build(&self, matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
        let type_name = route_matcher_type_name(&matcher);
        self.build_with_type(type_name, matcher)
    }

    fn build_with_type(
        &self,
        type_name: &str,
        matcher: RuleMatcher,
    ) -> HammerResult<RuntimeMatcher> {
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown route matcher type: {type_name}"))
        })?;
        builder(matcher)
    }
}

fn register_standard_route_matcher_builders(builders: &mut HashMap<&'static str, MatcherBuilder>) {
    register_components!(
        matcher,
        builders,
        [
            AnyMatcher,
            InboundMatcher,
            ProtocolMatcher,
            DomainMatcher,
            DomainSuffixMatcher,
            DomainKeywordMatcher,
            IpCidrMatcher,
        ]
    );
}

fn route_matcher_type_name(matcher: &RuleMatcher) -> &'static str {
    match matcher {
        RuleMatcher::Any => {
            <AnyMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::Inbound(_) => {
            <InboundMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::Protocol(_) => {
            <ProtocolMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::Domain(_) => {
            <DomainMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::DomainSuffix(_) => {
            <DomainSuffixMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::DomainKeyword(_) => {
            <DomainKeywordMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
        RuleMatcher::IpCidr(_) => {
            <IpCidrMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME
        }
    }
}

pub(crate) fn build_router(
    _logger: Logger,
    options: RouteOptions,
    outbound: Arc<OutboundManager>,
    metrics: Arc<MetricsRegistry>,
) -> HammerResult<Router> {
    build_router_with_endpoint_ids(options, outbound, metrics, std::iter::empty::<String>())
}

fn build_router_with_endpoint_ids(
    options: RouteOptions,
    outbound: Arc<OutboundManager>,
    metrics: Arc<MetricsRegistry>,
    endpoint_ids: impl IntoIterator<Item = String>,
) -> HammerResult<Router> {
    let scope = metrics.scope("route", "router", "default");
    let matcher_factories = RouteMatcherFactorySet::standard();
    let endpoint_ids: HashSet<String> = endpoint_ids.into_iter().collect();
    let rules: Vec<RuntimeRule> = options
        .rules
        .into_iter()
        .enumerate()
        .map(|(index, rule)| {
            RuntimeRule::from_options(
                index,
                rule,
                &scope,
                &matcher_factories,
                outbound.as_ref(),
                &endpoint_ids,
            )
        })
        .collect::<HammerResult<Vec<_>>>()?;
    let default_target = resolve_route_target(
        outbound.as_ref(),
        &endpoint_ids,
        &options.final_,
        "route.final outbound",
    )?;
    Ok(Router {
        rules,
        outbound: Some(outbound),
        metrics: RouterMetrics::new(&scope, &options.final_),
        default_target,
        cache: new_router_cache(),
    })
}

fn resolve_route_target(
    outbound: &OutboundManager,
    endpoint_ids: &HashSet<String>,
    id: &str,
    field: &str,
) -> HammerResult<RouteTarget> {
    if endpoint_ids.contains(id) {
        return Ok(RouteTarget::Endpoint(id.to_owned()));
    }
    if outbound.get(id).is_some() {
        return Ok(RouteTarget::Outbound(id.to_owned()));
    }
    Err(HammerError::config_validation(format!(
        "{field} {id:?} is not declared"
    )))
}

impl Router {
    pub fn new(_logger: Logger) -> Self {
        let metrics = MetricsRegistry::new().scope("route", "router", "default");
        Self {
            rules: Vec::new(),
            outbound: None,
            default_target: RouteTarget::Outbound(String::new()),
            metrics: RouterMetrics::new(&metrics, ""),
            cache: new_router_cache(),
        }
    }

    pub fn from_options(
        logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
    ) -> HammerResult<Self> {
        Self::from_options_with_metrics(logger, options, outbound, MetricsRegistry::new())
    }

    pub fn from_options_with_metrics(
        logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Self> {
        RouterFactorySet::standard().build_default(logger, options, outbound, metrics)
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options_with_metrics_and_endpoint_ids(
        _logger: Logger,
        options: RouteOptions,
        outbound: Arc<OutboundManager>,
        metrics: Arc<MetricsRegistry>,
        endpoint_ids: impl IntoIterator<Item = String>,
    ) -> HammerResult<Self> {
        build_router_with_endpoint_ids(options, outbound, metrics, endpoint_ids)
    }

    pub fn match_route(&self, metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
        let network = metadata.network;

        // Cache the terminal decision keyed on the subset of metadata that
        // actually drives matcher_matches + route_to_default. The dispatch
        // path runs `prepare_route_metadata` first so all non-terminal
        // mutations (Sniff override_destination, Resolve domain_strategy,
        // RouteOptions udp_disable_domain_unmapping) are already applied
        // by the time we land here — a hit can safely skip the second walk.
        let key = MatchKey::from(&*metadata);
        if let Some(cached) = self.cache.load().decisions.get(&key).cloned() {
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
                    self.store_cache_decision(key, decision.clone());
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
                self.store_cache_decision(key, decision.clone());
                Ok(decision)
            }
            Err(err) => {
                self.metrics.error_total.inc(network);
                Err(err)
            }
        }
    }

    pub(crate) fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> HammerResult<()> {
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

    pub fn tcp_sniff_timeout(&self, metadata: &RouteMetadata) -> Option<Duration> {
        if metadata.network != Network::Tcp {
            return self.sniff_timeout(metadata);
        }

        let mut sniff_timeout = None;
        for rule in &self.rules {
            if sniff_timeout.is_none() {
                if !rule.matches(metadata) {
                    continue;
                }
                let RuleActionKind::Sniff(options) = &rule.action else {
                    continue;
                };
                let timeout = options.timeout.unwrap_or(DEFAULT_SNIFF_TIMEOUT);
                if options.override_destination {
                    return Some(timeout);
                }
                sniff_timeout = Some(timeout);
                continue;
            }

            if rule.decides_without_sniffed_tcp_metadata(metadata) {
                return None;
            }
            if rule.needs_sniffed_tcp_metadata() {
                return sniff_timeout;
            }
        }
        None
    }

    pub fn should_sniff(&self, metadata: &RouteMetadata) -> bool {
        self.sniff_timeout(metadata).is_some()
    }

    fn store_cache_decision(&self, key: MatchKey, decision: RouteDecision) {
        let mut snapshot = self.cache.load_full().as_ref().clone();
        snapshot.decisions.insert(key, decision);
        trim_route_cache(&mut snapshot.decisions);
        self.cache.store(Arc::new(snapshot));
    }

    fn route_to_default(&self, network: Network) -> HammerResult<RouteDecision> {
        let Some(outbound) = &self.outbound else {
            return Ok(RouteDecision::Route {
                target: self.default_target.clone(),
            });
        };
        let RouteTarget::Outbound(default_id) = &self.default_target else {
            return Ok(RouteDecision::Route {
                target: self.default_target.clone(),
            });
        };
        let Some(default) = outbound.get(default_id) else {
            return Err(HammerError::internal(format!(
                "default outbound not found: {default_id}"
            )));
        };
        // ICMP is treated as a soft-supported network: the dispatch
        // path detects an outbound that lacks ICMP via `listen_icmp()`
        // and synthesises a Destination Unreachable back into the tun.
        // Skipping the strict pre-flight check here lets a user keep a
        // simple `final = "hysteria2"` route (which only declares
        // Tcp/Udp) and still receive a clean ICMP reply rather than a
        // hard route engine error every time an app pings.
        if network != Network::Icmp && !default.meta().networks().contains(&network) {
            return Err(HammerError::internal(format!(
                "{network} is not supported by default outbound: {}",
                default.meta().id()
            )));
        }
        Ok(RouteDecision::Route {
            target: self.default_target.clone(),
        })
    }
}

fn trim_route_cache(cache: &mut HashMap<MatchKey, RouteDecision>) {
    while cache.len() > ROUTER_CACHE_CAPACITY {
        let Some(key) = cache.keys().next().cloned() else {
            return;
        };
        cache.remove(&key);
    }
}

impl Lifecycle for Router {
    fn name(&self) -> &str {
        "router"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        tracing::debug!(target: "router", "stage {}", stage.name());
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        tracing::debug!(target: "router", "close");
        Ok(())
    }
}

impl RouterTrait for Router {
    fn reset_network(&self) {
        // Network change can flip reverse-DNS state and (eventually) any
        // network-aware matcher; flush the metadata→decision cache so the
        // next packet picks up the new view. No matcher state needs
        // resetting yet.
        self.cache.store(Arc::new(RouteCacheSnapshot::default()));
    }

    fn match_route(&self, metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
        Router::match_route(self, metadata)
    }

    fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> HammerResult<()> {
        Router::prepare_route_metadata(self, metadata)
    }

    fn sniff_timeout(&self, metadata: &RouteMetadata) -> Option<Duration> {
        Router::sniff_timeout(self, metadata)
    }

    fn tcp_sniff_timeout(&self, metadata: &RouteMetadata) -> Option<Duration> {
        Router::tcp_sniff_timeout(self, metadata)
    }

    fn should_sniff(&self, metadata: &RouteMetadata) -> bool {
        Router::should_sniff(self, metadata)
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
    matcher: RuntimeMatcher,
    action: RuleActionKind,
    route_target: Option<RouteTarget>,
    metrics: RuntimeRuleMetrics,
}

impl RuntimeRule {
    fn from_options(
        index: usize,
        options: RuleOptions,
        router_scope: &MetricsScope,
        matcher_factories: &RouteMatcherFactorySet,
        outbound: &OutboundManager,
        endpoint_ids: &HashSet<String>,
    ) -> HammerResult<Self> {
        let default = options.default_options;
        let matcher = matcher_factories.build(default.matcher)?;
        let action = default.action;
        let route_target = match &action {
            RuleActionKind::Route(action) => Some(resolve_route_target(
                outbound,
                endpoint_ids,
                &action.outbound,
                "route.rules outbound",
            )?),
            _ => None,
        };
        let metrics = RuntimeRuleMetrics::new(
            &router_scope.child("rule", index.to_string()),
            index,
            &action,
        );
        Ok(Self {
            matcher,
            action,
            route_target,
            metrics,
        })
    }

    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        self.matcher.matches(metadata)
    }

    fn apply(&self, metadata: &mut RouteMetadata) -> HammerResult<RuleApply> {
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
            RuleActionKind::Route(_) => Ok(RuleApply::Decision(RouteDecision::Route {
                target: self
                    .route_target
                    .clone()
                    .ok_or_else(|| HammerError::internal("missing runtime route target"))?,
            })),
        }
    }

    #[inline]
    fn needs_sniffed_tcp_metadata(&self) -> bool {
        self.matcher.needs_sniffed_tcp_metadata()
    }

    #[inline]
    fn decides_without_sniffed_tcp_metadata(&self, metadata: &RouteMetadata) -> bool {
        self.is_terminal() && self.matches(metadata)
    }

    #[inline]
    fn is_terminal(&self) -> bool {
        matches!(
            &self.action,
            RuleActionKind::HijackDns | RuleActionKind::Reject(_) | RuleActionKind::Route(_)
        )
    }
}

pub(crate) enum RuntimeMatcher {
    Any(AnyMatcher),
    Inbound(InboundMatcher),
    Protocol(ProtocolMatcher),
    Domain(DomainMatcher),
    DomainSuffix(DomainSuffixMatcher),
    DomainKeyword(DomainKeywordMatcher),
    IpCidr(IpCidrMatcher),
}

impl RuntimeMatcher {
    /// Hot path — called per TCP/UDP/ICMP packet, once per rule walked.
    /// Static dispatch on this enum keeps the old `RuleMatcher` performance
    /// property while letting construction go through the component registry.
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match self {
            Self::Any(matcher) => matcher.matches(metadata),
            Self::Inbound(matcher) => matcher.matches(metadata),
            Self::Protocol(matcher) => matcher.matches(metadata),
            Self::Domain(matcher) => matcher.matches(metadata),
            Self::DomainSuffix(matcher) => matcher.matches(metadata),
            Self::DomainKeyword(matcher) => matcher.matches(metadata),
            Self::IpCidr(matcher) => matcher.matches(metadata),
        }
    }

    #[inline]
    fn needs_sniffed_tcp_metadata(&self) -> bool {
        match self {
            Self::Protocol(matcher) => matcher.needs_sniffed_tcp_metadata(),
            Self::Domain(_) | Self::DomainSuffix(_) | Self::DomainKeyword(_) => true,
            Self::Any(_) | Self::Inbound(_) | Self::IpCidr(_) => false,
        }
    }
}

#[hammer_component_macros::hammer_component(matcher, name = "any", builder = build_any_matcher)]
pub(crate) struct AnyMatcher;

impl AnyMatcher {
    #[inline]
    fn matches(&self, _metadata: &RouteMetadata) -> bool {
        true
    }
}

#[hammer_component_macros::hammer_component(matcher, name = "inbound", builder = build_inbound_matcher)]
pub(crate) struct InboundMatcher {
    values: Vec<String>,
}

impl InboundMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.values, &metadata.inbound)
    }
}

#[hammer_component_macros::hammer_component(matcher, name = "protocol", builder = build_protocol_matcher)]
pub(crate) struct ProtocolMatcher {
    values: Vec<String>,
}

impl ProtocolMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match_list(&self.values, &metadata.protocol)
    }

    #[inline]
    fn needs_sniffed_tcp_metadata(&self) -> bool {
        self.values
            .iter()
            .any(|value| !matches!(value.as_str(), "dns" | "quic" | "stun"))
    }
}

#[hammer_component_macros::hammer_component(matcher, name = "domain", builder = build_domain_matcher)]
pub(crate) struct DomainMatcher {
    values: Vec<String>,
}

impl DomainMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match metadata.domain.as_deref() {
            Some(domain) => self.values.iter().any(|value| value == domain),
            None => false,
        }
    }
}

#[hammer_component_macros::hammer_component(
    matcher,
    name = "domain_suffix",
    builder = build_domain_suffix_matcher
)]
pub(crate) struct DomainSuffixMatcher {
    values: Vec<String>,
}

impl DomainSuffixMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match metadata.domain.as_deref() {
            Some(domain) => self
                .values
                .iter()
                .any(|suffix| domain_suffix_matches(domain, suffix)),
            None => false,
        }
    }
}

#[hammer_component_macros::hammer_component(
    matcher,
    name = "domain_keyword",
    builder = build_domain_keyword_matcher
)]
pub(crate) struct DomainKeywordMatcher {
    values: Vec<String>,
}

impl DomainKeywordMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match metadata.domain.as_deref() {
            Some(domain) => self
                .values
                .iter()
                .any(|keyword| domain.contains(keyword.as_str())),
            None => false,
        }
    }
}

#[hammer_component_macros::hammer_component(matcher, name = "ip_cidr", builder = build_ip_cidr_matcher)]
pub(crate) struct IpCidrMatcher {
    values: Vec<IpNet>,
}

impl IpCidrMatcher {
    #[inline]
    fn matches(&self, metadata: &RouteMetadata) -> bool {
        match metadata.destination.as_ref() {
            Some(SocksAddr { host, .. }) => self.values.iter().any(|net| net.contains(host)),
            None => false,
        }
    }
}

fn build_any_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::Any => Ok(RuntimeMatcher::Any(AnyMatcher)),
        matcher => Err(wrong_matcher_builder(
            <AnyMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_inbound_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::Inbound(values) => Ok(RuntimeMatcher::Inbound(InboundMatcher { values })),
        matcher => Err(wrong_matcher_builder(
            <InboundMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_protocol_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::Protocol(values) => Ok(RuntimeMatcher::Protocol(ProtocolMatcher { values })),
        matcher => Err(wrong_matcher_builder(
            <ProtocolMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_domain_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::Domain(values) => Ok(RuntimeMatcher::Domain(DomainMatcher { values })),
        matcher => Err(wrong_matcher_builder(
            <DomainMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_domain_suffix_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::DomainSuffix(values) => {
            Ok(RuntimeMatcher::DomainSuffix(DomainSuffixMatcher { values }))
        }
        matcher => Err(wrong_matcher_builder(
            <DomainSuffixMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_domain_keyword_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::DomainKeyword(values) => {
            Ok(RuntimeMatcher::DomainKeyword(DomainKeywordMatcher {
                values,
            }))
        }
        matcher => Err(wrong_matcher_builder(
            <DomainKeywordMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn build_ip_cidr_matcher(matcher: RuleMatcher) -> HammerResult<RuntimeMatcher> {
    match matcher {
        RuleMatcher::IpCidr(values) => Ok(RuntimeMatcher::IpCidr(IpCidrMatcher { values })),
        matcher => Err(wrong_matcher_builder(
            <IpCidrMatcher as crate::component_registry::RouteMatcherComponentDeclaration>::TYPE_NAME,
            &matcher,
        )),
    }
}

fn wrong_matcher_builder(expected: &str, actual: &RuleMatcher) -> HammerError {
    HammerError::internal(format!(
        "{expected} route matcher factory received {} options",
        route_matcher_type_name(actual)
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use async_trait::async_trait;
    use hammer_adapter::{
        ComponentMeta, Outbound, OutboundComponent, ProxyPacketConn, ProxyStream, RuntimeComponent,
    };
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::log::{DiscardWriter, Factory};

    struct StubOutbound;

    impl StubOutbound {
        fn component(id: &str) -> OutboundComponent {
            let runtime: Arc<dyn Outbound> = Arc::new(Self);
            RuntimeComponent::new(
                ComponentMeta::new(
                    "outbound",
                    "stub",
                    id,
                    vec![Network::Tcp, Network::Udp],
                    Vec::new(),
                    None,
                ),
                runtime,
            )
        }
    }

    #[async_trait]
    impl Outbound for StubOutbound {
        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> CoreResult<Box<dyn ProxyStream>> {
            Err(CoreError::internal("stub outbound: dial not supported"))
        }

        async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
            Err(CoreError::internal(
                "stub outbound: listen_packet not supported",
            ))
        }
    }

    fn test_logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn route_options() -> RouteOptions {
        RouteOptions {
            final_: "final".to_owned(),
            auto_detect_interface: true,
            rules: Vec::new(),
            default_domain_resolver: None,
        }
    }

    fn outbound_manager() -> Arc<OutboundManager> {
        let manager = OutboundManager::new(test_logger("outbound"), "final");
        manager
            .register_outbound(StubOutbound::component("final"))
            .expect("register stub outbound");
        Arc::new(manager)
    }

    #[test]
    fn router_factory_registers_default_router_component() {
        let router = RouterFactorySet::standard()
            .build_default(
                test_logger("router"),
                route_options(),
                outbound_manager(),
                MetricsRegistry::new(),
            )
            .expect("build default router");
        let mut metadata = RouteMetadata {
            network: Network::Tcp,
            ..Default::default()
        };

        let decision = router.match_route(&mut metadata).expect("match route");

        assert_eq!(
            decision,
            RouteDecision::Route {
                target: RouteTarget::Outbound("final".to_owned())
            }
        );
    }

    #[test]
    fn router_factory_rejects_unknown_router_type() {
        let result = RouterFactorySet::standard().build(
            "missing",
            test_logger("router"),
            route_options(),
            outbound_manager(),
            MetricsRegistry::new(),
        );
        let err = match result {
            Ok(_) => panic!("unknown router type unexpectedly built"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("unknown router type: missing"),
            "err = {err}"
        );
    }

    #[test]
    fn route_matcher_factory_registers_domain_suffix_matcher() {
        let matcher = RouteMatcherFactorySet::standard()
            .build(RuleMatcher::DomainSuffix(vec!["example.com".to_owned()]))
            .expect("build domain suffix matcher");
        let matching = RouteMetadata {
            domain: Some("api.example.com".to_owned()),
            ..Default::default()
        };
        let unrelated = RouteMetadata {
            domain: Some("example.org".to_owned()),
            ..Default::default()
        };

        assert!(matcher.matches(&matching));
        assert!(!matcher.matches(&unrelated));
    }

    #[test]
    fn route_matcher_factory_rejects_unknown_matcher_type() {
        let result =
            RouteMatcherFactorySet::standard().build_with_type("missing", RuleMatcher::Any);
        let err = match result {
            Ok(_) => panic!("unknown route matcher type unexpectedly built"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("unknown route matcher type: missing"),
            "err = {err}"
        );
    }

    #[test]
    fn route_matcher_builder_rejects_wrong_rule_matcher_variant() {
        let result = RouteMatcherFactorySet::standard().build_with_type(
            "domain_suffix",
            RuleMatcher::Domain(vec!["example.com".to_owned()]),
        );
        let err = match result {
            Ok(_) => panic!("wrong route matcher variant unexpectedly built"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("domain_suffix route matcher factory received domain options"),
            "err = {err}"
        );
    }
}
