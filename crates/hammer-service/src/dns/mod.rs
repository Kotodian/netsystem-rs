use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use hammer_adapter::{
    ComponentMetadata, DnsQueryOptions, DnsRouter as DnsRouterTrait, DnsTransport,
    DnsTransportComponent, DnsTransportManager as DnsTransportManagerTrait, Lifecycle,
    RuntimeComponent, StartStage,
};
use hammer_core::config::{
    DnsOptions, DnsRule, DnsRuleAction, DnsRuleMatcher, DnsServer, DnsServerKind, DomainStrategy,
    RejectKind, normalize_domain,
};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::log::Logger;
pub use hammer_core::protocol::dns::{
    FixedResponseCode, MessageExt, domain_from_name, fixed_address_response, parse_hosts,
    query_message, record_addr,
};
use hickory_proto::op::{Message, Query, ResponseCode};
use hickory_proto::rr::RecordType;

#[cfg(any(
    feature = "dns-udp",
    feature = "dns-tcp",
    feature = "dns-https",
    feature = "dns-hosts",
    feature = "dns-local"
))]
use crate::component_registry::register_components;
use hammer_runtime::{
    ControlEventFilter, ControlEventSubscriptionHandle, ControlThreadHandle, RuntimePlatform,
    SocketProtector,
};

use crate::OutboundManager;

mod transport;
#[cfg(feature = "dns-https")]
pub use transport::HttpsDnsTransport;
#[cfg(feature = "dns-tcp")]
pub use transport::TcpDnsTransport;
#[cfg(feature = "dns-udp")]
pub use transport::UdpDnsTransport;

const DEFAULT_DNS_TTL: u32 = 600;
const DNS_TIMEOUT: Duration = Duration::from_secs(10);
const DNS_CLIENT_CACHE_MAX_ENTRIES: usize = 1024;
const DNS_REVERSE_CACHE_MAX_ENTRIES: usize = 2048;
/// Capacity of the per-qname rule-match LRU. Sized to mirror
/// `ROUTER_CACHE_CAPACITY` (`route.rs`) — most flows share a small set of
/// query names (CDN clusters, ad domains, the user's own resolver bootstrap)
/// so 256 entries keep walks rare without bloating NetExt heap.
const DNS_MATCH_CACHE_CAPACITY: usize = 256;

pub type QueryType = RecordType;

/// Match a single rule against an already-normalised query name. Mirrors
/// `route::matcher_matches` for the three domain variants we surface. Hot
/// path on every DNS query — `#[inline]` lets LLVM fold the enum match into
/// the surrounding `match_rules` loop.
#[inline]
fn match_dns_rule(matcher: &DnsRuleMatcher, qname: &str) -> bool {
    match matcher {
        DnsRuleMatcher::Domain(values) => values.iter().any(|v| v == qname),
        DnsRuleMatcher::DomainSuffix(values) => values
            .iter()
            .any(|suffix| domain_suffix_matches(qname, suffix)),
        DnsRuleMatcher::DomainKeyword(values) => {
            values.iter().any(|kw| qname.contains(kw.as_str()))
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

fn first_query(message: &Message) -> HammerResult<&Query> {
    message
        .queries
        .first()
        .ok_or_else(|| HammerError::internal("bad question size: 0"))
}

fn dns_query_log_summary(message: &Message) -> String {
    match message.queries.first() {
        Some(query) => format!(
            "{} {:?}",
            domain_from_name(query.name()),
            query.query_type()
        ),
        None => "empty-query".to_owned(),
    }
}

fn dns_response_log_summary(message: &Message) -> String {
    let addresses = message
        .addresses()
        .into_iter()
        .map(|addr| addr.to_string())
        .collect::<Vec<_>>();
    let addresses = if addresses.is_empty() {
        "-".to_owned()
    } else {
        addresses.join(",")
    };
    format!(
        "rcode={:?} answers={} addresses={}",
        message.metadata.response_code,
        message.answers.len(),
        addresses
    )
}

fn response_ttl(response: &Message) -> u32 {
    response
        .answers
        .iter()
        .map(|record| record.ttl)
        .filter(|ttl| *ttl > 0)
        .min()
        .unwrap_or(0)
}

fn normalize_ttl(response: &mut Message, ttl: u32) {
    for record in &mut response.answers {
        record.ttl = ttl;
    }
}

fn apply_response_options(response: &mut Message, options: &DnsQueryOptions) -> u32 {
    let ttl = options
        .rewrite_ttl
        .unwrap_or_else(|| response_ttl(response));
    normalize_ttl(response, ttl);
    ttl
}

fn sort_addresses(
    mut v4: Vec<IpAddr>,
    mut v6: Vec<IpAddr>,
    strategy: DomainStrategy,
) -> Vec<IpAddr> {
    if strategy == DomainStrategy::PreferIpv6 {
        v6.append(&mut v4);
        v6
    } else {
        v4.append(&mut v6);
        v4
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    transport: String,
    name: String,
    record_type: RecordType,
}

#[derive(Clone)]
struct CacheValue {
    response: Message,
    expires_at: Instant,
}

#[derive(Clone)]
struct ReverseValue {
    domain: String,
    expires_at: Instant,
}

#[derive(Clone)]
struct DnsSnapshot {
    responses: HashMap<CacheKey, CacheValue>,
    reverse: HashMap<IpAddr, ReverseValue>,
    match_cache: HashMap<String, MatchedAction>,
}

impl DnsSnapshot {
    fn empty() -> Self {
        Self {
            responses: HashMap::new(),
            reverse: HashMap::new(),
            match_cache: HashMap::new(),
        }
    }
}

pub(crate) struct DnsControlCache {
    snapshot: ArcSwap<DnsSnapshot>,
}

impl fmt::Debug for DnsControlCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DnsControlCache").finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum DnsCacheUpdate {
    StoreResponse {
        key: CacheKey,
        response: Message,
        ttl: u32,
    },
    StoreReverseBatch {
        entries: Vec<(IpAddr, String, Instant)>,
    },
    StoreMatch {
        qname: String,
        action: MatchedAction,
    },
    Clear,
}

impl fmt::Debug for DnsCacheUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreResponse { .. } => f.write_str("StoreResponse"),
            Self::StoreReverseBatch { entries } => f
                .debug_tuple("StoreReverseBatch")
                .field(&entries.len())
                .finish(),
            Self::StoreMatch { qname, .. } => f.debug_tuple("StoreMatch").field(qname).finish(),
            Self::Clear => f.write_str("Clear"),
        }
    }
}

#[derive(Clone, Debug)]
struct DnsCacheUpdateArgs {
    cache: Arc<DnsControlCache>,
    update: DnsCacheUpdate,
}

impl DnsControlCache {
    fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(DnsSnapshot::empty()),
        }
    }

    fn load_response(&self, key: &CacheKey) -> Option<Message> {
        let snapshot = self.snapshot.load();
        let value = snapshot.responses.get(key)?;
        let now = Instant::now();
        if now >= value.expires_at {
            return None;
        }
        let ttl = value.expires_at.saturating_duration_since(now).as_secs() as u32;
        let mut response = value.response.clone();
        normalize_ttl(&mut response, ttl);
        Some(response)
    }

    fn apply_update(&self, update: DnsCacheUpdate) {
        match update {
            DnsCacheUpdate::StoreResponse { key, response, ttl } => {
                self.store_response_direct(key, &response, ttl);
            }
            DnsCacheUpdate::StoreReverseBatch { entries } => {
                self.store_reverse_batch_direct(entries);
            }
            DnsCacheUpdate::StoreMatch { qname, action } => {
                self.store_match_direct(qname, action);
            }
            DnsCacheUpdate::Clear => {
                self.clear_direct();
            }
        }
    }

    fn store_response_direct(&self, key: CacheKey, response: &Message, ttl: u32) {
        let now = Instant::now();
        let mut snapshot = self.snapshot.load_full().as_ref().clone();
        prune_response_snapshot(&mut snapshot.responses, now);
        snapshot.responses.insert(
            key,
            CacheValue {
                response: response.clone(),
                expires_at: now + Duration::from_secs(u64::from(ttl)),
            },
        );
        trim_response_snapshot(&mut snapshot.responses, DNS_CLIENT_CACHE_MAX_ENTRIES);
        self.snapshot.store(Arc::new(snapshot));
    }

    fn lookup_reverse(&self, ip: IpAddr) -> Option<String> {
        let snapshot = self.snapshot.load();
        let value = snapshot.reverse.get(&ip)?;
        if Instant::now() >= value.expires_at {
            return None;
        }
        Some(value.domain.clone())
    }

    fn store_reverse_batch_direct<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (IpAddr, String, Instant)>,
    {
        let mut snapshot = self.snapshot.load_full().as_ref().clone();
        prune_reverse_snapshot(&mut snapshot.reverse, Instant::now());
        for (addr, domain, expires_at) in entries {
            snapshot
                .reverse
                .insert(addr, ReverseValue { domain, expires_at });
        }
        trim_reverse_snapshot(&mut snapshot.reverse, DNS_REVERSE_CACHE_MAX_ENTRIES);
        self.snapshot.store(Arc::new(snapshot));
    }

    fn load_match(&self, qname: &str) -> Option<MatchedAction> {
        self.snapshot.load().match_cache.get(qname).cloned()
    }

    fn store_match_direct(&self, qname: String, action: MatchedAction) {
        let mut snapshot = self.snapshot.load_full().as_ref().clone();
        snapshot.match_cache.insert(qname, action);
        trim_match_snapshot(&mut snapshot.match_cache, DNS_MATCH_CACHE_CAPACITY);
        self.snapshot.store(Arc::new(snapshot));
    }

    fn clear(&self) {
        self.apply_update(DnsCacheUpdate::Clear);
    }

    fn clear_direct(&self) {
        self.snapshot.store(Arc::new(DnsSnapshot::empty()));
    }
}

pub struct DnsClient {
    cache: Arc<DnsControlCache>,
    control_handle: Option<Arc<ControlThreadHandle>>,
}

impl DnsClient {
    pub fn new(_logger: Logger) -> Self {
        Self::with_cache(Arc::new(DnsControlCache::new()))
    }

    fn with_cache(cache: Arc<DnsControlCache>) -> Self {
        Self {
            cache,
            control_handle: None,
        }
    }

    fn set_control_handle(&mut self, control_handle: Arc<ControlThreadHandle>) {
        self.control_handle = Some(control_handle);
    }

    pub async fn exchange(
        &self,
        transport: &DnsTransportComponent,
        message: Message,
        options: DnsQueryOptions,
    ) -> HammerResult<Message> {
        if message.queries.len() != 1 {
            warn!("bad question size: {}", message.queries.len());
            return Ok(message.fixed_response(FixedResponseCode::FormatError));
        }
        let query = first_query(&message)?.clone();
        if strategy_rejects(query.query_type(), options.strategy) {
            return Ok(message.fixed_response(FixedResponseCode::NoError));
        }
        let key = CacheKey {
            transport: transport.meta().id().to_owned(),
            name: query.name().to_ascii().to_ascii_lowercase(),
            record_type: query.query_type(),
        };
        if !options.disable_cache
            && let Some(mut cached) = self.load_cache(&key)
        {
            cached.metadata.id = message.metadata.id;
            if let Some(ttl) = options.rewrite_ttl {
                normalize_ttl(&mut cached, ttl);
            }
            return Ok(cached);
        }
        let mut response = tokio::time::timeout(DNS_TIMEOUT, transport.runtime().exchange(message))
            .await
            .map_err(|_| HammerError::internal("dns query timed out"))??;
        let ttl = apply_response_options(&mut response, &options);
        if !options.disable_cache && ttl > 0 {
            self.store_cache(key, &response, ttl);
        }
        Ok(response)
    }

    pub(crate) fn try_exchange_cached(
        &self,
        transport: &DnsTransportComponent,
        message: &Message,
        options: DnsQueryOptions,
    ) -> HammerResult<Option<Message>> {
        if message.queries.len() != 1 {
            warn!("bad question size: {}", message.queries.len());
            return Ok(Some(message.fixed_response(FixedResponseCode::FormatError)));
        }
        let query = first_query(message)?.clone();
        if strategy_rejects(query.query_type(), options.strategy) {
            return Ok(Some(message.fixed_response(FixedResponseCode::NoError)));
        }
        if options.disable_cache {
            return Ok(None);
        }
        let key = CacheKey {
            transport: transport.meta().id().to_owned(),
            name: query.name().to_ascii().to_ascii_lowercase(),
            record_type: query.query_type(),
        };
        let Some(mut cached) = self.load_cache(&key) else {
            return Ok(None);
        };
        cached.metadata.id = message.metadata.id;
        if let Some(ttl) = options.rewrite_ttl {
            normalize_ttl(&mut cached, ttl);
        }
        Ok(Some(cached))
    }

    pub async fn lookup(
        &self,
        transport: &DnsTransportComponent,
        domain: &str,
        options: DnsQueryOptions,
    ) -> HammerResult<Vec<IpAddr>> {
        let strategy = if options.lookup_strategy == DomainStrategy::AsIs {
            options.strategy
        } else {
            options.lookup_strategy
        };
        match strategy {
            DomainStrategy::Ipv4Only => {
                self.lookup_type(transport, domain, RecordType::A, options)
                    .await
            }
            DomainStrategy::Ipv6Only => {
                self.lookup_type(transport, domain, RecordType::AAAA, options)
                    .await
            }
            _ => {
                let v4 = self
                    .lookup_type(transport, domain, RecordType::A, options.clone())
                    .await
                    .unwrap_or_default();
                let v6 = self
                    .lookup_type(transport, domain, RecordType::AAAA, options)
                    .await
                    .unwrap_or_default();
                let addresses = sort_addresses(v4, v6, strategy);
                if addresses.is_empty() {
                    return Err(HammerError::internal(format!(
                        "lookup {domain}: empty result"
                    )));
                }
                Ok(addresses)
            }
        }
    }

    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    async fn lookup_type(
        &self,
        transport: &DnsTransportComponent,
        domain: &str,
        record_type: RecordType,
        mut options: DnsQueryOptions,
    ) -> HammerResult<Vec<IpAddr>> {
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = match record_type {
                RecordType::A => DomainStrategy::Ipv4Only,
                RecordType::AAAA => DomainStrategy::Ipv6Only,
                _ => DomainStrategy::AsIs,
            };
        }
        let response = self
            .exchange(transport, query_message(domain, record_type)?, options)
            .await?;
        if response.metadata.response_code != ResponseCode::NoError {
            return Err(HammerError::internal(format!(
                "dns response code: {:?}",
                response.metadata.response_code
            )));
        }
        Ok(response
            .addresses()
            .into_iter()
            .filter(|addr| {
                matches!(
                    (record_type, addr),
                    (RecordType::A, IpAddr::V4(_)) | (RecordType::AAAA, IpAddr::V6(_))
                )
            })
            .collect())
    }

    fn load_cache(&self, key: &CacheKey) -> Option<Message> {
        self.cache.load_response(key)
    }

    fn store_cache(&self, key: CacheKey, response: &Message, ttl: u32) {
        self.publish_or_apply(DnsCacheUpdate::StoreResponse {
            key,
            response: response.clone(),
            ttl,
        });
    }

    fn publish_or_apply(&self, update: DnsCacheUpdate) {
        if let Some(control_handle) = &self.control_handle {
            let _ = control_handle.publish_event(DnsCacheUpdateArgs {
                cache: Arc::clone(&self.cache),
                update,
            });
            return;
        }
        self.cache.apply_update(update);
    }
}

fn prune_response_snapshot(cache: &mut HashMap<CacheKey, CacheValue>, now: Instant) {
    let expired: Vec<CacheKey> = cache
        .iter()
        .filter(|(_, value)| now >= value.expires_at)
        .map(|(key, _)| key.clone())
        .collect();
    for key in expired {
        cache.remove(&key);
    }
}

fn trim_response_snapshot(cache: &mut HashMap<CacheKey, CacheValue>, capacity: usize) {
    while cache.len() > capacity {
        let Some(key) = cache
            .iter()
            .min_by_key(|(_, value)| value.expires_at)
            .map(|(key, _)| key.clone())
        else {
            return;
        };
        cache.remove(&key);
    }
}

fn strategy_rejects(record_type: RecordType, strategy: DomainStrategy) -> bool {
    matches!(
        (record_type, strategy),
        (RecordType::A, DomainStrategy::Ipv6Only) | (RecordType::AAAA, DomainStrategy::Ipv4Only)
    )
}

#[cfg(feature = "dns-hosts")]
#[hammer_component_macros::hammer_component(
    dns_transport,
    name = "hosts",
    builder = build_hosts_transport,
    metrics = ("dns", "transport")
)]
pub struct HostsTransport {
    id: String,
    dependencies: Vec<String>,
    predefined: HashMap<String, Vec<IpAddr>>,
}

#[cfg(feature = "dns-hosts")]
impl HostsTransport {
    pub fn from_predefined<I, S>(id: impl Into<String>, entries: I) -> Self
    where
        I: IntoIterator<Item = (S, IpAddr)>,
        S: AsRef<str>,
    {
        let mut predefined: HashMap<String, Vec<IpAddr>> = HashMap::new();
        for (domain, addr) in entries {
            predefined
                .entry(normalize_domain(domain.as_ref()))
                .or_default()
                .push(addr);
        }
        Self {
            id: id.into(),
            dependencies: Vec::new(),
            predefined,
        }
    }

    pub fn system(id: impl Into<String>) -> Self {
        let content = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
        let entries = parse_hosts(&content);
        Self::from_predefined(id, entries)
    }
}

#[cfg(feature = "dns-hosts")]
impl Lifecycle for HostsTransport {
    fn name(&self) -> &str {
        "dns/transport/hosts"
    }
    fn start(&self, _stage: StartStage) -> HammerResult<()> {
        Ok(())
    }
    fn close(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
#[cfg(feature = "dns-hosts")]
impl DnsTransport for HostsTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> HammerResult<Message> {
        let query = first_query(&message)?.clone();
        let domain = domain_from_name(query.name());
        let addresses = self.predefined.get(&domain).cloned().unwrap_or_default();
        if addresses.is_empty() {
            return Ok(message.fixed_response(FixedResponseCode::NXDomain));
        }
        Ok(fixed_address_response(
            &message,
            &query,
            addresses,
            DEFAULT_DNS_TTL,
        ))
    }
}

#[cfg(feature = "dns-local")]
#[hammer_component_macros::hammer_component(
    dns_transport,
    name = "local",
    builder = build_local_transport,
    metrics = ("dns", "transport")
)]
pub struct LocalDnsTransport {
    id: String,
    dependencies: Vec<String>,
}

#[cfg(feature = "dns-local")]
impl LocalDnsTransport {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            dependencies: Vec::new(),
        }
    }
}

#[cfg(feature = "dns-local")]
impl Lifecycle for LocalDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/local"
    }
    fn start(&self, _stage: StartStage) -> HammerResult<()> {
        Ok(())
    }
    fn close(&self) -> HammerResult<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
#[cfg(feature = "dns-local")]
impl DnsTransport for LocalDnsTransport {
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> HammerResult<Message> {
        let query = first_query(&message)?.clone();
        if !matches!(query.query_type(), RecordType::A | RecordType::AAAA) {
            return Ok(message.fixed_response(FixedResponseCode::NXDomain));
        }
        let domain = domain_from_name(query.name());
        let lookup = tokio::net::lookup_host((domain.as_str(), 0))
            .await
            .map_err(|e| HammerError::internal(format!("local DNS lookup {domain}: {e}")))?;
        let mut addresses = Vec::new();
        for addr in lookup {
            match (query.query_type(), addr.ip()) {
                (RecordType::A, IpAddr::V4(ip)) => addresses.push(IpAddr::V4(ip)),
                (RecordType::AAAA, IpAddr::V6(ip)) => addresses.push(IpAddr::V6(ip)),
                _ => {}
            }
        }
        if addresses.is_empty() {
            return Ok(message.fixed_response(FixedResponseCode::NXDomain));
        }
        Ok(fixed_address_response(
            &message,
            &query,
            addresses,
            DEFAULT_DNS_TTL,
        ))
    }
}

pub struct DnsTransportManager {
    items: ArcSwap<HashMap<String, DnsTransportComponent>>,
    default_id: String,
    factories: DnsTransportFactorySet,
}

pub struct DnsTransportRegistration(DnsTransportComponent);

impl From<DnsTransportComponent> for DnsTransportRegistration {
    fn from(transport: DnsTransportComponent) -> Self {
        Self(transport)
    }
}

impl<T> From<Arc<T>> for DnsTransportRegistration
where
    T: DnsTransport + ComponentMetadata + 'static,
{
    fn from(transport: Arc<T>) -> Self {
        let meta = ComponentMetadata::component_meta(transport.as_ref());
        let runtime: Arc<dyn DnsTransport> = transport;
        Self(RuntimeComponent::new(meta, runtime))
    }
}

pub(crate) type DnsTransportBuilder = fn(
    String,
    &DnsServerKind,
    Logger,
    Option<Arc<OutboundManager>>,
    Option<DnsTransportComponent>,
    SocketProtector,
) -> HammerResult<DnsTransportComponent>;

#[derive(Clone)]
struct DnsTransportFactorySet {
    builders: Arc<HashMap<&'static str, DnsTransportBuilder>>,
}

impl DnsTransportFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_dns_transport_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build(
        &self,
        server: &DnsServer,
        logger: &Logger,
        outbound: Option<Arc<OutboundManager>>,
        bootstrap: Option<DnsTransportComponent>,
        protector: SocketProtector,
    ) -> HammerResult<DnsTransportComponent> {
        let type_name = server.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown DNS transport type: {type_name}"))
        })?;
        builder(
            server.id.clone(),
            &server.kind,
            logger.clone(),
            outbound,
            bootstrap,
            protector,
        )
    }
}

#[allow(unused_variables)]
fn register_standard_dns_transport_builders(
    builders: &mut HashMap<&'static str, DnsTransportBuilder>,
) {
    #[cfg(feature = "dns-udp")]
    register_components!(dns_transport, builders, [transport::udp::UdpDnsTransport]);
    #[cfg(feature = "dns-tcp")]
    register_components!(dns_transport, builders, [transport::tcp::TcpDnsTransport]);
    #[cfg(feature = "dns-https")]
    register_components!(dns_transport, builders, [transport::doh::HttpsDnsTransport]);
    #[cfg(feature = "dns-hosts")]
    register_components!(dns_transport, builders, [HostsTransport]);
    #[cfg(feature = "dns-local")]
    register_components!(dns_transport, builders, [LocalDnsTransport]);
}

impl DnsTransportManager {
    pub fn new(_logger: Logger, default_id: impl Into<String>) -> Self {
        Self {
            items: ArcSwap::from_pointee(HashMap::new()),
            default_id: default_id.into(),
            factories: DnsTransportFactorySet::standard(),
        }
    }

    pub fn from_options(logger: Logger, options: &DnsOptions) -> HammerResult<Self> {
        Self::from_options_inner(logger, options, None, None, SocketProtector::default())
    }

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &DnsOptions,
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
        default_domain_resolver: Option<&str>,
    ) -> HammerResult<Self> {
        Self::from_options_inner(
            logger,
            options,
            Some(outbound),
            default_domain_resolver,
            SocketProtector::from(platform.into()),
        )
    }

    /// Topological build: a DNS server's bootstrap (if any) must already be
    /// registered before the server itself is built, since each transport
    /// holds a strong `Arc<dyn DnsTransport>` to its bootstrap.
    fn from_options_inner(
        logger: Logger,
        options: &DnsOptions,
        outbound: Option<Arc<OutboundManager>>,
        default_domain_resolver: Option<&str>,
        protector: impl Into<SocketProtector>,
    ) -> HammerResult<Self> {
        let protector = protector.into();
        let manager = Self::new(logger.clone(), options.final_.clone());
        let order = topo_sort_dns_servers(&options.servers, default_domain_resolver)?;
        for server in order {
            let bootstrap_tag = effective_runtime_bootstrap(server, default_domain_resolver);
            let bootstrap = bootstrap_tag.and_then(|tag| manager.get(tag));
            manager.insert(manager.factories.build(
                server,
                &logger,
                outbound.clone(),
                bootstrap,
                protector.clone(),
            )?);
        }
        Ok(manager)
    }

    pub fn insert(&self, transport: impl Into<DnsTransportRegistration>) {
        let transport = transport.into().0;
        let id = transport.meta().id().to_owned();
        let mut items = self.items.load_full().as_ref().clone();
        items.insert(id, transport);
        self.items.store(Arc::new(items));
    }

    pub fn list(&self) -> Vec<DnsTransportComponent> {
        self.items.load().values().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<DnsTransportComponent> {
        self.items.load().get(id).cloned()
    }

    pub fn default(&self) -> Option<DnsTransportComponent> {
        self.get(&self.default_id)
    }
}

/// Determine the bootstrap DNS server tag this server will actually use at
/// runtime. Mirrors the validator semantics in hammer-core: per-server
/// `domain_resolver` wins; falls back to `route.default_domain_resolver`;
/// returns `None` for servers that don't need bootstrap (IP-literal or
/// no via).
fn effective_runtime_bootstrap<'a>(
    server: &'a DnsServer,
    default: Option<&'a str>,
) -> Option<&'a str> {
    let is_domain = server
        .server_string()
        .map(|s| s.parse::<IpAddr>().is_err())
        .unwrap_or(false);
    if !is_domain || server.via().is_empty() {
        return None;
    }
    let tag = if !server.domain_resolver().is_empty() {
        server.domain_resolver()
    } else {
        default.unwrap_or("")
    };
    (!tag.is_empty()).then_some(tag)
}

/// DFS post-order topological sort of DNS servers by their bootstrap edges.
/// Servers with no bootstrap come first; each server appears after the
/// bootstrap it depends on. Cycles are returned as Err (defense-in-depth;
/// hammer-core's config validator rejects cycles up front).
fn topo_sort_dns_servers<'a>(
    servers: &'a [DnsServer],
    default: Option<&'a str>,
) -> HammerResult<Vec<&'a DnsServer>> {
    let by_id: HashMap<&str, &DnsServer> = servers.iter().map(|s| (s.id.as_str(), s)).collect();
    let mut visited: HashMap<&str, u8> = HashMap::new();
    let mut order: Vec<&DnsServer> = Vec::new();
    for s in servers {
        topo_visit_dns(s.id.as_str(), &by_id, default, &mut visited, &mut order)?;
    }
    Ok(order)
}

fn topo_visit_dns<'a>(
    node: &'a str,
    by_id: &HashMap<&'a str, &'a DnsServer>,
    default: Option<&'a str>,
    visited: &mut HashMap<&'a str, u8>,
    order: &mut Vec<&'a DnsServer>,
) -> HammerResult<()> {
    match visited.get(node).copied().unwrap_or(0) {
        2 => return Ok(()),
        1 => {
            return Err(HammerError::internal(format!(
                "dns transport bootstrap cycle at '{node}'"
            )));
        }
        _ => {}
    }
    visited.insert(node, 1);
    let Some(server) = by_id.get(node).copied() else {
        visited.insert(node, 2);
        return Ok(());
    };
    if let Some(bootstrap) = effective_runtime_bootstrap(server, default)
        && by_id.contains_key(bootstrap)
    {
        topo_visit_dns(bootstrap, by_id, default, visited, order)?;
    }
    order.push(server);
    visited.insert(node, 2);
    Ok(())
}

#[cfg(feature = "dns-hosts")]
fn build_hosts_transport(
    id: String,
    kind: &DnsServerKind,
    _logger: Logger,
    _outbound: Option<Arc<OutboundManager>>,
    _bootstrap: Option<DnsTransportComponent>,
    _protector: SocketProtector,
) -> HammerResult<Arc<HostsTransport>> {
    match kind {
        DnsServerKind::Hosts => Ok(Arc::new(HostsTransport::system(id))),
        _ => Err(HammerError::internal(
            "hosts DNS factory received wrong options",
        )),
    }
}

#[cfg(feature = "dns-local")]
fn build_local_transport(
    id: String,
    kind: &DnsServerKind,
    _logger: Logger,
    _outbound: Option<Arc<OutboundManager>>,
    _bootstrap: Option<DnsTransportComponent>,
    _protector: SocketProtector,
) -> HammerResult<Arc<LocalDnsTransport>> {
    match kind {
        DnsServerKind::Local => Ok(Arc::new(LocalDnsTransport::new(id))),
        _ => Err(HammerError::internal(
            "local DNS factory received wrong options",
        )),
    }
}

impl Lifecycle for DnsTransportManager {
    fn name(&self) -> &str {
        "dns-transport"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!("stage {}", stage.name());
        if stage != StartStage::Start {
            return Ok(());
        }
        if self.default().is_none() {
            return Err(HammerError::internal(format!(
                "default DNS server not found: {}",
                self.default_id
            )));
        }
        for transport in self.list() {
            transport.runtime().start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        debug!("close");
        for transport in self.list() {
            transport.runtime().close()?;
        }
        Ok(())
    }
}

impl DnsTransportManagerTrait for DnsTransportManager {
    fn list(&self) -> Vec<DnsTransportComponent> {
        self.list()
    }

    fn get(&self, id: &str) -> Option<DnsTransportComponent> {
        self.get(id)
    }

    fn default(&self) -> Option<DnsTransportComponent> {
        self.default()
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        let mut items = self.items.load_full().as_ref().clone();
        items.remove(id);
        self.items.store(Arc::new(items));
        Ok(())
    }
}

pub struct DnsRouter {
    transport: Arc<DnsTransportManager>,
    client: DnsClient,
    cache: Arc<DnsControlCache>,
    control_handle: Option<Arc<ControlThreadHandle>>,
    _control_subscription: Option<ControlEventSubscriptionHandle>,
    default_strategy: DomainStrategy,
    /// Resolved `[[dns.rules]]`. Empty = default-only behaviour, the
    /// pre-feature path. Populated once at construction; never mutated.
    rules: Vec<ResolvedDnsRule>,
    /// Per-qname RCU snapshot of the rule-walk outcome. Same role as the
    /// `Router::cache` for proxy routing — sustained traffic to the same
    /// host hits the same set of qnames repeatedly, so a 256-entry LRU
    /// collapses N rule walks into one hashmap lookup.
    match_cache_hits: AtomicU64,
    match_cache_misses: AtomicU64,
}

/// Resolved form of a `DnsRule`: `Route` carries the actual transport
/// reference (looked up at construction so the hot path is a single pointer
/// clone) instead of an opaque server-id string. The matcher's domain
/// strings are already normalised by `build_dns_rules` in hammer-core.
struct ResolvedDnsRule {
    matcher: DnsRuleMatcher,
    action: ResolvedDnsAction,
}

enum ResolvedDnsAction {
    Route(DnsTransportComponent),
    Reject(RejectKind),
}

/// Outcome of `match_rules`. `Default` means "no rule matched, fall
/// through to the existing transport-resolution path".
///
/// `Clone` so the per-qname LRU can hand out copies cheaply: `Route`
/// clones an `Arc`, `Reject` is `Copy`, `Default` is unit.
#[derive(Clone)]
enum MatchedAction {
    Route(DnsTransportComponent),
    Reject(RejectKind),
    Default,
}

impl DnsRouter {
    pub fn new(logger: Logger) -> Self {
        let transport = Arc::new(DnsTransportManager::new(logger.clone(), String::new()));
        Self::new_with_manager(logger, transport, DomainStrategy::AsIs)
    }

    pub fn new_with_manager(
        _logger: Logger,
        transport: Arc<DnsTransportManager>,
        default_strategy: DomainStrategy,
    ) -> Self {
        let cache = Arc::new(DnsControlCache::new());
        Self {
            client: DnsClient::with_cache(Arc::clone(&cache)),
            cache,
            control_handle: None,
            _control_subscription: None,
            transport,
            default_strategy,
            rules: Vec::new(),
            match_cache_hits: AtomicU64::new(0),
            match_cache_misses: AtomicU64::new(0),
        }
    }

    /// Install resolved rules, looking up each `Route` action's server tag
    /// against the transport manager. Server-id existence has been
    /// validated upstream in `build_dns_rules` (hammer-core); this lookup
    /// only fails if the transport manager is racing initialisation, in
    /// which case we surface an internal error rather than crashing.
    pub fn with_rules(mut self, rules: &[DnsRule]) -> HammerResult<Self> {
        let mut resolved = Vec::with_capacity(rules.len());
        for (idx, rule) in rules.iter().enumerate() {
            let action = match &rule.action {
                DnsRuleAction::Route { server } => {
                    let transport = self.transport.get(server).ok_or_else(|| {
                        HammerError::internal(format!(
                            "dns.rules[{idx}] server '{server}' missing from transport manager"
                        ))
                    })?;
                    ResolvedDnsAction::Route(transport)
                }
                DnsRuleAction::Reject { kind } => ResolvedDnsAction::Reject(*kind),
            };
            resolved.push(ResolvedDnsRule {
                matcher: rule.matcher.clone(),
                action,
            });
        }
        self.rules = resolved;
        Ok(self)
    }

    pub fn with_control_handle(
        mut self,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Self> {
        let subscription = control_handle.subscribe_event(
            ControlEventFilter::event::<DnsCacheUpdateArgs>(),
            move |event| async move {
                if let Some(args) = event.args::<DnsCacheUpdateArgs>() {
                    args.cache.apply_update(args.update.clone());
                }
            },
        )?;
        self.client.set_control_handle(Arc::clone(&control_handle));
        self.control_handle = Some(control_handle);
        self._control_subscription = Some(subscription);
        Ok(self)
    }

    #[inline]
    fn match_rules(&self, qname: &str) -> MatchedAction {
        if let Some(cached) = self.cache.load_match(qname) {
            self.match_cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached;
        }
        self.match_cache_misses.fetch_add(1, Ordering::Relaxed);

        let action = self.compute_match(qname);
        self.publish_or_apply(DnsCacheUpdate::StoreMatch {
            qname: qname.to_owned(),
            action: action.clone(),
        });
        action
    }

    fn compute_match(&self, qname: &str) -> MatchedAction {
        for rule in &self.rules {
            if !match_dns_rule(&rule.matcher, qname) {
                continue;
            }
            return match &rule.action {
                ResolvedDnsAction::Route(transport) => MatchedAction::Route(transport.clone()),
                ResolvedDnsAction::Reject(kind) => MatchedAction::Reject(*kind),
            };
        }
        MatchedAction::Default
    }

    #[cfg(test)]
    pub(crate) fn match_cache_hits(&self) -> u64 {
        self.match_cache_hits.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn match_cache_misses(&self) -> u64 {
        self.match_cache_misses.load(Ordering::Relaxed)
    }

    pub async fn exchange(
        &self,
        message: Message,
        mut options: DnsQueryOptions,
    ) -> HammerResult<Message> {
        let query_summary = dns_query_log_summary(&message);

        // `[[dns.rules]]` matching takes precedence over the explicit
        // `options.transport` override only when no override was set —
        // callers that hand-pick a transport (`DnsRouter::lookup` for
        // bootstrap, internal probes) should not be silently re-routed.
        //
        // Skip the qname allocation entirely on the rules-empty path
        // (i.e. configs that pre-date `[[dns.rules]]`); `domain_from_name`
        // costs one `String` alloc per call and runs on every query.
        if !self.rules.is_empty() && options.transport.is_none() {
            let qname = message
                .queries
                .first()
                .map(|q| domain_from_name(q.name()))
                .unwrap_or_default();
            if !qname.is_empty() {
                match self.match_rules(&qname) {
                    MatchedAction::Route(transport) => {
                        options.transport = Some(transport);
                    }
                    MatchedAction::Reject(kind) => {
                        let code = match kind {
                            RejectKind::NxDomain => FixedResponseCode::NXDomain,
                            RejectKind::Refused => FixedResponseCode::Refused,
                        };
                        info!("reject query={query_summary} kind={kind:?}");
                        return Ok(message.fixed_response(code));
                    }
                    MatchedAction::Default => {}
                }
            }
        }

        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        info!(
            "exchange query={} server={} strategy={:?}",
            query_summary,
            transport.meta().id(),
            options.strategy
        );
        let response = match self.client.exchange(&transport, message, options).await {
            Ok(response) => response,
            Err(err) => {
                warn!("exchange failed query={query_summary}: {err}");
                return Err(err);
            }
        };
        info!(
            "exchange response query={} {}",
            query_summary,
            dns_response_log_summary(&response)
        );
        self.save_reverse_mapping(&response);
        Ok(response)
    }

    pub(crate) fn try_exchange_fast(
        &self,
        message: &Message,
        mut options: DnsQueryOptions,
    ) -> HammerResult<Option<Message>> {
        if !self.rules.is_empty() && options.transport.is_none() {
            let qname = message
                .queries
                .first()
                .map(|q| domain_from_name(q.name()))
                .unwrap_or_default();
            if !qname.is_empty() {
                match self.match_rules(&qname) {
                    MatchedAction::Route(transport) => {
                        options.transport = Some(transport);
                    }
                    MatchedAction::Reject(kind) => {
                        let code = match kind {
                            RejectKind::NxDomain => FixedResponseCode::NXDomain,
                            RejectKind::Refused => FixedResponseCode::Refused,
                        };
                        return Ok(Some(message.fixed_response(code)));
                    }
                    MatchedAction::Default => {}
                }
            }
        }

        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        let Some(response) = self
            .client
            .try_exchange_cached(&transport, message, options)?
        else {
            return Ok(None);
        };
        self.save_reverse_mapping(&response);
        Ok(Some(response))
    }

    pub async fn lookup(
        &self,
        domain: &str,
        mut options: DnsQueryOptions,
    ) -> HammerResult<Vec<IpAddr>> {
        debug!("lookup domain {}", normalize_domain(domain));
        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        let addresses = self.client.lookup(&transport, domain, options).await?;
        let expires_at = Instant::now() + Duration::from_secs(u64::from(DEFAULT_DNS_TTL));
        let domain = normalize_domain(domain);
        self.publish_or_apply(DnsCacheUpdate::StoreReverseBatch {
            entries: addresses
                .iter()
                .map(|addr| (*addr, domain.clone(), expires_at))
                .collect(),
        });
        Ok(addresses)
    }

    pub fn clear_cache(&self) {
        self.publish_or_apply(DnsCacheUpdate::Clear);
    }

    pub fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
        self.cache.lookup_reverse(ip)
    }

    pub fn reset_network(&self) {
        self.clear_cache();
        for transport in self.transport.list() {
            transport.runtime().reset();
        }
    }

    fn resolve_transport(&self, options: &DnsQueryOptions) -> HammerResult<DnsTransportComponent> {
        if let Some(transport) = &options.transport {
            return Ok(transport.clone());
        }
        self.transport
            .default()
            .ok_or_else(|| HammerError::internal("default DNS server not found"))
    }

    fn save_reverse_mapping(&self, response: &Message) {
        self.publish_or_apply(DnsCacheUpdate::StoreReverseBatch {
            entries: response
                .answers
                .iter()
                .filter_map(|record| {
                    if record.ttl == 0 {
                        return None;
                    }
                    let addr = record_addr(record)?;
                    Some((
                        addr,
                        domain_from_name(&record.name),
                        Instant::now() + Duration::from_secs(u64::from(record.ttl)),
                    ))
                })
                .collect(),
        });
    }

    fn publish_or_apply(&self, update: DnsCacheUpdate) {
        if let Some(control_handle) = &self.control_handle {
            let _ = control_handle.publish_event(DnsCacheUpdateArgs {
                cache: Arc::clone(&self.cache),
                update,
            });
            return;
        }
        self.cache.apply_update(update);
    }
}

fn prune_reverse_snapshot(reverse: &mut HashMap<IpAddr, ReverseValue>, now: Instant) {
    let expired: Vec<IpAddr> = reverse
        .iter()
        .filter(|(_, value)| now >= value.expires_at)
        .map(|(addr, _)| *addr)
        .collect();
    for addr in expired {
        reverse.remove(&addr);
    }
}

fn trim_reverse_snapshot(reverse: &mut HashMap<IpAddr, ReverseValue>, capacity: usize) {
    while reverse.len() > capacity {
        let Some(addr) = reverse
            .iter()
            .min_by_key(|(_, value)| value.expires_at)
            .map(|(addr, _)| *addr)
        else {
            return;
        };
        reverse.remove(&addr);
    }
}

fn trim_match_snapshot(match_cache: &mut HashMap<String, MatchedAction>, capacity: usize) {
    while match_cache.len() > capacity {
        let Some(qname) = match_cache.keys().min().cloned() else {
            return;
        };
        match_cache.remove(&qname);
    }
}

impl Lifecycle for DnsRouter {
    fn name(&self) -> &str {
        "dns-router"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!("stage {}", stage.name());
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        debug!("close");
        self.clear_cache();
        Ok(())
    }
}

#[async_trait(?Send)]
impl DnsRouterTrait for DnsRouter {
    async fn exchange(&self, message: Message, options: DnsQueryOptions) -> HammerResult<Message> {
        self.exchange(message, options).await
    }

    async fn lookup(&self, domain: &str, options: DnsQueryOptions) -> HammerResult<Vec<IpAddr>> {
        self.lookup(domain, options).await
    }

    fn try_exchange_fast(
        &self,
        message: &Message,
        options: DnsQueryOptions,
    ) -> HammerResult<Option<Message>> {
        DnsRouter::try_exchange_fast(self, message, options)
    }

    fn clear_cache(&self) {
        self.clear_cache();
    }

    fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
        self.lookup_reverse_mapping(ip)
    }

    fn reset_network(&self) {
        self.reset_network();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use hammer_adapter::{ComponentMeta, RuntimeComponent};
    use hammer_core::error::CoreResult;
    use hammer_core::lifecycle::StartStage;
    use hammer_core::log::{DiscardWriter, Factory};
    use hickory_proto::op::{MessageType, OpCode};
    use hickory_proto::rr::{DNSClass, Name, RData, Record};
    use std::net::Ipv4Addr;

    fn test_logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    /// Bare-bones DnsTransport that records how many times it was queried
    /// and replies with a synthesised answer. Lets us prove which transport
    /// the router picked without going through real UDP / DoH plumbing.
    struct StubTransport {
        id: String,
        queries: AtomicUsize,
        answer_addr: Ipv4Addr,
    }

    impl StubTransport {
        fn arc(id: &str, answer_addr: Ipv4Addr) -> Arc<Self> {
            Arc::new(Self {
                id: id.to_owned(),
                queries: AtomicUsize::new(0),
                answer_addr,
            })
        }
    }

    impl Lifecycle for StubTransport {
        fn name(&self) -> &str {
            &self.id
        }
        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }
        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl DnsTransport for StubTransport {
        fn reset(&self) {}
        async fn exchange(&self, message: Message) -> CoreResult<Message> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let mut response = message.fixed_response(FixedResponseCode::NoError);
            if let Some(query) = message.queries.first() {
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    60,
                    RData::A(self.answer_addr.into()),
                ));
            }
            Ok(response)
        }
    }

    fn build_query(name: &str) -> Message {
        build_query_with_type(name, RecordType::A)
    }

    fn build_query_with_type(name: &str, record_type: RecordType) -> Message {
        let mut query = Message::new(7, MessageType::Query, OpCode::Query);
        query.add_query({
            let mut q = Query::query(Name::from_ascii(name).expect("name"), record_type);
            q.set_query_class(DNSClass::IN);
            q
        });
        query
    }

    fn manager_with(stubs: &[Arc<StubTransport>], default_id: &str) -> Arc<DnsTransportManager> {
        let manager = Arc::new(DnsTransportManager::new(
            test_logger("dns-transport"),
            default_id.to_owned(),
        ));
        for stub in stubs {
            manager.insert(stub_transport_component(Arc::clone(stub)));
        }
        manager
    }

    #[cfg(feature = "dns-local")]
    #[test]
    fn dns_transport_manager_insert_accepts_concrete_component_arc() {
        let manager = DnsTransportManager::new(test_logger("dns-transport"), "local-arc");
        let transport = Arc::new(LocalDnsTransport::new("local-arc"));

        manager.insert(Arc::clone(&transport));

        let registered = manager.get("local-arc").expect("registered transport");
        assert_eq!(registered.type_name(), "local");
    }

    fn stub_transport_component(stub: Arc<StubTransport>) -> DnsTransportComponent {
        let id = stub.id.clone();
        let runtime: Arc<dyn DnsTransport> = stub;
        RuntimeComponent::new(
            ComponentMeta::new("dns_transport", "stub", id, Vec::new(), Vec::new(), None),
            runtime,
        )
    }

    fn router_with_rules(manager: Arc<DnsTransportManager>, rules: Vec<DnsRule>) -> DnsRouter {
        DnsRouter::new_with_manager(test_logger("dns-router"), manager, DomainStrategy::AsIs)
            .with_rules(&rules)
            .expect("with_rules")
    }

    #[tokio::test]
    async fn dns_router_routes_domain_to_named_transport() {
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::Domain(vec!["ifconfig.so".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("ifconfig.so."), DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(local.queries.load(Ordering::SeqCst), 1);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_routes_domain_suffix_to_named_transport() {
        let cn = StubTransport::arc("cn", Ipv4Addr::new(223, 5, 5, 5));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&cn), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::DomainSuffix(vec!["cn".to_owned()]),
            action: DnsRuleAction::Route {
                server: "cn".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("Foo.CN."), DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(cn.queries.load(Ordering::SeqCst), 1);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_rejects_with_nxdomain() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::DomainKeyword(vec!["doubleclick".to_owned()]),
            action: DnsRuleAction::Reject {
                kind: RejectKind::NxDomain,
            },
        }];
        let router = router_with_rules(manager, rules);

        let query = build_query("ad.doubleclick.net.");
        let query_id = query.metadata.id;
        let response = router
            .exchange(query, DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(response.metadata.id, query_id);
        assert_eq!(response.metadata.response_code, ResponseCode::NXDomain);
        assert!(response.answers.is_empty());
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_rejects_with_refused() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::Domain(vec!["denied.example".to_owned()]),
            action: DnsRuleAction::Reject {
                kind: RejectKind::Refused,
            },
        }];
        let router = router_with_rules(manager, rules);

        let response = router
            .exchange(build_query("denied.example."), DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(response.metadata.response_code, ResponseCode::Refused);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_falls_through_to_default_when_no_rule_matches() {
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::Domain(vec!["ifconfig.so".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("example.com."), DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(local.queries.load(Ordering::SeqCst), 0);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dns_router_caches_match_for_repeated_qname() {
        // DnsRouter::match_rules is called on every query when [[dns.rules]]
        // is non-empty. Same qname should hit the LRU and skip the rule
        // walk; the StubTransport call count is irrelevant here because
        // the DnsClient itself caches responses — we assert via the
        // hits/misses counters that the per-qname cache fired.
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::DomainSuffix(vec!["ifconfig.so".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("ifconfig.so."), DnsQueryOptions::default())
            .await
            .expect("first exchange");
        let _ = router
            .exchange(build_query("ifconfig.so."), DnsQueryOptions::default())
            .await
            .expect("second exchange");

        assert_eq!(router.match_cache_misses(), 1);
        assert_eq!(router.match_cache_hits(), 1);
    }

    #[tokio::test]
    async fn dns_router_clear_cache_drops_match_cache() {
        // clear_cache (also driven by reset_network) must invalidate the
        // match LRU so transports refreshed on a network flip are picked
        // up next time.
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::DomainSuffix(vec!["ifconfig.so".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("ifconfig.so."), DnsQueryOptions::default())
            .await
            .expect("warm-up");
        router.clear_cache();
        let _ = router
            .exchange(build_query("ifconfig.so."), DnsQueryOptions::default())
            .await
            .expect("post-clear");

        assert_eq!(router.match_cache_misses(), 2);
        assert_eq!(router.match_cache_hits(), 0);
    }

    #[tokio::test]
    async fn dns_router_match_is_case_insensitive_after_normalize() {
        // Rule strings are normalised at config-load (verified upstream);
        // the *query* side is normalised at the exchange entry. This test
        // proves the runtime normalisation path matches the config one.
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            // pre-normalised; mimics what `dns_rule_from_text` produces
            matcher: DnsRuleMatcher::Domain(vec!["example.com".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let _ = router
            .exchange(build_query("EXAMPLE.COM."), DnsQueryOptions::default())
            .await
            .expect("exchange");

        assert_eq!(local.queries.load(Ordering::SeqCst), 1);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_fastpath_returns_cached_response_without_transport_exchange() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let router =
            DnsRouter::new_with_manager(test_logger("dns-router"), manager, DomainStrategy::AsIs);

        let _ = router
            .exchange(build_query("example.com."), DnsQueryOptions::default())
            .await
            .expect("warm cache");
        let mut query = build_query("example.com.");
        query.metadata.id = 99;
        let response = router
            .try_exchange_fast(&query, DnsQueryOptions::default())
            .expect("fastpath")
            .expect("cached response");

        assert_eq!(response.metadata.id, 99);
        assert_eq!(
            response.addresses(),
            vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]
        );
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dns_client_accepts_concrete_transport_reference() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let upstream_component = stub_transport_component(Arc::clone(&upstream));
        let client = DnsClient::new(test_logger("dns-client"));

        let response = client
            .exchange(
                &upstream_component,
                build_query("example.com."),
                DnsQueryOptions::default(),
            )
            .await
            .expect("exchange");

        assert_eq!(
            response.addresses(),
            vec![IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))]
        );
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dns_router_fastpath_returns_none_on_cache_miss_without_transport_exchange() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let router =
            DnsRouter::new_with_manager(test_logger("dns-router"), manager, DomainStrategy::AsIs);

        let query = build_query("example.com.");
        let response = router
            .try_exchange_fast(&query, DnsQueryOptions::default())
            .expect("fastpath");

        assert!(response.is_none());
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_fastpath_rejects_matching_rule_without_transport_exchange() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::DomainKeyword(vec!["doubleclick".to_owned()]),
            action: DnsRuleAction::Reject {
                kind: RejectKind::NxDomain,
            },
        }];
        let router = router_with_rules(manager, rules);

        let query = build_query("ad.doubleclick.net.");
        let response = router
            .try_exchange_fast(&query, DnsQueryOptions::default())
            .expect("fastpath")
            .expect("reject response");

        assert_eq!(response.metadata.response_code, ResponseCode::NXDomain);
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_fastpath_applies_strategy_reject_without_transport_exchange() {
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&upstream)], "upstream");
        let router =
            DnsRouter::new_with_manager(test_logger("dns-router"), manager, DomainStrategy::AsIs);
        let options = DnsQueryOptions {
            strategy: DomainStrategy::Ipv4Only,
            ..DnsQueryOptions::default()
        };

        let query = build_query_with_type("example.com.", RecordType::AAAA);
        let response = router
            .try_exchange_fast(&query, options)
            .expect("fastpath")
            .expect("strategy response");

        assert_eq!(response.metadata.response_code, ResponseCode::NoError);
        assert!(response.answers.is_empty());
        assert_eq!(upstream.queries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_router_caller_supplied_transport_bypasses_rules() {
        // Bootstrap / internal probes pre-pick a transport — rules must
        // not silently re-route them.
        let local = StubTransport::arc("local", Ipv4Addr::new(127, 0, 0, 1));
        let upstream = StubTransport::arc("upstream", Ipv4Addr::new(8, 8, 8, 8));
        let manager = manager_with(&[Arc::clone(&local), Arc::clone(&upstream)], "upstream");
        let rules = vec![DnsRule {
            matcher: DnsRuleMatcher::Domain(vec!["ifconfig.so".to_owned()]),
            action: DnsRuleAction::Route {
                server: "local".to_owned(),
            },
        }];
        let router = router_with_rules(manager, rules);

        let mut options = DnsQueryOptions::default();
        options.transport = Some(stub_transport_component(Arc::clone(&upstream)));

        let _ = router
            .exchange(build_query("ifconfig.so."), options)
            .await
            .expect("exchange");

        assert_eq!(upstream.queries.load(Ordering::SeqCst), 1);
        assert_eq!(local.queries.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dns_log_summary_includes_question_and_answers() {
        let mut query = Message::new(7, MessageType::Query, OpCode::Query);
        query.add_query({
            let mut q = Query::query(Name::from_ascii("Example.COM.").unwrap(), RecordType::A);
            q.set_query_class(DNSClass::IN);
            q
        });

        let mut response = query.fixed_response(FixedResponseCode::NoError);
        response.add_answer(Record::from_rdata(
            Name::from_ascii("example.com.").unwrap(),
            60,
            RData::A(Ipv4Addr::new(203, 0, 113, 9).into()),
        ));

        assert_eq!(dns_query_log_summary(&query), "example.com A");
        assert_eq!(
            dns_response_log_summary(&response),
            "rcode=NoError answers=1 addresses=203.0.113.9"
        );
    }
}
