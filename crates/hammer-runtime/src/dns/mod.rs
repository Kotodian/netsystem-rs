use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use async_trait::async_trait;
use hammer_adapter::{
    DnsQueryOptions, DnsRouter as DnsRouterTrait, DnsTransport,
    DnsTransportManager as DnsTransportManagerTrait, Lifecycle, PlatformInterface, StartStage,
};
use hammer_core::config::{
    DnsOptions, DnsRule, DnsRuleAction, DnsRuleMatcher, DnsServer, DnsServerKind, DomainStrategy,
    RejectKind, normalize_domain,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::serialize::binary::{BinDecodable, BinDecoder, BinEncodable, BinEncoder};
use lru::LruCache;

use crate::OutboundManager;
use crate::socket_protector::SocketProtector;

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

#[derive(Clone, Copy)]
pub enum FixedResponseCode {
    NoError,
    NXDomain,
    FormatError,
    Refused,
}

impl FixedResponseCode {
    fn response_code(self) -> ResponseCode {
        match self {
            Self::NoError => ResponseCode::NoError,
            Self::NXDomain => ResponseCode::NXDomain,
            Self::FormatError => ResponseCode::FormErr,
            Self::Refused => ResponseCode::Refused,
        }
    }
}

pub trait MessageExt {
    fn from_bytes(bytes: &[u8]) -> Result<Message, HammerError>;
    fn to_bytes(&self) -> Result<Vec<u8>, HammerError>;
    fn fixed_response(&self, code: FixedResponseCode) -> Message;
    fn addresses(&self) -> Vec<IpAddr>;
}

impl MessageExt for Message {
    fn from_bytes(bytes: &[u8]) -> Result<Message, HammerError> {
        let mut decoder = BinDecoder::new(bytes);
        Message::read(&mut decoder)
            .map_err(|e| HammerError::internal(format!("decode DNS message: {e}")))
    }

    fn to_bytes(&self) -> Result<Vec<u8>, HammerError> {
        let mut bytes = Vec::with_capacity(512);
        let mut encoder = BinEncoder::new(&mut bytes);
        self.emit(&mut encoder)
            .map_err(|e| HammerError::internal(format!("encode DNS message: {e}")))?;
        Ok(bytes)
    }

    fn fixed_response(&self, code: FixedResponseCode) -> Message {
        let mut response = Message::new(self.metadata.id, MessageType::Response, OpCode::Query);
        response.metadata.authoritative = true;
        response.metadata.recursion_desired = true;
        response.metadata.recursion_available = true;
        response.metadata.response_code = code.response_code();
        for query in &self.queries {
            response.add_query(query.clone());
        }
        response
    }

    fn addresses(&self) -> Vec<IpAddr> {
        self.answers.iter().filter_map(record_addr).collect()
    }
}

fn record_addr(record: &Record) -> Option<IpAddr> {
    match &record.data {
        RData::A(addr) => Some(IpAddr::V4(Ipv4Addr::from(*addr))),
        RData::AAAA(addr) => Some(IpAddr::V6(Ipv6Addr::from(*addr))),
        _ => None,
    }
}

fn fqdn(domain: &str) -> Result<Name, HammerError> {
    let name = if domain.ends_with('.') {
        domain.to_owned()
    } else {
        format!("{domain}.")
    };
    Name::from_ascii(&name).map_err(|e| HammerError::internal(format!("invalid domain: {e}")))
}

/// Hot path on every DNS query. We avoid `.trim_end_matches('.').to_ascii_lowercase()`
/// because that materialises two `String`s; instead we take the single
/// `to_ascii()` allocation, truncate the trailing-dot run in place, then
/// `make_ascii_lowercase()` in place. Functionally identical to
/// `normalize_domain(&name.to_ascii())` but one allocation instead of two.
#[inline]
fn domain_from_name(name: &Name) -> String {
    let mut s = name.to_ascii();
    let new_len = s.trim_end_matches('.').len();
    s.truncate(new_len);
    s.make_ascii_lowercase();
    s
}

pub(in crate::dns) fn query_message(
    domain: &str,
    record_type: RecordType,
) -> Result<Message, HammerError> {
    let mut message = Message::new(0, MessageType::Query, OpCode::Query);
    message.add_query({
        let mut query = Query::query(fqdn(domain)?, record_type);
        query.set_query_class(DNSClass::IN);
        query
    });
    message.metadata.recursion_desired = true;
    Ok(message)
}

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

fn first_query(message: &Message) -> Result<&Query, HammerError> {
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

struct CacheValue {
    response: Message,
    expires_at: Instant,
}

pub struct DnsClient {
    cache: Mutex<LruCache<CacheKey, CacheValue>>,
}

impl DnsClient {
    pub fn new(_logger: Logger) -> Self {
        Self {
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DNS_CLIENT_CACHE_MAX_ENTRIES)
                    .expect("DNS client cache capacity must be non-zero"),
            )),
        }
    }

    pub async fn exchange(
        &self,
        transport: Arc<dyn DnsTransport>,
        message: Message,
        options: DnsQueryOptions,
    ) -> Result<Message, HammerError> {
        if message.queries.len() != 1 {
            warn!("bad question size: {}", message.queries.len());
            return Ok(message.fixed_response(FixedResponseCode::FormatError));
        }
        let query = first_query(&message)?.clone();
        if strategy_rejects(query.query_type(), options.strategy) {
            return Ok(message.fixed_response(FixedResponseCode::NoError));
        }
        let key = CacheKey {
            transport: transport.id().to_owned(),
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
        let mut response = tokio::time::timeout(DNS_TIMEOUT, transport.exchange(message))
            .await
            .map_err(|_| HammerError::internal("dns query timed out"))??;
        let ttl = apply_response_options(&mut response, &options);
        if !options.disable_cache && ttl > 0 {
            self.store_cache(key, &response, ttl);
        }
        Ok(response)
    }

    pub async fn lookup(
        &self,
        transport: Arc<dyn DnsTransport>,
        domain: &str,
        options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, HammerError> {
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
                    .lookup_type(
                        Arc::clone(&transport),
                        domain,
                        RecordType::A,
                        options.clone(),
                    )
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
        self.cache.lock().expect("DnsClient cache poisoned").clear();
    }

    async fn lookup_type(
        &self,
        transport: Arc<dyn DnsTransport>,
        domain: &str,
        record_type: RecordType,
        mut options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, HammerError> {
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
        let mut cache = self.cache.lock().expect("DnsClient cache poisoned");
        let value = cache.get_mut(key)?;
        let now = Instant::now();
        if now >= value.expires_at {
            cache.pop(key);
            return None;
        }
        let ttl = value.expires_at.saturating_duration_since(now).as_secs() as u32;
        let mut response = value.response.clone();
        normalize_ttl(&mut response, ttl);
        Some(response)
    }

    fn store_cache(&self, key: CacheKey, response: &Message, ttl: u32) {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("DnsClient cache poisoned");
        prune_cache(&mut cache, now);
        cache.put(
            key,
            CacheValue {
                response: response.clone(),
                expires_at: now + Duration::from_secs(u64::from(ttl)),
            },
        );
    }
}

fn prune_cache(cache: &mut LruCache<CacheKey, CacheValue>, now: Instant) {
    let expired: Vec<CacheKey> = cache
        .iter()
        .filter(|(_, value)| now >= value.expires_at)
        .map(|(key, _)| key.clone())
        .collect();
    for key in expired {
        cache.pop(&key);
    }
}

fn strategy_rejects(record_type: RecordType, strategy: DomainStrategy) -> bool {
    matches!(
        (record_type, strategy),
        (RecordType::A, DomainStrategy::Ipv6Only) | (RecordType::AAAA, DomainStrategy::Ipv4Only)
    )
}

#[cfg(feature = "dns-hosts")]
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
fn parse_hosts(content: &str) -> Vec<(String, IpAddr)> {
    let mut entries = Vec::new();
    for line in content.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let Some(addr) = fields.next().and_then(|v| v.parse::<IpAddr>().ok()) else {
            continue;
        };
        for domain in fields {
            entries.push((domain.to_owned(), addr));
        }
    }
    entries
}

#[cfg(feature = "dns-hosts")]
impl Lifecycle for HostsTransport {
    fn name(&self) -> &str {
        "dns/transport/hosts"
    }
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
#[cfg(feature = "dns-hosts")]
impl DnsTransport for HostsTransport {
    fn type_name(&self) -> &str {
        "hosts"
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
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
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
#[cfg(feature = "dns-local")]
impl DnsTransport for LocalDnsTransport {
    fn type_name(&self) -> &str {
        "local"
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
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

#[cfg(any(feature = "dns-hosts", feature = "dns-local"))]
fn fixed_address_response(
    request: &Message,
    query: &Query,
    addresses: Vec<IpAddr>,
    ttl: u32,
) -> Message {
    let mut response = request.fixed_response(FixedResponseCode::NoError);
    for address in addresses {
        match (query.query_type(), address) {
            (RecordType::A, IpAddr::V4(ip)) => {
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    ttl,
                    RData::A(ip.into()),
                ));
            }
            (RecordType::AAAA, IpAddr::V6(ip)) => {
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    ttl,
                    RData::AAAA(ip.into()),
                ));
            }
            _ => {}
        }
    }
    response
}

pub struct DnsTransportManager {
    items: Mutex<HashMap<String, Arc<dyn DnsTransport>>>,
    default_id: String,
    factories: DnsTransportFactorySet,
}

type DnsTransportBuilder = fn(
    String,
    &DnsServerKind,
    Logger,
    Option<Arc<OutboundManager>>,
    Option<Arc<dyn DnsTransport>>,
    SocketProtector,
) -> Result<Arc<dyn DnsTransport>, HammerError>;

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
        bootstrap: Option<Arc<dyn DnsTransport>>,
        protector: SocketProtector,
    ) -> Result<Arc<dyn DnsTransport>, HammerError> {
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
    builders.insert(
        "udp",
        transport::udp::build_transport as DnsTransportBuilder,
    );
    #[cfg(feature = "dns-tcp")]
    builders.insert(
        "tcp",
        transport::tcp::build_transport as DnsTransportBuilder,
    );
    #[cfg(feature = "dns-https")]
    builders.insert(
        "https",
        transport::doh::build_transport as DnsTransportBuilder,
    );
    #[cfg(feature = "dns-hosts")]
    builders.insert("hosts", build_hosts_transport as DnsTransportBuilder);
    #[cfg(feature = "dns-local")]
    builders.insert("local", build_local_transport as DnsTransportBuilder);
}

impl DnsTransportManager {
    pub fn new(_logger: Logger, default_id: impl Into<String>) -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
            default_id: default_id.into(),
            factories: DnsTransportFactorySet::standard(),
        }
    }

    pub fn from_options(logger: Logger, options: &DnsOptions) -> Result<Self, HammerError> {
        Self::from_options_inner(logger, options, None, None, SocketProtector::default())
    }

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &DnsOptions,
        outbound: Arc<OutboundManager>,
        platform: Arc<dyn PlatformInterface>,
        default_domain_resolver: Option<&str>,
    ) -> Result<Self, HammerError> {
        Self::from_options_inner(
            logger,
            options,
            Some(outbound),
            default_domain_resolver,
            SocketProtector::new(platform),
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
        protector: SocketProtector,
    ) -> Result<Self, HammerError> {
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

    pub fn insert(&self, transport: Arc<dyn DnsTransport>) {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .insert(transport.id().to_owned(), transport);
    }

    pub fn list(&self) -> Vec<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .get(id)
            .cloned()
    }

    pub fn default(&self) -> Option<Arc<dyn DnsTransport>> {
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
) -> Result<Vec<&'a DnsServer>, HammerError> {
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
) -> Result<(), HammerError> {
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
    _bootstrap: Option<Arc<dyn DnsTransport>>,
    _protector: SocketProtector,
) -> Result<Arc<dyn DnsTransport>, HammerError> {
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
    _bootstrap: Option<Arc<dyn DnsTransport>>,
    _protector: SocketProtector,
) -> Result<Arc<dyn DnsTransport>, HammerError> {
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

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
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
            transport.start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        debug!("close");
        for transport in self.list() {
            transport.close()?;
        }
        Ok(())
    }
}

impl DnsTransportManagerTrait for DnsTransportManager {
    fn list(&self) -> Vec<Arc<dyn DnsTransport>> {
        self.list()
    }

    fn get(&self, id: &str) -> Option<Arc<dyn DnsTransport>> {
        self.get(id)
    }

    fn default(&self) -> Option<Arc<dyn DnsTransport>> {
        self.default()
    }

    fn remove(&self, id: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .remove(id);
        Ok(())
    }
}

struct ReverseValue {
    domain: String,
    expires_at: Instant,
}

pub struct DnsRouter {
    transport: Arc<DnsTransportManager>,
    client: DnsClient,
    default_strategy: DomainStrategy,
    /// Resolved `[[dns.rules]]`. Empty = default-only behaviour, the
    /// pre-feature path. Populated once at construction; never mutated.
    rules: Vec<ResolvedDnsRule>,
    reverse: Mutex<LruCache<IpAddr, ReverseValue>>,
    /// Per-qname LRU of the rule-walk outcome. Same role as the
    /// `Router::cache` for proxy routing — sustained traffic to the same
    /// host hits the same set of qnames repeatedly, so a 256-entry LRU
    /// collapses N rule walks into one hashmap lookup.
    match_cache: Mutex<LruCache<String, MatchedAction>>,
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
    Route(Arc<dyn DnsTransport>),
    Reject(RejectKind),
}

/// Outcome of `match_rules`. `Default` means "no rule matched, fall
/// through to the existing transport-resolution path".
///
/// `Clone` so the per-qname LRU can hand out copies cheaply: `Route`
/// clones an `Arc`, `Reject` is `Copy`, `Default` is unit.
#[derive(Clone)]
enum MatchedAction {
    Route(Arc<dyn DnsTransport>),
    Reject(RejectKind),
    Default,
}

impl DnsRouter {
    pub fn new(logger: Logger) -> Self {
        let transport = Arc::new(DnsTransportManager::new(logger.clone(), String::new()));
        Self::new_with_manager(logger, transport, DomainStrategy::AsIs)
    }

    pub fn new_with_manager(
        logger: Logger,
        transport: Arc<DnsTransportManager>,
        default_strategy: DomainStrategy,
    ) -> Self {
        Self {
            client: DnsClient::new(logger.clone()),
            transport,
            default_strategy,
            rules: Vec::new(),
            reverse: Mutex::new(LruCache::new(
                NonZeroUsize::new(DNS_REVERSE_CACHE_MAX_ENTRIES)
                    .expect("DNS reverse cache capacity must be non-zero"),
            )),
            match_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DNS_MATCH_CACHE_CAPACITY)
                    .expect("DNS match cache capacity must be non-zero"),
            )),
            match_cache_hits: AtomicU64::new(0),
            match_cache_misses: AtomicU64::new(0),
        }
    }

    /// Install resolved rules, looking up each `Route` action's server tag
    /// against the transport manager. Server-id existence has been
    /// validated upstream in `build_dns_rules` (hammer-core); this lookup
    /// only fails if the transport manager is racing initialisation, in
    /// which case we surface an internal error rather than crashing.
    pub fn with_rules(mut self, rules: &[DnsRule]) -> Result<Self, HammerError> {
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

    #[inline]
    fn match_rules(&self, qname: &str) -> MatchedAction {
        if let Some(cached) = self
            .match_cache
            .lock()
            .expect("DNS match cache poisoned")
            .get(qname)
            .cloned()
        {
            self.match_cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached;
        }
        self.match_cache_misses.fetch_add(1, Ordering::Relaxed);

        let action = self.compute_match(qname);
        self.match_cache
            .lock()
            .expect("DNS match cache poisoned")
            .put(qname.to_owned(), action.clone());
        action
    }

    fn compute_match(&self, qname: &str) -> MatchedAction {
        for rule in &self.rules {
            if !match_dns_rule(&rule.matcher, qname) {
                continue;
            }
            return match &rule.action {
                ResolvedDnsAction::Route(transport) => MatchedAction::Route(Arc::clone(transport)),
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
    ) -> Result<Message, HammerError> {
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
            transport.id(),
            options.strategy
        );
        let response = match self.client.exchange(transport, message, options).await {
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

    pub async fn lookup(
        &self,
        domain: &str,
        mut options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, HammerError> {
        debug!("lookup domain {}", normalize_domain(domain));
        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        let addresses = self.client.lookup(transport, domain, options).await?;
        let expires_at = Instant::now() + Duration::from_secs(u64::from(DEFAULT_DNS_TTL));
        let domain = normalize_domain(domain);
        let mut reverse = self.reverse.lock().expect("DnsRouter reverse poisoned");
        prune_reverse_cache(&mut reverse, Instant::now());
        for addr in &addresses {
            self.store_reverse_mapping_locked(&mut reverse, *addr, domain.clone(), expires_at);
        }
        Ok(addresses)
    }

    pub fn clear_cache(&self) {
        self.client.clear_cache();
        self.reverse
            .lock()
            .expect("DnsRouter reverse poisoned")
            .clear();
        // Match-cache entries point at `Arc<dyn DnsTransport>` clones from
        // the manager; on a network flip the underlying transports may
        // have been replaced or reset, so drop the cache to force a fresh
        // walk on the next query.
        self.match_cache
            .lock()
            .expect("DNS match cache poisoned")
            .clear();
    }

    pub fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
        let mut reverse = self.reverse.lock().expect("DnsRouter reverse poisoned");
        let value = reverse.get_mut(&ip)?;
        if Instant::now() >= value.expires_at {
            reverse.pop(&ip);
            return None;
        }
        Some(value.domain.clone())
    }

    pub fn reset_network(&self) {
        self.clear_cache();
        for transport in self.transport.list() {
            transport.reset();
        }
    }

    fn resolve_transport(
        &self,
        options: &DnsQueryOptions,
    ) -> Result<Arc<dyn DnsTransport>, HammerError> {
        if let Some(transport) = &options.transport {
            return Ok(Arc::clone(transport));
        }
        self.transport
            .default()
            .ok_or_else(|| HammerError::internal("default DNS server not found"))
    }

    fn save_reverse_mapping(&self, response: &Message) {
        let mut reverse = self.reverse.lock().expect("DnsRouter reverse poisoned");
        prune_reverse_cache(&mut reverse, Instant::now());
        for record in &response.answers {
            if let Some(addr) = record_addr(record) {
                if record.ttl == 0 {
                    continue;
                }
                self.store_reverse_mapping_locked(
                    &mut reverse,
                    addr,
                    domain_from_name(&record.name),
                    Instant::now() + Duration::from_secs(u64::from(record.ttl)),
                );
            }
        }
    }

    fn store_reverse_mapping_locked(
        &self,
        reverse: &mut LruCache<IpAddr, ReverseValue>,
        addr: IpAddr,
        domain: String,
        expires_at: Instant,
    ) {
        reverse.put(addr, ReverseValue { domain, expires_at });
    }
}

fn prune_reverse_cache(reverse: &mut LruCache<IpAddr, ReverseValue>, now: Instant) {
    let expired: Vec<IpAddr> = reverse
        .iter()
        .filter(|(_, value)| now >= value.expires_at)
        .map(|(addr, _)| *addr)
        .collect();
    for addr in expired {
        reverse.pop(&addr);
    }
}

impl Lifecycle for DnsRouter {
    fn name(&self) -> &str {
        "dns-router"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        debug!("stage {}", stage.name());
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        debug!("close");
        self.clear_cache();
        Ok(())
    }
}

#[async_trait]
impl DnsRouterTrait for DnsRouter {
    async fn exchange(
        &self,
        message: Message,
        options: DnsQueryOptions,
    ) -> Result<Message, HammerError> {
        self.exchange(message, options).await
    }

    async fn lookup(
        &self,
        domain: &str,
        options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, HammerError> {
        self.lookup(domain, options).await
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

    use hammer_core::error::CoreError;
    use hammer_core::lifecycle::StartStage;
    use hammer_core::log::{DiscardWriter, Factory};

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
        fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
            Ok(())
        }
        fn close(&self) -> Result<(), HammerError> {
            Ok(())
        }
    }

    #[async_trait]
    impl DnsTransport for StubTransport {
        fn type_name(&self) -> &str {
            "stub"
        }
        fn id(&self) -> &str {
            &self.id
        }
        fn dependencies(&self) -> &[String] {
            &[]
        }
        fn reset(&self) {}
        async fn exchange(&self, message: Message) -> Result<Message, CoreError> {
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
        let mut query = Message::new(7, MessageType::Query, OpCode::Query);
        query.add_query({
            let mut q = Query::query(Name::from_ascii(name).expect("name"), RecordType::A);
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
            manager.insert(Arc::clone(stub) as Arc<dyn DnsTransport>);
        }
        manager
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
        options.transport = Some(Arc::clone(&upstream) as Arc<dyn DnsTransport>);

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
