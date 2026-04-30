use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use async_trait::async_trait;
use hammer_adapter::{
    DnsQueryOptions, DnsRouter as DnsRouterTrait, DnsTransport,
    DnsTransportManager as DnsTransportManagerTrait, Lifecycle, PlatformInterface, StartStage,
};
use hammer_core::config::{DnsOptions, DnsServer, DnsServerKind, DomainStrategy};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::serialize::binary::{BinDecodable, BinDecoder, BinEncodable, BinEncoder};

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

fn domain_from_name(name: &Name) -> String {
    name.to_ascii().trim_end_matches('.').to_ascii_lowercase()
}

fn normalize_domain(domain: &str) -> String {
    domain.trim_end_matches('.').to_ascii_lowercase()
}

fn query_message(domain: &str, record_type: RecordType) -> Result<Message, HammerError> {
    let mut message = Message::new(0, MessageType::Query, OpCode::Query);
    message.add_query({
        let mut query = Query::query(fqdn(domain)?, record_type);
        query.set_query_class(DNSClass::IN);
        query
    });
    message.metadata.recursion_desired = true;
    Ok(message)
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
    last_used: u64,
}

pub struct DnsClient {
    cache: Mutex<HashMap<CacheKey, CacheValue>>,
    cache_tick: AtomicU64,
}

impl DnsClient {
    pub fn new(_logger: Logger) -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
            cache_tick: AtomicU64::new(0),
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
            cache.remove(key);
            return None;
        }
        value.last_used = self.next_cache_tick();
        let ttl = value.expires_at.saturating_duration_since(now).as_secs() as u32;
        let mut response = value.response.clone();
        normalize_ttl(&mut response, ttl);
        Some(response)
    }

    fn store_cache(&self, key: CacheKey, response: &Message, ttl: u32) {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("DnsClient cache poisoned");
        prune_cache(&mut cache, now);
        if !cache.contains_key(&key) && cache.len() >= DNS_CLIENT_CACHE_MAX_ENTRIES {
            evict_lru_cache_entry(&mut cache);
        }
        cache.insert(
            key,
            CacheValue {
                response: response.clone(),
                expires_at: now + Duration::from_secs(u64::from(ttl)),
                last_used: self.next_cache_tick(),
            },
        );
    }

    fn next_cache_tick(&self) -> u64 {
        self.cache_tick.fetch_add(1, Ordering::Relaxed) + 1
    }
}

fn prune_cache(cache: &mut HashMap<CacheKey, CacheValue>, now: Instant) {
    cache.retain(|_, value| now < value.expires_at);
}

fn evict_lru_cache_entry(cache: &mut HashMap<CacheKey, CacheValue>) {
    if let Some(key) = cache
        .iter()
        .min_by_key(|(_, value)| value.last_used)
        .map(|(key, _)| key.clone())
    {
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
        let manager = Self::new(logger.clone(), options.final_.clone());
        for server in &options.servers {
            manager.insert(manager.factories.build(
                server,
                &logger,
                None,
                SocketProtector::default(),
            )?);
        }
        Ok(manager)
    }

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &DnsOptions,
        outbound: Arc<OutboundManager>,
        platform: Arc<dyn PlatformInterface>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger.clone(), options.final_.clone());
        let protector = SocketProtector::new(platform);
        for server in &options.servers {
            manager.insert(manager.factories.build(
                server,
                &logger,
                Some(Arc::clone(&outbound)),
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

#[cfg(feature = "dns-hosts")]
fn build_hosts_transport(
    id: String,
    kind: &DnsServerKind,
    _logger: Logger,
    _outbound: Option<Arc<OutboundManager>>,
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
    last_used: u64,
}

pub struct DnsRouter {
    transport: Arc<DnsTransportManager>,
    client: DnsClient,
    default_strategy: DomainStrategy,
    reverse: Mutex<HashMap<IpAddr, ReverseValue>>,
    reverse_tick: AtomicU64,
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
            reverse: Mutex::new(HashMap::new()),
            reverse_tick: AtomicU64::new(0),
        }
    }

    pub async fn exchange(
        &self,
        message: Message,
        mut options: DnsQueryOptions,
    ) -> Result<Message, HammerError> {
        let query_summary = dns_query_log_summary(&message);
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
    }

    pub fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
        let mut reverse = self.reverse.lock().expect("DnsRouter reverse poisoned");
        let value = reverse.get_mut(&ip)?;
        if Instant::now() >= value.expires_at {
            reverse.remove(&ip);
            return None;
        }
        value.last_used = self.next_reverse_tick();
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
        reverse: &mut HashMap<IpAddr, ReverseValue>,
        addr: IpAddr,
        domain: String,
        expires_at: Instant,
    ) {
        if !reverse.contains_key(&addr) && reverse.len() >= DNS_REVERSE_CACHE_MAX_ENTRIES {
            evict_lru_reverse_entry(reverse);
        }
        reverse.insert(
            addr,
            ReverseValue {
                domain,
                expires_at,
                last_used: self.next_reverse_tick(),
            },
        );
    }

    fn next_reverse_tick(&self) -> u64 {
        self.reverse_tick.fetch_add(1, Ordering::Relaxed) + 1
    }
}

fn prune_reverse_cache(reverse: &mut HashMap<IpAddr, ReverseValue>, now: Instant) {
    reverse.retain(|_, value| now < value.expires_at);
}

fn evict_lru_reverse_entry(reverse: &mut HashMap<IpAddr, ReverseValue>) {
    if let Some(addr) = reverse
        .iter()
        .min_by_key(|(_, value)| value.last_used)
        .map(|(addr, _)| *addr)
    {
        reverse.remove(&addr);
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
