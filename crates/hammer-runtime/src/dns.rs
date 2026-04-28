use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hammer_adapter::{
    DnsQueryOptions, DnsRouter as DnsRouterTrait, DnsTransport,
    DnsTransportManager as DnsTransportManagerTrait, Lifecycle, StartStage,
};
use hammer_core::config::{
    DnsOptions, DnsServer, DnsServerKind, DomainStrategy, RemoteDnsServer, RemoteHttpsDnsServer,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;
use hickory_proto::op::{Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::serialize::binary::{BinDecodable, BinDecoder, BinEncodable, BinEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

const DEFAULT_DNS_TTL: u32 = 600;
const DNS_TIMEOUT: Duration = Duration::from_secs(10);
const DOH_MIME_TYPE: &str = "application/dns-message";

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

#[derive(Hash, PartialEq, Eq)]
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
    logger: Logger,
    cache: Mutex<HashMap<CacheKey, CacheValue>>,
}

impl DnsClient {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn exchange(
        &self,
        transport: Arc<dyn DnsTransport>,
        message: Message,
        options: DnsQueryOptions,
    ) -> Result<Message, HammerError> {
        if message.queries.len() != 1 {
            self.logger
                .warn(format!("bad question size: {}", message.queries.len()));
            return Ok(message.fixed_response(FixedResponseCode::FormatError));
        }
        let query = first_query(&message)?.clone();
        if strategy_rejects(query.query_type(), options.strategy) {
            return Ok(message.fixed_response(FixedResponseCode::NoError));
        }
        let key = CacheKey {
            transport: transport.tag().to_owned(),
            name: query.name().to_ascii().to_ascii_lowercase(),
            record_type: query.query_type(),
        };
        if !options.disable_cache {
            if let Some(mut cached) = self.load_cache(&key) {
                cached.metadata.id = message.metadata.id;
                return Ok(cached);
            }
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
        let ttl = value.expires_at.saturating_duration_since(now).as_secs() as u32;
        let mut response = value.response.clone();
        normalize_ttl(&mut response, ttl);
        Some(response)
    }

    fn store_cache(&self, key: CacheKey, response: &Message, ttl: u32) {
        self.cache.lock().expect("DnsClient cache poisoned").insert(
            key,
            CacheValue {
                response: response.clone(),
                expires_at: Instant::now() + Duration::from_secs(u64::from(ttl)),
            },
        );
    }
}

fn strategy_rejects(record_type: RecordType, strategy: DomainStrategy) -> bool {
    matches!(
        (record_type, strategy),
        (RecordType::A, DomainStrategy::Ipv6Only) | (RecordType::AAAA, DomainStrategy::Ipv4Only)
    )
}

pub struct HostsTransport {
    tag: String,
    dependencies: Vec<String>,
    predefined: HashMap<String, Vec<IpAddr>>,
}

impl HostsTransport {
    pub fn from_predefined<I, S>(tag: impl Into<String>, entries: I) -> Self
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
            tag: tag.into(),
            dependencies: Vec::new(),
            predefined,
        }
    }

    pub fn system(tag: impl Into<String>) -> Self {
        let content = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
        let entries = parse_hosts(&content);
        Self::from_predefined(tag, entries)
    }
}

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
impl DnsTransport for HostsTransport {
    fn type_name(&self) -> &str {
        "hosts"
    }
    fn tag(&self) -> &str {
        &self.tag
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

pub struct LocalDnsTransport {
    tag: String,
    dependencies: Vec<String>,
}

impl LocalDnsTransport {
    pub fn new(tag: impl Into<String>) -> Self {
        Self {
            tag: tag.into(),
            dependencies: Vec::new(),
        }
    }
}

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
impl DnsTransport for LocalDnsTransport {
    fn type_name(&self) -> &str {
        "local"
    }
    fn tag(&self) -> &str {
        &self.tag
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

pub struct UdpDnsTransport {
    tag: String,
    server: String,
    port: u16,
    logger: Logger,
    dependencies: Vec<String>,
}

impl UdpDnsTransport {
    pub fn new(tag: impl Into<String>, server: String, port: u16, logger: Logger) -> Self {
        Self {
            tag: tag.into(),
            server,
            port,
            logger,
            dependencies: Vec::new(),
        }
    }
}

impl Lifecycle for UdpDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/udp"
    }
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for UdpDnsTransport {
    fn type_name(&self) -> &str {
        "udp"
    }
    fn tag(&self) -> &str {
        &self.tag
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        let server = resolve_first(&self.server, self.port).await?;
        let bind = if server.is_ipv6() {
            "[::]:0"
        } else {
            "0.0.0.0:0"
        };
        let socket = UdpSocket::bind(bind)
            .await
            .map_err(|e| HammerError::internal(format!("bind UDP DNS socket: {e}")))?;
        socket
            .connect(server)
            .await
            .map_err(|e| HammerError::internal(format!("connect UDP DNS socket: {e}")))?;
        socket
            .send(&MessageExt::to_bytes(&message)?)
            .await
            .map_err(|e| HammerError::internal(format!("write UDP DNS request: {e}")))?;
        let mut buf = vec![0_u8; 4096];
        let len = socket
            .recv(&mut buf)
            .await
            .map_err(|e| HammerError::internal(format!("read UDP DNS response: {e}")))?;
        let response = <Message as MessageExt>::from_bytes(&buf[..len])?;
        if response.metadata.truncation {
            self.logger.info("response truncated, retrying with TCP");
            return tcp_exchange(&server, message).await;
        }
        Ok(response)
    }
}

pub struct TcpDnsTransport {
    tag: String,
    server: String,
    port: u16,
    dependencies: Vec<String>,
}

impl TcpDnsTransport {
    pub fn new(tag: impl Into<String>, server: String, port: u16, _logger: Logger) -> Self {
        Self {
            tag: tag.into(),
            server,
            port,
            dependencies: Vec::new(),
        }
    }
}

impl Lifecycle for TcpDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/tcp"
    }
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for TcpDnsTransport {
    fn type_name(&self) -> &str {
        "tcp"
    }
    fn tag(&self) -> &str {
        &self.tag
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        let server = resolve_first(&self.server, self.port).await?;
        tcp_exchange(&server, message).await
    }
}

async fn tcp_exchange(server: &SocketAddr, message: Message) -> Result<Message, HammerError> {
    let mut stream = TcpStream::connect(server)
        .await
        .map_err(|e| HammerError::internal(format!("connect TCP DNS socket: {e}")))?;
    let bytes = MessageExt::to_bytes(&message)?;
    let len = u16::try_from(bytes.len())
        .map_err(|_| HammerError::internal("DNS message exceeds TCP frame limit"))?;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| HammerError::internal(format!("write TCP DNS length: {e}")))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| HammerError::internal(format!("write TCP DNS request: {e}")))?;
    let mut len_buf = [0_u8; 2];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS length: {e}")))?;
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS response: {e}")))?;
    <Message as MessageExt>::from_bytes(&payload)
}

pub struct HttpsDnsTransport {
    tag: String,
    url: String,
    client: reqwest::Client,
    dependencies: Vec<String>,
}

impl HttpsDnsTransport {
    pub fn new(tag: impl Into<String>, options: &RemoteHttpsDnsServer) -> Self {
        let host = if options.server_port == 443 {
            options.server.clone()
        } else {
            format!("{}:{}", options.server, options.server_port)
        };
        let path = if options.path.is_empty() {
            "/dns-query"
        } else {
            &options.path
        };
        Self {
            tag: tag.into(),
            url: format!("https://{host}{path}"),
            client: reqwest::Client::new(),
            dependencies: Vec::new(),
        }
    }
}

impl Lifecycle for HttpsDnsTransport {
    fn name(&self) -> &str {
        "dns/transport/https"
    }
    fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
        Ok(())
    }
    fn close(&self) -> Result<(), HammerError> {
        Ok(())
    }
}

#[async_trait]
impl DnsTransport for HttpsDnsTransport {
    fn type_name(&self) -> &str {
        "https"
    }
    fn tag(&self) -> &str {
        &self.tag
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn reset(&self) {}

    async fn exchange(&self, message: Message) -> Result<Message, HammerError> {
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, DOH_MIME_TYPE)
            .header(reqwest::header::ACCEPT, DOH_MIME_TYPE)
            .body(MessageExt::to_bytes(&message)?)
            .send()
            .await
            .map_err(|e| HammerError::internal(format!("send HTTPS DNS request: {e}")))?;
        if !response.status().is_success() {
            return Err(HammerError::internal(format!(
                "unexpected DNS HTTPS status: {}",
                response.status()
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|e| HammerError::internal(format!("read HTTPS DNS response: {e}")))?;
        <Message as MessageExt>::from_bytes(&bytes)
    }
}

async fn resolve_first(server: &str, port: u16) -> Result<SocketAddr, HammerError> {
    if let Ok(ip) = server.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let mut addrs = tokio::net::lookup_host((server, port))
        .await
        .map_err(|e| HammerError::internal(format!("resolve DNS server {server}: {e}")))?;
    addrs
        .next()
        .ok_or_else(|| HammerError::internal(format!("resolve DNS server {server}: empty result")))
}

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
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn DnsTransport>>>,
    default_tag: String,
}

impl DnsTransportManager {
    pub fn new(logger: Logger, default_tag: impl Into<String>) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            default_tag: default_tag.into(),
        }
    }

    pub fn from_options(logger: Logger, options: &DnsOptions) -> Result<Self, HammerError> {
        let manager = Self::new(logger.clone(), options.final_.clone());
        for server in &options.servers {
            manager.insert(build_transport(server, &logger)?);
        }
        Ok(manager)
    }

    pub fn insert(&self, transport: Arc<dyn DnsTransport>) {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .insert(transport.tag().to_owned(), transport);
    }

    pub fn list(&self) -> Vec<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, tag: &str) -> Option<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .get(tag)
            .cloned()
    }

    pub fn default(&self) -> Option<Arc<dyn DnsTransport>> {
        self.get(&self.default_tag)
    }
}

fn build_transport(
    server: &DnsServer,
    logger: &Logger,
) -> Result<Arc<dyn DnsTransport>, HammerError> {
    let transport: Arc<dyn DnsTransport> = match &server.kind {
        DnsServerKind::Udp(RemoteDnsServer {
            server: host,
            server_port,
            ..
        }) => Arc::new(UdpDnsTransport::new(
            server.tag.clone(),
            host.clone(),
            *server_port,
            logger.clone(),
        )),
        DnsServerKind::Tcp(RemoteDnsServer {
            server: host,
            server_port,
            ..
        }) => Arc::new(TcpDnsTransport::new(
            server.tag.clone(),
            host.clone(),
            *server_port,
            logger.clone(),
        )),
        DnsServerKind::Https(options) => {
            Arc::new(HttpsDnsTransport::new(server.tag.clone(), options))
        }
        DnsServerKind::Hosts => Arc::new(HostsTransport::system(server.tag.clone())),
        DnsServerKind::Local => Arc::new(LocalDnsTransport::new(server.tag.clone())),
    };
    Ok(transport)
}

impl Lifecycle for DnsTransportManager {
    fn name(&self) -> &str {
        "dns-transport"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        if stage != StartStage::Start {
            return Ok(());
        }
        if self.default().is_none() {
            return Err(HammerError::internal(format!(
                "default DNS server not found: {}",
                self.default_tag
            )));
        }
        for transport in self.list() {
            transport.start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.logger.debug("close");
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

    fn get(&self, tag: &str) -> Option<Arc<dyn DnsTransport>> {
        self.get(tag)
    }

    fn default(&self) -> Option<Arc<dyn DnsTransport>> {
        self.default()
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .remove(tag);
        Ok(())
    }
}

struct ReverseValue {
    domain: String,
    expires_at: Instant,
}

pub struct DnsRouter {
    logger: Logger,
    transport: Arc<DnsTransportManager>,
    client: DnsClient,
    default_strategy: DomainStrategy,
    reverse: Mutex<HashMap<IpAddr, ReverseValue>>,
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
            logger: logger.clone(),
            client: DnsClient::new(logger.clone()),
            transport,
            default_strategy,
            reverse: Mutex::new(HashMap::new()),
        }
    }

    pub async fn exchange(
        &self,
        message: Message,
        mut options: DnsQueryOptions,
    ) -> Result<Message, HammerError> {
        self.logger.debug("exchange");
        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        let response = self.client.exchange(transport, message, options).await?;
        self.save_reverse_mapping(&response);
        Ok(response)
    }

    pub async fn lookup(
        &self,
        domain: &str,
        mut options: DnsQueryOptions,
    ) -> Result<Vec<IpAddr>, HammerError> {
        self.logger
            .debug(format!("lookup domain {}", normalize_domain(domain)));
        let transport = self.resolve_transport(&options)?;
        if options.strategy == DomainStrategy::AsIs {
            options.strategy = self.default_strategy;
        }
        let addresses = self.client.lookup(transport, domain, options).await?;
        let expires_at = Instant::now() + Duration::from_secs(u64::from(DEFAULT_DNS_TTL));
        let domain = normalize_domain(domain);
        let mut reverse = self.reverse.lock().expect("DnsRouter reverse poisoned");
        for addr in &addresses {
            reverse.insert(
                *addr,
                ReverseValue {
                    domain: domain.clone(),
                    expires_at,
                },
            );
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
        let value = reverse.get(&ip)?;
        if Instant::now() >= value.expires_at {
            reverse.remove(&ip);
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
        for record in &response.answers {
            if let Some(addr) = record_addr(record) {
                reverse.insert(
                    addr,
                    ReverseValue {
                        domain: domain_from_name(&record.name),
                        expires_at: Instant::now() + Duration::from_secs(u64::from(record.ttl)),
                    },
                );
            }
        }
    }
}

impl Lifecycle for DnsRouter {
    fn name(&self) -> &str {
        "dns-router"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.logger.debug("close");
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
