use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use hammer_adapter::{
    IcmpReply, Lifecycle, Network, Outbound, OutboundManager as OutboundManagerTrait,
    PlatformInterface, ProbeReport, ProxyDatagram, ProxyIcmpConn, ProxyPacketConn, ProxyStream,
    SocksAddr,
};
use hammer_core::config::{Outbound as OutboundOptions, OutboundKind};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::metrics::{MetricCounter, MetricsRegistry, MetricsScope, NetworkCounters};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::socket_protector::SocketProtector;

#[cfg(feature = "outbound-block")]
pub use crate::protocol::block::BlockOutbound;
#[cfg(feature = "outbound-direct")]
pub use crate::protocol::direct::DirectOutbound;

type OutboundBuilder =
    fn(Logger, String, &OutboundKind, SocketProtector) -> Result<Arc<dyn Outbound>, HammerError>;

#[derive(Clone)]
struct OutboundFactorySet {
    builders: Arc<HashMap<&'static str, OutboundBuilder>>,
}

impl OutboundFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_outbound_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build(
        &self,
        logger: Logger,
        option: &OutboundOptions,
        protector: SocketProtector,
    ) -> Result<Arc<dyn Outbound>, HammerError> {
        let type_name = option.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown outbound type: {type_name}"))
        })?;
        builder(logger, option.id.clone(), &option.kind, protector)
    }
}

#[allow(unused_variables)]
fn register_standard_outbound_builders(builders: &mut HashMap<&'static str, OutboundBuilder>) {
    #[cfg(feature = "outbound-hysteria2")]
    builders.insert(
        "hysteria2",
        crate::protocol::hysteria2::build_outbound as OutboundBuilder,
    );
    #[cfg(feature = "outbound-direct")]
    builders.insert(
        "direct",
        crate::protocol::direct::build_outbound as OutboundBuilder,
    );
    #[cfg(feature = "outbound-block")]
    builders.insert(
        "block",
        crate::protocol::block::build_outbound as OutboundBuilder,
    );
    #[cfg(feature = "outbound-urltest")]
    builders.insert(
        "urltest",
        crate::protocol::urltest::build_outbound as OutboundBuilder,
    );
}

/// `out.Manager` port. DNS is routed through DnsRouter/DnsTransport instead of
/// a dialable outbound.
pub struct OutboundManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn Outbound>>>,
    default_id: String,
    factories: OutboundFactorySet,
    metrics: Arc<MetricsRegistry>,
}

impl OutboundManager {
    pub fn new(logger: Logger, default_id: impl Into<String>) -> Self {
        Self::new_with_metrics(logger, default_id, MetricsRegistry::new())
    }

    pub fn new_with_metrics(
        logger: Logger,
        default_id: impl Into<String>,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            default_id: default_id.into(),
            factories: OutboundFactorySet::standard(),
            metrics,
        }
    }

    pub fn from_options(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
    ) -> Result<Self, HammerError> {
        Self::from_options_with_protector(logger, default_id, options, SocketProtector::default())
    }

    pub fn from_options_with_platform(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: Arc<dyn PlatformInterface>,
    ) -> Result<Self, HammerError> {
        Self::from_options_with_protector(
            logger,
            default_id,
            options,
            SocketProtector::new(platform),
        )
    }

    pub fn from_options_with_platform_and_metrics(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: Arc<dyn PlatformInterface>,
        metrics: Arc<MetricsRegistry>,
    ) -> Result<Self, HammerError> {
        Self::from_options_with_protector_and_metrics(
            logger,
            default_id,
            options,
            SocketProtector::new(platform),
            metrics,
        )
    }

    pub(crate) fn from_options_with_protector(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        protector: SocketProtector,
    ) -> Result<Self, HammerError> {
        Self::from_options_with_protector_and_metrics(
            logger,
            default_id,
            options,
            protector,
            MetricsRegistry::new(),
        )
    }

    pub(crate) fn from_options_with_protector_and_metrics(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        protector: SocketProtector,
        metrics: Arc<MetricsRegistry>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new_with_metrics(logger, default_id, metrics);
        for option in options {
            manager.register_descriptor_with_protector(option, protector.clone())?;
        }
        Ok(manager)
    }

    pub fn register_descriptor(&self, option: &OutboundOptions) -> Result<(), HammerError> {
        self.register_descriptor_with_protector(option, SocketProtector::default())
    }

    pub fn reset_network(&self) {
        for outbound in self.list() {
            outbound.reset();
        }
    }

    /// Register an already-constructed outbound (e.g. an endpoint that lives
    /// in `EndpointManager`) so the router can resolve its id through the
    /// usual `OutboundManager::get` path. Mirrors sing-box, where every
    /// endpoint shows up as both an Endpoint *and* an Outbound — same Arc,
    /// two views.
    pub fn register_outbound(
        &self,
        id: String,
        descriptor: Arc<dyn Outbound>,
    ) -> Result<(), HammerError> {
        let mut items = self.items.lock().expect("OutboundManager poisoned");
        if items.contains_key(&id) {
            return Err(HammerError::config_validation(format!(
                "duplicate outbound id: {id}"
            )));
        }
        let wrapped = self.wrap_outbound(&id, descriptor);
        items.insert(id, wrapped);
        Ok(())
    }

    fn register_descriptor_with_protector(
        &self,
        option: &OutboundOptions,
        protector: SocketProtector,
    ) -> Result<(), HammerError> {
        let descriptor = self
            .factories
            .build(self.logger.clone(), option, protector)?;
        let mut items = self.items.lock().expect("OutboundManager poisoned");
        if items.contains_key(&option.id) {
            return Err(HammerError::config_validation(format!(
                "duplicate outbound id: {}",
                option.id
            )));
        }
        let descriptor = self.wrap_outbound(&option.id, descriptor);
        items.insert(option.id.clone(), descriptor);
        Ok(())
    }

    fn wrap_outbound(&self, id: &str, outbound: Arc<dyn Outbound>) -> Arc<dyn Outbound> {
        Arc::new(InstrumentedOutbound::new(
            outbound,
            self.metrics.scope("outbound", "outbound", id),
        ))
    }

    /// Inject this manager (as a `Weak<dyn OutboundManager>`) into every
    /// registered aggregate outbound so they can resolve their children
    /// dynamically without owning the manager. Idempotent — leaves use the
    /// default no-op `bind_resolver` and don't care; aggregates ignore
    /// duplicate calls.
    ///
    /// Must be called **after** all outbounds (including endpoint-derived
    /// ones registered via `register_outbound`) are in place, otherwise
    /// late-registered children would be invisible to the aggregate.
    pub fn bind_aggregates(self: &Arc<Self>) {
        let weak: Weak<Self> = Arc::downgrade(self);
        let resolver: Weak<dyn OutboundManagerTrait> = weak;
        let items = self.items.lock().expect("OutboundManager poisoned");
        for outbound in items.values() {
            outbound.bind_resolver(resolver.clone());
        }
    }

    fn spawn_post_start_hooks(&self) {
        let items: Vec<Arc<dyn Outbound>> = self
            .items
            .lock()
            .expect("OutboundManager poisoned")
            .values()
            .cloned()
            .collect();
        for outbound in items {
            let id = outbound.id().to_owned();
            let logger = self.logger.clone();
            crate::spawn::spawn(async move {
                if let Err(err) = outbound.post_start().await {
                    logger.warn(format!("outbound '{id}' post_start: {err}"));
                }
            });
        }
    }
}

struct InstrumentedOutbound {
    inner: Arc<dyn Outbound>,
    metrics: OutboundMetrics,
}

impl InstrumentedOutbound {
    fn new(inner: Arc<dyn Outbound>, scope: MetricsScope) -> Self {
        Self {
            inner,
            metrics: OutboundMetrics::new(scope),
        }
    }
}

#[async_trait::async_trait]
impl Outbound for InstrumentedOutbound {
    fn type_name(&self) -> &str {
        self.inner.type_name()
    }

    fn id(&self) -> &str {
        self.inner.id()
    }

    fn networks(&self) -> &[Network] {
        self.inner.networks()
    }

    fn dependencies(&self) -> &[String] {
        self.inner.dependencies()
    }

    fn reset(&self) {
        self.inner.reset();
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        match self.inner.dial(network, destination, initial_payload).await {
            Ok(stream) => Ok(Box::new(InstrumentedProxyStream::new(
                stream,
                self.metrics.clone(),
            ))),
            Err(err) => {
                self.metrics.dial_error_total.inc(network);
                Err(err)
            }
        }
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        match self.inner.listen_packet().await {
            Ok(conn) => Ok(Box::new(InstrumentedPacketConn {
                inner: conn,
                metrics: self.metrics.clone(),
            })),
            Err(err) => {
                self.metrics.listen_packet_error_total.inc();
                Err(err)
            }
        }
    }

    async fn listen_icmp(&self) -> Result<Box<dyn ProxyIcmpConn>, HammerError> {
        match self.inner.listen_icmp().await {
            Ok(conn) => Ok(Box::new(InstrumentedIcmpConn { inner: conn })),
            Err(err) => Err(err),
        }
    }

    async fn probe_latency(
        &self,
        protocol: &str,
        timeout: Duration,
    ) -> Result<Duration, HammerError> {
        match self.inner.probe_latency(protocol, timeout).await {
            Ok(duration) => Ok(duration),
            Err(err) => {
                self.metrics.probe_latency_error_total.inc();
                Err(err)
            }
        }
    }

    async fn post_start(&self) -> Result<(), HammerError> {
        match self.inner.post_start().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.post_start_error_total.inc();
                Err(err)
            }
        }
    }

    fn now(&self) -> Option<String> {
        self.inner.now()
    }

    fn probe_group_timeout(&self, timeout: Duration) -> Duration {
        self.inner.probe_group_timeout(timeout)
    }

    fn bind_resolver(&self, resolver: Weak<dyn OutboundManagerTrait>) {
        self.inner.bind_resolver(resolver);
    }

    async fn probe_group(&self, timeout: Duration) -> Result<Vec<ProbeReport>, HammerError> {
        match self.inner.probe_group(timeout).await {
            Ok(reports) => Ok(reports),
            Err(err) => {
                self.metrics.probe_group_error_total.inc();
                Err(err)
            }
        }
    }
}

#[derive(Clone)]
struct OutboundMetrics {
    dial_error_total: NetworkCounters,
    listen_packet_error_total: MetricCounter,
    probe_latency_error_total: MetricCounter,
    probe_group_error_total: MetricCounter,
    post_start_error_total: MetricCounter,
    stream_read_error_total: MetricCounter,
    stream_write_error_total: MetricCounter,
    packet_send_error_total: MetricCounter,
    packet_recv_error_total: MetricCounter,
}

impl OutboundMetrics {
    fn new(scope: MetricsScope) -> Self {
        Self {
            dial_error_total: NetworkCounters::new(&scope, "dial_error_total"),
            listen_packet_error_total: scope.counter("listen_packet_error_total"),
            probe_latency_error_total: scope.counter("probe_latency_error_total"),
            probe_group_error_total: scope.counter("probe_group_error_total"),
            post_start_error_total: scope.counter("post_start_error_total"),
            stream_read_error_total: scope.counter("stream_read_error_total"),
            stream_write_error_total: scope.counter("stream_write_error_total"),
            packet_send_error_total: scope.counter("packet_send_error_total"),
            packet_recv_error_total: scope.counter("packet_recv_error_total"),
        }
    }
}

struct InstrumentedProxyStream {
    inner: Box<dyn ProxyStream>,
    metrics: OutboundMetrics,
}

impl InstrumentedProxyStream {
    fn new(inner: Box<dyn ProxyStream>, metrics: OutboundMetrics) -> Self {
        Self { inner, metrics }
    }
}

impl AsyncRead for InstrumentedProxyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut *this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                this.metrics.stream_read_error_total.inc();
                Poll::Ready(Err(err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for InstrumentedProxyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut *this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => Poll::Ready(Ok(written)),
            Poll::Ready(Err(err)) => {
                this.metrics.stream_write_error_total.inc();
                Poll::Ready(Err(err))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut *this.inner).poll_shutdown(cx)
    }
}

struct InstrumentedPacketConn {
    inner: Box<dyn ProxyPacketConn>,
    metrics: OutboundMetrics,
}

#[async_trait::async_trait]
impl ProxyPacketConn for InstrumentedPacketConn {
    async fn send_to(&mut self, destination: SocksAddr, payload: &[u8]) -> Result<(), HammerError> {
        match self.inner.send_to(destination, payload).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.packet_send_error_total.inc();
                Err(err)
            }
        }
    }

    async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
        match self.inner.recv_from().await {
            Ok(datagram) => Ok(datagram),
            Err(err) => {
                self.metrics.packet_recv_error_total.inc();
                Err(err)
            }
        }
    }
}

struct InstrumentedIcmpConn {
    inner: Box<dyn ProxyIcmpConn>,
}

#[async_trait::async_trait]
impl ProxyIcmpConn for InstrumentedIcmpConn {
    async fn send_echo(
        &mut self,
        destination: std::net::IpAddr,
        body: &[u8],
    ) -> Result<(), HammerError> {
        match self.inner.send_echo(destination, body).await {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn recv_reply(&mut self) -> Result<IcmpReply, HammerError> {
        match self.inner.recv_reply().await {
            Ok(reply) => Ok(reply),
            Err(err) => Err(err),
        }
    }
}

impl Lifecycle for OutboundManager {
    fn name(&self) -> &str {
        "outbound"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        tracing::debug!(target: "outbound", "stage {}", stage.name());
        if stage == StartStage::PostStart {
            self.spawn_post_start_hooks();
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.reset_network();
        tracing::debug!(target: "outbound", "close");
        Ok(())
    }
}

impl OutboundManagerTrait for OutboundManager {
    fn list(&self) -> Vec<Arc<dyn Outbound>> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<Arc<dyn Outbound>> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .get(id)
            .cloned()
    }

    fn default(&self) -> Option<Arc<dyn Outbound>> {
        if self.default_id.is_empty() {
            return None;
        }
        self.get(&self.default_id)
    }

    fn remove(&self, id: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .remove(id);
        Ok(())
    }
}
