use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, ComponentMeta, ComponentMetadata, DataPlaneBuffers, IcmpReply, Lifecycle, Network,
    Outbound, OutboundComponent, OutboundManager as AdapterOutboundManager, ProxyIcmpConn,
    ProxyPacketConn, ProxyStream, RuntimeComponent, SocksAddr,
};
use hammer_core::config::{Outbound as OutboundOptions, OutboundKind};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::metrics::{MetricCounter, MetricsRegistry, MetricsScope, NetworkCounters};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::ControlThreadHandle;
use crate::RuntimePlatform;
#[cfg(any(feature = "outbound-block"))]
use crate::component_registry::register_components;
use crate::socket_protector::SocketProtector;

#[cfg(feature = "outbound-block")]
pub use crate::protocol::block::BlockOutbound;

pub(crate) type OutboundBuilder = fn(
    Logger,
    String,
    &OutboundKind,
    SocketProtector,
    Option<Arc<ControlThreadHandle>>,
) -> HammerResult<OutboundComponent>;

pub struct OutboundRegistration(OutboundComponent);

impl From<OutboundComponent> for OutboundRegistration {
    fn from(outbound: OutboundComponent) -> Self {
        Self(outbound)
    }
}

impl<T> From<Arc<T>> for OutboundRegistration
where
    T: Outbound + ComponentMetadata + 'static,
{
    fn from(outbound: Arc<T>) -> Self {
        let meta = ComponentMetadata::component_meta(outbound.as_ref());
        let runtime: Arc<dyn Outbound> = outbound;
        Self(RuntimeComponent::new(meta, runtime))
    }
}

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
        control_handle: Option<Arc<ControlThreadHandle>>,
    ) -> HammerResult<OutboundComponent> {
        let type_name = option.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown outbound type: {type_name}"))
        })?;
        builder(
            logger,
            option.id.clone(),
            &option.kind,
            protector,
            control_handle,
        )
    }
}

fn register_standard_outbound_builders(builders: &mut HashMap<&'static str, OutboundBuilder>) {
    #[cfg(not(any(feature = "outbound-block")))]
    let _ = builders;
    #[cfg(feature = "outbound-block")]
    register_components!(outbound, builders, [crate::protocol::block::BlockOutbound]);
}

pub struct OutboundManager {
    logger: Logger,
    items: Mutex<HashMap<String, OutboundComponent>>,
    default_id: String,
    factories: OutboundFactorySet,
    metrics: Arc<MetricsRegistry>,
    control_handle: Option<Arc<ControlThreadHandle>>,
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
            control_handle: None,
        }
    }

    pub fn from_options(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
    ) -> HammerResult<Self> {
        Self::from_options_with_protector(logger, default_id, options, SocketProtector::default())
    }

    pub fn from_options_with_platform(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self> {
        Self::from_options_with_protector(
            logger,
            default_id,
            options,
            SocketProtector::from(platform.into()),
        )
    }

    pub fn from_options_with_platform_and_metrics(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: impl Into<RuntimePlatform>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Self> {
        Self::from_options_with_protector_and_metrics(
            logger,
            default_id,
            options,
            SocketProtector::from(platform.into()),
            metrics,
            None,
        )
    }

    pub fn from_options_with_platform_metrics_and_control(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: impl Into<RuntimePlatform>,
        metrics: Arc<MetricsRegistry>,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Self> {
        Self::from_options_with_protector_and_metrics(
            logger,
            default_id,
            options,
            SocketProtector::from(platform.into()),
            metrics,
            Some(control_handle),
        )
    }

    pub(crate) fn from_options_with_protector(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        protector: impl Into<SocketProtector>,
    ) -> HammerResult<Self> {
        Self::from_options_with_protector_and_metrics(
            logger,
            default_id,
            options,
            protector,
            MetricsRegistry::new(),
            None,
        )
    }

    pub(crate) fn from_options_with_protector_and_metrics(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        protector: impl Into<SocketProtector>,
        metrics: Arc<MetricsRegistry>,
        control_handle: Option<Arc<ControlThreadHandle>>,
    ) -> HammerResult<Self> {
        let protector = protector.into();
        let mut manager = Self::new_with_metrics(logger, default_id, metrics);
        manager.control_handle = control_handle;
        for option in options {
            manager.register_descriptor_with_protector(option, protector.clone())?;
        }
        Ok(manager)
    }

    pub fn register_descriptor(&self, option: &OutboundOptions) -> HammerResult<()> {
        self.register_descriptor_with_protector(option, SocketProtector::default())
    }

    pub fn reset_network(&self) {
        for outbound in self.list() {
            outbound.runtime().reset();
        }
    }

    pub fn ensure_connected(&self) {
        self.spawn_ensure_connected_hooks(self.list());
    }

    /// Register an already-constructed outbound. This is for tests and future
    /// embedders that need to supply a runtime outbound directly; L3 endpoints
    /// are routed through `RouteTarget::Endpoint`, not registered here.
    pub fn register_outbound(
        &self,
        component: impl Into<OutboundRegistration>,
    ) -> HammerResult<()> {
        let component = component.into().0;
        let id = component.meta().id().to_owned();
        let mut items = self.items.lock().expect("OutboundManager poisoned");
        if items.contains_key(&id) {
            return Err(HammerError::config_validation(format!(
                "duplicate outbound id: {id}"
            )));
        }
        let wrapped = self.wrap_outbound(component.meta().clone(), component.into_runtime());
        items.insert(id, wrapped);
        Ok(())
    }

    fn register_descriptor_with_protector(
        &self,
        option: &OutboundOptions,
        protector: impl Into<SocketProtector>,
    ) -> HammerResult<()> {
        let protector = protector.into();
        let descriptor = self.factories.build(
            self.logger.clone(),
            option,
            protector,
            self.control_handle.as_ref().map(Arc::clone),
        )?;
        let mut items = self.items.lock().expect("OutboundManager poisoned");
        if items.contains_key(&option.id) {
            return Err(HammerError::config_validation(format!(
                "duplicate outbound id: {}",
                option.id
            )));
        }
        let descriptor = self.wrap_outbound(descriptor.meta().clone(), descriptor.into_runtime());
        items.insert(option.id.clone(), descriptor);
        Ok(())
    }

    fn wrap_outbound(&self, meta: ComponentMeta, outbound: Arc<dyn Outbound>) -> OutboundComponent {
        let metrics = meta.metrics();
        let module = metrics.map_or("outbound", |m| m.module);
        let component_type = metrics.map_or("outbound", |m| m.component_type);
        let id = meta.id().to_owned();
        let runtime: Arc<dyn Outbound> = Arc::new(InstrumentedOutbound::new(
            outbound,
            self.metrics.scope(module, component_type, id),
        ));
        RuntimeComponent::new(meta, runtime)
    }

    fn spawn_post_start_hooks(&self) {
        let items: Vec<OutboundComponent> = self
            .items
            .lock()
            .expect("OutboundManager poisoned")
            .values()
            .cloned()
            .collect();
        for outbound in items {
            let id = outbound.meta().id().to_owned();
            let runtime = Arc::clone(outbound.runtime());
            let logger = self.logger.clone();
            crate::spawn::spawn(async move {
                if let Err(err) = runtime.post_start().await {
                    logger.warn(format!("outbound '{id}' post_start: {err}"));
                }
            });
        }
    }

    fn spawn_ensure_connected_hooks(&self, items: Vec<OutboundComponent>) {
        for outbound in items {
            let id = outbound.meta().id().to_owned();
            let runtime = Arc::clone(outbound.runtime());
            let logger = self.logger.clone();
            crate::spawn::spawn(async move {
                if let Err(err) = runtime.ensure_connected().await {
                    logger.warn(format!("outbound '{id}' ensure_connected: {err}"));
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
    fn reset(&self) {
        self.inner.reset();
    }

    async fn ensure_connected(&self) -> HammerResult<()> {
        match self.inner.ensure_connected().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.ensure_connected_error_total.inc();
                Err(err)
            }
        }
    }

    async fn dial(
        &self,
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> HammerResult<Box<dyn ProxyStream>> {
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

    async fn listen_packet(&self) -> HammerResult<Box<dyn ProxyPacketConn>> {
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

    async fn listen_icmp(&self) -> HammerResult<Box<dyn ProxyIcmpConn>> {
        match self.inner.listen_icmp().await {
            Ok(conn) => Ok(Box::new(InstrumentedIcmpConn { inner: conn })),
            Err(err) => Err(err),
        }
    }

    async fn probe_latency(&self, protocol: &str, timeout: Duration) -> HammerResult<Duration> {
        match self.inner.probe_latency(protocol, timeout).await {
            Ok(duration) => Ok(duration),
            Err(err) => {
                self.metrics.probe_latency_error_total.inc();
                Err(err)
            }
        }
    }

    async fn post_start(&self) -> HammerResult<()> {
        match self.inner.post_start().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.post_start_error_total.inc();
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
    post_start_error_total: MetricCounter,
    ensure_connected_error_total: MetricCounter,
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
            post_start_error_total: scope.counter("post_start_error_total"),
            ensure_connected_error_total: scope.counter("ensure_connected_error_total"),
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

#[async_trait::async_trait(?Send)]
impl ProxyPacketConn for InstrumentedPacketConn {
    async fn send(
        &mut self,
        runtime: &DataPlaneBuffers,
        frame: &mut BufferFrame,
    ) -> HammerResult<()> {
        match self.inner.send(runtime, frame).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.metrics.packet_send_error_total.inc();
                Err(err)
            }
        }
    }

    async fn recv(
        &mut self,
        runtime: &DataPlaneBuffers,
        frame: &mut BufferFrame,
        max: usize,
    ) -> HammerResult<()> {
        match self.inner.recv(runtime, frame, max).await {
            Ok(()) => Ok(()),
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
    async fn send_echo(&mut self, destination: std::net::IpAddr, body: &[u8]) -> HammerResult<()> {
        match self.inner.send_echo(destination, body).await {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn recv_reply(&mut self) -> HammerResult<IcmpReply> {
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

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        tracing::debug!(target: "outbound", "stage {}", stage.name());
        if stage == StartStage::PostStart {
            self.spawn_post_start_hooks();
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        self.reset_network();
        tracing::debug!(target: "outbound", "close");
        Ok(())
    }
}

impl AdapterOutboundManager for OutboundManager {
    fn list(&self) -> Vec<OutboundComponent> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<OutboundComponent> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .get(id)
            .cloned()
    }

    fn default(&self) -> Option<OutboundComponent> {
        if self.default_id.is_empty() {
            return None;
        }
        self.get(&self.default_id)
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use hammer_adapter::{ComponentMeta, Network, RuntimeComponent};
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::log::{DiscardWriter, Factory};

    fn test_logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    #[test]
    fn register_outbound_accepts_concrete_component_arc() {
        let manager = OutboundManager::new(test_logger("outbound-manager"), "test-outbound-arc");
        struct ConcreteOutbound;

        #[async_trait::async_trait]
        impl Outbound for ConcreteOutbound {
            async fn dial(
                &self,
                _network: Network,
                _destination: SocksAddr,
                _initial_payload: &[u8],
            ) -> CoreResult<Box<dyn ProxyStream>> {
                Err(CoreError::internal("concrete outbound: dial not supported"))
            }

            async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
                Err(CoreError::internal(
                    "concrete outbound: listen_packet not supported",
                ))
            }
        }

        impl ComponentMetadata for ConcreteOutbound {
            fn component_meta(&self) -> ComponentMeta {
                ComponentMeta::new(
                    "outbound",
                    "concrete",
                    "test-outbound-arc",
                    vec![Network::Tcp],
                    Vec::new(),
                    None,
                )
            }
        }

        let outbound = Arc::new(ConcreteOutbound);

        manager
            .register_outbound(Arc::clone(&outbound))
            .expect("register concrete outbound");

        let registered = manager
            .get("test-outbound-arc")
            .expect("registered outbound");
        assert_eq!(registered.type_name(), "concrete");
    }

    /// Bare-bones Outbound that only counts how many times `reset` was called
    /// so we can prove `OutboundManager::reset_network` actually fans out to
    /// every registered outbound. Required because service reset-network handling
    /// previously skipped this fan-out, leaving cached outbound clients stale
    /// after iOS sleep/wake.
    struct CountingOutbound {
        resets: AtomicUsize,
    }

    impl CountingOutbound {
        fn arc(id: &str) -> Arc<Self> {
            let _ = id;
            Arc::new(Self {
                resets: AtomicUsize::new(0),
            })
        }

        fn component(id: &str, outbound: Arc<Self>) -> OutboundComponent {
            let runtime: Arc<dyn Outbound> = outbound;
            RuntimeComponent::new(
                ComponentMeta::new(
                    "outbound",
                    "counting",
                    id,
                    vec![Network::Tcp],
                    Vec::new(),
                    None,
                ),
                runtime,
            )
        }
    }

    #[async_trait::async_trait]
    impl Outbound for CountingOutbound {
        fn reset(&self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }

        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> CoreResult<Box<dyn ProxyStream>> {
            Err(CoreError::internal("counting outbound: dial not supported"))
        }

        async fn listen_packet(&self) -> CoreResult<Box<dyn ProxyPacketConn>> {
            Err(CoreError::internal(
                "counting outbound: listen_packet not supported",
            ))
        }
    }

    #[test]
    fn reset_network_invokes_reset_on_every_registered_outbound() {
        let manager = OutboundManager::new(test_logger("outbound-test"), "default");
        let a = CountingOutbound::arc("a");
        let b = CountingOutbound::arc("b");
        manager
            .register_outbound(CountingOutbound::component("a", Arc::clone(&a)))
            .expect("register outbound a");
        manager
            .register_outbound(CountingOutbound::component("b", Arc::clone(&b)))
            .expect("register outbound b");

        manager.reset_network();

        assert_eq!(a.resets.load(Ordering::SeqCst), 1, "outbound a reset once");
        assert_eq!(b.resets.load(Ordering::SeqCst), 1, "outbound b reset once");

        manager.reset_network();
        assert_eq!(a.resets.load(Ordering::SeqCst), 2, "second pass also fans");
        assert_eq!(b.resets.load(Ordering::SeqCst), 2, "second pass also fans");
    }
}
