use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hammer_adapter::{
    ComponentMeta, ComponentMetadata, DnsQueryOptions, DnsRouter as DnsRouterTrait, Inbound,
    InboundComponent, InboundManager as InboundManagerTrait, Lifecycle, Network, PlatformInterface,
    Router as RouterTrait, RuntimeComponent, TunOptions,
};
use hammer_core::config::{Inbound as InboundOptions, InboundKind, TunInboundOptions};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;
use hickory_proto::op::Message;
use tracing::debug;

#[cfg(any(
    feature = "inbound-socks",
    feature = "inbound-http",
    feature = "inbound-mixed"
))]
use crate::component_registry::register_components;
use crate::{OutboundManager, RuntimePlatform};

pub struct RuntimeDnsRouter {
    inner: Arc<dyn DnsRouterTrait>,
}

impl RuntimeDnsRouter {
    fn new(inner: Arc<dyn DnsRouterTrait>) -> Self {
        Self { inner }
    }
}

impl Lifecycle for RuntimeDnsRouter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        self.inner.start(stage)
    }

    fn close(&self) -> HammerResult<()> {
        self.inner.close()
    }
}

#[async_trait(?Send)]
impl DnsRouterTrait for RuntimeDnsRouter {
    async fn exchange(&self, message: Message, options: DnsQueryOptions) -> HammerResult<Message> {
        self.inner.exchange(message, options).await
    }

    async fn lookup(&self, domain: &str, options: DnsQueryOptions) -> HammerResult<Vec<IpAddr>> {
        self.inner.lookup(domain, options).await
    }

    fn try_exchange_fast(
        &self,
        message: &Message,
        options: DnsQueryOptions,
    ) -> HammerResult<Option<Message>> {
        self.inner.try_exchange_fast(message, options)
    }

    fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    fn lookup_reverse_mapping(&self, ip: IpAddr) -> Option<String> {
        self.inner.lookup_reverse_mapping(ip)
    }

    fn reset_network(&self) {
        self.inner.reset_network();
    }
}

pub(crate) type InboundBuilder = fn(
    String,
    Logger,
    &InboundKind,
    Arc<dyn RouterTrait>,
    Option<Arc<RuntimeDnsRouter>>,
    Option<Arc<OutboundManager>>,
    Option<Arc<dyn PlatformInterface>>,
    Arc<MetricsRegistry>,
) -> HammerResult<InboundComponent>;

#[derive(Clone)]
struct InboundFactorySet {
    builders: Arc<HashMap<&'static str, InboundBuilder>>,
}

impl InboundFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_inbound_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        &self,
        logger: Logger,
        option: &InboundOptions,
        router: Arc<dyn RouterTrait>,
        dns_router: Option<Arc<RuntimeDnsRouter>>,
        outbound: Option<Arc<OutboundManager>>,
        platform: Option<Arc<dyn PlatformInterface>>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<InboundComponent> {
        let type_name = option.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown inbound type: {type_name}"))
        })?;
        builder(
            option.id.clone(),
            logger,
            &option.kind,
            router,
            dns_router,
            outbound,
            platform,
            metrics,
        )
    }
}

fn register_standard_inbound_builders(builders: &mut HashMap<&'static str, InboundBuilder>) {
    #[cfg(not(any(
        feature = "inbound-socks",
        feature = "inbound-http",
        feature = "inbound-mixed"
    )))]
    let _ = builders;
    builders.insert("tun", build_tun_inbound);
    #[cfg(feature = "inbound-socks")]
    register_components!(
        inbound,
        builders,
        [crate::protocol::proxy::inbound::SocksInbound]
    );
    #[cfg(feature = "inbound-http")]
    register_components!(
        inbound,
        builders,
        [crate::protocol::proxy::inbound::HttpInbound]
    );
    #[cfg(feature = "inbound-mixed")]
    register_components!(
        inbound,
        builders,
        [crate::protocol::proxy::inbound::MixedInbound]
    );
}

#[allow(clippy::too_many_arguments)]
fn build_tun_inbound(
    id: String,
    _logger: Logger,
    kind: &InboundKind,
    _router: Arc<dyn RouterTrait>,
    _dns_router: Option<Arc<RuntimeDnsRouter>>,
    _outbound: Option<Arc<OutboundManager>>,
    platform: Option<Arc<dyn PlatformInterface>>,
    _metrics: Arc<MetricsRegistry>,
) -> HammerResult<InboundComponent> {
    let InboundKind::Tun(options) = kind else {
        return Err(HammerError::config_validation(format!(
            "inbound type mismatch for tun builder: {}",
            match kind {
                InboundKind::Tun(_) => "tun",
                InboundKind::Socks(_) => "socks",
                InboundKind::Http(_) => "http",
                InboundKind::Mixed(_) => "mixed",
            }
        )));
    };
    let runtime: Arc<dyn Inbound> = Arc::new(TunInbound::new(options.clone(), platform));
    Ok(RuntimeComponent::new(
        ComponentMeta::new(
            "inbound",
            "tun",
            id,
            vec![Network::Tcp, Network::Udp],
            Vec::new(),
            None,
        ),
        runtime,
    ))
}

struct TunInbound {
    options: TunInboundOptions,
    platform: Option<Arc<dyn PlatformInterface>>,
    opened_fd: Mutex<Option<i32>>,
}

impl TunInbound {
    fn new(options: TunInboundOptions, platform: Option<Arc<dyn PlatformInterface>>) -> Self {
        Self {
            options,
            platform,
            opened_fd: Mutex::new(None),
        }
    }
}

impl Lifecycle for TunInbound {
    fn name(&self) -> &str {
        "tun"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if stage != StartStage::Start {
            return Ok(());
        }
        let Some(platform) = &self.platform else {
            return Ok(());
        };
        let mut opened_fd = self
            .opened_fd
            .lock()
            .map_err(|_| HammerError::internal("tun inbound state poisoned"))?;
        if opened_fd.is_some() {
            return Ok(());
        }
        let fd = platform.open_tun(TunOptions {
            name: self.options.interface_name.clone(),
            mtu: i32::try_from(self.options.mtu)
                .map_err(|_| HammerError::internal("tun mtu exceeds i32"))?,
            address: self
                .options
                .address
                .iter()
                .map(ToString::to_string)
                .collect(),
            route: self
                .options
                .route_address
                .iter()
                .map(ToString::to_string)
                .collect(),
            route_exclude: self
                .options
                .route_exclude_address
                .iter()
                .map(ToString::to_string)
                .collect(),
            auto_route: self.options.auto_route,
            strict_route: self.options.strict_route,
            tap: self.options.tap,
        })?;
        *opened_fd = Some(fd);
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        let mut opened_fd = self
            .opened_fd
            .lock()
            .map_err(|_| HammerError::internal("tun inbound state poisoned"))?;
        *opened_fd = None;
        Ok(())
    }
}

impl Inbound for TunInbound {}

pub struct InboundManager {
    items: Mutex<HashMap<String, InboundComponent>>,
    factories: InboundFactorySet,
}

pub struct InboundRegistration(InboundComponent);

impl From<InboundComponent> for InboundRegistration {
    fn from(inbound: InboundComponent) -> Self {
        Self(inbound)
    }
}

impl<T> From<Arc<T>> for InboundRegistration
where
    T: Inbound + ComponentMetadata + 'static,
{
    fn from(inbound: Arc<T>) -> Self {
        let meta = ComponentMetadata::component_meta(inbound.as_ref());
        let runtime: Arc<dyn Inbound> = inbound;
        Self(RuntimeComponent::new(meta, runtime))
    }
}

impl InboundManager {
    pub fn new(logger: Logger) -> Self {
        let _ = logger;
        Self {
            items: Mutex::new(HashMap::new()),
            factories: InboundFactorySet::standard(),
        }
    }

    pub fn from_options<R>(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<R>,
    ) -> HammerResult<Self>
    where
        R: RouterTrait + 'static,
    {
        let router: Arc<dyn RouterTrait> = router;
        let manager = Self::new(logger.clone());
        for option in options {
            manager.register(manager.factories.build(
                logger.clone(),
                option,
                Arc::clone(&router),
                None,
                None,
                None,
                MetricsRegistry::new(),
            )?);
        }
        Ok(manager)
    }

    pub fn from_options_with_runtime<R>(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<R>,
        dns_router: Arc<dyn DnsRouterTrait>,
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self>
    where
        R: RouterTrait + 'static,
    {
        Self::from_options_with_runtime_and_metrics(
            logger,
            options,
            router,
            dns_router,
            outbound,
            platform,
            MetricsRegistry::new(),
        )
    }

    pub fn from_options_with_runtime_and_metrics<R>(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<R>,
        dns_router: Arc<dyn DnsRouterTrait>,
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Self>
    where
        R: RouterTrait + 'static,
    {
        let platform = platform.into().into_inner();
        let dns_router = Arc::new(RuntimeDnsRouter::new(dns_router));
        let router: Arc<dyn RouterTrait> = router;
        let manager = Self::new(logger.clone());
        for option in options {
            manager.register(manager.factories.build(
                logger.clone(),
                option,
                Arc::clone(&router),
                Some(Arc::clone(&dns_router)),
                Some(Arc::clone(&outbound)),
                Some(Arc::clone(&platform)),
                Arc::clone(&metrics),
            )?);
        }
        Ok(manager)
    }

    pub fn register(&self, inbound: impl Into<InboundRegistration>) {
        let inbound = inbound.into().0;
        let id = inbound.meta().id().to_owned();
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .insert(id, inbound);
    }
}

impl Lifecycle for InboundManager {
    fn name(&self) -> &str {
        "inbound"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!("stage {}", stage.name());
        for inbound in self.list() {
            inbound.runtime().start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        debug!("close");
        let mut errors = Vec::new();
        for inbound in self.list() {
            if let Err(err) = inbound.runtime().close() {
                errors.push(format!("{}: {}", inbound.meta().id(), err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(HammerError::internal(format!(
                "inbound close errors: {}",
                errors.join("; ")
            )))
        }
    }
}

impl InboundManagerTrait for InboundManager {
    fn list(&self) -> Vec<InboundComponent> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<InboundComponent> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    use hammer_adapter::{
        DefaultInterfaceUpdateListener, NetworkInterface, RouteDecision, RouteMetadata, TunOptions,
        WifiState,
    };
    use hammer_core::config::{TunInboundOptions, TunStack};
    use hammer_core::log::{DiscardWriter, Factory};

    struct TestPlatform;

    impl PlatformInterface for TestPlatform {
        fn open_tun(&self, _options: TunOptions) -> HammerResult<i32> {
            Ok(-1)
        }

        fn use_platform_auto_detect_interface_control(&self) -> bool {
            false
        }

        fn auto_detect_interface_control(&self, _fd: i32) -> HammerResult<()> {
            Ok(())
        }

        fn start_default_interface_monitor(
            &self,
            _listener: Arc<dyn DefaultInterfaceUpdateListener>,
        ) -> HammerResult<()> {
            Ok(())
        }

        fn close_default_interface_monitor(
            &self,
            _listener: Arc<dyn DefaultInterfaceUpdateListener>,
        ) -> HammerResult<()> {
            Ok(())
        }

        fn get_interfaces(&self) -> HammerResult<Vec<NetworkInterface>> {
            Ok(Vec::new())
        }

        fn read_wifi_state(&self) -> Option<WifiState> {
            None
        }
    }

    struct NoopRouter;

    impl Lifecycle for NoopRouter {
        fn name(&self) -> &str {
            "noop-router"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl DnsRouterTrait for NoopRouter {
        async fn exchange(
            &self,
            _message: Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Message> {
            Err(HammerError::internal("noop dns router"))
        }

        async fn lookup(
            &self,
            _domain: &str,
            _options: DnsQueryOptions,
        ) -> HammerResult<Vec<IpAddr>> {
            Ok(Vec::new())
        }

        fn try_exchange_fast(
            &self,
            _message: &Message,
            _options: DnsQueryOptions,
        ) -> HammerResult<Option<Message>> {
            Ok(None)
        }

        fn clear_cache(&self) {}

        fn lookup_reverse_mapping(&self, _ip: IpAddr) -> Option<String> {
            None
        }

        fn reset_network(&self) {}
    }

    impl RouterTrait for NoopRouter {
        fn reset_network(&self) {}

        fn match_route(&self, _metadata: &mut RouteMetadata) -> HammerResult<RouteDecision> {
            Err(HammerError::internal("noop router"))
        }

        fn prepare_route_metadata(&self, _metadata: &mut RouteMetadata) -> HammerResult<()> {
            Ok(())
        }

        fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<std::time::Duration> {
            None
        }

        fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
            false
        }
    }

    fn test_logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    #[test]
    fn standard_factory_accepts_tun_inbounds() {
        let options = vec![hammer_core::config::Inbound {
            id: "tun".to_owned(),
            kind: InboundKind::Tun(TunInboundOptions {
                interface_name: "utun".to_owned(),
                mtu: 1400,
                address: Vec::new(),
                route_address: Vec::new(),
                route_exclude_address: Vec::new(),
                auto_route: false,
                strict_route: true,
                stack: TunStack::Disabled,
                tap: false,
                udp_timeout: None,
            }),
        }];
        let outbound = Arc::new(OutboundManager::new(test_logger("outbound"), "direct"));
        let router = Arc::new(NoopRouter);
        let dns_router: Arc<dyn DnsRouterTrait> = Arc::new(NoopRouter);

        let manager = InboundManager::from_options_with_runtime_and_metrics(
            test_logger("inbound"),
            &options,
            router,
            dns_router,
            outbound,
            Arc::new(TestPlatform),
            MetricsRegistry::new(),
        )
        .expect("tun inbound should register");

        let registered = manager.get("tun").expect("registered inbound");
        assert_eq!(registered.type_name(), "tun");
    }
}
