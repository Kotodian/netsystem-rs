use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{
    ComponentMeta, ComponentMetadata, Inbound, InboundComponent,
    InboundManager as InboundManagerTrait, Lifecycle, Network, PlatformInterface, RuntimeComponent,
    TunOptions,
};
use hammer_core::config::{Inbound as InboundOptions, InboundKind, TunInboundOptions};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;
use tracing::debug;

use crate::{OutboundManager, RuntimePlatform};

pub(crate) type InboundBuilder = fn(
    String,
    Logger,
    &InboundKind,
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
            outbound,
            platform,
            metrics,
        )
    }
}

fn register_standard_inbound_builders(builders: &mut HashMap<&'static str, InboundBuilder>) {
    builders.insert("tun", build_tun_inbound);
}

#[allow(clippy::too_many_arguments)]
fn build_tun_inbound(
    id: String,
    _logger: Logger,
    kind: &InboundKind,
    _outbound: Option<Arc<OutboundManager>>,
    platform: Option<Arc<dyn PlatformInterface>>,
    _metrics: Arc<MetricsRegistry>,
) -> HammerResult<InboundComponent> {
    let InboundKind::Tun(options) = kind;
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

    pub fn from_options(logger: Logger, options: &[InboundOptions]) -> HammerResult<Self> {
        let manager = Self::new(logger.clone());
        for option in options {
            manager.register(manager.factories.build(
                logger.clone(),
                option,
                None,
                None,
                MetricsRegistry::new(),
            )?);
        }
        Ok(manager)
    }

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &[InboundOptions],
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self> {
        Self::from_options_with_runtime_and_metrics(
            logger,
            options,
            outbound,
            platform,
            MetricsRegistry::new(),
        )
    }

    pub fn from_options_with_runtime_and_metrics(
        logger: Logger,
        options: &[InboundOptions],
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
        metrics: Arc<MetricsRegistry>,
    ) -> HammerResult<Self> {
        let platform = platform.into().into_inner();
        let manager = Self::new(logger.clone());
        for option in options {
            manager.register(manager.factories.build(
                logger.clone(),
                option,
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

    use hammer_adapter::{DefaultInterfaceUpdateListener, NetworkInterface, TunOptions, WifiState};
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
        let outbound = Arc::new(OutboundManager::new(test_logger("outbound"), "block"));

        let manager = InboundManager::from_options_with_runtime_and_metrics(
            test_logger("inbound"),
            &options,
            outbound,
            Arc::new(TestPlatform),
            MetricsRegistry::new(),
        )
        .expect("tun inbound should register");

        let registered = manager.get("tun").expect("registered inbound");
        assert_eq!(registered.type_name(), "tun");
    }
}
