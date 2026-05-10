use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{
    ComponentMetadata, Inbound, InboundComponent, InboundManager as InboundManagerTrait, Lifecycle,
    PlatformInterface, RuntimeComponent,
};
use hammer_core::config::{Inbound as InboundOptions, InboundKind};
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::metrics::MetricsRegistry;
use tracing::debug;

#[cfg(feature = "inbound-tun")]
use crate::component_registry::register_components;
#[cfg(feature = "inbound-tun")]
use crate::protocol::tun;
use crate::{DnsRouter, OutboundManager, Router, RuntimePlatform};

pub(crate) type InboundBuilder = fn(
    String,
    Logger,
    &InboundKind,
    Arc<Router>,
    Option<Arc<DnsRouter>>,
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
        router: Arc<Router>,
        dns_router: Option<Arc<DnsRouter>>,
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

#[allow(unused_variables)]
fn register_standard_inbound_builders(builders: &mut HashMap<&'static str, InboundBuilder>) {
    #[cfg(feature = "inbound-tun")]
    register_components!(inbound, builders, [tun::RuntimeTunInbound]);
}

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

    pub fn from_options(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<Router>,
    ) -> HammerResult<Self> {
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

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self> {
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

    pub fn from_options_with_runtime_and_metrics(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
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
