use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::debug;

use hammer_adapter::PlatformInterface;
use hammer_adapter::{
    EndpointComponent, EndpointManager as EndpointManagerTrait, Lifecycle, OutboundComponent,
};
#[cfg(feature = "endpoint")]
use hammer_core::config::Endpoint as EndpointOptions;
#[cfg(feature = "wireguard")]
use hammer_core::config::EndpointKind;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;

use crate::RuntimePlatform;
#[cfg(feature = "wireguard")]
use crate::component_registry::register_components;

#[cfg(feature = "wireguard")]
pub(crate) type EndpointViews = (EndpointComponent, OutboundComponent);

#[cfg(feature = "wireguard")]
pub(crate) type EndpointBuilder = fn(
    Logger,
    String,
    &EndpointKind,
    Option<Arc<dyn PlatformInterface>>,
) -> HammerResult<EndpointViews>;

#[cfg(feature = "wireguard")]
#[derive(Clone)]
struct EndpointFactorySet {
    builders: Arc<HashMap<&'static str, EndpointBuilder>>,
}

#[cfg(feature = "wireguard")]
impl EndpointFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_endpoint_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build(
        &self,
        logger: Logger,
        option: &EndpointOptions,
        platform: Option<Arc<dyn PlatformInterface>>,
    ) -> HammerResult<EndpointViews> {
        let type_name = option.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown endpoint type: {type_name}"))
        })?;
        builder(logger, option.id.clone(), &option.kind, platform)
    }
}

#[cfg(feature = "wireguard")]
fn register_standard_endpoint_builders(builders: &mut HashMap<&'static str, EndpointBuilder>) {
    register_components!(
        endpoint,
        builders,
        [crate::protocol::ipstack::wireguard::WireguardEndpoint]
    );
}

pub struct EndpointManager {
    #[cfg_attr(not(feature = "wireguard"), allow(dead_code))]
    logger: Logger,
    items: Mutex<HashMap<String, EndpointComponent>>,
    /// Same set of endpoints as `items`, but viewed through their `Outbound`
    /// trait — sing-box keeps a single object behind both a `Endpoint` and an
    /// `Outbound` registration so the router can resolve the id through
    /// `OutboundManager::get` exactly like any other outbound.
    outbound_view: Mutex<Vec<OutboundComponent>>,
    #[cfg(feature = "wireguard")]
    factories: EndpointFactorySet,
}

impl EndpointManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            outbound_view: Mutex::new(Vec::new()),
            #[cfg(feature = "wireguard")]
            factories: EndpointFactorySet::standard(),
        }
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options(logger: Logger, options: &[EndpointOptions]) -> HammerResult<Self> {
        Self::build(logger, options, None)
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options_with_platform(
        logger: Logger,
        options: &[EndpointOptions],
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self> {
        let platform = platform.into().into_inner();
        Self::build(logger, options, Some(platform))
    }

    #[cfg(feature = "endpoint")]
    fn build(
        logger: Logger,
        options: &[EndpointOptions],
        platform: Option<Arc<dyn PlatformInterface>>,
    ) -> HammerResult<Self> {
        let manager = Self::new(logger);
        #[cfg(not(feature = "wireguard"))]
        for _ in options {
            return Err(HammerError::config_validation(
                "no endpoint protocols are enabled",
            ));
        }
        #[cfg(feature = "wireguard")]
        for option in options {
            let (endpoint_view, outbound_view) = manager.factories.build(
                manager.logger.clone(),
                option,
                platform.as_ref().map(Arc::clone),
            )?;
            let mut items = manager.items.lock().expect("EndpointManager poisoned");
            if items.contains_key(&option.id) {
                return Err(HammerError::config_validation(format!(
                    "duplicate endpoint id: {}",
                    option.id
                )));
            }
            items.insert(option.id.clone(), endpoint_view);
            drop(items);
            manager
                .outbound_view
                .lock()
                .expect("EndpointManager poisoned")
                .push(outbound_view);
        }
        #[cfg(not(feature = "wireguard"))]
        let _ = platform;
        Ok(manager)
    }

    /// Snapshot of the (id, outbound-view) pairs so the assembler can register
    /// them with `OutboundManager` after both managers exist. Cloning the Arcs
    /// is cheap and the slice is only walked once at boot.
    pub fn outbound_view(&self) -> Vec<OutboundComponent> {
        self.outbound_view
            .lock()
            .expect("EndpointManager poisoned")
            .clone()
    }
}

impl Lifecycle for EndpointManager {
    fn name(&self) -> &str {
        "endpoint"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!("stage {}", stage.name());
        let items: Vec<EndpointComponent> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        for ep in items {
            ep.runtime().start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        debug!("close");
        let items: Vec<EndpointComponent> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        let mut errors = Vec::new();
        for ep in items {
            if let Err(err) = ep.runtime().close() {
                errors.push(format!("{}: {}", ep.meta().id(), err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(HammerError::internal(format!(
                "endpoint close errors: {}",
                errors.join("; ")
            )))
        }
    }
}

impl EndpointManagerTrait for EndpointManager {
    fn list(&self) -> Vec<EndpointComponent> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<EndpointComponent> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .remove(id);
        Ok(())
    }
}
