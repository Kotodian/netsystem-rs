use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use hammer_adapter::{
    Lifecycle, Outbound, OutboundManager as OutboundManagerTrait, PlatformInterface,
};
use hammer_core::config::{Outbound as OutboundOptions, OutboundKind};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;

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
}

impl OutboundManager {
    pub fn new(logger: Logger, default_id: impl Into<String>) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            default_id: default_id.into(),
            factories: OutboundFactorySet::standard(),
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

    pub(crate) fn from_options_with_protector(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        protector: SocketProtector,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger, default_id);
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
        items.insert(id, descriptor);
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
        items.insert(option.id.clone(), descriptor);
        Ok(())
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
