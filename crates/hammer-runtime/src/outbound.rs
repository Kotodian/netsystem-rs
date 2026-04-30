use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Outbound, OutboundManager as OutboundManagerTrait, PlatformInterface};
use hammer_core::config::{Outbound as OutboundOptions, OutboundKind};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::hysteria2::Hysteria2Outbound;
use crate::impl_logging_lifecycle;
use crate::outbounds::{BlockOutbound, DirectOutbound, DnsOutbound};
use crate::socket_protector::SocketProtector;

/// `out.Manager` port. M2 ships an empty registry; concrete outbounds (direct,
/// hysteria2, block, dns) register themselves in M6/M7.
pub struct OutboundManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn Outbound>>>,
    default_id: String,
}

impl OutboundManager {
    pub fn new(logger: Logger, default_id: impl Into<String>) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            default_id: default_id.into(),
        }
    }

    pub fn from_options(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
    ) -> Self {
        Self::from_options_with_protector(logger, default_id, options, SocketProtector::default())
    }

    pub fn from_options_with_platform(
        logger: Logger,
        default_id: impl Into<String>,
        options: &[OutboundOptions],
        platform: Arc<dyn PlatformInterface>,
    ) -> Self {
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
    ) -> Self {
        let manager = Self::new(logger, default_id);
        for option in options {
            manager.register_descriptor_with_protector(option, protector.clone());
        }
        manager
    }

    pub fn register_descriptor(&self, option: &OutboundOptions) {
        self.register_descriptor_with_protector(option, SocketProtector::default());
    }

    /// Register an already-constructed outbound (e.g. an endpoint that lives
    /// in `EndpointManager`) so the router can resolve its id through the
    /// usual `OutboundManager::get` path. Mirrors sing-box, where every
    /// endpoint shows up as both an Endpoint *and* an Outbound — same Arc,
    /// two views.
    pub fn register_outbound(&self, id: String, descriptor: Arc<dyn Outbound>) {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .insert(id, descriptor);
    }

    fn register_descriptor_with_protector(
        &self,
        option: &OutboundOptions,
        protector: SocketProtector,
    ) {
        let descriptor: Arc<dyn Outbound> = match &option.kind {
            OutboundKind::Hysteria2(o) => Arc::new(Hysteria2Outbound::new_with_protector(
                self.logger.clone(),
                option.id.clone(),
                o.clone(),
                protector.clone(),
            )),
            OutboundKind::Direct(_) => Arc::new(DirectOutbound::new_with_protector(
                self.logger.clone(),
                option.id.clone(),
                protector,
            )),
            OutboundKind::Block => {
                Arc::new(BlockOutbound::new(self.logger.clone(), option.id.clone()))
            }
            OutboundKind::Dns => Arc::new(DnsOutbound::new(option.id.clone())),
        };
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .insert(option.id.clone(), descriptor);
    }
}

impl_logging_lifecycle!(OutboundManager, "outbound");

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
