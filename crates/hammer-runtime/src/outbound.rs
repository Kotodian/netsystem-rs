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
    default_tag: String,
}

impl OutboundManager {
    pub fn new(logger: Logger, default_tag: impl Into<String>) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            default_tag: default_tag.into(),
        }
    }

    pub fn from_options(
        logger: Logger,
        default_tag: impl Into<String>,
        options: &[OutboundOptions],
    ) -> Self {
        Self::from_options_with_protector(logger, default_tag, options, SocketProtector::default())
    }

    pub fn from_options_with_platform(
        logger: Logger,
        default_tag: impl Into<String>,
        options: &[OutboundOptions],
        platform: Arc<dyn PlatformInterface>,
    ) -> Self {
        Self::from_options_with_protector(
            logger,
            default_tag,
            options,
            SocketProtector::new(platform),
        )
    }

    pub(crate) fn from_options_with_protector(
        logger: Logger,
        default_tag: impl Into<String>,
        options: &[OutboundOptions],
        protector: SocketProtector,
    ) -> Self {
        let manager = Self::new(logger, default_tag);
        for option in options {
            manager.register_descriptor_with_protector(option, protector.clone());
        }
        manager
    }

    pub fn register_descriptor(&self, option: &OutboundOptions) {
        self.register_descriptor_with_protector(option, SocketProtector::default());
    }

    /// Register an already-constructed outbound (e.g. an endpoint that lives
    /// in `EndpointManager`) so the router can resolve its tag through the
    /// usual `OutboundManager::get` path. Mirrors sing-box, where every
    /// endpoint shows up as both an Endpoint *and* an Outbound — same Arc,
    /// two views.
    pub fn register_outbound(&self, tag: String, descriptor: Arc<dyn Outbound>) {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .insert(tag, descriptor);
    }

    fn register_descriptor_with_protector(
        &self,
        option: &OutboundOptions,
        protector: SocketProtector,
    ) {
        let descriptor: Arc<dyn Outbound> = match &option.kind {
            OutboundKind::Hysteria2(o) => Arc::new(Hysteria2Outbound::new_with_protector(
                self.logger.clone(),
                option.tag.clone(),
                o.clone(),
                protector.clone(),
            )),
            OutboundKind::Direct(_) => Arc::new(DirectOutbound::new_with_protector(
                self.logger.clone(),
                option.tag.clone(),
                protector,
            )),
            OutboundKind::Block => {
                Arc::new(BlockOutbound::new(self.logger.clone(), option.tag.clone()))
            }
            OutboundKind::Dns => Arc::new(DnsOutbound::new(option.tag.clone())),
        };
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .insert(option.tag.clone(), descriptor);
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

    fn get(&self, tag: &str) -> Option<Arc<dyn Outbound>> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .get(tag)
            .cloned()
    }

    fn default(&self) -> Option<Arc<dyn Outbound>> {
        if self.default_tag.is_empty() {
            return None;
        }
        self.get(&self.default_tag)
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("OutboundManager poisoned")
            .remove(tag);
        Ok(())
    }
}
