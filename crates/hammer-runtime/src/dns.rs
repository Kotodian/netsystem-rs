use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{
    DnsRouter as DnsRouterTrait, DnsTransport,
    DnsTransportManager as DnsTransportManagerTrait,
};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

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
}

impl_logging_lifecycle!(DnsTransportManager, "dns-transport");

impl DnsTransportManagerTrait for DnsTransportManager {
    fn list(&self) -> Vec<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, tag: &str) -> Option<Arc<dyn DnsTransport>> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .get(tag)
            .cloned()
    }

    fn default(&self) -> Option<Arc<dyn DnsTransport>> {
        if self.default_tag.is_empty() {
            return None;
        }
        self.get(&self.default_tag)
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("DnsTransportManager poisoned")
            .remove(tag);
        Ok(())
    }
}

/// `dns.Router` placeholder. Real query routing arrives in M3.
pub struct DnsRouter {
    logger: Logger,
}

impl DnsRouter {
    pub fn new(logger: Logger) -> Self {
        Self { logger }
    }
}

impl_logging_lifecycle!(DnsRouter, "dns-router");

impl DnsRouterTrait for DnsRouter {
    fn clear_cache(&self) {
        self.logger.debug("clear_cache (M2 stub)");
    }

    fn reset_network(&self) {
        self.logger.debug("reset_network (M2 stub)");
    }
}
