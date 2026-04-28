use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Outbound, OutboundManager as OutboundManagerTrait};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

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
