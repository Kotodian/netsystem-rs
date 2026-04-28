use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Inbound, InboundManager as InboundManagerTrait};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

pub struct InboundManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn Inbound>>>,
}

impl InboundManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl_logging_lifecycle!(InboundManager, "inbound");

impl InboundManagerTrait for InboundManager {
    fn list(&self) -> Vec<Arc<dyn Inbound>> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, tag: &str) -> Option<Arc<dyn Inbound>> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .get(tag)
            .cloned()
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .remove(tag);
        Ok(())
    }
}
