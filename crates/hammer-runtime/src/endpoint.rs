use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Endpoint, EndpointManager as EndpointManagerTrait};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

pub struct EndpointManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn Endpoint>>>,
}

impl EndpointManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl_logging_lifecycle!(EndpointManager, "endpoint");

impl EndpointManagerTrait for EndpointManager {
    fn list(&self) -> Vec<Arc<dyn Endpoint>> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, tag: &str) -> Option<Arc<dyn Endpoint>> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .get(tag)
            .cloned()
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .remove(tag);
        Ok(())
    }
}
