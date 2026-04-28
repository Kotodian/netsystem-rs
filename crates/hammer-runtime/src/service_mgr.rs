use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{LifecycleService, ServiceManager as ServiceManagerTrait};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

pub struct ServiceManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn LifecycleService>>>,
}

impl ServiceManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl_logging_lifecycle!(ServiceManager, "service");

impl ServiceManagerTrait for ServiceManager {
    fn list(&self) -> Vec<Arc<dyn LifecycleService>> {
        self.items
            .lock()
            .expect("ServiceManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, tag: &str) -> Option<Arc<dyn LifecycleService>> {
        self.items
            .lock()
            .expect("ServiceManager poisoned")
            .get(tag)
            .cloned()
    }

    fn remove(&self, tag: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("ServiceManager poisoned")
            .remove(tag);
        Ok(())
    }
}
