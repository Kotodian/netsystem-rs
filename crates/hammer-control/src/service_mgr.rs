use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{LifecycleService, ServiceManager as ServiceManagerTrait};
use hammer_core::error::HammerResult;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

pub struct ServiceManager {
    items: Mutex<HashMap<String, Arc<dyn LifecycleService>>>,
}

impl ServiceManager {
    pub fn new(_logger: Logger) -> Self {
        Self {
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

    fn get(&self, id: &str) -> Option<Arc<dyn LifecycleService>> {
        self.items
            .lock()
            .expect("ServiceManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        self.items
            .lock()
            .expect("ServiceManager poisoned")
            .remove(id);
        Ok(())
    }
}
