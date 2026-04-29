use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "wireguard")]
use hammer_adapter::PlatformInterface;
use hammer_adapter::{Endpoint, EndpointManager as EndpointManagerTrait};
#[cfg(feature = "wireguard")]
use hammer_core::config::{Endpoint as EndpointOptions, EndpointKind};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;
#[cfg(feature = "wireguard")]
use crate::wireguard::WireguardEndpoint;

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

    #[cfg(feature = "wireguard")]
    pub fn from_options(
        logger: Logger,
        options: &[EndpointOptions],
    ) -> Result<Self, HammerError> {
        Self::build(logger, options, None)
    }

    #[cfg(feature = "wireguard")]
    pub fn from_options_with_platform(
        logger: Logger,
        options: &[EndpointOptions],
        platform: Arc<dyn PlatformInterface>,
    ) -> Result<Self, HammerError> {
        Self::build(logger, options, Some(platform))
    }

    #[cfg(feature = "wireguard")]
    fn build(
        logger: Logger,
        options: &[EndpointOptions],
        // Held for symmetry with OutboundManager::from_options_with_platform.
        // The wg endpoint will route its outer UDP socket through this once
        // the boringtun transport lands; the placeholder ignores it.
        _platform: Option<Arc<dyn PlatformInterface>>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger);
        for option in options {
            let endpoint: Arc<dyn Endpoint> = match &option.kind {
                EndpointKind::Wireguard(opts) => Arc::new(WireguardEndpoint::new(
                    manager.logger.clone(),
                    option.tag.clone(),
                    opts.clone(),
                )),
            };
            let mut items = manager.items.lock().expect("EndpointManager poisoned");
            if items.contains_key(&option.tag) {
                return Err(HammerError::config_validation(format!(
                    "duplicate endpoint tag: {}",
                    option.tag
                )));
            }
            items.insert(option.tag.clone(), endpoint);
        }
        Ok(manager)
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
