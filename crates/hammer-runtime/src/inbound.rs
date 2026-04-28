use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Inbound, InboundManager as InboundManagerTrait};
use hammer_core::config::{Inbound as InboundOptions, InboundKind};
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;
use crate::{DnsRouter, OutboundManager, Router, TunInbound};

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

    pub fn from_options(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<Router>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger.clone());
        for option in options {
            match &option.kind {
                InboundKind::Tun(tun) => {
                    manager.register(Arc::new(TunInbound::new(
                        option.tag.clone(),
                        logger.clone(),
                        tun.clone(),
                        Arc::clone(&router),
                    )));
                }
            }
        }
        Ok(manager)
    }

    pub fn from_options_with_runtime(
        logger: Logger,
        options: &[InboundOptions],
        router: Arc<Router>,
        dns_router: Arc<DnsRouter>,
        outbound: Arc<OutboundManager>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger.clone());
        for option in options {
            match &option.kind {
                InboundKind::Tun(tun) => {
                    manager.register(Arc::new(TunInbound::new_with_runtime(
                        option.tag.clone(),
                        logger.clone(),
                        tun.clone(),
                        Arc::clone(&router),
                        Arc::clone(&dns_router),
                        Arc::clone(&outbound),
                    )));
                }
            }
        }
        Ok(manager)
    }

    pub fn register(&self, inbound: Arc<dyn Inbound>) {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .insert(inbound.tag().to_owned(), inbound);
    }

    pub fn get(&self, tag: &str) -> Option<Arc<dyn Inbound>> {
        <Self as InboundManagerTrait>::get(self, tag)
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
