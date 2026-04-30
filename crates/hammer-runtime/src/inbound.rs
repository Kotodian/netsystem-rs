use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use hammer_adapter::{Inbound, InboundManager as InboundManagerTrait, PlatformInterface};
use hammer_core::config::{Inbound as InboundOptions, InboundKind};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_core::log::Logger;

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
                        option.id.clone(),
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
        platform: Arc<dyn PlatformInterface>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger.clone());
        for option in options {
            match &option.kind {
                InboundKind::Tun(tun) => {
                    manager.register(Arc::new(TunInbound::new_with_runtime(
                        option.id.clone(),
                        logger.clone(),
                        tun.clone(),
                        Arc::clone(&router),
                        Arc::clone(&dns_router),
                        Arc::clone(&outbound),
                        Arc::clone(&platform),
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
            .insert(inbound.id().to_owned(), inbound);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn Inbound>> {
        <Self as InboundManagerTrait>::get(self, id)
    }
}

impl Lifecycle for InboundManager {
    fn name(&self) -> &str {
        "inbound"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        for inbound in self.list() {
            inbound.start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        let mut errors = Vec::new();
        for inbound in self.list() {
            if let Err(err) = inbound.close() {
                errors.push(format!("{}: {}", inbound.id(), err));
            }
        }
        self.logger.debug("close");
        if errors.is_empty() {
            Ok(())
        } else {
            Err(HammerError::internal(errors.join("; ")))
        }
    }
}

impl InboundManagerTrait for InboundManager {
    fn list(&self) -> Vec<Arc<dyn Inbound>> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<Arc<dyn Inbound>> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("InboundManager poisoned")
            .remove(id);
        Ok(())
    }
}
