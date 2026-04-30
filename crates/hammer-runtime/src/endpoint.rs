use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "wireguard")]
use hammer_adapter::PlatformInterface;
use hammer_adapter::{Endpoint, EndpointManager as EndpointManagerTrait, Lifecycle, Outbound};
#[cfg(feature = "wireguard")]
use hammer_core::config::{Endpoint as EndpointOptions, EndpointKind};
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;

pub struct EndpointManager {
    logger: Logger,
    items: Mutex<HashMap<String, Arc<dyn Endpoint>>>,
    /// Same set of endpoints as `items`, but viewed through their `Outbound`
    /// trait — sing-box keeps a single object behind both a `Endpoint` and an
    /// `Outbound` registration so the router can resolve the id through
    /// `OutboundManager::get` exactly like any other outbound.
    outbound_view: Mutex<Vec<(String, Arc<dyn Outbound>)>>,
}

impl EndpointManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            outbound_view: Mutex::new(Vec::new()),
        }
    }

    #[cfg(feature = "wireguard")]
    pub fn from_options(logger: Logger, options: &[EndpointOptions]) -> Result<Self, HammerError> {
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
        platform: Option<Arc<dyn PlatformInterface>>,
    ) -> Result<Self, HammerError> {
        let manager = Self::new(logger);
        for option in options {
            let (endpoint_view, outbound_view) = match &option.kind {
                EndpointKind::Wireguard(opts) => {
                    // One concrete Arc, two trait-object views. The unsizing
                    // coercion `Arc<WireguardEndpoint> → Arc<dyn Trait>` is
                    // standard Rust — no need for a manual upcast helper.
                    let arc = Arc::new(crate::wireguard::build_with_platform(
                        manager.logger.clone(),
                        option.id.clone(),
                        opts.clone(),
                        platform.clone(),
                    ));
                    let ep: Arc<dyn Endpoint> = arc.clone();
                    let ob: Arc<dyn Outbound> = arc;
                    (ep, ob)
                }
            };
            let mut items = manager.items.lock().expect("EndpointManager poisoned");
            if items.contains_key(&option.id) {
                return Err(HammerError::config_validation(format!(
                    "duplicate endpoint id: {}",
                    option.id
                )));
            }
            items.insert(option.id.clone(), endpoint_view);
            drop(items);
            manager
                .outbound_view
                .lock()
                .expect("EndpointManager poisoned")
                .push((option.id.clone(), outbound_view));
        }
        Ok(manager)
    }

    /// Snapshot of the (id, outbound-view) pairs so the assembler can register
    /// them with `OutboundManager` after both managers exist. Cloning the Arcs
    /// is cheap and the slice is only walked once at boot.
    pub fn outbound_view(&self) -> Vec<(String, Arc<dyn Outbound>)> {
        self.outbound_view
            .lock()
            .expect("EndpointManager poisoned")
            .clone()
    }
}

impl Lifecycle for EndpointManager {
    fn name(&self) -> &str {
        "endpoint"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        self.logger.debug(format!("stage {}", stage.name()));
        // Forward the stage to every concrete endpoint so that protocols
        // wanting to spin up actor tasks (e.g. wg's transport + smoltcp stack)
        // can latch onto the right phase. We snapshot the Arc list to avoid
        // holding the lock across the recursive `start` calls.
        let items: Vec<Arc<dyn Endpoint>> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        for ep in items {
            ep.start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.logger.debug("close");
        let items: Vec<Arc<dyn Endpoint>> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        let mut errors = Vec::new();
        for ep in items {
            if let Err(err) = ep.close() {
                errors.push(format!("{}: {}", ep.id(), err));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(HammerError::internal(format!(
                "endpoint close errors: {}",
                errors.join("; ")
            )))
        }
    }
}

impl EndpointManagerTrait for EndpointManager {
    fn list(&self) -> Vec<Arc<dyn Endpoint>> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<Arc<dyn Endpoint>> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> Result<(), HammerError> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .remove(id);
        Ok(())
    }
}
