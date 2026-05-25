use std::collections::HashMap;
#[cfg(feature = "endpoint")]
use std::sync::Arc;
use std::sync::Mutex;
use tracing::debug;

#[cfg(feature = "endpoint")]
mod outbound_adapter;
#[cfg(feature = "endpoint")]
pub use outbound_adapter::EndpointOutboundAdapter;

#[cfg(feature = "endpoint")]
use hammer_adapter::PlatformInterface;
use hammer_adapter::{EndpointComponent, EndpointManager as EndpointManagerTrait, Lifecycle};
#[cfg(feature = "endpoint")]
use hammer_core::config::Endpoint as EndpointOptions;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;

#[cfg(feature = "endpoint")]
use crate::ControlThreadHandle;
#[cfg(feature = "endpoint")]
use crate::RuntimePlatform;
#[cfg(feature = "endpoint-wireguard")]
use crate::component_registry::register_components;

#[cfg(feature = "endpoint-wireguard")]
pub(crate) type EndpointBuilder = fn(
    Logger,
    &EndpointOptions,
    Option<Arc<dyn PlatformInterface>>,
    Option<Arc<ControlThreadHandle>>,
) -> HammerResult<EndpointComponent>;

#[cfg(feature = "endpoint-wireguard")]
#[derive(Clone)]
struct EndpointFactorySet {
    builders: Arc<HashMap<&'static str, EndpointBuilder>>,
}

#[cfg(feature = "endpoint-wireguard")]
impl EndpointFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_endpoint_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build(
        &self,
        logger: Logger,
        option: &EndpointOptions,
        platform: Option<Arc<dyn PlatformInterface>>,
        control_handle: Option<Arc<ControlThreadHandle>>,
    ) -> HammerResult<EndpointComponent> {
        let type_name = option.type_name();
        let builder = self.builders.get(type_name).ok_or_else(|| {
            HammerError::config_validation(format!("unknown endpoint type: {type_name}"))
        })?;
        builder(logger, option, platform, control_handle)
    }
}

#[cfg(feature = "endpoint-wireguard")]
fn register_standard_endpoint_builders(builders: &mut HashMap<&'static str, EndpointBuilder>) {
    register_components!(
        endpoint,
        builders,
        [crate::protocol::endpoint::wireguard::WireguardEndpoint]
    );
}

pub struct EndpointManager {
    #[cfg_attr(not(feature = "endpoint-wireguard"), allow(dead_code))]
    logger: Logger,
    items: Mutex<HashMap<String, EndpointComponent>>,
    #[cfg(feature = "endpoint-wireguard")]
    factories: EndpointFactorySet,
}

impl EndpointManager {
    pub fn new(logger: Logger) -> Self {
        Self {
            logger,
            items: Mutex::new(HashMap::new()),
            #[cfg(feature = "endpoint-wireguard")]
            factories: EndpointFactorySet::standard(),
        }
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options(logger: Logger, options: &[EndpointOptions]) -> HammerResult<Self> {
        Self::build(logger, options, None, None)
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options_with_platform(
        logger: Logger,
        options: &[EndpointOptions],
        platform: impl Into<RuntimePlatform>,
    ) -> HammerResult<Self> {
        let platform = platform.into().into_inner();
        Self::build(logger, options, Some(platform), None)
    }

    #[cfg(feature = "endpoint")]
    pub fn from_options_with_platform_and_control(
        logger: Logger,
        options: &[EndpointOptions],
        platform: impl Into<RuntimePlatform>,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Self> {
        let platform = platform.into().into_inner();
        Self::build(logger, options, Some(platform), Some(control_handle))
    }

    #[cfg(feature = "endpoint")]
    fn build(
        logger: Logger,
        options: &[EndpointOptions],
        platform: Option<Arc<dyn PlatformInterface>>,
        #[cfg_attr(not(feature = "endpoint-wireguard"), allow(unused_variables))]
        control_handle: Option<Arc<ControlThreadHandle>>,
    ) -> HammerResult<Self> {
        let manager = Self::new(logger);
        #[cfg(not(feature = "endpoint-wireguard"))]
        for _ in options {
            return Err(HammerError::config_validation(
                "no endpoint protocols are enabled",
            ));
        }
        #[cfg(feature = "endpoint-wireguard")]
        for option in options {
            let endpoint = manager.factories.build(
                manager.logger.clone(),
                option,
                platform.as_ref().map(Arc::clone),
                control_handle.as_ref().map(Arc::clone),
            )?;
            let mut items = manager.items.lock().expect("EndpointManager poisoned");
            if items.contains_key(&option.id) {
                return Err(HammerError::config_validation(format!(
                    "duplicate endpoint id: {}",
                    option.id
                )));
            }
            items.insert(option.id.clone(), endpoint);
        }
        #[cfg(not(feature = "endpoint-wireguard"))]
        let _ = platform;
        Ok(manager)
    }

    pub fn reset_network(&self) {
        for endpoint in self.list() {
            endpoint.runtime().reset();
        }
    }
}

impl Lifecycle for EndpointManager {
    fn name(&self) -> &str {
        "endpoint"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        debug!("stage {}", stage.name());
        let items: Vec<EndpointComponent> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        for ep in items {
            ep.runtime().start(stage)?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        debug!("close");
        let items: Vec<EndpointComponent> = self
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect();
        let mut errors = Vec::new();
        for ep in items {
            if let Err(err) = ep.runtime().close() {
                errors.push(format!("{}: {}", ep.meta().id(), err));
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
    fn list(&self) -> Vec<EndpointComponent> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .values()
            .cloned()
            .collect()
    }

    fn get(&self, id: &str) -> Option<EndpointComponent> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .get(id)
            .cloned()
    }

    fn remove(&self, id: &str) -> HammerResult<()> {
        self.items
            .lock()
            .expect("EndpointManager poisoned")
            .remove(id);
        Ok(())
    }
}

#[cfg(all(test, feature = "endpoint"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant as StdInstant;

    use bytes::Bytes;
    use hammer_adapter::{ComponentMeta, Endpoint, RuntimeComponent};
    use hammer_core::lifecycle::StartStage;
    use hammer_core::log::{DiscardWriter, Factory};
    use tokio::sync::mpsc;

    use super::*;

    struct ResetEndpoint {
        sender: mpsc::Sender<Bytes>,
        resets: Arc<AtomicUsize>,
    }

    impl Lifecycle for ResetEndpoint {
        fn name(&self) -> &str {
            "reset-endpoint"
        }

        fn start(&self, _stage: StartStage) -> HammerResult<()> {
            Ok(())
        }

        fn close(&self) -> HammerResult<()> {
            Ok(())
        }
    }

    impl Endpoint for ResetEndpoint {
        fn id(&self) -> &str {
            "wg-out"
        }

        fn ip_send_clone(&self) -> mpsc::Sender<Bytes> {
            self.sender.clone()
        }

        fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
            None
        }

        fn reset(&self) {
            self.resets.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn test_logger() -> Logger {
        Factory::new(StdInstant::now(), Arc::new(DiscardWriter)).new_logger("endpoint-test")
    }

    #[test]
    fn reset_network_resets_registered_endpoints() {
        let manager = EndpointManager::new(test_logger());
        let (sender, _receiver) = mpsc::channel(1);
        let resets = Arc::new(AtomicUsize::new(0));
        let runtime: Arc<dyn Endpoint> = Arc::new(ResetEndpoint {
            sender,
            resets: Arc::clone(&resets),
        });
        manager
            .items
            .lock()
            .expect("EndpointManager poisoned")
            .insert(
                "wg-out".to_owned(),
                RuntimeComponent::new(
                    ComponentMeta::new("endpoint", "test", "wg-out", Vec::new(), Vec::new(), None),
                    runtime,
                ),
            );

        manager.reset_network();

        assert_eq!(resets.load(Ordering::Relaxed), 1);
    }
}
