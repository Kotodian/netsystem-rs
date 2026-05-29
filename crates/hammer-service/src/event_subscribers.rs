use std::collections::HashMap;
use std::sync::Arc;

use hammer_core::error::HammerResult;
use hammer_core::log::Logger;
use hammer_runtime::{ControlEventSubscriptionHandle, ControlThreadHandle, EventSubscriberBuilder};

#[derive(Clone)]
struct EventSubscriberFactorySet {
    builders: Arc<HashMap<&'static str, EventSubscriberBuilder>>,
}

impl EventSubscriberFactorySet {
    fn standard() -> Self {
        let mut builders = HashMap::new();
        register_standard_event_subscriber_builders(&mut builders);
        Self {
            builders: Arc::new(builders),
        }
    }

    fn build_all(
        &self,
        logger: Logger,
        control_handle: Arc<ControlThreadHandle>,
    ) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
        let mut subscriptions = Vec::new();
        for builder in self.builders.values() {
            subscriptions.extend(builder(logger.clone(), Arc::clone(&control_handle))?);
        }
        Ok(subscriptions)
    }
}

fn register_standard_event_subscriber_builders(
    _builders: &mut HashMap<&'static str, EventSubscriberBuilder>,
) {
    #[cfg(feature = "outbound-hysteria2")]
    hammer_runtime::register_event_subscriber_component::<
        hammer_runtime::hysteria2::Hysteria2AuthLogSubscriber,
    >(_builders);
    #[cfg(feature = "endpoint-wireguard")]
    {
        hammer_runtime::register_event_subscriber_component::<
            hammer_runtime::wireguard::WireguardStartHandshakeSubscriber,
        >(_builders);
        hammer_runtime::register_event_subscriber_component::<
            hammer_runtime::wireguard::WireguardInboundControlSubscriber,
        >(_builders);
    }
}

pub(crate) fn build_standard_event_subscribers(
    logger: Logger,
    control_handle: Arc<ControlThreadHandle>,
) -> HammerResult<Vec<ControlEventSubscriptionHandle>> {
    EventSubscriberFactorySet::standard().build_all(logger, control_handle)
}
