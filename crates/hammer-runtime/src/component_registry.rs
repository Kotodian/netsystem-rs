use std::collections::HashMap;

#[cfg(any(
    feature = "outbound-hysteria2",
    feature = "outbound-direct",
    feature = "outbound-block",
    feature = "outbound-urltest"
))]
pub(crate) trait OutboundComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        logger: hammer_core::log::Logger,
        id: String,
        kind: &hammer_core::config::OutboundKind,
        protector: crate::socket_protector::SocketProtector,
    ) -> hammer_core::error::HammerResult<hammer_adapter::outbound::OutboundComponent>;
}

#[cfg(any(
    feature = "outbound-hysteria2",
    feature = "outbound-direct",
    feature = "outbound-block",
    feature = "outbound-urltest"
))]
pub(crate) fn register_outbound_component<C>(
    builders: &mut HashMap<&'static str, crate::outbounds::OutboundBuilder>,
) where
    C: OutboundComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

#[cfg(feature = "inbound-tun")]
pub(crate) trait InboundComponentDeclaration {
    const TYPE_NAME: &'static str;

    #[allow(clippy::too_many_arguments)]
    fn build(
        id: String,
        logger: hammer_core::log::Logger,
        kind: &hammer_core::config::InboundKind,
        router: std::sync::Arc<crate::Router>,
        dns_router: Option<std::sync::Arc<crate::DnsRouter>>,
        outbound: Option<std::sync::Arc<crate::OutboundManager>>,
        platform: Option<std::sync::Arc<dyn hammer_adapter::PlatformInterface>>,
        metrics: std::sync::Arc<hammer_core::metrics::MetricsRegistry>,
    ) -> hammer_core::error::HammerResult<hammer_adapter::inbound::InboundComponent>;
}

#[cfg(feature = "inbound-tun")]
pub(crate) fn register_inbound_component<C>(
    builders: &mut HashMap<&'static str, crate::inbounds::InboundBuilder>,
) where
    C: InboundComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

#[cfg(feature = "endpoint-wireguard")]
pub(crate) trait EndpointComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        logger: hammer_core::log::Logger,
        option: &hammer_core::config::Endpoint,
        platform: Option<std::sync::Arc<dyn hammer_adapter::PlatformInterface>>,
    ) -> hammer_core::error::HammerResult<crate::endpoints::EndpointViews>;
}

#[cfg(feature = "endpoint-wireguard")]
pub(crate) fn register_endpoint_component<C>(
    builders: &mut HashMap<&'static str, crate::endpoints::EndpointBuilder>,
) where
    C: EndpointComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

#[cfg(any(
    feature = "dns-udp",
    feature = "dns-tcp",
    feature = "dns-https",
    feature = "dns-hosts",
    feature = "dns-local"
))]
pub(crate) trait DnsTransportComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        id: String,
        kind: &hammer_core::config::DnsServerKind,
        logger: hammer_core::log::Logger,
        outbound: Option<std::sync::Arc<crate::OutboundManager>>,
        bootstrap: Option<hammer_adapter::dns::DnsTransportComponent>,
        protector: crate::socket_protector::SocketProtector,
    ) -> hammer_core::error::HammerResult<hammer_adapter::dns::DnsTransportComponent>;
}

#[cfg(any(
    feature = "dns-udp",
    feature = "dns-tcp",
    feature = "dns-https",
    feature = "dns-hosts",
    feature = "dns-local"
))]
pub(crate) fn register_dns_transport_component<C>(
    builders: &mut HashMap<&'static str, crate::dns::DnsTransportBuilder>,
) where
    C: DnsTransportComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

pub(crate) trait RouterComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        logger: hammer_core::log::Logger,
        options: hammer_core::config::RouteOptions,
        outbound: std::sync::Arc<crate::OutboundManager>,
        metrics: std::sync::Arc<hammer_core::metrics::MetricsRegistry>,
    ) -> hammer_core::error::HammerResult<crate::Router>;
}

pub(crate) fn register_router_component<C>(
    builders: &mut HashMap<&'static str, crate::route::RouterBuilder>,
) where
    C: RouterComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

pub(crate) trait RouteMatcherComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build(
        matcher: hammer_core::config::RuleMatcher,
    ) -> hammer_core::error::HammerResult<crate::route::RuntimeMatcher>;
}

pub(crate) fn register_route_matcher_component<C>(
    builders: &mut HashMap<&'static str, crate::route::MatcherBuilder>,
) where
    C: RouteMatcherComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

#[cfg(feature = "probe")]
pub(crate) trait ProbeComponentDeclaration {
    const TYPE_NAME: &'static str;

    fn build() -> hammer_adapter::probe::ProbeProtocolComponent;
}

#[cfg(feature = "probe")]
pub(crate) fn register_probe_component<C>(
    builders: &mut HashMap<&'static str, crate::probe::ProbeProtocolBuilder>,
) where
    C: ProbeComponentDeclaration,
{
    builders.insert(C::TYPE_NAME, C::build);
}

macro_rules! register_components {
    (outbound, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_outbound_component::<$component>($builders);)*
    };
    (inbound, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_inbound_component::<$component>($builders);)*
    };
    (endpoint, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_endpoint_component::<$component>($builders);)*
    };
    (dns_transport, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_dns_transport_component::<$component>($builders);)*
    };
    (router, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_router_component::<$component>($builders);)*
    };
    (matcher, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_route_matcher_component::<$component>($builders);)*
    };
    (probe, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_probe_component::<$component>($builders);)*
    };
}

pub(crate) use register_components;
