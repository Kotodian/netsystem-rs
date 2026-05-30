use std::collections::HashMap;

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

macro_rules! register_components {
    (dns_transport, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_dns_transport_component::<$component>($builders);)*
    };
    (router, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_router_component::<$component>($builders);)*
    };
    (matcher, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_route_matcher_component::<$component>($builders);)*
    };
}

pub(crate) use register_components;
