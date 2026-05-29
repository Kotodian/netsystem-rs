use std::collections::HashMap;

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

#[cfg(any(
    feature = "dns-udp",
    feature = "dns-tcp",
    feature = "dns-https",
    feature = "dns-hosts",
    feature = "dns-local"
))]
macro_rules! register_components {
    (dns_transport, $builders:expr, [$($component:path),* $(,)?]) => {
        $(crate::component_registry::register_dns_transport_component::<$component>($builders);)*
    };
}

#[cfg(any(
    feature = "dns-udp",
    feature = "dns-tcp",
    feature = "dns-https",
    feature = "dns-hosts",
    feature = "dns-local"
))]
pub(crate) use register_components;
