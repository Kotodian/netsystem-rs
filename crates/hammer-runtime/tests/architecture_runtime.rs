use hammer_runtime::{
    dns::{DnsRouter, DnsTransportManager},
    inbounds::InboundManager,
    outbounds::OutboundManager,
    protocol::{
        block::BlockOutbound, direct::DirectOutbound, hysteria2::Hysteria2Outbound, tun::TunInbound,
    },
    route::Router,
};

#[cfg(feature = "endpoint")]
use hammer_runtime::endpoints::EndpointManager;
#[cfg(feature = "wireguard")]
use hammer_runtime::protocol::wireguard::WireguardEndpoint;

#[test]
fn protocol_namespace_exposes_runtime_protocols() {
    let _ = std::any::type_name::<BlockOutbound>();
    let _ = std::any::type_name::<DirectOutbound>();
    let _ = std::any::type_name::<Hysteria2Outbound>();
    let _ = std::any::type_name::<TunInbound>();

    #[cfg(feature = "wireguard")]
    let _ = std::any::type_name::<WireguardEndpoint>();
}

#[test]
fn domain_namespaces_expose_runtime_managers() {
    let _ = std::any::type_name::<DnsRouter>();
    let _ = std::any::type_name::<DnsTransportManager>();
    #[cfg(feature = "endpoint")]
    let _ = std::any::type_name::<EndpointManager>();
    let _ = std::any::type_name::<InboundManager>();
    let _ = std::any::type_name::<OutboundManager>();
    let _ = std::any::type_name::<Router>();
}
