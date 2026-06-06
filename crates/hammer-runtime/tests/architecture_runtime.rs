#[cfg(feature = "outbound-block")]
use hammer_runtime::protocol::block::BlockOutbound;
#[cfg(feature = "outbound-direct")]
use hammer_runtime::protocol::direct::DirectOutbound;
#[cfg(feature = "endpoint-wireguard")]
use hammer_runtime::protocol::endpoint::wireguard::WireguardEndpoint;
#[cfg(feature = "outbound-hysteria2")]
use hammer_runtime::protocol::hysteria2::Hysteria2Outbound;
use hammer_runtime::{inbounds::InboundManager, outbounds::OutboundManager};

#[test]
fn protocol_namespace_exposes_runtime_protocols() {
    #[cfg(feature = "outbound-block")]
    let _ = std::any::type_name::<BlockOutbound>();
    #[cfg(feature = "outbound-direct")]
    let _ = std::any::type_name::<DirectOutbound>();
    #[cfg(feature = "outbound-hysteria2")]
    let _ = std::any::type_name::<Hysteria2Outbound>();
    #[cfg(feature = "endpoint-wireguard")]
    let _ = std::any::type_name::<WireguardEndpoint>();
}

#[test]
fn domain_namespaces_expose_runtime_managers() {
    #[cfg(feature = "endpoint")]
    let _ = std::any::type_name::<hammer_runtime::endpoints::EndpointManager>();
    let _ = std::any::type_name::<InboundManager>();
    let _ = std::any::type_name::<OutboundManager>();
}
