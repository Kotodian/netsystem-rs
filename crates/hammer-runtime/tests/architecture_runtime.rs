#[cfg(feature = "outbound-block")]
use hammer_runtime::protocol::block::BlockOutbound;
#[cfg(feature = "endpoint-wireguard")]
use hammer_runtime::protocol::endpoint::wireguard::WireguardEndpoint;
use hammer_runtime::{inbounds::InboundManager, outbounds::OutboundManager};

#[test]
fn protocol_namespace_exposes_runtime_protocols() {
    #[cfg(feature = "outbound-block")]
    let _ = std::any::type_name::<BlockOutbound>();
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
