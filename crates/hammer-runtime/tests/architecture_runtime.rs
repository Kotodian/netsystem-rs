#[cfg(feature = "inbound-tun")]
use hammer_adapter::Router as AdapterRouter;
#[cfg(feature = "outbound-block")]
use hammer_runtime::protocol::block::BlockOutbound;
#[cfg(feature = "outbound-direct")]
use hammer_runtime::protocol::direct::DirectOutbound;
#[cfg(feature = "endpoint-wireguard")]
use hammer_runtime::protocol::endpoint::wireguard::WireguardEndpoint;
#[cfg(feature = "outbound-hysteria2")]
use hammer_runtime::protocol::hysteria2::Hysteria2Outbound;
use hammer_runtime::{inbounds::InboundManager, outbounds::OutboundManager};
#[cfg(feature = "inbound-tun")]
use hammer_runtime::{inbounds::RuntimeDnsRouter, protocol::tun::TunInbound};

#[test]
fn protocol_namespace_exposes_runtime_protocols() {
    #[cfg(feature = "outbound-block")]
    let _ = std::any::type_name::<BlockOutbound>();
    #[cfg(feature = "outbound-direct")]
    let _ = std::any::type_name::<DirectOutbound>();
    #[cfg(feature = "outbound-hysteria2")]
    let _ = std::any::type_name::<Hysteria2Outbound>();
    #[cfg(feature = "inbound-tun")]
    let _ = std::any::type_name::<
        TunInbound<
            dyn AdapterRouter,
            RuntimeDnsRouter,
            OutboundManager,
            hammer_runtime::endpoints::EndpointManager,
        >,
    >();

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
