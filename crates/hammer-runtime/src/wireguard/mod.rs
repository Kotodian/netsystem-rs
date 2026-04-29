//! WireGuard endpoint scaffold.
//!
//! Mirrors sing-box's split between an outer `protocol/wireguard.Endpoint` (which
//! the router dials into via `DialContext`) and an inner `transport/wireguard`
//! device that owns the gVisor stack + wireguard-go runtime. Hammer's plan
//! lands these in three commits:
//!
//!   commit 2 (this one): scaffold the public type + manager wiring with the
//!     dial/listen surface returning `unimplemented`. Lets us plumb config →
//!     `EndpointManager` → service lifecycle without dragging boringtun in yet.
//!   commit 3: real boringtun + UDP transport so two wg peers can encapsulate.
//!   commit 4: smoltcp netstack so dial(TCP/UDP) terminates inside the tunnel.

use async_trait::async_trait;
use hammer_adapter::{Endpoint, Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::WireguardEndpointOptions;
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

/// Endpoint backed by WireGuard. Currently a placeholder — `dial` /
/// `listen_packet` short-circuit with a clear error so any router or test
/// hitting this surface fails loudly until the boringtun + smoltcp stacks land
/// in later commits.
pub struct WireguardEndpoint {
    logger: Logger,
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    #[allow(dead_code)]
    options: WireguardEndpointOptions,
}

impl WireguardEndpoint {
    pub fn new(logger: Logger, tag: String, options: WireguardEndpointOptions) -> Self {
        Self {
            logger,
            tag,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            options,
        }
    }
}

impl_logging_lifecycle!(WireguardEndpoint, "wireguard-endpoint");

#[async_trait]
impl Outbound for WireguardEndpoint {
    fn type_name(&self) -> &str {
        hammer_core::config::constants::TYPE_WIREGUARD
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn networks(&self) -> &[Network] {
        &self.networks
    }

    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    async fn dial(
        &self,
        _network: Network,
        destination: SocksAddr,
        _initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        self.logger
            .warn(format!("wireguard dial scaffold hit: {destination}"));
        Err(HammerError::internal(
            "wireguard endpoint dial is not implemented yet (commit 4)",
        ))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        self.logger.warn("wireguard listen_packet scaffold hit");
        Err(HammerError::internal(
            "wireguard endpoint listen_packet is not implemented yet (commit 4)",
        ))
    }
}

impl Endpoint for WireguardEndpoint {}
