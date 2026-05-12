use bytes::Bytes;
use hammer_core::error::CoreResult;
use hammer_core::lifecycle::Lifecycle;
use ipnet::IpNet;
use std::net::SocketAddr;
use tokio::sync::mpsc;

use crate::Network;
use crate::RuntimeComponent;

pub type EndpointComponent = RuntimeComponent<dyn Endpoint>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EndpointLocalFlow {
    pub network: Network,
    pub local: SocketAddr,
    pub remote: SocketAddr,
}

/// L3 tunnel endpoint (wireguard today; future ipsec / wg-over-tcp later).
///
/// Endpoints exchange **raw IP packets** with the runtime — they own their
/// own UDP/TCP sockets internally and surface a packet-level interface,
/// not the L4 dial/listen stream API that lives on `Outbound`. The two
/// traits sit **side-by-side**, not in inheritance: an endpoint never
/// pretends to be a stream-style outbound. Forcing them to do so (the old
/// `Endpoint: Outbound` supertrait) made wireguard run a second user-space IP
/// stack inside its endpoint just to reassemble TCP into IP packets, which
/// blew up both latency and NetExt memory.
pub trait Endpoint: Lifecycle {
    /// Runtime component id used by route decisions.
    fn id(&self) -> &str;

    /// Hot-path egress: clone the encrypt channel sender so the TUN packet
    /// loop can push raw IP packets without going through `dyn Trait`
    /// dispatch per packet. Cheap (`Arc` clone under the hood).
    fn ip_send_clone(&self) -> mpsc::Sender<Bytes>;

    /// One-shot ingress receiver. Returns the inbound IP packet stream
    /// (already decapsulated, ready to write back to TUN) the first time
    /// it is called; subsequent calls return `None`. The runtime takes
    /// ownership of the receiver at start-up and fans it into the TUN
    /// writer.
    fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>>;

    /// One-shot ingress receiver for a *local* consumer (the
    /// EndpointOutboundAdapter). Calling this enables a lazy fan-out
    /// inside the endpoint: decapsulated IP packets matching a registered
    /// local flow are delivered to this channel instead of the default
    /// `ip_recv_take` channel that feeds the TUN packet loop.
    ///
    /// Default `None` keeps non-WG endpoints honest — they opt in by
    /// overriding this and honoring `register_local_flow`.
    fn ip_local_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        None
    }

    /// Register a locally originated flow that should be consumed by
    /// `ip_local_recv_take` rather than mirrored into the default TUN
    /// receiver. Endpoint outbound adapters use this for their own DNS
    /// TCP/UDP sessions.
    fn register_local_flow(&self, _flow: EndpointLocalFlow) {}

    fn unregister_local_flow(&self, _flow: EndpointLocalFlow) {}

    /// Maximum raw IP packet size this endpoint can encapsulate without
    /// fragmentation / PMTU feedback. `None` means the endpoint accepts the
    /// stack's packet size directly.
    fn ip_packet_mtu(&self) -> Option<usize> {
        None
    }

    /// IP destinations this endpoint owns. The TUN packet loop builds a
    /// longest-prefix table from every endpoint's `allowed_destinations`;
    /// a packet whose destination IP falls inside one of these prefixes
    /// is forwarded **as a raw IP packet** to that endpoint's encrypt
    /// channel, skipping the system-stack NAT/listener round-trip.
    ///
    /// Defaults to empty so non-routing endpoints opt out automatically.
    fn allowed_destinations(&self) -> Vec<IpNet> {
        Vec::new()
    }

    /// Interface addresses owned by this endpoint. The system TUN can use a
    /// different local address, so the L3 fast path rewrites between the TUN
    /// address and endpoint address when forwarding raw IP packets.
    fn interface_addresses(&self) -> Vec<IpNet> {
        Vec::new()
    }

    /// Reset transport state after the platform network/interface changes.
    fn reset(&self) {}
}

pub trait EndpointManager: Lifecycle {
    fn list(&self) -> Vec<EndpointComponent>;
    fn get(&self, id: &str) -> Option<EndpointComponent>;
    fn remove(&self, id: &str) -> CoreResult<()>;
}
