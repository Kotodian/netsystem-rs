//! WireGuard endpoint.
//!
//! Mirrors sing-box's split between an outer `protocol/wireguard.Endpoint` (which
//! the router dials into via `DialContext`) and an inner `transport/wireguard`
//! device that owns the gVisor stack + wireguard-go runtime. Hammer lands this
//! in stages:
//!
//!   commit 2: scaffold the public type + manager wiring with the dial/listen
//!     surface returning `unimplemented`.
//!   commit 3: real boringtun `Tunn` per peer with LPM routing.
//!   commit 4a (this one): UDP transport actor that drives boringtun handshakes
//!     and shuttles encapsulated frames over a real socket. Endpoint integration
//!     and dial(TCP/UDP) land in 4b.
//!   commit 4b: smoltcp netstack inside the endpoint, lifecycle Start spawns the
//!     transport actor, and dial(TCP/UDP) closes the loop.
//!
//! Plenty of items here are reachable only from `#[cfg(test)]` modules or from
//! commit 4b's not-yet-written endpoint wiring. Suppressing the dead-code
//! warnings keeps the build clean during the staged rollout; the lint goes
//! away as 4b connects everything to a live `lifecycle::start`.
#![allow(dead_code)]

mod peer;
mod transport;

use std::net::IpAddr;

use async_trait::async_trait;
use boringtun::x25519;
use hammer_adapter::{Endpoint, Network, Outbound, ProxyPacketConn, ProxyStream, SocksAddr};
use hammer_core::config::WireguardEndpointOptions;
use hammer_core::error::HammerError;
use hammer_core::log::Logger;

use crate::impl_logging_lifecycle;

use peer::Peer;

/// Tunnel-side overhead added to every IP packet by WireGuard's data frame
/// (16 byte poly1305 tag + 16 byte header). Buffers passed to `Tunn::encapsulate`
/// must be at least `src.len() + 32` bytes.
const WIREGUARD_OVERHEAD: usize = 32;

/// Endpoint backed by WireGuard. The `Tunn` state machines for every peer are
/// alive as soon as `new` returns; commit 4 will wrap them with the smoltcp
/// stack + UDP transport so dial / listen_packet stop returning `unimplemented`.
pub struct WireguardEndpoint {
    logger: Logger,
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    mtu: u32,
    peers: Vec<Peer>,
}

impl WireguardEndpoint {
    pub fn new(logger: Logger, tag: String, options: WireguardEndpointOptions) -> Self {
        let private_key = x25519::StaticSecret::from(options.private_key);
        let peers = options
            .peers
            .into_iter()
            .enumerate()
            .map(|(idx, peer_opts)| Peer::new(peer_opts, &private_key, idx as u32))
            .collect::<Vec<_>>();
        Self {
            logger,
            tag,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            mtu: options.mtu,
            peers,
        }
    }

    /// Largest IP packet the inner stack should hand to `encapsulate`. Anything
    /// bigger has to be fragmented before the WG layer.
    pub(crate) fn mtu(&self) -> u32 {
        self.mtu
    }

    /// Pick the peer that owns `dst` according to longest-prefix `allowed_ips`.
    /// Lifted into a method so commit 4's smoltcp tx loop can route per packet.
    pub(crate) fn route_outbound(&self, dst: IpAddr) -> Option<&Peer> {
        peer::route_outbound(&self.peers, dst).map(|idx| &self.peers[idx])
    }

    pub(crate) fn peers(&self) -> &[Peer] {
        &self.peers
    }

    /// Scratch buffer sized for the largest packet `Tunn::encapsulate` can emit
    /// for this endpoint's MTU.
    pub(crate) fn encapsulate_buffer(&self) -> Vec<u8> {
        vec![0u8; self.mtu as usize + WIREGUARD_OVERHEAD]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Instant;

    use boringtun::noise::TunnResult;
    use hammer_core::config::{WireguardEndpointOptions, WireguardPeerOptions};
    use hammer_core::log::{DiscardWriter, Factory};

    fn logger(tag: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(tag)
    }

    fn x25519_public(secret: [u8; 32]) -> [u8; 32] {
        x25519::PublicKey::from(&x25519::StaticSecret::from(secret)).to_bytes()
    }

    fn make_endpoint(
        name: &'static str,
        my_priv: [u8; 32],
        peer_pub: [u8; 32],
    ) -> WireguardEndpoint {
        let options = WireguardEndpointOptions {
            private_key: my_priv,
            listen_port: 0,
            mtu: 1408,
            address: vec!["10.66.0.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        WireguardEndpoint::new(logger(name), name.to_owned(), options)
    }

    /// Two `WireguardEndpoint`s configured as each other's peer must complete
    /// the noise handshake, after which an IP packet sent through one comes
    /// out byte-for-byte at the other. This is the smoke test that proves the
    /// boringtun + Peer wiring is correct before commit 4 layers smoltcp on top.
    #[test]
    fn boringtun_round_trip_recovers_ip_packet() {
        let a_priv = [1u8; 32];
        let b_priv = [2u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let a = make_endpoint("a", a_priv, b_pub);
        let b = make_endpoint("b", b_priv, a_pub);
        let a_peer = &a.peers()[0];
        let b_peer = &b.peers()[0];

        // A.encapsulate(empty) emits a handshake_init because no session exists.
        let mut buf1 = vec![0u8; 2048];
        let init = match a_peer.lock_tunn().encapsulate(&[], &mut buf1) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: expected handshake_init, got {other:?}"),
        };

        // B receives handshake_init -> emits handshake_response.
        let mut buf2 = vec![0u8; 2048];
        let response = match b_peer.lock_tunn().decapsulate(None, &init, &mut buf2) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("B: expected handshake_response, got {other:?}"),
        };

        // A consumes handshake_response. boringtun queues a keepalive packet
        // back onto the network at this stage, so we accept either Done or
        // WriteToNetwork.
        let mut buf3 = vec![0u8; 2048];
        match a_peer.lock_tunn().decapsulate(None, &response, &mut buf3) {
            TunnResult::Done | TunnResult::WriteToNetwork(_) => {}
            other => panic!("A: handshake_response result {other:?}"),
        }

        // Sessions are now live on both ends. Encrypt an IP packet from A to B.
        let mut ip_packet = vec![0u8; 60];
        ip_packet[0] = 0x45; // IPv4 / IHL=5
        ip_packet[3] = 60; // total length

        let mut enc_buf = vec![0u8; 2048];
        let encrypted = match a_peer.lock_tunn().encapsulate(&ip_packet, &mut enc_buf) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: encapsulate {other:?}"),
        };

        let mut dec_buf = vec![0u8; 2048];
        match b_peer.lock_tunn().decapsulate(None, &encrypted, &mut dec_buf) {
            TunnResult::WriteToTunnelV4(out, _src) => assert_eq!(out, ip_packet),
            other => panic!("B: decapsulate {other:?}"),
        }
    }

    /// Multi-peer routing: a wider `allowed_ips` peer must lose to a more
    /// specific peer when the destination falls inside both — sing-box does
    /// the same longest-prefix match (`route/peers.go::matchAddress`).
    #[test]
    fn route_outbound_picks_longest_prefix_peer() {
        let local_priv = [3u8; 32];
        let peer1_pub = x25519_public([4u8; 32]);
        let peer2_pub = x25519_public([5u8; 32]);
        let opts = WireguardEndpointOptions {
            private_key: local_priv,
            listen_port: 0,
            mtu: 1408,
            address: vec!["10.66.0.2/32".parse().unwrap()],
            peers: vec![
                WireguardPeerOptions {
                    public_key: peer1_pub,
                    pre_shared_key: None,
                    endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 51820),
                    allowed_ips: vec!["10.0.0.0/8".parse().unwrap()],
                    persistent_keepalive: None,
                    reserved: [0; 3],
                },
                WireguardPeerOptions {
                    public_key: peer2_pub,
                    pre_shared_key: None,
                    endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2)), 51820),
                    allowed_ips: vec!["10.66.0.0/16".parse().unwrap()],
                    persistent_keepalive: None,
                    reserved: [0; 3],
                },
            ],
        };
        let ep = WireguardEndpoint::new(logger("multi"), "multi".to_owned(), opts);

        let chosen = ep
            .route_outbound(IpAddr::V4(Ipv4Addr::new(10, 66, 0, 5)))
            .expect("must route to specific peer");
        assert_eq!(chosen.endpoint().to_string(), "2.2.2.2:51820");

        let chosen = ep
            .route_outbound(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)))
            .expect("must route to wider peer");
        assert_eq!(chosen.endpoint().to_string(), "1.1.1.1:51820");

        assert!(
            ep.route_outbound(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)))
                .is_none(),
            "no peer must match an out-of-range destination"
        );
    }

    #[test]
    fn encapsulate_buffer_sized_for_mtu_plus_overhead() {
        let priv_key = [9u8; 32];
        let peer_pub = x25519_public([10u8; 32]);
        let opts = WireguardEndpointOptions {
            private_key: priv_key,
            listen_port: 0,
            mtu: 1408,
            address: vec!["10.66.0.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [1, 2, 3],
            }],
        };
        let ep = WireguardEndpoint::new(logger("buf"), "buf".to_owned(), opts);
        assert_eq!(
            ep.encapsulate_buffer().len(),
            ep.mtu() as usize + WIREGUARD_OVERHEAD
        );
        assert_eq!(ep.peers()[0].reserved(), [1, 2, 3]);
    }
}
