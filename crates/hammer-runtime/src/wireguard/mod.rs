//! WireGuard endpoint.
//!
//! Mirrors sing-box's split between an outer `protocol/wireguard.Endpoint`
//! (which the router dials into via `DialContext`) and an inner
//! `transport/wireguard` device that owns the gVisor stack + wireguard-go
//! runtime. Hammer's stack equivalent is `wireguard::stack` (smoltcp) +
//! `wireguard::transport` (boringtun + UDP); this module ties them together
//! into the public `Outbound`/`Endpoint` surface and the lifecycle.
//!
//! A handful of helpers (mtu/peers/route_outbound/encapsulate_buffer) are only
//! reachable from `#[cfg(test)]` until commit 5 wires the endpoint into
//! `OutboundManager` and the router can actually invoke `dial`. The mod-level
//! `allow(dead_code)` suppresses the warnings during the staged rollout.
#![allow(dead_code)]

mod device;
mod peer;
mod stack;
mod transport;

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use boringtun::x25519;
use ipnet::IpNet;
use tokio::io::AsyncWriteExt;

use hammer_adapter::{
    Endpoint, Lifecycle, Network, Outbound, PlatformInterface, ProxyPacketConn, ProxyStream,
    SocksAddr,
};
use hammer_core::config::WireguardEndpointOptions;
use hammer_core::error::HammerError;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;

use crate::socket_protector::SocketProtector;

use peer::Peer;
use stack::StackHandles;
use transport::TransportHandles;

/// Tunnel-side overhead added to every IP packet by WireGuard's data frame
/// (16 byte poly1305 tag + 16 byte header). Buffers passed to `Tunn::encapsulate`
/// must be at least `src.len() + 32` bytes.
const WIREGUARD_OVERHEAD: usize = 32;

/// Endpoint backed by WireGuard. Its `Tunn` state machines and smoltcp stack
/// are inert until the lifecycle reaches `Start`; before that, calls to
/// `dial`/`listen_packet` fail with a clear error.
pub struct WireguardEndpoint {
    logger: Logger,
    tag: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    mtu: u32,
    listen_port: u16,
    addresses: Vec<IpNet>,
    peers: Arc<Vec<Peer>>,
    protector: SocketProtector,
    inner: Mutex<EndpointState>,
}

struct EndpointRuntime {
    transport: TransportHandles,
    stack: Arc<StackHandles>,
}

enum EndpointState {
    Idle,
    Running(EndpointRuntime),
    Closed,
}

impl WireguardEndpoint {
    pub fn new(
        logger: Logger,
        tag: String,
        options: WireguardEndpointOptions,
        protector: SocketProtector,
    ) -> Self {
        let private_key = x25519::StaticSecret::from(options.private_key);
        let peers: Vec<Peer> = options
            .peers
            .into_iter()
            .enumerate()
            .map(|(idx, peer_opts)| Peer::new(peer_opts, &private_key, idx as u32))
            .collect();
        Self {
            logger,
            tag,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            mtu: options.mtu,
            listen_port: options.listen_port,
            addresses: options.address,
            peers: Arc::new(peers),
            protector,
            inner: Mutex::new(EndpointState::Idle),
        }
    }

    /// Largest IP packet the inner stack should hand to `encapsulate`.
    pub(crate) fn mtu(&self) -> u32 {
        self.mtu
    }

    /// Pick the peer that owns `dst` according to longest-prefix `allowed_ips`.
    pub(crate) fn route_outbound(&self, dst: IpAddr) -> Option<&Peer> {
        peer::route_outbound(&self.peers, dst).map(|idx| &self.peers[idx])
    }

    pub(crate) fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub(crate) fn encapsulate_buffer(&self) -> Vec<u8> {
        vec![0u8; self.mtu as usize + WIREGUARD_OVERHEAD]
    }

    /// Bring up the transport + stack actors. Idempotent within a single
    /// lifecycle Start sequence (the runtime guarantees `Start` only fires
    /// once per stage anyway, but defensively no-op on re-entry).
    fn boot(&self) -> Result<(), HammerError> {
        {
            let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            if matches!(*inner, EndpointState::Running(_) | EndpointState::Closed) {
                return Ok(());
            }
        }

        let transport = transport::spawn_transport(
            self.logger.clone(),
            Arc::clone(&self.peers),
            self.listen_port,
            self.mtu,
            self.protector.clone(),
        )?;

        // The transport's `inbound_rx` is only useful to the stack actor, so we
        // destructure here and hand it straight over.
        let TransportHandles {
            encrypt_tx,
            inbound_rx,
            local_addr: _,
            shutdown,
            join,
        } = transport;

        let stack = stack::spawn_stack(
            self.logger.clone(),
            self.addresses.clone(),
            self.mtu,
            inbound_rx,
            encrypt_tx.clone(),
        )?;

        let runtime = EndpointRuntime {
            transport: TransportHandles {
                encrypt_tx,
                // After hand-off the transport handle no longer needs to expose
                // inbound_rx; replace with a sentinel closed channel so the type
                // stays the same.
                inbound_rx: tokio::sync::mpsc::channel(1).1,
                local_addr: "0.0.0.0:0".parse().expect("placeholder addr"),
                shutdown,
                join,
            },
            stack: Arc::new(stack),
        };

        *self.inner.lock().expect("WireguardEndpoint poisoned") =
            EndpointState::Running(runtime);
        Ok(())
    }

    /// Cancel the actors. Safe to call multiple times.
    fn shutdown(&self) {
        let runtime = {
            let mut inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            match std::mem::replace(&mut *inner, EndpointState::Closed) {
                EndpointState::Running(rt) => Some(rt),
                _ => None,
            }
        };
        if let Some(rt) = runtime {
            // Transport: send shutdown signal then abort to be sure.
            let _ = rt.transport.shutdown.send(());
            rt.transport.join.abort();
            // Stack: same — Arc may have other refs (in-flight dial calls),
            // signal_shutdown is safe under shared ownership.
            rt.stack.signal_shutdown();
            rt.stack.abort();
        }
    }

    fn stack_handle(&self) -> Result<Arc<StackHandles>, HammerError> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            EndpointState::Running(rt) => Ok(Arc::clone(&rt.stack)),
            EndpointState::Idle => Err(HammerError::internal(
                "wireguard endpoint not started yet",
            )),
            EndpointState::Closed => Err(HammerError::internal(
                "wireguard endpoint is closed",
            )),
        }
    }
}

impl Lifecycle for WireguardEndpoint {
    fn name(&self) -> &str {
        "wireguard-endpoint"
    }

    fn start(&self, stage: StartStage) -> Result<(), HammerError> {
        // The actor needs to exist by the time the router/inbounds touch us,
        // and our dependencies (the platform protector) are ready by `Start`.
        if matches!(stage, StartStage::Start) {
            self.boot()?;
        }
        Ok(())
    }

    fn close(&self) -> Result<(), HammerError> {
        self.shutdown();
        Ok(())
    }
}

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
        network: Network,
        destination: SocksAddr,
        initial_payload: &[u8],
    ) -> Result<Box<dyn ProxyStream>, HammerError> {
        if network != Network::Tcp {
            return Err(HammerError::internal(
                "wireguard endpoint dial only supports TCP — UDP goes through listen_packet",
            ));
        }
        let stack = self.stack_handle()?;
        let dst = SocketAddr::new(destination.host, destination.port);
        self.logger
            .info(format!("wireguard dial -> {dst}"));
        let mut stream = stack.dial_tcp(dst).await?;
        if !initial_payload.is_empty() {
            stream
                .write_all(initial_payload)
                .await
                .map_err(|err| HammerError::internal(format!("wireguard initial write: {err}")))?;
        }
        Ok(Box::new(stream))
    }

    async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
        let stack = self.stack_handle()?;
        let handle = stack.bind_udp().await?;
        Ok(Box::new(handle))
    }
}

impl Endpoint for WireguardEndpoint {}

/// Helper used by tests + EndpointManager: build an endpoint with a
/// no-platform protector. Production callers go through
/// `WireguardEndpoint::new` with a real protector.
#[cfg(test)]
fn endpoint_for_test(
    logger: Logger,
    tag: String,
    options: WireguardEndpointOptions,
) -> WireguardEndpoint {
    WireguardEndpoint::new(logger, tag, options, SocketProtector::default())
}

/// Convenience constructor used by `EndpointManager::from_options_with_platform`.
pub(crate) fn build_with_platform(
    logger: Logger,
    tag: String,
    options: WireguardEndpointOptions,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> WireguardEndpoint {
    let protector = platform
        .map(SocketProtector::new)
        .unwrap_or_default();
    WireguardEndpoint::new(logger, tag, options, protector)
}

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
        endpoint_for_test(logger(name), name.to_owned(), options)
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
        let ep = endpoint_for_test(logger("multi"), "multi".to_owned(), opts);

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
        let ep = endpoint_for_test(logger("buf"), "buf".to_owned(), opts);
        assert_eq!(
            ep.encapsulate_buffer().len(),
            ep.mtu() as usize + WIREGUARD_OVERHEAD
        );
        assert_eq!(ep.peers()[0].reserved(), [1, 2, 3]);
    }

    /// End-to-end tunnel smoke: stand up two `WireguardEndpoint`s configured
    /// as each other's peer, drive lifecycle Start so the transport + smoltcp
    /// actors are live, bind a UDP socket on each side, then push a payload
    /// from A's in-tunnel address to B's. The packet has to traverse:
    ///
    ///   A.dial(UDP) → smoltcp tx → boringtun.encapsulate → real UDP → B
    ///   → boringtun.decapsulate → smoltcp rx → B.recv_from
    ///
    /// This exercises every layer that landed in commits 3 → 4b in one pass.
    /// We don't try TCP yet because the stack only exposes `dial`, not
    /// `listen` — closing the loop on inbound TCP is a future follow-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tunnel_udp_round_trip_end_to_end() {
        use hammer_adapter::ProxyPacketConn;
        use std::time::Duration;
        use tokio::time::timeout;

        // Pick two ephemeral ports for the outer UDP sockets. Bind+drop is the
        // standard "ask the OS for a free port" trick — fine on localhost.
        fn ephemeral_port() -> u16 {
            std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }

        let a_priv = [33u8; 32];
        let b_priv = [44u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let outer_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let outer_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let a_options = WireguardEndpointOptions {
            private_key: a_priv,
            listen_port: port_a,
            mtu: 1408,
            address: vec!["10.0.0.1/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: b_pub,
                pre_shared_key: None,
                endpoint: outer_b,
                allowed_ips: vec!["10.0.0.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        let b_options = WireguardEndpointOptions {
            private_key: b_priv,
            listen_port: port_b,
            mtu: 1408,
            address: vec!["10.0.0.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: a_pub,
                pre_shared_key: None,
                endpoint: outer_a,
                allowed_ips: vec!["10.0.0.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };

        let a = endpoint_for_test(logger("wg-a"), "wg-a".to_owned(), a_options);
        let b = endpoint_for_test(logger("wg-b"), "wg-b".to_owned(), b_options);
        a.boot().expect("boot a");
        b.boot().expect("boot b");

        let stack_a = a.stack_handle().expect("stack a ready");
        let stack_b = b.stack_handle().expect("stack b ready");

        let mut udp_a = stack_a.bind_udp().await.expect("bind udp a");
        let mut udp_b = stack_b.bind_udp().await.expect("bind udp b");
        let port_b_inside = udp_b.local_port();

        // A's in-tunnel address is 10.0.0.1 ← bound by the smoltcp Interface;
        // we want to *send to* B's in-tunnel address (10.0.0.2:<udp_b port>).
        let dst = SocksAddr {
            host: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            port: port_b_inside,
        };
        let payload = b"hello over wireguard".to_vec();
        udp_a
            .send_to(dst, &payload)
            .await
            .expect("udp_a.send_to");

        // 5s is plenty: the boringtun handshake completes in <50 ms over
        // localhost even with the 250 ms timer driving retransmits.
        let datagram = timeout(Duration::from_secs(5), udp_b.recv_from())
            .await
            .expect("timed out waiting for tunnel UDP")
            .expect("udp_b.recv_from");
        assert_eq!(datagram.payload, payload);
        assert_eq!(
            datagram.destination.host,
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            "src must be A's in-tunnel address"
        );

        // Aborting actors via shutdown — Drop order is undefined across the two
        // endpoints' Mutex guards, so do it explicitly.
        a.shutdown();
        b.shutdown();
    }

    /// End-to-end TCP: B listens inside the tunnel, A dials it, both ends
    /// exchange a payload in each direction. Touches everything UDP doesn't:
    /// smoltcp `tcp::Socket` connect/listen, the SYN/ACK + handshake driving
    /// `may_send`/`may_recv` transitions, partial-write retries on the actor
    /// side, and bidirectional `DuplexStream` plumbing through bridges on
    /// both endpoints.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tunnel_tcp_round_trip_end_to_end() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::timeout;

        fn ephemeral_port() -> u16 {
            std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }

        let a_priv = [55u8; 32];
        let b_priv = [66u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let outer_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let outer_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let a_options = WireguardEndpointOptions {
            private_key: a_priv,
            listen_port: port_a,
            mtu: 1408,
            address: vec!["10.0.0.1/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: b_pub,
                pre_shared_key: None,
                endpoint: outer_b,
                allowed_ips: vec!["10.0.0.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        let b_options = WireguardEndpointOptions {
            private_key: b_priv,
            listen_port: port_b,
            mtu: 1408,
            address: vec!["10.0.0.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: a_pub,
                pre_shared_key: None,
                endpoint: outer_a,
                allowed_ips: vec!["10.0.0.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };

        let a = endpoint_for_test(logger("wg-tcp-a"), "wg-a".to_owned(), a_options);
        let b = endpoint_for_test(logger("wg-tcp-b"), "wg-b".to_owned(), b_options);
        a.boot().expect("boot a");
        b.boot().expect("boot b");

        let stack_a = a.stack_handle().expect("stack a ready");
        let stack_b = b.stack_handle().expect("stack b ready");

        const PORT: u16 = 8080;
        let listener = stack_b.listen_tcp(PORT);
        // Park the accept side first so the listening socket is in SocketSet
        // before A's SYN arrives — otherwise smoltcp would RST it.
        let accept_task = tokio::spawn(async move { listener.accept().await });

        // A dials B's in-tunnel address.
        let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), PORT);
        let mut stream_a = timeout(Duration::from_secs(5), stack_a.dial_tcp(dst))
            .await
            .expect("dial timed out")
            .expect("dial_tcp");

        let mut stream_b = timeout(Duration::from_secs(5), accept_task)
            .await
            .expect("accept timed out")
            .expect("accept join")
            .expect("accept");

        // A → B
        stream_a
            .write_all(b"ping from A")
            .await
            .expect("write A");
        stream_a.flush().await.expect("flush A");
        let mut buf = [0u8; 11];
        timeout(Duration::from_secs(5), stream_b.read_exact(&mut buf))
            .await
            .expect("read B timed out")
            .expect("read B");
        assert_eq!(&buf, b"ping from A");

        // B → A
        stream_b
            .write_all(b"pong from B")
            .await
            .expect("write B");
        stream_b.flush().await.expect("flush B");
        let mut buf = [0u8; 11];
        timeout(Duration::from_secs(5), stream_a.read_exact(&mut buf))
            .await
            .expect("read A timed out")
            .expect("read A");
        assert_eq!(&buf, b"pong from B");

        drop(stream_a);
        drop(stream_b);
        a.shutdown();
        b.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tunnel_tcp_preserves_large_backpressured_stream() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{sleep, timeout};

        fn ephemeral_port() -> u16 {
            std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }

        let a_priv = [77u8; 32];
        let b_priv = [88u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let outer_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let outer_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let a_options = WireguardEndpointOptions {
            private_key: a_priv,
            listen_port: port_a,
            mtu: 1408,
            address: vec!["10.0.1.1/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: b_pub,
                pre_shared_key: None,
                endpoint: outer_b,
                allowed_ips: vec!["10.0.1.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        let b_options = WireguardEndpointOptions {
            private_key: b_priv,
            listen_port: port_b,
            mtu: 1408,
            address: vec!["10.0.1.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: a_pub,
                pre_shared_key: None,
                endpoint: outer_a,
                allowed_ips: vec!["10.0.1.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };

        let a = endpoint_for_test(logger("wg-big-a"), "wg-a".to_owned(), a_options);
        let b = endpoint_for_test(logger("wg-big-b"), "wg-b".to_owned(), b_options);
        a.boot().expect("boot a");
        b.boot().expect("boot b");

        let stack_a = a.stack_handle().expect("stack a ready");
        let stack_b = b.stack_handle().expect("stack b ready");

        const PORT: u16 = 8081;
        let listener = stack_b.listen_tcp(PORT);
        let accept_task = tokio::spawn(async move { listener.accept().await });

        let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 2)), PORT);
        let mut stream_a = timeout(Duration::from_secs(5), stack_a.dial_tcp(dst))
            .await
            .expect("dial timed out")
            .expect("dial_tcp");
        let mut stream_b = timeout(Duration::from_secs(5), accept_task)
            .await
            .expect("accept timed out")
            .expect("accept join")
            .expect("accept");

        let payload = (0..(512 * 1024))
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            for chunk in payload.chunks(4096) {
                stream_a.write_all(chunk).await.expect("write payload");
            }
            stream_a.shutdown().await.expect("shutdown stream");
        });

        // Let B's user-side reader lag so the actor-to-bridge channel and
        // DuplexStream hit backpressure before we drain them.
        sleep(Duration::from_millis(200)).await;

        let mut received = vec![0u8; expected.len()];
        timeout(Duration::from_secs(10), stream_b.read_exact(&mut received))
            .await
            .expect("read timed out")
            .expect("read_exact");
        writer.await.expect("writer join");

        assert_eq!(received, expected);

        a.shutdown();
        b.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tunnel_tcp_dial_fails_when_remote_port_is_closed() {
        use std::time::Duration;
        use tokio::time::timeout;

        fn ephemeral_port() -> u16 {
            std::net::UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }

        let a_priv = [99u8; 32];
        let b_priv = [111u8; 32];
        let a_pub = x25519_public(a_priv);
        let b_pub = x25519_public(b_priv);

        let port_a = ephemeral_port();
        let port_b = ephemeral_port();
        let outer_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_a);
        let outer_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port_b);

        let a_options = WireguardEndpointOptions {
            private_key: a_priv,
            listen_port: port_a,
            mtu: 1408,
            address: vec!["10.0.2.1/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: b_pub,
                pre_shared_key: None,
                endpoint: outer_b,
                allowed_ips: vec!["10.0.2.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        let b_options = WireguardEndpointOptions {
            private_key: b_priv,
            listen_port: port_b,
            mtu: 1408,
            address: vec!["10.0.2.2/32".parse().unwrap()],
            peers: vec![WireguardPeerOptions {
                public_key: a_pub,
                pre_shared_key: None,
                endpoint: outer_a,
                allowed_ips: vec!["10.0.2.0/24".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };

        let a = endpoint_for_test(logger("wg-closed-a"), "wg-a".to_owned(), a_options);
        let b = endpoint_for_test(logger("wg-closed-b"), "wg-b".to_owned(), b_options);
        a.boot().expect("boot a");
        b.boot().expect("boot b");

        let stack_a = a.stack_handle().expect("stack a ready");
        let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 2, 2)), 9099);
        let err = timeout(Duration::from_secs(5), stack_a.dial_tcp(dst))
            .await
            .expect("dial should resolve")
            .expect_err("dial to closed remote port must fail");
        assert!(
            err.to_string().contains("wireguard tcp connect failed"),
            "unexpected error: {err}"
        );

        a.shutdown();
        b.shutdown();
    }
}
