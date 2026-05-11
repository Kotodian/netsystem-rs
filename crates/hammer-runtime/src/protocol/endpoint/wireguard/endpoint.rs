//! WireGuard endpoint.
//!
//! Owns the boringtun + UDP transport actor and exposes a pure L3 surface
//! (`Endpoint::ip_send_clone` / `Endpoint::ip_recv_take`) so the TUN packet
//! loop can forward raw IP packets without any L4 reassembly. The old
//! double user-space-IP-stack data path is gone: WG sits next to outbounds,
//! not under them.
#![allow(dead_code)]

use boringtun::x25519;
use bytes::Bytes;
use ipnet::IpNet;
use std::net::IpAddr;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use hammer_adapter::{Endpoint, Lifecycle, Network, PlatformInterface};
use hammer_core::config::{Endpoint as EndpointOptions, EndpointKind, WireguardEndpointOptions};
#[cfg(test)]
use hammer_core::error::HammerError;
use hammer_core::error::HammerResult;
use hammer_core::lifecycle::StartStage;
use hammer_core::log::Logger;
use hammer_core::protocol::wireguard::WIREGUARD_OVERHEAD;
use hammer_core::protocol::wireguard::peer::{self, Peer};

use super::transport::{self, TransportHandles};
use crate::protocol::endpoint::EndpointRuntimeOptions;
use crate::socket_protector::SocketProtector;

/// L3 WireGuard endpoint. `Tunn` state machines and the UDP transport actor
/// are inert until lifecycle reaches `Start`; before that, `ip_send_clone`
/// hands out a closed sender and `ip_recv_take` returns `None`.
#[hammer_component_macros::hammer_component(
    endpoint,
    name = "wireguard",
    builder = build_endpoint_views,
    metrics = ("endpoint", "wireguard")
)]
pub struct WireguardEndpoint {
    logger: Logger,
    id: String,
    networks: Vec<Network>,
    dependencies: Vec<String>,
    mtu: u32,
    listen_port: u16,
    addresses: Vec<IpNet>,
    peers: Arc<Vec<Peer>>,
    protector: SocketProtector,
    inner: Mutex<WireguardState>,
    #[cfg(test)]
    fail_next_start: AtomicBool,
}

struct WireguardRuntime {
    transport: TransportHandles,
    /// Cloneable hot-path sender so `ip_send_clone` doesn't have to mutex-walk
    /// for every packet.
    encrypt_tx: mpsc::Sender<Bytes>,
    /// One-shot inbound receiver. Taken by the TUN packet-loop fan-in at
    /// start-up; subsequent calls return `None`.
    ip_recv_rx: Mutex<Option<mpsc::Receiver<Bytes>>>,
}

enum WireguardState {
    Idle,
    Running(WireguardRuntime),
    Closed,
}

impl WireguardEndpoint {
    fn new(
        logger: Logger,
        options: EndpointRuntimeOptions<WireguardEndpointOptions>,
        protector: SocketProtector,
    ) -> Self {
        let EndpointRuntimeOptions {
            id,
            interface,
            protocol: options,
        } = options;
        let private_key = x25519::StaticSecret::from(options.private_key);
        let peers: Vec<Peer> = options
            .peers
            .into_iter()
            .enumerate()
            .map(|(idx, peer_opts)| Peer::new(peer_opts, &private_key, idx as u32))
            .collect();
        Self {
            logger,
            id,
            networks: vec![Network::Tcp, Network::Udp],
            dependencies: Vec::new(),
            mtu: interface.mtu,
            listen_port: options.listen_port,
            addresses: interface.address,
            peers: Arc::new(peers),
            protector,
            inner: Mutex::new(WireguardState::Idle),
            #[cfg(test)]
            fail_next_start: AtomicBool::new(false),
        }
    }

    pub(crate) fn mtu(&self) -> u32 {
        self.mtu
    }

    pub(crate) fn route_outbound(&self, dst: IpAddr) -> Option<&Peer> {
        peer::route_outbound(&self.peers, dst).map(|idx| &self.peers[idx])
    }

    pub(crate) fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub(crate) fn encapsulate_buffer(&self) -> Vec<u8> {
        vec![0u8; self.mtu as usize + WIREGUARD_OVERHEAD]
    }

    pub(crate) fn addresses(&self) -> &[IpNet] {
        &self.addresses
    }

    fn boot(&self) -> HammerResult<()> {
        {
            let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            if matches!(*inner, WireguardState::Running(_) | WireguardState::Closed) {
                return Ok(());
            }
        }

        let runtime = self.start_runtime()?;
        let mut inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        if matches!(*inner, WireguardState::Idle) {
            *inner = WireguardState::Running(runtime);
        } else {
            stop_runtime(runtime);
        }
        Ok(())
    }

    fn start_runtime(&self) -> HammerResult<WireguardRuntime> {
        #[cfg(test)]
        if self.fail_next_start.swap(false, Ordering::SeqCst) {
            return Err(HammerError::internal("injected wireguard start failure"));
        }

        let transport = transport::spawn_transport(
            self.logger.clone(),
            Arc::clone(&self.peers),
            self.listen_port,
            self.mtu,
            self.protector.clone(),
        )?;

        let encrypt_tx_clone = transport.encrypt_tx.clone();
        let TransportHandles {
            encrypt_tx,
            inbound_rx,
            local_addr,
            shutdown,
            reset_tx,
            join,
        } = transport;

        Ok(WireguardRuntime {
            transport: TransportHandles {
                encrypt_tx,
                // Sentinel — the live receiver has moved into `ip_recv_rx`.
                inbound_rx: mpsc::channel::<Bytes>(1).1,
                local_addr,
                shutdown,
                reset_tx,
                join,
            },
            encrypt_tx: encrypt_tx_clone,
            ip_recv_rx: Mutex::new(Some(inbound_rx)),
        })
    }

    fn shutdown(&self) {
        let runtime = {
            let mut inner = self.inner.lock().expect("WireguardEndpoint poisoned");
            match std::mem::replace(&mut *inner, WireguardState::Closed) {
                WireguardState::Running(rt) => Some(rt),
                _ => None,
            }
        };
        if let Some(rt) = runtime {
            stop_runtime(rt);
        }
    }

    fn restart(&self) {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        if let WireguardState::Running(rt) = &*inner {
            rt.transport.reset();
        }
    }

    #[cfg(test)]
    fn fail_next_start_for_test(&self) {
        self.fail_next_start.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        matches!(
            *self.inner.lock().expect("WireguardEndpoint poisoned"),
            WireguardState::Running(_)
        )
    }
}

fn stop_runtime(rt: WireguardRuntime) {
    let _ = rt.transport.shutdown.send(());
    rt.transport.join.abort();
    drop(rt.encrypt_tx);
    drop(rt.ip_recv_rx);
}

impl Lifecycle for WireguardEndpoint {
    fn name(&self) -> &str {
        "wireguard-endpoint"
    }

    fn start(&self, stage: StartStage) -> HammerResult<()> {
        if matches!(stage, StartStage::Start) {
            self.boot()?;
        }
        Ok(())
    }

    fn close(&self) -> HammerResult<()> {
        self.shutdown();
        Ok(())
    }
}

impl Endpoint for WireguardEndpoint {
    fn id(&self) -> &str {
        &self.id
    }

    fn ip_send_clone(&self) -> mpsc::Sender<Bytes> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt.encrypt_tx.clone(),
            // Endpoint not started yet — hand out a sentinel sender whose
            // receiver is immediately dropped, so `try_send` reports the
            // channel as closed. The TUN packet loop only resolves a route
            // to this endpoint after lifecycle Start has completed, so this
            // branch is reachable only in tests / mid-restart races.
            WireguardState::Idle | WireguardState::Closed => mpsc::channel::<Bytes>(1).0,
        }
    }

    fn ip_recv_take(&self) -> Option<mpsc::Receiver<Bytes>> {
        let inner = self.inner.lock().expect("WireguardEndpoint poisoned");
        match &*inner {
            WireguardState::Running(rt) => rt
                .ip_recv_rx
                .lock()
                .expect("ip_recv_rx mutex poisoned")
                .take(),
            _ => None,
        }
    }

    fn ip_packet_mtu(&self) -> Option<usize> {
        Some(self.mtu as usize)
    }

    fn allowed_destinations(&self) -> Vec<IpNet> {
        // Union of every peer's allowed_ips — the L3 fast path uses this to
        // build a longest-prefix table mapping packet dst IP to the WG
        // encrypt channel. Sorted longest-prefix-first so consumers doing a
        // linear walk see the most specific match first.
        let mut nets: Vec<IpNet> = self
            .peers
            .iter()
            .flat_map(|p| p.allowed_ips().iter().copied())
            .collect();
        nets.sort_by(|a, b| b.prefix_len().cmp(&a.prefix_len()));
        nets.dedup();
        nets
    }

    fn interface_addresses(&self) -> Vec<IpNet> {
        self.addresses.clone()
    }

    fn reset(&self) {
        self.restart();
    }
}

#[cfg(test)]
fn endpoint_for_test(
    logger: Logger,
    id: String,
    interface: hammer_core::config::EndpointInterfaceOptions,
    options: WireguardEndpointOptions,
) -> WireguardEndpoint {
    WireguardEndpoint::new(
        logger,
        EndpointRuntimeOptions {
            id,
            interface,
            protocol: options,
        },
        SocketProtector::default(),
    )
}

pub(crate) fn build_with_platform(
    logger: Logger,
    options: EndpointRuntimeOptions<WireguardEndpointOptions>,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> WireguardEndpoint {
    let protector = SocketProtector::from(platform);
    WireguardEndpoint::new(logger, options, protector)
}

pub(crate) fn build_endpoint_views(
    logger: Logger,
    option: &EndpointOptions,
    platform: Option<Arc<dyn PlatformInterface>>,
) -> HammerResult<Arc<WireguardEndpoint>> {
    match &option.kind {
        EndpointKind::Wireguard(options) => Ok(Arc::new(build_with_platform(
            logger,
            EndpointRuntimeOptions::from_endpoint(option, options.clone()),
            platform,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Instant;

    use boringtun::noise::TunnResult;
    use hammer_core::config::{
        EndpointInterfaceOptions, WireguardEndpointOptions, WireguardPeerOptions,
    };
    use hammer_core::log::{DiscardWriter, Factory};

    fn logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    fn x25519_public(secret: [u8; 32]) -> [u8; 32] {
        x25519::PublicKey::from(&x25519::StaticSecret::from(secret)).to_bytes()
    }

    fn test_interface(address: &str) -> EndpointInterfaceOptions {
        EndpointInterfaceOptions {
            mtu: 1408,
            address: vec![address.parse().unwrap()],
        }
    }

    fn make_endpoint(
        name: &'static str,
        my_priv: [u8; 32],
        peer_pub: [u8; 32],
    ) -> WireguardEndpoint {
        let options = WireguardEndpointOptions {
            private_key: my_priv,
            listen_port: 0,
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [0; 3],
            }],
        };
        endpoint_for_test(
            logger(name),
            name.to_owned(),
            test_interface("10.66.0.2/32"),
            options,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_keeps_existing_runtime_and_sender() {
        let a_priv = [3u8; 32];
        let b_pub = x25519_public([4u8; 32]);
        let endpoint = make_endpoint("wg-restart", a_priv, b_pub);
        endpoint.boot().expect("boot endpoint");
        assert!(endpoint.is_running());

        let before = endpoint.ip_send_clone();
        endpoint.restart();

        assert!(endpoint.is_running(), "reset must keep the endpoint live");
        before
            .try_send(Bytes::from_static(b"stable"))
            .expect("pre-reset sender must remain attached");
        endpoint.shutdown();
    }

    /// Two `WireguardEndpoint`s configured as each other's peer must complete
    /// the noise handshake, after which an IP packet sent through one comes
    /// out byte-for-byte at the other. This is the smoke test that proves the
    /// boringtun + Peer wiring is correct independently of any data path.
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

        let mut buf1 = vec![0u8; 2048];
        let init = match a_peer.lock_tunn().encapsulate(&[], &mut buf1) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: expected handshake_init, got {other:?}"),
        };

        let mut buf2 = vec![0u8; 2048];
        let response = match b_peer.lock_tunn().decapsulate(None, &init, &mut buf2) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("B: expected handshake_response, got {other:?}"),
        };

        let mut buf3 = vec![0u8; 2048];
        match a_peer.lock_tunn().decapsulate(None, &response, &mut buf3) {
            TunnResult::Done | TunnResult::WriteToNetwork(_) => {}
            other => panic!("A: handshake_response result {other:?}"),
        }

        let mut ip_packet = vec![0u8; 60];
        ip_packet[0] = 0x45;
        ip_packet[3] = 60;

        let mut enc_buf = vec![0u8; 2048];
        let encrypted = match a_peer.lock_tunn().encapsulate(&ip_packet, &mut enc_buf) {
            TunnResult::WriteToNetwork(out) => out.to_vec(),
            other => panic!("A: encapsulate {other:?}"),
        };

        let mut dec_buf = vec![0u8; 2048];
        match b_peer
            .lock_tunn()
            .decapsulate(None, &encrypted, &mut dec_buf)
        {
            TunnResult::WriteToTunnelV4(out, _src) => assert_eq!(out, ip_packet),
            other => panic!("B: decapsulate {other:?}"),
        }
    }

    #[test]
    fn route_outbound_picks_longest_prefix_peer() {
        let local_priv = [3u8; 32];
        let peer1_pub = x25519_public([4u8; 32]);
        let peer2_pub = x25519_public([5u8; 32]);
        let opts = WireguardEndpointOptions {
            private_key: local_priv,
            listen_port: 0,
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
        let ep = endpoint_for_test(
            logger("multi"),
            "multi".to_owned(),
            test_interface("10.66.0.2/32"),
            opts,
        );

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
            peers: vec![WireguardPeerOptions {
                public_key: peer_pub,
                pre_shared_key: None,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 51820),
                allowed_ips: vec!["0.0.0.0/0".parse().unwrap()],
                persistent_keepalive: None,
                reserved: [1, 2, 3],
            }],
        };
        let ep = endpoint_for_test(
            logger("buf"),
            "buf".to_owned(),
            test_interface("10.66.0.2/32"),
            opts,
        );
        assert_eq!(
            ep.encapsulate_buffer().len(),
            ep.mtu() as usize + WIREGUARD_OVERHEAD
        );
        assert_eq!(ep.peers()[0].reserved(), [1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn endpoint_reset_preserves_packet_channels() {
        let priv_key = [12u8; 32];
        let peer_pub = x25519_public([13u8; 32]);
        let ep = make_endpoint("wg-reset", priv_key, peer_pub);

        ep.boot().expect("boot endpoint");
        assert!(ep.is_running());

        let before = ep.ip_send_clone();
        let inbound = ep.ip_recv_take().expect("first inbound receiver");
        ep.restart();
        assert!(ep.is_running(), "reset must keep the endpoint live");

        before
            .try_send(Bytes::from_static(b"still-live"))
            .expect("pre-reset sender must remain usable");
        assert!(
            ep.ip_recv_take().is_none(),
            "reset must not swap in an unattached inbound receiver"
        );
        drop(inbound);

        let after = ep.ip_send_clone();
        after
            .try_send(Bytes::from_static(b"fresh"))
            .expect("post-reset sender must accept a packet");

        ep.shutdown();
    }
}
