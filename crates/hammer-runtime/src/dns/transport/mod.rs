#[cfg(feature = "dns-https")]
pub mod doh;
#[cfg(feature = "dns-tcp")]
pub mod tcp;
#[cfg(feature = "dns-udp")]
pub mod udp;

#[cfg(feature = "dns-https")]
pub use doh::HttpsDnsTransport;
#[cfg(feature = "dns-tcp")]
pub use tcp::TcpDnsTransport;
#[cfg(feature = "dns-udp")]
pub use udp::UdpDnsTransport;

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
use std::net::{IpAddr, SocketAddr};
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
use std::sync::Arc;
#[cfg(feature = "dns-udp")]
use std::time::Duration;

#[cfg(feature = "dns-udp")]
use bytes::Bytes;
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
use hammer_adapter::{OutboundManager as _, SocksAddr};
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
use hammer_core::error::HammerError;
#[cfg(any(feature = "dns-udp", feature = "dns-tcp"))]
use hickory_proto::op::Message;
#[cfg(feature = "dns-tcp")]
use tokio::io::AsyncReadExt;
#[cfg(any(feature = "dns-tcp", feature = "dns-https"))]
use tokio::net::{TcpSocket, TcpStream};
#[cfg(feature = "dns-udp")]
use tokio::time::timeout;
#[cfg(feature = "dns-udp")]
use tracing::debug;

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
use crate::OutboundManager;
#[cfg(any(feature = "dns-udp", feature = "dns-tcp"))]
use crate::dns::MessageExt;
#[cfg(any(feature = "dns-tcp", feature = "dns-https"))]
use crate::socket_protector::SocketProtector;

#[cfg(feature = "dns-udp")]
const DNS_UDP_VIA_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
pub(super) async fn resolve_first(server: &str, port: u16) -> Result<SocketAddr, HammerError> {
    if let Ok(ip) = server.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let mut addrs = tokio::net::lookup_host((server, port))
        .await
        .map_err(|e| HammerError::internal(format!("resolve DNS server {server}: {e}")))?;
    addrs
        .next()
        .ok_or_else(|| HammerError::internal(format!("resolve DNS server {server}: empty result")))
}

// Subsumed by `destination_via_bootstrap`; left in place for direct callers
// that don't need bootstrap-aware resolution.
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
#[allow(dead_code)]
pub(super) fn server_destination(server: &str, port: u16) -> SocksAddr {
    if let Ok(ip) = server.parse::<IpAddr>() {
        SocksAddr::ip(ip, port)
    } else {
        SocksAddr::domain(server, IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port)
    }
}

/// Resolve `server` into a `SocksAddr` suitable for handing to an outbound's
/// dial / packet send. IP literal → returned directly. Domain → if a
/// `bootstrap` DNS transport is configured, query it for an A record and
/// return an IP-resolved `SocksAddr`. With no bootstrap, fall back to a
/// domain `SocksAddr` (host field unspecified) — direct outbounds can still
/// resolve domains themselves; outbounds that require IPs (wireguard) will
/// reject it, but the config validator should already have caught that case
/// at startup for via-routed domain servers.
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
pub(super) async fn destination_via_bootstrap(
    bootstrap: Option<&Arc<dyn hammer_adapter::DnsTransport>>,
    server: &str,
    port: u16,
) -> Result<SocksAddr, HammerError> {
    if let Ok(ip) = server.parse::<IpAddr>() {
        return Ok(SocksAddr::ip(ip, port));
    }
    if let Some(bootstrap) = bootstrap {
        let ip = lookup_first_a(bootstrap, server).await?;
        return Ok(SocksAddr::ip(ip, port));
    }
    Ok(SocksAddr::domain(
        server,
        IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        port,
    ))
}

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
async fn lookup_first_a(
    bootstrap: &Arc<dyn hammer_adapter::DnsTransport>,
    host: &str,
) -> Result<IpAddr, HammerError> {
    use crate::dns::{MessageExt, query_message};
    use hickory_proto::rr::RecordType;
    let msg = query_message(host, RecordType::A)?;
    let response = bootstrap.exchange(msg).await?;
    response.addresses().into_iter().next().ok_or_else(|| {
        HammerError::internal(format!(
            "bootstrap '{}' returned no A record for {host}",
            bootstrap.id()
        ))
    })
}

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
pub(super) fn dependency(via: &str) -> Vec<String> {
    if via.is_empty() {
        Vec::new()
    } else {
        vec![via.to_owned()]
    }
}

/// Combined dependency list including the via outbound and an optional
/// bootstrap DNS server tag. Either or both may be empty.
#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
pub(super) fn dependency_with_bootstrap(via: &str, bootstrap_tag: &str) -> Vec<String> {
    let mut deps = dependency(via);
    if !bootstrap_tag.is_empty() {
        deps.push(bootstrap_tag.to_owned());
    }
    deps
}

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
#[cfg(any(feature = "dns-tcp", feature = "dns-https"))]
pub(super) fn socks_to_socket_addr(destination: &SocksAddr) -> Option<SocketAddr> {
    destination
        .domain
        .is_none()
        .then_some(SocketAddr::new(destination.host, destination.port))
}

#[cfg(any(feature = "dns-udp", feature = "dns-tcp", feature = "dns-https"))]
pub(super) fn outbound_by_id(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
) -> Result<Arc<dyn hammer_adapter::Outbound>, HammerError> {
    let Some(manager) = outbound else {
        return Err(HammerError::internal(format!(
            "outbound via not configured: {via}"
        )));
    };
    manager
        .get(via)
        .ok_or_else(|| HammerError::internal(format!("outbound via not found: {via}")))
}

#[cfg(any(feature = "dns-tcp", feature = "dns-https"))]
pub(super) async fn direct_tcp_connect(
    server: SocketAddr,
    protector: &SocketProtector,
) -> Result<TcpStream, HammerError> {
    let socket = if server.is_ipv6() {
        TcpSocket::new_v6()
    } else {
        TcpSocket::new_v4()
    }
    .map_err(|e| HammerError::internal(format!("create TCP DNS socket: {e}")))?;
    protector.protect(&socket)?;
    socket
        .connect(server)
        .await
        .map_err(|e| HammerError::internal(format!("connect TCP DNS socket: {e}")))
}

#[cfg(feature = "dns-udp")]
pub(super) async fn udp_exchange_via(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
    destination: SocksAddr,
    payload: &[u8],
) -> Result<Bytes, HammerError> {
    udp_exchange_via_with_timeout(
        outbound,
        via,
        destination,
        payload,
        DNS_UDP_VIA_RESPONSE_TIMEOUT,
    )
    .await
}

#[cfg(feature = "dns-udp")]
async fn udp_exchange_via_with_timeout(
    outbound: Option<&Arc<OutboundManager>>,
    via: &str,
    destination: SocksAddr,
    payload: &[u8],
    response_timeout: Duration,
) -> Result<Bytes, HammerError> {
    debug!(
        "dns udp via outbound={via} dest={destination} bytes={}",
        payload.len()
    );
    let outbound_item = outbound_by_id(outbound, via)?;
    let mut conn = outbound_item.listen_packet().await?;
    debug!("dns udp via outbound={via} packet conn ready");
    conn.send_to(destination.clone(), payload).await?;
    debug!("dns udp via outbound={via} sent dest={destination}");
    let response = match timeout(response_timeout, conn.recv_from()).await {
        Ok(response) => response?,
        Err(_) => {
            debug!(
                "dns udp via timeout outbound={via} dest={destination} elapsed={response_timeout:?}"
            );
            outbound_item.reset();
            return Err(HammerError::internal(format!(
                "dns udp via timed out after {response_timeout:?}: outbound={via} dest={destination}"
            )));
        }
    };
    debug!(
        "dns udp via outbound={via} received from {} bytes={}",
        response.destination,
        response.payload.len()
    );
    Ok(response.payload)
}

#[cfg(feature = "dns-tcp")]
pub(super) fn encode_tcp_dns_query(message: &Message) -> Result<Vec<u8>, HammerError> {
    let bytes = MessageExt::to_bytes(message)?;
    let len = u16::try_from(bytes.len())
        .map_err(|_| HammerError::internal("DNS message exceeds TCP frame limit"))?;
    let mut frame = Vec::with_capacity(bytes.len() + 2);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}

#[cfg(feature = "dns-tcp")]
async fn read_tcp_dns_response<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<Message, HammerError> {
    let mut len_buf = [0_u8; 2];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS length: {e}")))?;
    let len = usize::from(u16::from_be_bytes(len_buf));
    let mut payload = vec![0_u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| HammerError::internal(format!("read TCP DNS response: {e}")))?;
    <Message as MessageExt>::from_bytes(&payload)
}

#[cfg(feature = "dns-https")]
pub(super) fn host_header(server: &str, port: u16) -> String {
    if port == 443 {
        server.to_owned()
    } else {
        format!("{server}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use hammer_adapter::{Network, Outbound, ProxyDatagram, ProxyPacketConn, ProxyStream};
    use hammer_core::log::{DiscardWriter, Factory, Logger};

    fn logger(id: &str) -> Logger {
        Factory::new(Instant::now(), Arc::new(DiscardWriter)).new_logger(id)
    }

    #[derive(Default)]
    struct HangingOutbound {
        resets: AtomicUsize,
    }

    #[async_trait]
    impl Outbound for HangingOutbound {
        fn type_name(&self) -> &str {
            "hanging"
        }

        fn id(&self) -> &str {
            "proxy"
        }

        fn networks(&self) -> &[Network] {
            &[Network::Udp]
        }

        fn dependencies(&self) -> &[String] {
            &[]
        }

        fn reset(&self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }

        async fn dial(
            &self,
            _network: Network,
            _destination: SocksAddr,
            _initial_payload: &[u8],
        ) -> Result<Box<dyn ProxyStream>, HammerError> {
            Err(HammerError::internal("hanging outbound does not dial"))
        }

        async fn listen_packet(&self) -> Result<Box<dyn ProxyPacketConn>, HammerError> {
            Ok(Box::new(HangingPacketConn))
        }
    }

    struct HangingPacketConn;

    #[async_trait]
    impl ProxyPacketConn for HangingPacketConn {
        async fn send_to(
            &mut self,
            _destination: SocksAddr,
            _payload: &[u8],
        ) -> Result<(), HammerError> {
            Ok(())
        }

        async fn recv_from(&mut self) -> Result<ProxyDatagram, HammerError> {
            future::pending::<()>().await;
            unreachable!("pending future never returns")
        }
    }

    #[tokio::test]
    async fn udp_exchange_via_times_out_and_resets_outbound() {
        let manager = Arc::new(crate::OutboundManager::new(logger("outbound"), "proxy"));
        let outbound = Arc::new(HangingOutbound::default());
        manager
            .register_outbound("proxy".to_owned(), outbound.clone() as Arc<dyn Outbound>)
            .expect("register outbound");

        let err = udp_exchange_via_with_timeout(
            Some(&manager),
            "proxy",
            SocksAddr::ip(IpAddr::V4(std::net::Ipv4Addr::new(1, 1, 1, 1)), 53),
            b"query",
            Duration::from_millis(50),
        )
        .await
        .expect_err("hung UDP DNS exchange should time out");

        assert!(err.to_string().contains("dns udp via timed out"));
        assert_eq!(outbound.resets.load(Ordering::SeqCst), 1);
    }
}
