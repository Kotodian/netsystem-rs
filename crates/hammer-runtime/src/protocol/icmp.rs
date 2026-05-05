use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use hammer_adapter::{IcmpReply, ProxyIcmpConn};
use hammer_core::error::HammerError;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::socket_protector::SocketProtector;

const PROBE_TOKEN_PREFIX: &[u8] = b"hammer-icmp-probe";

static NEXT_SEQUENCE: AtomicU16 = AtomicU16::new(1);
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

/// ICMP echo conduit backed by unprivileged ping sockets.
///
/// `SOCK_DGRAM, IPPROTO_ICMP{,V6}` sockets let the kernel own the outer IP
/// header and the socket-local ICMP identifier. Callers should not rely on the
/// identifier they put into an echo request being present in the reply.
pub(crate) struct IcmpSocketConn {
    protector: SocketProtector,
    ipv4: Option<UdpSocket>,
    ipv6: Option<UdpSocket>,
}

impl IcmpSocketConn {
    pub(crate) fn new(protector: SocketProtector) -> Self {
        Self {
            protector,
            ipv4: None,
            ipv6: None,
        }
    }

    fn socket_for(&mut self, destination: IpAddr) -> Result<&UdpSocket, HammerError> {
        if destination.is_ipv6() {
            if self.ipv6.is_none() {
                self.ipv6 = Some(bind_icmp_socket(
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                    &self.protector,
                )?);
            }
            return Ok(self
                .ipv6
                .as_ref()
                .expect("ICMP IPv6 socket just initialized"));
        }
        if self.ipv4.is_none() {
            self.ipv4 = Some(bind_icmp_socket(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                &self.protector,
            )?);
        }
        Ok(self
            .ipv4
            .as_ref()
            .expect("ICMP IPv4 socket just initialized"))
    }
}

#[async_trait]
impl ProxyIcmpConn for IcmpSocketConn {
    async fn send_echo(&mut self, destination: IpAddr, body: &[u8]) -> Result<(), HammerError> {
        let target = SocketAddr::new(destination, 0);
        self.socket_for(destination)?
            .send_to(body, target)
            .await
            .map_err(|err| HammerError::internal(format!("icmp send: {err}")))?;
        Ok(())
    }

    async fn recv_reply(&mut self) -> Result<IcmpReply, HammerError> {
        match (self.ipv4.as_mut(), self.ipv6.as_mut()) {
            (Some(ipv4), Some(ipv6)) => {
                let mut v4 = vec![0_u8; 64 * 1024];
                let mut v6 = vec![0_u8; 64 * 1024];
                tokio::select! {
                    res = ipv4.recv_from(&mut v4) => icmp_reply_from_recv(res, v4),
                    res = ipv6.recv_from(&mut v6) => icmp_reply_from_recv(res, v6),
                }
            }
            (Some(ipv4), None) => {
                let mut buf = vec![0_u8; 64 * 1024];
                icmp_reply_from_recv(ipv4.recv_from(&mut buf).await, buf)
            }
            (None, Some(ipv6)) => {
                let mut buf = vec![0_u8; 64 * 1024];
                icmp_reply_from_recv(ipv6.recv_from(&mut buf).await, buf)
            }
            (None, None) => Err(HammerError::internal(
                "icmp recv before any socket is opened",
            )),
        }
    }
}

pub(crate) async fn probe_echo(
    destination: IpAddr,
    timeout_duration: Duration,
    protector: SocketProtector,
) -> Result<Duration, HammerError> {
    let mut conn = IcmpSocketConn::new(protector);
    measure_echo(&mut conn, destination, timeout_duration).await
}

async fn measure_echo(
    conn: &mut dyn ProxyIcmpConn,
    destination: IpAddr,
    timeout_duration: Duration,
) -> Result<Duration, HammerError> {
    let sequence = NEXT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let token = next_probe_token();
    measure_echo_with_token(conn, destination, sequence, &token, timeout_duration).await
}

async fn measure_echo_with_token(
    conn: &mut dyn ProxyIcmpConn,
    destination: IpAddr,
    sequence: u16,
    token: &[u8],
    timeout_duration: Duration,
) -> Result<Duration, HammerError> {
    let request = echo_request_body(destination, sequence, token);
    let started = Instant::now();
    conn.send_echo(destination, &request).await?;

    let wait = async {
        loop {
            let reply = conn.recv_reply().await?;
            if echo_reply_matches(destination, sequence, token, &reply) {
                return Ok(started.elapsed());
            }
        }
    };

    match timeout(timeout_duration, wait).await {
        Ok(result) => result,
        Err(_) => Err(HammerError::internal(format!(
            "icmp probe timed out after {timeout_duration:?}"
        ))),
    }
}

fn next_probe_token() -> Vec<u8> {
    let id = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
    let mut token = Vec::with_capacity(PROBE_TOKEN_PREFIX.len() + 8);
    token.extend_from_slice(PROBE_TOKEN_PREFIX);
    token.extend_from_slice(&id.to_be_bytes());
    token
}

fn echo_request_body(destination: IpAddr, sequence: u16, payload: &[u8]) -> Vec<u8> {
    let request_type = if destination.is_ipv6() { 128 } else { 8 };
    let mut body = Vec::with_capacity(8 + payload.len());
    body.push(request_type);
    body.push(0);
    body.extend_from_slice(&0_u16.to_be_bytes());
    body.extend_from_slice(&0_u16.to_be_bytes());
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(payload);
    if destination.is_ipv4() {
        let checksum = checksum(&body);
        body[2..4].copy_from_slice(&checksum.to_be_bytes());
    }
    body
}

fn echo_reply_matches(
    destination: IpAddr,
    sequence: u16,
    payload: &[u8],
    reply: &IcmpReply,
) -> bool {
    if reply.source != destination || reply.body.len() < 8 {
        return false;
    }
    let expected_type = if destination.is_ipv6() { 129 } else { 0 };
    reply.body[0] == expected_type
        && reply.body[1] == 0
        && u16::from_be_bytes([reply.body[6], reply.body[7]]) == sequence
        && &reply.body[8..] == payload
}

fn bind_icmp_socket(
    bind_ip: IpAddr,
    protector: &SocketProtector,
) -> Result<UdpSocket, HammerError> {
    let (domain, protocol) = match bind_ip {
        IpAddr::V4(_) => (Domain::IPV4, Protocol::ICMPV4),
        IpAddr::V6(_) => (Domain::IPV6, Protocol::ICMPV6),
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(protocol))
        .map_err(|err| HammerError::internal(format!("icmp socket {bind_ip}: {err}")))?;
    if matches!(bind_ip, IpAddr::V6(_)) {
        socket
            .set_only_v6(true)
            .map_err(|err| HammerError::internal(format!("icmp set_only_v6: {err}")))?;
    }
    socket
        .bind(&SocketAddr::new(bind_ip, 0).into())
        .map_err(|err| HammerError::internal(format!("icmp bind {bind_ip}: {err}")))?;
    let std_socket: std::net::UdpSocket = socket.into();
    std_socket
        .set_nonblocking(true)
        .map_err(|err| HammerError::internal(format!("icmp set_nonblocking: {err}")))?;
    let socket = UdpSocket::from_std(std_socket)
        .map_err(|err| HammerError::internal(format!("icmp from_std: {err}")))?;
    protector.protect(&socket)?;
    Ok(socket)
}

fn icmp_reply_from_recv(
    result: std::io::Result<(usize, SocketAddr)>,
    mut buf: Vec<u8>,
) -> Result<IcmpReply, HammerError> {
    let (len, source) = result.map_err(|err| HammerError::internal(format!("icmp recv: {err}")))?;
    buf.truncate(len);
    Ok(IcmpReply {
        source: source.ip(),
        body: Bytes::from(buf),
    })
}

fn checksum(data: &[u8]) -> u16 {
    let mut sum = 0u32;
    let mut chunks = data.chunks_exact(2);
    for chunk in &mut chunks {
        sum = sum.wrapping_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
    }
    if let Some(&byte) = chunks.remainder().first() {
        sum = sum.wrapping_add((byte as u32) << 8);
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[test]
    fn echo_reply_match_ignores_kernel_rewritten_identifier() {
        let destination = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let sequence = 7_u16;
        let payload = b"probe-token";
        let mut reply_body = vec![0, 0, 0, 0, 0x12, 0x34];
        reply_body.extend_from_slice(&sequence.to_be_bytes());
        reply_body.extend_from_slice(payload);
        let reply = IcmpReply {
            source: destination,
            body: Bytes::from(reply_body),
        };

        assert!(echo_reply_matches(destination, sequence, payload, &reply));
    }

    #[test]
    fn echo_reply_rejects_unrelated_payload() {
        let destination = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let sequence = 7_u16;
        let reply = IcmpReply {
            source: destination,
            body: Bytes::from_static(b"\0\0\0\0\0\0\0\x07other"),
        };

        assert!(!echo_reply_matches(
            destination,
            sequence,
            b"probe-token",
            &reply
        ));
    }

    struct FakeIcmpConn {
        replies: VecDeque<IcmpReply>,
        sent: Vec<(IpAddr, Vec<u8>)>,
    }

    #[async_trait]
    impl ProxyIcmpConn for FakeIcmpConn {
        async fn send_echo(&mut self, destination: IpAddr, body: &[u8]) -> Result<(), HammerError> {
            self.sent.push((destination, body.to_vec()));
            Ok(())
        }

        async fn recv_reply(&mut self) -> Result<IcmpReply, HammerError> {
            self.replies
                .pop_front()
                .ok_or_else(|| HammerError::internal("no reply"))
        }
    }

    #[tokio::test]
    async fn measure_echo_waits_for_matching_reply() {
        let destination = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let payload = b"probe-token";
        let sequence = 7_u16;
        let unrelated = IcmpReply {
            source: destination,
            body: Bytes::from_static(b"\0\0\0\0\0\0\0\x01wrong"),
        };
        let mut matching = vec![0, 0, 0, 0, 0xaa, 0xbb];
        matching.extend_from_slice(&sequence.to_be_bytes());
        matching.extend_from_slice(payload);
        let mut conn = FakeIcmpConn {
            replies: VecDeque::from([
                unrelated,
                IcmpReply {
                    source: destination,
                    body: Bytes::from(matching),
                },
            ]),
            sent: Vec::new(),
        };

        let elapsed = measure_echo_with_token(
            &mut conn,
            destination,
            sequence,
            payload,
            Duration::from_secs(1),
        )
        .await
        .expect("matching reply");

        assert!(elapsed <= Duration::from_secs(1));
        assert_eq!(conn.sent.len(), 1);
        assert_eq!(conn.sent[0].0, destination);
    }
}
