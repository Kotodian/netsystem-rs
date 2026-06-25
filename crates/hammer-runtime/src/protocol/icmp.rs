use std::net::IpAddr;
#[cfg(test)]
use std::net::Ipv4Addr;
#[cfg(test)]
use std::time::{Duration, Instant};

use bytes::Bytes;
use hammer_adapter::IcmpReply;
#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use hammer_adapter::ProxyIcmpConn;
#[cfg(test)]
use hammer_core::error::{HammerError, HammerResult};
#[cfg(test)]
use tokio::time::timeout;

#[cfg(test)]
async fn measure_echo_with_token(
    conn: &mut dyn ProxyIcmpConn,
    destination: IpAddr,
    sequence: u16,
    token: &[u8],
    timeout_duration: Duration,
) -> HammerResult<Duration> {
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn normalize_received_icmp_body(source: IpAddr, packet: &[u8]) -> &[u8] {
    match source {
        IpAddr::V4(_) => strip_ipv4_header(packet).unwrap_or(packet),
        IpAddr::V6(_) => strip_ipv6_header(packet).unwrap_or(packet),
    }
}

#[cfg(test)]
fn strip_ipv4_header(packet: &[u8]) -> Option<&[u8]> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    let header_len = usize::from(packet[0] & 0x0f) * 4;
    if header_len < 20 || packet.len() < header_len || packet[9] != 1 {
        return None;
    }
    Some(&packet[header_len..])
}

#[cfg(test)]
fn strip_ipv6_header(packet: &[u8]) -> Option<&[u8]> {
    if packet.len() < 40 || packet[0] >> 4 != 6 || packet[6] != 58 {
        return None;
    }
    Some(&packet[40..])
}

#[cfg(test)]
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

    #[test]
    fn received_ipv4_packet_is_normalized_to_icmp_body() {
        let icmp_body = b"\0\0\0\0\0\0\0\x07probe-token";
        let mut packet = vec![
            0x45, 0, 0, 0, 0, 0, 0, 0, 64, 1, 0, 0, 127, 0, 0, 1, 127, 0, 0, 1,
        ];
        packet.extend_from_slice(icmp_body);

        assert_eq!(
            normalize_received_icmp_body(IpAddr::V4(Ipv4Addr::LOCALHOST), &packet),
            icmp_body
        );
    }

    #[test]
    fn received_linux_icmp_body_is_left_unchanged() {
        let icmp_body = b"\0\0\0\0\0\0\0\x07probe-token";

        assert_eq!(
            normalize_received_icmp_body(IpAddr::V4(Ipv4Addr::LOCALHOST), icmp_body),
            icmp_body
        );
    }

    struct FakeIcmpConn {
        replies: VecDeque<IcmpReply>,
        sent: Vec<(IpAddr, Vec<u8>)>,
    }

    #[async_trait]
    impl ProxyIcmpConn for FakeIcmpConn {
        async fn send_echo(&mut self, destination: IpAddr, body: &[u8]) -> HammerResult<()> {
            self.sent.push((destination, body.to_vec()));
            Ok(())
        }

        async fn recv_reply(&mut self) -> HammerResult<IcmpReply> {
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
