use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};

use crate::error::{CoreError, CoreResult};

use super::TcpControlFlags;

pub fn synthesize_ipv4_tcp_control(
    local: SocketAddr,
    remote: SocketAddr,
    send_sequence: u32,
    receive_acknowledgment: u32,
    window: u16,
    flags: TcpControlFlags,
    options: &[u8],
) -> CoreResult<std::vec::Vec<u8>> {
    let (local_ip, remote_ip) = match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => (local_ip, remote_ip),
        _ => return Err(CoreError::internal("ipv4 tcp control requires IPv4 addrs")),
    };
    if options.len() % 4 != 0 {
        return Err(CoreError::internal("tcp control options must be aligned"));
    }

    let tcp_header_len = 20usize
        .checked_add(options.len())
        .ok_or_else(|| CoreError::internal("tcp control header length overflow"))?;
    if tcp_header_len > 60 {
        return Err(CoreError::internal("tcp control header too large"));
    }
    let packet_len = 20usize
        .checked_add(tcp_header_len)
        .ok_or_else(|| CoreError::internal("tcp control packet length overflow"))?;
    let total_len = u16::try_from(packet_len)
        .map_err(|_| CoreError::internal("tcp control packet too large"))?;

    let mut packet = vec![0u8; packet_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&local_ip.octets());
    packet[16..20].copy_from_slice(&remote_ip.octets());

    let tcp = &mut packet[20..];
    tcp[0..2].copy_from_slice(&local.port().to_be_bytes());
    tcp[2..4].copy_from_slice(&remote.port().to_be_bytes());
    tcp[4..8].copy_from_slice(&send_sequence.to_be_bytes());
    tcp[8..12].copy_from_slice(&receive_acknowledgment.to_be_bytes());
    tcp[12] = ((tcp_header_len / 4) as u8) << 4;
    tcp[13] = flags.bits();
    tcp[14..16].copy_from_slice(&window.to_be_bytes());
    if !options.is_empty() {
        tcp[20..20 + options.len()].copy_from_slice(options);
    }

    let checksum = ipv4_l4_checksum(local_ip, remote_ip, 6, &packet[20..]);
    packet[36..38].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    Ok(packet)
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source.octets(),
        &destination.octets(),
        &[0, protocol],
        &(segment.len() as u16).to_be_bytes(),
        segment,
    ])
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}
