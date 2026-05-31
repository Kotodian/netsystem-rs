use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::protocol::ip::{
    IpFragmentKey, parse_ip_fragment, parse_ip_fragment_with_chain_len,
    parse_ip_packet_with_chain_len,
};

#[test]
fn parse_ipv4_fragment_accepts_payload_spanning_buffer_chain() {
    let packet = ipv4_fragment(
        &ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"payload-spans-chain"),
        100,
        0,
        16,
        true,
    );
    let first = &packet[..20];
    assert!(parse_ip_fragment(first).is_err());

    let parsed = parse_ip_fragment_with_chain_len(first, packet.len() - first.len())
        .expect("parse chained IPv4 fragment");

    assert_eq!(parsed.payload_offset, 0);
    assert_eq!(parsed.payload_len, packet.len() - 20);
    assert!(parsed.more_fragments);
    assert_eq!(
        parsed.key,
        IpFragmentKey::V4 {
            source: Ipv4Addr::new(10, 0, 0, 2),
            destination: Ipv4Addr::new(198, 51, 100, 7),
            protocol: 17,
            identification: 100,
        }
    );
}

#[test]
fn parse_ipv6_fragment_accepts_payload_spanning_buffer_chain() {
    let packet = ipv6_fragment(
        &ipv6_udp_packet(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1),
            Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2),
            b"payload-spans-chain",
        ),
        0x0102_0304,
        0,
        16,
        true,
    );
    let first = &packet[..48];
    assert!(parse_ip_fragment(first).is_err());

    let parsed = parse_ip_fragment_with_chain_len(first, packet.len() - first.len())
        .expect("parse chained IPv6 fragment");

    assert_eq!(parsed.payload_offset, 0);
    assert_eq!(parsed.payload_len, packet.len() - 48);
    assert!(parsed.more_fragments);
    assert_eq!(
        parsed.key,
        IpFragmentKey::V6 {
            source: Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1),
            destination: Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2),
            next_header: 17,
            identification: 0x0102_0304,
        }
    );
}

#[test]
fn parse_ipv6_packet_rejects_short_chained_packet() {
    let packet = ipv6_udp_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1),
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2),
        b"payload",
    );

    assert!(parse_ip_packet_with_chain_len(&packet[..40], packet.len() - 41).is_err());
}

fn ipv4_udp_packet(source: [u8; 4], destination: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..22].copy_from_slice(&12345u16.to_be_bytes());
    packet[22..24].copy_from_slice(&53u16.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_fragment(
    packet: &[u8],
    identification: u16,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    let mut fragment = Vec::with_capacity(20 + payload_len);
    fragment.extend_from_slice(&packet[..20]);
    fragment.extend_from_slice(&packet[20 + payload_offset..20 + payload_offset + payload_len]);
    let fragment_len = fragment.len() as u16;
    fragment[2..4].copy_from_slice(&fragment_len.to_be_bytes());
    fragment[4..6].copy_from_slice(&identification.to_be_bytes());
    let flags_offset = ((payload_offset / 8) as u16) | if more_fragments { 0x2000 } else { 0 };
    fragment[6..8].copy_from_slice(&flags_offset.to_be_bytes());
    update_ipv4_checksum(&mut fragment);
    fragment
}

fn ipv6_udp_packet(source: Ipv6Addr, destination: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
    let payload_len = 8 + payload.len();
    let mut packet = vec![0; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 17;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40..42].copy_from_slice(&12345u16.to_be_bytes());
    packet[42..44].copy_from_slice(&53u16.to_be_bytes());
    packet[44..46].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[48..].copy_from_slice(payload);
    packet
}

fn ipv6_fragment(
    packet: &[u8],
    identification: u32,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    let mut fragment = Vec::with_capacity(48 + payload_len);
    fragment.extend_from_slice(&packet[..40]);
    fragment[6] = 44;
    fragment.extend_from_slice(&[
        packet[6],
        0,
        0,
        0,
        (identification >> 24) as u8,
        (identification >> 16) as u8,
        (identification >> 8) as u8,
        identification as u8,
    ]);
    let mut offset_more = ((payload_offset / 8) as u16) << 3;
    if more_fragments {
        offset_more |= 1;
    }
    fragment[42..44].copy_from_slice(&offset_more.to_be_bytes());
    fragment.extend_from_slice(&packet[40 + payload_offset..40 + payload_offset + payload_len]);
    fragment[4..6].copy_from_slice(&((8 + payload_len) as u16).to_be_bytes());
    fragment
}

fn update_ipv4_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}
