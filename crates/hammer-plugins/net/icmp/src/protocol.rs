use hammer_infra::checksum::internet_checksum;
use hammer_plugin_ip::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};
use hammer_plugin_ip::protocol::wire::read_header;

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}

impl IcmpHeader {
    #[inline(always)]
    pub const fn icmp_type(self) -> u8 {
        self.icmp_type
    }

    #[inline(always)]
    pub const fn code(self) -> u8 {
        self.code
    }

    #[inline(always)]
    pub fn checksum(self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpBuildError {
    BadLength,
    WrongProtocol,
    WrongType,
    BadCode,
}

/// IP local has already validated the complete message checksum. Only the
/// first buffer's headers change; payload and chain ownership remain untouched.
pub fn build_echo_reply(packet: &mut [u8], parsed: &ParsedIpPacket) -> Result<(), IcmpBuildError> {
    let ip_offset = parsed.network_header_offset;
    let icmp_offset = parsed.transport_header_offset;
    let (request, reply, ip_length) = match (parsed.version, parsed.protocol) {
        (IpVersion::V4, IpProtocol::Icmpv4) => (8, 0, parsed.network_header_len),
        (IpVersion::V6, IpProtocol::Icmpv6) => (128, 129, 40),
        _ => return Err(IcmpBuildError::WrongProtocol),
    };
    if packet
        .get(ip_offset..ip_offset.saturating_add(ip_length))
        .is_none()
        || ip_length
            < match parsed.version {
                IpVersion::V4 => 20,
                IpVersion::V6 => 40,
            }
        || packet
            .get(icmp_offset..icmp_offset.saturating_add(8))
            .is_none()
    {
        return Err(IcmpBuildError::BadLength);
    }
    let header =
        read_header::<IcmpHeader>(packet, icmp_offset).map_err(|_| IcmpBuildError::BadLength)?;
    if header.icmp_type() != request {
        return Err(IcmpBuildError::WrongType);
    }
    if header.code() != 0 {
        return Err(IcmpBuildError::BadCode);
    }

    // RFC 1624: replace the type/code word, retaining the payload contribution.
    // Swapping IPv6 addresses does not change the pseudo-header checksum sum.
    let mut sum = u32::from(!header.checksum())
        + u32::from(!u16::from_be_bytes([request, 0]))
        + u32::from(u16::from_be_bytes([reply, 0]));
    sum = (sum & 0xffff) + (sum >> 16);
    sum = (sum & 0xffff) + (sum >> 16);
    packet[icmp_offset] = reply;
    packet[icmp_offset + 2..icmp_offset + 4].copy_from_slice(&(!(sum as u16)).to_be_bytes());

    let ip = &mut packet[ip_offset..ip_offset + ip_length];
    match parsed.version {
        IpVersion::V4 => {
            ip[12..20].rotate_left(4);
            ip[8] = 64;
            ip[10..12].fill(0);
            let checksum = internet_checksum(ip);
            ip[10..12].copy_from_slice(&checksum.to_be_bytes());
        }
        IpVersion::V6 => {
            ip[8..40].rotate_left(16);
            ip[7] = 64;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_core::data_plane::BufferPacketCursor;
    use hammer_infra::checksum::internet_checksum_parts;
    use hammer_plugin_ip::ip::ip_header;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn ipv4_echo_reply_preserves_identifier_sequence_and_payload() {
        // test_ip4.py::TestICMPEcho.test_icmp_echo, including its payload.
        let source = Ipv4Addr::new(192, 0, 2, 2).octets();
        let destination = Ipv4Addr::new(192, 0, 2, 1).octets();
        let mut request = [0x0a; 46];
        request[..20].fill(0);
        request[0] = 0x45;
        request[2..4].copy_from_slice(&46u16.to_be_bytes());
        request[8] = 32;
        request[9] = IpProtocol::Icmpv4.into();
        request[12..16].copy_from_slice(&source);
        request[16..20].copy_from_slice(&destination);
        request[20..28].copy_from_slice(&[8, 0, 0, 0, 0, 0x0b, 0, 5]);
        let checksum = internet_checksum(&request[20..]);
        request[22..24].copy_from_slice(&checksum.to_be_bytes());
        let checksum = internet_checksum(&request[..20]);
        request[10..12].copy_from_slice(&checksum.to_be_bytes());
        let parsed = ip_header(
            &request,
            BufferPacketCursor::new()
                .with_packet_len(request.len())
                .with_network_header(0, 20)
                .with_transport_header(20, 8),
        )
        .unwrap();

        for first_segment_len in [request.len(), 28] {
            let mut reply = request;
            build_echo_reply(&mut reply[..first_segment_len], &parsed).unwrap();
            assert_eq!(&reply[12..16], &destination);
            assert_eq!(&reply[16..20], &source);
            assert_eq!(&reply[20..22], &[0, 0]);
            assert_eq!(&reply[24..28], &[0, 0x0b, 0, 5]);
            assert_eq!(&reply[28..], &request[28..]);
            assert_eq!(internet_checksum(&reply[..20]), 0);
            assert_eq!(internet_checksum(&reply[20..]), 0);
        }
    }

    #[test]
    fn ipv6_echo_reply_preserves_message_for_global_and_link_local_destinations() {
        // test_ip6.py::TestICMPv6Echo.test_icmpv6_echo sends a global source
        // to both global and link-local destinations on the same interface.
        let source = "2001:db8::2".parse::<Ipv6Addr>().unwrap().octets();
        for destination in ["2001:db8::1", "fe80::1"] {
            let destination = destination.parse::<Ipv6Addr>().unwrap().octets();
            let mut request = [0x0a; 66];
            request[..40].fill(0);
            request[0] = 0x60;
            request[4..6].copy_from_slice(&26u16.to_be_bytes());
            request[6] = IpProtocol::Icmpv6.into();
            request[7] = 32;
            request[8..24].copy_from_slice(&source);
            request[24..40].copy_from_slice(&destination);
            request[40..48].copy_from_slice(&[128, 0, 0, 0, 0, 0x0b, 0, 5]);
            let pseudo_tail = [0, 0, 0, 26, 0, 0, 0, 58];
            let checksum =
                internet_checksum_parts(&[&request[8..40], &pseudo_tail, &request[40..]]);
            request[42..44].copy_from_slice(&checksum.to_be_bytes());
            let parsed = ip_header(
                &request,
                BufferPacketCursor::new()
                    .with_packet_len(request.len())
                    .with_network_header(0, 40)
                    .with_transport_header(40, 8),
            )
            .unwrap();

            for first_segment_len in [request.len(), 48] {
                let mut reply = request;
                build_echo_reply(&mut reply[..first_segment_len], &parsed).unwrap();
                assert_eq!(&reply[8..24], &destination);
                assert_eq!(&reply[24..40], &source);
                assert_eq!(&reply[40..42], &[129, 0]);
                assert_eq!(&reply[44..48], &[0, 0x0b, 0, 5]);
                assert_eq!(&reply[48..], &request[48..]);
                assert_eq!(
                    internet_checksum_parts(&[&reply[8..40], &pseudo_tail, &reply[40..]]),
                    0,
                );
            }
        }
    }
}
