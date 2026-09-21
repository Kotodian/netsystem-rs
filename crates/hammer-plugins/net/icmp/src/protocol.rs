use hammer_infra::checksum::internet_checksum;
use hammer_plugin_ip::protocol::ip::{IpProtocol, IpVersion, Ipv4Header, Ipv6Header};
use hammer_service::opaque::NetworkOpaque;
use zerocopy::FromBytes;

#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub(crate) struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}

impl IcmpHeader {
    #[inline(always)]
    pub const fn icmp_type(&self) -> u8 {
        self.icmp_type
    }

    #[inline(always)]
    pub const fn code(&self) -> u8 {
        self.code
    }

    #[inline(always)]
    pub fn checksum(&self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }

    #[inline(always)]
    fn set_icmp_type(&mut self, icmp_type: u8) {
        self.icmp_type = icmp_type;
    }

    #[inline(always)]
    fn set_checksum(&mut self, checksum: u16) {
        self.checksum = checksum.to_be_bytes();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpBuildError {
    BadLength,
    WrongProtocol,
    WrongType,
}

/// IP local has already validated the complete message checksum. Only the
/// first buffer's headers change; payload and chain ownership remain untouched.
pub(crate) fn build_echo_reply(
    buffer: &mut hammer_core::buffer::Buffer,
    version: IpVersion,
    fragment_id: u16,
) -> Result<(), IcmpBuildError> {
    let (cursor, opaque_version, protocol) = {
        let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
        (
            network.packet_cursor(),
            network.ip().ip_version(),
            network.ip().ip_protocol().map(IpProtocol::from),
        )
    };
    let (request, reply, expected_version, expected_protocol) = match version {
        IpVersion::V4 => (8, 0, 4, IpProtocol::Icmpv4),
        IpVersion::V6 => (128, 129, 6, IpProtocol::Icmpv6),
    };
    if opaque_version != Some(expected_version) || protocol != Some(expected_protocol) {
        return Err(IcmpBuildError::WrongProtocol);
    }

    let ip_offset = cursor.network_header_offset();
    let ip_length = cursor.network_header_len();
    let icmp_offset = cursor.transport_header_offset();
    let ip_end = ip_offset
        .checked_add(ip_length)
        .ok_or(IcmpBuildError::BadLength)?;
    let echo_end = icmp_offset
        .checked_add(8)
        .ok_or(IcmpBuildError::BadLength)?;
    let echo_packet_end = echo_end
        .checked_sub(ip_offset)
        .ok_or(IcmpBuildError::BadLength)?;
    let packet = buffer.current();
    if icmp_offset < ip_end
        || cursor.packet_len() < echo_packet_end
        || packet.get(ip_offset..ip_end).is_none()
        || packet.get(icmp_offset..echo_end).is_none()
    {
        return Err(IcmpBuildError::BadLength);
    }

    match version {
        IpVersion::V4 => {
            let (header, _) = Ipv4Header::ref_from_prefix(&packet[ip_offset..ip_end])
                .map_err(|_| IcmpBuildError::BadLength)?;
            if header.version() != 4 || header.header_len() != ip_length {
                return Err(IcmpBuildError::BadLength);
            }
            if IpProtocol::from(header.protocol()) != expected_protocol {
                return Err(IcmpBuildError::WrongProtocol);
            }
        }
        IpVersion::V6 => {
            if ip_length != core::mem::size_of::<Ipv6Header>() {
                return Err(IcmpBuildError::BadLength);
            }
            let (header, _) = Ipv6Header::ref_from_prefix(&packet[ip_offset..ip_end])
                .map_err(|_| IcmpBuildError::BadLength)?;
            if header.version() != 6 {
                return Err(IcmpBuildError::WrongProtocol);
            }
        }
    }
    let (header, _) = IcmpHeader::ref_from_prefix(&packet[icmp_offset..echo_end])
        .map_err(|_| IcmpBuildError::BadLength)?;
    if header.icmp_type() != request {
        return Err(IcmpBuildError::WrongType);
    }
    // ip_csum_update updates the complemented checksum with end-around borrow.
    // Preserve negative zero for an all-zero reply; code and payload stay intact.
    // Swapping IPv6 addresses leaves the pseudo-header sum unchanged.
    let sum = u32::from(header.checksum()) + (u32::from(request) << 8);
    let (sum, borrow) = sum.overflowing_sub(u32::from(reply) << 8);
    let mut sum = sum.wrapping_sub(u32::from(borrow));
    sum = (sum & 0xffff) + (sum >> 16);
    sum = (sum & 0xffff) + (sum >> 16);
    let packet = buffer.current_mut();
    let (header, _) = IcmpHeader::mut_from_prefix(&mut packet[icmp_offset..echo_end])
        .expect("validated ICMP header remains contiguous");
    header.set_icmp_type(reply);
    header.set_checksum(sum as u16);

    match version {
        IpVersion::V4 => {
            let (header, _) = Ipv4Header::mut_from_prefix(&mut packet[ip_offset..ip_end])
                .expect("validated IPv4 header remains contiguous");
            header.swap_addresses();
            header.set_identification(fragment_id);
            header.set_ttl(64);
            header.set_checksum(0);
            let checksum = internet_checksum(&packet[ip_offset..ip_end]);
            let (header, _) = Ipv4Header::mut_from_prefix(&mut packet[ip_offset..ip_end])
                .expect("validated IPv4 header remains contiguous");
            header.set_checksum(checksum);
        }
        IpVersion::V6 => {
            let (header, _) = Ipv6Header::mut_from_prefix(&mut packet[ip_offset..ip_end])
                .expect("validated IPv6 header remains contiguous");
            header.swap_addresses();
            header.set_hop_limit(64);
        }
    }
    Ok(())
}
