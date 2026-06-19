use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_adapter::{BufferIndex, DataPlaneRuntime, Network, RouteMetadata, SocksAddr};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::write_tcp_segment_header;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpSackBlock, TcpSegmentFlags, TcpSegmentHeader, TcpSegmentParseError,
    TcpSegmentView, tcp_options_from_bytes,
};

use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};

#[derive(Debug, Clone)]
pub(crate) struct TcpPacket {
    pub(crate) local: SocketAddr,
    pub(crate) remote: SocketAddr,
    pub(crate) sequence: u32,
    pub(crate) acknowledgment: Option<u32>,
    pub(crate) advertised_window: u16,
    pub(crate) flags: TcpSegmentFlags,
    pub(crate) capabilities: TcpCapabilities,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) sack_blocks: std::vec::Vec<TcpSackBlock>,
    pub(crate) payload_offset: usize,
    pub(crate) payload_len: usize,
}

pub(crate) fn parse_tcp_packet(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpPacket> {
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    let cursor = buffer.packet_cursor();
    let parsed = parse_ip_packet_with_chain_len(current, buffer.total_len_not_including_first())
        .map_err(|_| CoreError::internal("tcp packet has invalid IP header"))?;
    if parsed.protocol != IpProtocol::Tcp || parsed.input_error != IpInputError::None {
        return Err(CoreError::internal("packet is not TCP"));
    }
    let first_len = current.len().min(parsed.packet_len);
    let packet = current
        .get(..first_len)
        .ok_or_else(|| CoreError::internal("tcp packet length is invalid"))?;
    let source_ip = source_ip(parsed.version, packet)?;
    let destination_ip = destination_ip(parsed.version, packet)?;
    let segment = TcpSegmentView::parse(
        packet
            .get(cursor.transport_header_offset()..first_len)
            .ok_or_else(|| CoreError::internal("tcp transport header is missing"))?,
    )
    .map_err(tcp_parse_error)?;
    let parsed_options = tcp_options_from_bytes(segment.options());
    let payload_offset = cursor
        .transport_header_offset()
        .checked_add(segment.header_len())
        .ok_or_else(|| CoreError::internal("tcp payload offset overflow"))?;
    let payload_len = parsed
        .packet_len
        .checked_sub(payload_offset)
        .ok_or_else(|| CoreError::internal("tcp payload offset exceeds packet length"))?;
    let metadata = runtime.metadata(index)?;
    let local = metadata
        .destination
        .as_ref()
        .map(|addr| SocketAddr::new(addr.host, addr.port))
        .unwrap_or_else(|| SocketAddr::new(destination_ip, segment.destination_port()));
    let remote = metadata
        .source
        .as_ref()
        .map(|addr| SocketAddr::new(addr.host, addr.port))
        .unwrap_or_else(|| SocketAddr::new(source_ip, segment.source_port()));
    Ok(TcpPacket {
        local,
        remote,
        sequence: segment.sequence_number(),
        acknowledgment: segment.acknowledgment_number(),
        advertised_window: segment.advertised_window(),
        flags: segment.flags(),
        capabilities: parsed_options.capabilities,
        sack_blocks: parsed_options.sack_blocks,
        payload_offset,
        payload_len,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSegment {
    local: SocketAddr,
    remote: SocketAddr,
    sequence: u32,
    acknowledgment: u32,
    advertised_window: u16,
    flags: TcpSegmentFlags,
    capabilities: TcpCapabilities,
    sack_blocks: Option<[TcpSackBlock; 4]>,
    sack_block_count: u8,
    payload_len: usize,
}

impl TcpSegment {
    #[inline]
    pub fn new(
        local: SocketAddr,
        remote: SocketAddr,
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u16,
        flags: TcpSegmentFlags,
        capabilities: TcpCapabilities,
        sack_blocks: Option<&[TcpSackBlock]>,
        payload_len: usize,
    ) -> Self {
        let (sack_blocks, sack_block_count) = copy_sack_blocks(sack_blocks);
        Self {
            local,
            remote,
            sequence,
            acknowledgment,
            advertised_window,
            flags,
            capabilities,
            sack_blocks,
            sack_block_count,
            payload_len,
        }
    }

    #[inline]
    pub fn write_header(&self, output: &mut [u8]) -> CoreResult<usize> {
        write_tcp_segment_header(
            output,
            TcpSegmentHeader {
                source_port: self.local.port(),
                destination_port: self.remote.port(),
                sequence_number: self.sequence,
                acknowledgment_number: self.acknowledgment,
                flags: self.flags,
                advertised_window: self.advertised_window,
                capabilities: self.capabilities,
            },
            self.sack_blocks(),
        )
    }

    #[inline]
    pub fn route_metadata(&self) -> RouteMetadata {
        RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(self.local.ip(), self.local.port())),
            destination: Some(SocksAddr::ip(self.remote.ip(), self.remote.port())),
            ..RouteMetadata::default()
        }
    }

    #[inline]
    pub const fn payload_len(&self) -> usize {
        self.payload_len
    }

    #[inline]
    fn sack_blocks(&self) -> Option<&[TcpSackBlock]> {
        let Some(sack_blocks) = self.sack_blocks.as_ref() else {
            return None;
        };
        Some(&sack_blocks[..usize::from(self.sack_block_count)])
    }
}

fn source_ip(version: IpVersion, packet: &[u8]) -> CoreResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let source = packet
                .get(12..16)
                .ok_or_else(|| CoreError::internal("missing IPv4 source"))?;
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        IpVersion::V6 => {
            let source = packet
                .get(8..24)
                .ok_or_else(|| CoreError::internal("missing IPv6 source"))?;
            let bytes: [u8; 16] = source
                .try_into()
                .map_err(|_| CoreError::internal("invalid IPv6 source length"))?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

fn destination_ip(version: IpVersion, packet: &[u8]) -> CoreResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let destination = packet
                .get(16..20)
                .ok_or_else(|| CoreError::internal("missing IPv4 destination"))?;
            Ok(Ipv4Addr::new(
                destination[0],
                destination[1],
                destination[2],
                destination[3],
            )
            .into())
        }
        IpVersion::V6 => {
            let destination = packet
                .get(24..40)
                .ok_or_else(|| CoreError::internal("missing IPv6 destination"))?;
            let bytes: [u8; 16] = destination
                .try_into()
                .map_err(|_| CoreError::internal("invalid IPv6 destination length"))?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline]
fn tcp_parse_error(error: TcpSegmentParseError) -> CoreError {
    match error {
        TcpSegmentParseError::ShortHeader
        | TcpSegmentParseError::BadDataOffset
        | TcpSegmentParseError::InvalidSlice => CoreError::internal("tcp segment is invalid"),
    }
}

#[inline]
fn copy_sack_blocks(sack_blocks: Option<&[TcpSackBlock]>) -> (Option<[TcpSackBlock; 4]>, u8) {
    let Some(sack_blocks) = sack_blocks.filter(|blocks| !blocks.is_empty()) else {
        return (None, 0);
    };
    let mut copied = [TcpSackBlock {
        left_edge: 0,
        right_edge: 0,
    }; 4];
    let len = sack_blocks.len().min(copied.len());
    copied[..len].copy_from_slice(&sack_blocks[..len]);
    (Some(copied), len as u8)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use hammer_adapter::{BufferPacketCursor, DataPlaneRuntime, Network, RouteMetadata, SocksAddr};

    use super::*;

    #[test]
    fn transport_tcp_parse_packet_keeps_inbound_sack_blocks() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let packet = ipv4_tcp_packet_with_sack();
        let index = runtime
            .alloc_index_with_bytes(tcp_metadata(), &packet)
            .expect("packet");
        stamp_tcp_cursor(&runtime, index, &packet);

        let parsed = parse_tcp_packet(&runtime, index).expect("parse tcp packet");

        assert_eq!(
            parsed.sack_blocks,
            vec![TcpSackBlock {
                left_edge: 30,
                right_edge: 40,
            }]
        );
    }

    fn ipv4_tcp_packet_with_sack() -> Vec<u8> {
        let mut packet = vec![0u8; 52];
        let packet_len = packet.len() as u16;
        let source = Ipv4Addr::new(198, 51, 100, 20);
        let destination = Ipv4Addr::new(192, 0, 2, 10);
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&packet_len.to_be_bytes());
        packet[8] = 64;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&source.octets());
        packet[16..20].copy_from_slice(&destination.octets());
        packet[20..22].copy_from_slice(&443u16.to_be_bytes());
        packet[22..24].copy_from_slice(&50_000u16.to_be_bytes());
        packet[24..28].copy_from_slice(&0x0102_0304u32.to_be_bytes());
        packet[28..32].copy_from_slice(&0x1112_1314u32.to_be_bytes());
        packet[32] = 8 << 4;
        packet[33] = TcpSegmentFlags::ACK.bits();
        packet[34..36].copy_from_slice(&32_768u16.to_be_bytes());
        packet[40..52].copy_from_slice(&[1, 1, 5, 10, 0, 0, 0, 30, 0, 0, 0, 40]);
        let tcp_checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
        let ip_checksum = internet_checksum(&packet[..20]);
        packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        packet
    }

    fn tcp_metadata() -> RouteMetadata {
        RouteMetadata {
            network: Network::Tcp,
            source: Some(SocksAddr::ip(
                IpAddr::V4(Ipv4Addr::new(198, 51, 100, 20)),
                443,
            )),
            destination: Some(SocksAddr::ip(
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
                50_000,
            )),
            ..RouteMetadata::default()
        }
    }

    fn stamp_tcp_cursor(runtime: &DataPlaneRuntime, index: BufferIndex, packet: &[u8]) {
        let network_header_len = ((*packet.first().expect("ipv4 version") & 0x0f) as usize) * 4;
        let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        let tcp_header_len = ((packet[network_header_len + 12] >> 4) as usize) * 4;
        runtime
            .get_buffer_mut(index)
            .expect("buffer mut")
            .set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, network_header_len)
                    .with_transport_header(network_header_len, tcp_header_len)
                    .with_transport_payload_offset(network_header_len + tcp_header_len),
            );
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
        pseudo.extend_from_slice(&source.octets());
        pseudo.extend_from_slice(&destination.octets());
        pseudo.push(0);
        pseudo.push(protocol);
        pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
        pseudo.extend_from_slice(segment);
        internet_checksum(&pseudo)
    }

    fn internet_checksum(bytes: &[u8]) -> u16 {
        let mut sum = 0u32;
        for chunk in bytes.chunks(2) {
            let word = match chunk {
                [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
                [hi] => u16::from_be_bytes([*hi, 0]) as u32,
                _ => unreachable!(),
            };
            sum += word;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }
}
