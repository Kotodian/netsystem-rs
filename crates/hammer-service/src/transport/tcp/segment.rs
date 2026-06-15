use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, Network, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::write_tcp_segment_header;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpSegmentFlags, TcpSegmentHeader, TcpSegmentParseError, TcpSegmentView,
    tcp_capabilities_from_options,
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
    pub(crate) payload_offset: usize,
    pub(crate) payload_len: usize,
}

impl TcpPacket {
    #[inline]
    pub(crate) const fn has_payload(&self) -> bool {
        self.payload_len != 0
    }
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
        capabilities: tcp_capabilities_from_options(segment.options()),
        payload_offset,
        payload_len,
    })
}

pub(crate) fn alloc_tcp_segment(
    buffers: &DataPlaneBuffers,
    metadata: RouteMetadata,
    header: TcpSegmentHeader,
) -> CoreResult<BufferIndex> {
    let index = buffers.alloc_index(metadata)?;
    let result = (|| {
        let mut buffer = buffers.get_buffer_mut(index)?;
        let output = buffer.writable_tail_mut();
        let written = write_tcp_segment_header(output, header)?;
        buffer.commit_writable_tail(written)?;
        Ok(())
    })();
    if let Err(error) = result {
        buffers.free_index(index);
        return Err(error);
    }
    Ok(index)
}

pub(crate) fn tcp_segment_metadata(local: SocketAddr, remote: SocketAddr) -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(local.ip(), local.port())),
        destination: Some(SocksAddr::ip(remote.ip(), remote.port())),
        ..RouteMetadata::default()
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
