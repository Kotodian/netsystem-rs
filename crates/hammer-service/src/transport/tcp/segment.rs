use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::net::{IpEcnCodepoint, NetworkOpaque};
use hammer_adapter::{BufferIndex, BufferPacketCursor, DataPlaneRuntime};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpError, TcpFastOpenCookie, TcpPacket, TcpSackBlock, TcpSegmentFlags,
    TcpSegmentHeader, TcpSeq, TcpTimestampOption, TcpWireHeader, tcp_header,
    tcp_options_from_bytes, tcp_segment_header_len, write_tcp_segment_header,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSegment {
    local_port: u16,
    remote_port: u16,
    sequence: u32,
    acknowledgment: u32,
    advertised_window: u16,
    flags: TcpSegmentFlags,
    capabilities: TcpCapabilities,
    sack_blocks: Option<[TcpSackBlock; 4]>,
    sack_block_count: u8,
    timestamp: Option<TcpTimestampOption>,
    fast_open_cookie: Option<TcpFastOpenCookie>,
    ip_ecn: Option<IpEcnCodepoint>,
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
        timestamp: Option<TcpTimestampOption>,
        fast_open_cookie: Option<TcpFastOpenCookie>,
        ip_ecn: Option<IpEcnCodepoint>,
        payload_len: usize,
    ) -> Self {
        let (sack_blocks, sack_block_count) = copy_sack_blocks(sack_blocks);
        Self {
            local_port: local.port(),
            remote_port: remote.port(),
            sequence,
            acknowledgment,
            advertised_window,
            flags,
            capabilities,
            sack_blocks,
            sack_block_count,
            timestamp,
            fast_open_cookie,
            ip_ecn,
            payload_len,
        }
    }

    #[inline]
    pub fn write_header(&self, output: &mut [u8]) -> Result<usize, TcpError> {
        write_tcp_segment_header(output, self.header(), self.sack_blocks())
    }

    #[inline]
    pub fn header_len(&self) -> usize {
        tcp_segment_header_len(self.header(), self.sack_blocks())
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

    #[inline]
    fn header(&self) -> TcpSegmentHeader<'_> {
        TcpSegmentHeader {
            source_port: self.local_port,
            destination_port: self.remote_port,
            sequence_number: self.sequence,
            acknowledgment_number: self.acknowledgment,
            flags: self.flags,
            advertised_window: self.advertised_window,
            capabilities: self.capabilities,
            timestamp: self.timestamp,
            fast_open_cookie: self.fast_open_cookie.as_ref(),
        }
    }

    pub(crate) fn write_to_buffer(
        &self,
        buffers: &hammer_adapter::DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let mut buffer = buffers.get_buffer_mut(index)?;
        let header = buffer.prepend_mut(self.header_len())?;
        self.write_header(header)?;
        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
            .ip_mut()
            .set_ip_ecn(self.ip_ecn.map(Into::into));
        Ok(())
    }
}

pub(crate) fn tcp_packet(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<TcpPacket> {
    let buffer = runtime.get_buffer(index)?;
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let cursor = network.packet_cursor();
    let packet = buffer.current();
    let first_len = packet.len().min(cursor.packet_len());
    if !valid_tcp_cursor(cursor, first_len) {
        return Err(TcpError::Length.into());
    }
    let network_header = packet
        .get(cursor.network_header_offset()..cursor.transport_header_offset())
        .ok_or(TcpError::Length)?;
    let tcp = tcp_header(
        packet
            .get(cursor.transport_header_offset()..first_len)
            .ok_or(TcpError::Length)?,
    )
    .map_err(|_| TcpError::Length)?;
    let header_len = tcp.header_len();
    if cursor.transport_header_len() != header_len
        || cursor.transport_payload_offset()
            != cursor
                .transport_header_offset()
                .checked_add(header_len)
                .ok_or(TcpError::Length)?
    {
        return Err(TcpError::Length.into());
    }
    let version = packet
        .get(cursor.network_header_offset())
        .copied()
        .ok_or(TcpError::Length)?
        >> 4;
    let source_ip = source_ip(version, network_header)?;
    let destination_ip = destination_ip(version, network_header)?;
    let options = packet
        .get(
            cursor.transport_header_offset() + size_of::<TcpWireHeader>()
                ..cursor.transport_header_offset() + header_len,
        )
        .ok_or(TcpError::Length)?;
    let parsed_options = tcp_options_from_bytes(options);
    let flags = tcp.flags();
    Ok(TcpPacket {
        local: SocketAddr::new(destination_ip, tcp.destination_port()),
        remote: SocketAddr::new(source_ip, tcp.source_port()),
        sequence: TcpSeq::from(tcp.sequence_number()),
        acknowledgment: flags
            .contains(TcpSegmentFlags::ACK)
            .then(|| TcpSeq::from(tcp.acknowledgment_number())),
        advertised_window: tcp.advertised_window(),
        flags,
        capabilities: parsed_options.capabilities,
        sack_blocks: parsed_options.sack_blocks,
        timestamp: parsed_options.timestamp,
        fast_open_cookie: parsed_options.fast_open_cookie,
        ip_ecn: network.ip().ip_ecn().map(IpEcnCodepoint::from),
        payload_offset: cursor.transport_payload_offset(),
        payload_len: first_len
            .checked_sub(cursor.transport_payload_offset())
            .ok_or(TcpError::Length)?,
    })
}

#[inline(always)]
fn valid_tcp_cursor(cursor: BufferPacketCursor, available_len: usize) -> bool {
    cursor.network_header_offset() <= cursor.transport_header_offset()
        && cursor.transport_header_offset() <= cursor.transport_payload_offset()
        && cursor.transport_payload_offset() <= cursor.packet_len()
        && cursor.transport_payload_offset() <= available_len
}

#[inline(always)]
fn source_ip(version: u8, packet: &[u8]) -> Result<IpAddr, TcpError> {
    match version {
        4 => {
            let source = packet.get(12..16).ok_or(TcpError::Length)?;
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        6 => {
            let source = packet.get(8..24).ok_or(TcpError::Length)?;
            let bytes: [u8; 16] = source.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
        _ => Err(TcpError::SegmentInvalid),
    }
}

#[inline(always)]
fn destination_ip(version: u8, packet: &[u8]) -> Result<IpAddr, TcpError> {
    match version {
        4 => {
            let destination = packet.get(16..20).ok_or(TcpError::Length)?;
            Ok(Ipv4Addr::new(
                destination[0],
                destination[1],
                destination[2],
                destination[3],
            )
            .into())
        }
        6 => {
            let destination = packet.get(24..40).ok_or(TcpError::Length)?;
            let bytes: [u8; 16] = destination.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
        _ => Err(TcpError::SegmentInvalid),
    }
}

#[inline]
fn copy_sack_blocks(sack_blocks: Option<&[TcpSackBlock]>) -> (Option<[TcpSackBlock; 4]>, u8) {
    let Some(sack_blocks) = sack_blocks.filter(|blocks| !blocks.is_empty()) else {
        return (None, 0);
    };
    let mut copied = [TcpSackBlock {
        left_edge: TcpSeq::from(0),
        right_edge: TcpSeq::from(0),
    }; 4];
    let len = sack_blocks.len().min(copied.len());
    let mut index = 0usize;
    while index < len {
        copied[index] = sack_blocks[index];
        index += 1;
    }
    (Some(copied), len as u8)
}

#[cfg(test)]
mod tests {
    use hammer_adapter::DataPlaneRuntime;
    use hammer_core::protocol::tcp::tcp_options_from_bytes;
    use hammer_core::protocol::wire::read_header;

    use super::*;

    #[test]
    fn transport_tcp_segment_write_to_buffer_prepends_sack_blocks() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let index = runtime.alloc_index().expect("buffer");
        let local = "192.0.2.10:50000".parse().expect("local");
        let remote = "198.51.100.20:443".parse().expect("remote");
        let blocks = [TcpSackBlock {
            left_edge: TcpSeq::from(30),
            right_edge: TcpSeq::from(40),
        }];
        let segment = TcpSegment::new(
            local,
            remote,
            100,
            200,
            4096,
            TcpSegmentFlags::ACK,
            TcpCapabilities::default(),
            Some(&blocks),
            None,
            None,
            None,
            0,
        );

        segment
            .write_to_buffer(runtime.buffers(), index)
            .expect("write segment");
        let buffer = runtime.get_buffer(index).expect("buffer");
        let header = read_header::<TcpWireHeader>(buffer.current(), 0).expect("tcp header");
        let parsed = tcp_options_from_bytes(&buffer.current()[20..header.header_len()]);
        assert_eq!(parsed.sack_blocks, blocks);
    }

    #[test]
    fn transport_tcp_segment_write_to_buffer_prepends_fast_open_cookie() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let index = runtime.alloc_index().expect("buffer");
        let local = "192.0.2.10:50000".parse().expect("local");
        let remote = "198.51.100.20:443".parse().expect("remote");
        let segment = TcpSegment::new(
            local,
            remote,
            100,
            200,
            4096,
            TcpSegmentFlags::SYN,
            TcpCapabilities {
                fast_open: true,
                ..TcpCapabilities::default()
            },
            None,
            None,
            Some((&[1, 2, 3, 4][..]).try_into().expect("cookie")),
            None,
            5,
        );

        segment
            .write_to_buffer(runtime.buffers(), index)
            .expect("write segment");
        let buffer = runtime.get_buffer(index).expect("buffer");
        let header = read_header::<TcpWireHeader>(buffer.current(), 0).expect("tcp header");
        let parsed = tcp_options_from_bytes(&buffer.current()[20..header.header_len()]);
        assert_eq!(
            parsed
                .fast_open_cookie
                .as_ref()
                .map(TcpFastOpenCookie::as_slice),
            Some(&[1, 2, 3, 4][..])
        );
    }
}
