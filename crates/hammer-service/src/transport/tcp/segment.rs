use std::mem::transmute;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};
use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, IpEcnCodepoint, NetworkOpaque,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpError, TcpFastOpenCookie, TcpPacket, TcpSackBlock, TcpSegmentFlags,
    TcpSegmentHeader, TcpSegmentParseError, TcpSeq, TcpTimestampOption, tcp_options_from_bytes,
    tcp_segment_header_len, write_tcp_segment_header,
};

const TCP_SEGMENT_OPAQUE_PRESENT: u8 = 1 << 7;

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub(crate) struct TcpWireHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    sequence_number: [u8; 4],
    acknowledgment_number: [u8; 4],
    data_offset_reserved_flags: [u8; 2],
    advertised_window: [u8; 2],
    checksum: [u8; 2],
    urgent_pointer: [u8; 2],
}

impl TcpWireHeader {
    #[inline(always)]
    pub(crate) fn header_len(self) -> usize {
        usize::from(self.data_offset_reserved_flags[0] >> 4) * 4
    }

    #[inline(always)]
    pub(crate) fn source_port(self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    pub(crate) fn destination_port(self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    pub(crate) fn sequence_number(self) -> u32 {
        u32::from_be_bytes(self.sequence_number)
    }

    #[inline(always)]
    pub(crate) fn acknowledgment_number(self) -> u32 {
        u32::from_be_bytes(self.acknowledgment_number)
    }

    #[inline(always)]
    pub(crate) fn advertised_window(self) -> u16 {
        u16::from_be_bytes(self.advertised_window)
    }
}

pub(crate) fn parse_tcp_packet(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpPacket> {
    let (
        local,
        remote,
        sequence,
        acknowledgment,
        advertised_window,
        flags,
        capabilities,
        sack_blocks,
        timestamp,
        fast_open_cookie,
        ip_ecn,
        payload_offset,
        payload_len,
    ) = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        let cursor = buffer.packet_cursor();
        let parsed =
            parse_ip_packet_with_chain_len(current, buffer.total_len_not_including_first())
                .map_err(|_| TcpError::from(TcpSegmentParseError::InvalidIpHeader))?;
        if parsed.protocol != IpProtocol::Tcp || parsed.input_error != IpInputError::None {
            return Err(TcpError::from(TcpSegmentParseError::WrongProtocol).into());
        }
        let first_len = current.len().min(parsed.packet_len);
        let packet = current
            .get(..first_len)
            .ok_or(TcpError::from(TcpSegmentParseError::InvalidPacketLength))?;
        let source_ip = source_ip(parsed.version, packet)?;
        let destination_ip = destination_ip(parsed.version, packet)?;
        let transport = packet
            .get(cursor.transport_header_offset()..first_len)
            .ok_or(TcpError::from(TcpSegmentParseError::MissingTransportHeader))?;
        let segment = parse_tcp_wire_header(transport).map_err(|error| TcpError::from(error))?;
        let header_len = segment.header_len();
        let options = transport
            .get(20..header_len)
            .ok_or(TcpError::from(TcpSegmentParseError::InvalidSlice))?;
        let parsed_options = tcp_options_from_bytes(options);
        let payload_offset = cursor
            .transport_header_offset()
            .checked_add(header_len)
            .ok_or(TcpError::from(TcpSegmentParseError::PayloadOffsetOverflow))?;
        let payload_len = parsed
            .packet_len
            .checked_sub(payload_offset)
            .ok_or(TcpError::from(
                TcpSegmentParseError::PayloadOffsetExceedsPacketLength,
            ))?;
        let local = SocketAddr::new(destination_ip, segment.destination_port());
        let remote = SocketAddr::new(source_ip, segment.source_port());
        let flags = tcp_wire_flags(segment);
        let acknowledgment = flags
            .contains(TcpSegmentFlags::ACK)
            .then(|| TcpSeq::from(segment.acknowledgment_number()));
        (
            local,
            remote,
            TcpSeq::from(segment.sequence_number()),
            acknowledgment,
            segment.advertised_window(),
            flags,
            parsed_options.capabilities,
            parsed_options.sack_blocks,
            parsed_options.timestamp,
            parsed_options.fast_open_cookie,
            unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }
                .ip()
                .ip_ecn()
                .map(Into::into),
            payload_offset,
            payload_len,
        )
    };
    Ok(TcpPacket {
        local,
        remote,
        sequence,
        acknowledgment,
        advertised_window,
        flags,
        capabilities,
        sack_blocks,
        timestamp,
        fast_open_cookie,
        ip_ecn,
        payload_offset,
        payload_len,
    })
}

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
    fast_open_cookie_len: u8,
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
        let fast_open_cookie_len = fast_open_cookie
            .as_ref()
            .map_or(0, |cookie| cookie.len() as u8);
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
            fast_open_cookie_len,
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
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let mut buffer = buffers.get_buffer_mut(index)?;
        buffer.opaque_mut().write(self.primary_opaque());
        buffer.opaque2_mut().write(self.secondary_opaque());
        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
            .ip_mut()
            .set_ip_ecn(self.ip_ecn.map(Into::into));
        Ok(())
    }

    pub(crate) fn read_from_buffer(buffer: &hammer_adapter::Buffer) -> Result<Self, TcpError> {
        let mut segment: Self = buffer
            .opaque()
            .read::<TcpSegmentHeaderOpaque>()
            .try_into()?;
        segment.apply_secondary_opaque(buffer.opaque2().read::<TcpSegmentSackOpaque>());
        segment.ip_ecn = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }
            .ip()
            .ip_ecn()
            .map(Into::into);
        Ok(segment)
    }

    #[inline]
    fn primary_opaque(&self) -> TcpSegmentHeaderOpaque {
        let mut capabilities = 0u8;
        capabilities |= u8::from(self.capabilities.sack);
        capabilities |= u8::from(self.capabilities.timestamps) << 1;
        capabilities |= u8::from(self.capabilities.ecn) << 2;
        capabilities |= u8::from(self.capabilities.accurate_ecn) << 3;
        capabilities |= u8::from(self.capabilities.fast_open) << 4;
        capabilities |= TCP_SEGMENT_OPAQUE_PRESENT;
        TcpSegmentHeaderOpaque {
            fields: TcpSegmentHeaderFields {
                local_port: self.local_port,
                remote_port: self.remote_port,
                advertised_window: self.advertised_window,
                flags: self.flags.bits(),
                capabilities,
                sack_block_count: self.sack_block_count,
                fast_open_cookie_len: self.fast_open_cookie_len,
                reserved: [0; 2],
                sequence: self.sequence,
                acknowledgment: self.acknowledgment,
                reserved_words: [0; 2],
            },
        }
    }
}

impl TryFrom<TcpSegmentHeaderOpaque> for TcpSegment {
    type Error = TcpError;

    #[inline]
    fn try_from(opaque: TcpSegmentHeaderOpaque) -> Result<Self, TcpError> {
        let fields = unsafe { opaque.fields };
        if (fields.capabilities & TCP_SEGMENT_OPAQUE_PRESENT) == 0 {
            return Err(TcpSegmentParseError::MissingIntent.into());
        }
        Ok(Self {
            local_port: fields.local_port,
            remote_port: fields.remote_port,
            sequence: fields.sequence,
            acknowledgment: fields.acknowledgment,
            advertised_window: fields.advertised_window,
            flags: TcpSegmentFlags::from_bits_truncate(fields.flags),
            capabilities: TcpCapabilities {
                max_segment_size: None,
                window_scale: None,
                sack: fields.capabilities & 0x01 != 0,
                timestamps: fields.capabilities & 0x02 != 0,
                ecn: fields.capabilities & 0x04 != 0,
                accurate_ecn: fields.capabilities & 0x08 != 0,
                fast_open: fields.capabilities & 0x10 != 0,
            },
            sack_blocks: None,
            sack_block_count: fields.sack_block_count,
            timestamp: None,
            fast_open_cookie: None,
            fast_open_cookie_len: fields.fast_open_cookie_len,
            ip_ecn: None,
            payload_len: 0,
        })
    }
}

impl TcpSegment {
    #[inline]
    fn secondary_opaque(&self) -> TcpSegmentSackOpaque {
        let mut blocks = [0u64; 4];
        if let Some(sack_blocks) = self.sack_blocks() {
            for (slot, block) in sack_blocks.iter().take(blocks.len()).enumerate() {
                blocks[slot] = (u64::from(u32::from(block.left_edge)) << 32)
                    | u64::from(u32::from(block.right_edge));
            }
        }
        let mut cookie_words = [0u64; 2];
        let timestamp = self
            .timestamp
            .unwrap_or(TcpTimestampOption { tsval: 0, tsecr: 0 });
        if let Some(cookie) = self.fast_open_cookie.as_ref() {
            let mut bytes = [0u8; TcpFastOpenCookie::MAX_LEN];
            let len = cookie.len();
            bytes[..len].copy_from_slice(cookie.as_slice());
            cookie_words[0] = u64::from_ne_bytes(bytes[..8].try_into().expect("cookie word 0"));
            cookie_words[1] = u64::from_ne_bytes(bytes[8..16].try_into().expect("cookie word 1"));
        }
        TcpSegmentSackOpaque {
            fields: TcpSegmentSackFields {
                blocks,
                timestamp: [timestamp.tsval, timestamp.tsecr],
                cookie_words,
            },
        }
    }

    #[inline]
    fn apply_secondary_opaque(&mut self, opaque: TcpSegmentSackOpaque) {
        let fields = unsafe { opaque.fields };
        let mut blocks = [TcpSackBlock {
            left_edge: TcpSeq::from(0),
            right_edge: TcpSeq::from(0),
        }; 4];
        let mut count = 0usize;
        for word in fields.blocks {
            let left = (word >> 32) as u32;
            let right = word as u32;
            if left == 0 && right == 0 {
                continue;
            }
            blocks[count] = TcpSackBlock {
                left_edge: TcpSeq::from(left),
                right_edge: TcpSeq::from(right),
            };
            count += 1;
            if count == blocks.len() {
                break;
            }
        }
        let mut cookie = None;
        if self.fast_open_cookie_len != 0 {
            let mut bytes = [0u8; TcpFastOpenCookie::MAX_LEN];
            bytes[..8].copy_from_slice(&fields.cookie_words[0].to_ne_bytes());
            bytes[8..16].copy_from_slice(&fields.cookie_words[1].to_ne_bytes());
            cookie = (&bytes[..usize::from(self.fast_open_cookie_len)])
                .try_into()
                .ok();
        }
        self.sack_blocks = (count != 0).then_some(blocks);
        self.sack_block_count = count as u8;
        self.timestamp = self.capabilities.timestamps.then_some(TcpTimestampOption {
            tsval: fields.timestamp[0],
            tsecr: fields.timestamp[1],
        });
        self.fast_open_cookie = cookie;
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct TcpSegmentHeaderFields {
    local_port: u16,
    remote_port: u16,
    advertised_window: u16,
    flags: u16,
    capabilities: u8,
    sack_block_count: u8,
    fast_open_cookie_len: u8,
    reserved: [u8; 2],
    sequence: u32,
    acknowledgment: u32,
    reserved_words: [u64; 2],
}

#[derive(Clone, Copy)]
#[repr(C)]
union TcpSegmentHeaderOpaque {
    raw: [u64; 5],
    fields: TcpSegmentHeaderFields,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct TcpSegmentSackFields {
    blocks: [u64; 4],
    timestamp: [u32; 2],
    cookie_words: [u64; 2],
}

#[derive(Clone, Copy)]
#[repr(C)]
union TcpSegmentSackOpaque {
    raw: [u64; 7],
    fields: TcpSegmentSackFields,
}

fn source_ip(version: IpVersion, packet: &[u8]) -> Result<IpAddr, TcpError> {
    match version {
        IpVersion::V4 => {
            let source = packet
                .get(12..16)
                .ok_or(TcpSegmentParseError::MissingIpv4Source)?;
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        IpVersion::V6 => {
            let source = packet
                .get(8..24)
                .ok_or(TcpSegmentParseError::MissingIpv6Source)?;
            let bytes: [u8; 16] = source
                .try_into()
                .map_err(|_| TcpSegmentParseError::InvalidIpv6SourceLength)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

fn destination_ip(version: IpVersion, packet: &[u8]) -> Result<IpAddr, TcpError> {
    match version {
        IpVersion::V4 => {
            let destination = packet
                .get(16..20)
                .ok_or(TcpSegmentParseError::MissingIpv4Destination)?;
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
                .ok_or(TcpSegmentParseError::MissingIpv6Destination)?;
            let bytes: [u8; 16] = destination
                .try_into()
                .map_err(|_| TcpSegmentParseError::InvalidIpv6DestinationLength)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline]
pub(crate) fn parse_tcp_wire_header(packet: &[u8]) -> Result<TcpWireHeader, TcpSegmentParseError> {
    if packet.len() < std::mem::size_of::<TcpWireHeader>() {
        return Err(TcpSegmentParseError::ShortHeader);
    }
    // SAFETY: `TcpWireHeader` is packed and the length check above guarantees
    // the bytes are present in the slice.
    let header_ptr = unsafe { transmute::<_, *const TcpWireHeader>(packet.as_ptr()) };
    let header = unsafe { header_ptr.read_unaligned() };
    let header_len = header.header_len();
    if header_len < std::mem::size_of::<TcpWireHeader>() {
        return Err(TcpSegmentParseError::BadDataOffset);
    }
    if packet.len() < header_len {
        return Err(TcpSegmentParseError::InvalidSlice);
    }
    Ok(header)
}

#[inline(always)]
pub(crate) fn tcp_wire_flags(segment: TcpWireHeader) -> TcpSegmentFlags {
    let first = segment.data_offset_reserved_flags[0];
    let second = segment.data_offset_reserved_flags[1];
    let mut flags = TcpSegmentFlags::empty();
    flags.set(TcpSegmentFlags::NS, first & 0x01 != 0);
    flags.set(TcpSegmentFlags::FIN, second & 0x01 != 0);
    flags.set(TcpSegmentFlags::SYN, second & 0x02 != 0);
    flags.set(TcpSegmentFlags::RST, second & 0x04 != 0);
    flags.set(TcpSegmentFlags::PSH, second & 0x08 != 0);
    flags.set(TcpSegmentFlags::ACK, second & 0x10 != 0);
    flags.set(TcpSegmentFlags::URG, second & 0x20 != 0);
    flags.set(TcpSegmentFlags::ECE, second & 0x40 != 0);
    flags.set(TcpSegmentFlags::CWR, second & 0x80 != 0);
    flags
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
    copied[..len].copy_from_slice(&sack_blocks[..len]);
    (Some(copied), len as u8)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use hammer_adapter::{BufferPacketCursor, DataPlaneRuntime, NetworkOpaque};

    use super::*;

    #[test]
    fn transport_tcp_parse_packet_keeps_inbound_sack_blocks() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 4, 4, 4);
        let packet = ipv4_tcp_packet_with_sack();
        let index = runtime.alloc_index_with_bytes(&packet).expect("packet");
        stamp_tcp_cursor(&runtime, index, &packet);

        let parsed = parse_tcp_packet(&runtime, index).expect("parse tcp packet");

        assert_eq!(
            parsed.sack_blocks,
            vec![TcpSackBlock {
                left_edge: TcpSeq::from(30),
                right_edge: TcpSeq::from(40),
            }]
        );
    }

    #[test]
    fn transport_tcp_segment_buffer_round_trip_keeps_sack_blocks() {
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
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");
        let restored = TcpSegment::read_from_buffer(&runtime.get_buffer(index).expect("buffer"))
            .expect("read segment");

        let mut header = [0u8; 64];
        let written = restored.write_header(&mut header).expect("write header");
        let parsed = tcp_options_from_bytes(&header[20..written]);
        assert_eq!(parsed.sack_blocks, blocks);
    }

    #[test]
    fn transport_tcp_segment_buffer_round_trip_keeps_fast_open_cookie() {
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
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");
        let restored = TcpSegment::read_from_buffer(&runtime.get_buffer(index).expect("buffer"))
            .expect("read segment");

        let mut header = [0u8; 64];
        let written = restored.write_header(&mut header).expect("write header");
        let parsed = tcp_options_from_bytes(&header[20..written]);
        assert_eq!(
            parsed
                .fast_open_cookie
                .as_ref()
                .map(TcpFastOpenCookie::as_slice),
            Some(&[1, 2, 3, 4][..])
        );
    }

    fn ipv4_tcp_packet_with_sack() -> std::vec::Vec<u8> {
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
        packet[33] = TcpSegmentFlags::ACK.bits() as u8;
        packet[34..36].copy_from_slice(&32_768u16.to_be_bytes());
        packet[40..52].copy_from_slice(&[1, 1, 5, 10, 0, 0, 0, 30, 0, 0, 0, 40]);
        let tcp_checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
        let ip_checksum = internet_checksum(&packet[..20]);
        packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        packet
    }

    fn stamp_tcp_cursor(runtime: &DataPlaneRuntime, index: BufferIndex, packet: &[u8]) {
        let network_header_len = ((*packet.first().expect("ipv4 version") & 0x0f) as usize) * 4;
        let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
        let tcp_header_len = ((packet[network_header_len + 12] >> 4) as usize) * 4;
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        buffer.set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(network_header_len, tcp_header_len)
                .with_transport_payload_offset(network_header_len + tcp_header_len),
        );
        unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
            .ip_mut()
            .set_ip_ecn(Some(0));
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        internet_checksum_parts(&[
            &source.octets(),
            &destination.octets(),
            &[0, protocol],
            &(segment.len() as u16).to_be_bytes(),
            segment,
        ])
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

    fn internet_checksum_parts(parts: &[&[u8]]) -> u16 {
        let mut sum = 0u32;
        let mut high = None;
        for part in parts {
            let mut index = 0usize;
            if let Some(hi) = high.take() {
                if let Some(&lo) = part.first() {
                    sum += u16::from_be_bytes([hi, lo]) as u32;
                    while sum > 0xffff {
                        sum = (sum & 0xffff) + (sum >> 16);
                    }
                    index = 1;
                } else {
                    high = Some(hi);
                    continue;
                }
            }
            let mut chunks = part[index..].chunks_exact(2);
            for chunk in &mut chunks {
                sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
                while sum > 0xffff {
                    sum = (sum & 0xffff) + (sum >> 16);
                }
            }
            if let [hi] = chunks.remainder() {
                high = Some(*hi);
            }
        }
        if let Some(hi) = high {
            sum += u16::from_be_bytes([hi, 0]) as u32;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }
}
