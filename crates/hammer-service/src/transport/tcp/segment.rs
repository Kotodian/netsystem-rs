use std::mem::transmute;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, IpEcnCodepoint, NetworkOpaque,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::write_tcp_segment_header;
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpSackBlock, TcpSegmentFlags, TcpSegmentHeader, tcp_options_from_bytes,
};
use hammer_infra::vec::Vec;

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
    pub(crate) sack_blocks: Vec<TcpSackBlock>,
    pub(crate) fast_open_cookie: Option<Vec<u8>>,
    pub(crate) ip_ecn: Option<IpEcnCodepoint>,
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
    let transport = packet
        .get(cursor.transport_header_offset()..first_len)
        .ok_or_else(|| CoreError::internal("tcp transport header is missing"))?;
    let segment = etherparse::TcpSlice::from_slice(transport).map_err(tcp_parse_error)?;
    let parsed_options = tcp_options_from_bytes(segment.options());
    let payload_offset = cursor
        .transport_header_offset()
        .checked_add(segment.header_len())
        .ok_or_else(|| CoreError::internal("tcp payload offset overflow"))?;
    let payload_len = parsed
        .packet_len
        .checked_sub(payload_offset)
        .ok_or_else(|| CoreError::internal("tcp payload offset exceeds packet length"))?;
    let local = SocketAddr::new(destination_ip, segment.destination_port());
    let remote = SocketAddr::new(source_ip, segment.source_port());
    let acknowledgment = segment.ack().then(|| segment.acknowledgment_number());
    let mut flags = TcpSegmentFlags::empty();
    flags.set(TcpSegmentFlags::NS, segment.ns());
    flags.set(TcpSegmentFlags::FIN, segment.fin());
    flags.set(TcpSegmentFlags::SYN, segment.syn());
    flags.set(TcpSegmentFlags::RST, segment.rst());
    flags.set(TcpSegmentFlags::PSH, segment.psh());
    flags.set(TcpSegmentFlags::ACK, segment.ack());
    flags.set(TcpSegmentFlags::URG, segment.urg());
    flags.set(TcpSegmentFlags::ECE, segment.ece());
    flags.set(TcpSegmentFlags::CWR, segment.cwr());
    Ok(TcpPacket {
        local,
        remote,
        sequence: segment.sequence_number(),
        acknowledgment,
        advertised_window: segment.window_size(),
        flags,
        capabilities: parsed_options.capabilities,
        sack_blocks: parsed_options.sack_blocks,
        fast_open_cookie: parsed_options.fast_open_cookie,
        ip_ecn: unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }
            .ip()
            .ip_ecn()
            .map(Into::into),
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
    fast_open_cookie: Option<[u8; 16]>,
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
        fast_open_cookie: Option<&[u8]>,
        ip_ecn: Option<IpEcnCodepoint>,
        payload_len: usize,
    ) -> Self {
        let (sack_blocks, sack_block_count) = copy_sack_blocks(sack_blocks);
        let mut copied_fast_open_cookie = None;
        let mut fast_open_cookie_len = 0;
        if let Some(cookie) = fast_open_cookie.filter(|cookie| !cookie.is_empty()) {
            let mut copied = [0u8; 16];
            let len = cookie.len().min(copied.len());
            copied[..len].copy_from_slice(&cookie[..len]);
            copied_fast_open_cookie = Some(copied);
            fast_open_cookie_len = len as u8;
        }
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
            fast_open_cookie: copied_fast_open_cookie,
            fast_open_cookie_len,
            ip_ecn,
            payload_len,
        }
    }

    #[inline]
    pub fn write_header(&self, output: &mut [u8]) -> CoreResult<usize> {
        let fast_open_cookie = self
            .fast_open_cookie
            .as_ref()
            .map(|cookie| &cookie[..usize::from(self.fast_open_cookie_len)]);
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
                fast_open_cookie,
            },
            self.sack_blocks(),
        )
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

    pub(crate) fn read_from_buffer(
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<Self> {
        let buffer = runtime.get_buffer(index)?;
        let mut segment =
            Self::from_primary_opaque(buffer.opaque().read::<TcpSegmentHeaderOpaque>());
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
        TcpSegmentHeaderOpaque {
            fields: TcpSegmentHeaderFields {
                local_port: self.local.port(),
                remote_port: self.remote.port(),
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

    #[inline]
    fn from_primary_opaque(opaque: TcpSegmentHeaderOpaque) -> Self {
        let fields = unsafe { opaque.fields };
        Self {
            local: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), fields.local_port),
            remote: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), fields.remote_port),
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
            fast_open_cookie: None,
            fast_open_cookie_len: fields.fast_open_cookie_len,
            ip_ecn: None,
            payload_len: 0,
        }
    }

    #[inline]
    fn secondary_opaque(&self) -> TcpSegmentSackOpaque {
        let mut blocks = [0u64; 4];
        if let Some(sack_blocks) = self.sack_blocks() {
            for (slot, block) in sack_blocks.iter().take(blocks.len()).enumerate() {
                blocks[slot] = (u64::from(block.left_edge) << 32) | u64::from(block.right_edge);
            }
        }
        let mut cookie_words = [0u64; 2];
        if let Some(cookie) = self.fast_open_cookie.as_ref() {
            let mut bytes = [0u8; 16];
            let len = usize::from(self.fast_open_cookie_len);
            bytes[..len].copy_from_slice(&cookie[..len]);
            cookie_words[0] = u64::from_ne_bytes(bytes[..8].try_into().expect("cookie word 0"));
            cookie_words[1] = u64::from_ne_bytes(bytes[8..16].try_into().expect("cookie word 1"));
        }
        TcpSegmentSackOpaque {
            fields: TcpSegmentSackFields {
                blocks,
                cookie_words,
                reserved: [0; 1],
            },
        }
    }

    #[inline]
    fn apply_secondary_opaque(&mut self, opaque: TcpSegmentSackOpaque) {
        let fields = unsafe { opaque.fields };
        let mut blocks = [TcpSackBlock {
            left_edge: 0,
            right_edge: 0,
        }; 4];
        let mut count = 0usize;
        for word in fields.blocks {
            let left = (word >> 32) as u32;
            let right = word as u32;
            if left == 0 && right == 0 {
                continue;
            }
            blocks[count] = TcpSackBlock {
                left_edge: left,
                right_edge: right,
            };
            count += 1;
            if count == blocks.len() {
                break;
            }
        }
        let mut cookie = None;
        if fields.cookie_words[0] != 0 || fields.cookie_words[1] != 0 {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&fields.cookie_words[0].to_ne_bytes());
            bytes[8..16].copy_from_slice(&fields.cookie_words[1].to_ne_bytes());
            cookie = Some(bytes);
        }
        self.sack_blocks = (count != 0).then_some(blocks);
        self.sack_block_count = count as u8;
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
    cookie_words: [u64; 2],
    reserved: [u64; 1],
}

#[derive(Clone, Copy)]
#[repr(C)]
union TcpSegmentSackOpaque {
    raw: [u64; 7],
    fields: TcpSegmentSackFields,
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
fn tcp_parse_error(error: etherparse::err::tcp::HeaderSliceError) -> CoreError {
    match error {
        etherparse::err::tcp::HeaderSliceError::Len(_)
        | etherparse::err::tcp::HeaderSliceError::Content(
            etherparse::err::tcp::HeaderError::DataOffsetTooSmall { .. },
        ) => CoreError::internal("tcp segment is invalid"),
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
                left_edge: 30,
                right_edge: 40,
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
            left_edge: 30,
            right_edge: 40,
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
            0,
        );

        segment
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");
        let restored = TcpSegment::read_from_buffer(&runtime, index).expect("read segment");

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
            Some(&[1, 2, 3, 4]),
            None,
            5,
        );

        segment
            .write_to_buffer(runtime.packet_buffers(), index)
            .expect("write segment");
        let restored = TcpSegment::read_from_buffer(&runtime, index).expect("read segment");

        let mut header = [0u8; 64];
        let written = restored.write_header(&mut header).expect("write header");
        let parsed = tcp_options_from_bytes(&header[20..written]);
        assert_eq!(parsed.fast_open_cookie.as_deref(), Some(&[1, 2, 3, 4][..]));
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
