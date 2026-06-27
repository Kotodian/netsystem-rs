use super::{
    TcpCapabilities, TcpError, TcpFastOpenCookie, TcpSackBlock, TcpSegmentFlags, TcpTimestampOption,
};
use crate::protocol::wire::{header_mut_ptr, read_header};
use std::mem::size_of;

const TCP_HEADER_MIN_LEN: usize = 20;
const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_SACK: u8 = 5;
const TCP_OPTION_SACK_BLOCK_BYTES: usize = 8;
const TCP_MAX_SACK_BLOCKS: usize = 4;

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct TcpWireHeader {
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
    pub fn header_len(self) -> usize {
        usize::from(self.data_offset_reserved_flags[0] >> 4) * 4
    }

    #[inline(always)]
    pub fn source_port(self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    pub fn destination_port(self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    pub fn sequence_number(self) -> u32 {
        u32::from_be_bytes(self.sequence_number)
    }

    #[inline(always)]
    pub fn acknowledgment_number(self) -> u32 {
        u32::from_be_bytes(self.acknowledgment_number)
    }

    #[inline(always)]
    pub fn advertised_window(self) -> u16 {
        u16::from_be_bytes(self.advertised_window)
    }

    #[inline(always)]
    pub fn flags(self) -> TcpSegmentFlags {
        let first = self.data_offset_reserved_flags[0];
        let second = self.data_offset_reserved_flags[1];
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

    #[inline(always)]
    fn set_source_port(&mut self, value: u16) {
        self.source_port = [(value >> 8) as u8, value as u8];
    }

    #[inline(always)]
    fn set_destination_port(&mut self, value: u16) {
        self.destination_port = [(value >> 8) as u8, value as u8];
    }

    #[inline(always)]
    fn set_sequence_number(&mut self, value: u32) {
        self.sequence_number = [
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ];
    }

    #[inline(always)]
    fn set_acknowledgment_number(&mut self, value: u32) {
        self.acknowledgment_number = [
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ];
    }

    #[inline(always)]
    fn set_data_offset_flags(&mut self, value: u16) {
        self.data_offset_reserved_flags = [(value >> 8) as u8, value as u8];
    }

    #[inline(always)]
    fn set_advertised_window(&mut self, value: u16) {
        self.advertised_window = [(value >> 8) as u8, value as u8];
    }
}

#[inline]
pub fn tcp_header(packet: &[u8]) -> Result<TcpWireHeader, TcpError> {
    let header = read_header::<TcpWireHeader>(packet, 0).map_err(|_| TcpError::SegmentInvalid)?;
    let header_len = header.header_len();
    if header_len < size_of::<TcpWireHeader>() {
        return Err(TcpError::SegmentInvalid);
    }
    if packet.get(..header_len).is_none() {
        return Err(TcpError::SegmentInvalid);
    }
    Ok(header)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSegmentHeader<'a> {
    pub source_port: u16,
    pub destination_port: u16,
    pub sequence_number: u32,
    pub acknowledgment_number: u32,
    pub flags: TcpSegmentFlags,
    pub advertised_window: u16,
    pub capabilities: TcpCapabilities,
    pub timestamp: Option<TcpTimestampOption>,
    pub fast_open_cookie: Option<&'a TcpFastOpenCookie>,
}

pub fn write_tcp_segment_header(
    output: &mut [u8],
    header: TcpSegmentHeader<'_>,
    sack_blocks: Option<&[TcpSackBlock]>,
) -> Result<usize, TcpError> {
    let header_len = tcp_segment_header_len(header, sack_blocks);
    if output.len() < header_len {
        return Err(TcpError::Length);
    }
    output[..header_len].fill(0);
    let wire = tcp_wire_header_mut(output);
    wire.set_source_port(header.source_port);
    wire.set_destination_port(header.destination_port);
    wire.set_sequence_number(header.sequence_number);
    wire.set_acknowledgment_number(header.acknowledgment_number);
    wire.set_data_offset_flags(tcp_data_offset_flags(header_len, header.flags));
    wire.set_advertised_window(header.advertised_window);
    let options = &mut output[TCP_HEADER_MIN_LEN..header_len];
    write_tcp_options(options, header, sack_blocks);
    Ok(header_len)
}

#[inline]
pub fn tcp_segment_header_len(
    header: TcpSegmentHeader<'_>,
    sack_blocks: Option<&[TcpSackBlock]>,
) -> usize {
    TCP_HEADER_MIN_LEN + tcp_options_len(header, sack_blocks)
}

#[inline(always)]
fn tcp_wire_header_mut(output: &mut [u8]) -> &mut TcpWireHeader {
    let ptr = header_mut_ptr::<TcpWireHeader>(output, 0).expect("tcp header length checked");
    // SAFETY: `header_mut_ptr` checked the header range, and `TcpWireHeader`
    // contains only byte arrays so field access cannot create unaligned
    // references to multi-byte fields.
    unsafe { &mut *ptr }
}

#[inline(always)]
fn tcp_data_offset_flags(header_len: usize, flags: TcpSegmentFlags) -> u16 {
    let mut data_offset_flags = u16::from((header_len / 4) as u8) << 12;
    if flags.contains(TcpSegmentFlags::NS) {
        data_offset_flags |= 0x0100;
    }
    data_offset_flags | (flags.bits() & 0xff)
}

fn tcp_options_len(header: TcpSegmentHeader<'_>, sack_blocks: Option<&[TcpSackBlock]>) -> usize {
    if header.flags.contains(TcpSegmentFlags::SYN) {
        let mut len = 0usize;
        if header.capabilities.max_segment_size.is_some() {
            len += 4;
        }
        if header.capabilities.window_scale.is_some() {
            len += 4;
        }
        if header.capabilities.sack {
            len += 4;
        }
        if header.capabilities.timestamps {
            len += 12;
        }
        if header.capabilities.fast_open {
            len += 2 + header.fast_open_cookie.map_or(0, TcpFastOpenCookie::len);
        }
        if header.capabilities.accurate_ecn {
            len += 3;
        }
        return round_tcp_options_len(len);
    }
    if !header.flags.contains(TcpSegmentFlags::ACK) {
        return 0;
    }
    let mut len = 0usize;
    if header.capabilities.timestamps {
        len += 12;
    }
    let limited_sack_len = sack_blocks.map_or(0, |blocks| blocks.len().min(TCP_MAX_SACK_BLOCKS));
    if limited_sack_len != 0 {
        len += 4 + limited_sack_len * TCP_OPTION_SACK_BLOCK_BYTES;
    }
    round_tcp_options_len(len)
}

fn write_tcp_options(
    output: &mut [u8],
    header: TcpSegmentHeader<'_>,
    sack_blocks: Option<&[TcpSackBlock]>,
) {
    let mut written = 0usize;
    if header.flags.contains(TcpSegmentFlags::SYN) {
        if let Some(max_segment_size) = header.capabilities.max_segment_size {
            output[written] = super::options::TCP_OPTION_MSS_VALUE;
            output[written + 1] = super::options::TCP_OPTION_MSS_LEN_VALUE as u8;
            write_be_u16(output, written + 2, max_segment_size);
            written += 4;
        }
        if let Some(window_scale) = header.capabilities.window_scale {
            output[written] = super::options::TCP_OPTION_NOP_VALUE;
            output[written + 1] = super::options::TCP_OPTION_WINDOW_SCALE_VALUE;
            output[written + 2] = super::options::TCP_OPTION_WINDOW_SCALE_LEN_VALUE as u8;
            output[written + 3] = window_scale.min(super::options::TCP_MAX_WINDOW_SCALE_VALUE);
            written += 4;
        }
        if header.capabilities.sack {
            output[written] = super::options::TCP_OPTION_NOP_VALUE;
            output[written + 1] = super::options::TCP_OPTION_NOP_VALUE;
            output[written + 2] = super::options::TCP_OPTION_SACK_PERMITTED_VALUE;
            output[written + 3] = super::options::TCP_OPTION_SACK_PERMITTED_LEN_VALUE as u8;
            written += 4;
        }
        if header.capabilities.timestamps {
            written = write_tcp_timestamp_option(output, written, header.timestamp);
        }
        if header.capabilities.fast_open {
            let cookie = header
                .fast_open_cookie
                .map_or(&[][..], TcpFastOpenCookie::as_slice);
            output[written] = super::options::TCP_OPTION_FAST_OPEN_VALUE;
            output[written + 1] = (2 + cookie.len()) as u8;
            let mut index = 0usize;
            while index < cookie.len() {
                output[written + 2 + index] = cookie[index];
                index += 1;
            }
            written += 2 + cookie.len();
        }
        if header.capabilities.accurate_ecn {
            output[written] = super::options::TCP_OPTION_NOP_VALUE;
            output[written + 1] = super::options::TCP_OPTION_ACCURATE_ECN_ORDER_0_VALUE;
            output[written + 2] = 2;
            written += 3;
        }
    } else if header.flags.contains(TcpSegmentFlags::ACK) {
        if header.capabilities.timestamps {
            written = write_tcp_timestamp_option(output, written, header.timestamp);
        }
        if let Some(sack_blocks) = sack_blocks {
            let limited_sack_len = sack_blocks.len().min(TCP_MAX_SACK_BLOCKS);
            if limited_sack_len != 0 {
                output[written] = TCP_OPTION_NOP;
                output[written + 1] = TCP_OPTION_NOP;
                output[written + 2] = TCP_OPTION_SACK;
                output[written + 3] = (2 + limited_sack_len * TCP_OPTION_SACK_BLOCK_BYTES) as u8;
                written += 4;
                for block in &sack_blocks[..limited_sack_len] {
                    write_be_u32(output, written, u32::from(block.left_edge));
                    write_be_u32(output, written + 4, u32::from(block.right_edge));
                    written += TCP_OPTION_SACK_BLOCK_BYTES;
                }
            }
        }
    }
    output[written..].fill(TCP_OPTION_EOL);
}

#[inline]
fn write_tcp_timestamp_option(
    output: &mut [u8],
    offset: usize,
    timestamp: Option<TcpTimestampOption>,
) -> usize {
    let timestamp = timestamp.unwrap_or(TcpTimestampOption { tsval: 0, tsecr: 0 });
    output[offset] = super::options::TCP_OPTION_NOP_VALUE;
    output[offset + 1] = super::options::TCP_OPTION_NOP_VALUE;
    output[offset + 2] = super::options::TCP_OPTION_TIMESTAMPS_VALUE;
    output[offset + 3] = super::options::TCP_OPTION_TIMESTAMPS_LEN_VALUE as u8;
    write_be_u32(output, offset + 4, timestamp.tsval);
    write_be_u32(output, offset + 8, timestamp.tsecr);
    offset + 12
}

#[inline(always)]
fn write_be_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset] = (value >> 8) as u8;
    output[offset + 1] = value as u8;
}

#[inline(always)]
fn write_be_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset] = (value >> 24) as u8;
    output[offset + 1] = (value >> 16) as u8;
    output[offset + 2] = (value >> 8) as u8;
    output[offset + 3] = value as u8;
}

#[inline]
const fn round_tcp_options_len(len: usize) -> usize {
    (len + 3) & !3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_segment_header_writes_fast_open_cookie_from_value_type() {
        let cookie: TcpFastOpenCookie = (&[1, 2, 3, 4][..]).try_into().expect("cookie");
        let mut output = [0u8; 64];

        let written = write_tcp_segment_header(
            &mut output,
            TcpSegmentHeader {
                source_port: 1000,
                destination_port: 2000,
                sequence_number: 1,
                acknowledgment_number: 0,
                flags: TcpSegmentFlags::SYN,
                advertised_window: 4096,
                capabilities: TcpCapabilities {
                    fast_open: true,
                    ..TcpCapabilities::default()
                },
                timestamp: None,
                fast_open_cookie: Some(&cookie),
            },
            None,
        )
        .expect("write header");

        assert_eq!(
            &output[TCP_HEADER_MIN_LEN..written],
            &[
                super::super::options::TCP_OPTION_FAST_OPEN_VALUE,
                6,
                1,
                2,
                3,
                4,
                0,
                0
            ]
        );
    }
}
