use std::mem::transmute;

use super::{
    TcpCapabilities, TcpError, TcpFastOpenCookie, TcpSackBlock, TcpSegmentFlags, TcpTimestampOption,
};
use thiserror::Error;

const TCP_HEADER_MIN_LEN: usize = 20;
const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_SACK: u8 = 5;
const TCP_OPTION_SACK_BLOCK_BYTES: usize = 8;
const TCP_MAX_SACK_BLOCKS: usize = 4;

#[repr(C, packed)]
struct TcpWireHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    sequence_number: [u8; 4],
    acknowledgment_number: [u8; 4],
    data_offset_reserved_flags: [u8; 2],
    advertised_window: [u8; 2],
    checksum: [u8; 2],
    urgent_pointer: [u8; 2],
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum TcpSegmentParseError {
    #[error("tcp packet has invalid IP header")]
    InvalidIpHeader,
    #[error("packet is not TCP")]
    WrongProtocol,
    #[error("tcp packet length is invalid")]
    InvalidPacketLength,
    #[error("tcp transport header is missing")]
    MissingTransportHeader,
    #[error("tcp payload offset overflow")]
    PayloadOffsetOverflow,
    #[error("tcp payload offset exceeds packet length")]
    PayloadOffsetExceedsPacketLength,
    #[error("tcp segment intent is missing")]
    MissingIntent,
    #[error("missing IPv4 source")]
    MissingIpv4Source,
    #[error("missing IPv6 source")]
    MissingIpv6Source,
    #[error("invalid IPv6 source length")]
    InvalidIpv6SourceLength,
    #[error("missing IPv4 destination")]
    MissingIpv4Destination,
    #[error("missing IPv6 destination")]
    MissingIpv6Destination,
    #[error("invalid IPv6 destination length")]
    InvalidIpv6DestinationLength,
    #[error("tcp segment is invalid")]
    InvalidSegment,
    #[error("tcp segment is invalid")]
    ShortHeader,
    #[error("tcp segment is invalid")]
    BadDataOffset,
    #[error("tcp segment is invalid")]
    InvalidSlice,
}

impl From<TcpSegmentParseError> for TcpError {
    #[inline]
    fn from(error: TcpSegmentParseError) -> Self {
        match error {
            TcpSegmentParseError::WrongProtocol => TcpError::Dispatch,
            TcpSegmentParseError::MissingIntent => TcpError::Dispatch,
            TcpSegmentParseError::InvalidIpHeader
            | TcpSegmentParseError::InvalidPacketLength
            | TcpSegmentParseError::MissingTransportHeader
            | TcpSegmentParseError::PayloadOffsetOverflow
            | TcpSegmentParseError::PayloadOffsetExceedsPacketLength
            | TcpSegmentParseError::MissingIpv4Source
            | TcpSegmentParseError::MissingIpv6Source
            | TcpSegmentParseError::InvalidIpv6SourceLength
            | TcpSegmentParseError::MissingIpv4Destination
            | TcpSegmentParseError::MissingIpv6Destination
            | TcpSegmentParseError::InvalidIpv6DestinationLength
            | TcpSegmentParseError::InvalidSegment
            | TcpSegmentParseError::ShortHeader
            | TcpSegmentParseError::BadDataOffset
            | TcpSegmentParseError::InvalidSlice => TcpError::SegmentInvalid,
        }
    }
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
    wire.source_port = header.source_port.to_be_bytes();
    wire.destination_port = header.destination_port.to_be_bytes();
    wire.sequence_number = header.sequence_number.to_be_bytes();
    wire.acknowledgment_number = header.acknowledgment_number.to_be_bytes();
    wire.data_offset_reserved_flags = tcp_data_offset_flags(header_len, header.flags).to_be_bytes();
    wire.advertised_window = header.advertised_window.to_be_bytes();
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
    // SAFETY: caller guarantees `output` has at least `TCP_HEADER_MIN_LEN`
    // bytes. `TcpWireHeader` is packed, so alignment is 1.
    unsafe { &mut *transmute::<_, *mut TcpWireHeader>(output.as_mut_ptr()) }
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
            output[written..written + 2].copy_from_slice(&[
                super::options::TCP_OPTION_MSS_VALUE,
                super::options::TCP_OPTION_MSS_LEN_VALUE as u8,
            ]);
            output[written + 2..written + 4].copy_from_slice(&max_segment_size.to_be_bytes());
            written += 4;
        }
        if let Some(window_scale) = header.capabilities.window_scale {
            output[written..written + 4].copy_from_slice(&[
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_WINDOW_SCALE_VALUE,
                super::options::TCP_OPTION_WINDOW_SCALE_LEN_VALUE as u8,
                window_scale.min(super::options::TCP_MAX_WINDOW_SCALE_VALUE),
            ]);
            written += 4;
        }
        if header.capabilities.sack {
            output[written..written + 4].copy_from_slice(&[
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_SACK_PERMITTED_VALUE,
                super::options::TCP_OPTION_SACK_PERMITTED_LEN_VALUE as u8,
            ]);
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
            output[written + 2..written + 2 + cookie.len()].copy_from_slice(cookie);
            written += 2 + cookie.len();
        }
        if header.capabilities.accurate_ecn {
            output[written..written + 3].copy_from_slice(&[
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_ACCURATE_ECN_ORDER_0_VALUE,
                2,
            ]);
            written += 3;
        }
    } else if header.flags.contains(TcpSegmentFlags::ACK) {
        if header.capabilities.timestamps {
            written = write_tcp_timestamp_option(output, written, header.timestamp);
        }
        if let Some(sack_blocks) = sack_blocks {
            let limited_sack_len = sack_blocks.len().min(TCP_MAX_SACK_BLOCKS);
            if limited_sack_len != 0 {
                output[written..written + 4].copy_from_slice(&[
                    TCP_OPTION_NOP,
                    TCP_OPTION_NOP,
                    TCP_OPTION_SACK,
                    (2 + limited_sack_len * TCP_OPTION_SACK_BLOCK_BYTES) as u8,
                ]);
                written += 4;
                for block in &sack_blocks[..limited_sack_len] {
                    output[written..written + 4]
                        .copy_from_slice(&u32::from(block.left_edge).to_be_bytes());
                    output[written + 4..written + 8]
                        .copy_from_slice(&u32::from(block.right_edge).to_be_bytes());
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
    output[offset..offset + 4].copy_from_slice(&[
        super::options::TCP_OPTION_NOP_VALUE,
        super::options::TCP_OPTION_NOP_VALUE,
        super::options::TCP_OPTION_TIMESTAMPS_VALUE,
        super::options::TCP_OPTION_TIMESTAMPS_LEN_VALUE as u8,
    ]);
    output[offset + 4..offset + 8].copy_from_slice(&timestamp.tsval.to_be_bytes());
    output[offset + 8..offset + 12].copy_from_slice(&timestamp.tsecr.to_be_bytes());
    offset + 12
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
