use super::{TcpCapabilities, TcpSackBlock, TcpSegmentFlags, TcpTimestampOption};
use crate::error::{CoreError, CoreResult};

const TCP_HEADER_MIN_LEN: usize = 20;
const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_SACK: u8 = 5;
const TCP_OPTION_SACK_BLOCK_BYTES: usize = 8;
const TCP_MAX_SACK_BLOCKS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpSegmentParseError {
    ShortHeader,
    BadDataOffset,
    InvalidSlice,
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
    pub fast_open_cookie: Option<&'a [u8]>,
}

pub fn write_tcp_segment_header(
    output: &mut [u8],
    header: TcpSegmentHeader<'_>,
    sack_blocks: Option<&[TcpSackBlock]>,
) -> CoreResult<usize> {
    let options_len = tcp_options_len(header, sack_blocks);
    let header_len = TCP_HEADER_MIN_LEN + options_len;
    if output.len() < header_len {
        return Err(CoreError::internal(format!(
            "tcp segment output too small: {} < {}",
            output.len(),
            header_len
        )));
    }
    output[..header_len].fill(0);
    output[0..2].copy_from_slice(&header.source_port.to_be_bytes());
    output[2..4].copy_from_slice(&header.destination_port.to_be_bytes());
    output[4..8].copy_from_slice(&header.sequence_number.to_be_bytes());
    output[8..12].copy_from_slice(&header.acknowledgment_number.to_be_bytes());
    output[12] = ((header_len / 4) as u8) << 4;
    if header.flags.contains(TcpSegmentFlags::NS) {
        output[12] |= 0x01;
    }
    output[13] = (header.flags.bits() & 0xff) as u8;
    output[14..16].copy_from_slice(&header.advertised_window.to_be_bytes());
    let options = &mut output[TCP_HEADER_MIN_LEN..header_len];
    write_tcp_options(options, header, sack_blocks);
    Ok(header_len)
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
            len += 2 + header.fast_open_cookie.map_or(0, <[u8]>::len);
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
            let cookie = header.fast_open_cookie.unwrap_or(&[]);
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
