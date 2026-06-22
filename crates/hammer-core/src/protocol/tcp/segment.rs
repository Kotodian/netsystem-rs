use super::{TcpCapabilities, TcpSackBlock, TcpSegmentFlags};
use crate::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;

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
    pub fast_open_cookie: Option<&'a [u8]>,
}

pub fn write_tcp_segment_header(
    output: &mut [u8],
    header: TcpSegmentHeader<'_>,
    sack_blocks: Option<&[TcpSackBlock]>,
) -> CoreResult<usize> {
    let options = if header.flags.contains(TcpSegmentFlags::SYN) {
        let mut options = Vec::new();
        if let Some(max_segment_size) = header.capabilities.max_segment_size {
            options.extend([
                super::options::TCP_OPTION_MSS_VALUE,
                super::options::TCP_OPTION_MSS_LEN_VALUE as u8,
            ]);
            options.extend(max_segment_size.to_be_bytes());
        }
        if let Some(window_scale) = header.capabilities.window_scale {
            options.extend([
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_WINDOW_SCALE_VALUE,
                super::options::TCP_OPTION_WINDOW_SCALE_LEN_VALUE as u8,
                window_scale.min(super::options::TCP_MAX_WINDOW_SCALE_VALUE),
            ]);
        }
        if header.capabilities.sack {
            options.extend([
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_SACK_PERMITTED_VALUE,
                super::options::TCP_OPTION_SACK_PERMITTED_LEN_VALUE as u8,
            ]);
        }
        if header.capabilities.timestamps {
            options.extend([
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_TIMESTAMPS_VALUE,
                super::options::TCP_OPTION_TIMESTAMPS_LEN_VALUE as u8,
            ]);
            options.extend(0u32.to_be_bytes());
            options.extend(0u32.to_be_bytes());
        }
        if header.capabilities.fast_open {
            let cookie_len = header.fast_open_cookie.map_or(0, <[u8]>::len);
            options.extend([
                super::options::TCP_OPTION_FAST_OPEN_VALUE,
                (2 + cookie_len) as u8,
            ]);
            if let Some(cookie) = header.fast_open_cookie {
                options.extend_from_slice(cookie);
            }
        }
        if header.capabilities.accurate_ecn {
            options.extend([
                super::options::TCP_OPTION_NOP_VALUE,
                super::options::TCP_OPTION_ACCURATE_ECN_ORDER_0_VALUE,
                2,
            ]);
        }
        while options.len() % 4 != 0 {
            options.push(super::options::TCP_OPTION_EOL_VALUE);
        }
        options
    } else if header.flags.contains(TcpSegmentFlags::ACK) {
        if let Some(sack_blocks) = sack_blocks.filter(|blocks| !blocks.is_empty()) {
            let limited_len = sack_blocks.len().min(TCP_MAX_SACK_BLOCKS);
            let mut options = Vec::with_capacity(2 + limited_len * TCP_OPTION_SACK_BLOCK_BYTES + 3);
            options.extend([TCP_OPTION_NOP, TCP_OPTION_NOP, TCP_OPTION_SACK]);
            options.push((2 + limited_len * TCP_OPTION_SACK_BLOCK_BYTES) as u8);
            for block in &sack_blocks[..limited_len] {
                options.extend(block.left_edge.to_be_bytes());
                options.extend(block.right_edge.to_be_bytes());
            }
            while options.len() % 4 != 0 {
                options.push(TCP_OPTION_EOL);
            }
            options
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let header_len = TCP_HEADER_MIN_LEN + options.len();
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
    output[TCP_HEADER_MIN_LEN..header_len].copy_from_slice(&options);
    Ok(header_len)
}
