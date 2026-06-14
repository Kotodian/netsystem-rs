use super::TcpCapabilities;

const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_MSS: u8 = 2;
const TCP_OPTION_WINDOW_SCALE: u8 = 3;
const TCP_OPTION_SACK_PERMITTED: u8 = 4;
const TCP_OPTION_SACK: u8 = 5;
const TCP_OPTION_TIMESTAMPS: u8 = 8;
const TCP_OPTION_ACCURATE_ECN_ORDER_0: u8 = 172;
const TCP_OPTION_ACCURATE_ECN_ORDER_1: u8 = 174;
const TCP_OPTION_MSS_LEN: usize = 4;
const TCP_OPTION_WINDOW_SCALE_LEN: usize = 3;
const TCP_OPTION_SACK_PERMITTED_LEN: usize = 2;
const TCP_OPTION_TIMESTAMPS_LEN: usize = 10;
const TCP_OPTION_SACK_BLOCK_BYTES: usize = 8;
const TCP_MAX_SACK_BLOCKS: usize = 4;
const TCP_MAX_WINDOW_SCALE: u8 = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSackBlock {
    pub left_edge: u32,
    pub right_edge: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTimestampOption {
    pub tsval: u32,
    pub tsecr: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedTcpOptions {
    pub capabilities: TcpCapabilities,
    pub sack_blocks: std::vec::Vec<TcpSackBlock>,
    pub timestamp: Option<TcpTimestampOption>,
}

#[inline]
pub fn tcp_capabilities_from_options(options: &[u8]) -> TcpCapabilities {
    tcp_options_from_bytes(options).capabilities
}

pub fn tcp_options_from_bytes(options: &[u8]) -> ParsedTcpOptions {
    let mut parsed = ParsedTcpOptions::default();
    let mut index = 0;
    while index < options.len() {
        match options[index] {
            TCP_OPTION_EOL => break,
            TCP_OPTION_NOP => {
                index += 1;
            }
            kind => {
                let Some(len) = options.get(index + 1).copied().map(usize::from) else {
                    break;
                };
                if len < 2 || len > options.len() - index {
                    break;
                }
                match kind {
                    TCP_OPTION_MSS if len == TCP_OPTION_MSS_LEN => {
                        parsed.capabilities.max_segment_size =
                            Some(u16::from_be_bytes([options[index + 2], options[index + 3]]));
                    }
                    TCP_OPTION_WINDOW_SCALE if len == TCP_OPTION_WINDOW_SCALE_LEN => {
                        parsed.capabilities.window_scale =
                            Some(options[index + 2].min(TCP_MAX_WINDOW_SCALE));
                    }
                    TCP_OPTION_SACK_PERMITTED if len == TCP_OPTION_SACK_PERMITTED_LEN => {
                        parsed.capabilities.sack = true;
                    }
                    TCP_OPTION_SACK if is_valid_sack_option_len(len) => {
                        for block in options[index + 2..index + len]
                            .chunks_exact(TCP_OPTION_SACK_BLOCK_BYTES)
                            .take(TCP_MAX_SACK_BLOCKS)
                        {
                            parsed.sack_blocks.push(TcpSackBlock {
                                left_edge: u32::from_be_bytes([
                                    block[0], block[1], block[2], block[3],
                                ]),
                                right_edge: u32::from_be_bytes([
                                    block[4], block[5], block[6], block[7],
                                ]),
                            });
                        }
                    }
                    TCP_OPTION_TIMESTAMPS if len == TCP_OPTION_TIMESTAMPS_LEN => {
                        parsed.capabilities.timestamps = true;
                        parsed.timestamp = Some(TcpTimestampOption {
                            tsval: u32::from_be_bytes([
                                options[index + 2],
                                options[index + 3],
                                options[index + 4],
                                options[index + 5],
                            ]),
                            tsecr: u32::from_be_bytes([
                                options[index + 6],
                                options[index + 7],
                                options[index + 8],
                                options[index + 9],
                            ]),
                        });
                    }
                    TCP_OPTION_ACCURATE_ECN_ORDER_0 | TCP_OPTION_ACCURATE_ECN_ORDER_1 => {
                        parsed.capabilities.ecn = true;
                    }
                    _ => {}
                }
                index += len;
            }
        }
    }
    parsed
}

#[inline]
fn is_valid_sack_option_len(len: usize) -> bool {
    len > 2 && (len - 2) % TCP_OPTION_SACK_BLOCK_BYTES == 0
}
