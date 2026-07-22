use super::{TcpCapabilities, TcpFastOpenCookie, TcpSeq};

const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_MSS: u8 = 2;
const TCP_OPTION_WINDOW_SCALE: u8 = 3;
const TCP_OPTION_SACK_PERMITTED: u8 = 4;
const TCP_OPTION_SACK: u8 = 5;
const TCP_OPTION_TIMESTAMPS: u8 = 8;
const TCP_OPTION_FAST_OPEN: u8 = 34;
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
    pub left_edge: TcpSeq,
    pub right_edge: TcpSeq,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTimestampOption {
    pub tsval: u32,
    pub tsecr: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedTcpOptions {
    pub capabilities: TcpCapabilities,
    pub sack_blocks: Vec<TcpSackBlock>,
    pub timestamp: Option<TcpTimestampOption>,
    pub fast_open_cookie: Option<TcpFastOpenCookie>,
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
                                left_edge: TcpSeq::from(u32::from_be_bytes([
                                    block[0], block[1], block[2], block[3],
                                ])),
                                right_edge: TcpSeq::from(u32::from_be_bytes([
                                    block[4], block[5], block[6], block[7],
                                ])),
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
                    TCP_OPTION_FAST_OPEN if is_valid_fast_open_option_len(len) => {
                        parsed.capabilities.fast_open = true;
                        let cookie_len = len - 2;
                        if cookie_len != 0 {
                            parsed.fast_open_cookie =
                                (&options[index + 2..index + len]).try_into().ok();
                        }
                    }
                    TCP_OPTION_ACCURATE_ECN_ORDER_0 | TCP_OPTION_ACCURATE_ECN_ORDER_1 => {
                        parsed.capabilities.ecn = true;
                        parsed.capabilities.accurate_ecn = true;
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
pub(crate) fn is_valid_sack_option_len(len: usize) -> bool {
    len > 2 && (len - 2) % TCP_OPTION_SACK_BLOCK_BYTES == 0
}

#[inline]
pub(crate) fn is_valid_fast_open_option_len(len: usize) -> bool {
    let cookie_len = len.saturating_sub(2);
    cookie_len == 0 || TcpFastOpenCookie::is_valid_len(cookie_len)
}

pub(crate) const TCP_OPTION_NOP_VALUE: u8 = TCP_OPTION_NOP;
pub(crate) const TCP_OPTION_MSS_VALUE: u8 = TCP_OPTION_MSS;
pub(crate) const TCP_OPTION_WINDOW_SCALE_VALUE: u8 = TCP_OPTION_WINDOW_SCALE;
pub(crate) const TCP_OPTION_SACK_PERMITTED_VALUE: u8 = TCP_OPTION_SACK_PERMITTED;
pub(crate) const TCP_OPTION_TIMESTAMPS_VALUE: u8 = TCP_OPTION_TIMESTAMPS;
pub(crate) const TCP_OPTION_FAST_OPEN_VALUE: u8 = TCP_OPTION_FAST_OPEN;
pub(crate) const TCP_OPTION_ACCURATE_ECN_ORDER_0_VALUE: u8 = TCP_OPTION_ACCURATE_ECN_ORDER_0;
pub(crate) const TCP_OPTION_MSS_LEN_VALUE: usize = TCP_OPTION_MSS_LEN;
pub(crate) const TCP_OPTION_WINDOW_SCALE_LEN_VALUE: usize = TCP_OPTION_WINDOW_SCALE_LEN;
pub(crate) const TCP_OPTION_SACK_PERMITTED_LEN_VALUE: usize = TCP_OPTION_SACK_PERMITTED_LEN;
pub(crate) const TCP_OPTION_TIMESTAMPS_LEN_VALUE: usize = TCP_OPTION_TIMESTAMPS_LEN;
pub(crate) const TCP_MAX_WINDOW_SCALE_VALUE: u8 = TCP_MAX_WINDOW_SCALE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_options_parse_fast_open_cookie_as_value_type() {
        let options = [TCP_OPTION_FAST_OPEN, 6, 1, 2, 3, 4];

        let parsed = tcp_options_from_bytes(&options);

        assert!(parsed.capabilities.fast_open);
        assert_eq!(
            parsed
                .fast_open_cookie
                .as_ref()
                .map(TcpFastOpenCookie::as_slice),
            Some(&[1, 2, 3, 4][..])
        );
    }
}
