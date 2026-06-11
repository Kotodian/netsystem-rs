use hammer_core::protocol::tcp::TcpCapabilities;

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
pub(super) struct TcpSackBlock {
    pub(super) left_edge: u32,
    pub(super) right_edge: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TcpTimestampOption {
    pub(super) tsval: u32,
    pub(super) tsecr: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ParsedTcpOptions {
    pub(super) capabilities: TcpCapabilities,
    pub(super) sack_blocks: Vec<TcpSackBlock>,
    pub(super) timestamp: Option<TcpTimestampOption>,
}

pub(super) fn tcp_capabilities_from_options(options: &[u8]) -> TcpCapabilities {
    tcp_options_from_bytes(options).capabilities
}

pub(super) fn tcp_options_from_bytes(options: &[u8]) -> ParsedTcpOptions {
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

fn is_valid_sack_option_len(len: usize) -> bool {
    len > 2 && (len - 2) % TCP_OPTION_SACK_BLOCK_BYTES == 0
}

#[cfg(test)]
mod tests {
    use super::{
        TcpSackBlock, TcpTimestampOption, tcp_capabilities_from_options, tcp_options_from_bytes,
    };

    #[test]
    fn parses_max_segment_size_option() {
        let capabilities = tcp_capabilities_from_options(&[2, 4, 0x05, 0xb4]);

        assert_eq!(capabilities.max_segment_size, Some(1_460));
    }

    #[test]
    fn parses_common_capability_options() {
        let capabilities = tcp_capabilities_from_options(&[
            2, 4, 0x05, 0xb4, 1, 3, 3, 7, 4, 2, 8, 10, 0, 0, 0, 1, 0, 0, 0, 2,
        ]);

        assert_eq!(capabilities.max_segment_size, Some(1_460));
        assert_eq!(capabilities.window_scale, Some(7));
        assert!(capabilities.sack);
        assert!(capabilities.timestamps);
        assert!(!capabilities.ecn);
    }

    #[test]
    fn clamps_window_scale_to_protocol_maximum() {
        let capabilities = tcp_capabilities_from_options(&[3, 3, 15]);

        assert_eq!(capabilities.window_scale, Some(14));
    }

    #[test]
    fn parses_accurate_ecn_option_as_ecn_capability() {
        let capabilities =
            tcp_capabilities_from_options(&[172, 2, 174, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

        assert!(capabilities.ecn);
    }

    #[test]
    fn skips_known_options_with_unexpected_lengths() {
        let capabilities = tcp_capabilities_from_options(&[3, 2, 4, 3, 0, 8, 2, 2, 4, 0x04, 0xc4]);

        assert_eq!(capabilities.max_segment_size, Some(1_220));
        assert_eq!(capabilities.window_scale, None);
        assert!(!capabilities.sack);
        assert!(!capabilities.timestamps);
        assert!(!capabilities.ecn);
    }

    #[test]
    fn skips_nop_and_stops_at_eol() {
        let capabilities = tcp_capabilities_from_options(&[1, 2, 4, 0x04, 0xc4, 0, 2, 4, 0x01, 0]);

        assert_eq!(capabilities.max_segment_size, Some(1_220));
    }

    #[test]
    fn parses_sack_blocks_with_bounded_storage() {
        let options = sack_option(&[(10, 20), (30, 40), (50, 60), (70, 80), (90, 100)]);

        let parsed = tcp_options_from_bytes(&options);

        assert_eq!(parsed.sack_blocks.len(), 4);
        assert_eq!(
            parsed.sack_blocks.as_slice(),
            &[
                TcpSackBlock {
                    left_edge: 10,
                    right_edge: 20
                },
                TcpSackBlock {
                    left_edge: 30,
                    right_edge: 40
                },
                TcpSackBlock {
                    left_edge: 50,
                    right_edge: 60
                },
                TcpSackBlock {
                    left_edge: 70,
                    right_edge: 80
                },
            ]
        );
    }

    #[test]
    fn skips_sack_blocks_with_invalid_lengths() {
        let parsed = tcp_options_from_bytes(&[5, 9, 0, 0, 0, 10, 0, 0, 0, 2, 4, 0x04, 0xc4]);

        assert!(parsed.sack_blocks.is_empty());
        assert_eq!(parsed.capabilities.max_segment_size, Some(1_220));
    }

    #[test]
    fn parses_timestamp_values_and_preserves_capability_flag() {
        let parsed =
            tcp_options_from_bytes(&[8, 10, 0x01, 0x02, 0x03, 0x04, 0xa1, 0xa2, 0xa3, 0xa4]);
        let capabilities =
            tcp_capabilities_from_options(&[8, 10, 0x01, 0x02, 0x03, 0x04, 0xa1, 0xa2, 0xa3, 0xa4]);

        assert_eq!(
            parsed.timestamp,
            Some(TcpTimestampOption {
                tsval: 0x0102_0304,
                tsecr: 0xa1a2_a3a4
            })
        );
        assert!(parsed.capabilities.timestamps);
        assert!(capabilities.timestamps);
    }

    fn sack_option(blocks: &[(u32, u32)]) -> Vec<u8> {
        let mut option = vec![5, (2 + blocks.len() * 8) as u8];
        for (left_edge, right_edge) in blocks {
            option.extend_from_slice(&left_edge.to_be_bytes());
            option.extend_from_slice(&right_edge.to_be_bytes());
        }
        option
    }
}
