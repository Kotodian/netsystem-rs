use hammer_core::protocol::tcp::TcpCapabilities;

const TCP_OPTION_EOL: u8 = 0;
const TCP_OPTION_NOP: u8 = 1;
const TCP_OPTION_MSS: u8 = 2;
const TCP_OPTION_MSS_LEN: usize = 4;

pub(super) fn tcp_capabilities_from_options(options: &[u8]) -> TcpCapabilities {
    let mut capabilities = TcpCapabilities::default();
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
                if len < 2 || index + len > options.len() {
                    break;
                }
                if kind == TCP_OPTION_MSS && len == TCP_OPTION_MSS_LEN {
                    capabilities.max_segment_size =
                        Some(u16::from_be_bytes([options[index + 2], options[index + 3]]));
                }
                index += len;
            }
        }
    }
    capabilities
}

#[cfg(test)]
mod tests {
    use super::tcp_capabilities_from_options;

    #[test]
    fn parses_max_segment_size_option() {
        let capabilities = tcp_capabilities_from_options(&[2, 4, 0x05, 0xb4]);

        assert_eq!(capabilities.max_segment_size, Some(1_460));
    }

    #[test]
    fn skips_nop_and_stops_at_eol() {
        let capabilities = tcp_capabilities_from_options(&[1, 2, 4, 0x04, 0xc4, 0, 2, 4, 0x01, 0]);

        assert_eq!(capabilities.max_segment_size, Some(1_220));
    }
}
