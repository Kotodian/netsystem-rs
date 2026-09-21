use super::{TcpCapabilities, TcpError, TcpFastOpenCookie, TcpSeq};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpOption<'a> {
    End,
    NoOperation,
    MaximumSegmentSize(u16),
    WindowScale(u8),
    SackPermitted,
    SackBlock(TcpSackBlock),
    Timestamp(TcpTimestampOption),
    FastOpenCookie(&'a [u8]),
    AccurateEcn,
    Unknown { number: u8, bytes: &'a [u8] },
}

pub struct TcpOptionIter<'a> {
    options: &'a [u8],
    offset: usize,
    sack_blocks: &'a [u8],
    sack_offset: usize,
    finished: bool,
}

impl<'a> TcpOptionIter<'a> {
    #[inline]
    pub const fn new(options: &'a [u8]) -> Self {
        Self {
            options,
            offset: 0,
            sack_blocks: &[],
            sack_offset: 0,
            finished: false,
        }
    }

    #[inline(always)]
    fn stop_with_error(&mut self) -> Option<Result<TcpOption<'a>, TcpError>> {
        self.finished = true;
        Some(Err(TcpError::SegmentInvalid))
    }
}

impl<'a> Iterator for TcpOptionIter<'a> {
    type Item = Result<TcpOption<'a>, TcpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sack_offset < self.sack_blocks.len() {
            let block =
                &self.sack_blocks[self.sack_offset..self.sack_offset + TCP_OPTION_SACK_BLOCK_BYTES];
            self.sack_offset += TCP_OPTION_SACK_BLOCK_BYTES;
            return Some(Ok(TcpOption::SackBlock(TcpSackBlock {
                left_edge: TcpSeq::from(u32::from_be_bytes(block[..4].try_into().unwrap())),
                right_edge: TcpSeq::from(u32::from_be_bytes(block[4..].try_into().unwrap())),
            })));
        }
        if self.finished || self.offset >= self.options.len() {
            return None;
        }

        let number = self.options[self.offset];
        match number {
            TCP_OPTION_EOL => {
                self.finished = true;
                Some(Ok(TcpOption::End))
            }
            TCP_OPTION_NOP => {
                self.offset += 1;
                Some(Ok(TcpOption::NoOperation))
            }
            _ => {
                let Some(len) = self.options.get(self.offset + 1).copied().map(usize::from) else {
                    return self.stop_with_error();
                };
                if len < 2 || len > self.options.len() - self.offset {
                    return self.stop_with_error();
                }
                let option = &self.options[self.offset..self.offset + len];
                self.offset += len;
                match number {
                    TCP_OPTION_MSS if len == TCP_OPTION_MSS_LEN => {
                        Some(Ok(TcpOption::MaximumSegmentSize(u16::from_be_bytes(
                            option[2..4].try_into().unwrap(),
                        ))))
                    }
                    TCP_OPTION_WINDOW_SCALE if len == TCP_OPTION_WINDOW_SCALE_LEN => Some(Ok(
                        TcpOption::WindowScale(option[2].min(TCP_MAX_WINDOW_SCALE)),
                    )),
                    TCP_OPTION_SACK_PERMITTED if len == TCP_OPTION_SACK_PERMITTED_LEN => {
                        Some(Ok(TcpOption::SackPermitted))
                    }
                    TCP_OPTION_SACK if is_valid_sack_option_len(len) => {
                        let sack_len =
                            (len - 2).min(TCP_MAX_SACK_BLOCKS * TCP_OPTION_SACK_BLOCK_BYTES);
                        self.sack_blocks = &option[2..2 + sack_len];
                        self.sack_offset = TCP_OPTION_SACK_BLOCK_BYTES;
                        let block = &self.sack_blocks[..TCP_OPTION_SACK_BLOCK_BYTES];
                        Some(Ok(TcpOption::SackBlock(TcpSackBlock {
                            left_edge: TcpSeq::from(u32::from_be_bytes(
                                block[..4].try_into().unwrap(),
                            )),
                            right_edge: TcpSeq::from(u32::from_be_bytes(
                                block[4..].try_into().unwrap(),
                            )),
                        })))
                    }
                    TCP_OPTION_TIMESTAMPS if len == TCP_OPTION_TIMESTAMPS_LEN => {
                        Some(Ok(TcpOption::Timestamp(TcpTimestampOption {
                            tsval: u32::from_be_bytes(option[2..6].try_into().unwrap()),
                            tsecr: u32::from_be_bytes(option[6..10].try_into().unwrap()),
                        })))
                    }
                    TCP_OPTION_FAST_OPEN if is_valid_fast_open_option_len(len) => {
                        Some(Ok(TcpOption::FastOpenCookie(&option[2..])))
                    }
                    TCP_OPTION_ACCURATE_ECN_ORDER_0 | TCP_OPTION_ACCURATE_ECN_ORDER_1 => {
                        Some(Ok(TcpOption::AccurateEcn))
                    }
                    TCP_OPTION_MSS
                    | TCP_OPTION_WINDOW_SCALE
                    | TCP_OPTION_SACK_PERMITTED
                    | TCP_OPTION_SACK
                    | TCP_OPTION_TIMESTAMPS
                    | TCP_OPTION_FAST_OPEN => self.stop_with_error(),
                    _ => Some(Ok(TcpOption::Unknown {
                        number,
                        bytes: &option[2..],
                    })),
                }
            }
        }
    }
}

#[inline]
pub fn tcp_capabilities_from_options(options: &[u8]) -> Result<TcpCapabilities, TcpError> {
    let mut capabilities = TcpCapabilities::default();
    for option in TcpOptionIter::new(options) {
        match option? {
            TcpOption::MaximumSegmentSize(value) => capabilities.max_segment_size = Some(value),
            TcpOption::WindowScale(value) => capabilities.window_scale = Some(value),
            TcpOption::SackPermitted => capabilities.sack = true,
            TcpOption::Timestamp(_) => capabilities.timestamps = true,
            TcpOption::FastOpenCookie(_) => capabilities.fast_open = true,
            TcpOption::AccurateEcn => {
                capabilities.ecn = true;
                capabilities.accurate_ecn = true;
            }
            TcpOption::End
            | TcpOption::NoOperation
            | TcpOption::SackBlock(_)
            | TcpOption::Unknown { .. } => {}
        }
    }
    Ok(capabilities)
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
