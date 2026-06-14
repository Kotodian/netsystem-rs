use super::{TcpCapabilities, TcpSegmentFlags};
use crate::error::{CoreError, CoreResult};

const TCP_HEADER_MIN_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpSegmentParseError {
    ShortHeader,
    BadDataOffset,
    InvalidSlice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSegmentView<'a> {
    source_port: u16,
    destination_port: u16,
    sequence_number: u32,
    acknowledgment_number: Option<u32>,
    advertised_window: u16,
    flags: TcpSegmentFlags,
    header_len: usize,
    options: &'a [u8],
    payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpSegmentHeader {
    pub source_port: u16,
    pub destination_port: u16,
    pub sequence_number: u32,
    pub acknowledgment_number: u32,
    pub flags: TcpSegmentFlags,
    pub advertised_window: u16,
    pub capabilities: TcpCapabilities,
}

impl<'a> TcpSegmentView<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, TcpSegmentParseError> {
        if bytes.len() < TCP_HEADER_MIN_LEN {
            return Err(TcpSegmentParseError::ShortHeader);
        }
        let tcp = etherparse::TcpSlice::from_slice(bytes).map_err(tcp_segment_parse_error)?;
        let flags = tcp_segment_flags(&tcp);
        let header_len = tcp.header_len();
        Ok(Self {
            source_port: tcp.source_port(),
            destination_port: tcp.destination_port(),
            sequence_number: tcp.sequence_number(),
            acknowledgment_number: flags
                .contains(TcpSegmentFlags::ACK)
                .then(|| tcp.acknowledgment_number()),
            advertised_window: tcp.window_size(),
            flags,
            header_len,
            options: &bytes[TCP_HEADER_MIN_LEN..header_len],
            payload: &bytes[header_len..],
        })
    }

    #[inline]
    pub const fn source_port(self) -> u16 {
        self.source_port
    }

    #[inline]
    pub const fn destination_port(self) -> u16 {
        self.destination_port
    }

    #[inline]
    pub const fn sequence_number(self) -> u32 {
        self.sequence_number
    }

    #[inline]
    pub const fn acknowledgment_number(self) -> Option<u32> {
        self.acknowledgment_number
    }

    #[inline]
    pub const fn advertised_window(self) -> u16 {
        self.advertised_window
    }

    #[inline]
    pub const fn flags(self) -> TcpSegmentFlags {
        self.flags
    }

    #[inline]
    pub const fn header_len(self) -> usize {
        self.header_len
    }

    #[inline]
    pub const fn options(self) -> &'a [u8] {
        self.options
    }

    #[inline]
    pub const fn payload(self) -> &'a [u8] {
        self.payload
    }
}

pub fn write_tcp_segment_header(output: &mut [u8], header: TcpSegmentHeader) -> CoreResult<usize> {
    let options = if header.flags.contains(TcpSegmentFlags::SYN) {
        super::options::tcp_syn_options_from_capabilities(header.capabilities)
    } else {
        std::vec::Vec::new()
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
    output[13] = header.flags.bits();
    output[14..16].copy_from_slice(&header.advertised_window.to_be_bytes());
    output[TCP_HEADER_MIN_LEN..header_len].copy_from_slice(&options);
    Ok(header_len)
}

fn tcp_segment_parse_error(error: etherparse::err::tcp::HeaderSliceError) -> TcpSegmentParseError {
    match error {
        etherparse::err::tcp::HeaderSliceError::Len(_) => TcpSegmentParseError::ShortHeader,
        etherparse::err::tcp::HeaderSliceError::Content(
            etherparse::err::tcp::HeaderError::DataOffsetTooSmall { .. },
        ) => TcpSegmentParseError::BadDataOffset,
    }
}

#[inline]
fn tcp_segment_flags(tcp: &etherparse::TcpSlice<'_>) -> TcpSegmentFlags {
    let mut flags = TcpSegmentFlags::empty();
    flags.set(TcpSegmentFlags::FIN, tcp.fin());
    flags.set(TcpSegmentFlags::SYN, tcp.syn());
    flags.set(TcpSegmentFlags::RST, tcp.rst());
    flags.set(TcpSegmentFlags::PSH, tcp.psh());
    flags.set(TcpSegmentFlags::ACK, tcp.ack());
    flags.set(TcpSegmentFlags::URG, tcp.urg());
    flags.set(TcpSegmentFlags::ECE, tcp.ece());
    flags.set(TcpSegmentFlags::CWR, tcp.cwr());
    flags
}
