use super::TcpSegmentFlags;

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
