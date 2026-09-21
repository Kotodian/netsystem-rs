use zerocopy::FromBytes;

#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::KnownLayout, zerocopy::Immutable)]
#[repr(C)]
pub(crate) struct UdpHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    length: [u8; 2],
    checksum: [u8; 2],
}

impl UdpHeader {
    #[inline(always)]
    pub(crate) fn source_port(&self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    pub(crate) fn destination_port(&self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    pub(crate) fn length(&self) -> usize {
        usize::from(u16::from_be_bytes(self.length))
    }

    #[inline(always)]
    pub(crate) fn checksum(&self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }

    #[inline(always)]
    fn set_source_port(&mut self, source_port: u16) {
        self.source_port = source_port.to_be_bytes();
    }

    #[inline(always)]
    fn set_destination_port(&mut self, destination_port: u16) {
        self.destination_port = destination_port.to_be_bytes();
    }

    #[inline(always)]
    fn set_length(&mut self, length: u16) {
        self.length = length.to_be_bytes();
    }

    #[inline(always)]
    pub(crate) fn set_checksum(&mut self, checksum: u16) {
        self.checksum = checksum.to_be_bytes();
    }
}

pub(crate) fn write_udp_header(
    output: &mut [u8],
    source_port: u16,
    destination_port: u16,
    payload_len: usize,
) -> Option<()> {
    let length = u16::try_from(payload_len.checked_add(UDP_HEADER_LEN)?).ok()?;
    let (header, _) = UdpHeader::mut_from_prefix(output).ok()?;
    header.set_source_port(source_port);
    header.set_destination_port(destination_port);
    header.set_length(length);
    header.set_checksum(0);
    Some(())
}

const UDP_HEADER_LEN: usize = 8;
