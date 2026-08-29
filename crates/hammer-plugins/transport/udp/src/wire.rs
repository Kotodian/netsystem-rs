use std::mem::size_of;

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub(crate) struct UdpHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    length: [u8; 2],
    checksum: [u8; 2],
}

impl UdpHeader {
    #[inline(always)]
    pub(crate) fn new(source_port: u16, destination_port: u16, length: u16, checksum: u16) -> Self {
        Self {
            source_port: source_port.to_be_bytes(),
            destination_port: destination_port.to_be_bytes(),
            length: length.to_be_bytes(),
            checksum: checksum.to_be_bytes(),
        }
    }

    #[inline(always)]
    pub(crate) fn source_port(self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    pub(crate) fn destination_port(self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    pub(crate) fn length(self) -> usize {
        usize::from(u16::from_be_bytes(self.length))
    }

    #[inline(always)]
    pub(crate) fn checksum(self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }
}

pub(crate) fn write_udp_header(
    output: &mut [u8],
    source_port: u16,
    destination_port: u16,
    payload_len: usize,
) -> Option<()> {
    let length = u16::try_from(payload_len.checked_add(UDP_HEADER_LEN)?).ok()?;
    let bytes = output.get_mut(..size_of::<UdpHeader>())?;
    let header = UdpHeader::new(source_port, destination_port, length, 0);
    // SAFETY: `bytes` has exactly the packed header size; unaligned writes are
    // valid because network headers may start at arbitrary buffer offsets.
    unsafe {
        bytes
            .as_mut_ptr()
            .cast::<UdpHeader>()
            .write_unaligned(header);
    }
    Some(())
}

const UDP_HEADER_LEN: usize = 8;
