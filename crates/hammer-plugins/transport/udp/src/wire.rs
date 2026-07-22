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

#[cfg(test)]
mod tests {
    use super::UdpHeader;

    #[test]
    fn reads_network_order_fields() {
        let bytes: [u8; 8] = [0x30, 0x39, 0x00, 0x35, 0x00, 0x10, 0xab, 0xcd];
        // SAFETY: the byte array contains a complete `UdpHeader`; the packed
        // wire layout permits an unaligned read.
        let header = unsafe { bytes.as_ptr().cast::<UdpHeader>().read_unaligned() };

        assert_eq!(header.source_port(), 12_345);
        assert_eq!(header.destination_port(), 53);
        assert_eq!(header.length(), 16);
        assert_eq!(header.checksum(), 0xabcd);
    }
}
