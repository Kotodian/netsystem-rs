#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPacketCursor {
    pub(crate) packet_len: u32,
    pub(crate) network_header_offset: u16,
    pub(crate) network_header_len: u16,
    pub(crate) transport_header_offset: u16,
    pub(crate) transport_header_len: u16,
    pub(crate) transport_payload_offset: u16,
}

impl BufferPacketCursor {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn packet_len(self) -> usize {
        self.packet_len as usize
    }

    #[inline]
    pub fn network_header_offset(self) -> usize {
        self.network_header_offset as usize
    }

    #[inline]
    pub fn network_header_len(self) -> usize {
        usize::from(self.network_header_len)
    }

    #[inline]
    pub fn transport_header_offset(self) -> usize {
        usize::from(self.transport_header_offset)
    }

    #[inline]
    pub fn transport_header_len(self) -> usize {
        usize::from(self.transport_header_len)
    }

    #[inline]
    pub fn transport_payload_offset(self) -> usize {
        usize::from(self.transport_payload_offset)
    }

    #[inline]
    pub fn with_packet_len(mut self, packet_len: usize) -> Self {
        self.packet_len = u32::try_from(packet_len).expect("packet length exceeds u32");
        self
    }

    #[inline]
    pub fn with_network_header(mut self, offset: usize, len: usize) -> Self {
        self.network_header_offset =
            u16::try_from(offset).expect("network header offset exceeds u16");
        self.network_header_len = u16::try_from(len).expect("network header length exceeds u16");
        self
    }

    #[inline]
    pub fn with_transport_header(mut self, offset: usize, len: usize) -> Self {
        self.transport_header_offset =
            u16::try_from(offset).expect("transport header offset exceeds u16");
        self.transport_header_len =
            u16::try_from(len).expect("transport header length exceeds u16");
        self
    }

    #[inline]
    pub fn with_transport_payload_offset(mut self, offset: usize) -> Self {
        self.transport_payload_offset =
            u16::try_from(offset).expect("transport payload offset exceeds u16");
        self
    }
}
