use core::mem::{align_of, size_of};

use hammer_core::data_plane::{BufferPacketCursor, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES};

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::Immutable,
)]
#[repr(transparent)]
pub struct NetworkFlags(u8);

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::Immutable,
)]
#[repr(transparent)]
pub struct NetworkOffloadFlags(u8);

bitflags::bitflags! {
    impl NetworkFlags: u8 {
        const LOCALLY_ORIGINATED = 1 << 0;
        const L4_CHECKSUM_COMPUTED = 1 << 1;
        const L4_CHECKSUM_CORRECT = 1 << 2;
    }
    impl NetworkOffloadFlags: u8 {
        const TCP_CHECKSUM = 1 << 0;
        const UDP_CHECKSUM = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TapEthernetMetadata {
    pub destination: [u8; 6],
    pub source: [u8; 6],
    pub ethertype: u16,
    pub header_present: bool,
}

impl TapEthernetMetadata {
    #[inline]
    pub const fn new(destination: [u8; 6], source: [u8; 6], ethertype: u16) -> Self {
        Self {
            destination,
            source,
            ethertype,
            header_present: false,
        }
    }

    #[inline]
    pub fn header(self) -> [u8; 14] {
        let mut header = [0u8; 14];
        header[..6].copy_from_slice(&self.destination);
        header[6..12].copy_from_slice(&self.source);
        header[12..14].copy_from_slice(&self.ethertype.to_be_bytes());
        header
    }
}

#[derive(Clone, Copy, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::Immutable)]
#[repr(C)]
pub struct NetworkIpOpaque {
    packet_len: u32,
    network_header_len: u16,
    transport_header_len: u16,
    transport_payload_offset: u16,
    ip_version: u8,
    ip_protocol: u8,
    ip_ecn: u8,
    ip_ecn_valid: u8,
    padding: [u8; 2],
    fib_index: u32,
    pub rx_sw_if_index: u32,
    reserved: [u8; 4],
}

impl Default for NetworkIpOpaque {
    fn default() -> Self {
        let mut opaque = Self {
            packet_len: 0,
            network_header_len: 0,
            transport_header_len: 0,
            transport_payload_offset: 0,
            ip_version: 0,
            ip_protocol: 0,
            ip_ecn: 0,
            ip_ecn_valid: 0,
            padding: [0; 2],
            fib_index: u32::MAX,
            rx_sw_if_index: u32::MAX,
            reserved: [0; 4],
        };
        opaque.set_fib_index_override(None);
        opaque
    }
}

impl NetworkIpOpaque {
    #[inline]
    pub fn packet_len(&self) -> u32 {
        self.packet_len
    }

    #[inline]
    pub fn set_packet_len(&mut self, len: u32) {
        self.packet_len = len;
    }

    #[inline]
    pub fn network_header_len(&self) -> u16 {
        self.network_header_len
    }

    #[inline]
    pub fn set_network_header_len(&mut self, len: u16) {
        self.network_header_len = len;
    }

    #[inline]
    pub fn transport_header_len(&self) -> u16 {
        self.transport_header_len
    }

    #[inline]
    pub fn set_transport_header_len(&mut self, len: u16) {
        self.transport_header_len = len;
    }

    #[inline]
    pub fn transport_payload_offset(&self) -> u16 {
        self.transport_payload_offset
    }

    #[inline]
    pub fn set_transport_payload_offset(&mut self, offset: u16) {
        self.transport_payload_offset = offset;
    }

    #[inline]
    pub fn ip_version(&self) -> Option<u8> {
        (self.ip_version != 0).then_some(self.ip_version)
    }

    #[inline]
    pub fn set_ip_version(&mut self, version: Option<u8>) {
        self.ip_version = version.unwrap_or(0);
    }

    #[inline]
    pub fn ip_protocol(&self) -> Option<u8> {
        (self.ip_protocol != 0).then_some(self.ip_protocol)
    }

    #[inline]
    pub fn set_ip_protocol(&mut self, protocol: Option<u8>) {
        self.ip_protocol = protocol.unwrap_or(0);
    }

    #[inline]
    pub fn ip_ecn(&self) -> Option<u8> {
        (self.ip_ecn_valid != 0).then_some(self.ip_ecn)
    }

    #[inline]
    pub fn set_ip_ecn(&mut self, ecn: Option<u8>) {
        if let Some(value) = ecn {
            self.ip_ecn = value;
            self.ip_ecn_valid = 1;
        } else {
            self.ip_ecn = 0;
            self.ip_ecn_valid = 0;
        }
    }

    #[inline]
    pub fn fib_index(&self) -> Option<u32> {
        (self.fib_index != u32::MAX).then_some(self.fib_index)
    }

    #[inline]
    pub fn set_fib_index(&mut self, index: Option<u32>) {
        self.fib_index = index.unwrap_or(u32::MAX);
    }

    #[inline]
    pub fn fib_index_override(&self) -> Option<u32> {
        let index = u32::from_le_bytes([
            self.reserved[0],
            self.reserved[1],
            self.reserved[2],
            self.reserved[3],
        ]);
        (index != u32::MAX).then_some(index)
    }

    #[inline]
    pub fn set_fib_index_override(&mut self, index: Option<u32>) {
        self.reserved
            .copy_from_slice(&index.unwrap_or(u32::MAX).to_le_bytes());
    }
}

#[derive(Clone, Copy, Default, zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::Immutable)]
#[repr(C)]
pub struct NetworkReassemblyOpaque {
    next_index: u32,
    error_next_index: u32,
    owner_thread_index: u16,
    save_rewrite_length: u8,
    reserved: [u8; 17],
}

impl NetworkReassemblyOpaque {
    #[inline]
    pub fn handoff_source_worker(&self) -> Option<u16> {
        if self.owner_thread_index == 0 {
            None
        } else {
            Some(self.owner_thread_index - 1)
        }
    }

    #[inline]
    pub fn set_handoff_source_worker(&mut self, worker: Option<u16>) {
        self.owner_thread_index = worker.map_or(0, |value| value.saturating_add(1));
    }
}

#[hammer_component_macros::buffer_opaque(primary)]
#[derive(Clone, Copy)]
#[repr(C)]
pub union NetworkOpaqueOverlay {
    ip: NetworkIpOpaque,
    reass: NetworkReassemblyOpaque,
}

impl Default for NetworkOpaqueOverlay {
    fn default() -> Self {
        Self {
            ip: NetworkIpOpaque::default(),
        }
    }
}

#[hammer_component_macros::buffer_opaque(primary)]
#[derive(Clone, Copy)]
#[repr(C)]
pub struct NetworkOpaque {
    pub sw_if_index: [u32; 2],
    pub l3_hdr_offset: i16,
    pub oflags: NetworkOffloadFlags,
    pub flags: NetworkFlags,
    overlay: NetworkOpaqueOverlay,
}

const _: () = assert!(size_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_BYTES);
const _: () = assert!(align_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_ALIGN);

impl Default for NetworkOpaque {
    fn default() -> Self {
        Self {
            sw_if_index: [u32::MAX; 2],
            l3_hdr_offset: 0,
            oflags: NetworkOffloadFlags::empty(),
            flags: NetworkFlags::empty(),
            overlay: NetworkOpaqueOverlay::default(),
        }
    }
}

impl NetworkOpaque {
    #[inline]
    pub fn ip(&self) -> &NetworkIpOpaque {
        self.overlay.ip()
    }

    #[inline]
    pub fn ip_mut(&mut self) -> &mut NetworkIpOpaque {
        self.overlay.ip_mut()
    }

    #[inline]
    pub fn reassembly(&self) -> &NetworkReassemblyOpaque {
        self.overlay.reass()
    }

    #[inline]
    pub fn reassembly_mut(&mut self) -> &mut NetworkReassemblyOpaque {
        self.overlay.reass_mut()
    }

    #[inline]
    pub fn packet_cursor(&self) -> BufferPacketCursor {
        let ip = self.ip();
        BufferPacketCursor::new()
            .with_packet_len(ip.packet_len() as usize)
            .with_network_header(
                self.l3_hdr_offset.max(0) as usize,
                usize::from(ip.network_header_len()),
            )
            .with_transport_header(
                usize::from(ip.transport_payload_offset())
                    .saturating_sub(usize::from(ip.transport_header_len())),
                usize::from(ip.transport_header_len()),
            )
            .with_transport_payload_offset(usize::from(ip.transport_payload_offset()))
    }

    #[inline]
    pub fn set_packet_cursor(&mut self, cursor: BufferPacketCursor) {
        self.l3_hdr_offset = i16::try_from(cursor.network_header_offset())
            .expect("network header offset exceeds i16");
        let ip = self.ip_mut();
        ip.set_packet_len(u32::try_from(cursor.packet_len()).expect("packet length exceeds u32"));
        ip.set_network_header_len(
            u16::try_from(cursor.network_header_len()).expect("network header length exceeds u16"),
        );
        ip.set_transport_header_len(
            u16::try_from(cursor.transport_header_len())
                .expect("transport header length exceeds u16"),
        );
        ip.set_transport_payload_offset(
            u16::try_from(cursor.transport_payload_offset())
                .expect("transport payload offset exceeds u16"),
        );
    }

    #[inline]
    pub fn handoff_source_worker(&self) -> Option<u16> {
        self.reassembly().handoff_source_worker()
    }

    #[inline]
    pub fn set_handoff_source_worker(&mut self, worker: Option<u16>) {
        self.reassembly_mut().set_handoff_source_worker(worker);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // vnet/buffer.h: graph paths select members of vnet_buffer_opaque_t's
    // union while interface indices outside the union remain shared facts.
    #[test]
    fn network_paths_borrow_one_union_and_preserve_interface_indices() {
        let mut network = NetworkOpaque::default();
        network.sw_if_index = [17, 23];
        let ip_address = std::ptr::from_ref(network.ip()).addr();
        assert_eq!(ip_address, std::ptr::from_ref(network.reassembly()).addr());
        network.ip_mut().set_packet_len(1500);
        assert_eq!(network.ip().packet_len(), 1500);
        network.reassembly_mut().set_handoff_source_worker(Some(3));
        assert_eq!(network.reassembly().handoff_source_worker(), Some(3));
        assert_eq!(network.sw_if_index, [17, 23]);
    }
}
