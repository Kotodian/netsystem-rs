use core::mem::{align_of, size_of, transmute};

use crate::BufferPacketCursor;
use hammer_core::forwarding::DpoType;

use super::opaque::{PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IpEcnCodepoint {
    NotEct = 0,
    Ect1 = 1,
    Ect0 = 2,
    Ce = 3,
}

impl From<IpEcnCodepoint> for u8 {
    #[inline]
    fn from(value: IpEcnCodepoint) -> Self {
        value as u8
    }
}

impl From<u8> for IpEcnCodepoint {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Ect1,
            2 => Self::Ect0,
            3 => Self::Ce,
            _ => Self::NotEct,
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardingMetadata {
    pub fib_index: u32,
    pub route_dpo_type: DpoType,
    pub route_dpo_index: u32,
    pub load_balance_index: u32,
    pub bucket_index: u16,
    pub dpo_type: DpoType,
    pub dpo_index: u32,
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union NetworkPayloadStorage {
    words64: [u64; 3],
    words32: [u32; 6],
    bytes: [u8; 24],
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct NetworkPayloadOpaque {
    storage: NetworkPayloadStorage,
}

impl NetworkPayloadOpaque {
    #[inline]
    pub fn clear(&mut self) {
        self.storage = NetworkPayloadStorage { words64: [0; 3] };
    }

    #[inline]
    pub fn write<P: Copy>(&mut self, payload: P) {
        const {
            assert!(size_of::<P>() == size_of::<NetworkPayloadOpaque>());
            assert!(align_of::<P>() <= align_of::<NetworkPayloadOpaque>());
        }
        unsafe { (self as *mut Self).cast::<P>().write(payload) };
    }

    #[inline]
    pub fn read<P: Copy>(&self) -> P {
        const {
            assert!(size_of::<P>() == size_of::<NetworkPayloadOpaque>());
            assert!(align_of::<P>() <= align_of::<NetworkPayloadOpaque>());
        }
        unsafe { (self as *const Self).cast::<P>().read() }
    }
}

impl Default for NetworkPayloadOpaque {
    fn default() -> Self {
        Self {
            storage: NetworkPayloadStorage { words64: [0; 3] },
        }
    }
}

#[derive(Clone, Copy, Default)]
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
    reserved: [u8; 10],
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
        if self.ip_ecn_valid == 0 {
            None
        } else {
            Some(self.ip_ecn)
        }
    }

    #[inline]
    pub fn set_ip_ecn(&mut self, ecn: Option<u8>) {
        match ecn {
            Some(value) => {
                self.ip_ecn = value;
                self.ip_ecn_valid = 1;
            }
            None => {
                self.ip_ecn = 0;
                self.ip_ecn_valid = 0;
            }
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct NetworkReassemblyOpaque {
    next_index: u32,
    error_next_index: u32,
    owner_thread_index: u16,
    save_rewrite_length: u8,
    reserved: [u8; 13],
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

#[derive(Clone, Copy)]
#[repr(C)]
pub union NetworkOpaqueOverlay {
    pub payload: NetworkPayloadOpaque,
    pub ip: NetworkIpOpaque,
    pub reass: NetworkReassemblyOpaque,
}

impl NetworkOpaqueOverlay {
    #[inline]
    pub fn payload(&self) -> &NetworkPayloadOpaque {
        unsafe { transmute::<&NetworkOpaqueOverlay, &NetworkPayloadOpaque>(self) }
    }

    #[inline]
    pub fn payload_mut(&mut self) -> &mut NetworkPayloadOpaque {
        unsafe { transmute::<&mut NetworkOpaqueOverlay, &mut NetworkPayloadOpaque>(self) }
    }

    #[inline]
    pub fn ip(&self) -> &NetworkIpOpaque {
        unsafe { transmute::<&NetworkOpaqueOverlay, &NetworkIpOpaque>(self) }
    }

    #[inline]
    pub fn ip_mut(&mut self) -> &mut NetworkIpOpaque {
        unsafe { transmute::<&mut NetworkOpaqueOverlay, &mut NetworkIpOpaque>(self) }
    }

    #[inline]
    pub fn reass(&self) -> &NetworkReassemblyOpaque {
        unsafe { transmute::<&NetworkOpaqueOverlay, &NetworkReassemblyOpaque>(self) }
    }

    #[inline]
    pub fn reass_mut(&mut self) -> &mut NetworkReassemblyOpaque {
        unsafe { transmute::<&mut NetworkOpaqueOverlay, &mut NetworkReassemblyOpaque>(self) }
    }
}

impl Default for NetworkOpaqueOverlay {
    fn default() -> Self {
        Self {
            payload: NetworkPayloadOpaque::default(),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct NetworkOpaque {
    pub sw_if_index: [u32; 2],
    pub l2_hdr_offset: i16,
    pub l3_hdr_offset: i16,
    pub l4_hdr_offset: i16,
    pub feature_arc_index: u8,
    pub oflags: u8,
    overlay: NetworkOpaqueOverlay,
}

const _: () = assert!(size_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_BYTES);
const _: () = assert!(align_of::<NetworkOpaque>() <= PRIMARY_OPAQUE_ALIGN);

impl Default for NetworkOpaque {
    fn default() -> Self {
        Self {
            sw_if_index: [0; 2],
            l2_hdr_offset: 0,
            l3_hdr_offset: 0,
            l4_hdr_offset: 0,
            feature_arc_index: 0,
            oflags: 0,
            overlay: NetworkOpaqueOverlay::default(),
        }
    }
}

impl NetworkOpaque {
    #[inline]
    pub fn payload(&self) -> &NetworkPayloadOpaque {
        self.overlay.payload()
    }

    #[inline]
    pub fn payload_mut(&mut self) -> &mut NetworkPayloadOpaque {
        self.overlay.payload_mut()
    }

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
                self.l4_hdr_offset.max(0) as usize,
                usize::from(ip.transport_header_len()),
            )
            .with_transport_payload_offset(usize::from(ip.transport_payload_offset()))
    }

    #[inline]
    pub fn set_packet_cursor(&mut self, cursor: BufferPacketCursor) {
        self.l3_hdr_offset = cursor.network_header_offset as i16;
        self.l4_hdr_offset = cursor.transport_header_offset as i16;

        let ip = self.ip_mut();
        ip.set_packet_len(cursor.packet_len);
        ip.set_network_header_len(cursor.network_header_len);
        ip.set_transport_header_len(cursor.transport_header_len);
        ip.set_transport_payload_offset(cursor.transport_payload_offset);
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
