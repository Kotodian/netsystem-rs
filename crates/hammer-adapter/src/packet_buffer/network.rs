use core::mem::{align_of, size_of};

use super::header::PacketBufferHeader;
use super::opaque::{PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES};

pub trait NetworkOpaquePayload: Sized {
    fn encode_network(&self) -> [u64; 3];
    fn decode_network(words: [u64; 3]) -> Self;
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
    pub fn write<P: NetworkOpaquePayload>(&mut self, payload: &P) {
        self.storage = NetworkPayloadStorage {
            words64: payload.encode_network(),
        };
    }

    #[inline]
    pub fn read<P: NetworkOpaquePayload>(&self) -> P {
        P::decode_network(unsafe { self.storage.words64 })
    }
}

impl Default for NetworkPayloadOpaque {
    fn default() -> Self {
        Self {
            storage: NetworkPayloadStorage { words64: [0; 3] },
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
    pub payload: NetworkPayloadOpaque,
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
            payload: NetworkPayloadOpaque::default(),
        }
    }
}

impl PacketBufferHeader {
    #[inline]
    pub fn network(&self) -> &NetworkOpaque {
        unsafe { &*((&self.opaque as *const _) as *const NetworkOpaque) }
    }

    #[inline]
    pub fn network_mut(&mut self) -> &mut NetworkOpaque {
        unsafe { &mut *((&mut self.opaque as *mut _) as *mut NetworkOpaque) }
    }
}
