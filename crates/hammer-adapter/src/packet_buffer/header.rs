use super::opaque::{PrimaryOpaque, SecondaryOpaque};

pub const PACKET_BUFFER_INVALID_INDEX: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct PacketBufferFlags(u32);

impl PacketBufferFlags {
    pub const NEXT_PRESENT: Self = Self(1 << 0);
    pub const TOTAL_LENGTH_VALID: Self = Self(1 << 1);
    pub const TRACED: Self = Self(1 << 2);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }

    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 &= !other.0;
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct PacketBufferHeader {
    pub current_data: i16,
    pub current_length: u16,
    pub flags: PacketBufferFlags,
    pub flow_id: u32,
    pub ref_count: u8,
    pub buffer_pool_index: u8,
    pub error: u16,
    pub next_buffer: u32,
    pub current_config_or_punt: u32,
    pub opaque: PrimaryOpaque,
}

impl Default for PacketBufferHeader {
    fn default() -> Self {
        Self {
            current_data: 0,
            current_length: 0,
            flags: PacketBufferFlags::empty(),
            flow_id: 0,
            ref_count: 0,
            buffer_pool_index: 0,
            error: 0,
            next_buffer: PACKET_BUFFER_INVALID_INDEX,
            current_config_or_punt: 0,
            opaque: PrimaryOpaque::default(),
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct PacketBufferHeaderExt {
    pub trace_handle: u32,
    pub total_length_not_including_first: u32,
    pub opaque2: SecondaryOpaque,
}

impl Default for PacketBufferHeaderExt {
    fn default() -> Self {
        Self {
            trace_handle: 0,
            total_length_not_including_first: 0,
            opaque2: SecondaryOpaque::default(),
        }
    }
}
