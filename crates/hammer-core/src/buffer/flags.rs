#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct BufferFlags(u32);

impl BufferFlags {
    // third_party/vpp/src/vlib/buffer.h: foreach_vlib_buffer_flag.
    pub const TRACED: Self = Self(1 << 0);
    pub const NEXT_PRESENT: Self = Self(1 << 1);
    pub const TOTAL_LENGTH_VALID: Self = Self(1 << 2);
    pub const EXT_HDR_VALID: Self = Self(1 << 3);
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
        self.bits() & other.bits() == other.bits()
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
