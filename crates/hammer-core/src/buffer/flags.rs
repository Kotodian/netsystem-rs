#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct BufferFlags(u32);

impl BufferFlags {
    const PUBLIC_MASK: u32 = (1 << 4) - 1;
    const PRIVATE_CAPACITY_SHIFT: u32 = 4;
    const PRIVATE_CAPACITY_MASK: u32 = !Self::PUBLIC_MASK;

    pub const NEXT_PRESENT: Self = Self(1 << 0);
    pub const TOTAL_LENGTH_VALID: Self = Self(1 << 1);
    pub const TRACED: Self = Self(1 << 2);
    /// Cacheline1 (trace_handle / total_length_not_including_first / opaque2)
    /// is known to be zeroed. Set by the free fast path and by the full reset
    /// routines; cleared by any mutator that dirties cacheline1. Lets the
    /// alloc fast path skip the second cacheline write when the slot was
    /// cleanly freed.
    pub const SLOT_CLEAN: Self = Self(1 << 3);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0 & Self::PUBLIC_MASK
    }

    #[inline]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits & Self::PUBLIC_MASK)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.bits() & other.bits() == other.bits()
    }

    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 = (self.0 & Self::PRIVATE_CAPACITY_MASK) | (self.bits() | other.bits());
    }

    #[inline]
    pub fn remove(&mut self, other: Self) {
        self.0 = (self.0 & Self::PRIVATE_CAPACITY_MASK) | (self.bits() & !other.bits());
    }

    #[inline]
    pub(super) const fn with_private_data_capacity(self, data_capacity: usize) -> Self {
        let max_capacity = Self::max_private_data_capacity();
        let capped = if data_capacity > max_capacity {
            max_capacity
        } else {
            data_capacity
        };
        Self(self.bits() | ((capped as u32) << Self::PRIVATE_CAPACITY_SHIFT))
    }

    #[inline]
    pub(super) const fn private_data_capacity(self) -> usize {
        ((self.0 & Self::PRIVATE_CAPACITY_MASK) >> Self::PRIVATE_CAPACITY_SHIFT) as usize
    }

    #[inline]
    const fn max_private_data_capacity() -> usize {
        (u32::MAX >> Self::PRIVATE_CAPACITY_SHIFT) as usize
    }
}
