use hammer_infra::pool::Index as PoolIndex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SessionId(PoolIndex);

impl SessionId {
    #[inline(always)]
    pub const fn from_raw(value: u64) -> Self {
        Self(PoolIndex::new(value as u32, (value >> 32) as u32))
    }

    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self::from_raw(value)
    }

    #[inline(always)]
    pub const fn from_pool_index(index: PoolIndex) -> Self {
        Self(index)
    }

    #[inline(always)]
    pub const fn pool_index(self) -> PoolIndex {
        self.0
    }

    #[inline(always)]
    pub const fn get(self) -> u64 {
        ((self.0.generation() as u64) << 32) | self.0.slot() as u64
    }
}
