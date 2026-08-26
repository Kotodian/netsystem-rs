#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SessionId(u64);

impl SessionId {
    #[inline(always)]
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn new(value: u64) -> Self {
        Self::from_raw(value)
    }

    #[inline(always)]
    pub const fn pool_index(self) -> u32 {
        self.0 as u32
    }

    #[inline(always)]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u32> for SessionId {
    #[inline(always)]
    fn from(index: u32) -> Self {
        Self(u64::from(index))
    }
}
