#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct SessionHandle(u64);

impl SessionHandle {
    #[inline(always)]
    pub const fn new(session_index: u32, worker_index: u32) -> Self {
        Self((session_index as u64) | ((worker_index as u64) << 32))
    }

    #[inline(always)]
    pub const fn raw(self) -> u64 {
        self.0
    }

    #[inline(always)]
    pub const fn session_index(self) -> u32 {
        self.0 as u32
    }

    #[inline(always)]
    pub const fn worker_index(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

impl From<SessionHandle> for u64 {
    #[inline(always)]
    fn from(handle: SessionHandle) -> Self {
        handle.raw()
    }
}

impl From<u64> for SessionHandle {
    #[inline(always)]
    fn from(value: u64) -> Self {
        Self(value)
    }
}
