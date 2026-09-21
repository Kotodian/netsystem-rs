#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTableIndex(u32);

impl SessionTableIndex {
    pub const INVALID: Self = Self(u32::MAX);

    #[inline]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != u32::MAX
    }
}

pub struct SessionTable<S, H> {
    sessions: S,
    half_open: H,
}

impl<S, H> SessionTable<S, H> {
    #[inline]
    pub const fn new(sessions: S, half_open: H) -> Self {
        Self {
            sessions,
            half_open,
        }
    }

    #[inline]
    pub const fn sessions(&self) -> &S {
        &self.sessions
    }

    #[inline]
    pub const fn half_open(&self) -> &H {
        &self.half_open
    }
}
