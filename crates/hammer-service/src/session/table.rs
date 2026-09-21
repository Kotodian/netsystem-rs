pub struct SessionTable<S, H> {
    sessions: S,
    half_open: H,
}

impl<S, H> SessionTable<S, H> {
    #[inline(always)]
    pub const fn new(sessions: S, half_open: H) -> Self {
        Self {
            sessions,
            half_open,
        }
    }

    #[inline(always)]
    pub const fn sessions(&self) -> &S {
        &self.sessions
    }

    #[inline(always)]
    pub const fn half_open(&self) -> &H {
        &self.half_open
    }
}
