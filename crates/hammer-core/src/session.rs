//! Cross-crate Session ABI values.

/// Identity of one Session in its owning worker pool.
///
/// VPP's `session_handle_t` carries these two `u32` facts. Keeping them as
/// fields makes the owner and Pool index explicit. The C representation is
/// not part of this Rust domain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionHandle {
    pub session_index: u32,
    pub thread_index: u32,
}

impl SessionHandle {
    #[inline(always)]
    pub const fn new(session_index: u32, thread_index: u32) -> Self {
        Self {
            session_index,
            thread_index,
        }
    }
}

impl From<SessionHandle> for u64 {
    #[inline]
    fn from(handle: SessionHandle) -> Self {
        let session_index: u64 = handle.session_index.into();
        let thread_index: u64 = handle.thread_index.into();
        session_index | (thread_index << 32)
    }
}

impl From<u64> for SessionHandle {
    #[inline]
    fn from(value: u64) -> Self {
        Self::new(value as u32, (value >> 32) as u32)
    }
}
