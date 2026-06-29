use thiserror::Error;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SessionQueueError {
    #[error("dispatch failed")]
    DispatchFailed,
}

impl SessionQueueError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}
