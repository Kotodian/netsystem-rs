use core::mem;
use std::fmt;

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union PrimaryOpaque {
    words64: [u64; 5],
    words32: [u32; 10],
    bytes: [u8; 40],
}

pub const PRIMARY_OPAQUE_BYTES: usize = mem::size_of::<PrimaryOpaque>();
pub const PRIMARY_OPAQUE_ALIGN: usize = mem::align_of::<PrimaryOpaque>();

impl PrimaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        *self = Self { words64: [0; 5] };
    }
}

impl Default for PrimaryOpaque {
    fn default() -> Self {
        Self { words64: [0; 5] }
    }
}

impl fmt::Debug for PrimaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.words64 };
        f.debug_struct("PrimaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union SecondaryOpaque {
    words64: [u64; 7],
    words32: [u32; 14],
    bytes: [u8; 56],
}

impl SecondaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        *self = Self { words64: [0; 7] };
    }
}

impl Default for SecondaryOpaque {
    fn default() -> Self {
        Self { words64: [0; 7] }
    }
}

impl fmt::Debug for SecondaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.words64 };
        f.debug_struct("SecondaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}
