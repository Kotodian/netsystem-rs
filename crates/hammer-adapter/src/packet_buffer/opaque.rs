use core::fmt;
use core::mem::{align_of, size_of};

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union PrimaryOpaque {
    words64: [u64; 5],
    words32: [u32; 10],
    bytes: [u8; 40],
}

pub const PRIMARY_OPAQUE_BYTES: usize = core::mem::size_of::<PrimaryOpaque>();
pub const PRIMARY_OPAQUE_ALIGN: usize = core::mem::align_of::<PrimaryOpaque>();

impl PrimaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        *self = Self { words64: [0; 5] };
    }

    #[inline]
    pub fn write<P: Copy>(&mut self, payload: P) {
        const {
            assert!(size_of::<P>() == size_of::<PrimaryOpaque>());
            assert!(align_of::<P>() <= align_of::<PrimaryOpaque>());
        }
        unsafe { (self as *mut Self).cast::<P>().write(payload) };
    }

    #[inline]
    pub fn read<P: Copy>(&self) -> P {
        const {
            assert!(size_of::<P>() == size_of::<PrimaryOpaque>());
            assert!(align_of::<P>() <= align_of::<PrimaryOpaque>());
        }
        unsafe { (self as *const Self).cast::<P>().read() }
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

    #[inline]
    pub fn write<P: Copy>(&mut self, payload: P) {
        const {
            assert!(size_of::<P>() == size_of::<SecondaryOpaque>());
            assert!(align_of::<P>() <= align_of::<SecondaryOpaque>());
        }
        unsafe { (self as *mut Self).cast::<P>().write(payload) };
    }

    #[inline]
    pub fn read<P: Copy>(&self) -> P {
        const {
            assert!(size_of::<P>() == size_of::<SecondaryOpaque>());
            assert!(align_of::<P>() <= align_of::<SecondaryOpaque>());
        }
        unsafe { (self as *const Self).cast::<P>().read() }
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
