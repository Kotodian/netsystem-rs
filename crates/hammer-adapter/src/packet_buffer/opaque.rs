use core::fmt;

pub trait PrimaryOpaquePayload: Sized {
    fn encode_primary(&self) -> [u64; 5];
    fn decode_primary(words: [u64; 5]) -> Self;
}

pub trait SecondaryOpaquePayload: Sized {
    fn encode_secondary(&self) -> [u64; 7];
    fn decode_secondary(words: [u64; 7]) -> Self;
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union PrimaryOpaqueStorage {
    words64: [u64; 5],
    words32: [u32; 10],
    bytes: [u8; 40],
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct PrimaryOpaque {
    storage: PrimaryOpaqueStorage,
}

pub const PRIMARY_OPAQUE_BYTES: usize = core::mem::size_of::<PrimaryOpaque>();
pub const PRIMARY_OPAQUE_ALIGN: usize = core::mem::align_of::<PrimaryOpaque>();

impl PrimaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        self.storage = PrimaryOpaqueStorage { words64: [0; 5] };
    }

    #[inline]
    pub fn write<P: PrimaryOpaquePayload>(&mut self, payload: &P) {
        self.storage = PrimaryOpaqueStorage {
            words64: payload.encode_primary(),
        };
    }

    #[inline]
    pub fn read<P: PrimaryOpaquePayload>(&self) -> P {
        P::decode_primary(unsafe { self.storage.words64 })
    }
}

impl Default for PrimaryOpaque {
    fn default() -> Self {
        Self {
            storage: PrimaryOpaqueStorage { words64: [0; 5] },
        }
    }
}

impl fmt::Debug for PrimaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.storage.words64 };
        f.debug_struct("PrimaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union SecondaryOpaqueStorage {
    words64: [u64; 7],
    words32: [u32; 14],
    bytes: [u8; 56],
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct SecondaryOpaque {
    storage: SecondaryOpaqueStorage,
}

impl SecondaryOpaque {
    #[inline]
    pub fn clear(&mut self) {
        self.storage = SecondaryOpaqueStorage { words64: [0; 7] };
    }

    #[inline]
    pub fn write<P: SecondaryOpaquePayload>(&mut self, payload: &P) {
        self.storage = SecondaryOpaqueStorage {
            words64: payload.encode_secondary(),
        };
    }

    #[inline]
    pub fn read<P: SecondaryOpaquePayload>(&self) -> P {
        P::decode_secondary(unsafe { self.storage.words64 })
    }
}

impl Default for SecondaryOpaque {
    fn default() -> Self {
        Self {
            storage: SecondaryOpaqueStorage { words64: [0; 7] },
        }
    }
}

impl fmt::Debug for SecondaryOpaque {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words64 = unsafe { self.storage.words64 };
        f.debug_struct("SecondaryOpaque")
            .field("words64", &words64)
            .finish()
    }
}
