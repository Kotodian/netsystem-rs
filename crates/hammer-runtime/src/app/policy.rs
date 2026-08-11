#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ApplicationId(u64);

impl ApplicationId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ApplicationListenerId(u64);

impl ApplicationListenerId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub struct ApplicationConnectionId(u64);

impl ApplicationConnectionId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}
