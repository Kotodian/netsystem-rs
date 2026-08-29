use abi_stable::StableAbi;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StableAbi)]
#[repr(transparent)]
pub struct NodeId(u32);

impl NodeId {
    #[inline]
    pub const fn new(slot: u32) -> Self {
        Self(slot)
    }

    #[inline]
    pub fn slot(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeHandle(u32);

impl NodeHandle {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}
