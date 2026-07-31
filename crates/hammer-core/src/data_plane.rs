use abi_stable::StableAbi;

mod buffer;
mod memory;

pub use buffer::{
    BUFFER_CACHE_LINE_SIZE, Buffer, BufferFlags, BufferFrame, BufferNodeError, BufferPacketCursor,
    BufferPoolArena, BufferRef, BufferRefMut, DEFAULT_BUFFER_FRAME_CAPACITY,
    DEFAULT_BUFFER_FRAME_POOL_SIZE, DEFAULT_PACKET_HEADROOM, DataPlaneBuffers, Frame,
    FrameBatchWidth, Index, Next, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, Pending,
    PrimaryOpaque, SecondaryOpaque,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, StableAbi)]
#[repr(transparent)]
pub struct NodeId(u32);

impl NodeId {
    #[inline(always)]
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
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Plain,
    Driver,
    PreInput,
    Internal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NodeState {
    Disabled,
    #[default]
    Polling,
    Interrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistration {
    Plain,
    Next {
        name: &'static str,
        next_count: usize,
    },
    Sibling {
        name: &'static str,
        sibling_of: &'static str,
    },
}

impl NodeRegistration {
    #[inline]
    pub const fn next(name: &'static str, next_count: usize) -> Self {
        Self::Next { name, next_count }
    }

    #[inline]
    pub const fn sibling_of(name: &'static str, sibling_of: &'static str) -> Self {
        Self::Sibling { name, sibling_of }
    }

    #[inline]
    pub fn name(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Next { name, .. } | Self::Sibling { name, .. } => Some(name),
        }
    }
}

pub trait NodeNext: Copy + Eq {
    fn slot(self) -> u16;
}

impl NodeNext for u16 {
    #[inline(always)]
    fn slot(self) -> u16 {
        self
    }
}
