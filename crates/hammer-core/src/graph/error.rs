use std::num::NonZeroU16;

use thiserror::Error;

/// Mirrors VPP's `vlib_error_t`: a global node error index where zero means no error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct NodeErrorIndex(NonZeroU16);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("node error index must be non-zero")]
pub struct NodeErrorIndexError;

impl NodeErrorIndex {
    #[inline]
    pub const fn new(encoded: u16) -> Option<Self> {
        match NonZeroU16::new(encoded) {
            Some(encoded) => Some(Self(encoded)),
            None => None,
        }
    }

    #[inline(always)]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<u16> for NodeErrorIndex {
    type Error = NodeErrorIndexError;

    #[inline]
    fn try_from(encoded: u16) -> Result<Self, Self::Error> {
        Self::new(encoded).ok_or(NodeErrorIndexError)
    }
}

impl From<NonZeroU16> for NodeErrorIndex {
    #[inline(always)]
    fn from(encoded: NonZeroU16) -> Self {
        Self(encoded)
    }
}

impl From<NodeErrorIndex> for NonZeroU16 {
    #[inline(always)]
    fn from(index: NodeErrorIndex) -> Self {
        index.0
    }
}

impl From<NodeErrorIndex> for u16 {
    #[inline(always)]
    fn from(index: NodeErrorIndex) -> Self {
        index.get()
    }
}
