//! Push ID (RFC 9114 Section 8.4, used by CANCEL_PUSH, MAX_PUSH_ID, and
//! PUSH_PROMISE frames).
//!
//! Adapted from `third_party/h3/h3/src/proto/push.rs`.

use std::fmt;

use super::varint::VarInt;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PushId(VarInt);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct InvalidPushId(u64);

impl TryFrom<u64> for PushId {
    type Error = InvalidPushId;

    fn try_from(v: u64) -> Result<Self, Self::Error> {
        match VarInt::try_from(v) {
            Ok(id) => Ok(PushId(id)),
            Err(_) => Err(InvalidPushId(v)),
        }
    }
}

impl From<VarInt> for PushId {
    fn from(v: VarInt) -> Self {
        PushId(v)
    }
}

impl From<PushId> for VarInt {
    fn from(v: PushId) -> Self {
        v.0
    }
}

impl fmt::Display for PushId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "push {}", self.0.into_inner())
    }
}

impl fmt::Display for InvalidPushId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid push id: {:x}", self.0)
    }
}
