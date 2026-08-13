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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_id_from_u64() {
        let id = PushId::try_from(2u64).unwrap();
        assert_eq!(VarInt::from(id), VarInt::from_u32(2));
        assert_eq!(
            VarInt::from(PushId::try_from(0u64).unwrap()),
            VarInt::from_u32(0)
        );
    }

    #[test]
    fn push_id_bounds() {
        let max = VarInt::MAX.into_inner();
        assert!(PushId::try_from(max).is_ok());
        assert!(PushId::try_from(max + 1).is_err());
        assert!(PushId::try_from(u64::MAX).is_err());
    }

    #[test]
    fn push_id_from_varint() {
        let id = PushId::from(VarInt::from_u32(3));
        assert_eq!(VarInt::from(id), VarInt::from_u32(3));
    }

    #[test]
    fn push_id_display() {
        assert_eq!(PushId::try_from(2u64).unwrap().to_string(), "push 2");
    }
}
