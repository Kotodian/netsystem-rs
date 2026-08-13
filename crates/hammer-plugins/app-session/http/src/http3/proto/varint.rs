//! QUIC variable-length integer (RFC 9000 Section 16), the base encoding for
//! HTTP/3 frame headers, stream types, settings, and QPACK prefixes.
//!
//! Adapted from `third_party/h3/h3/src/proto/varint.rs` with checked
//! construction (no `unsafe`, no panicking `write_var`).

use bytes::{Buf, BufMut};
use std::fmt;

/// An integer less than 2^62, suitable for QUIC variable-length encoding.
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(u64);

impl VarInt {
    /// The largest representable value.
    pub const MAX: VarInt = VarInt((1 << 62) - 1);
    /// The largest encoded value length in bytes.
    pub const MAX_SIZE: usize = 8;

    /// Construct a `VarInt` from a `u32` (always in range).
    pub const fn from_u32(x: u32) -> Self {
        VarInt(x as u64)
    }

    /// Succeeds iff `x` < 2^62.
    pub const fn from_u64(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        if x < (1 << 62) {
            Ok(VarInt(x))
        } else {
            Err(VarIntBoundsExceeded(x))
        }
    }

    /// Extract the integer value.
    pub const fn into_inner(self) -> u64 {
        self.0
    }

    /// Number of bytes needed to encode this value.
    pub fn size(self) -> usize {
        let x = self.0;
        if x < (1 << 6) {
            1
        } else if x < (1 << 14) {
            2
        } else if x < (1 << 30) {
            4
        } else {
            8
        }
    }

    /// Length of an encoded value given its first byte.
    pub fn encoded_size(first: u8) -> usize {
        1usize << (first >> 6)
    }

    /// Decode from the head of `r`, consuming the bytes read.
    ///
    /// Returns `UnexpectedEnd(n)` with `n` the number of missing bytes when the
    /// input ends before the varint is complete.
    pub fn decode<B: Buf>(r: &mut B) -> Result<Self, UnexpectedEnd> {
        if !r.has_remaining() {
            return Err(UnexpectedEnd(0));
        }
        let first = r.get_u8();
        let tag = first >> 6;
        let mut x = u64::from(first & 0b0011_1111);
        match tag {
            0b00 => {}
            0b01 => {
                if r.remaining() < 1 {
                    return Err(UnexpectedEnd(1));
                }
                x = (x << 8) | u64::from(r.get_u8());
            }
            0b10 => {
                if r.remaining() < 3 {
                    return Err(UnexpectedEnd(2));
                }
                x = (x << 24)
                    | u64::from(r.get_u8()) << 16
                    | u64::from(r.get_u8()) << 8
                    | u64::from(r.get_u8());
            }
            // tag 0b11: 8-byte encoding, the only remaining case
            _ => {
                if r.remaining() < 7 {
                    return Err(UnexpectedEnd(3));
                }
                for _ in 0..7 {
                    x = (x << 8) | u64::from(r.get_u8());
                }
            }
        }
        Ok(VarInt(x))
    }

    /// Encode to the tail of `w`.
    pub fn encode<B: BufMut>(&self, w: &mut B) {
        let x = self.0;
        if x < (1 << 6) {
            w.put_u8(x as u8);
        } else if x < (1 << 14) {
            w.put_u16((0b01 << 14) | x as u16);
        } else if x < (1 << 30) {
            w.put_u32((0b10 << 30) | x as u32);
        } else {
            w.put_u64((0b11 << 62) | x);
        }
    }
}

impl From<VarInt> for u64 {
    fn from(x: VarInt) -> u64 {
        x.0
    }
}

impl From<u8> for VarInt {
    fn from(x: u8) -> Self {
        VarInt(x.into())
    }
}

impl From<u16> for VarInt {
    fn from(x: u16) -> Self {
        VarInt(x.into())
    }
}

impl From<u32> for VarInt {
    fn from(x: u32) -> Self {
        VarInt(x.into())
    }
}

impl TryFrom<u64> for VarInt {
    type Error = VarIntBoundsExceeded;

    /// Succeeds iff `x` < 2^62.
    fn try_from(x: u64) -> Result<Self, VarIntBoundsExceeded> {
        VarInt::from_u64(x)
    }
}

impl TryFrom<usize> for VarInt {
    type Error = VarIntBoundsExceeded;

    /// Succeeds iff `x` < 2^62.
    fn try_from(x: usize) -> Result<Self, VarIntBoundsExceeded> {
        VarInt::from_u64(x as u64)
    }
}

impl fmt::Debug for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Error returned when the input ends before a varint is complete, carrying
/// the number of missing bytes.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UnexpectedEnd(pub usize);

impl fmt::Display for UnexpectedEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected end, missing {} bytes", self.0)
    }
}

/// Error returned when constructing a `VarInt` from a value >= 2^62.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct VarIntBoundsExceeded(pub u64);

impl fmt::Display for VarIntBoundsExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "value {} exceeds varint range", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: u64) {
        let v = VarInt::from_u64(value).unwrap();
        assert_eq!(v.into_inner(), value);
        let mut out = Vec::new();
        v.encode(&mut out);
        assert_eq!(out.len(), v.size());
        let mut read: &[u8] = &out;
        assert_eq!(VarInt::decode(&mut read).unwrap(), v);
        assert!(!read.has_remaining());
    }

    #[test]
    fn boundaries_round_trip() {
        // 1-byte range
        round_trip(0);
        round_trip(63);
        // 2-byte range
        round_trip(64);
        round_trip(16383);
        // 4-byte range
        round_trip(16384);
        round_trip((1 << 30) - 1);
        // 8-byte range
        round_trip(1 << 30);
        round_trip((1 << 62) - 1);
    }

    #[test]
    fn size_selection() {
        assert_eq!(VarInt::from_u64(0).unwrap().size(), 1);
        assert_eq!(VarInt::from_u64(63).unwrap().size(), 1);
        assert_eq!(VarInt::from_u64(64).unwrap().size(), 2);
        assert_eq!(VarInt::from_u64(16384).unwrap().size(), 4);
        assert_eq!(VarInt::from_u64(1 << 30).unwrap().size(), 8);
        assert_eq!(VarInt::from_u64((1 << 62) - 1).unwrap().size(), 8);
    }

    #[test]
    fn encoded_size_from_first_byte() {
        assert_eq!(VarInt::encoded_size(0b00 << 6), 1);
        assert_eq!(VarInt::encoded_size(0b01 << 6), 2);
        assert_eq!(VarInt::encoded_size(0b10 << 6), 4);
        assert_eq!(VarInt::encoded_size(0b11 << 6), 8);
    }

    #[test]
    fn bounds_enforced() {
        assert!(VarInt::from_u64((1 << 62) - 1).is_ok());
        assert!(VarInt::from_u64(1 << 62).is_err());
        assert!(VarInt::from_u64(u64::MAX).is_err());
    }

    #[test]
    fn maximum_value_encodes_in_8_bytes() {
        let v = VarInt::from_u64((1 << 62) - 1).unwrap();
        let mut out = Vec::new();
        v.encode(&mut out);
        // tag bits 0b11 followed by the 62-bit value, all ones
        assert_eq!(out, vec![0xff; 8]);
        // decoding consumes the whole buffer
        let mut read: &[u8] = &out;
        assert_eq!(VarInt::decode(&mut read).unwrap(), v);
    }

    #[test]
    fn truncated_input_errors_with_missing_byte_count() {
        let mut empty: &[u8] = &[];
        assert_eq!(VarInt::decode(&mut empty), Err(UnexpectedEnd(0)));

        // 2-byte varint missing its second byte
        let mut short2: &[u8] = &[0b01 << 6];
        assert_eq!(VarInt::decode(&mut short2), Err(UnexpectedEnd(1)));

        // 4-byte varint missing three bytes
        let mut short4: &[u8] = &[0b10 << 6, 0xaa, 0xbb];
        assert_eq!(VarInt::decode(&mut short4), Err(UnexpectedEnd(2)));

        // 8-byte varint missing seven bytes
        let mut short8: &[u8] = &[0b11 << 6];
        assert_eq!(VarInt::decode(&mut short8), Err(UnexpectedEnd(3)));
    }

    #[test]
    fn encoding_is_deterministic_shortest_form() {
        let mut out = Vec::new();
        VarInt::from_u64(1).unwrap().encode(&mut out);
        assert_eq!(out, vec![0x01]);
        out.clear();
        VarInt::from_u64(64).unwrap().encode(&mut out);
        assert_eq!(out, vec![0b01 << 6 | 0, 64]);
        out.clear();
        VarInt::from_u64(16384).unwrap().encode(&mut out);
        assert_eq!(out, vec![0b10 << 6 | 0, 0, 0x40, 0x00]);
    }
}
