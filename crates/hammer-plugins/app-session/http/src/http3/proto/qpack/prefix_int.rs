//! QPACK prefix integers (RFC 9204 Section 4.1.1), the same encoding as
//! HTTP/2 HPACK integers.
//!
//! Ported from `third_party/h3/h3/src/qpack/prefix_int.rs`: unlike h3 there
//! are no `assert!`s — an invalid prefix size is a typed error.

use bytes::{Buf, BufMut};

use super::super::coding::BufExt;
use super::QpackError;

/// Decode a prefix integer, returning the flag bits above the prefix and the
/// value. Single pass: one byte for short values, otherwise a running
/// 7-bit mantissa with an overflow guard.
pub fn decode<B: Buf>(size: u8, buf: &mut B) -> Result<(u8, u64), QpackError> {
    // VPP constrains prefixes to 1..=8 bits (`ASSERT (prefix_len >= 1 ...)`);
    // a 0-bit prefix would shift the mask out of its byte.
    if !(1..=8).contains(&size) {
        return Err(QpackError::InvalidSize);
    }
    let first = buf.get::<u8>()?;

    // The casts to u8 trim the bits above the prefix.
    let flags = ((first as usize) >> size) as u8;
    let mask = 0xFF >> (8 - size);
    let first = first & mask;

    if first < mask {
        return Ok((flags, first as u64));
    }

    let mut value = mask as u64;
    let mut power = 0u8;
    loop {
        let byte = buf.get::<u8>()? as u64;
        value += (byte & 127) << power;
        power += 7;

        if byte & 128 == 0 {
            break;
        }
        if power >= MAX_POWER {
            return Err(QpackError::Overflow);
        }
    }

    Ok((flags, value))
}

/// Encode a prefix integer. `flags` holds the bits written above the prefix.
pub fn encode<B: BufMut>(size: u8, flags: u8, value: u64, buf: &mut B) -> Result<(), QpackError> {
    if !(1..=8).contains(&size) {
        return Err(QpackError::InvalidSize);
    }
    let mask = !(0xFF << size) as u8;
    let flags = ((flags as usize) << size) as u8;

    if value < (mask as u64) {
        buf.put_u8(flags | value as u8);
        return Ok(());
    }

    buf.put_u8(mask | flags);
    let mut remaining = value - mask as u64;

    while remaining >= 128 {
        let rest = (remaining % 128) as u8;
        buf.put_u8(rest + 128);
        remaining /= 128;
    }
    buf.put_u8(remaining as u8);
    Ok(())
}

/// A value of 63+7 bits can never fit the RFC 9204 2^63 bound.
const MAX_POWER: u8 = 9 * 7;

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(size: u8, flags: u8, value: u64) {
        let mut out = Vec::new();
        encode(size, flags, value, &mut out).unwrap();
        let mut read: &[u8] = &out;
        assert_eq!(decode(size, &mut read), Ok((flags, value)));
        assert!(!read.has_remaining());
    }

    #[test]
    fn short_values_encode_in_one_byte() {
        round_trip(8, 0, 12); // 12 < 2^8 - 1; an 8-bit prefix leaves no flag bits
        round_trip(6, 0b10, 0);
        round_trip(7, 0b1, 63);
    }

    #[test]
    fn long_values_use_continuation_bytes() {
        round_trip(8, 0, 255);
        round_trip(8, 0, 16384);
        round_trip(4, 0b0001, 42);
        round_trip(6, 0b11, 42);
        round_trip(8, 0, (1 << 62) - 1);
    }

    #[test]
    fn flags_are_preserved() {
        let mut out = Vec::new();
        encode(5, 0b011, 1000, &mut out).unwrap();
        let mut read: &[u8] = &out;
        assert_eq!(decode(5, &mut read), Ok((0b011, 1000)));
    }

    #[test]
    fn truncated_input_errors() {
        let mut empty: &[u8] = &[];
        assert_eq!(decode(8, &mut empty), Err(QpackError::UnexpectedEnd));
        // continuation byte promised but absent
        let mut short: &[u8] = &[0xff, 0x80];
        assert_eq!(decode(8, &mut short), Err(QpackError::UnexpectedEnd));
    }

    #[test]
    fn overflow_detected() {
        // nine continuation bytes carry a value beyond the 63-bit mantissa
        let mut buf: &[u8] = &[0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(decode(8, &mut buf), Err(QpackError::Overflow));
    }

    #[test]
    fn invalid_size_rejected_without_panicking() {
        let mut buf: &[u8] = &[0xff];
        assert_eq!(decode(9, &mut buf), Err(QpackError::InvalidSize));
        assert_eq!(decode(0, &mut buf), Err(QpackError::InvalidSize));
        assert_eq!(
            encode(9, 0, 1, &mut Vec::new()),
            Err(QpackError::InvalidSize)
        );
        assert_eq!(
            encode(0, 0, 1, &mut Vec::new()),
            Err(QpackError::InvalidSize)
        );
    }

    #[test]
    fn maximum_value_round_trips() {
        // 2^62 - 1 requires 10 bytes: prefix byte + nine continuation bytes
        let mut out = Vec::new();
        encode(8, 0, (1 << 62) - 1, &mut out).unwrap();
        assert_eq!(out.len(), 10);
        let mut read: &[u8] = &out;
        assert_eq!(decode(8, &mut read), Ok((0, (1 << 62) - 1)));
    }

    /// Exact wire bytes from the vendored h3 vectors (RFC 9204 Section 4.1.1).
    /// `value = 2^N - 1 + 128` (143 with a 4-bit prefix) is the boundary where
    /// the continuation must not end in 0x80.
    #[test]
    fn wire_bytes_match_spec() {
        let mut out = Vec::new();
        encode(5, 0b010, 1337, &mut out).unwrap();
        assert_eq!(out, vec![0b0101_1111, 154, 10]);

        let mut out = Vec::new();
        encode(4, 0b0001, 143, &mut out).unwrap();
        assert_eq!(out, vec![31, 128, 1]);

        let mut out = Vec::new();
        encode(8, 0, 424_242, &mut out).unwrap();
        assert_eq!(out, vec![255, 179, 240, 25]);
    }

    /// The largest value RFC 9204 Section 4.1.1 allows: with a 1-bit prefix
    /// the wire can carry 2^63 (vendored h3 `allow_62_bit`).
    #[test]
    fn two_to_sixty_three_boundary() {
        let mut out = Vec::new();
        encode(1, 1, 1 << 63, &mut out).unwrap();
        assert_eq!(out, vec![3, 255, 255, 255, 255, 255, 255, 255, 255, 127]);
        let mut read: &[u8] = &out;
        assert_eq!(decode(1, &mut read), Ok((1, 1 << 63)));
        assert!(!read.has_remaining());
    }

    /// The first byte can equal the mask even without a continuation byte
    /// being present; that must be UnexpectedEnd, not a short read.
    #[test]
    fn truncated_after_masked_prefix() {
        let mut short: &[u8] = &[0x1f];
        assert_eq!(decode(4, &mut short), Err(QpackError::UnexpectedEnd));
    }
}
