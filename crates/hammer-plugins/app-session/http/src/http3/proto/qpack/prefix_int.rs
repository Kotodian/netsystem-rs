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
