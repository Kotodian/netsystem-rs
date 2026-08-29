//! QPACK prefix strings (RFC 9204 Section 4.2): an optional Huffman flag, a
//! prefix-integer length, and the string bytes.
//!
//! Ported from `third_party/h3/h3/src/qpack/prefix_string/mod.rs`. Hammer
//! differences: errors surface as `QpackError` (h3's `Error` enum is folded
//! into `QpackError::HuffmanDecoding`/`HuffmanEncoding` and the existing
//! `UnexpectedEnd`), the length is validated against `Buf::remaining` before
//! the `usize` conversion instead of via `TryFromIntError`, and a zero-size
//! prefix is a typed `InvalidSize` rather than an h3 `assert!`.

use bytes::{Buf, BufMut};

use super::QpackError;
use super::huffman::{HpackStringDecode, HpackStringEncode};
use super::prefix_int;

/// Decode a prefix string. `size` is the total prefix size in bits: the low
/// bit carries the Huffman flag and the length uses the `size - 1` bits
/// above it (RFC 9204 Section 4.2).
pub(crate) fn decode<B: Buf>(size: u8, buf: &mut B) -> Result<Vec<u8>, QpackError> {
    // A 0-bit length prefix is not representable on the wire; h3 asserts the
    // size, Hammer reports it as a typed error.
    let size = size.checked_sub(1).ok_or(QpackError::InvalidSize)?;
    let (flags, len) = prefix_int::decode(size, buf)?;

    // `len` is an RFC byte count; it cannot exceed what the buffer holds.
    if len > buf.remaining() as u64 {
        return Err(QpackError::UnexpectedEnd);
    }
    let len = len as usize;

    let payload = buf.copy_to_bytes(len);
    let value = if flags & 1 == 0 {
        payload.to_vec()
    } else {
        let mut decoded = Vec::new();
        for byte in payload.to_vec().hpack_decode() {
            decoded.push(byte?);
        }
        decoded
    };
    Ok(value)
}

/// Encode a prefix string, always Huffman-coded with the Huffman flag set,
/// like h3. `flags` holds the field-line bits written above the prefix.
pub(crate) fn encode<B: BufMut>(
    size: u8,
    flags: u8,
    value: &[u8],
    buf: &mut B,
) -> Result<(), QpackError> {
    let size = size.checked_sub(1).ok_or(QpackError::InvalidSize)?;
    let encoded = Vec::from(value).hpack_encode()?;
    prefix_int::encode(size, flags << 1 | 1, encoded.len() as u64, buf)?;
    buf.put_slice(&encoded);
    Ok(())
}
