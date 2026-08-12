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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn codec_6() {
        let mut buf = Vec::new();
        encode(6, 0b01, b"name without ref", &mut buf).unwrap();
        let mut read = Cursor::new(&buf);
        assert_eq!(
            &buf,
            &[
                0b0110_1100,
                168,
                116,
                149,
                79,
                6,
                76,
                231,
                181,
                42,
                88,
                89,
                127
            ]
        );
        assert_eq!(decode(6, &mut read).unwrap(), b"name without ref");
    }

    #[test]
    fn codec_8() {
        let mut buf = Vec::new();
        encode(8, 0b01, b"name with ref", &mut buf).unwrap();
        let mut read = Cursor::new(&buf);
        assert_eq!(
            &buf,
            &[0b1000_1010, 168, 116, 149, 79, 6, 76, 234, 88, 89, 127]
        );
        assert_eq!(decode(8, &mut read).unwrap(), b"name with ref");
    }

    #[test]
    fn codec_8_empty() {
        let mut buf = Vec::new();
        encode(8, 0b01, b"", &mut buf).unwrap();
        let mut read = Cursor::new(&buf);
        assert_eq!(&buf, &[0b1000_0000]);
        assert_eq!(decode(8, &mut read).unwrap(), b"");
    }

    /// The Huffman flag is read from the wire: flag bit 0 means the payload
    /// is the raw string bytes (RFC 9204 Section 4.2).
    #[test]
    fn decode_non_huffman() {
        let buf = vec![0b0100_0011, b'b', b'a', b'r'];
        let mut read = Cursor::new(&buf);
        assert_eq!(decode(6, &mut read).unwrap(), b"bar");
    }

    /// The length prefix promises more bytes than the buffer holds.
    #[test]
    fn decode_too_short() {
        let buf = vec![0b0100_0011, b'b', b'a'];
        let mut read = Cursor::new(&buf);
        assert_eq!(decode(6, &mut read), Err(QpackError::UnexpectedEnd));
    }

    /// A zero-size prefix is not representable on the wire.
    #[test]
    fn zero_size_prefix_is_invalid() {
        let mut buf: &[u8] = &[0];
        assert_eq!(decode(0, &mut buf), Err(QpackError::InvalidSize));
        assert_eq!(
            encode(0, 0, b"x", &mut Vec::new()),
            Err(QpackError::InvalidSize)
        );
    }
}
