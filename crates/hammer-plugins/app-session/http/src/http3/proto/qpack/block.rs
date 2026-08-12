//! QPACK encoded field section prefix (RFC 9204 Section 4.5.1).
//!
//! This slice is capacity-zero: there is no dynamic table, so the only
//! accepted prefix is Required Insert Count = 0 and Delta Base = 0 with the
//! sign bit clear — the canonical `0x00 0x00` bytes VPP's serializer emits
//! (`third_party/vpp/src/plugins/http/http3/qpack.c`,
//! `qpack_serialize_request` / `qpack_serialize_response`: "two zero bytes
//! because we don't use dynamic table").
//!
//! Ported from `third_party/h3/h3/src/qpack/block.rs` (`HeaderPrefix::decode`)
//! and VPP's `qpack_parse_headers_prefix`, restricted to the static-only
//! form: the sign bit rides above the 7-bit Delta Base prefix exactly as VPP
//! reads it (`ctx->delta_base_sign = *p & 0x80; ctx->delta_base =
//! hpack_decode_int (&p, end, 7)`). A nonzero Required Insert Count cannot be
//! satisfied by an empty dynamic table, and a nonzero or sign-set Delta Base
//! can only occur when the block references dynamic table entries, so both
//! surface as typed `QpackError`s; VPP reports the same conditions as
//! `HPACK_ERROR_COMPRESSION`. Truncated and overflowing prefixes propagate
//! the existing typed errors from `prefix_int`.
//!
//! Field line representations (RFC 9204 Sections 4.5.2-4.5.6): this slice
//! implements the indexed form (Section 4.5.2); literal and post-base
//! representations are a later slice.

use bytes::{Buf, BufMut};

use super::field::HeaderField;
use super::{QpackError, prefix_int, static_table};

/// Decode the encoded field section prefix (RFC 9204 Section 4.5.1).
///
/// Capacity-zero only: Required Insert Count = 0 and Delta Base = 0 with the
/// sign bit clear, the `0x00 0x00` form VPP's serializer writes. Consumes
/// exactly the two prefix bytes and leaves the encoded field lines for the
/// caller.
pub(crate) fn decode_prefix<B: Buf>(buf: &mut B) -> Result<(), QpackError> {
    let (_, required_insert_count) = prefix_int::decode(8, buf)?;
    let (sign, delta_base) = prefix_int::decode(7, buf)?;
    if required_insert_count != 0 {
        return Err(QpackError::NonZeroInsertCount(required_insert_count));
    }
    if sign != 0 || delta_base != 0 {
        return Err(QpackError::NonZeroDeltaBase { sign, delta_base });
    }
    Ok(())
}

/// Encode the encoded field section prefix: Required Insert Count = 0 and
/// Delta Base = 0, the only form `decode_prefix` accepts. Two zero bytes,
/// exactly what VPP's serializers write.
pub(crate) fn encode_prefix<W: BufMut>(buf: &mut W) {
    buf.put_u8(0);
    buf.put_u8(0);
}

/// Decode one field line, recognizing only the indexed representation
/// (RFC 9204 Section 4.5.2, `1Txxxxxx`).
///
/// A static reference (T = 1) resolves the 6-bit prefix index through
/// `static_table::get` in O(1), returning the exact entry as a `'static`
/// borrow — the same field VPP copies out for `case 12 ... 15` in
/// `qpack_parse_headers` (`third_party/vpp/src/plugins/http/http3/qpack.c`,
/// qpack.c:960-975). A dynamic reference (T = 0) cannot be resolved by this
/// capacity-zero decoder and is the typed `QpackError::DynamicReference`;
/// VPP reports the same condition as `HPACK_ERROR_COMPRESSION`
/// (`case 8 ... 11`).
///
/// The other representations (RFC 9204 Sections 4.5.3-4.5.6) are later
/// slices: the first byte is only peeked, so on `Ok(None)` the input is left
/// untouched for the decoder that handles them. Truncated and overflowing
/// indices propagate the typed errors from `prefix_int`.
pub(crate) fn decode_field_line<B: Buf>(
    buf: &mut B,
) -> Result<Option<&'static HeaderField>, QpackError> {
    let first = match buf.chunk().first() {
        Some(&first) => first,
        // No first byte at all: the field line is truncated, not merely of
        // another representation.
        None => return Err(QpackError::UnexpectedEnd),
    };
    if first & 0b1000_0000 == 0 {
        return Ok(None);
    }
    match prefix_int::decode(6, buf)? {
        (0b11, index) => Ok(Some(static_table::get(index as usize)?)),
        // T bit clear: an indexed reference into the dynamic table.
        (0b10, _) => Err(QpackError::DynamicReference),
        // The peek above admits only bytes with the top bit set, whose flags
        // are exactly 0b10 or 0b11, so this arm cannot fire; `Ok(None)`
        // keeps it typed rather than unreachable.
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_prefix_bytes(bytes: &[u8]) -> Result<(), QpackError> {
        let mut read: &[u8] = bytes;
        decode_prefix(&mut read)
    }

    #[test]
    fn encode_prefix_writes_two_zero_bytes() {
        let mut out = Vec::new();
        encode_prefix(&mut out);
        assert_eq!(out, vec![0x00, 0x00]);
    }

    /// The canonical zero prefix decodes and consumes exactly two bytes,
    /// leaving the field lines untouched.
    #[test]
    fn decode_prefix_accepts_only_the_canonical_zero_form() {
        assert_eq!(decode_prefix_bytes(&[0x00, 0x00]), Ok(()));

        let mut read: &[u8] = &[0x00, 0x00, 0xD1];
        assert_eq!(decode_prefix(&mut read), Ok(()));
        assert_eq!(read.chunk(), &[0xD1]);
    }

    /// Required Insert Count > 0 cannot be satisfied by an empty dynamic
    /// table, so the block can only contain references we cannot resolve.
    #[test]
    fn decode_prefix_rejects_nonzero_insert_count() {
        assert_eq!(
            decode_prefix_bytes(&[0x01, 0x00]),
            Err(QpackError::NonZeroInsertCount(1))
        );
        assert_eq!(
            decode_prefix_bytes(&[0x80, 0x00]),
            Err(QpackError::NonZeroInsertCount(128))
        );
        assert_eq!(
            decode_prefix_bytes(&[0xFF, 0x02, 0x00]),
            Err(QpackError::NonZeroInsertCount(257))
        );
    }

    /// A nonzero Delta Base (or a set sign bit) only occurs when the block
    /// references dynamic table entries below the base.
    #[test]
    fn decode_prefix_rejects_nonzero_delta_base() {
        assert_eq!(
            decode_prefix_bytes(&[0x00, 0x01]),
            Err(QpackError::NonZeroDeltaBase {
                sign: 0,
                delta_base: 1
            })
        );
        assert_eq!(
            decode_prefix_bytes(&[0x00, 0xFF, 0x02]),
            Err(QpackError::NonZeroDeltaBase {
                sign: 1,
                delta_base: 129
            })
        );
    }

    /// With Required Insert Count = 0 the base must be 0, so a set sign bit
    /// makes the prefix invalid (RFC 9204 Section 4.5.1.1: a field block
    /// with a sign bit of 1 whose Delta Base is not less than the Required
    /// Insert Count is invalid).
    #[test]
    fn decode_prefix_rejects_set_sign_bit() {
        assert_eq!(
            decode_prefix_bytes(&[0x00, 0x80]),
            Err(QpackError::NonZeroDeltaBase {
                sign: 1,
                delta_base: 0
            })
        );
    }

    #[test]
    fn decode_prefix_truncated_rejected() {
        assert_eq!(decode_prefix_bytes(&[]), Err(QpackError::UnexpectedEnd));
        assert_eq!(decode_prefix_bytes(&[0x00]), Err(QpackError::UnexpectedEnd));
        assert_eq!(decode_prefix_bytes(&[0xFF]), Err(QpackError::UnexpectedEnd));
        // Parse precedes validation, like h3 and VPP: a nonzero insert
        // count with a truncated Delta Base reports the truncation.
        assert_eq!(decode_prefix_bytes(&[0x80]), Err(QpackError::UnexpectedEnd));
    }

    #[test]
    fn decode_prefix_overflow_rejected() {
        let bytes = [
            0x00, 0xFF, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
        ];
        assert_eq!(decode_prefix_bytes(&bytes), Err(QpackError::Overflow));
    }

    fn decode_line(bytes: &[u8]) -> Result<Option<&'static HeaderField>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_field_line(&mut read)
    }

    /// Short indices decode in a single byte and resolve to the exact
    /// static-table entry: 0xC1 is `:path` "/" and 0xD1 is `:method` "GET".
    #[test]
    fn decode_indexed_static_short_index_matches_table() {
        let cases: [(u8, usize); 3] = [(0xC0, 0), (0xC1, 1), (0xD1, 17)];
        for (wire, index) in cases {
            let mut read: &[u8] = &[wire];
            assert_eq!(
                decode_field_line(&mut read),
                Ok(Some(static_table::get(index).expect("index in range")))
            );
            assert!(!read.has_remaining());
        }
    }

    /// Long indices use continuation bytes: 0xFF 0x23 is 63 + 35 = 98 and
    /// 0xFF 0x00 is 63 + 0 = 63.
    #[test]
    fn decode_indexed_static_long_index_matches_table() {
        let cases: [(&[u8], usize); 2] = [(&[0xFF, 0x23], 98), (&[0xFF, 0x00], 63)];
        for (wire, index) in cases {
            let mut read: &[u8] = wire;
            assert_eq!(
                decode_field_line(&mut read),
                Ok(Some(static_table::get(index).expect("index in range")))
            );
            assert!(!read.has_remaining());
        }
    }

    /// T bit clear is a dynamic-table reference; with no dynamic table this
    /// capacity-zero decoder cannot resolve it (RFC 9204 Section 4.5.2).
    #[test]
    fn decode_indexed_dynamic_reference_is_typed_error() {
        let wires: [&[u8]; 3] = [&[0x80], &[0x81], &[0xBF, 0x00]];
        for wire in wires {
            assert_eq!(decode_line(wire), Err(QpackError::DynamicReference));
        }
    }

    /// The other representations are later slices; the first byte is left in
    /// the input for the decoder that handles them.
    #[test]
    fn decode_field_line_ignores_other_representations() {
        let wires: [&[u8]; 4] = [&[0x00], &[0x10], &[0x20], &[0x40]];
        for wire in wires {
            let mut read: &[u8] = wire;
            assert_eq!(decode_field_line(&mut read), Ok(None));
            assert!(read.has_remaining());
        }
    }

    /// 0xFF 0x40 is 63 + 64 = 127, beyond the fixed 99-entry table.
    #[test]
    fn decode_field_line_out_of_range_index() {
        assert_eq!(
            decode_line(&[0xFF, 0x40]),
            Err(QpackError::InvalidIndex(127))
        );
    }

    #[test]
    fn decode_field_line_truncated_rejected() {
        assert_eq!(decode_line(&[]), Err(QpackError::UnexpectedEnd));
        assert_eq!(decode_line(&[0xFF]), Err(QpackError::UnexpectedEnd));
        assert_eq!(decode_line(&[0xBF]), Err(QpackError::UnexpectedEnd));
    }

    #[test]
    fn decode_field_line_overflow_rejected() {
        let bytes = [0xFF, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(decode_line(&bytes), Err(QpackError::Overflow));
    }
}
