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
//! implements the indexed form (Section 4.5.2), the literal form with static
//! name reference (Section 4.5.4) and the literal-name form (Section 4.5.6);
//! the post-base forms (Sections 4.5.3 and 4.5.5) are recognized and
//! rejected as dynamic-table references. `decode_block` composes the prefix
//! and the five selectors into the full field-section decoder.

use std::borrow::Cow;

use bytes::{Buf, BufMut};

use super::field::HeaderField;
use super::{QpackError, prefix_int, prefix_string, static_table};

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

/// Decode a complete encoded field section (RFC 9204 Section 4.5): the
/// prefix first, then every field line until the input is exhausted,
/// returning the ordered fields.
///
/// This composes `decode_prefix` with the field-line selectors below,
/// mirroring VPP's `qpack_parse_request` / `qpack_parse_response`
/// (`third_party/vpp/src/plugins/http/http3/qpack.c`, qpack.c:1341-1397):
/// `qpack_parse_headers_prefix` first, then the per-field-line loop through
/// `qpack_decode_header` in VPP's dispatch order — indexed, literal with
/// name reference, literal with literal name, then the post-base rejections
/// (qpack.c:958-1023, case labels 12-15, 7/5, 3/2, 1, 0). h3's
/// `decode_stateless` (`third_party/h3/h3/src/qpack/decoder.rs`) is the
/// Rust reference: prefix, then the same `while has_remaining` loop.
/// An empty field section (only the two prefix bytes) decodes to an empty
/// block, as in h3.
///
/// Each iteration claims the first byte with exactly one selector, which
/// consumes at least that byte; a byte claimed by none falls through to the
/// typed `InvalidFieldPrefix`, so a non-consuming `Ok(None)` selector can
/// never spin the loop.
///
/// A static-table entry is cloned once, here at the ownership boundary of
/// the returned block; the clone copies the borrowed Cows' pointers without
/// allocating.
pub(crate) fn decode_block<B: Buf>(buf: &mut B) -> Result<Vec<HeaderField>, QpackError> {
    decode_prefix(buf)?;
    let mut fields = Vec::new();
    while buf.has_remaining() {
        let field = if let Some(field) = decode_field_line(buf)? {
            (*field).clone()
        } else if let Some(field) = decode_literal_name_ref(buf)? {
            field
        } else if let Some(field) = decode_literal_name(buf)? {
            field
        } else {
            // The post-base selectors reject rather than produce: a match is
            // the typed `DynamicReference`, a non-match `Ok(None)` falls
            // through.
            decode_post_base_indexed(buf)?;
            decode_post_base_name_ref(buf)?;
            // The selectors above partition every first byte, so no byte
            // reaches this arm; it keeps the dispatch total and typed so
            // that no non-consuming `Ok(None)` selector can ever spin the
            // loop.
            return Err(QpackError::InvalidFieldPrefix(buf.chunk()[0]));
        };
        fields.push(field);
    }
    Ok(fields)
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
/// The other representations (RFC 9204 Sections 4.5.3-4.5.6) are handled by
/// sibling decoders: the first byte is only peeked, so on `Ok(None)` the
/// input is left untouched for the decoder that handles them. Truncated and
/// overflowing indices propagate the typed errors from `prefix_int`.
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

/// Decode one field line, recognizing only the literal-with-name-reference
/// representation (RFC 9204 Section 4.5.4, `01N T xxxx`).
///
/// T = 1 resolves the 4-bit name index through `static_table::get` in O(1)
/// and reads the value as an 8-bit-prefix string (RFC 9204 Section 4.2),
/// returning a field whose name is the borrowed static-table entry and whose
/// value owns the decoded bytes. The N bit marks the field never-indexed:
/// it affects the encoder's indexing policy, not the decoded field, so it
/// is parsed and dropped here.
///
/// This mirrors VPP's `case 5`/`case 7` in `qpack_parse_headers`
/// (`third_party/vpp/src/plugins/http/http3/qpack.c`, qpack.c:982-997): the
/// 4-bit name index via `hpack_decode_int`, the name-only lookup
/// `qpack_get_static_table_entry (index, ..., 0)`, and the value via
/// `qpack_decode_string (&p, end, buf, buf_len, 8)`; `case 7` sets
/// `never_index` for the N bit, and the encoder writes `*a = never_index ?
/// 0x70 : 0x50` in `qpack_encode_header` (qpack.c:1057). h3's
/// `LiteralWithNameRef::decode` (`third_party/h3/h3/src/qpack/block.rs`) is
/// the Rust reference.
///
/// T = 0 is a name reference into the dynamic table, which this
/// capacity-zero decoder cannot resolve; VPP reports the same condition as
/// `HPACK_ERROR_COMPRESSION` (`case 4`/`case 6`, qpack.c:998-1001).
///
/// The other representations (RFC 9204 Sections 4.5.2, 4.5.3, 4.5.5,
/// 4.5.6) are handled by sibling decoders: the first byte is only peeked,
/// so on `Ok(None)` the input is left untouched. Truncated and overflowing
/// indices and prefix strings propagate the typed errors from `prefix_int`
/// and `prefix_string`; a name index beyond the fixed 99-entry table is
/// `QpackError::InvalidIndex`, and a failing value decode surfaces
/// `QpackError::HuffmanDecoding`.
pub(crate) fn decode_literal_name_ref<B: Buf>(
    buf: &mut B,
) -> Result<Option<HeaderField>, QpackError> {
    let first = match buf.chunk().first() {
        Some(&first) => first,
        // No first byte at all: the field line is truncated, not merely of
        // another representation.
        None => return Err(QpackError::UnexpectedEnd),
    };
    // The two high bits `01` select this representation.
    if first & 0b1100_0000 != 0b0100_0000 {
        return Ok(None);
    }
    let (flags, index) = prefix_int::decode(4, buf)?;
    if flags & 0b0001 == 0 {
        // T bit clear: a name reference into the dynamic table.
        return Err(QpackError::DynamicReference);
    }
    // The N bit (bit 5 of the first byte) marks the field never-indexed;
    // it affects the encoder's indexing policy, not the decoded field, so
    // it is dropped here.
    let name = static_table::get(index as usize)?.name.clone();
    let value = prefix_string::decode(8, buf)?;
    Ok(Some(HeaderField {
        name,
        value: Cow::Owned(value),
    }))
}

/// Decode one field line, recognizing only the literal-with-literal-name
/// representation (RFC 9204 Section 4.5.6, `001NHxxxx`).
///
/// The name is read as a 4-bit-prefix string (Huffman flag plus 3-bit length
/// prefix, RFC 9204 Section 4.2) and the value as an 8-bit-prefix string;
/// the returned field owns the decoded bytes of both. The N bit marks the
/// field never-indexed: it affects the encoder's indexing policy, not the
/// decoded field, so it is parsed and dropped here.
///
/// This mirrors VPP's `case 2`/`case 3` in `qpack_parse_headers`
/// (`third_party/vpp/src/plugins/http/http3/qpack.c`, qpack.c:1002-1017):
/// the name via `qpack_decode_string (&p, end, buf, buf_len, 4)` and the
/// value via the same call with an 8-bit prefix; `case 3` sets `never_index`
/// for the N bit, and the encoder writes `*a = never_index ? 0x30 : 0x20` in
/// `qpack_encode_custom_header` (qpack.c:1088-1092). h3's `Literal::decode`
/// (`third_party/h3/h3/src/qpack/block.rs`) is the Rust reference.
///
/// The other representations (RFC 9204 Sections 4.5.2-4.5.5) are handled by
/// sibling decoders: the first byte is only peeked, so on `Ok(None)` the
/// input is left untouched. Truncated and overflowing prefix strings
/// propagate the typed errors from `prefix_string`, and a failing Huffman
/// decode surfaces `QpackError::HuffmanDecoding`.
pub(crate) fn decode_literal_name<B: Buf>(buf: &mut B) -> Result<Option<HeaderField>, QpackError> {
    let first = match buf.chunk().first() {
        Some(&first) => first,
        // No first byte at all: the field line is truncated, not merely of
        // another representation.
        None => return Err(QpackError::UnexpectedEnd),
    };
    // The three high bits `001` select this representation.
    if first & 0b1110_0000 != 0b0010_0000 {
        return Ok(None);
    }
    // The N bit (bit 4 of the first byte) marks the field never-indexed; it
    // affects the encoder's indexing policy, not the decoded field, so it is
    // parsed and dropped here.
    let name = prefix_string::decode(4, buf)?;
    let value = prefix_string::decode(8, buf)?;
    Ok(Some(HeaderField {
        name: Cow::Owned(name),
        value: Cow::Owned(value),
    }))
}

/// Decode one field line, recognizing only the indexed-with-post-base-index
/// representation (RFC 9204 Section 4.5.3, `0001xxxx`).
///
/// A post-base index addresses the dynamic table below the Base
/// (Section 2.2.2.2), which this capacity-zero decoder cannot resolve, so
/// the typed `QpackError::DynamicReference` is returned; VPP reports the
/// same condition as `HPACK_ERROR_COMPRESSION` (`case 1` in
/// `qpack_parse_headers`, `third_party/vpp/src/plugins/http/http3/qpack.c`,
/// qpack.c:1018-1020). The index is decoded as a 4-bit prefix integer
/// (Section 4.1.1) before the rejection, so truncated and overflowing
/// indices surface the typed errors from `prefix_int`, exactly as
/// `hpack_decode_int`'s failure precedes VPP's compression error. h3's
/// `IndexedWithPostBase::decode` (`third_party/h3/h3/src/qpack/block.rs`)
/// is the Rust reference.
///
/// The other representations (RFC 9204 Sections 4.5.2, 4.5.4-4.5.6) are
/// handled by sibling decoders: the first byte is only peeked, so on
/// `Ok(None)` the input is left untouched.
pub(crate) fn decode_post_base_indexed<B: Buf>(buf: &mut B) -> Result<Option<()>, QpackError> {
    let first = match buf.chunk().first() {
        Some(&first) => first,
        // No first byte at all: the field line is truncated, not merely of
        // another representation.
        None => return Err(QpackError::UnexpectedEnd),
    };
    // The four high bits `0001` select this representation.
    if first & 0b1111_0000 != 0b0001_0000 {
        return Ok(None);
    }
    let _ = prefix_int::decode(4, buf)?;
    Err(QpackError::DynamicReference)
}

/// Decode one field line, recognizing only the literal-with-post-base-name-
/// reference representation (RFC 9204 Section 4.5.5, `0000Nxxx`).
///
/// A post-base name index addresses the dynamic table below the Base
/// (Section 2.2.2.2), which this capacity-zero decoder cannot resolve, so
/// the typed `QpackError::DynamicReference` is returned; VPP reports the
/// same condition as `HPACK_ERROR_COMPRESSION` (`case 0` in
/// `qpack_parse_headers`, `third_party/vpp/src/plugins/http/http3/qpack.c`,
/// qpack.c:1021-1023). The N bit (bit 3) marks the field never-indexed: it
/// affects the encoder's indexing policy, not the rejection, so both N
/// forms are rejected alike. The name index is decoded as a 3-bit prefix
/// integer with the N bit riding above it (Section 4.1.1) before the
/// rejection, so truncated and overflowing indices surface the typed errors
/// from `prefix_int`; h3's `LiteralWithPostBaseNameRef::decode`
/// (`third_party/h3/h3/src/qpack/block.rs`) is the Rust reference. The
/// value string is not read: the rejection precedes it, as in VPP's
/// `case 0`.
///
/// The other representations (RFC 9204 Sections 4.5.2-4.5.4, 4.5.6) are
/// handled by sibling decoders: the first byte is only peeked, so on
/// `Ok(None)` the input is left untouched.
pub(crate) fn decode_post_base_name_ref<B: Buf>(buf: &mut B) -> Result<Option<()>, QpackError> {
    let first = match buf.chunk().first() {
        Some(&first) => first,
        // No first byte at all: the field line is truncated, not merely of
        // another representation.
        None => return Err(QpackError::UnexpectedEnd),
    };
    // The four high bits `0000` select this representation.
    if first & 0b1111_0000 != 0 {
        return Ok(None);
    }
    let _ = prefix_int::decode(3, buf)?;
    Err(QpackError::DynamicReference)
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

    fn decode_block_bytes(bytes: &[u8]) -> Result<Vec<HeaderField>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_block(&mut read)
    }

    /// The canonical zero prefix with no field lines decodes to an empty
    /// block, as in h3's `decode_stateless`.
    #[test]
    fn decode_block_empty_section() {
        assert_eq!(decode_block_bytes(&[0x00, 0x00]), Ok(Vec::new()));
    }

    /// Fields decode in wire order: 0xD1 is index 17 (`:method` GET) and
    /// 0xC1 is index 1 (`:path` "/").
    #[test]
    fn decode_block_indexed_static_fields_in_order() {
        let fields = decode_block_bytes(&[0x00, 0x00, 0xD1, 0xC1]).expect("decoded");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], *static_table::get(17).unwrap());
        assert_eq!(fields[1], *static_table::get(1).unwrap());
        // The clone at the block boundary copies the borrowed Cows' pointers,
        // not the bytes.
        assert!(matches!(fields[0].name, Cow::Borrowed(_)));
        assert!(matches!(fields[0].value, Cow::Borrowed(_)));
    }

    /// Mixed representations decode in wire order: indexed static (0xC1),
    /// literal with static name reference (0x51 ...), literal with literal
    /// name (0x23 ...).
    #[test]
    fn decode_block_mixed_representations() {
        let fields = decode_block_bytes(&[
            0x00, 0x00, 0xC1, 0x51, 0x03, b'f', b'o', b'o', 0x23, b'f', b'o', b'o', 0x03, b'b',
            b'a', b'r',
        ])
        .expect("decoded");
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], *static_table::get(1).unwrap());
        assert_eq!(
            fields[1],
            HeaderField {
                name: static_table::get(1).unwrap().name.clone(),
                value: Cow::Owned(b"foo".to_vec()),
            }
        );
        assert_eq!(
            fields[2],
            HeaderField {
                name: Cow::Owned(b"foo".to_vec()),
                value: Cow::Owned(b"bar".to_vec()),
            }
        );
    }

    /// The prefix is validated before any field line: a nonzero Required
    /// Insert Count or Delta Base fails the whole block.
    #[test]
    fn decode_block_rejects_nonzero_prefix() {
        assert_eq!(
            decode_block_bytes(&[0x01, 0x00, 0xC1]),
            Err(QpackError::NonZeroInsertCount(1))
        );
        assert_eq!(
            decode_block_bytes(&[0x00, 0x01, 0xC1]),
            Err(QpackError::NonZeroDeltaBase {
                sign: 0,
                delta_base: 1
            })
        );
    }

    /// Dynamic-table references reject the whole block in every
    /// representation: indexed (0x80), literal name reference (0x40),
    /// indexed post-base (0x10) and literal post-base name reference (0x00).
    #[test]
    fn decode_block_dynamic_references_rejected() {
        let wires: [&[u8]; 4] = [
            &[0x00, 0x00, 0x80],
            &[0x00, 0x00, 0x40, 0x00],
            &[0x00, 0x00, 0x10],
            &[0x00, 0x00, 0x00],
        ];
        for wire in wires {
            assert_eq!(decode_block_bytes(wire), Err(QpackError::DynamicReference));
        }
    }

    #[test]
    fn decode_block_truncated_field_rejected() {
        assert_eq!(
            decode_block_bytes(&[0x00, 0x00, 0xFF]),
            Err(QpackError::UnexpectedEnd)
        );
        assert_eq!(
            decode_block_bytes(&[0x00, 0x00, 0x50, 0x03, b'a']),
            Err(QpackError::UnexpectedEnd)
        );
    }

    #[test]
    fn decode_block_overflow_rejected() {
        let bytes = [
            0x00, 0x00, 0xFF, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
        ];
        assert_eq!(decode_block_bytes(&bytes), Err(QpackError::Overflow));
    }

    #[test]
    fn decode_block_bad_huffman_rejected() {
        assert!(matches!(
            decode_block_bytes(&[0x00, 0x00, 0x50, 0x81, 0xFF]),
            Err(QpackError::HuffmanDecoding(_))
        ));
    }

    #[test]
    fn decode_block_out_of_range_index_rejected() {
        assert_eq!(
            decode_block_bytes(&[0x00, 0x00, 0xFF, 0x40]),
            Err(QpackError::InvalidIndex(127))
        );
    }

    /// A Huffman-coded literal round-trips through `prefix_string::encode`,
    /// the encoder-side twin of the 4-bit and 8-bit string decoders.
    #[test]
    fn decode_block_huffman_round_trip() {
        let mut wire = vec![0x00, 0x00];
        prefix_string::encode(4, 0b0010, b"foo", &mut wire).unwrap();
        prefix_string::encode(8, 0, b"bar", &mut wire).unwrap();
        let fields = decode_block_bytes(&wire).expect("decoded");
        assert_eq!(fields.len(), 1);
        assert_eq!(&fields[0].name[..], b"foo");
        assert_eq!(&fields[0].value[..], b"bar");
    }

    /// The selectors partition every first byte, so no byte can reach the
    /// `InvalidFieldPrefix` fallback; that arm keeps the dispatch total so a
    /// non-consuming `Ok(None)` selector can never spin the loop. Every
    /// single-byte field section must return some typed result instead.
    #[test]
    fn decode_block_claims_every_first_byte() {
        for first in 0u8..=255 {
            let mut read: &[u8] = &[0x00, 0x00, first];
            let result = decode_block(&mut read);
            assert!(
                !matches!(result, Err(QpackError::InvalidFieldPrefix(_))),
                "first byte {first:#04x} claimed by no selector"
            );
        }
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

    fn decode_literal(bytes: &[u8]) -> Result<Option<HeaderField>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_literal_name_ref(&mut read)
    }

    /// T = 1 with a short index: the name comes from the static table
    /// (entry 1 is `:path`), the value is the decoded wire string.
    #[test]
    fn decode_literal_name_ref_static_short_index() {
        let mut read: &[u8] = &[0x51, 0x03, b'f', b'o', b'o'];
        let field = decode_literal_name_ref(&mut read)
            .expect("decoded")
            .expect("recognized");
        assert_eq!(&field.name[..], b":path");
        assert_eq!(&field.value[..], b"foo");
        assert!(!read.has_remaining());
    }

    /// Long name indices use continuation bytes: 0x5F 0x02 is 15 + 2 = 17.
    #[test]
    fn decode_literal_name_ref_static_long_index() {
        let mut read: &[u8] = &[0x5F, 0x02, 0x03, b'f', b'o', b'o'];
        let field = decode_literal_name_ref(&mut read)
            .expect("decoded")
            .expect("recognized");
        assert_eq!(&field.name[..], b":method");
        assert_eq!(&field.value[..], b"foo");
        assert!(!read.has_remaining());
    }

    /// The N bit (0x70 vs 0x50) marks the field never-indexed; it affects
    /// indexing policy, not the decoded field, so both forms decode alike.
    #[test]
    fn decode_literal_name_ref_never_index_bit_ignored() {
        let never = decode_literal(&[0x70, 0x03, b'b', b'a', b'r']);
        let plain = decode_literal(&[0x50, 0x03, b'b', b'a', b'r']);
        assert_eq!(never, plain);
    }

    /// A Huffman-coded value decodes through the seam (flag bit set in the
    /// 8-bit prefix string).
    #[test]
    fn decode_literal_name_ref_huffman_value() {
        let mut out = Vec::new();
        prefix_string::encode(8, 0, b"foo", &mut out).unwrap();
        let mut wire = vec![0x50];
        wire.extend_from_slice(&out);
        let field = decode_literal(&wire).expect("decoded").expect("recognized");
        assert_eq!(field.name, static_table::get(0).unwrap().name);
        assert_eq!(&field.value[..], b"foo");
    }

    /// T bit clear is a name reference into the dynamic table; with no
    /// dynamic table this capacity-zero decoder cannot resolve it (RFC
    /// 9204 Section 4.5.4).
    #[test]
    fn decode_literal_name_ref_dynamic_is_typed_error() {
        let wires: [&[u8]; 2] = [&[0x40, 0x03, b'a'], &[0x60, 0x00]];
        for wire in wires {
            assert_eq!(decode_literal(wire), Err(QpackError::DynamicReference));
        }
    }

    /// 0x5F 0x54 is 15 + 84 = 99, beyond the fixed 99-entry table.
    #[test]
    fn decode_literal_name_ref_out_of_range_index() {
        assert_eq!(
            decode_literal(&[0x5F, 0x54, 0x00]),
            Err(QpackError::InvalidIndex(99))
        );
    }

    #[test]
    fn decode_literal_name_ref_truncated_rejected() {
        assert_eq!(decode_literal(&[]), Err(QpackError::UnexpectedEnd));
        assert_eq!(decode_literal(&[0x50]), Err(QpackError::UnexpectedEnd));
        assert_eq!(
            decode_literal(&[0x50, 0x05, b'a']),
            Err(QpackError::UnexpectedEnd)
        );
    }

    /// A mid-code Huffman value propagates the typed decode error.
    #[test]
    fn decode_literal_name_ref_bad_huffman_value() {
        assert!(matches!(
            decode_literal(&[0x50, 0x81, 0xFF]),
            Err(QpackError::HuffmanDecoding(_))
        ));
    }

    /// The other representations are sibling decoders' seams; the first
    /// byte is left in the input for them.
    #[test]
    fn decode_literal_name_ref_ignores_other_representations() {
        let wires: [&[u8]; 4] = [&[0x00], &[0x10], &[0x20], &[0xC1]];
        for wire in wires {
            let mut read: &[u8] = wire;
            assert_eq!(decode_literal_name_ref(&mut read), Ok(None));
            assert!(read.has_remaining());
        }
    }

    fn decode_literal_literal(bytes: &[u8]) -> Result<Option<HeaderField>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_literal_name(&mut read)
    }

    /// A plain (non-Huffman) name and value: 0x23 is `001 N=0 H=0 len=3`,
    /// the value starts with its own 8-bit-prefix length 0x03.
    #[test]
    fn decode_literal_name_plain_wire() {
        let mut read: &[u8] = &[0x23, b'f', b'o', b'o', 0x03, b'b', b'a', b'r'];
        let field = decode_literal_name(&mut read)
            .expect("decoded")
            .expect("recognized");
        assert_eq!(&field.name[..], b"foo");
        assert_eq!(&field.value[..], b"bar");
        assert!(!read.has_remaining());
    }

    /// The encoder side always Huffman-codes, so a round trip through
    /// `prefix_string::encode` covers the Huffman path for both strings:
    /// `encode(4, 0b0010, ...)` writes the `001 N=0 H=1` first byte exactly
    /// as h3's `Literal::encode` and VPP's `0x20` do.
    #[test]
    fn decode_literal_name_huffman_round_trip() {
        let mut wire = Vec::new();
        prefix_string::encode(4, 0b0010, b"foo", &mut wire).unwrap();
        prefix_string::encode(8, 0, b"bar", &mut wire).unwrap();
        let field = decode_literal_literal(&wire)
            .expect("decoded")
            .expect("recognized");
        assert_eq!(&field.name[..], b"foo");
        assert_eq!(&field.value[..], b"bar");
    }

    /// The N bit (0x30 vs 0x20 in VPP's encoder) marks the field
    /// never-indexed; it affects indexing policy, not the decoded field, so
    /// both forms decode alike.
    #[test]
    fn decode_literal_name_never_index_bit_ignored() {
        let never = decode_literal_literal(&[0x33, b'f', b'o', b'o', 0x03, b'b', b'a', b'r']);
        let plain = decode_literal_literal(&[0x23, b'f', b'o', b'o', 0x03, b'b', b'a', b'r']);
        assert_eq!(never, plain);
    }

    /// Zero-length name and value strings still consume their length bytes.
    #[test]
    fn decode_literal_name_empty_strings() {
        let mut read: &[u8] = &[0x20, 0x00];
        let field = decode_literal_name(&mut read)
            .expect("decoded")
            .expect("recognized");
        assert!(field.name.is_empty());
        assert!(field.value.is_empty());
        assert!(!read.has_remaining());
    }

    /// The other representations are sibling decoders' seams; the first
    /// byte is left in the input for them.
    #[test]
    fn decode_literal_name_ignores_other_representations() {
        let wires: [&[u8]; 5] = [&[0x00], &[0x10], &[0x40], &[0x80], &[0xC1]];
        for wire in wires {
            let mut read: &[u8] = wire;
            assert_eq!(decode_literal_name(&mut read), Ok(None));
            assert!(read.has_remaining());
        }
    }

    #[test]
    fn decode_literal_name_truncated_rejected() {
        assert_eq!(decode_literal_literal(&[]), Err(QpackError::UnexpectedEnd));
        // Name length 7 (continuation byte) with no name payload.
        assert_eq!(
            decode_literal_literal(&[0x27, 0x00]),
            Err(QpackError::UnexpectedEnd)
        );
        // Name shorter than its length prefix claims.
        assert_eq!(
            decode_literal_literal(&[0x23, b'f', b'o']),
            Err(QpackError::UnexpectedEnd)
        );
        // Value length prefix missing after a complete name.
        assert_eq!(
            decode_literal_literal(&[0x20]),
            Err(QpackError::UnexpectedEnd)
        );
        // Value shorter than its length prefix claims.
        assert_eq!(
            decode_literal_literal(&[0x20, 0x03, b'a']),
            Err(QpackError::UnexpectedEnd)
        );
    }

    /// A name-length prefix integer that runs past the 63-bit mantissa.
    #[test]
    fn decode_literal_name_overflow_rejected() {
        let bytes = [0x27, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(decode_literal_literal(&bytes), Err(QpackError::Overflow));
    }

    /// A mid-code Huffman name or value propagates the typed decode error:
    /// 0x29 is `001 N=0 H=1 len=1` with the all-ones payload 0xFF; the value
    /// case reuses the `decode_literal_name_ref_bad_huffman_value` wire
    /// (0x81 length, 0xFF payload) after an empty name.
    #[test]
    fn decode_literal_name_bad_huffman() {
        assert!(matches!(
            decode_literal_literal(&[0x29, 0xFF]),
            Err(QpackError::HuffmanDecoding(_))
        ));
        assert!(matches!(
            decode_literal_literal(&[0x20, 0x81, 0xFF]),
            Err(QpackError::HuffmanDecoding(_))
        ));
    }

    fn decode_post_base_indexed_bytes(bytes: &[u8]) -> Result<Option<()>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_post_base_indexed(&mut read)
    }

    /// A post-base index addresses the dynamic table, which this
    /// capacity-zero decoder cannot resolve (RFC 9204 Section 4.5.3); VPP
    /// reports the same condition as `HPACK_ERROR_COMPRESSION` (`case 1`).
    #[test]
    fn decode_post_base_indexed_is_dynamic_reference() {
        let wires: [&[u8]; 4] = [&[0x10], &[0x11], &[0x1E], &[0x1F, 0x00]];
        for wire in wires {
            assert_eq!(
                decode_post_base_indexed_bytes(wire),
                Err(QpackError::DynamicReference)
            );
        }
    }

    /// The other representations, including the sibling post-base literal
    /// form, are other decoders' seams; the first byte is left in the
    /// input for them.
    #[test]
    fn decode_post_base_indexed_ignores_other_representations() {
        let wires: [&[u8]; 6] = [&[0x00], &[0x08], &[0x20], &[0x40], &[0x80], &[0xC1]];
        for wire in wires {
            let mut read: &[u8] = wire;
            assert_eq!(decode_post_base_indexed(&mut read), Ok(None));
            assert!(read.has_remaining());
        }
    }

    #[test]
    fn decode_post_base_indexed_truncated_rejected() {
        assert_eq!(
            decode_post_base_indexed_bytes(&[]),
            Err(QpackError::UnexpectedEnd)
        );
        // Index 15 (all-ones prefix) needs a continuation byte.
        assert_eq!(
            decode_post_base_indexed_bytes(&[0x1F]),
            Err(QpackError::UnexpectedEnd)
        );
    }

    /// An index prefix integer running past the 63-bit mantissa.
    #[test]
    fn decode_post_base_indexed_overflow_rejected() {
        let bytes = [0x1F, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(
            decode_post_base_indexed_bytes(&bytes),
            Err(QpackError::Overflow)
        );
    }

    fn decode_post_base_name_ref_bytes(bytes: &[u8]) -> Result<Option<()>, QpackError> {
        let mut read: &[u8] = bytes;
        decode_post_base_name_ref(&mut read)
    }

    /// The N bit (0x08) marks the field never-indexed; both N forms are
    /// post-base name references into the dynamic table, unresolvable here
    /// (RFC 9204 Section 4.5.5); VPP reports the same condition as
    /// `HPACK_ERROR_COMPRESSION` (`case 0`).
    #[test]
    fn decode_post_base_name_ref_is_dynamic_reference() {
        let wires: [&[u8]; 4] = [&[0x00], &[0x06], &[0x08], &[0x07, 0x00]];
        for wire in wires {
            assert_eq!(
                decode_post_base_name_ref_bytes(wire),
                Err(QpackError::DynamicReference)
            );
        }
    }

    /// The other representations, including the sibling post-base indexed
    /// form, are other decoders' seams; the first byte is left in the
    /// input for them.
    #[test]
    fn decode_post_base_name_ref_ignores_other_representations() {
        let wires: [&[u8]; 6] = [&[0x10], &[0x1F], &[0x20], &[0x40], &[0x80], &[0xC1]];
        for wire in wires {
            let mut read: &[u8] = wire;
            assert_eq!(decode_post_base_name_ref(&mut read), Ok(None));
            assert!(read.has_remaining());
        }
    }

    #[test]
    fn decode_post_base_name_ref_truncated_rejected() {
        assert_eq!(
            decode_post_base_name_ref_bytes(&[]),
            Err(QpackError::UnexpectedEnd)
        );
        // Index 7 (all-ones prefix) needs a continuation byte, N clear and
        // set.
        assert_eq!(
            decode_post_base_name_ref_bytes(&[0x07]),
            Err(QpackError::UnexpectedEnd)
        );
        assert_eq!(
            decode_post_base_name_ref_bytes(&[0x0F]),
            Err(QpackError::UnexpectedEnd)
        );
    }

    /// A name index prefix integer running past the 63-bit mantissa.
    #[test]
    fn decode_post_base_name_ref_overflow_rejected() {
        let bytes = [0x07, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
        assert_eq!(
            decode_post_base_name_ref_bytes(&bytes),
            Err(QpackError::Overflow)
        );
    }
}
