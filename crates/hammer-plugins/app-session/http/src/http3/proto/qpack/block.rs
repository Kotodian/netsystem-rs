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
//! and the five selectors into the full field-section decoder;
//! `encode_block` is the capacity-zero encoder, choosing a representation
//! per field line through the static-table reverse lookups
//! (`static_table::find` / `find_name`) exactly as h3's `encode_stateless`
//! and VPP's `qpack_encode_header` do.

use std::borrow::Cow;

use bytes::{Buf, BufMut};

use super::field::HeaderField;
use super::{QpackError, prefix_int, prefix_string, static_table};
use crate::http_common::FieldLineFlags;

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

/// Encode a complete encoded field section (RFC 9204 Section 4.5): the
/// `00 00` prefix first, then every field line in input order.
///
/// This is the encoder-side twin of `decode_block`: every line is a
/// static-table representation, so the block needs no dynamic-table state
/// and round-trips through the committed decoder unchanged. It mirrors h3's
/// `encode_stateless` (`third_party/h3/h3/src/qpack/encoder.rs`,
/// encoder.rs:192-215: `HeaderPrefix::new(0, 0, 0, 0)` then the per-field
/// static path) and VPP's `qpack_serialize_request` / `qpack_serialize_response`
/// (`third_party/vpp/src/plugins/http/http3/qpack.c`, qpack.c:1384-1510:
/// "two zero bytes because we don't use dynamic table").
///
/// Total work is O(sum of field bytes): each line costs one bounded
/// `static_table` reverse lookup plus the string encodings, and the only
/// allocations are the Huffman-coded string outputs of `prefix_string`.
pub(crate) fn encode_block<W: BufMut>(
    buf: &mut W,
    fields: &[HeaderField],
) -> Result<(), QpackError> {
    encode_prefix(buf);
    for field in fields {
        encode_field_line(buf, field)?;
    }
    Ok(())
}

/// Encode one field line, choosing the representation by reverse lookup:
/// an exact static-table match is the indexed form (RFC 9204 Section
/// 4.5.2), a static name match the literal-with-name-reference form
/// (Section 4.5.4), and anything else the literal-with-literal-name form
/// (Section 4.5.6).
///
/// Mirrors h3's `encode_stateless` field path (`third_party/h3/h3/src/
/// qpack/encoder.rs`, encoder.rs:201-210) and VPP's `qpack_encode_header`
/// (`third_party/vpp/src/plugins/http/http3/qpack.c`, qpack.c:1034-1079):
/// a full match through `static_table::find` writes `0xC0` plus the 6-bit
/// index; otherwise a name match through `static_table::find_name` writes
/// `0x50` plus the 4-bit index and the value string; otherwise the
/// literal-name form writes `0x20` and both strings. Both reverse lookups
/// are explicit fixed matches in bounded O(1), never a scan of the 99-entry
/// table; the strings always Huffman-code through `prefix_string::encode`,
/// as h3's encoder does. A literal form whose field carries `NEVER_INDEX`
/// sets the wire N bit (`0x70` / `0x30`), exactly VPP's `qpack_encode_header`
/// (`*a = never_index ? 0x70 : 0x50`, qpack.c:1057) and
/// `qpack_encode_custom_header` (`*a = never_index ? 0x30 : 0x20`,
/// qpack.c:1090); the indexed form has no N bit, so the flag is dropped
/// there, as in VPP.
pub(crate) fn encode_field_line<W: BufMut>(
    buf: &mut W,
    field: &HeaderField,
) -> Result<(), QpackError> {
    if let Some(index) = static_table::find(field) {
        prefix_int::encode(6, 0b11, index as u64, buf)?;
    } else if let Some(index) = static_table::find_name(field.name.as_ref()) {
        // The N bit rides above the 4-bit index: `0b0111` with `NEVER_INDEX`,
        // exactly VPP's `*a = never_index ? 0x70 : 0x50` (qpack.c:1057).
        let flags = if field.flags.contains(FieldLineFlags::NEVER_INDEX) {
            0b0111
        } else {
            0b0101
        };
        prefix_int::encode(4, flags, index as u64, buf)?;
        prefix_string::encode(8, 0, field.value.as_ref(), buf)?;
    } else {
        // The N bit rides above the 3-bit name length: `0b0011` with
        // `NEVER_INDEX`, exactly VPP's `*a = never_index ? 0x30 : 0x20`
        // (qpack.c:1090).
        let flags = if field.flags.contains(FieldLineFlags::NEVER_INDEX) {
            0b0011
        } else {
            0b0010
        };
        prefix_string::encode(4, flags, field.name.as_ref(), buf)?;
        prefix_string::encode(8, 0, field.value.as_ref(), buf)?;
    }
    Ok(())
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
/// value owns the decoded bytes. The N bit marks the field never-indexed
/// and is carried into `flags` as `NEVER_INDEX`, the same merge VPP performs
/// into its name token at `http.h:1114`.
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
    let (prefix, index) = prefix_int::decode(4, buf)?;
    if prefix & 0b0001 == 0 {
        // T bit clear: a name reference into the dynamic table.
        return Err(QpackError::DynamicReference);
    }
    // The N bit (bit 5 of the first byte) marks the field never-indexed; it
    // is carried into `flags` as `NEVER_INDEX`, exactly the merge VPP
    // performs in `case 7` (`*never_index = 1`, qpack.c:980-983).
    let flags = if first & 0b0010_0000 != 0 {
        FieldLineFlags::NEVER_INDEX
    } else {
        FieldLineFlags::empty()
    };
    let name = static_table::get(index as usize)?.name.clone();
    let value = prefix_string::decode(8, buf)?;
    Ok(Some(HeaderField {
        name,
        value: Cow::Owned(value),
        flags,
    }))
}

/// Decode one field line, recognizing only the literal-with-literal-name
/// representation (RFC 9204 Section 4.5.6, `001NHxxxx`).
///
/// The name is read as a 4-bit-prefix string (Huffman flag plus 3-bit length
/// prefix, RFC 9204 Section 4.2) and the value as an 8-bit-prefix string;
/// the returned field owns the decoded bytes of both. The N bit marks the
/// field never-indexed and is carried into `flags` as `NEVER_INDEX`, the
/// same merge VPP performs into its name token at `http.h:1114`.
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
    // is carried into `flags` as `NEVER_INDEX`, exactly the merge VPP
    // performs in `case 3` (`*never_index = 1`, qpack.c:1003-1006).
    let flags = if first & 0b0001_0000 != 0 {
        FieldLineFlags::NEVER_INDEX
    } else {
        FieldLineFlags::empty()
    };
    let name = prefix_string::decode(4, buf)?;
    let value = prefix_string::decode(8, buf)?;
    Ok(Some(HeaderField {
        name: Cow::Owned(name),
        value: Cow::Owned(value),
        flags,
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
