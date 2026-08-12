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
//! Field line representations (RFC 9204 Sections 4.5.2-4.5.6) are a later
//! slice.

use bytes::{Buf, BufMut};

use super::{QpackError, prefix_int};

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
}
