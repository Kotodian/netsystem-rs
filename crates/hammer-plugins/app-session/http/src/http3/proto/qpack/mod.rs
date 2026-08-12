//! QPACK (RFC 9204) primitives: prefix integers, prefix strings, Huffman
//! coding, field lines, and the static table.
//!
//! This slice wires the prefix integer codec (`prefix_int`), the prefix
//! string codec (`prefix_string`) with its Huffman coding (`huffman`), the
//! field line type (`field`), the fixed 99-entry static table
//! (`static_table`), and the capacity-zero encoded field section prefix
//! (`block`). Field line parsing, the encoder and the decoder are later
//! slices.
//!
//! References:
//! - RFC 9204 Section 4.1.1 (prefix integers), Section 4.2 (prefix strings
//!   and Huffman coding), Appendix A (static table), RFC 7541 Section 4.1
//!   (entry size accounting) and Appendix B (Huffman table)
//! - `third_party/h3/h3/src/qpack/{mod,prefix_int,prefix_string,field,static_}.rs`
//! - `third_party/vpp/src/plugins/http/http3/qpack.c` (static table),
//!   `third_party/vpp/src/plugins/http/http2/hpack_inlines.h` (integer
//!   codec and Huffman decode), `huffman_table.h` (RFC 7541 Appendix B
//!   data)

use super::varint::UnexpectedEnd;

pub(crate) mod block;
pub(crate) mod field;
pub(crate) mod huffman;
pub(crate) mod prefix_int;
pub(crate) mod prefix_string;
pub(crate) mod static_table;

/// Errors produced by the QPACK wire primitives wired in this slice.
///
/// The variant set is what the prefix and static table primitives can
/// produce; later QPACK slices extend this only when their seams prove new
/// variants. Unlike h3 (which `assert!`s the prefix size) and VPP (which
/// `ASSERT`s it), an invalid prefix size is a typed error here.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum QpackError {
    /// A prefix size outside 1..=8 bits, which has no wire representation.
    InvalidSize,
    /// The value grew beyond the 63-bit mantissa of RFC 9204 Section 4.1.1.
    /// VPP reports the same condition as `HPACK_INVALID_INT`.
    Overflow,
    /// The input ended before the integer was complete.
    /// VPP reports the same condition as `HPACK_INVALID_INT`.
    UnexpectedEnd,
    /// A nonzero Required Insert Count in the encoded field section prefix
    /// (RFC 9204 Section 4.5.1). This capacity-zero slice has no dynamic
    /// table, so no insert count above zero can be satisfied; VPP reports
    /// the same condition as `HPACK_ERROR_COMPRESSION`.
    NonZeroInsertCount(u64),
    /// A nonzero or sign-set Delta Base in the encoded field section prefix.
    /// With no dynamic table the Base is necessarily 0, so a sign bit or a
    /// nonzero delta can only occur in a block that references dynamic
    /// entries.
    NonZeroDeltaBase { sign: u8, delta_base: u64 },
    /// A field-line or static-table index that resolves to no entry. The
    /// fixed 99-entry table (RFC 9204 Appendix A) is the only reference
    /// target this static-only decoder supports; VPP reports the failed
    /// static-table lookup as `HPACK_ERROR_COMPRESSION`.
    InvalidIndex(usize),
    /// Huffman decoding failed: input ending mid-code, trailing bits that
    /// are not all-ones (EOS-prefix) padding, or the all-ones EOS path
    /// resolving to no symbol (h3 `HuffmanDecodingError`).
    HuffmanDecoding(huffman::HuffmanDecodingError),
    /// Huffman encoding failed. h3's encoder never constructs this; the
    /// variant exists for parity with h3's `HuffmanEncodingError`.
    HuffmanEncoding(huffman::HuffmanEncodingError),
}

impl From<UnexpectedEnd> for QpackError {
    fn from(_: UnexpectedEnd) -> Self {
        QpackError::UnexpectedEnd
    }
}

impl From<huffman::HuffmanDecodingError> for QpackError {
    fn from(error: huffman::HuffmanDecodingError) -> Self {
        QpackError::HuffmanDecoding(error)
    }
}

impl From<huffman::HuffmanEncodingError> for QpackError {
    fn from(error: huffman::HuffmanEncodingError) -> Self {
        QpackError::HuffmanEncoding(error)
    }
}

impl std::fmt::Display for QpackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QpackError::InvalidSize => write!(f, "prefix size must be between 1 and 8 bits"),
            QpackError::Overflow => write!(f, "prefix integer overflow"),
            QpackError::UnexpectedEnd => write!(f, "unexpected end of prefix integer"),
            QpackError::NonZeroInsertCount(count) => {
                write!(f, "required insert count {count} requires a dynamic table")
            }
            QpackError::NonZeroDeltaBase { sign, delta_base } => {
                write!(
                    f,
                    "delta base sign {sign} value {delta_base} requires a dynamic table"
                )
            }
            QpackError::InvalidIndex(index) => {
                write!(f, "static table index {index} out of range")
            }
            QpackError::HuffmanDecoding(e) => write!(f, "huffman decode failed: {:?}", e),
            QpackError::HuffmanEncoding(e) => write!(f, "huffman encode failed: {:?}", e),
        }
    }
}
