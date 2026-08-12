//! QPACK (RFC 9204) primitives: prefix integers and field lines.
//!
//! This slice wires only the prefix integer codec (`prefix_int`) and the
//! field line type (`field`). The static table, prefix strings, block
//! parsing, encoder and decoder are later slices and are not declared here.
//!
//! References:
//! - RFC 9204 Section 4.1.1 (prefix integers), RFC 7541 Section 4.1 (entry
//!   size accounting)
//! - `third_party/h3/h3/src/qpack/{mod,prefix_int,field}.rs`
//! - `third_party/vpp/src/plugins/http/http2/hpack_inlines.h` (integer codec)
//! - `third_party/vpp/src/plugins/http/http3/qpack.c`

use super::varint::UnexpectedEnd;

pub(crate) mod field;
pub(crate) mod prefix_int;

/// Errors produced by the QPACK wire primitives wired in this slice.
///
/// The variant set is exactly what the prefix primitives can produce; later
/// QPACK slices extend this only when their seams prove new variants. Unlike
/// h3 (which `assert!`s the prefix size) and VPP (which `ASSERT`s it), an
/// invalid prefix size is a typed error here.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum QpackError {
    /// A prefix size outside 1..=8 bits, which has no wire representation.
    InvalidSize,
    /// The value grew beyond the 63-bit mantissa of RFC 9204 Section 4.1.1.
    /// VPP reports the same condition as `HPACK_INVALID_INT`.
    Overflow,
    /// The input ended before the integer was complete.
    /// VPP reports the same condition as `HPACK_INVALID_INT`.
    UnexpectedEnd,
}

impl From<UnexpectedEnd> for QpackError {
    fn from(_: UnexpectedEnd) -> Self {
        QpackError::UnexpectedEnd
    }
}

impl std::fmt::Display for QpackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QpackError::InvalidSize => write!(f, "prefix size must be between 1 and 8 bits"),
            QpackError::Overflow => write!(f, "prefix integer overflow"),
            QpackError::UnexpectedEnd => write!(f, "unexpected end of prefix integer"),
        }
    }
}
