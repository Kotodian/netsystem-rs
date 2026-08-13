//! A decoded field line (RFC 9204 Section 1.2): name and value bytes, with
//! the RFC 7541 Section 4.1 table-size accounting.
//!
//! Ported from `third_party/h3/h3/src/qpack/field.rs`. `Cow<'static, [u8]>`
//! keeps static-table entries allocation-free while decoded literals own
//! their bytes.

use std::borrow::Cow;
use std::fmt;

use crate::http_common::FieldLineFlags;

/// The per-entry overhead of the dynamic table size calculation
/// (RFC 7541 Section 4.1): the size of an entry is the sum of its name and
/// value lengths plus 32.
pub const ESTIMATED_OVERHEAD_BYTES: usize = 32;

/// A field line: name and value as byte strings. Static-table entries borrow
/// `'static` data; decoded literals own their bytes. `flags` carries the
/// field-line policy bits of the ABI (`FieldLineFlags`): the decoder sets
/// `NEVER_INDEX` when the wire N bit (RFC 9204 Sections 4.5.4 and 4.5.6) is
/// set — the same merge VPP performs at `http.h:1114` — and the encoder
/// emits the N bit back when the flag is present. Static entries and
/// ordinary `(name, value)` construction default to empty flags.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct HeaderField {
    pub name: Cow<'static, [u8]>,
    pub value: Cow<'static, [u8]>,
    pub flags: FieldLineFlags,
}

/// `FieldLineFlags` does not derive `Hash`; `HeaderField` does, so the flag
/// bits hash as the underlying `u16`. The impl lives beside `HeaderField`
/// because it exists solely to keep that derive sound.
impl std::hash::Hash for FieldLineFlags {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bits().hash(state);
    }
}

impl HeaderField {
    pub fn new<T, S>(name: T, value: S) -> HeaderField
    where
        T: Into<Vec<u8>>,
        S: Into<Vec<u8>>,
    {
        HeaderField {
            name: Cow::Owned(name.into()),
            value: Cow::Owned(value.into()),
            flags: FieldLineFlags::empty(),
        }
    }

    /// The size of this field in the QPACK table accounting: name length plus
    /// value length plus 32, without Huffman encoding applied (RFC 9204
    /// Section 4.1.1.3).
    pub fn mem_size(&self) -> usize {
        self.name.len() + self.value.len() + ESTIMATED_OVERHEAD_BYTES
    }

    pub fn with_value<T>(&self, value: T) -> Self
    where
        T: Into<Vec<u8>>,
    {
        Self {
            name: self.name.clone(),
            value: Cow::Owned(value.into()),
            flags: self.flags,
        }
    }
}

/// Build a field from a pair of byte strings, copying them into owned bytes
/// regardless of the inputs' lifetimes.
impl<N, V> From<(N, V)> for HeaderField
where
    N: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    fn from(header: (N, V)) -> Self {
        let (name, value) = header;
        Self {
            name: Cow::Owned(Vec::from(name.as_ref())),
            value: Cow::Owned(Vec::from(value.as_ref())),
            flags: FieldLineFlags::empty(),
        }
    }
}

impl fmt::Display for HeaderField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\"{}\": \"{}\"",
            String::from_utf8_lossy(&self.name),
            String::from_utf8_lossy(&self.value)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7541 Section 4.1: the size of an entry is name + value + 32.
    #[test]
    fn test_field_size_is_offset_by_32() {
        let field = HeaderField {
            name: Cow::Borrowed(b"Name"),
            value: Cow::Borrowed(b"Value"),
            flags: FieldLineFlags::empty(),
        };
        assert_eq!(field.mem_size(), 4 + 5 + 32);
    }

    #[test]
    fn with_value() {
        let field = HeaderField {
            name: Cow::Borrowed(b"Name"),
            value: Cow::Borrowed(b"Value"),
            flags: FieldLineFlags::empty(),
        };
        assert_eq!(
            field.with_value("New value"),
            HeaderField {
                name: Cow::Borrowed(b"Name"),
                value: Cow::Borrowed(b"New value"),
                flags: FieldLineFlags::empty(),
            }
        );
        // Flags are indexing policy, independent of the value: replacing the
        // value keeps `NEVER_INDEX` (VPP merges the same bit into the name
        // token, http.h:1114).
        let never = HeaderField {
            name: Cow::Borrowed(b"Name"),
            value: Cow::Borrowed(b"Value"),
            flags: FieldLineFlags::NEVER_INDEX,
        };
        assert_eq!(
            never.with_value("New value"),
            HeaderField {
                name: Cow::Borrowed(b"Name"),
                value: Cow::Borrowed(b"New value"),
                flags: FieldLineFlags::NEVER_INDEX,
            }
        );
    }

    /// `From<(N, V)>` mirrors the vendored h3 conversion: name and value are
    /// copied into owned bytes regardless of the inputs' lifetimes, and the
    /// RFC 7541 Section 4.1 accounting applies to the copied lengths.
    #[test]
    fn from_tuple_owns_bytes() {
        let field = HeaderField::from((":method", "GET"));
        assert_eq!(field.mem_size(), 7 + 3 + 32);
        assert!(matches!(&field.name, Cow::Owned(_)));
        assert!(matches!(&field.value, Cow::Owned(_)));

        let mixed = HeaderField::from((b"x".to_vec(), &b"y"[..]));
        assert_eq!(mixed.mem_size(), 1 + 1 + 32);
    }

    /// Ordinary construction defaults to empty flags: a field built by the
    /// app must not carry `NEVER_INDEX` unless a decoded wire N bit or an
    /// explicit flag assignment put it there.
    #[test]
    fn constructors_default_flags_empty() {
        assert_eq!(
            HeaderField::new(":method", "GET").flags,
            FieldLineFlags::empty()
        );
        assert_eq!(
            HeaderField::from(("x-custom", "1")).flags,
            FieldLineFlags::empty()
        );
    }
}
