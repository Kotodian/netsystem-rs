//! The QPACK static table (RFC 9204 Appendix A): the fixed 99 entries that
//! never change, referenced by index from encoded field lines.
//!
//! Decode-side only: `get` resolves an index in O(1) to a `'static`-borrowed
//! `HeaderField`. The encoder-side reverse lookups are not part of this
//! slice (`find`/`find_name` in `third_party/h3/h3/src/qpack/static_.rs`,
//! the per-name perfect hash in
//! `third_party/vpp/src/plugins/http/http3/qpack.c`).
//!
//! Entry 16 is `:method` `DELETE` per RFC 9204 and h3; VPP's `qpack.c`
//! carries a typo there (`:metho`), so that single entry intentionally
//! deviates from the VPP data.

use std::borrow::Cow;

use super::QpackError;
use super::field::HeaderField;

/// The RFC 9204 Appendix A table has exactly 99 entries. The value is
/// derived from the array so a bad manual count cannot drift; the tests pin
/// it to 99 (VPP asserts the same count with `STATIC_ASSERT`).
pub(crate) const STATIC_TABLE_SIZE: usize = STATIC_TABLE.len();

/// Look up a static-table entry by its RFC 9204 Appendix A index, in O(1).
///
/// The entry is `'static`-borrowed: decoding an `Indexed` field line never
/// allocates. An index beyond the 99 fixed entries is a typed error, since
/// the RFC table has no dynamic extension and such an index can only come
/// from a malformed block.
pub(crate) fn get(index: usize) -> Result<&'static HeaderField, QpackError> {
    STATIC_TABLE
        .get(index)
        .ok_or(QpackError::InvalidIndex(index))
}

/// The RFC 9204 Appendix A table, in index order. All entries borrow
/// `'static` bytes.
pub(crate) static STATIC_TABLE: [HeaderField; 99] = [
    HeaderField {
        name: Cow::Borrowed(b":authority"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b":path"),
        value: Cow::Borrowed(b"/"),
    },
    HeaderField {
        name: Cow::Borrowed(b"age"),
        value: Cow::Borrowed(b"0"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-disposition"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-length"),
        value: Cow::Borrowed(b"0"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cookie"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"date"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"etag"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-modified-since"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-none-match"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"last-modified"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"link"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"location"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"referer"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"set-cookie"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"CONNECT"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"DELETE"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"GET"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"HEAD"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"OPTIONS"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"POST"),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"PUT"),
    },
    HeaderField {
        name: Cow::Borrowed(b":scheme"),
        value: Cow::Borrowed(b"http"),
    },
    HeaderField {
        name: Cow::Borrowed(b":scheme"),
        value: Cow::Borrowed(b"https"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"103"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"200"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"304"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"404"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"503"),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept"),
        value: Cow::Borrowed(b"*/*"),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept"),
        value: Cow::Borrowed(b"application/dns-message"),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-encoding"),
        value: Cow::Borrowed(b"gzip, deflate, br"),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-ranges"),
        value: Cow::Borrowed(b"bytes"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"cache-control"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"content-type"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-origin"),
        value: Cow::Borrowed(b"*"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=0"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=2592000"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=604800"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"no-cache"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"no-store"),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"public, max-age=31536000"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-encoding"),
        value: Cow::Borrowed(b"br"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-encoding"),
        value: Cow::Borrowed(b"gzip"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/dns-message"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/javascript"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/json"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/x-www-form-urlencoded"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/gif"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/jpeg"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/png"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/css"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/html; charset=utf-8"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/plain"),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/plain;charset=utf-8"),
    },
    HeaderField {
        name: Cow::Borrowed(b"range"),
        value: Cow::Borrowed(b"bytes=0-"),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000"),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000; includesubdomains"),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000; includesubdomains; preload"),
    },
    HeaderField {
        name: Cow::Borrowed(b"vary"),
        value: Cow::Borrowed(b"accept-encoding"),
    },
    HeaderField {
        name: Cow::Borrowed(b"vary"),
        value: Cow::Borrowed(b"origin"),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-content-type-options"),
        value: Cow::Borrowed(b"nosniff"),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-xss-protection"),
        value: Cow::Borrowed(b"1; mode=block"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"100"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"204"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"206"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"302"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"400"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"403"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"421"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"425"),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"500"),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-language"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-credentials"),
        value: Cow::Borrowed(b"FALSE"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-credentials"),
        value: Cow::Borrowed(b"TRUE"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"*"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"get"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"get, post, options"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"options"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-expose-headers"),
        value: Cow::Borrowed(b"content-length"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-headers"),
        value: Cow::Borrowed(b"content-type"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-method"),
        value: Cow::Borrowed(b"get"),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-method"),
        value: Cow::Borrowed(b"post"),
    },
    HeaderField {
        name: Cow::Borrowed(b"alt-svc"),
        value: Cow::Borrowed(b"clear"),
    },
    HeaderField {
        name: Cow::Borrowed(b"authorization"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-security-policy"),
        value: Cow::Borrowed(b"script-src 'none'; object-src 'none'; base-uri 'none'"),
    },
    HeaderField {
        name: Cow::Borrowed(b"early-data"),
        value: Cow::Borrowed(b"1"),
    },
    HeaderField {
        name: Cow::Borrowed(b"expect-ct"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"forwarded"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-range"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"origin"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"purpose"),
        value: Cow::Borrowed(b"prefetch"),
    },
    HeaderField {
        name: Cow::Borrowed(b"server"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"timing-allow-origin"),
        value: Cow::Borrowed(b"*"),
    },
    HeaderField {
        name: Cow::Borrowed(b"upgrade-insecure-requests"),
        value: Cow::Borrowed(b"1"),
    },
    HeaderField {
        name: Cow::Borrowed(b"user-agent"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-forwarded-for"),
        value: Cow::Borrowed(b""),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-frame-options"),
        value: Cow::Borrowed(b"deny"),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-frame-options"),
        value: Cow::Borrowed(b"sameorigin"),
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 9204 Appendix A pins the table at 99 entries; VPP asserts the
    /// same count (`STATIC_ASSERT (QPACK_STATIC_TABLE_SIZE == 99)`).
    #[test]
    fn table_has_exactly_99_entries() {
        assert_eq!(STATIC_TABLE.len(), 99);
        assert_eq!(STATIC_TABLE_SIZE, 99);
    }

    /// Exact name and value bytes at index 0, at the method, status, and
    /// content-type block boundaries, and at the last index 98.
    #[test]
    fn representative_entries_match_rfc_9204_appendix_a() {
        let cases = [
            (0, b":authority" as &[u8], b"" as &[u8]),
            (1, b":path", b"/"),
            (15, b":method", b"CONNECT"),
            (21, b":method", b"PUT"),
            (24, b":status", b"103"),
            (28, b":status", b"503"),
            (44, b"content-type", b"application/dns-message"),
            (54, b"content-type", b"text/plain;charset=utf-8"),
            (63, b":status", b"100"),
            (71, b":status", b"500"),
            (98, b"x-frame-options", b"sameorigin"),
        ];
        for (index, name, value) in cases {
            let entry = get(index).expect("index in range");
            assert_eq!(entry.name.as_ref(), name, "name at index {index}");
            assert_eq!(entry.value.as_ref(), value, "value at index {index}");
        }
    }

    /// Index 99 and beyond reference no entry: the static table is fixed,
    /// so such an index can only come from a malformed block.
    #[test]
    fn out_of_bounds_index_is_a_typed_error() {
        assert_eq!(get(99), Err(QpackError::InvalidIndex(99)));
        assert_eq!(get(100), Err(QpackError::InvalidIndex(100)));
        assert_eq!(get(usize::MAX), Err(QpackError::InvalidIndex(usize::MAX)));
    }
}
