//! The QPACK static table (RFC 9204 Appendix A): the fixed 99 entries that
//! never change, referenced by index from encoded field lines.
//!
//! `get` resolves an index in O(1) to a `'static`-borrowed `HeaderField` for
//! decoding. `find` and `find_name` are the encoder-side reverse lookups: an
//! explicit fixed match over the entry bytes with bounded O(1) dispatch (no
//! hash table, no heap, no generator), selecting the indexed representation
//! for an exact (name, value) match and the literal-with-name-reference
//! representation for a name match. Ported from `find`/`find_name` in
//! `third_party/h3/h3/src/qpack/static_.rs`; VPP's per-name perfect hash in
//! `third_party/vpp/src/plugins/http/http3/qpack.c` serves the same purpose
//! with generated code, which this slice avoids.
//!
//! Entry 16 is `:method` `DELETE` per RFC 9204 and h3; VPP's `qpack.c`
//! carries a typo there (`:metho`), so that single entry intentionally
//! deviates from the VPP data.

use std::borrow::Cow;

use super::QpackError;
use super::field::HeaderField;
use crate::http_common::FieldLineFlags;

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

/// Reverse-lookup a full (name, value) match, in bounded O(1): an explicit
/// fixed match over the entry bytes, not a scan of the 99-entry table.
///
/// Ported from h3's `StaticTable::find` (`third_party/h3/h3/src/qpack/
/// static_.rs`), which the QPACK encoders use for the indexed representation
/// (RFC 9204 Section 4.5.2); VPP reaches the same index through its
/// generated per-name perfect hash. The arms follow the table in index
/// order, so a name with several value entries resolves to the first full
/// match, exactly as h3 does.
pub(crate) fn find(field: &HeaderField) -> Option<usize> {
    match (&field.name[..], &field.value[..]) {
        (b":authority", b"") => Some(0),
        (b":path", b"/") => Some(1),
        (b"age", b"0") => Some(2),
        (b"content-disposition", b"") => Some(3),
        (b"content-length", b"0") => Some(4),
        (b"cookie", b"") => Some(5),
        (b"date", b"") => Some(6),
        (b"etag", b"") => Some(7),
        (b"if-modified-since", b"") => Some(8),
        (b"if-none-match", b"") => Some(9),
        (b"last-modified", b"") => Some(10),
        (b"link", b"") => Some(11),
        (b"location", b"") => Some(12),
        (b"referer", b"") => Some(13),
        (b"set-cookie", b"") => Some(14),
        (b":method", b"CONNECT") => Some(15),
        (b":method", b"DELETE") => Some(16),
        (b":method", b"GET") => Some(17),
        (b":method", b"HEAD") => Some(18),
        (b":method", b"OPTIONS") => Some(19),
        (b":method", b"POST") => Some(20),
        (b":method", b"PUT") => Some(21),
        (b":scheme", b"http") => Some(22),
        (b":scheme", b"https") => Some(23),
        (b":status", b"103") => Some(24),
        (b":status", b"200") => Some(25),
        (b":status", b"304") => Some(26),
        (b":status", b"404") => Some(27),
        (b":status", b"503") => Some(28),
        (b"accept", b"*/*") => Some(29),
        (b"accept", b"application/dns-message") => Some(30),
        (b"accept-encoding", b"gzip, deflate, br") => Some(31),
        (b"accept-ranges", b"bytes") => Some(32),
        (b"access-control-allow-headers", b"cache-control") => Some(33),
        (b"access-control-allow-headers", b"content-type") => Some(34),
        (b"access-control-allow-origin", b"*") => Some(35),
        (b"cache-control", b"max-age=0") => Some(36),
        (b"cache-control", b"max-age=2592000") => Some(37),
        (b"cache-control", b"max-age=604800") => Some(38),
        (b"cache-control", b"no-cache") => Some(39),
        (b"cache-control", b"no-store") => Some(40),
        (b"cache-control", b"public, max-age=31536000") => Some(41),
        (b"content-encoding", b"br") => Some(42),
        (b"content-encoding", b"gzip") => Some(43),
        (b"content-type", b"application/dns-message") => Some(44),
        (b"content-type", b"application/javascript") => Some(45),
        (b"content-type", b"application/json") => Some(46),
        (b"content-type", b"application/x-www-form-urlencoded") => Some(47),
        (b"content-type", b"image/gif") => Some(48),
        (b"content-type", b"image/jpeg") => Some(49),
        (b"content-type", b"image/png") => Some(50),
        (b"content-type", b"text/css") => Some(51),
        (b"content-type", b"text/html; charset=utf-8") => Some(52),
        (b"content-type", b"text/plain") => Some(53),
        (b"content-type", b"text/plain;charset=utf-8") => Some(54),
        (b"range", b"bytes=0-") => Some(55),
        (b"strict-transport-security", b"max-age=31536000") => Some(56),
        (b"strict-transport-security", b"max-age=31536000; includesubdomains") => Some(57),
        (b"strict-transport-security", b"max-age=31536000; includesubdomains; preload") => Some(58),
        (b"vary", b"accept-encoding") => Some(59),
        (b"vary", b"origin") => Some(60),
        (b"x-content-type-options", b"nosniff") => Some(61),
        (b"x-xss-protection", b"1; mode=block") => Some(62),
        (b":status", b"100") => Some(63),
        (b":status", b"204") => Some(64),
        (b":status", b"206") => Some(65),
        (b":status", b"302") => Some(66),
        (b":status", b"400") => Some(67),
        (b":status", b"403") => Some(68),
        (b":status", b"421") => Some(69),
        (b":status", b"425") => Some(70),
        (b":status", b"500") => Some(71),
        (b"accept-language", b"") => Some(72),
        (b"access-control-allow-credentials", b"FALSE") => Some(73),
        (b"access-control-allow-credentials", b"TRUE") => Some(74),
        (b"access-control-allow-headers", b"*") => Some(75),
        (b"access-control-allow-methods", b"get") => Some(76),
        (b"access-control-allow-methods", b"get, post, options") => Some(77),
        (b"access-control-allow-methods", b"options") => Some(78),
        (b"access-control-expose-headers", b"content-length") => Some(79),
        (b"access-control-request-headers", b"content-type") => Some(80),
        (b"access-control-request-method", b"get") => Some(81),
        (b"access-control-request-method", b"post") => Some(82),
        (b"alt-svc", b"clear") => Some(83),
        (b"authorization", b"") => Some(84),
        (b"content-security-policy", b"script-src 'none'; object-src 'none'; base-uri 'none'") => {
            Some(85)
        }
        (b"early-data", b"1") => Some(86),
        (b"expect-ct", b"") => Some(87),
        (b"forwarded", b"") => Some(88),
        (b"if-range", b"") => Some(89),
        (b"origin", b"") => Some(90),
        (b"purpose", b"prefetch") => Some(91),
        (b"server", b"") => Some(92),
        (b"timing-allow-origin", b"*") => Some(93),
        (b"upgrade-insecure-requests", b"1") => Some(94),
        (b"user-agent", b"") => Some(95),
        (b"x-forwarded-for", b"") => Some(96),
        (b"x-frame-options", b"deny") => Some(97),
        (b"x-frame-options", b"sameorigin") => Some(98),
        _ => None,
    }
}

/// Reverse-lookup a static name, in bounded O(1): an explicit fixed match
/// over the names in the table, returning the first entry with that name.
///
/// Ported from h3's `StaticTable::find_name` (`third_party/h3/h3/src/qpack/
/// static_.rs`), which the QPACK encoders use for the literal-with-name-
/// reference representation (RFC 9204 Section 4.5.4); VPP's generated
/// `qpack_static_table_lookup` serves the same purpose. The arm order
/// follows the table, so a name with several value entries resolves to the
/// first entry with that name, exactly as h3 does; the encoder needs only
/// the name of that entry.
pub(crate) fn find_name(name: &[u8]) -> Option<usize> {
    match name {
        b":authority" => Some(0),
        b":path" => Some(1),
        b"age" => Some(2),
        b"content-disposition" => Some(3),
        b"content-length" => Some(4),
        b"cookie" => Some(5),
        b"date" => Some(6),
        b"etag" => Some(7),
        b"if-modified-since" => Some(8),
        b"if-none-match" => Some(9),
        b"last-modified" => Some(10),
        b"link" => Some(11),
        b"location" => Some(12),
        b"referer" => Some(13),
        b"set-cookie" => Some(14),
        b":method" => Some(15),
        b":scheme" => Some(22),
        b":status" => Some(24),
        b"accept" => Some(29),
        b"accept-encoding" => Some(31),
        b"accept-ranges" => Some(32),
        b"access-control-allow-headers" => Some(33),
        b"access-control-allow-origin" => Some(35),
        b"cache-control" => Some(36),
        b"content-encoding" => Some(42),
        b"content-type" => Some(44),
        b"range" => Some(55),
        b"strict-transport-security" => Some(56),
        b"vary" => Some(59),
        b"x-content-type-options" => Some(61),
        b"x-xss-protection" => Some(62),
        b"accept-language" => Some(72),
        b"access-control-allow-credentials" => Some(73),
        b"access-control-allow-methods" => Some(76),
        b"access-control-expose-headers" => Some(79),
        b"access-control-request-headers" => Some(80),
        b"access-control-request-method" => Some(81),
        b"alt-svc" => Some(83),
        b"authorization" => Some(84),
        b"content-security-policy" => Some(85),
        b"early-data" => Some(86),
        b"expect-ct" => Some(87),
        b"forwarded" => Some(88),
        b"if-range" => Some(89),
        b"origin" => Some(90),
        b"purpose" => Some(91),
        b"server" => Some(92),
        b"timing-allow-origin" => Some(93),
        b"upgrade-insecure-requests" => Some(94),
        b"user-agent" => Some(95),
        b"x-forwarded-for" => Some(96),
        b"x-frame-options" => Some(97),
        _ => None,
    }
}

/// The RFC 9204 Appendix A table, in index order. All entries borrow
/// `'static` bytes.
pub(crate) static STATIC_TABLE: [HeaderField; 99] = [
    HeaderField {
        name: Cow::Borrowed(b":authority"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":path"),
        value: Cow::Borrowed(b"/"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"age"),
        value: Cow::Borrowed(b"0"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-disposition"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-length"),
        value: Cow::Borrowed(b"0"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cookie"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"date"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"etag"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-modified-since"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-none-match"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"last-modified"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"link"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"location"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"referer"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"set-cookie"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"CONNECT"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"DELETE"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"GET"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"HEAD"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"OPTIONS"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"POST"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":method"),
        value: Cow::Borrowed(b"PUT"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":scheme"),
        value: Cow::Borrowed(b"http"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":scheme"),
        value: Cow::Borrowed(b"https"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"103"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"200"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"304"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"404"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"503"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept"),
        value: Cow::Borrowed(b"*/*"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept"),
        value: Cow::Borrowed(b"application/dns-message"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-encoding"),
        value: Cow::Borrowed(b"gzip, deflate, br"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-ranges"),
        value: Cow::Borrowed(b"bytes"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"cache-control"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"content-type"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-origin"),
        value: Cow::Borrowed(b"*"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=0"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=2592000"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"max-age=604800"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"no-cache"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"no-store"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"cache-control"),
        value: Cow::Borrowed(b"public, max-age=31536000"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-encoding"),
        value: Cow::Borrowed(b"br"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-encoding"),
        value: Cow::Borrowed(b"gzip"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/dns-message"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/javascript"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/json"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"application/x-www-form-urlencoded"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/gif"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/jpeg"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"image/png"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/css"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/html; charset=utf-8"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/plain"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-type"),
        value: Cow::Borrowed(b"text/plain;charset=utf-8"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"range"),
        value: Cow::Borrowed(b"bytes=0-"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000; includesubdomains"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"strict-transport-security"),
        value: Cow::Borrowed(b"max-age=31536000; includesubdomains; preload"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"vary"),
        value: Cow::Borrowed(b"accept-encoding"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"vary"),
        value: Cow::Borrowed(b"origin"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-content-type-options"),
        value: Cow::Borrowed(b"nosniff"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-xss-protection"),
        value: Cow::Borrowed(b"1; mode=block"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"100"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"204"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"206"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"302"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"400"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"403"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"421"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"425"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b":status"),
        value: Cow::Borrowed(b"500"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"accept-language"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-credentials"),
        value: Cow::Borrowed(b"FALSE"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-credentials"),
        value: Cow::Borrowed(b"TRUE"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-headers"),
        value: Cow::Borrowed(b"*"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"get"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"get, post, options"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-allow-methods"),
        value: Cow::Borrowed(b"options"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-expose-headers"),
        value: Cow::Borrowed(b"content-length"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-headers"),
        value: Cow::Borrowed(b"content-type"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-method"),
        value: Cow::Borrowed(b"get"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"access-control-request-method"),
        value: Cow::Borrowed(b"post"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"alt-svc"),
        value: Cow::Borrowed(b"clear"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"authorization"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"content-security-policy"),
        value: Cow::Borrowed(b"script-src 'none'; object-src 'none'; base-uri 'none'"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"early-data"),
        value: Cow::Borrowed(b"1"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"expect-ct"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"forwarded"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"if-range"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"origin"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"purpose"),
        value: Cow::Borrowed(b"prefetch"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"server"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"timing-allow-origin"),
        value: Cow::Borrowed(b"*"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"upgrade-insecure-requests"),
        value: Cow::Borrowed(b"1"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"user-agent"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-forwarded-for"),
        value: Cow::Borrowed(b""),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-frame-options"),
        value: Cow::Borrowed(b"deny"),
        flags: FieldLineFlags::empty(),
    },
    HeaderField {
        name: Cow::Borrowed(b"x-frame-options"),
        value: Cow::Borrowed(b"sameorigin"),
        flags: FieldLineFlags::empty(),
    },
];
