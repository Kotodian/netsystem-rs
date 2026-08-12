//! VPP HTTP FIFO ABI: checked byte-level encode/decode of one app request.
//!
//! Wire layout for one published request (inline data path,
//! `third_party/vpp/src/plugins/hs_apps/http_client.c:hc_request`,
//! lines 229-250):
//!
//! ```text
//! [HttpMsg: 88 bytes][target_path][header list][body]
//! ```
//!
//! Offsets follow the app writer `hc_msg_set_offsets`
//! (`http_client.c:164-170`): path at 0, headers after the path, body after
//! the headers; `data.len` is the sum of the three lengths. Header-list
//! entries follow `http_add_header2` / `http_add_custom_header2`
//! (`third_party/vpp/src/plugins/http/http.h:1103-1161`):
//!
//! ```text
//! known : [flags:u16][name:u16][value_len:u32][value]          (8 + n)
//! custom: [flags:u16][name_len:u16][name][value_len:u32][value] (8 + m + n)
//! ```
//!
//! Semantics: decode returns offset/length descriptors borrowing the input
//! buffer (adjacent-FIFO style, no copied payload ownership); no movement
//! between FIFOs happens in this slice.
//!
//! Hammer differences (documented, bytes identical to VPP on x86_64):
//! - byte order is explicit little-endian; VPP memcpys native-endian structs.
//! - header entries are tight: `http_app_header_name_t`'s name union is
//!   exactly `u16` and the `token[0]` flexible arrays add no bytes, so there
//!   is no padding between `name`/`name_len` and `value_len`.
//! - the upper 3 bytes of the method/code union word and `headers_ctx` are
//!   written as zero; VPP leaves them undefined.
//!
//! The codec allocates nothing, takes no locks, and panics on no input:
//! every read is bounds-checked and every arithmetic step is checked.

#[cfg(test)]
mod tests;
mod types;

pub use types::*;

/// Size of the fixed `http_msg_t` header on the wire.
pub const MSG_HEADER_LEN: usize = 88;

/// A single header to encode: known name by `http_header_name_t` index, or a
/// custom literal name. `flags` may carry `NEVER_INDEX` / `HOP_BY_HOP`;
/// `CUSTOM_NAME` is implied for `Custom` and rejected on `Known`.
#[derive(Debug, Clone, Copy)]
pub enum AppHeader<'a> {
    Known {
        flags: FieldLineFlags,
        name: u16,
        value: &'a [u8],
    },
    Custom {
        flags: FieldLineFlags,
        name: &'a [u8],
        value: &'a [u8],
    },
}

/// One request to publish. All payloads are borrowed slices; the encoder
/// writes the complete VPP byte layout into a caller-provided buffer.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub method: ReqMethod,
    pub scheme: UrlScheme,
    pub target_path: &'a [u8],
    pub headers: &'a [AppHeader<'a>],
    pub body: &'a [u8],
}

/// A decoded header: name and value are spans into the decoded buffer.
#[derive(Debug, Clone, Copy)]
pub struct DecodedHeader<'a> {
    pub flags: FieldLineFlags,
    pub name: DecodedHeaderName<'a>,
    pub value: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodedHeaderName<'a> {
    Known(u16),
    Custom(&'a [u8]),
}

/// A decoded request. All payloads are spans into the decoded buffer; the
/// header list is walked through [`DecodedRequest::headers`] without
/// allocation.
#[derive(Debug, Clone, Copy)]
pub struct DecodedRequest<'a> {
    pub method: ReqMethod,
    pub scheme: UrlScheme,
    pub upgrade_proto: UpgradeProto,
    pub target_authority: &'a [u8],
    pub target_path: &'a [u8],
    pub target_query: &'a [u8],
    pub body: &'a [u8],
    header_region: &'a [u8],
}

impl DecodedRequest<'_> {
    /// Iterate the decoded header descriptors. Infallible: `decode` already
    /// validated that entries tile the header region exactly; this re-walk is
    /// one linear pass and allocates nothing.
    pub fn headers(&self) -> HeaderIter<'_> {
        HeaderIter {
            region: self.header_region,
            pos: 0,
        }
    }
}

/// Infallible iterator over decoded header descriptors (see
/// [`DecodedRequest::headers`]).
pub struct HeaderIter<'a> {
    region: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = DecodedHeader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.region.len() {
            return None;
        }
        let flags = u16_le(self.region, self.pos)?;
        if flags & FieldLineFlags::CUSTOM_NAME != 0 {
            let name_len = u16_le(self.region, self.pos + 2)? as usize;
            let name = self.region.get(self.pos + 4..self.pos + 4 + name_len)?;
            let value_len = u32_le(self.region, self.pos + 4 + name_len)? as usize;
            let value = self
                .region
                .get(self.pos + 8 + name_len..self.pos + 8 + name_len + value_len)?;
            self.pos += 8 + name_len + value_len;
            Some(DecodedHeader {
                flags: FieldLineFlags(flags),
                name: DecodedHeaderName::Custom(name),
                value,
            })
        } else {
            let name = u16_le(self.region, self.pos + 2)?;
            let value_len = u32_le(self.region, self.pos + 4)? as usize;
            let value_start = self.pos + 8;
            let value = self.region.get(value_start..value_start + value_len)?;
            self.pos = value_start + value_len;
            Some(DecodedHeader {
                flags: FieldLineFlags(flags),
                name: DecodedHeaderName::Known(name),
                value,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    /// Caller buffer is smaller than the encoded request.
    BufferTooSmall { needed: usize, available: usize },
    /// Offsets or lengths overflow their wire widths (`u32`/`u64`/`u16`).
    LengthOverflow,
    /// `CUSTOM_NAME` bit set by the caller; it is implied for `Custom` and
    /// forbidden on `Known`.
    ReservedFlag,
}

impl core::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooSmall { needed, available } => {
                write!(f, "buffer too small: need {needed} bytes, have {available}")
            }
            Self::LengthOverflow => write!(f, "offset or length overflows its wire width"),
            Self::ReservedFlag => {
                write!(f, "caller set the CUSTOM_NAME flag on a header entry")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// Buffer shorter than the fixed header or the declared data length.
    Truncated,
    /// Buffer longer than one complete request.
    TrailingData,
    /// Offset/length arithmetic overflows its wire width.
    LengthOverflow,
    /// Discriminant is not a valid `http_msg_type_t` (only `Request` is
    /// publishable in this seam).
    InvalidMsgType { value: u32 },
    /// Discriminant is not a valid `http_req_method_t`.
    InvalidMethod { value: u8 },
    /// Data type is not `Inline` (`Ptr`/`Streaming` are out of this seam).
    InvalidDataType { value: u32 },
    /// Discriminant is not a valid `http_url_scheme_t`.
    InvalidScheme { value: u8 },
    /// Discriminant is not a valid `http_upgrade_proto_t`.
    InvalidUpgradeProto { value: u8 },
    /// Offsets contradict the app publish layout (path at 0, headers after
    /// the path, body after the headers, `len` equal to the total).
    LayoutMismatch,
    /// A declared span (authority/query) lies outside the data area.
    InvalidDataSpan,
    /// A header entry extends past the end of the declared header region
    /// (also raised when entries end early: the leftover stub is then shorter
    /// than the 8-byte minimum entry prefix).
    HeaderListOverrun,
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated ABI input"),
            Self::TrailingData => write!(f, "input longer than one request"),
            Self::LengthOverflow => write!(f, "offset or length arithmetic overflow"),
            Self::InvalidMsgType { value } => {
                write!(f, "invalid msg type discriminant {value}")
            }
            Self::InvalidMethod { value } => {
                write!(f, "invalid request method discriminant {value}")
            }
            Self::InvalidDataType { value } => {
                write!(f, "invalid msg data type discriminant {value}")
            }
            Self::InvalidScheme { value } => write!(f, "invalid URL scheme discriminant {value}"),
            Self::InvalidUpgradeProto { value } => {
                write!(f, "invalid upgrade protocol discriminant {value}")
            }
            Self::LayoutMismatch => write!(f, "offsets contradict the app publish layout"),
            Self::InvalidDataSpan => write!(f, "declared span outside the data area"),
            Self::HeaderListOverrun => write!(f, "header entry past the header region"),
        }
    }
}

impl<'a> Request<'a> {
    /// Total encoded size: 88-byte header + path + header list + body, with
    /// checked arithmetic and width validation. This is the only place
    /// `encode` can fail; `encode` then writes the full request or nothing.
    pub fn encoded_len(&self) -> Result<usize, EncodeError> {
        let path_len = self.target_path.len() as u64;
        let body_len = self.body.len() as u64;
        let mut headers_len: u64 = 0;
        for header in self.headers {
            let entry = match header {
                AppHeader::Known {
                    flags,
                    name: _,
                    value,
                } => {
                    if flags.bits() & FieldLineFlags::CUSTOM_NAME != 0 {
                        return Err(EncodeError::ReservedFlag);
                    }
                    8u64.checked_add(value.len() as u64)
                        .ok_or(EncodeError::LengthOverflow)?
                }
                AppHeader::Custom { flags, name, value } => {
                    if flags.bits() & FieldLineFlags::CUSTOM_NAME != 0 {
                        return Err(EncodeError::ReservedFlag);
                    }
                    if name.len() > u16::MAX as usize {
                        return Err(EncodeError::LengthOverflow);
                    }
                    8u64.checked_add(name.len() as u64)
                        .and_then(|n| n.checked_add(value.len() as u64))
                        .ok_or(EncodeError::LengthOverflow)?
                }
            };
            headers_len = headers_len
                .checked_add(entry)
                .ok_or(EncodeError::LengthOverflow)?;
        }
        if path_len > u32::MAX as u64 || headers_len > u32::MAX as u64 {
            return Err(EncodeError::LengthOverflow);
        }
        let data_len = path_len
            .checked_add(headers_len)
            .and_then(|n| n.checked_add(body_len))
            .ok_or(EncodeError::LengthOverflow)?;
        MSG_HEADER_LEN
            .checked_add(data_len as usize)
            .ok_or(EncodeError::LengthOverflow)
    }

    /// Encode the request into `buf`, returning the number of bytes written
    /// (equal to [`Self::encoded_len`]). Writes nothing on error.
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
        let total = self.encoded_len()?;
        if buf.len() < total {
            return Err(EncodeError::BufferTooSmall {
                needed: total,
                available: buf.len(),
            });
        }
        let path_len = self.target_path.len() as u32;
        let body_len = self.body.len() as u64;
        // Derived from the validated total instead of re-walking the headers.
        let headers_len =
            (total - MSG_HEADER_LEN - self.target_path.len() - self.body.len()) as u64;
        let data_len = (path_len as u64) + headers_len + body_len;
        let headers_offset = path_len;
        // Validated in `encoded_len`; checked here for the no-panic contract.
        let body_offset = headers_offset
            .checked_add(headers_len as u32)
            .ok_or(EncodeError::LengthOverflow)?;
        let mut w = 0usize;
        w = put_u32(buf, w, MsgType::Request as u32);
        w = put_u32(buf, w, self.method as u32);
        w = put_u32(buf, w, MsgDataType::Inline as u32);
        w += 4; // `http_msg_data_t` padding: type @0, len @8
        w = put_u64(buf, w, data_len);
        w = put_u8(buf, w, self.scheme as u8);
        w += 3; // `http_msg_data_t` padding: scheme @16, authority @20
        w = put_u32(buf, w, 0); // target_authority_offset
        w = put_u32(buf, w, 0); // target_authority_len
        w = put_u32(buf, w, 0); // target_path_offset: app layout puts path at 0
        w = put_u32(buf, w, path_len);
        w = put_u32(buf, w, 0); // target_query_offset
        w = put_u32(buf, w, 0); // target_query_len
        w = put_u32(buf, w, headers_offset);
        w = put_u32(buf, w, headers_len as u32);
        w = put_u32(buf, w, body_offset);
        w = put_u64(buf, w, body_len);
        w = put_u64(buf, w, 0); // headers_ctx: server-side scratch, zero on publish
        w = put_u8(buf, w, UpgradeProto::Na as u8);
        w += 7; // tail padding of `http_msg_data_t` up to 80 bytes
        buf[w..w + self.target_path.len()].copy_from_slice(self.target_path);
        w += self.target_path.len();
        for header in self.headers {
            w = encode_header(*header, buf, w);
        }
        buf[w..w + self.body.len()].copy_from_slice(self.body);
        w += self.body.len();
        Ok(w)
    }
}

/// Encode one header-list entry at `w`; returns the new write position.
/// Preconditions: caller flags carry no `CUSTOM_NAME` bit and custom names
/// fit `u16` (validated by `encoded_len`).
fn encode_header(header: AppHeader<'_>, buf: &mut [u8], w: usize) -> usize {
    match header {
        AppHeader::Known { flags, name, value } => {
            let mut w = put_u16(buf, w, flags.bits());
            w = put_u16(buf, w, name);
            w = put_u32(buf, w, value.len() as u32);
            put_bytes(buf, w, value)
        }
        AppHeader::Custom { flags, name, value } => {
            let mut w = put_u16(buf, w, flags.bits() | FieldLineFlags::CUSTOM_NAME);
            w = put_u16(buf, w, name.len() as u16);
            w = put_bytes(buf, w, name);
            w = put_u32(buf, w, value.len() as u32);
            put_bytes(buf, w, value)
        }
    }
}

/// Checked decode of one complete request from `buf`. Requires `buf` to be
/// exactly `88 + data.len` bytes: shorter input is [`DecodeError::Truncated`],
/// longer [`DecodeError::TrailingData`]. Returns spans borrowing `buf`.
pub fn decode(buf: &[u8]) -> Result<DecodedRequest<'_>, DecodeError> {
    if buf.len() < MSG_HEADER_LEN {
        return Err(DecodeError::Truncated);
    }
    let msg_type = u32_le(buf, 0).ok_or(DecodeError::Truncated)?;
    if msg_type != MsgType::Request as u32 {
        return Err(DecodeError::InvalidMsgType { value: msg_type });
    }
    let method = ReqMethod::try_from(u8_le(buf, 4).ok_or(DecodeError::Truncated)?)
        .map_err(|value| DecodeError::InvalidMethod { value })?;
    let data_type = u32_le(buf, 8).ok_or(DecodeError::Truncated)?;
    if data_type != MsgDataType::Inline as u32 {
        return Err(DecodeError::InvalidDataType { value: data_type });
    }
    let scheme = UrlScheme::try_from(u8_le(buf, 24).ok_or(DecodeError::Truncated)?)
        .map_err(|value| DecodeError::InvalidScheme { value })?;
    let upgrade_proto = UpgradeProto::try_from(u8_le(buf, 80).ok_or(DecodeError::Truncated)?)
        .map_err(|value| DecodeError::InvalidUpgradeProto { value })?;

    let data_len = u64_le(buf, 16).ok_or(DecodeError::Truncated)?;
    let target_authority_offset = u32_le(buf, 28).ok_or(DecodeError::Truncated)?;
    let target_authority_len = u32_le(buf, 32).ok_or(DecodeError::Truncated)?;
    let target_path_offset = u32_le(buf, 36).ok_or(DecodeError::Truncated)?;
    let target_path_len = u32_le(buf, 40).ok_or(DecodeError::Truncated)?;
    let target_query_offset = u32_le(buf, 44).ok_or(DecodeError::Truncated)?;
    let target_query_len = u32_le(buf, 48).ok_or(DecodeError::Truncated)?;
    let headers_offset = u32_le(buf, 52).ok_or(DecodeError::Truncated)?;
    let headers_len = u32_le(buf, 56).ok_or(DecodeError::Truncated)?;
    let body_offset = u32_le(buf, 60).ok_or(DecodeError::Truncated)?;
    let body_len = u64_le(buf, 64).ok_or(DecodeError::Truncated)?;

    // App publish layout (hc_msg_set_offsets): path at 0, headers after the
    // path, body after the headers, data.len equal to the sum.
    if target_path_offset != 0 {
        return Err(DecodeError::LayoutMismatch);
    }
    if headers_offset != target_path_len {
        return Err(DecodeError::LayoutMismatch);
    }
    let headers_end = (headers_offset as u64) + (headers_len as u64);
    if headers_end > u32::MAX as u64 {
        // The header region end must be representable as a u32 body offset.
        return Err(DecodeError::LengthOverflow);
    }
    if body_offset as u64 != headers_end {
        return Err(DecodeError::LayoutMismatch);
    }
    let body_end = headers_end
        .checked_add(body_len)
        .ok_or(DecodeError::LengthOverflow)?;
    if data_len != body_end {
        return Err(DecodeError::LayoutMismatch);
    }
    // Authority/query spans (CONNECT-UDP payloads) must stay inside the data
    // area; both are empty for the plain publish path.
    for (off, len) in [
        (target_authority_offset as u64, target_authority_len as u64),
        (target_query_offset as u64, target_query_len as u64),
    ] {
        if off.checked_add(len).is_none_or(|end| end > data_len) {
            return Err(DecodeError::InvalidDataSpan);
        }
    }

    let expected = MSG_HEADER_LEN
        .checked_add(data_len as usize)
        .ok_or(DecodeError::LengthOverflow)?;
    if buf.len() < expected {
        return Err(DecodeError::Truncated);
    }
    if buf.len() > expected {
        return Err(DecodeError::TrailingData);
    }

    // All spans were validated above; `get_span` keeps every slice take
    // bounds-checked and typed rather than panicking.
    let path =
        get_span(buf, MSG_HEADER_LEN, target_path_len as usize).ok_or(DecodeError::Truncated)?;
    let header_region = get_span(
        buf,
        MSG_HEADER_LEN + target_path_len as usize,
        headers_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    let body = get_span(
        buf,
        MSG_HEADER_LEN + headers_offset as usize + headers_len as usize,
        body_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    validate_header_region(header_region)?;

    Ok(DecodedRequest {
        method,
        scheme,
        upgrade_proto,
        target_authority: get_span(
            buf,
            MSG_HEADER_LEN + target_authority_offset as usize,
            target_authority_len as usize,
        )
        .ok_or(DecodeError::Truncated)?,
        target_path: path,
        target_query: get_span(
            buf,
            MSG_HEADER_LEN + target_query_offset as usize,
            target_query_len as usize,
        )
        .ok_or(DecodeError::Truncated)?,
        body,
        header_region,
    })
}

/// Validate that header entries tile `region` exactly: each entry must fit
/// entirely inside the region, and the walk must end at its boundary.
fn validate_header_region(region: &[u8]) -> Result<(), DecodeError> {
    let mut pos = 0usize;
    while pos < region.len() {
        let remaining = region.len() - pos;
        if remaining < 8 {
            return Err(DecodeError::HeaderListOverrun);
        }
        let flags = u16_le(region, pos).ok_or(DecodeError::HeaderListOverrun)?;
        let entry_len = if flags & FieldLineFlags::CUSTOM_NAME != 0 {
            let name_len = u16_le(region, pos + 2).ok_or(DecodeError::HeaderListOverrun)? as usize;
            let value_len =
                u32_le(region, pos + 4 + name_len).ok_or(DecodeError::HeaderListOverrun)? as usize;
            8usize
                .checked_add(name_len)
                .and_then(|n| n.checked_add(value_len))
                .ok_or(DecodeError::LengthOverflow)?
        } else {
            let value_len = u32_le(region, pos + 4).ok_or(DecodeError::HeaderListOverrun)? as usize;
            8usize
                .checked_add(value_len)
                .ok_or(DecodeError::LengthOverflow)?
        };
        if entry_len > remaining {
            return Err(DecodeError::HeaderListOverrun);
        }
        pos += entry_len;
    }
    Ok(())
}

// --- checked little-endian readers/writers ---------------------------------

fn u8_le(buf: &[u8], off: usize) -> Option<u8> {
    buf.get(off).copied()
}

fn u16_le(buf: &[u8], off: usize) -> Option<u16> {
    Some(u16::from_le_bytes(buf.get(off..off + 2)?.try_into().ok()?))
}

fn u32_le(buf: &[u8], off: usize) -> Option<u32> {
    Some(u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?))
}

fn u64_le(buf: &[u8], off: usize) -> Option<u64> {
    Some(u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?))
}

fn get_span(buf: &[u8], off: usize, len: usize) -> Option<&[u8]> {
    let end = off.checked_add(len)?;
    buf.get(off..end)
}

fn put_u8(buf: &mut [u8], w: usize, v: u8) -> usize {
    buf[w] = v;
    w + 1
}

fn put_u16(buf: &mut [u8], w: usize, v: u16) -> usize {
    buf[w..w + 2].copy_from_slice(&v.to_le_bytes());
    w + 2
}

fn put_u32(buf: &mut [u8], w: usize, v: u32) -> usize {
    buf[w..w + 4].copy_from_slice(&v.to_le_bytes());
    w + 4
}

fn put_u64(buf: &mut [u8], w: usize, v: u64) -> usize {
    buf[w..w + 8].copy_from_slice(&v.to_le_bytes());
    w + 8
}

fn put_bytes(buf: &mut [u8], w: usize, v: &[u8]) -> usize {
    buf[w..w + v.len()].copy_from_slice(v);
    w + v.len()
}
