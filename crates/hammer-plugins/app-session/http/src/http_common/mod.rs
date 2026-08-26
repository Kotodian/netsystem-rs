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
//! the headers; `data.len` is the sum of the three lengths. The server side
//! tiles the same data area as `[authority][path][query][header list][body]`
//! with one offset/len pair per span; `decode` accepts both tilings through
//! the checked chain in [`decode`]. Header-list entries follow
//! `http_add_header2` / `http_add_custom_header2`
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
//! [`publish_request`] writes the same layout to a FIFO in one
//! reserve/commit: every length is validated first, then metadata and
//! payloads are scattered from a fixed stack buffer, and the commit is the
//! only visibility point (single producer, no locks, no event flag).
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

mod body;
#[cfg(test)]
mod tests;
mod types;

pub(crate) use body::{BodyAccumulator, BodyError};
pub use types::*;

use hammer_infra::fifo::{Fifo, FifoError, FifoWriteReservation};

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
        // The raw u16 is admitted with `from_bits_retain` so unknown/future
        // VPP bits round-trip instead of being rejected or truncated.
        let flags = FieldLineFlags::from_bits_retain(u16_le(self.region, self.pos)?);
        if flags.contains(FieldLineFlags::CUSTOM_NAME) {
            let name_len = u16_le(self.region, self.pos + 2)? as usize;
            let name = self.region.get(self.pos + 4..self.pos + 4 + name_len)?;
            let value_len = u32_le(self.region, self.pos + 4 + name_len)? as usize;
            let value = self
                .region
                .get(self.pos + 8 + name_len..self.pos + 8 + name_len + value_len)?;
            self.pos += 8 + name_len + value_len;
            Some(DecodedHeader {
                flags,
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
                flags,
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
    /// Offsets contradict the tiled data-area layout (authority, path,
    /// query, header list, body in order, `len` equal to the body end).
    LayoutMismatch,
    /// A declared request-target span (authority/path/query) lies outside
    /// the data area.
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
            Self::LayoutMismatch => write!(f, "offsets contradict the tiled request layout"),
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
        let headers_len = header_entries_len(self.headers)?;
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
        let layout = WireLayout::derive(self, total)?;
        let mut w = self.encode_msg_header(&layout, buf);
        buf[w..w + self.target_path.len()].copy_from_slice(self.target_path);
        w += self.target_path.len();
        for header in self.headers {
            w = encode_header(*header, buf, w);
        }
        buf[w..w + self.body.len()].copy_from_slice(self.body);
        w += self.body.len();
        Ok(w)
    }

    /// Encode the fixed 88-byte `http_msg_t` header into `buf` (which must
    /// hold `MSG_HEADER_LEN` bytes), returning `MSG_HEADER_LEN`. Offsets
    /// follow the app writer `hc_msg_set_offsets`
    /// (`third_party/vpp/src/plugins/hs_apps/http_client.c:163-166`): path
    /// at 0, headers after the path, body after the headers.
    fn encode_msg_header(&self, layout: &WireLayout, buf: &mut [u8]) -> usize {
        let mut w = 0usize;
        w = put_u32(buf, w, MsgType::Request as u32);
        w = put_u32(buf, w, self.method as u32);
        w = put_u32(buf, w, MsgDataType::Inline as u32);
        w += 4; // `http_msg_data_t` padding: type @0, len @8
        w = put_u64(buf, w, layout.data_len);
        w = put_u8(buf, w, self.scheme as u8);
        w += 3; // `http_msg_data_t` padding: scheme @16, authority @20
        w = put_u32(buf, w, 0); // target_authority_offset
        w = put_u32(buf, w, 0); // target_authority_len
        w = put_u32(buf, w, 0); // target_path_offset: app layout puts path at 0
        w = put_u32(buf, w, layout.path_len);
        w = put_u32(buf, w, 0); // target_query_offset
        w = put_u32(buf, w, 0); // target_query_len
        w = put_u32(buf, w, layout.path_len); // headers_offset
        w = put_u32(buf, w, layout.headers_len);
        w = put_u32(buf, w, layout.body_offset);
        w = put_u64(buf, w, layout.body_len);
        w = put_u64(buf, w, 0); // headers_ctx: server-side scratch, zero on publish
        w = put_u8(buf, w, UpgradeProto::Na as u8);
        w += 7; // tail padding of `http_msg_data_t` up to 80 bytes
        w
    }
}

/// Validated wire lengths and offsets shared by the contiguous and FIFO
/// writers, derived once from an `encoded_len`-validated `total` so both
/// agree on every offset without re-walking the header list.
#[derive(Debug, Clone, Copy)]
struct WireLayout {
    data_len: u64,
    path_len: u32,
    headers_len: u32,
    body_len: u64,
    body_offset: u32,
}

impl WireLayout {
    /// `total` must come from `Request::encoded_len` for the same request;
    /// re-checked here so the no-panic contract does not rely on that
    /// precondition.
    fn derive(req: &Request<'_>, total: usize) -> Result<Self, EncodeError> {
        let path_len =
            u32::try_from(req.target_path.len()).map_err(|_| EncodeError::LengthOverflow)?;
        let headers_len = total
            .checked_sub(MSG_HEADER_LEN)
            .and_then(|n| n.checked_sub(req.target_path.len()))
            .and_then(|n| n.checked_sub(req.body.len()))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(EncodeError::LengthOverflow)?;
        let body_len = req.body.len() as u64;
        let data_len = (path_len as u64)
            .checked_add(headers_len as u64)
            .and_then(|n| n.checked_add(body_len))
            .ok_or(EncodeError::LengthOverflow)?;
        let body_offset = path_len
            .checked_add(headers_len)
            .ok_or(EncodeError::LengthOverflow)?;
        Ok(Self {
            data_len,
            path_len,
            headers_len,
            body_len,
            body_offset,
        })
    }
}

/// Why a FIFO publish failed. `Encode` and `Capacity` leave the FIFO
/// unchanged; `Fifo` covers reservation/copy/commit failures, which roll
/// back so nothing becomes visible.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // exercised from tests; wired to the app publisher later
pub(crate) enum PublishError {
    /// The request is not encodable (wire-width or flag violations).
    Encode(EncodeError),
    /// The FIFO cannot hold the whole request. `want_deq_notification` was
    /// armed so the producer is signalled when space frees up.
    Capacity { requested: usize, available: usize },
    /// Reservation, scatter copy, or commit failed on the FIFO.
    Fifo(FifoError),
}

impl core::fmt::Display for PublishError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(inner) => write!(f, "request does not encode: {inner}"),
            Self::Capacity {
                requested,
                available,
            } => {
                write!(f, "FIFO holds {available} bytes, request needs {requested}")
            }
            Self::Fifo(inner) => write!(f, "FIFO publish failed: {inner}"),
        }
    }
}

/// Publish one request to `fifo` in the VPP app-writer layout, all-or-nothing.
///
/// Wire order matches `hc_request`
/// (`third_party/vpp/src/plugins/hs_apps/http_client.c:229-250`): the
/// 88-byte `http_msg_t`, then the target path, then the header list, then
/// the body. Synchronization mirrors that path: the caller is the single
/// producer, `reserve_write` re-checks the FIFO head with an acquire load,
/// and `commit` is the only visibility point (a release tail publication).
/// No FIFO event flag is raised here.
///
/// Every length is computed and validated before anything is reserved. If
/// the FIFO cannot hold the whole request, the capacity preflight arms
/// `want_deq_notification` and returns with the FIFO untouched; otherwise
/// one `reserve_write(total)` is followed by a scatter copy (metadata from
/// a fixed stack buffer, path, header entries, body) and a single commit.
/// Any copy or commit failure cancels the reservation, exposing zero bytes.
#[allow(dead_code)] // tests exercise it; the app-session publisher wires it in a later seam
pub(crate) fn publish_request(fifo: &Fifo, req: &Request<'_>) -> Result<(), PublishError> {
    let total = req.encoded_len().map_err(PublishError::Encode)?;
    let layout = WireLayout::derive(req, total).map_err(PublishError::Encode)?;
    let available = fifo.max_enqueue();
    if available < total {
        fifo.want_deq_notification();
        return Err(PublishError::Capacity {
            requested: total,
            available,
        });
    }
    let mut reservation = fifo.reserve_write(total).map_err(PublishError::Fifo)?;
    let mut meta = [0u8; MSG_HEADER_LEN];
    req.encode_msg_header(&layout, &mut meta);
    scatter(&mut reservation, [&meta[..]]).map_err(PublishError::Fifo)?;
    scatter(&mut reservation, [req.target_path]).map_err(PublishError::Fifo)?;
    scatter_headers(&mut reservation, req.headers).map_err(PublishError::Fifo)?;
    let copied = scatter(&mut reservation, [req.body]).map_err(PublishError::Fifo)?;
    if copied != total {
        reservation.cancel();
        return Err(PublishError::Fifo(FifoError::CommitExceedsReservation {
            initialized: copied,
            reserved: total,
        }));
    }
    match reservation.commit(copied) {
        Ok(_) => Ok(()),
        Err(source) => {
            reservation.cancel();
            Err(PublishError::Fifo(source))
        }
    }
}

/// Scatter-copy one group of source slices into `reservation`, returning the
/// cumulative byte count; on failure the reservation is cancelled so no
/// bytes become visible.
#[allow(dead_code)] // used only by the FIFO publish paths, wired in a later seam
fn scatter<I, S>(reservation: &mut FifoWriteReservation<'_>, segs: I) -> Result<usize, FifoError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<[u8]>,
{
    let copied = reservation.copy_from_segments(segs);
    if copied.is_err() {
        reservation.cancel();
    }
    copied
}

/// Scatter-copy the header-list entries into `reservation`, each as its
/// 8-byte prefix plus the value (name/value split for custom names), all
/// prefixes reusing one stack buffer; returns the cumulative byte count. On
/// failure the reservation is cancelled so no bytes become visible.
#[allow(dead_code)] // used only by the FIFO publish paths, wired in a later seam
fn scatter_headers(
    reservation: &mut FifoWriteReservation<'_>,
    headers: &[AppHeader<'_>],
) -> Result<usize, FifoError> {
    let mut prefix = [0u8; 8];
    let mut copied = 0usize;
    for header in headers {
        copied = match *header {
            AppHeader::Known { flags, name, value } => {
                let mut w = put_u16(&mut prefix, 0, flags.bits());
                w = put_u16(&mut prefix, w, name);
                put_u32(&mut prefix, w, value.len() as u32);
                scatter(reservation, [&prefix[..], value])?
            }
            AppHeader::Custom { flags, name, value } => {
                let mut w = put_u16(&mut prefix, 0, (flags | FieldLineFlags::CUSTOM_NAME).bits());
                w = put_u16(&mut prefix, w, name.len() as u16);
                put_u32(&mut prefix, w, value.len() as u32);
                scatter(reservation, [&prefix[..4], name, &prefix[4..], value])?
            }
        };
    }
    Ok(copied)
}

// --- Inbound publish: server/transport -> app --------------------------------

/// One inbound request to publish: a request received from the transport and
/// delivered to the app. All payloads are borrowed slices; the encoder writes
/// the complete VPP byte layout into a FIFO in one reserve/commit.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // tests exercise it; the app-session publisher wires it in a later seam
pub(crate) struct InboundRequest<'a> {
    pub(crate) method: ReqMethod,
    pub(crate) scheme: UrlScheme,
    /// Request-target pseudo fields; each is a span with its own
    /// offset/len pair in the fixed header, never a header-list entry.
    pub(crate) target_authority: &'a [u8],
    pub(crate) target_path: &'a [u8],
    pub(crate) target_query: &'a [u8],
    pub(crate) headers: &'a [AppHeader<'a>],
    pub(crate) body: &'a [u8],
}

impl<'a> InboundRequest<'a> {
    /// Total encoded size: 88-byte header + authority + path + query +
    /// header list + body, with checked arithmetic and width validation.
    /// Mirrors [`Request::encoded_len`]; the server layout prepends the
    /// authority and query pseudo fields to the app layout's path region.
    #[allow(dead_code)] // used only by `publish_inbound_request`
    fn encoded_len(&self) -> Result<usize, EncodeError> {
        let authority_len = self.target_authority.len() as u64;
        let path_len = self.target_path.len() as u64;
        let query_len = self.target_query.len() as u64;
        let body_len = self.body.len() as u64;
        let headers_len = header_entries_len(self.headers)?;
        if authority_len > u32::MAX as u64
            || path_len > u32::MAX as u64
            || query_len > u32::MAX as u64
            || headers_len > u32::MAX as u64
        {
            return Err(EncodeError::LengthOverflow);
        }
        let data_len = authority_len
            .checked_add(path_len)
            .and_then(|n| n.checked_add(query_len))
            .and_then(|n| n.checked_add(headers_len))
            .and_then(|n| n.checked_add(body_len))
            .ok_or(EncodeError::LengthOverflow)?;
        MSG_HEADER_LEN
            .checked_add(data_len as usize)
            .ok_or(EncodeError::LengthOverflow)
    }

    /// Encode the fixed 88-byte `http_msg_t` header into `buf` (which must
    /// hold `MSG_HEADER_LEN` bytes), returning `MSG_HEADER_LEN`. Pseudo-field
    /// spans tile the data area in order: authority at 0, path, query,
    /// headers, body. `data.len` includes the body (the `decode` invariant).
    #[allow(dead_code)] // used only by `publish_inbound_request`
    fn encode_msg_header(&self, layout: &InboundLayout, buf: &mut [u8]) -> usize {
        let mut w = 0usize;
        w = put_u32(buf, w, MsgType::Request as u32);
        w = put_u32(buf, w, self.method as u32);
        w = put_u32(buf, w, MsgDataType::Inline as u32);
        w += 4; // `http_msg_data_t` padding: type @8, len @16
        w = put_u64(buf, w, layout.data_len);
        w = put_u8(buf, w, self.scheme as u8);
        w += 3; // `http_msg_data_t` padding: scheme @16, authority @20
        w = put_u32(buf, w, 0); // target_authority_offset: authority is first
        w = put_u32(buf, w, layout.authority_len);
        w = put_u32(buf, w, layout.authority_len); // target_path_offset
        w = put_u32(buf, w, layout.path_len);
        w = put_u32(buf, w, layout.query_offset);
        w = put_u32(buf, w, layout.query_len);
        w = put_u32(buf, w, layout.headers_offset);
        w = put_u32(buf, w, layout.headers_len);
        w = put_u32(buf, w, layout.body_offset);
        w = put_u64(buf, w, layout.body_len);
        w = put_u64(buf, w, 0); // headers_ctx: server-side scratch, zero on publish
        w = put_u8(buf, w, UpgradeProto::Na as u8);
        w += 7; // tail padding of `http_msg_data_t` up to 80 bytes
        w
    }
}

/// Validated wire lengths and offsets for the inbound (server -> app) layout,
/// derived once from an `encoded_len`-validated `total` so the header encode
/// and the scatter copy agree on every offset without re-walking the header
/// list. The data area tiles `[authority][path][query][headers][body]`; the
/// authority is the first span (offset 0), unlike the app writer's path-first
/// layout.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // used only by `publish_inbound_request`
struct InboundLayout {
    data_len: u64,
    authority_len: u32,
    path_len: u32,
    query_len: u32,
    query_offset: u32,
    headers_len: u32,
    headers_offset: u32,
    body_len: u64,
    body_offset: u32,
}

impl InboundLayout {
    /// `total` must come from `InboundRequest::encoded_len` for the same
    /// request; re-checked here so the no-panic contract does not rely on
    /// that precondition.
    #[allow(dead_code)] // used only by `publish_inbound_request`
    fn derive(req: &InboundRequest<'_>, total: usize) -> Result<Self, EncodeError> {
        let authority_len =
            u32::try_from(req.target_authority.len()).map_err(|_| EncodeError::LengthOverflow)?;
        let path_len =
            u32::try_from(req.target_path.len()).map_err(|_| EncodeError::LengthOverflow)?;
        let query_len =
            u32::try_from(req.target_query.len()).map_err(|_| EncodeError::LengthOverflow)?;
        let body_len = req.body.len() as u64;
        let headers_len = total
            .checked_sub(MSG_HEADER_LEN)
            .and_then(|n| n.checked_sub(req.target_authority.len()))
            .and_then(|n| n.checked_sub(req.target_path.len()))
            .and_then(|n| n.checked_sub(req.target_query.len()))
            .and_then(|n| n.checked_sub(req.body.len()))
            .and_then(|n| u32::try_from(n).ok())
            .ok_or(EncodeError::LengthOverflow)?;
        let query_offset = authority_len
            .checked_add(path_len)
            .ok_or(EncodeError::LengthOverflow)?;
        let headers_offset = query_offset
            .checked_add(query_len)
            .ok_or(EncodeError::LengthOverflow)?;
        let body_offset = headers_offset
            .checked_add(headers_len)
            .ok_or(EncodeError::LengthOverflow)?;
        let data_len = (body_offset as u64)
            .checked_add(body_len)
            .ok_or(EncodeError::LengthOverflow)?;
        Ok(Self {
            data_len,
            authority_len,
            path_len,
            query_len,
            query_offset,
            headers_len,
            headers_offset,
            body_len,
            body_offset,
        })
    }
}

/// Publish one server/transport -> app request to `fifo` in the VPP server
/// `http_msg_t` layout, all-or-nothing.
///
/// Wire order: the 88-byte `http_msg_t`, then the request-target pseudo
/// fields — authority, path, query — each tracked by its own offset/len pair
/// in the fixed header (28/32, 36/40, 44/48) and never encoded as
/// header-list entries, then the regular header list (52/56), then the body
/// (60/64):
///
/// ```text
/// [HttpMsg: 88 bytes][authority][path][query][header list][body]
/// ```
///
/// `headers_ctx` (72) is written zero; `data.len` (u64 @16) is the sum of all
/// five payload lengths including the body, exactly the invariant `decode`
/// enforces.
///
/// Synchronization mirrors [`publish_request`]: the caller is the single
/// producer, a capacity preflight (`max_enqueue`) arms
/// `want_deq_notification` and returns with the FIFO untouched on failure,
/// then one `reserve_write(total)` is followed by a scatter copy (the fixed
/// 88-byte header from a stack buffer, then each borrowed span) and a single
/// commit as the only visibility point. No FIFO event flag is raised here.
///
/// Hammer differences (documented): VPP servers may stream large bodies out
/// of the data area (`HTTP_DATA_STREAMING`); this seam publishes one complete
/// request with the body inline and `data.len` including it, so `decode`'s
/// inline-only contract holds. That is a bounded Hammer choice for this
/// issue, not exact HTTP/3 streaming parity: a streamed body remains a later
/// seam. `decode` accepts the server layout — authority first, then path,
/// query, header list, body — via the same checked tiling as the app layout.
#[allow(dead_code)] // tests exercise it; the app-session publisher wires it in a later seam
pub(crate) fn publish_inbound_request(
    fifo: &Fifo,
    req: &InboundRequest<'_>,
) -> Result<(), PublishError> {
    let total = req.encoded_len().map_err(PublishError::Encode)?;
    let layout = InboundLayout::derive(req, total).map_err(PublishError::Encode)?;
    let available = fifo.max_enqueue();
    if available < total {
        fifo.want_deq_notification();
        return Err(PublishError::Capacity {
            requested: total,
            available,
        });
    }
    let mut reservation = fifo.reserve_write(total).map_err(PublishError::Fifo)?;
    let mut meta = [0u8; MSG_HEADER_LEN];
    req.encode_msg_header(&layout, &mut meta);
    scatter(&mut reservation, [&meta[..]]).map_err(PublishError::Fifo)?;
    scatter(&mut reservation, [req.target_authority]).map_err(PublishError::Fifo)?;
    scatter(&mut reservation, [req.target_path]).map_err(PublishError::Fifo)?;
    scatter(&mut reservation, [req.target_query]).map_err(PublishError::Fifo)?;
    scatter_headers(&mut reservation, req.headers).map_err(PublishError::Fifo)?;
    let copied = scatter(&mut reservation, [req.body]).map_err(PublishError::Fifo)?;
    if copied != total {
        reservation.cancel();
        return Err(PublishError::Fifo(FifoError::CommitExceedsReservation {
            initialized: copied,
            reserved: total,
        }));
    }
    match reservation.commit(copied) {
        Ok(_) => Ok(()),
        Err(source) => {
            reservation.cancel();
            Err(PublishError::Fifo(source))
        }
    }
}

/// Publish one body chunk to `fifo` all-or-nothing.
///
/// Mirrors the bounded app write of `http3_req_state_transport_io_more_data`
/// (`third_party/vpp/src/plugins/http/http3/http3.c:1184-1263`): the whole
/// chunk is capacity-checked first, and zero or short capacity arms the
/// dequeue notification and returns with the FIFO untouched, without any
/// reservation or mutation; otherwise one `reserve_write` is followed by a
/// single scatter copy of the borrowed chunk directly into the reservation
/// segments (no temporary buffer, no separate sizing pass) and one commit as
/// the only visibility point. A reservation is cancelled only on a pre-commit
/// failure; committed bytes are never dequeued as rollback.
///
/// Hammer difference (documented): the VPP state machine copies at most
/// `min(max_deq, max_enq)` bytes per call and carries the remainder in the
/// transport stream; this seam publishes one whole chunk and reports
/// `Capacity` for it instead, leaving incremental delivery to the caller. As
/// in `publish_inbound_request`, no FIFO event flag is raised here: RX
/// notification stays with the caller, and this seam does not speculate on
/// moving it.
#[allow(dead_code)] // tests exercise it; the app-session publisher wires it in a later seam
pub(crate) fn publish_body_chunk(fifo: &Fifo, chunk: &[u8]) -> Result<(), PublishError> {
    let total = chunk.len();
    let available = fifo.max_enqueue();
    if available < total {
        fifo.want_deq_notification();
        return Err(PublishError::Capacity {
            requested: total,
            available,
        });
    }
    let mut reservation = fifo.reserve_write(total).map_err(PublishError::Fifo)?;
    let copied = scatter(&mut reservation, [chunk]).map_err(PublishError::Fifo)?;
    if copied != total {
        reservation.cancel();
        return Err(PublishError::Fifo(FifoError::CommitExceedsReservation {
            initialized: copied,
            reserved: total,
        }));
    }
    match reservation.commit(copied) {
        Ok(_) => Ok(()),
        Err(source) => {
            reservation.cancel();
            Err(PublishError::Fifo(source))
        }
    }
}

/// Total wire size of a header list: each entry's 8-byte prefix plus its
/// value (and custom name), with the caller-set `CUSTOM_NAME` flag rejected
/// and custom names width-checked against `u16`.
fn header_entries_len(headers: &[AppHeader<'_>]) -> Result<u64, EncodeError> {
    let mut headers_len: u64 = 0;
    for header in headers {
        let entry = match header {
            AppHeader::Known {
                flags,
                name: _,
                value,
            } => {
                if flags.contains(FieldLineFlags::CUSTOM_NAME) {
                    return Err(EncodeError::ReservedFlag);
                }
                8u64.checked_add(value.len() as u64)
                    .ok_or(EncodeError::LengthOverflow)?
            }
            AppHeader::Custom { flags, name, value } => {
                if flags.contains(FieldLineFlags::CUSTOM_NAME) {
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
    Ok(headers_len)
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
            let mut w = put_u16(buf, w, (flags | FieldLineFlags::CUSTOM_NAME).bits());
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
///
/// The data-area spans must tile in order — authority, path, query (a
/// zero-length query may sit anywhere, as the app writer leaves its offset
/// at 0), header list, body — each inside the data area, with `data.len`
/// equal to the body end. Both the app-writer layout (path at 0) and the
/// server layout (authority first) decode.
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

    // Span ends, checked against the u32 offset width: an end beyond u32::MAX
    // could not be represented as the next span's offset.
    let authority_end = target_authority_offset
        .checked_add(target_authority_len)
        .ok_or(DecodeError::LengthOverflow)?;
    let path_end = target_path_offset
        .checked_add(target_path_len)
        .ok_or(DecodeError::LengthOverflow)?;
    let query_end = target_query_offset
        .checked_add(target_query_len)
        .ok_or(DecodeError::LengthOverflow)?;
    let headers_end = headers_offset
        .checked_add(headers_len)
        .ok_or(DecodeError::LengthOverflow)?;
    let body_end = (body_offset as u64)
        .checked_add(body_len)
        .ok_or(DecodeError::LengthOverflow)?;

    // The request-target pseudo-field spans (authority, path, query) must
    // stay inside the data area; the headers and body spans are then pinned
    // inside it by the tiling chain below (`headers_end == body_offset` and
    // `data.len == body_end`).
    for end in [authority_end as u64, path_end as u64, query_end as u64] {
        if end > data_len {
            return Err(DecodeError::InvalidDataSpan);
        }
    }

    // Offsets tile the data area in order — [authority][path][query][header
    // list][body] — with `data.len` equal to the body end. This accepts both
    // the app-writer layout (path at 0, empty authority/query) and the server
    // layout with the authority first. A zero-length query may sit anywhere,
    // as the app writer leaves its offset at 0.
    if target_path_offset != authority_end {
        return Err(DecodeError::LayoutMismatch);
    }
    if target_query_len != 0 && target_query_offset != path_end {
        return Err(DecodeError::LayoutMismatch);
    }
    if headers_offset != path_end.max(query_end) {
        return Err(DecodeError::LayoutMismatch);
    }
    if body_offset != headers_end {
        return Err(DecodeError::LayoutMismatch);
    }
    if data_len != body_end {
        return Err(DecodeError::LayoutMismatch);
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
    let authority = get_span(
        buf,
        MSG_HEADER_LEN + target_authority_offset as usize,
        target_authority_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    let path = get_span(
        buf,
        MSG_HEADER_LEN + target_path_offset as usize,
        target_path_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    let query = get_span(
        buf,
        MSG_HEADER_LEN + target_query_offset as usize,
        target_query_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    let header_region = get_span(
        buf,
        MSG_HEADER_LEN + headers_offset as usize,
        headers_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    let body = get_span(
        buf,
        MSG_HEADER_LEN + body_offset as usize,
        body_len as usize,
    )
    .ok_or(DecodeError::Truncated)?;
    validate_header_region(header_region)?;

    Ok(DecodedRequest {
        method,
        scheme,
        upgrade_proto,
        target_authority: authority,
        target_path: path,
        target_query: query,
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
        let flags = FieldLineFlags::from_bits_retain(flags);
        let entry_len = if flags.contains(FieldLineFlags::CUSTOM_NAME) {
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
