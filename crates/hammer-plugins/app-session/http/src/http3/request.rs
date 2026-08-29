//! Client request-stream semantics on the server side: frame ordering
//! (RFC 9114 Section 4.1) and request field-section validation (Sections
//! 4.3 and 4.3.1).
//!
//! ## Frame ordering
//!
//! A request stream must open with a HEADERS frame, then carries DATA frames
//! and at most one trailing HEADERS frame, then ends. Any other sequence is
//! a connection error of type H3_FRAME_UNEXPECTED; control-stream-only
//! frames (SETTINGS, CANCEL_PUSH, GOAWAY, MAX_PUSH_ID) and PUSH_PROMISE are
//! forbidden on a client request stream. Unknown frame types are extension
//! points and are tolerated without changing state, mirroring VPP's drain of
//! unknown frames.
//!
//! References:
//! - RFC 9114 Sections 4.1, 7.2.3-7.2.7
//! - `third_party/vpp/src/plugins/http/http3/http3.c`
//!   (`http3_stream_transport_rx_req`, lines 1732-1799: HEADERS only in
//!   `WAIT_TRANSPORT_METHOD`, DATA only in transport-IO states, else
//!   `HTTP3_ERROR_FRAME_UNEXPECTED`)
//! - `third_party/h3/h3/src/client/stream.rs` (first frame must be HEADERS;
//!   any known frame after trailers is `H3_FRAME_UNEXPECTED`)
//!
//! This seam validates frame type only, over already-decoded frame
//! metadata: it does not decode QPACK field sections, accumulate body
//! bytes, or create sessions. It is synchronous, allocation-free, and O(1)
//! per frame.
//!
//! ## Request field-section validation
//!
//! [`validate_request_field_section`] consumes one complete encoded field
//! section (the payload of the request's initial HEADERS frame), decodes it
//! with the committed capacity-zero QPACK decoder, and validates it as an
//! ordinary HTTP request per RFC 9114 Section 4.3.1, mirroring VPP's
//! `qpack_parse_request` (qpack.c), called by
//! `http3_stream_transport_rx_req` (http3.c:871), and its
//! `hpack_decode_request` chain (see the function doc for the exact checks
//! and error mapping). It returns a compact
//! [`RequestPseudoHeaders`] — method, scheme, authority, and path — for the
//! later publication slice, with no body and no AppSession. It is
//! synchronous, allocates only what the decoder allocates (the field-block
//! `Vec` and the literal bytes it owns), and is O(total field bytes).
//!
//! Plain CONNECT is validated by the sibling
//! [`validate_connect_request_field_section`], which returns
//! [`ConnectRequestPseudoHeaders`] — method and tunnel authority — and
//! rejects `:scheme`, `:path`, and the extended-CONNECT `:protocol` pseudo,
//! matching VPP's plain-CONNECT handling (http3.c). The tunnel authority
//! must syntactically carry a port, per VPP's `http3_verify_port_in_authority`
//! (http3.c); see the function doc.
//!
//! [`publish_request_field_section`] is the publication sibling: it decodes
//! one encoded field section, validates it in the same single walk, converts
//! the regular fields to [`AppHeader`] entries, and publishes one
//! [`InboundRequest`] with an empty body to the app FIFO (see the function
//! doc).
//!
//! Awaiting their consumers: the request-stream slice wires the frame
//! state machine and the field-section validation into stream dispatch, at
//! which point the `dead_code` allow can go.
#![allow(dead_code)]

use std::borrow::Cow;
use std::mem;

use crate::http_common::{
    AppHeader, FieldLineFlags, HeaderName, InboundRequest, PublishError, ReqMethod, UrlScheme,
    publish_inbound_request,
};
use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::frame::FrameType;
use crate::http3::proto::headers::{FieldSectionValidator, MessageKind};
use crate::http3::proto::qpack::block::decode_block;
use crate::http3::request_fields::validate_request_field_line;
use hammer_infra::fifo::Fifo;

/// The phase of a client request stream, advanced one frame at a time.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum RequestPhase {
    /// No initial HEADERS seen yet; the next frame must be HEADERS.
    AwaitingHeaders,
    /// Initial HEADERS seen; DATA and one trailing HEADERS are allowed.
    Body,
    /// Trailing HEADERS seen; the stream must now end.
    Trailers,
}

impl RequestPhase {
    /// The phase of a newly opened request stream.
    pub(crate) const fn initial() -> Self {
        Self::AwaitingHeaders
    }

    /// Validate `ty` against the current phase and advance.
    ///
    /// Returns the next phase, or `ErrorCode::FrameUnexpected` for a frame
    /// that violates the request-stream ordering (RFC 9114 Section 4.1) or
    /// is forbidden on a client request stream (Sections 7.2.3-7.2.7).
    /// Unknown frame types are tolerated and leave the phase unchanged.
    pub(crate) fn on_frame(self, ty: FrameType) -> Result<Self, ErrorCode> {
        match (ty, self) {
            // The initial HEADERS opens the request.
            (FrameType::HEADERS, Self::AwaitingHeaders) => Ok(Self::Body),
            // One trailing HEADERS section ends the request body.
            (FrameType::HEADERS, Self::Body) => Ok(Self::Trailers),
            (FrameType::HEADERS, Self::Trailers) => Err(ErrorCode::FrameUnexpected),
            // DATA is only allowed in the body.
            (FrameType::DATA, Self::Body) => Ok(self),
            (FrameType::DATA, _) => Err(ErrorCode::FrameUnexpected),
            // Control-stream-only frames (RFC 9114 Sections 7.2.3, 7.2.4,
            // 7.2.6, 7.2.7).
            (
                FrameType::CANCEL_PUSH
                | FrameType::SETTINGS
                | FrameType::GOAWAY
                | FrameType::MAX_PUSH_ID,
                _,
            ) => Err(ErrorCode::FrameUnexpected),
            // A client MUST NOT send PUSH_PROMISE (RFC 9114 Section 7.2.5).
            (FrameType::PUSH_PROMISE, _) => Err(ErrorCode::FrameUnexpected),
            // Unknown types are extension frames; ignore (VPP drains them).
            _ => Ok(self),
        }
    }
}

/// The validated pseudo-headers of an ordinary (non-CONNECT) request field
/// section.
///
/// `method`, `scheme`, `authority`, and `path` are always present: the
/// committed [`FieldSectionValidator`] requires `:method` and — outside
/// plain CONNECT, which this seam rejects as unsupported — `:scheme` and
/// `:path` exactly once each, and this seam requires the `:authority` pseudo
/// itself even if a `Host` field is present, as VPP does (http3.c rejects a
/// missing `:authority` with `HTTP3_ERROR_MESSAGE_ERROR`; there is no Host
/// fallback). The value mappings below reject any value they do not know.
/// The `Host` value is not extracted, since regular fields are out of this
/// seam's scope.
///
/// Allocation truth: the `authority` and `path` Cows are moved out of the
/// decoded field block, never copied. Static-table entries keep borrowing
/// their `'static` bytes; literal entries keep the single allocation the
/// decoder made for them. No copy is added by this struct.
#[derive(Debug, PartialEq)]
pub(crate) struct RequestPseudoHeaders {
    pub(crate) method: ReqMethod,
    pub(crate) scheme: UrlScheme,
    pub(crate) authority: Cow<'static, [u8]>,
    pub(crate) path: Cow<'static, [u8]>,
}

/// The validated pseudo-headers of a plain CONNECT request field section
/// (RFC 9114 Section 4.4): the CONNECT method and the `:authority` of the
/// tunnel target. `scheme` and `path` are deliberately absent — a plain
/// CONNECT must not carry them.
///
/// Allocation truth: the `authority` Cow is moved out of the decoded field
/// block, never copied; see [`RequestPseudoHeaders`].
#[derive(Debug, PartialEq)]
pub(crate) struct ConnectRequestPseudoHeaders {
    pub(crate) method: ReqMethod,
    pub(crate) authority: Cow<'static, [u8]>,
}

/// Validate one complete encoded plain-CONNECT request field section (RFC
/// 9114 Section 4.4) and extract its tunnel authority.
///
/// The section is decoded and walked exactly like
/// [`validate_request_field_section`], sharing its error mapping: a QPACK
/// decode failure maps to [`ErrorCode::QpackDecompressionFailed`]; field-line
/// violations, duplicate or empty pseudos, missing `:method`, and a missing
/// `:authority` map to [`ErrorCode::MessageError`]. Plain CONNECT then
/// requires the method value to be exactly CONNECT and the `:authority`
/// pseudo itself — not a `Host` substitute, as VPP requires (http3.c, no
/// Host fallback).
///
/// A plain CONNECT must carry no `:scheme` or `:path` (RFC 9114 Section
/// 4.4); this seam rejects their presence with [`ErrorCode::MessageError`],
/// matching VPP, which likewise rejects them on a CONNECT (http3.c) even
/// though it exempts CONNECT from the missing-scheme and missing-path
/// checks.
///
/// The tunnel `:authority` must syntactically carry a port, per VPP's
/// `http3_verify_port_in_authority` (http3.c): the authority must end in a
/// nonempty ASCII digit run immediately preceded by `:`. The check is
/// deliberately syntactic — VPP does not parse or range-check the port, so
/// an empty host, leading zeros, huge values, and multiple colons all pass,
/// while a missing or malformed port maps to [`ErrorCode::MessageError`].
///
/// Extended CONNECT (`:protocol`, RFC 8441 / RFC 9220) is out of this seam's
/// scope: after the structural checks pass, a `:protocol` pseudo maps to
/// [`ErrorCode::GeneralProtocolError`], and any method other than CONNECT
/// maps to [`ErrorCode::GeneralProtocolError`] too. The `:method` presence
/// and value are checked before the structural `finish`, matching VPP
/// (http3.c): a missing `:method` maps to [`ErrorCode::MessageError`] first,
/// then a non-CONNECT method value maps to
/// [`ErrorCode::GeneralProtocolError`] before the
/// missing-`:scheme`/`:path` structural checks.
///
/// Returns only the pseudo values; the decoded field block is dropped.
/// Synchronous and lock-free, with one decode allocation; the returned
/// authority is moved out of the decoded block, never copied.
pub(crate) fn validate_connect_request_field_section(
    encoded: &[u8],
) -> Result<ConnectRequestPseudoHeaders, ErrorCode> {
    let mut read = encoded;
    let fields = decode_block(&mut read).map_err(|_| ErrorCode::QpackDecompressionFailed)?;

    let mut validator = FieldSectionValidator::new(MessageKind::Request);
    // The pseudo values are moved out of the decoded block as the walk
    // passes them; the method is mapped before `finish`, matching VPP:
    // `:method` presence, then its value, precede the missing-`:scheme` and
    // missing-`:path` structural checks (http3.c).
    let mut saw_method = false;
    let mut is_connect = false;
    let mut authority = None;
    let mut saw_scheme = false;
    let mut saw_path = false;
    let mut saw_protocol = false;
    for field in fields {
        validator
            .on_field(&field.name, &field.value)
            .map_err(|_| ErrorCode::MessageError)?;
        match &field.name[..] {
            b":method" => {
                saw_method = true;
                is_connect = field.value.as_ref() == b"CONNECT";
            }
            b":scheme" => saw_scheme = true,
            b":authority" => authority = Some(field.value),
            b":path" => saw_path = true,
            b":protocol" => saw_protocol = true,
            _ => {}
        }
    }
    // VPP checks `:method` presence first (MESSAGE_ERROR), then the method
    // value (GENERAL_PROTOCOL_ERROR for an unsupported method), before the
    // missing-`:scheme`/`:path` structural checks; a non-CONNECT method here
    // is exactly that unsupported value, so it maps before `finish`.
    if !saw_method {
        return Err(ErrorCode::MessageError);
    }
    if !is_connect {
        // Any other method is a different request kind, not a malformed
        // CONNECT.
        return Err(ErrorCode::GeneralProtocolError);
    }
    validator.finish().map_err(|_| ErrorCode::MessageError)?;

    // `finish` guarantees an `:authority` or a matching `Host`; this seam
    // requires the `:authority` pseudo itself (VPP has no Host fallback),
    // so the `ok_or` arm is a typed error rather than a panic.
    if saw_protocol {
        // Extended CONNECT (RFC 9220) is out of this seam's scope.
        return Err(ErrorCode::GeneralProtocolError);
    }
    if saw_scheme || saw_path {
        // RFC 9114 Section 4.4: a plain CONNECT MUST NOT contain them.
        return Err(ErrorCode::MessageError);
    }
    let authority = authority.ok_or(ErrorCode::MessageError)?;
    // The tunnel authority must syntactically carry a port, per VPP's
    // `http3_verify_port_in_authority` (http3.c); failure maps to
    // MESSAGE_ERROR. Runs after the structural, method, protocol, and
    // scheme/path checks above.
    if !verify_port_in_authority(&authority) {
        return Err(ErrorCode::MessageError);
    }
    Ok(ConnectRequestPseudoHeaders {
        method: ReqMethod::Connect,
        authority,
    })
}

/// Validate one complete encoded request field section (RFC 9114 Section
/// 4.3.1) and extract its pseudo-headers, mirroring VPP's
/// `qpack_parse_request` (qpack.c), called by
/// `http3_stream_transport_rx_req` (http3.c:871).
///
/// The section is decoded with the committed capacity-zero QPACK decoder
/// and walked once through the committed [`FieldSectionValidator`], which
/// enforces field-line validity, pseudo-before-regular ordering, unknown
/// and duplicate pseudos, `:status` in a request, empty pseudo values, and
/// the required `:method`, `:scheme`, `:path`, and `:authority`/`Host` rules
/// (with the plain-CONNECT exemption, RFC 9114 Section 4.4). This seam then
/// requires the `:authority` pseudo itself even if a `Host` field is
/// present, as VPP does (http3.c rejects a missing `:authority` with
/// `HTTP3_ERROR_MESSAGE_ERROR`; there is no Host fallback). Any violation
/// maps to [`ErrorCode::MessageError`]; a QPACK decode failure maps to
/// [`ErrorCode::QpackDecompressionFailed`].
///
/// Only ordinary requests are supported: the `:method` value must be GET,
/// POST, or PUT (VPP `foreach_http_method`), and the `:scheme` value must be
/// http or https. Any other scheme — including masque, which VPP's scheme
/// table knows but which belongs to the CONNECT/CONNECT-UDP seam — maps to
/// [`ErrorCode::InternalError`]. An unsupported method — including plain
/// and extended CONNECT, which the validator structurally permits — maps to
/// [`ErrorCode::GeneralProtocolError`]. The `:method` value is checked
/// before `finish`, matching VPP: an unsupported method maps to
/// [`ErrorCode::GeneralProtocolError`] even when `:scheme` is missing,
/// and a missing `:method` maps to [`ErrorCode::MessageError`] first, as
/// VPP checks presence before value. All other structural errors are
/// reported before value mapping.
///
/// Returns only the pseudo values; the decoded field block is dropped.
/// Synchronous and lock-free, with one decode allocation; the returned
/// authority and path values are moved out of the decoded block, never
/// copied (see [`RequestPseudoHeaders`]).
pub(crate) fn validate_request_field_section(
    encoded: &[u8],
) -> Result<RequestPseudoHeaders, ErrorCode> {
    let mut read = encoded;
    let fields = decode_block(&mut read).map_err(|_| ErrorCode::QpackDecompressionFailed)?;

    let mut validator = FieldSectionValidator::new(MessageKind::Request);
    // The pseudo values are moved out of the decoded block as the walk
    // passes them; the method value is mapped before `finish` — VPP checks
    // the method value before the missing-`:scheme` check (http3.c) — and
    // the scheme value after it.
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    for field in fields {
        validator
            .on_field(&field.name, &field.value)
            .map_err(|_| ErrorCode::MessageError)?;
        match &field.name[..] {
            b":method" => method = Some(field.value),
            b":scheme" => scheme = Some(field.value),
            b":authority" => authority = Some(field.value),
            b":path" => path = Some(field.value),
            _ => {}
        }
    }
    // VPP rejects an unsupported `:method` (http3.c, GENERAL_PROTOCOL_ERROR)
    // before the missing-`:scheme` structural check (http3.c, MESSAGE_ERROR),
    // so the method value is mapped before `finish`; a missing `:method`
    // still maps to MessageError here, before the value check, as VPP
    // checks presence before value.
    let method = parse_method(&method.ok_or(ErrorCode::MessageError)?)?;
    validator.finish().map_err(|_| ErrorCode::MessageError)?;

    // `finish` guarantees exactly one non-empty `:scheme` and `:path` for
    // an ordinary request and an `:authority` or a matching `Host`; this
    // seam requires the `:authority` pseudo itself (VPP has no Host
    // fallback), so the `ok_or` arms are typed errors rather than panics.
    let scheme = parse_scheme(&scheme.ok_or(ErrorCode::MessageError)?)?;
    let authority = authority.ok_or(ErrorCode::MessageError)?;
    let path = path.ok_or(ErrorCode::MessageError)?;
    Ok(RequestPseudoHeaders {
        method,
        scheme,
        authority,
        path,
    })
}

/// Why publishing one request field section failed. The protocol-level
/// variants carry the same connection-error mapping as the sibling
/// [`validate_request_field_section`]; `Publish` passes a FIFO-level failure
/// (capacity, reservation, or commit) through.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RequestPublishError {
    /// QPACK decompression of the encoded field section failed.
    QpackDecompressionFailed,
    /// The field section violates RFC 9114 request semantics: structural
    /// validation, the request field-line policy, a missing required
    /// pseudo, or a malformed declared Content-Length.
    MessageError,
    /// The `:method` value is not an ordinary method supported by this seam.
    GeneralProtocolError,
    /// The `:scheme` value is not http or https.
    InternalError,
    /// The request could not be published to the FIFO.
    Publish(PublishError),
}

impl From<PublishError> for RequestPublishError {
    fn from(error: PublishError) -> Self {
        Self::Publish(error)
    }
}

/// A request's declared Content-Length: a non-empty run of ASCII decimal
/// digits that fits in a `u64`.
///
/// Only the first regular `content-length` field of a section is recognized,
/// VPP's first-wins rule (`content_len_header_index == ~0` guard,
/// hpack_inlines.h:594; http3.c:998); later duplicates are ignored without
/// validation. The value is parsed with VPP's checked digit walk
/// (`http_parse_content_length`, http_private.h:816): a non-digit or an
/// overflow fails with [`RequestPublishError::MessageError`], as VPP
/// terminates with `HTTP3_ERROR_MESSAGE_ERROR` (http3.c:1003). An empty
/// value is deliberately rejected too, although VPP's loop accepts it as
/// zero: RFC 9110 Section 8.6 requires Content-Length to be a non-empty
/// digit run.
///
/// No allocation: one pass over the value bytes, O(n) time, O(1) space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentLength(u64);

impl TryFrom<&[u8]> for ContentLength {
    type Error = RequestPublishError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(RequestPublishError::MessageError);
        }
        let mut len: u64 = 0;
        for &b in value {
            if !b.is_ascii_digit() {
                return Err(RequestPublishError::MessageError);
            }
            // Checked accumulate: fail before `len * 10 + digit` can wrap.
            let digit = u64::from(b - b'0');
            if len > (u64::MAX - digit) / 10 {
                return Err(RequestPublishError::MessageError);
            }
            len = len * 10 + digit;
        }
        Ok(Self(len))
    }
}

impl From<ContentLength> for u64 {
    fn from(len: ContentLength) -> Self {
        len.0
    }
}

/// Decode one complete encoded request field section, validate it as an
/// ordinary HTTP request (RFC 9114 Section 4.3.1), and publish it to `fifo`
/// as one [`InboundRequest`] with an empty body.
///
/// Unlike [`validate_request_field_section`], which returns only the pseudo
/// values and drops the decoded block, this seam keeps the decoded block
/// alive for the whole build+publish call: the regular fields are converted
/// to [`AppHeader`] entries borrowing the decoded bytes, and the pseudo
/// values are moved out of the block without copying. The decoded block, the
/// header-entry vector, and the FIFO reservation are the only allocations.
///
/// The walk mirrors `validate_request_field_section` (and VPP's
/// `qpack_parse_request`, qpack.c, called by `http3_stream_transport_rx_req`,
/// http3.c:871): one `decode_block`, then one pass through the committed
/// [`FieldSectionValidator`] with the request-only field-line policy of
/// [`validate_request_field_line`] applied to regular names in that same
/// walk. A QPACK decode failure maps to
/// [`RequestPublishError::QpackDecompressionFailed`]; any field-section
/// violation maps to [`RequestPublishError::MessageError`]; method and
/// scheme values map exactly as [`parse_method`] and [`parse_scheme`] do;
/// `PublishError` passes through unchanged.
///
/// On success the request's declared Content-Length is returned: the first
/// regular `content-length` field, recorded in the same single walk (no
/// second pass, no collection, no copy) and parsed with VPP's first-wins
/// rule ([`ContentLength`]) only after the method, field-section
/// structural, scheme, and authority/path checks have all succeeded — the
/// order VPP uses before its own parse (http3.c:899-1013). `None` when the
/// section declares none. A non-digit, empty, or overflowing declared value
/// maps to [`RequestPublishError::MessageError`], as VPP's failure does
/// (http3.c:1003). The declared length does not yet size the body: the
/// published [`InboundRequest`] keeps its empty body, and the field still
/// publishes as a regular [`AppHeader`] entry.
///
/// `:path` is split at the first `?` into the request-target path and query
/// spans, both borrowing the same decoded buffer (no byte copy); a path
/// without `?` yields an empty query. The leading slash of the path is
/// retained, matching the Hammer golden fixtures.
///
/// Synchronous and lock-free: one QPACK decode, one validation/publication
/// walk over the decoded fields (plus the `?` scan of the single path
/// buffer), O(total field bytes).
pub(crate) fn publish_request_field_section(
    fifo: &Fifo,
    encoded: &[u8],
) -> Result<Option<u64>, RequestPublishError> {
    let mut read = encoded;
    let mut fields =
        decode_block(&mut read).map_err(|_| RequestPublishError::QpackDecompressionFailed)?;

    let mut validator = FieldSectionValidator::new(MessageKind::Request);
    // The pseudo values are moved out of the decoded block during the walk;
    // the regular-field `AppHeader` entries borrow the block, which stays
    // alive through the publish. The method value is mapped before `finish`
    // — VPP checks the method value before the missing-`:scheme` check
    // (http3.c) — and the scheme value after it.
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut headers = Vec::new();
    // Value of the first regular `content-length` field, if any: recorded
    // as a shared reference here in the walk, parsed only after the
    // validation sequence (see the function doc); the decoded block stays
    // alive through the publish.
    let mut content_length_value = None;
    for field in &mut fields {
        validator
            .on_field(&field.name, &field.value)
            .map_err(|_| RequestPublishError::MessageError)?;
        // Unknown pseudos were already rejected by `on_field`; this arm is
        // exactly the regular fields.
        match &field.name[..] {
            b":method" => method = Some(mem::take(&mut field.value)),
            b":scheme" => scheme = Some(mem::take(&mut field.value)),
            b":authority" => authority = Some(mem::take(&mut field.value)),
            b":path" => path = Some(mem::take(&mut field.value)),
            _ => {
                validate_request_field_line(field)
                    .map_err(|_| RequestPublishError::MessageError)?;
                // The first regular `content-length` field alone declares
                // the body length (VPP's first-wins rule, hpack_inlines.h:
                // 594); later duplicates are ignored without validation.
                // Names are already lowercase tchar — the validator enforces
                // VPP's lowercase fold — so the byte-exact match suffices.
                if content_length_value.is_none() && field.name.as_ref() == b"content-length" {
                    content_length_value = Some(field.value.as_ref());
                }
                // The decoded N bit (or an empty set) is published verbatim:
                // the ABI flags must reflect the wire's never-index policy.
                let flags = field.flags;
                headers.push(match HeaderName::try_from(field.name.as_ref()) {
                    Ok(name) => AppHeader::Known {
                        flags,
                        name: name.into(),
                        value: field.value.as_ref(),
                    },
                    Err(_) => AppHeader::Custom {
                        flags,
                        name: field.name.as_ref(),
                        value: field.value.as_ref(),
                    },
                });
            }
        }
    }
    // VPP rejects an unsupported `:method` (http3.c, GENERAL_PROTOCOL_ERROR)
    // before the missing-`:scheme` structural check (http3.c, MESSAGE_ERROR),
    // so the method value is mapped before `finish`; a missing `:method`
    // still maps to MessageError here, before the value check, as VPP
    // checks presence before value.
    let method = parse_method(&method.ok_or(RequestPublishError::MessageError)?)
        .map_err(|_| RequestPublishError::GeneralProtocolError)?;
    validator
        .finish()
        .map_err(|_| RequestPublishError::MessageError)?;

    // `finish` guarantees exactly one non-empty `:scheme` and `:path` for
    // an ordinary request and an `:authority` or a matching `Host`; this
    // seam requires the `:authority` pseudo itself (VPP has no Host
    // fallback), so the `ok_or` arms are typed errors rather than panics.
    let scheme = parse_scheme(&scheme.ok_or(RequestPublishError::MessageError)?)
        .map_err(|_| RequestPublishError::InternalError)?;
    let authority = authority.ok_or(RequestPublishError::MessageError)?;
    let path = path.ok_or(RequestPublishError::MessageError)?;

    // Split `:path` at the first `?` into the target path and query spans.
    // Both borrow the same decoded buffer, so no bytes are copied; a path
    // without `?` yields an empty query.
    let (target_path, target_query) = match path.iter().position(|&b| b == b'?') {
        Some(query_start) => (&path[..query_start], &path[query_start + 1..]),
        None => (&path[..], &[][..]),
    };

    // VPP parses the declared Content-Length only after the method,
    // structural, scheme, and authority/path checks have all succeeded
    // (http3.c:998), so the recorded first occurrence is parsed here, from
    // the shared reference into the untouched regular-field value: no
    // clone.
    let content_length = match content_length_value {
        Some(value) => Some(ContentLength::try_from(value)?.into()),
        None => None,
    };

    let request = InboundRequest {
        method,
        scheme,
        target_authority: authority.as_ref(),
        target_path,
        target_query,
        headers: &headers,
        body: b"",
    };
    publish_inbound_request(fifo, &request)?;
    Ok(content_length)
}

/// Map an `:method` value to the VPP request-method enum
/// (`foreach_http_method`, http.h). Only the ordinary methods supported by
/// this seam are accepted; VPP rejects an unsupported method with
/// `H3_GENERAL_PROTOCOL_ERROR`. The match is case-sensitive, as VPP's
/// method table is.
fn parse_method(method: &[u8]) -> Result<ReqMethod, ErrorCode> {
    match method {
        b"GET" => Ok(ReqMethod::Get),
        b"POST" => Ok(ReqMethod::Post),
        b"PUT" => Ok(ReqMethod::Put),
        _ => Err(ErrorCode::GeneralProtocolError),
    }
}

/// Map a `:scheme` value to the VPP scheme enum (`http_url_scheme_t`,
/// http.h). Only ordinary-request schemes are accepted here; any other
/// value — including masque, which VPP's scheme table knows but which
/// belongs to the CONNECT/CONNECT-UDP seam — maps to VPP's internal error.
fn parse_scheme(scheme: &[u8]) -> Result<UrlScheme, ErrorCode> {
    match scheme {
        b"http" => Ok(UrlScheme::Http),
        b"https" => Ok(UrlScheme::Https),
        _ => Err(ErrorCode::InternalError),
    }
}

/// VPP-compatible syntactic port check (`http3_verify_port_in_authority`,
/// http3.c): the authority is accepted iff it ends in a nonempty ASCII digit
/// run immediately preceded by `:`.
///
/// The check is deliberately syntactic, exactly like VPP's: no parsing or
/// range check, so an empty host (`:443`), leading zeros, values beyond any
/// port range, and multiple colons all pass; no `:`, a trailing `:`, and any
/// non-digit tail fail.
///
/// One reverse pass over the authority: O(n) time, O(1) extra space, no
/// allocation, copy, or pre-scan.
fn verify_port_in_authority(authority: &[u8]) -> bool {
    let mut digits = 0;
    for &b in authority.iter().rev() {
        if b.is_ascii_digit() {
            digits += 1;
            continue;
        }
        // The trailing digit run must be immediately preceded by `:`.
        return digits > 0 && b == b':';
    }
    // The whole authority is digits, so it has no preceding `:`.
    false
}
