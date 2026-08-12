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
//! Awaiting their consumers: the request-stream slice wires the frame
//! state machine and the field-section validation into stream dispatch, at
//! which point the `dead_code` allow can go.
#![allow(dead_code)]

use std::borrow::Cow;

use crate::http_common::{ReqMethod, UrlScheme};
use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::frame::FrameType;
use crate::http3::proto::headers::{FieldSectionValidator, MessageKind};
use crate::http3::proto::qpack::block::decode_block;

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
                FrameType::CANCEL_PUSH | FrameType::SETTINGS | FrameType::GOAWAY | FrameType::MAX_PUSH_ID,
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
/// scope: only after the structural validation passes does a `:protocol`
/// pseudo map to [`ErrorCode::GeneralProtocolError`], and any method other
/// than CONNECT — which the structural validator admits — maps to
/// [`ErrorCode::GeneralProtocolError`] too. Structural errors therefore take
/// precedence over both mappings.
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
    // passes them; the method is compared only after `finish`, so
    // structural errors take precedence over the method check.
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
            b":method" => is_connect = field.value.as_ref() == b"CONNECT",
            b":scheme" => saw_scheme = true,
            b":authority" => authority = Some(field.value),
            b":path" => saw_path = true,
            b":protocol" => saw_protocol = true,
            _ => {}
        }
    }
    validator.finish().map_err(|_| ErrorCode::MessageError)?;

    // `finish` guarantees a `:method` and an `:authority` or a matching
    // `Host`; this seam requires the `:authority` pseudo itself (VPP has no
    // Host fallback), so the `ok_or` arm is a typed error rather than a
    // panic.
    if !is_connect {
        // Any other method is a different request kind, not a malformed
        // CONNECT.
        return Err(ErrorCode::GeneralProtocolError);
    }
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
/// [`ErrorCode::GeneralProtocolError`]. Structural errors are always
/// reported before value mapping.
///
/// Returns only the pseudo values; the decoded field block is dropped.
/// Synchronous and lock-free, with one decode allocation; the returned
/// authority and path values are moved out of the decoded block, never
/// copied (see [`RequestPseudoHeaders`]).
pub(crate) fn validate_request_field_section(encoded: &[u8]) -> Result<RequestPseudoHeaders, ErrorCode> {
    let mut read = encoded;
    let fields = decode_block(&mut read).map_err(|_| ErrorCode::QpackDecompressionFailed)?;

    let mut validator = FieldSectionValidator::new(MessageKind::Request);
    // The pseudo values are moved out of the decoded block as the walk
    // passes them; method and scheme are mapped only after `finish`, so
    // structural errors take precedence over value mapping.
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
    validator.finish().map_err(|_| ErrorCode::MessageError)?;

    // `finish` guarantees exactly one non-empty `:method`, `:scheme`, and
    // `:path` for an ordinary request and an `:authority` or a matching
    // `Host`; this seam requires the `:authority` pseudo itself (VPP has no
    // Host fallback), so the `ok_or` arms are typed errors rather than
    // panics.
    let method = parse_method(&method.ok_or(ErrorCode::MessageError)?)?;
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

#[cfg(test)]
mod tests {
    use super::{
        RequestPhase, validate_connect_request_field_section, validate_request_field_section,
    };
    use crate::http3::proto::error::ErrorCode;
    use crate::http3::proto::frame::FrameType;
    use crate::http3::proto::qpack::block::encode_block;
    use crate::http3::proto::qpack::field::HeaderField;
    use crate::http_common::{ReqMethod, UrlScheme};

    #[test]
    fn data_before_headers_rejected() {
        let phase = RequestPhase::initial();
        let res = phase.on_frame(FrameType::DATA);
        assert_eq!(res, Err(ErrorCode::FrameUnexpected));
    }

    #[test]
    fn headers_data_trailers_accepted() {
        let phase = RequestPhase::initial();
        let phase = phase.on_frame(FrameType::HEADERS).expect("initial headers");
        assert_eq!(phase, RequestPhase::Body);

        let phase = phase.on_frame(FrameType::DATA).expect("body data");
        assert_eq!(phase, RequestPhase::Body);

        let phase = phase.on_frame(FrameType::DATA).expect("more body data");
        assert_eq!(phase, RequestPhase::Body);

        let phase = phase.on_frame(FrameType::HEADERS).expect("trailers");
        assert_eq!(phase, RequestPhase::Trailers);
    }

    #[test]
    fn duplicate_trailing_headers_rejected() {
        let phase = RequestPhase::initial();
        let phase = phase.on_frame(FrameType::HEADERS).expect("initial headers");
        let phase = phase.on_frame(FrameType::DATA).expect("body data");
        let phase = phase.on_frame(FrameType::HEADERS).expect("trailers");
        assert_eq!(phase, RequestPhase::Trailers);

        let res = phase.on_frame(FrameType::HEADERS);
        assert_eq!(res, Err(ErrorCode::FrameUnexpected));
    }

    #[test]
    fn data_after_trailers_rejected() {
        let phase = RequestPhase::initial();
        let phase = phase.on_frame(FrameType::HEADERS).expect("initial headers");
        let phase = phase.on_frame(FrameType::HEADERS).expect("trailers");
        assert_eq!(phase, RequestPhase::Trailers);

        let res = phase.on_frame(FrameType::DATA);
        assert_eq!(res, Err(ErrorCode::FrameUnexpected));
    }

    #[test]
    fn control_only_frames_rejected_in_every_phase() {
        for ty in [
            FrameType::SETTINGS,
            FrameType::CANCEL_PUSH,
            FrameType::GOAWAY,
            FrameType::MAX_PUSH_ID,
        ] {
            let phase = RequestPhase::initial();
            assert_eq!(phase.on_frame(ty), Err(ErrorCode::FrameUnexpected));

            let phase = phase.on_frame(FrameType::HEADERS).expect("initial headers");
            assert_eq!(phase.on_frame(ty), Err(ErrorCode::FrameUnexpected));

            let phase = phase.on_frame(FrameType::DATA).expect("body data");
            assert_eq!(phase.on_frame(ty), Err(ErrorCode::FrameUnexpected));
        }
    }

    #[test]
    fn push_promise_rejected() {
        let phase = RequestPhase::initial();
        let res = phase.on_frame(FrameType::PUSH_PROMISE);
        assert_eq!(res, Err(ErrorCode::FrameUnexpected));
    }

    #[test]
    fn unknown_frames_are_tolerated() {
        let unknown = FrameType::from_value(0x21).expect("unknown type in range");
        assert_ne!(unknown, FrameType::HEADERS);
        assert_ne!(unknown, FrameType::DATA);

        let phase = RequestPhase::initial();
        let phase = phase.on_frame(unknown).expect("unknown before headers");
        assert_eq!(phase, RequestPhase::AwaitingHeaders);

        let phase = phase.on_frame(FrameType::HEADERS).expect("initial headers");
        let phase = phase.on_frame(unknown).expect("unknown in body");
        assert_eq!(phase, RequestPhase::Body);

        let phase = phase.on_frame(FrameType::HEADERS).expect("trailers");
        let phase = phase.on_frame(unknown).expect("unknown after trailers");
        assert_eq!(phase, RequestPhase::Trailers);
    }

    // --- Request field-section validation ---

    /// Encode a field section with the committed encoder: one `Vec` written
    /// by `encode_block` (RFC 9204 Section 4.5.1 prefix included), with no
    /// capacity pre-scan and no hand-built bytes.
    fn section(fields: &[HeaderField]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_block(&mut out, fields).expect("test field section encodes");
        out
    }

    #[test]
    fn valid_get_request_yields_pseudo_headers() {
        let encoded = section(&[
            HeaderField::new(":method", "GET"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        let pseudo = validate_request_field_section(&encoded).expect("valid GET request");
        assert_eq!(pseudo.method, ReqMethod::Get);
        assert_eq!(pseudo.scheme, UrlScheme::Https);
        assert_eq!(pseudo.authority.as_ref(), b"example.com".as_slice());
        assert_eq!(pseudo.path.as_ref(), b"/".as_slice());
    }

    #[test]
    fn valid_post_request_yields_pseudo_headers() {
        let encoded = section(&[
            HeaderField::new(":method", "POST"),
            HeaderField::new(":scheme", "http"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        let pseudo = validate_request_field_section(&encoded).expect("valid POST request");
        assert_eq!(pseudo.method, ReqMethod::Post);
        assert_eq!(pseudo.scheme, UrlScheme::Http);
        assert_eq!(pseudo.authority.as_ref(), b"example.com".as_slice());
        assert_eq!(pseudo.path.as_ref(), b"/".as_slice());
    }

    #[test]
    fn unknown_method_rejected() {
        let encoded = section(&[
            HeaderField::new(":method", "GEPT"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        assert_eq!(
            validate_request_field_section(&encoded),
            Err(ErrorCode::GeneralProtocolError)
        );
    }

    #[test]
    fn unknown_scheme_rejected() {
        // masque is known to VPP's scheme table but belongs to the CONNECT
        // seam; this seam maps every non-http(s) scheme to an internal error.
        for scheme in ["ftp", "masque"] {
            let encoded = section(&[
                HeaderField::new(":method", "GET"),
                HeaderField::new(":scheme", scheme),
                HeaderField::new(":authority", "example.com"),
                HeaderField::new(":path", "/"),
            ]);
            assert_eq!(
                validate_request_field_section(&encoded),
                Err(ErrorCode::InternalError)
            );
        }
    }

    #[test]
    fn corrupted_block_rejected() {
        assert_eq!(
            validate_request_field_section(&[0x00]),
            Err(ErrorCode::QpackDecompressionFailed)
        );
    }

    #[test]
    fn status_pseudo_rejected_in_request() {
        let encoded = section(&[HeaderField::new(":status", "200")]);
        assert_eq!(
            validate_request_field_section(&encoded),
            Err(ErrorCode::MessageError)
        );
    }

    #[test]
    fn missing_authority_rejected_even_with_host() {
        // VPP requires the `:authority` pseudo unconditionally (http3.c);
        // a Host field does not substitute.
        let encoded = section(&[
            HeaderField::new(":method", "GET"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new("host", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        assert_eq!(
            validate_request_field_section(&encoded),
            Err(ErrorCode::MessageError)
        );
    }

    #[test]
    fn missing_required_pseudo_rejected() {
        // Small table: each required ordinary-request pseudo omitted from
        // an otherwise valid section (`:authority` is covered separately).
        for fields in [
            vec![
                HeaderField::new(":scheme", "https"),
                HeaderField::new(":authority", "example.com"),
                HeaderField::new(":path", "/"),
            ],
            vec![
                HeaderField::new(":method", "GET"),
                HeaderField::new(":authority", "example.com"),
                HeaderField::new(":path", "/"),
            ],
            vec![
                HeaderField::new(":method", "GET"),
                HeaderField::new(":scheme", "https"),
                HeaderField::new(":authority", "example.com"),
            ],
        ] {
            assert_eq!(
                validate_request_field_section(&section(&fields)),
                Err(ErrorCode::MessageError)
            );
        }
    }

    #[test]
    fn pseudo_after_regular_rejected() {
        // Pseudo-headers must precede every regular field, as VPP's
        // hpack_decode_request enforces (HPACK_ERROR_PROTOCOL).
        let encoded = section(&[
            HeaderField::new(":method", "GET"),
            HeaderField::new("host", "example.com"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        assert_eq!(
            validate_request_field_section(&encoded),
            Err(ErrorCode::MessageError)
        );
    }

    #[test]
    fn connect_method_rejected() {
        // Plain CONNECT passes the structural validator (scheme/path
        // exempt, RFC 9114 Section 4.4) but is outside this seam's
        // ordinary-method set.
        let encoded = section(&[
            HeaderField::new(":method", "CONNECT"),
            HeaderField::new(":scheme", "https"),
            HeaderField::new(":authority", "example.com"),
            HeaderField::new(":path", "/"),
        ]);
        assert_eq!(
            validate_request_field_section(&encoded),
            Err(ErrorCode::GeneralProtocolError)
        );
    }

    // --- Plain CONNECT field-section validation (RFC 9114 Section 4.4) ---

    #[test]
    fn valid_connect_authority_yields_pseudo_headers() {
        let encoded = section(&[
            HeaderField::new(":method", "CONNECT"),
            HeaderField::new(":authority", "example.com:443"),
        ]);
        let pseudo =
            validate_connect_request_field_section(&encoded).expect("valid plain CONNECT request");
        assert_eq!(pseudo.method, ReqMethod::Connect);
        assert_eq!(pseudo.authority.as_ref(), b"example.com:443".as_slice());
    }

    #[test]
    fn connect_missing_authority_rejected() {
        // The `:authority` pseudo itself is required, as VPP requires
        // (http3.c, no Host fallback); the plain-CONNECT exemption covers
        // only `:scheme` and `:path`.
        let encoded = section(&[
            HeaderField::new(":method", "CONNECT"),
            HeaderField::new("host", "example.com:443"),
        ]);
        assert_eq!(
            validate_connect_request_field_section(&encoded),
            Err(ErrorCode::MessageError)
        );
    }

    #[test]
    fn connect_restrictions_rejected() {
        // RFC 9114 Section 4.4 forbids `:scheme` and `:path` on a plain
        // CONNECT; extended CONNECT (`:protocol`) and any other method are
        // out of this seam's scope.
        for (fields, error) in [
            (
                vec![
                    HeaderField::new(":method", "CONNECT"),
                    HeaderField::new(":scheme", "https"),
                    HeaderField::new(":authority", "example.com:443"),
                ],
                ErrorCode::MessageError,
            ),
            (
                vec![
                    HeaderField::new(":method", "CONNECT"),
                    HeaderField::new(":authority", "example.com:443"),
                    HeaderField::new(":path", "/"),
                ],
                ErrorCode::MessageError,
            ),
            (
                vec![
                    HeaderField::new(":method", "CONNECT"),
                    HeaderField::new(":scheme", "https"),
                    HeaderField::new(":authority", "example.com:443"),
                    HeaderField::new(":path", "/"),
                ],
                ErrorCode::MessageError,
            ),
            (
                vec![
                    HeaderField::new(":method", "CONNECT"),
                    HeaderField::new(":protocol", "webtransport"),
                    HeaderField::new(":scheme", "https"),
                    HeaderField::new(":authority", "example.com:443"),
                    HeaderField::new(":path", "/"),
                ],
                ErrorCode::GeneralProtocolError,
            ),
            (
                vec![
                    HeaderField::new(":method", "GET"),
                    HeaderField::new(":scheme", "https"),
                    HeaderField::new(":authority", "example.com:443"),
                    HeaderField::new(":path", "/"),
                ],
                ErrorCode::GeneralProtocolError,
            ),
        ] {
            assert_eq!(
                validate_connect_request_field_section(&section(&fields)),
                Err(error)
            );
        }
    }

    #[test]
    fn connect_authority_without_port_rejected() {
        // VPP's http3_verify_port_in_authority requires a port on a plain
        // CONNECT authority: no `:`, a trailing `:`, or a non-digit tail all
        // map to MESSAGE_ERROR.
        for authority in ["example.com", "example.com:", "example.com:abc"] {
            let encoded = section(&[
                HeaderField::new(":method", "CONNECT"),
                HeaderField::new(":authority", authority),
            ]);
            assert_eq!(
                validate_connect_request_field_section(&encoded),
                Err(ErrorCode::MessageError)
            );
        }
    }

    #[test]
    fn connect_authority_port_is_syntactic_only() {
        // The port check is deliberately syntactic, exactly like VPP's
        // http3_verify_port_in_authority: an empty host, leading zeros, huge
        // values beyond any port range, and multiple colons all pass
        // unparsed and range-checked, and the authority is preserved.
        for authority in [
            ":443",
            "example.com:0443",
            "example.com:4294967296",
            "example.com:443:8080",
        ] {
            let encoded = section(&[
                HeaderField::new(":method", "CONNECT"),
                HeaderField::new(":authority", authority),
            ]);
            let pseudo = validate_connect_request_field_section(&encoded)
                .expect("syntactically ported CONNECT authority");
            assert_eq!(pseudo.authority.as_ref(), authority.as_bytes());
        }
    }
}
