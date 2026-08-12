//! Client request-stream frame ordering (RFC 9114 Section 4.1).
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
//! Awaiting its consumer: the request-stream slice wires this state machine
//! into stream dispatch, at which point the `dead_code` allow can go.
#![allow(dead_code)]

use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::frame::FrameType;

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

#[cfg(test)]
mod tests {
    use super::RequestPhase;
    use crate::http3::proto::error::ErrorCode;
    use crate::http3::proto::frame::FrameType;

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
}
