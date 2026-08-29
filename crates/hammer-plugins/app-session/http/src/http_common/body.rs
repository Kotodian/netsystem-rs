//! Body-length tracking shared by the HTTP/1.1, HTTP/2, and HTTP/3 request
//! adapters (RFC 9112 Section 6.3, RFC 9113 Section 8.1.2, RFC 9114
//! Section 4.1).
//!
//! A plain worker-local counter: no heap, lock, atomic, or async. The state
//! is one enum, so invalid combinations (e.g. a body both complete and still
//! receiving) cannot be represented. Mirrors the VPP `to_recv` accounting in
//! `third_party/vpp/src/plugins/http/http_private.h:392` ("remaining bytes of
//! body to receive from transport") and the per-version DATA handlers:
//! - `http1.c:1093-1101` (`http1_req_state_transport_io_more_data`): a write
//!   exceeding `to_recv` is "received more data than expected" and errors the
//!   request;
//! - `http2.c:1672-1687`: `payload_len > to_recv` is
//!   `HTTP2_ERROR_PROTOCOL_ERROR` before any accounting mutation; a stream
//!   that reaches its end state with `to_recv != 0` (peer closed early) is
//!   also `HTTP2_ERROR_PROTOCOL_ERROR`;
//! - `http3.c:1202-1252`: `fh.length > to_recv` is
//!   `HTTP3_ERROR_GENERAL_PROTOCOL_ERROR`; a half-closed stream with a
//!   pending `to_recv` and no more transport data is
//!   `HTTP3_ERROR_REQUEST_INCOMPLETE`.
//!
//! [`BodyError`] is protocol-neutral; each version adapter maps it to its own
//! wire error code. The only adapter today is HTTP/3
//! (`crate::http3::proto::error::ErrorCode`):
//! `DataWithoutDeclaredLength` -> `FrameUnexpected` (DATA outside the body
//! phase, `http3_stream_transport_rx_req`), `MoreDataThanDeclared` ->
//! `GeneralProtocolError`, `IncompleteAtEnd` -> `RequestIncomplete`.
//!
//! Deliberate Hammer/RFC 9114 difference from VPP: a declared Content-Length
//! of zero leaves the body `Complete`, so any subsequent DATA is
//! `DataWithoutDeclaredLength` (RFC 9114 Section 4.1.2). VPP's `to_recv == 0`
//! transport state can instead classify a non-empty DATA frame as
//! `GeneralProtocolError` (`http3_req_state_transport_io_more_data`).

/// Body-length state of a request stream.
///
/// Copy: the state is a tag plus an `u64` counter (16 bytes), and the
/// worker's generation-checked `StreamContext` copy-and-commit pattern
/// (e.g. `HttpWorker::process_request_data`) snapshots the accumulator
/// before mutating, so a rejected FIFO publication never advances it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyAccumulator {
    /// No Content-Length declared: DATA is unexpected and the body ends at FIN
    /// (RFC 9114 Section 4.1.2).
    NoBody,
    /// Declared Content-Length with `remaining` bytes still expected.
    Receiving { remaining: u64 },
    /// Declared body fully received.
    Complete,
}

/// Protocol-neutral body-length violation. Each version adapter maps these to
/// its own wire error code; the HTTP/3 mapping is documented in the module
/// docs above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyError {
    /// DATA received with no declared Content-Length or after the declared
    /// body completed.
    DataWithoutDeclaredLength,
    /// A DATA frame larger than the remaining declared body.
    MoreDataThanDeclared,
    /// The stream ended (FIN) while the declared body was still incomplete.
    IncompleteAtEnd,
}

impl From<Option<u64>> for BodyAccumulator {
    /// `Some(n)` declares Content-Length `n`; `None` is a stream with no
    /// declared length. A declared zero is immediately complete.
    fn from(declared: Option<u64>) -> Self {
        match declared {
            None => Self::NoBody,
            Some(0) => Self::Complete,
            Some(remaining) => Self::Receiving { remaining },
        }
    }
}

impl BodyAccumulator {
    /// Accounts for `len` body bytes of a DATA frame.
    ///
    /// DATA with no declared Content-Length or after completion is
    /// `DataWithoutDeclaredLength`; a frame larger than the remaining body is
    /// `MoreDataThanDeclared` and leaves the state unchanged (matching VPP's
    /// reject-before-mutation checks, e.g. http2.c:1672-1678 and
    /// http3.c:1202-1209). Otherwise the frame is accepted and the body is
    /// complete exactly when the remaining bytes reach zero.
    pub(crate) fn on_data(&mut self, len: u64) -> Result<(), BodyError> {
        match self {
            BodyAccumulator::NoBody | BodyAccumulator::Complete => {
                Err(BodyError::DataWithoutDeclaredLength)
            }
            BodyAccumulator::Receiving { remaining } => {
                if len > *remaining {
                    return Err(BodyError::MoreDataThanDeclared);
                }
                *remaining -= len;
                if *remaining == 0 {
                    *self = BodyAccumulator::Complete;
                }
                Ok(())
            }
        }
    }

    /// Checks the body against the stream's end.
    ///
    /// A FIN (`stream_finished`) with a declared but incomplete body is
    /// `IncompleteAtEnd` (VPP: http2.c:1682-1687 a stream closed with
    /// `to_recv != 0`; http3.c:1246-1252 a half-closed stream with no data
    /// left); a complete body, an undeclared length, or a mid-body checkpoint
    /// all pass.
    pub(crate) fn finish(&self, stream_finished: bool) -> Result<(), BodyError> {
        match self {
            BodyAccumulator::Receiving { .. } if stream_finished => Err(BodyError::IncompleteAtEnd),
            _ => Ok(()),
        }
    }
}
