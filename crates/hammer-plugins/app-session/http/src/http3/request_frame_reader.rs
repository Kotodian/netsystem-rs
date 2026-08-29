//! Incremental frame reader for a client request stream: reads frames one
//! at a time from arbitrary byte slices, capturing each HEADERS field
//! section (the initial one and the optional trailer), surfacing each DATA
//! frame's payload as a slice borrowed from the call's input, and draining
//! every other frame's payload without buffering it.
//!
//! Mirrors VPP `http3_stream_transport_rx_req` (http3.c ~1732): a frame
//! header is parsed against the per-stream table (`FrameHeader::parse` with
//! `FrameStream::Request`, frame.c) and against the request-stream ordering
//! (`RequestPhase`, RFC 9114 Section 4.1); a field section longer than
//! [`MAX_FIELD_SECTION_LEN`] is rejected with H3_EXCESSIVE_LOAD before any
//! of its payload is buffered; DATA payloads are surfaced as slices
//! borrowed from the call's input (no allocation); unknown frame payloads
//! are drained by counting, with no allocation. Unlike the one-shot
//! control-stream reader,
//! this reader spans the whole stream: it reports the exact number of bytes
//! each call consumes, so the caller keeps the trailing bytes of the next
//! frame in its own buffer.
//!
//! State is O(1): a fixed 16-byte header staging array, the in-progress
//! frame metadata, and a payload byte counter. The only allocation is the
//! single bounded `Vec` that captures a complete HEADERS field section.
//!
//! The worker-owned parsing API (`RequestFrameReader` through
//! `HttpWorker::process_request_bytes`) exists and is consumed by the
//! worker and its tests; production builtin RX callback wiring is a later
//! seam (`builtin_rx` remains `None`), after which the `dead_code` allow
//! can be removed.
#![allow(dead_code)]

use crate::http3::proto::error::ErrorCode;
use crate::http3::proto::frame::{FrameError, FrameHeader, FrameStream, FrameType};
use crate::http3::request::RequestPhase;

/// Maximum bytes of a frame header: two varints of at most 8 bytes each
/// (VPP `HTTP3_FRAME_HEADER_MAX_LEN`), the same bound as the control-stream
/// reader's same-named constant.
const MAX_FRAME_HEADER_LEN: usize = 16;

/// The largest encoded field section (RFC 9114 Section 4.2) the reader
/// captures: Hammer bounds one request field section at 64 KiB as load
/// protection, rejected with H3_EXCESSIVE_LOAD (RFC 9114 Section 8.1) from
/// the header alone, before any payload byte is buffered.
pub(crate) const MAX_FIELD_SECTION_LEN: usize = 64 * 1024;

/// Request-stream errors.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RequestFrameError {
    /// A malformed, reserved, or stream-invalid frame header (`FrameError`).
    Frame(FrameError),
    /// The frame violates request-stream ordering or is forbidden on a
    /// client request stream (RFC 9114 Section 4.1); the `ErrorCode` comes
    /// from [`RequestPhase::on_frame`].
    Phase(ErrorCode),
    /// A HEADERS field section longer than [`MAX_FIELD_SECTION_LEN`],
    /// rejected before any of its payload is buffered.
    OversizedFieldSection(u64),
}

impl RequestFrameError {
    /// The connection error code to send.
    pub(crate) fn error_code(&self) -> ErrorCode {
        match self {
            RequestFrameError::Frame(e) => e.error_code().unwrap_or(ErrorCode::FrameError),
            RequestFrameError::Phase(code) => *code,
            RequestFrameError::OversizedFieldSection(_) => ErrorCode::ExcessiveLoad,
        }
    }
}

/// The result of feeding bytes to [`RequestFrameReader`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum RequestFrameRead<'a> {
    /// More bytes are needed for the current frame; all input was consumed
    /// into the reader's partial-frame state.
    Incomplete,
    /// A complete HEADERS frame: its encoded field section (RFC 9114
    /// Section 4.2), exactly one allocation.
    Headers(Vec<u8>),
    /// The current call's DATA payload, borrowed from the call's input (no
    /// copy); `completed` is true exactly when the chunk exhausts the
    /// frame's payload.
    Data { chunk: &'a [u8], completed: bool },
    /// A complete non-HEADERS, non-DATA frame was drained without buffering
    /// its payload; carries the frame type and payload length.
    Drained(FrameType, u64),
}

/// Fixed-state, incremental reader for the frames of a client request
/// stream.
///
/// The reader is synchronous and allocation-free except for the single
/// bounded `Vec` capturing a HEADERS field section. Header and payload may
/// arrive across any number of calls: `push` returns the outcome of the
/// frame the call completes together with the exact number of bytes
/// consumed, surfacing each call's DATA payload bytes as a slice borrowed
/// from the call's input, and bytes beyond the completed frame are never
/// consumed, so the caller keeps them and passes them back on the next call.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct RequestFrameReader {
    /// Staging for the current frame header; briefly also holds the payload
    /// bytes that arrived alongside the header.
    header: [u8; MAX_FRAME_HEADER_LEN],
    /// Bytes of `header` currently staged.
    header_len: usize,
    /// Request-stream ordering state; advances only when a frame's entire
    /// payload has been consumed.
    phase: RequestPhase,
    /// The frame whose payload is being consumed, or `None` while reading a
    /// header.
    frame: Option<FrameHeader>,
    /// The phase the completed frame moves the reader to, published when its
    /// payload completes.
    pending_phase: Option<RequestPhase>,
    /// Payload bytes of the current frame consumed so far.
    payload_len: usize,
    /// The HEADERS field section being captured; only non-empty while
    /// `frame` is a HEADERS frame.
    field_section: Vec<u8>,
}

impl RequestFrameReader {
    /// A fresh reader for a new client request stream, awaiting its initial
    /// HEADERS frame.
    pub(crate) fn new() -> Self {
        RequestFrameReader {
            header: [0; MAX_FRAME_HEADER_LEN],
            header_len: 0,
            phase: RequestPhase::initial(),
            frame: None,
            pending_phase: None,
            payload_len: 0,
            field_section: Vec::new(),
        }
    }

    /// Feed stream bytes and return the outcome of the frame they complete,
    /// or [`RequestFrameRead::Incomplete`] while a frame is still partial,
    /// together with the exact number of bytes consumed.
    ///
    /// While a DATA frame is in progress, each call surfaces the payload
    /// bytes it contributed as a slice borrowed from the call's input, with
    /// `completed` set exactly when the chunk exhausts the frame's payload.
    ///
    /// Bytes beyond the completed frame are never consumed: the caller keeps
    /// them and passes them back on the next call. On error the stream is
    /// dead and the reader must not be fed again.
    pub(crate) fn push<'a>(
        &mut self,
        mut bytes: &'a [u8],
    ) -> Result<(RequestFrameRead<'a>, usize), RequestFrameError> {
        let input: &'a [u8] = bytes;
        let payload_at_entry = self.payload_len;
        let mut taken = 0usize;
        // Header staging bytes that lie beyond the current frame: they were
        // pulled from the input with the header but are not part of the
        // frame, so the consumed count must exclude them.
        let mut past = 0usize;
        // Input offset of this call's first DATA payload byte, when the
        // current frame is DATA: the staged payload that followed a header
        // parsed this call, or the start of the input on a continuation
        // call.
        let mut chunk_start = 0usize;

        if self.frame.is_none() {
            let head_at_entry = self.header_len;
            let take = (MAX_FRAME_HEADER_LEN - self.header_len).min(bytes.len());
            self.header[self.header_len..self.header_len + take].copy_from_slice(&bytes[..take]);
            self.header_len += take;
            taken += take;
            bytes = &bytes[take..];

            let mut buf: &[u8] = &self.header[..self.header_len];
            match FrameHeader::parse(&mut buf, FrameStream::Request) {
                Err(FrameError::Incomplete(_)) if self.header_len < MAX_FRAME_HEADER_LEN => {
                    return Ok((RequestFrameRead::Incomplete, taken));
                }
                // 16 bytes must suffice for two varints; anything longer is a
                // malformed header.
                Err(FrameError::Incomplete(_)) => {
                    return Err(RequestFrameError::Frame(FrameError::Malformed));
                }
                Err(e) => return Err(RequestFrameError::Frame(e)),
                Ok(frame) => {
                    let next = self
                        .phase
                        .on_frame(frame.ty)
                        .map_err(RequestFrameError::Phase)?;
                    // Payload bytes already staged with the header, extracted
                    // before the staging is reset.
                    let carry = (self.header_len - frame.header_len).min(frame.len as usize);
                    past = self.header_len - frame.header_len - carry;
                    if frame.ty == FrameType::HEADERS {
                        if frame.len > MAX_FIELD_SECTION_LEN as u64 {
                            return Err(RequestFrameError::OversizedFieldSection(frame.len));
                        }
                        self.field_section = Vec::with_capacity(frame.len as usize);
                        self.field_section.extend_from_slice(
                            &self.header[frame.header_len..frame.header_len + carry],
                        );
                        self.pending_phase = Some(next);
                    }
                    if frame.ty == FrameType::DATA {
                        // The staged payload bytes were appended this call
                        // (the header was incomplete at the call's start), so
                        // they are surfaced by borrowing the input.
                        chunk_start = frame.header_len - head_at_entry;
                    }
                    self.header_len = 0;
                    self.frame = Some(frame);
                    self.payload_len = carry;
                }
            }
        }

        if let Some(frame) = self.frame {
            let pending = frame.len as usize - self.payload_len;
            if pending > 0 {
                let take = pending.min(bytes.len());
                if frame.ty == FrameType::HEADERS {
                    self.field_section.extend_from_slice(&bytes[..take]);
                }
                self.payload_len += take;
                taken += take;
                if self.payload_len < frame.len as usize {
                    if frame.ty == FrameType::DATA && self.payload_len > payload_at_entry {
                        return Ok((
                            RequestFrameRead::Data {
                                chunk: &input[chunk_start
                                    ..chunk_start + (self.payload_len - payload_at_entry)],
                                completed: false,
                            },
                            taken - past,
                        ));
                    }
                    return Ok((RequestFrameRead::Incomplete, taken));
                }
            }
            // The frame's entire payload has been consumed.
            if let Some(next) = self.pending_phase.take() {
                self.phase = next;
            }
            self.frame = None;
            self.payload_len = 0;
            if frame.ty == FrameType::HEADERS {
                let section = std::mem::take(&mut self.field_section);
                return Ok((RequestFrameRead::Headers(section), taken - past));
            }
            if frame.ty == FrameType::DATA && frame.len > 0 {
                return Ok((
                    RequestFrameRead::Data {
                        chunk: &input
                            [chunk_start..chunk_start + (frame.len as usize - payload_at_entry)],
                        completed: true,
                    },
                    taken - past,
                ));
            }
            return Ok((RequestFrameRead::Drained(frame.ty, frame.len), taken - past));
        }

        // Unreachable: the header phase above returns or sets `frame`.
        Ok((RequestFrameRead::Incomplete, taken))
    }
}

impl Default for RequestFrameReader {
    fn default() -> Self {
        Self::new()
    }
}
