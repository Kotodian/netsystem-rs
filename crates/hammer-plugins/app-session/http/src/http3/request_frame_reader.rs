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
                                chunk: &input
                                    [chunk_start..chunk_start + (self.payload_len - payload_at_entry)],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http3::proto::error::ErrorCode;
    use crate::http3::proto::frame::FrameType;

    /// Encode a QUIC variable-length integer (RFC 9000 Section 16).
    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        if value < 1 << 6 {
            out.push(value as u8);
        } else if value < 1 << 14 {
            out.push(0x40 | (value >> 8) as u8);
            out.push(value as u8);
        } else if value < 1 << 30 {
            out.push(0x80 | (value >> 24) as u8);
            out.push((value >> 16) as u8);
            out.push((value >> 8) as u8);
            out.push(value as u8);
        } else {
            out.push(0xc0 | (value >> 56) as u8);
            for shift in (0..7).rev() {
                out.push((value >> (8 * shift)) as u8);
            }
        }
        out
    }

    /// Encode a frame: type and payload-length varints plus the payload.
    fn frame(ty: u64, len: u64, payload: &[u8]) -> Vec<u8> {
        let mut wire = varint(ty);
        wire.extend(varint(len));
        wire.extend_from_slice(payload);
        wire
    }

    /// Push `wire` one byte at a time, asserting every intermediate result is
    /// `Incomplete` with exactly one byte consumed, and return the first
    /// completed frame outcome with the total bytes consumed.
    fn feed_bytewise<'a>(
        reader: &mut RequestFrameReader,
        wire: &'a [u8],
    ) -> (RequestFrameRead<'a>, usize) {
        let mut consumed = 0;
        for i in 0..wire.len() {
            let (read, n) = reader.push(&wire[i..i + 1]).expect("a partial frame must not error");
            consumed += n;
            if !matches!(read, RequestFrameRead::Incomplete) {
                return (read, consumed);
            }
            assert_eq!(n, 1, "partial-frame calls consume exactly one byte");
        }
        (RequestFrameRead::Incomplete, consumed)
    }

    /// The connection error code produced by feeding `wire` to a fresh
    /// reader, which must reject the frame.
    fn error_code_on(wire: &[u8]) -> ErrorCode {
        let mut reader = RequestFrameReader::new();
        match reader.push(wire) {
            Err(e) => e.error_code(),
            Ok((read, _)) => panic!("expected an error, got {:?}", read),
        }
    }

    /// A HEADERS header whose payload length is a two-byte varint, split
    /// bytewise across calls, with the payload arriving over later calls.
    #[test]
    fn bytewise_header_split_including_multibyte_varint() {
        let mut reader = RequestFrameReader::new();
        // HEADERS, payload length 300 (two-byte varint 0x41 0x2c).
        let wire = &[0x01, 0x41, 0x2c];
        let (read, consumed) = feed_bytewise(&mut reader, wire);
        assert_eq!(read, RequestFrameRead::Incomplete);
        assert_eq!(consumed, 3);

        let payload: Vec<u8> = (0..300).map(|i| i as u8).collect();
        let (read, consumed) = feed_bytewise(&mut reader, &payload[..100]);
        assert_eq!(read, RequestFrameRead::Incomplete);
        assert_eq!(consumed, 100);
        let (read, consumed) = feed_bytewise(&mut reader, &payload[100..]);
        // `read` borrows `payload` (the helper returns the completing call's
        // outcome), so extract its section before moving the payload.
        let section = match read {
            RequestFrameRead::Headers(section) => section,
            _ => panic!("expected a completed HEADERS frame"),
        };
        assert_eq!(section, payload);
        assert_eq!(consumed, 200);
    }

    /// A HEADERS payload split across calls, with the next frame's bytes
    /// arriving alongside the last payload byte: exactly the payload is
    /// consumed and the trailing bytes are preserved for the next call.
    #[test]
    fn partial_headers_payload_then_trailing_bytes_exact_consumed() {
        let mut reader = RequestFrameReader::new();
        // HEADERS, length 4; three payload bytes arrive with the header.
        assert_eq!(
            reader.push(&[0x01, 0x04, b'a', b'b', b'c']),
            Ok((RequestFrameRead::Incomplete, 5))
        );
        // The last payload byte plus the start of the next frame.
        assert_eq!(
            reader.push(&[b'd', 0x00, 0x00]),
            Ok((RequestFrameRead::Headers(b"abcd".to_vec()), 1))
        );
        // The two trailing bytes were preserved for the next call.
        assert_eq!(
            reader.push(&[0x00, 0x00]),
            Ok((RequestFrameRead::Drained(FrameType::DATA, 0), 2))
        );
    }

    /// Frames forbidden before the initial HEADERS map to
    /// H3_FRAME_UNEXPECTED through the existing rules (`FrameHeader::parse`
    /// validation and `RequestPhase::on_frame`).
    #[test]
    fn forbidden_frames_before_initial_headers() {
        // DATA before the initial HEADERS is a request-stream ordering error
        // from the phase machine (RFC 9114 Section 4.1).
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x00, 0x00]),
            Err(RequestFrameError::Phase(ErrorCode::FrameUnexpected))
        );
        // Control-stream-only frames (RFC 9114 Sections 7.2.3-7.2.7).
        for ty in [0x03, 0x04, 0x07, 0x0d] {
            assert_eq!(error_code_on(&frame(ty, 0, &[])), ErrorCode::FrameUnexpected);
        }
        // PUSH_PROMISE is forbidden on a client request stream (7.2.5).
        assert_eq!(error_code_on(&frame(0x05, 0, &[])), ErrorCode::FrameUnexpected);
        // Reserved HTTP/2 frame types (7.2.8).
        for ty in [0x02, 0x06, 0x08, 0x09] {
            assert_eq!(error_code_on(&frame(ty, 0, &[])), ErrorCode::FrameUnexpected);
        }
    }

    /// An unknown frame before the initial HEADERS is drained without
    /// allocation and leaves the phase unchanged: a HEADERS frame after it is
    /// still accepted.
    #[test]
    fn unknown_drain_then_headers_across_calls() {
        let mut reader = RequestFrameReader::new();
        // Unknown type 0x2a, payload "xyz", split across two calls.
        assert_eq!(
            reader.push(&[0x2a, 0x03, b'x']),
            Ok((RequestFrameRead::Incomplete, 3))
        );
        assert_eq!(
            reader.push(&[b'y', b'z']),
            Ok((RequestFrameRead::Drained(
                FrameType::from_value(0x2a).unwrap(),
                3
            ), 2))
        );
        // The initial HEADERS is still accepted after the unknown frame.
        assert_eq!(
            reader.push(&[0x01, 0x02, b'a', b'b']),
            Ok((RequestFrameRead::Headers(b"ab".to_vec()), 4))
        );
    }

    /// A field section longer than [`MAX_FIELD_SECTION_LEN`] is rejected
    /// with H3_EXCESSIVE_LOAD from the header alone, before any payload byte
    /// is buffered.
    #[test]
    fn oversized_field_section_rejected_before_allocation() {
        let mut reader = RequestFrameReader::new();
        let len = MAX_FIELD_SECTION_LEN as u64 + 1;
        assert_eq!(
            reader.push(&frame(0x01, len, &[])),
            Err(RequestFrameError::OversizedFieldSection(len))
        );
        assert_eq!(
            RequestFrameError::OversizedFieldSection(len).error_code(),
            ErrorCode::ExcessiveLoad
        );
    }

    /// An empty initial HEADERS frame completes immediately and advances the
    /// phase: DATA frames are accepted afterwards.
    #[test]
    fn zero_length_headers_completes() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        // The phase advanced to the body: DATA is now allowed.
        assert_eq!(
            reader.push(&[0x00, 0x00]),
            Ok((RequestFrameRead::Drained(FrameType::DATA, 0), 2))
        );
    }

    /// A single call may contain a complete frame plus the next frame's
    /// header: only the frame's bytes are consumed.
    #[test]
    fn complete_frame_plus_next_header_exact_consumed() {
        let mut reader = RequestFrameReader::new();
        // HEADERS (length 2) and its payload, plus the start of a DATA frame.
        assert_eq!(
            reader.push(&[0x01, 0x02, b'a', b'b', 0x00, 0x00]),
            Ok((RequestFrameRead::Headers(b"ab".to_vec()), 4))
        );
        assert_eq!(
            reader.push(&[0x00, 0x00]),
            Ok((RequestFrameRead::Drained(FrameType::DATA, 0), 2))
        );
    }

    /// After the initial HEADERS the stream may carry DATA and one trailing
    /// HEADERS; a frame after the trailer is rejected (RFC 9114 Section 4.1).
    #[test]
    fn body_then_trailers_then_error() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        assert_eq!(
            reader.push(&frame(0x00, 2, b"hi")),
            Ok((
                RequestFrameRead::Data {
                    chunk: b"hi",
                    completed: true,
                },
                4
            ))
        );
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        assert_eq!(
            reader.push(&[0x00, 0x00]),
            Err(RequestFrameError::Phase(ErrorCode::FrameUnexpected))
        );
    }

    /// A DATA frame split across calls: each call surfaces the borrowed
    /// payload bytes of that call with `completed` false until the call that
    /// finishes the frame.
    #[test]
    fn data_payload_borrowed_partial_then_complete() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        // DATA, length 10: header plus the first four payload bytes.
        assert_eq!(
            reader.push(&[0x00, 0x0a, b'a', b'b', b'c', b'd']),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"abcd"[..],
                    completed: false,
                },
                6
            ))
        );
        // The remaining six payload bytes complete the frame.
        assert_eq!(
            reader.push(b"efghij"),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"efghij"[..],
                    completed: true,
                },
                6
            ))
        );
    }

    /// A DATA chunk must never include trailing next-frame bytes, and the
    /// consumed count must exclude them: the caller keeps them for the next
    /// call.
    #[test]
    fn data_chunk_excludes_trailing_next_frame_bytes() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        // DATA (length 2) with its payload, plus the next DATA frame's
        // header and payload, in one call.
        assert_eq!(
            reader.push(&[0x00, 0x02, b'h', b'i', 0x00, 0x01, b'x']),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"hi"[..],
                    completed: true,
                },
                4
            ))
        );
        assert_eq!(
            reader.push(&[0x00, 0x01, b'x']),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"x"[..],
                    completed: true,
                },
                3
            ))
        );
    }

    /// DATA payload bytes staged with the header in the fixed 16-byte
    /// staging are surfaced from the input call's borrow, with the exact
    /// consumed count; the header and payload need not be split across
    /// calls.
    #[test]
    fn data_payload_staged_with_header_borrowed_from_input() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        // DATA header and its whole five-byte payload arrive together.
        assert_eq!(
            reader.push(&[0x00, 0x05, b'a', b'b', b'c', b'd', b'e']),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"abcde"[..],
                    completed: true,
                },
                7
            ))
        );
    }

    /// A DATA header split across calls, with the payload starting in the
    /// same call that completes the header: the staged payload bytes are
    /// not lost.
    #[test]
    fn data_header_split_across_calls_then_payload() {
        let mut reader = RequestFrameReader::new();
        assert_eq!(
            reader.push(&[0x01, 0x00]),
            Ok((RequestFrameRead::Headers(Vec::new()), 2))
        );
        // The DATA type byte alone: header still incomplete.
        assert_eq!(reader.push(&[0x00]), Ok((RequestFrameRead::Incomplete, 1)));
        // The length varint plus the first two payload bytes.
        assert_eq!(
            reader.push(&[0x05, b'a', b'b']),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"ab"[..],
                    completed: false,
                },
                3
            ))
        );
        // The remaining payload completes the frame.
        assert_eq!(
            reader.push(b"cde"),
            Ok((
                RequestFrameRead::Data {
                    chunk: &b"cde"[..],
                    completed: true,
                },
                3
            ))
        );
    }

    /// A payload larger than the header staging is drained by counting
    /// across many calls, with no buffering.
    #[test]
    fn unknown_payload_drained_without_allocation() {
        let mut reader = RequestFrameReader::new();
        let payload = vec![0xaa; 1000];
        let wire = frame(0x2a, 1000, &payload);
        let mut seen = 0usize;
        let mut result = None;
        for chunk in wire.chunks(100) {
            let (read, n) = reader.push(chunk).expect("draining must not error");
            seen += n;
            if matches!(read, RequestFrameRead::Incomplete) {
                continue;
            }
            assert_eq!(n, chunk.len(), "the completing call consumes its whole chunk");
            result = Some(read);
            break;
        }
        assert_eq!(
            result,
            Some(RequestFrameRead::Drained(
                FrameType::from_value(0x2a).unwrap(),
                1000
            ))
        );
        assert_eq!(seen, wire.len());
    }
}
