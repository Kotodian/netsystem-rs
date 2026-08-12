//! HTTP/3 frames (RFC 9114 Section 7): frame header parse with VPP stream-type
//! validation, frame encode/decode, and the SETTINGS frame.
//!
//! Adapted from `third_party/h3/h3/src/proto/frame.rs` and
//! `third_party/vpp/src/plugins/http/http3/{frame.h,frame.c}`.

use std::fmt;

use bytes::{Buf, BufMut, Bytes};

use super::coding::{BufExt, Decode, Encode};
use super::error::ErrorCode;
use super::push::{InvalidPushId, PushId};
use super::varint::{UnexpectedEnd, VarInt, VarIntBoundsExceeded};

/// A frame type (RFC 9114 Section 7.2).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct FrameType(VarInt);

impl FrameType {
    pub const DATA: FrameType = FrameType(VarInt::from_u32(0x0));
    pub const HEADERS: FrameType = FrameType(VarInt::from_u32(0x1));
    pub const CANCEL_PUSH: FrameType = FrameType(VarInt::from_u32(0x3));
    pub const SETTINGS: FrameType = FrameType(VarInt::from_u32(0x4));
    pub const PUSH_PROMISE: FrameType = FrameType(VarInt::from_u32(0x5));
    pub const GOAWAY: FrameType = FrameType(VarInt::from_u32(0x7));
    pub const MAX_PUSH_ID: FrameType = FrameType(VarInt::from_u32(0xd));
    /// HTTP/2 frame types that are reserved in HTTP/3 (RFC 9114 Section 7.2.8)
    /// and must be rejected with H3_FRAME_UNEXPECTED.
    pub const H2_PRIORITY: FrameType = FrameType(VarInt::from_u32(0x2));
    pub const H2_PING: FrameType = FrameType(VarInt::from_u32(0x6));
    pub const H2_WINDOW_UPDATE: FrameType = FrameType(VarInt::from_u32(0x8));
    pub const H2_CONTINUATION: FrameType = FrameType(VarInt::from_u32(0x9));

    /// The raw frame type value.
    pub const fn value(&self) -> u64 {
        self.0.into_inner()
    }

    /// Build a `FrameType` from a value, failing if it exceeds the varint range.
    pub fn from_value(value: u64) -> Result<Self, VarIntBoundsExceeded> {
        Ok(FrameType(VarInt::from_u64(value)?))
    }

    /// Whether this is a reserved HTTP/2 frame type (RFC 9114 Section 7.2.8).
    pub fn is_reserved_h2(&self) -> bool {
        matches!(self.value(), 0x02 | 0x06 | 0x08 | 0x09)
    }
}

impl Decode for FrameType {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, UnexpectedEnd> {
        Ok(FrameType(VarInt::decode(buf)?))
    }
}

impl Encode for FrameType {
    fn encode<W: BufMut>(&self, buf: &mut W) {
        self.0.encode(buf);
    }
}

impl fmt::Display for FrameType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.value() {
            0x00 => "DATA",
            0x01 => "HEADERS",
            0x03 => "CANCEL_PUSH",
            0x04 => "SETTINGS",
            0x05 => "PUSH_PROMISE",
            0x07 => "GOAWAY",
            0x0d => "MAX_PUSH_ID",
            0x02 => "H2_PRIORITY (reserved)",
            0x06 => "H2_PING (reserved)",
            0x08 => "H2_WINDOW_UPDATE (reserved)",
            0x09 => "H2_CONTINUATION (reserved)",
            other => return write!(f, "frame type {:#x}", other),
        };
        f.write_str(name)
    }
}

/// The kind of stream a frame arrives on, used for the per-stream frame-type
/// table (RFC 9114 Section 7.2 / VPP `frame.h`).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FrameStream {
    /// The connection control stream (uni type 0x00).
    Control,
    /// A client-initiated bidi stream carrying requests and responses.
    Request,
    /// A server-initiated uni push stream (type 0x01).
    Push,
}

/// A parsed frame header: type, payload length, and the number of bytes the
/// header itself consumed.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FrameHeader {
    pub ty: FrameType,
    pub len: u64,
    pub header_len: usize,
}

impl FrameHeader {
    /// Parse and validate a frame header from the head of `buf`, mirroring
    /// VPP `http3_frame_header_read` (frame.c): known types are checked
    /// against the per-stream table, reserved HTTP/2 types are rejected, and
    /// unknown types are tolerated so they can be drained.
    pub fn parse<B: Buf>(buf: &mut B, stream: FrameStream) -> Result<FrameHeader, FrameError> {
        let remaining = buf.remaining();
        let ty = FrameType::decode(buf).map_err(|_| FrameError::Incomplete(remaining + 1))?;
        let len = buf.get_var().map_err(|_| FrameError::Incomplete(remaining + 1))?;
        let header_len = remaining - buf.remaining();
        let header = FrameHeader { ty, len, header_len };
        header.validate(stream)?;
        Ok(header)
    }

    /// Per-stream frame-type validation, ported from the table in VPP
    /// `third_party/vpp/src/plugins/http/http3/frame.h`:
    /// DATA/HEADERS on request+push, PUSH_PROMISE on request, and
    /// CANCEL_PUSH/SETTINGS/GOAWAY/MAX_PUSH_ID on the control stream.
    pub fn validate(&self, stream: FrameStream) -> Result<(), FrameError> {
        if self.ty.is_reserved_h2() {
            return Err(FrameError::UnsupportedFrame(self.ty));
        }
        let allowed = match self.ty.value() {
            0x00 | 0x01 => matches!(stream, FrameStream::Request | FrameStream::Push),
            0x03 | 0x04 | 0x07 | 0x0d => stream == FrameStream::Control,
            0x05 => stream == FrameStream::Request,
            // unknown frame types are tolerated on any stream
            _ => return Ok(()),
        };
        if allowed {
            Ok(())
        } else {
            Err(FrameError::FrameUnexpected(self.ty, stream))
        }
    }
}

/// A decoded HTTP/3 frame. `Data` carries only the payload length: the caller
/// reads the payload directly from the stream, and `Headers`/`PushPromise`
/// hold the encoded field section (QPACK) as bytes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Frame {
    Data(u64),
    Headers(Bytes),
    CancelPush(PushId),
    Settings(Settings),
    PushPromise(PushPromise),
    Goaway(VarInt),
    MaxPushId(PushId),
    Unknown(FrameType),
}

impl Frame {
    /// Decode a complete frame from the head of `buf`, consuming it. An
    /// unknown frame type consumes its payload and reports
    /// `FrameError::UnknownFrame`, mirroring both h3 and VPP's drain of
    /// unknown frames.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Frame, FrameError> {
        let remaining = buf.remaining();
        let ty = FrameType::decode(buf).map_err(|_| FrameError::Incomplete(remaining + 1))?;
        let len = buf.get_var().map_err(|_| FrameError::Incomplete(remaining + 1))?;

        // DATA payloads are streamed, not copied: only the length is decoded.
        if ty == FrameType::DATA {
            return Ok(Frame::Data(len));
        }

        if ty.is_reserved_h2() {
            return Err(FrameError::UnsupportedFrame(ty));
        }

        if buf.remaining() < len as usize {
            return Err(FrameError::Incomplete(2 + len as usize));
        }

        let mut payload = buf.take(len as usize);
        let frame = match ty {
            FrameType::HEADERS => Frame::Headers(payload.copy_to_bytes(len as usize)),
            FrameType::CANCEL_PUSH => {
                let id = payload.get_var().map_err(|_| FrameError::Malformed)?;
                if payload.has_remaining() {
                    return Err(FrameError::Malformed);
                }
                Frame::CancelPush(PushId::try_from(id).map_err(FrameError::InvalidPushId)?)
            }
            FrameType::SETTINGS => {
                Frame::Settings(Settings::decode(&mut payload).map_err(FrameError::Settings)?)
            }
            FrameType::PUSH_PROMISE => {
                Frame::PushPromise(PushPromise::decode(&mut payload).map_err(|_| FrameError::Malformed)?)
            }
            FrameType::GOAWAY => {
                let id = payload.get_var().map_err(|_| FrameError::Malformed)?;
                // the payload must hold exactly one varint (VPP: FRAME_ERROR)
                if payload.has_remaining() {
                    return Err(FrameError::Malformed);
                }
                Frame::Goaway(VarInt::from_u64(id).map_err(|_| FrameError::Malformed)?)
            }
            FrameType::MAX_PUSH_ID => {
                let id = payload.get_var().map_err(|_| FrameError::Malformed)?;
                if payload.has_remaining() {
                    return Err(FrameError::Malformed);
                }
                Frame::MaxPushId(PushId::try_from(id).map_err(FrameError::InvalidPushId)?)
            }
            // unknown frame type: consume and ignore the payload
            _ => {
                buf.advance(len as usize);
                return Err(FrameError::UnknownFrame(ty));
            }
        };
        Ok(frame)
    }

    /// Encode the frame, returning `InvalidFrameValue` for values that cannot
    /// be represented on the wire. `Data` writes only the header; the caller
    /// writes the payload after it.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), FrameError> {
        match self {
            Frame::Data(len) => {
                let len = VarInt::from_u64(*len).map_err(|_| FrameError::InvalidFrameValue)?;
                FrameType::DATA.encode(buf);
                len.encode(buf);
                Ok(())
            }
            Frame::Headers(payload) => {
                let len =
                    VarInt::from_u64(payload.len() as u64).map_err(|_| FrameError::InvalidFrameValue)?;
                FrameType::HEADERS.encode(buf);
                len.encode(buf);
                buf.put_slice(payload);
                Ok(())
            }
            Frame::Settings(settings) => settings.encode(buf).map_err(FrameError::Settings),
            Frame::CancelPush(id) => simple_frame(FrameType::CANCEL_PUSH, VarInt::from(*id), buf),
            Frame::PushPromise(push) => push.encode(buf),
            Frame::Goaway(id) => simple_frame(FrameType::GOAWAY, *id, buf),
            Frame::MaxPushId(id) => simple_frame(FrameType::MAX_PUSH_ID, VarInt::from(*id), buf),
            Frame::Unknown(ty) => Err(FrameError::UnknownFrame(*ty)),
        }
    }
}

/// Encode a frame whose payload is a single varint.
fn simple_frame<B: BufMut>(ty: FrameType, id: VarInt, buf: &mut B) -> Result<(), FrameError> {
    ty.encode(buf);
    let len = VarInt::from_u64(id.size() as u64).map_err(|_| FrameError::InvalidFrameValue)?;
    len.encode(buf);
    id.encode(buf);
    Ok(())
}

/// A setting identifier (RFC 9114 Section 7.2.4.1).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SettingId(VarInt);

impl SettingId {
    pub const QPACK_MAX_TABLE_CAPACITY: SettingId = SettingId(VarInt::from_u32(0x1));
    pub const MAX_FIELD_SECTION_SIZE: SettingId = SettingId(VarInt::from_u32(0x6));
    pub const QPACK_BLOCKED_STREAMS: SettingId = SettingId(VarInt::from_u32(0x7));
    pub const ENABLE_CONNECT_PROTOCOL: SettingId = SettingId(VarInt::from_u32(0x8));
    pub const H3_DATAGRAM: SettingId = SettingId(VarInt::from_u32(0x33));

    /// The raw setting identifier value.
    pub const fn value(&self) -> u64 {
        self.0.into_inner()
    }

    /// Build a `SettingId` from a value, failing if it exceeds the varint range.
    pub fn from_value(value: u64) -> Result<Self, VarIntBoundsExceeded> {
        Ok(SettingId(VarInt::from_u64(value)?))
    }

    /// Identifiers reserved in HTTP/2 without an HTTP/3 equivalent must be
    /// rejected with H3_SETTINGS_ERROR (RFC 9114 Section 7.2.4.1).
    pub fn is_forbidden(self) -> bool {
        matches!(self.0.into_inner(), 0x00 | 0x02 | 0x03 | 0x04 | 0x05)
    }

    /// Identifiers this crate understands; everything else is ignored.
    pub fn is_supported(self) -> bool {
        matches!(self.0.into_inner(), 0x01 | 0x06 | 0x07 | 0x08 | 0x33)
    }
}

impl Decode for SettingId {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, UnexpectedEnd> {
        Ok(SettingId(VarInt::decode(buf)?))
    }
}

impl Encode for SettingId {
    fn encode<W: BufMut>(&self, buf: &mut W) {
        self.0.encode(buf);
    }
}

impl fmt::Display for SettingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.value() {
            0x01 => "QPACK_MAX_TABLE_CAPACITY",
            0x06 => "MAX_FIELD_SECTION_SIZE",
            0x07 => "QPACK_BLOCKED_STREAMS",
            0x08 => "ENABLE_CONNECT_PROTOCOL",
            0x33 => "H3_DATAGRAM",
            other => return write!(f, "setting id {:#x}", other),
        };
        f.write_str(name)
    }
}

/// Maximum number of settings a `Settings` frame can hold. The frame is
/// bounded (RFC 9114 permits any number of settings, but a compliant peer
/// only sends the identifiers this crate supports, so this never truncates
/// valid traffic).
const SETTINGS_LEN: usize = 8;

/// The SETTINGS frame payload (RFC 9114 Section 7.2.4), decoded into a fixed
/// array. Values are validated on insert and stored as `VarInt`, so encoding
/// can never panic.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Settings {
    entries: [(SettingId, VarInt); SETTINGS_LEN],
    len: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            entries: [(SettingId(VarInt::from_u32(0)), VarInt::from_u32(0)); SETTINGS_LEN],
            len: 0,
        }
    }
}

impl Settings {
    /// The largest possible encoded size of a `Settings` frame.
    pub const MAX_ENCODED_SIZE: usize = SETTINGS_LEN * 2 * VarInt::MAX_SIZE;

    /// A SETTINGS frame advertising a static-only QPACK table: maximum table
    /// capacity 0 and 0 blocked streams (RFC 9204 Section 3.2.2). This slice
    /// only ever encodes static-table references, so these are the only
    /// values sent.
    pub fn qpack_zero_capacity() -> Result<Settings, SettingsError> {
        let mut settings = Settings::default();
        settings.insert(SettingId::QPACK_MAX_TABLE_CAPACITY, 0)?;
        settings.insert(SettingId::QPACK_BLOCKED_STREAMS, 0)?;
        Ok(settings)
    }

    /// Insert a setting. A duplicate identifier, more than `SETTINGS_LEN`
    /// settings, or an out-of-range value is rejected. Value ranges come from
    /// VPP's `foreach_http3_settings` table (frame.c): boolean settings accept
    /// 0 or 1, everything else the full varint range.
    pub fn insert(&mut self, id: SettingId, value: u64) -> Result<(), SettingsError> {
        if self.len >= self.entries.len() {
            return Err(SettingsError::Exceeded);
        }
        // RFC 9114 Section 7.2.4: the same identifier MUST NOT appear twice.
        if self.entries[..self.len].iter().any(|(i, _)| *i == id) {
            return Err(SettingsError::Repeated(id));
        }
        let (min, max) = match id.value() {
            0x08 | 0x33 => (0, 1),
            _ => (0, VarInt::MAX.into_inner()),
        };
        if value < min || value > max {
            return Err(SettingsError::InvalidSettingValue(id, value));
        }
        let value = VarInt::from_u64(value).map_err(|_| SettingsError::InvalidSettingValue(id, value))?;
        self.entries[self.len] = (id, value);
        self.len += 1;
        Ok(())
    }

    /// The value for a setting, if present.
    pub fn get(&self, id: SettingId) -> Option<u64> {
        for (entry_id, value) in self.entries[..self.len].iter() {
            if id == *entry_id {
                return Some(value.into_inner());
            }
        }
        None
    }

    /// The length of the encoded payload, in bytes.
    fn encoded_len(&self) -> usize {
        self.entries[..self.len]
            .iter()
            .fold(0, |len, (id, value)| len + id.0.size() + value.size())
    }

    /// Encode the SETTINGS frame. Never fails in practice: values were
    /// validated at insert, so the only possible error is a payload length
    /// beyond the varint range, which the bounded capacity excludes.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), SettingsError> {
        FrameType::SETTINGS.encode(buf);
        let len = VarInt::from_u64(self.encoded_len() as u64).map_err(|_| SettingsError::Malformed)?;
        len.encode(buf);
        for (id, value) in self.entries[..self.len].iter() {
            id.encode(buf);
            value.encode(buf);
        }
        Ok(())
    }

    /// Decode a SETTINGS payload: forbidden identifiers are rejected with
    /// H3_SETTINGS_ERROR, supported identifiers range-checked, and unknown
    /// identifiers ignored (RFC 9114 Section 7.2.4.1).
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Settings, SettingsError> {
        let mut settings = Settings::default();
        while buf.has_remaining() {
            if buf.remaining() < 2 {
                // less than two minimal varints remain
                return Err(SettingsError::Malformed);
            }
            let identifier = SettingId::decode(buf).map_err(|_| SettingsError::Malformed)?;
            let value = buf.get_var().map_err(|_| SettingsError::Malformed)?;

            if identifier.is_forbidden() {
                return Err(SettingsError::InvalidSettingId(identifier.value()));
            }
            if identifier.is_supported() {
                settings.insert(identifier, value)?;
            }
            // unknown identifiers are ignored
        }
        Ok(settings)
    }
}

/// A PUSH_PROMISE frame payload (RFC 9114 Section 7.2.6): a push ID followed
/// by the encoded field section of the promised request.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PushPromise {
    id: u64,
    encoded: Bytes,
}

impl PushPromise {
    pub fn new(id: u64, encoded: Bytes) -> PushPromise {
        PushPromise { id, encoded }
    }

    /// The promised push ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The encoded field section.
    pub fn encoded(&self) -> &Bytes {
        &self.encoded
    }

    /// The length of the payload in bytes.
    pub fn len(&self) -> usize {
        self.id_size() + self.encoded.len()
    }

    fn id_size(&self) -> usize {
        VarInt::from_u64(self.id).map_or(0, |id| id.size())
    }

    /// Encode the PUSH_PROMISE frame header (type, length, push ID).
    pub fn encode_header<B: BufMut>(&self, buf: &mut B) -> Result<(), FrameError> {
        let id = VarInt::try_from(self.id).map_err(|_| FrameError::InvalidFrameValue)?;
        FrameType::PUSH_PROMISE.encode(buf);
        let len = VarInt::from_u64(self.len() as u64).map_err(|_| FrameError::InvalidFrameValue)?;
        len.encode(buf);
        id.encode(buf);
        Ok(())
    }

    /// Encode the full PUSH_PROMISE frame.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), FrameError> {
        self.encode_header(buf)?;
        buf.put_slice(&self.encoded);
        Ok(())
    }

    /// Decode a PUSH_PROMISE payload: the push ID followed by the rest as the
    /// encoded field section.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<PushPromise, UnexpectedEnd> {
        let id = buf.get_var()?;
        let encoded = buf.copy_to_bytes(buf.remaining());
        Ok(PushPromise { id, encoded })
    }
}

/// Errors while decoding or encoding frames and settings. `error_code`
/// maps each to the connection error code to send (RFC 9114 Section 8.1).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FrameError {
    /// The frame payload violates layout requirements (H3_FRAME_ERROR).
    Malformed,
    /// A reserved HTTP/2 frame type was received (H3_FRAME_UNEXPECTED).
    UnsupportedFrame(FrameType),
    /// A known frame type appeared on a stream that does not allow it
    /// (H3_FRAME_UNEXPECTED).
    FrameUnexpected(FrameType, FrameStream),
    /// An unknown frame type was received and drained.
    UnknownFrame(FrameType),
    /// A value could not be represented in the frame (H3_FRAME_ERROR).
    InvalidFrameValue,
    /// The input ended mid-frame; `usize` is the total number of bytes needed.
    Incomplete(usize),
    /// An error in a SETTINGS payload (H3_FRAME_ERROR if malformed,
    /// H3_SETTINGS_ERROR otherwise).
    Settings(SettingsError),
    /// An invalid push ID in CANCEL_PUSH or MAX_PUSH_ID (H3_ID_ERROR).
    InvalidPushId(InvalidPushId),
}

impl FrameError {
    /// The connection error code to send, or `None` for internal errors that
    /// never leave this crate (e.g. incomplete input awaiting more data).
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            FrameError::Malformed | FrameError::InvalidFrameValue => Some(ErrorCode::FrameError),
            FrameError::UnsupportedFrame(_) | FrameError::FrameUnexpected(_, _) => {
                Some(ErrorCode::FrameUnexpected)
            }
            FrameError::UnknownFrame(_) | FrameError::Incomplete(_) => None,
            FrameError::Settings(e) => Some(e.error_code()),
            FrameError::InvalidPushId(_) => Some(ErrorCode::IdError),
        }
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Malformed => write!(f, "malformed frame"),
            FrameError::UnsupportedFrame(ty) => write!(f, "unsupported frame: {}", ty),
            FrameError::FrameUnexpected(ty, stream) => {
                write!(f, "unexpected {} on {:?} stream", ty, stream)
            }
            FrameError::UnknownFrame(ty) => write!(f, "unknown {}", ty),
            FrameError::InvalidFrameValue => write!(f, "frame value out of range"),
            FrameError::Incomplete(n) => write!(f, "incomplete frame, need {} bytes", n),
            FrameError::Settings(e) => write!(f, "settings error: {}", e),
            FrameError::InvalidPushId(e) => write!(f, "{}", e),
        }
    }
}

/// Errors while parsing a SETTINGS frame payload (RFC 9114 Section 7.2.4).
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SettingsError {
    /// The payload is truncated or otherwise unparsable.
    Malformed,
    /// The same setting identifier appeared twice.
    Repeated(SettingId),
    /// More settings than the fixed capacity.
    Exceeded,
    /// A reserved HTTP/2 setting identifier was received.
    InvalidSettingId(u64),
    /// A setting value outside the allowed range (VPP `foreach_http3_settings`).
    InvalidSettingValue(SettingId, u64),
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SettingsError::Malformed => write!(f, "malformed SETTINGS payload"),
            SettingsError::Repeated(id) => write!(f, "duplicate setting {}", id),
            SettingsError::Exceeded => write!(f, "too many settings"),
            SettingsError::InvalidSettingId(id) => write!(f, "reserved setting id {:#x}", id),
            SettingsError::InvalidSettingValue(id, value) => {
                write!(f, "invalid value {} for setting {}", value, id)
            }
        }
    }
}

impl SettingsError {
    /// The connection error code to send (RFC 9114 Sections 7.1, 7.2.5, 8.1).
    /// A payload that is truncated or otherwise unparsable is a malformed
    /// frame (H3_FRAME_ERROR, mirroring VPP `http3_frame_settings_read`);
    /// duplicate, forbidden, or out-of-range entries are semantic SETTINGS
    /// errors (H3_SETTINGS_ERROR).
    pub(crate) fn error_code(&self) -> ErrorCode {
        match self {
            SettingsError::Malformed => ErrorCode::FrameError,
            SettingsError::Repeated(_)
            | SettingsError::Exceeded
            | SettingsError::InvalidSettingId(_)
            | SettingsError::InvalidSettingValue(_, _) => ErrorCode::SettingsError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http3::proto::error::ErrorCode;

    /// Encode a frame and append its payload for `Data` frames.
    fn codec_frame_check(frame: &Frame, wire: &[u8], expected: &Frame) {
        let mut buf = Vec::new();
        frame.encode(&mut buf).expect("encode");
        if let Frame::Data(n) = frame {
            buf.extend_from_slice(&vec![0x30; *n as usize]);
        }
        assert_eq!(&buf, wire);

        let mut read: &[u8] = &buf;
        let decoded = Frame::decode(&mut read).expect("decode");
        assert_eq!(&decoded, expected);
    }

    #[test]
    fn unknown_frame_type() {
        let mut buf: &[u8] = &[22, 4, 0, 255, 128, 0, 3, 1, 2];
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(FrameError::UnknownFrame(t)) if t.value() == 22
        ));
        // payload was consumed; next frame decodes normally
        assert_eq!(Frame::decode(&mut buf), Ok(Frame::CancelPush(PushId::try_from(2).unwrap())));
    }

    #[test]
    fn len_unexpected_end() {
        let mut buf: &[u8] = &[0, 255];
        assert_eq!(Frame::decode(&mut buf), Err(FrameError::Incomplete(3)));
    }

    #[test]
    fn type_unexpected_end() {
        let mut buf: &[u8] = &[255];
        assert_eq!(Frame::decode(&mut buf), Err(FrameError::Incomplete(2)));
    }

    #[test]
    fn truncated_header_single_byte() {
        let mut buf: &[u8] = &[0x00];
        assert_eq!(Frame::decode(&mut buf), Err(FrameError::Incomplete(2)));
    }

    #[test]
    fn buffer_too_short() {
        let mut buf: &[u8] = &[4, 4, 0, 255, 128];
        assert_eq!(Frame::decode(&mut buf), Err(FrameError::Incomplete(6)));
    }

    #[test]
    fn settings_frame() {
        let mut settings = Settings::default();
        settings.insert(SettingId::MAX_FIELD_SECTION_SIZE, 0xfad1).unwrap();
        settings.insert(SettingId::QPACK_MAX_TABLE_CAPACITY, 0xfad2).unwrap();
        settings.insert(SettingId::QPACK_BLOCKED_STREAMS, 0xfad3).unwrap();
        settings.insert(SettingId::from_value(95).unwrap(), 0).unwrap();
        let frame = Frame::Settings(settings);

        let mut expected = Settings::default();
        expected.insert(SettingId::MAX_FIELD_SECTION_SIZE, 0xfad1).unwrap();
        expected.insert(SettingId::QPACK_MAX_TABLE_CAPACITY, 0xfad2).unwrap();
        expected.insert(SettingId::QPACK_BLOCKED_STREAMS, 0xfad3).unwrap();
        codec_frame_check(
            &frame,
            &[
                4, 18, 6, 128, 0, 250, 209, 1, 128, 0, 250, 210, 7, 128, 0, 250, 211, 64, 95, 0,
            ],
            // the unknown setting id 95 is ignored on decode
            &Frame::Settings(expected),
        );
    }

    #[test]
    fn settings_frame_empty() {
        codec_frame_check(&Frame::Settings(Settings::default()), &[4, 0], &Frame::Settings(Settings::default()));
    }

    #[test]
    fn settings_decode_reserved_identifiers() {
        // reserved ids 0x00 and 0x02 must fail with a settings error
        let mut buf: &[u8] = &[4, 4, 0, 0, 2, 0];
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(FrameError::Settings(SettingsError::InvalidSettingId(0)))
        ));
        let mut buf: &[u8] = &[4, 4, 3, 0, 4, 0];
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(FrameError::Settings(SettingsError::InvalidSettingId(3)))
        ));
    }

    #[test]
    fn settings_decode_truncated_payload() {
        // payload holds an id but no value
        let mut buf: &[u8] = &[4, 1, 6];
        assert!(matches!(
            Frame::decode(&mut buf),
            Err(FrameError::Settings(SettingsError::Malformed))
        ));
    }

    #[test]
    fn settings_decode_duplicate_identifier() {
        let mut settings = Settings::default();
        settings.insert(SettingId::MAX_FIELD_SECTION_SIZE, 1).unwrap();
        assert!(matches!(
            settings.insert(SettingId::MAX_FIELD_SECTION_SIZE, 2),
            Err(SettingsError::Repeated(_))
        ));
    }

    #[test]
    fn settings_insert_range_checks() {
        let mut settings = Settings::default();
        // ENABLE_CONNECT_PROTOCOL is a boolean per the VPP settings table
        assert!(matches!(
            settings.insert(SettingId::ENABLE_CONNECT_PROTOCOL, 2),
            Err(SettingsError::InvalidSettingValue(_, 2))
        ));
        assert!(settings.insert(SettingId::ENABLE_CONNECT_PROTOCOL, 1).is_ok());
        // values must fit a varint
        assert!(settings.insert(SettingId::H3_DATAGRAM, 1 << 62).is_err());
    }

    #[test]
    fn settings_insert_overflow() {
        let mut settings = Settings::default();
        for i in 0..8u64 {
            settings.insert(SettingId::from_value(i).unwrap(), 0).unwrap();
        }
        assert!(matches!(
            settings.insert(SettingId::from_value(8).unwrap(), 0),
            Err(SettingsError::Exceeded)
        ));
    }

    #[test]
    fn settings_encode_needs_no_payload_allocation() {
        let mut settings = Settings::default();
        settings.insert(SettingId::MAX_FIELD_SECTION_SIZE, 0xfad1).unwrap();
        let mut buf = Vec::new();
        settings.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![4, 5, 6, 128, 0, 250, 209]);
    }

    #[test]
    fn settings_qpack_zero_capacity_round_trip() {
        let settings = Settings::qpack_zero_capacity().unwrap();
        assert_eq!(settings.get(SettingId::QPACK_MAX_TABLE_CAPACITY), Some(0));
        assert_eq!(settings.get(SettingId::QPACK_BLOCKED_STREAMS), Some(0));
        let mut buf = Vec::new();
        settings.encode(&mut buf).unwrap();
        let mut read: &[u8] = &buf;
        // decode the whole frame: the type byte must be consumed by Frame
        let decoded = match Frame::decode(&mut read).unwrap() {
            Frame::Settings(settings) => settings,
            other => panic!("expected settings frame, got {:?}", other),
        };
        assert_eq!(decoded, settings);
    }

    #[test]
    fn data_frame() {
        codec_frame_check(&Frame::Data(7), &[0, 7, 48, 48, 48, 48, 48, 48, 48], &Frame::Data(7));
    }

    #[test]
    fn simple_frames() {
        codec_frame_check(
            &Frame::CancelPush(PushId::try_from(2).unwrap()),
            &[3, 1, 2],
            &Frame::CancelPush(PushId::try_from(2).unwrap()),
        );
        codec_frame_check(&Frame::Goaway(VarInt::from_u32(2)), &[7, 1, 2], &Frame::Goaway(VarInt::from_u32(2)));
        codec_frame_check(
            &Frame::MaxPushId(PushId::try_from(2).unwrap()),
            &[13, 1, 2],
            &Frame::MaxPushId(PushId::try_from(2).unwrap()),
        );
    }

    #[test]
    fn goaway_payload_must_be_exactly_one_varint() {
        let mut empty: &[u8] = &[7, 0];
        assert_eq!(Frame::decode(&mut empty), Err(FrameError::Malformed));
        let mut extra: &[u8] = &[7, 2, 1, 0];
        assert_eq!(Frame::decode(&mut extra), Err(FrameError::Malformed));
    }

    #[test]
    fn headers_frames() {
        codec_frame_check(
            &Frame::Headers(Bytes::from_static(b"TODO QPACK")),
            &[1, 10, 84, 79, 68, 79, 32, 81, 80, 65, 67, 75],
            &Frame::Headers(Bytes::from_static(b"TODO QPACK")),
        );
        codec_frame_check(
            &Frame::PushPromise(PushPromise::new(134, Bytes::from_static(b"TODO QPACK"))),
            &[5, 12, 64, 134, 84, 79, 68, 79, 32, 81, 80, 65, 67, 75],
            &Frame::PushPromise(PushPromise::new(134, Bytes::from_static(b"TODO QPACK"))),
        );
    }

    #[test]
    fn reserved_frame_types_are_unsupported() {
        for ty in [0x02u64, 0x06, 0x08, 0x09] {
            let mut buf: Vec<u8> = Vec::new();
            VarInt::from_u64(ty).unwrap().encode(&mut buf);
            buf.extend_from_slice(&[0, 0]);
            let mut read: &[u8] = &buf;
            assert!(matches!(
                Frame::decode(&mut read),
                Err(FrameError::UnsupportedFrame(t)) if t.value() == ty
            ));
        }
    }

    #[test]
    fn grease_like_unknown_frame() {
        let mut raw = Vec::new();
        VarInt::from_u32(0x21 + 2 * 0x1f).encode(&mut raw);
        raw.extend_from_slice(&[6, 0, 255, 128, 0, 250, 218]);
        let mut read: &[u8] = &raw;
        assert!(matches!(
            Frame::decode(&mut read),
            Err(FrameError::UnknownFrame(t)) if t.value() == 95
        ));
    }

    #[test]
    fn frame_header_parse_and_validate() {
        // SETTINGS on the control stream is fine
        let mut buf: &[u8] = &[4, 0];
        let fh = FrameHeader::parse(&mut buf, FrameStream::Control).unwrap();
        assert_eq!(fh.ty, FrameType::SETTINGS);
        assert_eq!(fh.len, 0);
        assert_eq!(fh.header_len, 2);

        // known frames on the wrong stream: FRAME_UNEXPECTED semantics
        let mut buf: &[u8] = &[4, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Request),
            Err(FrameError::FrameUnexpected(t, FrameStream::Request)) if t.value() == 0x04
        ));
        let mut buf: &[u8] = &[0, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Control),
            Err(FrameError::FrameUnexpected(t, FrameStream::Control)) if t.value() == 0x00
        ));
        let mut buf: &[u8] = &[1, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Control),
            Err(FrameError::FrameUnexpected(_, FrameStream::Control))
        ));
        let mut buf: &[u8] = &[7, 1, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Request),
            Err(FrameError::FrameUnexpected(_, FrameStream::Request))
        ));
        let mut buf: &[u8] = &[13, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Request),
            Err(FrameError::FrameUnexpected(_, FrameStream::Request))
        ));

        // unknown frame types are tolerated on any stream
        let mut buf: &[u8] = &[22, 4];
        assert!(FrameHeader::parse(&mut buf, FrameStream::Request).is_ok());
        let mut buf: &[u8] = &[22, 4];
        assert!(FrameHeader::parse(&mut buf, FrameStream::Control).is_ok());

        // reserved H2 frame types are rejected on any stream
        let mut buf: &[u8] = &[2, 0];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Control),
            Err(FrameError::UnsupportedFrame(_))
        ));

        // truncated header
        let mut buf: &[u8] = &[4];
        assert!(matches!(
            FrameHeader::parse(&mut buf, FrameStream::Control),
            Err(FrameError::Incomplete(_))
        ));
    }

    #[test]
    fn multi_byte_length_header() {
        let mut buf: &[u8] = &[7, 0x40, 0x41, 1, 2];
        let fh = FrameHeader::parse(&mut buf, FrameStream::Control).unwrap();
        assert_eq!(fh.ty, FrameType::GOAWAY);
        assert_eq!(fh.len, 0x41);
        assert_eq!(fh.header_len, 3);
    }

    #[test]
    fn data_length_beyond_varint_bounds_rejected_on_encode() {
        assert!(matches!(
            Frame::Data(1 << 62).encode(&mut Vec::new()),
            Err(FrameError::InvalidFrameValue)
        ));
    }

    #[test]
    fn encode_unknown_frame_is_an_error() {
        let unknown = Frame::Unknown(FrameType::from_value(22).unwrap());
        assert!(matches!(unknown.encode(&mut Vec::new()), Err(_)));
    }

    #[test]
    fn frame_error_maps_to_connection_error_codes() {
        assert_eq!(
            FrameError::Malformed.error_code(),
            Some(ErrorCode::FrameError)
        );
        assert_eq!(
            FrameError::UnsupportedFrame(FrameType::H2_PING).error_code(),
            Some(ErrorCode::FrameUnexpected)
        );
        assert_eq!(
            FrameError::FrameUnexpected(FrameType::SETTINGS, FrameStream::Request).error_code(),
            Some(ErrorCode::FrameUnexpected)
        );
        assert_eq!(FrameError::Incomplete(2).error_code(), None);
        // malformed SETTINGS encoding is a frame error (RFC 9114 7.1)
        assert_eq!(
            FrameError::Settings(SettingsError::Malformed).error_code(),
            Some(ErrorCode::FrameError)
        );
        // semantic SETTINGS errors keep H3_SETTINGS_ERROR
        assert_eq!(
            FrameError::Settings(SettingsError::Repeated(SettingId::MAX_FIELD_SECTION_SIZE))
                .error_code(),
            Some(ErrorCode::SettingsError)
        );
    }

    #[test]
    fn frame_type_constants() {
        assert_eq!(FrameType::DATA.value(), 0x0);
        assert_eq!(FrameType::HEADERS.value(), 0x1);
        assert_eq!(FrameType::CANCEL_PUSH.value(), 0x3);
        assert_eq!(FrameType::SETTINGS.value(), 0x4);
        assert_eq!(FrameType::PUSH_PROMISE.value(), 0x5);
        assert_eq!(FrameType::GOAWAY.value(), 0x7);
        assert_eq!(FrameType::MAX_PUSH_ID.value(), 0xd);
    }
}
