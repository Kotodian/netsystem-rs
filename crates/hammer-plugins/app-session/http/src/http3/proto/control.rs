//! Control stream state (RFC 9114 Section 6.2.1): the first frame on the
//! control stream must be SETTINGS; a second SETTINGS frame is a connection
//! error.
//!
//! Mirrors VPP `HTTP_CONN_F_EXPECT_PEER_SETTINGS` in
//! `third_party/vpp/src/plugins/http/http3/http3.c` (`http3_stream_read_settings`
//! ~1548 and the control-stream frame loop ~1620).
//!
//! `ControlStreamReader` is the byte-level companion: a fixed-size,
//! allocation-free incremental reader for the first complete frame on the
//! peer control stream, mirroring VPP `http3_stream_transport_rx_ctrl`
//! (settings-first check) and `http3_stream_read_settings` (payload
//! validation).

use super::error::ErrorCode;
use super::frame::{
    FrameError, FrameHeader, FrameStream, FrameType, SettingId, Settings, SettingsError,
};

/// Maximum bytes of a frame header: two varints of at most 8 bytes each
/// (VPP `HTTP3_FRAME_HEADER_MAX_LEN`).
const MAX_FRAME_HEADER_LEN: usize = 16;

/// Maximum SETTINGS payload bytes the reader buffers: the largest payload
/// the fixed `Settings` array can hold.
const MAX_SETTINGS_PAYLOAD_LEN: usize = Settings::MAX_ENCODED_SIZE;

/// SETTINGS-first validation state for the peer's control stream. The caller
/// owns a `ControlStreamState` per connection and calls `on_frame` for every
/// frame header received on the control stream, mirroring VPP's
/// `HTTP_CONN_F_EXPECT_PEER_SETTINGS` flag: the first frame must be SETTINGS
/// (H3_MISSING_SETTINGS otherwise), and a second SETTINGS frame is
/// H3_FRAME_UNEXPECTED.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ControlStreamState {
    expect_settings: bool,
}

impl ControlStreamState {
    /// A fresh peer control stream: the first frame must be SETTINGS.
    pub fn new() -> Self {
        ControlStreamState {
            expect_settings: true,
        }
    }

    /// Advance the state for a received frame type. Returns the connection
    /// error to signal, if any. After a valid SETTINGS frame, only a second
    /// SETTINGS frame is rejected; the per-stream frame table in
    /// `FrameHeader::validate` handles frame types that do not belong on the
    /// control stream.
    pub fn on_frame(&mut self, frame: FrameType) -> Result<(), ControlStreamError> {
        if self.expect_settings {
            if frame != FrameType::SETTINGS {
                return Err(ControlStreamError::MissingSettings);
            }
            self.expect_settings = false;
        } else if frame == FrameType::SETTINGS {
            return Err(ControlStreamError::DuplicateSettings);
        }
        Ok(())
    }
}

impl Default for ControlStreamState {
    fn default() -> Self {
        Self::new()
    }
}

/// Control-stream errors (RFC 9114 Section 6.2.1). `Frame` and `Settings`
/// wrap the typed errors from frame-header parsing and SETTINGS payload
/// validation; `OversizedPayload` and `QpackNotSupported` are bounds of this
/// implementation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ControlStreamError {
    /// The first frame on the control stream was not SETTINGS.
    MissingSettings,
    /// A second SETTINGS frame was received.
    DuplicateSettings,
    /// A malformed, reserved, or stream-invalid frame header (`FrameError`).
    Frame(FrameError),
    /// A SETTINGS payload error: forbidden, duplicate, malformed, or
    /// out-of-range entry (`SettingsError`).
    Settings(SettingsError),
    /// A SETTINGS payload longer than the reader can buffer
    /// (`Settings::MAX_ENCODED_SIZE` bytes), rejected before buffering.
    OversizedPayload(u64),
    /// A nonzero `QPACK_MAX_TABLE_CAPACITY` or `QPACK_BLOCKED_STREAMS`:
    /// this implementation supports only the static QPACK table (RFC 9204
    /// Section 3.2.2), so the peer setting is rejected instead of
    /// mishandled.
    QpackNotSupported(SettingId, u64),
}

impl ControlStreamError {
    /// The connection error code to send.
    pub fn error_code(self) -> ErrorCode {
        match self {
            ControlStreamError::MissingSettings => ErrorCode::MissingSettings,
            ControlStreamError::DuplicateSettings => ErrorCode::FrameUnexpected,
            ControlStreamError::Frame(e) => e.error_code().unwrap_or(ErrorCode::FrameError),
            ControlStreamError::Settings(e) => e.error_code(),
            // Refused before decoding: the endpoint is being asked to do
            // more work than it can (RFC 9114 7.2.5).
            ControlStreamError::OversizedPayload(_) => ErrorCode::ExcessiveLoad,
            ControlStreamError::QpackNotSupported(..) => ErrorCode::SettingsError,
        }
    }
}

/// The result of feeding bytes to [`ControlStreamReader`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ControlRead {
    /// More bytes are needed; nothing but the reader's own buffers changed.
    Incomplete,
    /// The first complete frame was a valid SETTINGS frame.
    Complete(Settings),
}

/// Fixed-size, allocation-free incremental reader for the first complete
/// frame on the peer control stream (RFC 9114 Section 6.2.1).
///
/// Mirrors VPP `http3_stream_transport_rx_ctrl` (http3.c ~1620): the first
/// frame must be SETTINGS, and a type that cannot appear on a control stream
/// fails header validation first (frame.c), a payload longer than
/// `Settings::MAX_ENCODED_SIZE` bytes is rejected before any of it is
/// buffered, and payload validation reuses `Settings::decode`, so forbidden,
/// duplicate, and malformed entries fail while unknown identifiers are
/// ignored. Like VPP `http3_stream_read_settings` (~1548), an empty SETTINGS
/// frame is accepted. Beyond VPP, a nonzero `QPACK_MAX_TABLE_CAPACITY` or
/// `QPACK_BLOCKED_STREAMS` is rejected because this implementation only
/// supports the static QPACK table.
///
/// The reader is one-shot: after `Complete` it must not be fed again, and it
/// never processes later control frames, closes connections, or classifies
/// streams.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ControlStreamReader {
    header: [u8; MAX_FRAME_HEADER_LEN],
    header_len: usize,
    payload: [u8; MAX_SETTINGS_PAYLOAD_LEN],
    payload_len: usize,
    frame: Option<FrameHeader>,
}

impl ControlStreamReader {
    /// A fresh reader for a peer control stream.
    pub fn new() -> Self {
        ControlStreamReader {
            header: [0; MAX_FRAME_HEADER_LEN],
            header_len: 0,
            payload: [0; MAX_SETTINGS_PAYLOAD_LEN],
            payload_len: 0,
            frame: None,
        }
    }

    /// Feed stream bytes. Returns `Incomplete` until the first complete
    /// frame is buffered, `Complete` once a valid SETTINGS frame has been
    /// parsed, or a typed control error.
    pub fn push(&mut self, mut bytes: &[u8]) -> Result<ControlRead, ControlStreamError> {
        if self.frame.is_none() {
            let take = (MAX_FRAME_HEADER_LEN - self.header_len).min(bytes.len());
            self.header[self.header_len..self.header_len + take].copy_from_slice(&bytes[..take]);
            self.header_len += take;
            bytes = &bytes[take..];

            let mut buf: &[u8] = &self.header[..self.header_len];
            match FrameHeader::parse(&mut buf, FrameStream::Control) {
                Err(FrameError::Incomplete(_)) if self.header_len < MAX_FRAME_HEADER_LEN => {
                    return Ok(ControlRead::Incomplete);
                }
                // 16 bytes must suffice for two varints; anything longer is
                // a malformed header.
                Err(FrameError::Incomplete(_)) => {
                    return Err(ControlStreamError::Frame(FrameError::Malformed));
                }
                Err(e) => return Err(ControlStreamError::Frame(e)),
                Ok(frame) => {
                    if frame.ty != FrameType::SETTINGS {
                        return Err(ControlStreamError::MissingSettings);
                    }
                    if frame.len > MAX_SETTINGS_PAYLOAD_LEN as u64 {
                        return Err(ControlStreamError::OversizedPayload(frame.len));
                    }
                    // Bytes received beyond the header belong to the payload;
                    // cap at the frame length so a next frame's bytes are not
                    // decoded as settings.
                    let carry = (self.header_len - frame.header_len).min(frame.len as usize);
                    self.payload[..carry]
                        .copy_from_slice(&self.header[frame.header_len..frame.header_len + carry]);
                    self.payload_len = carry;
                    self.header_len = 0;
                    self.frame = Some(frame);
                }
            }
        }

        let frame = match self.frame {
            Some(frame) => frame,
            // Unreachable: the header phase above returns or sets `frame`.
            None => return Ok(ControlRead::Incomplete),
        };

        let pending = frame.len as usize - self.payload_len;
        let take = pending.min(bytes.len());
        self.payload[self.payload_len..self.payload_len + take].copy_from_slice(&bytes[..take]);
        self.payload_len += take;

        if self.payload_len < frame.len as usize {
            return Ok(ControlRead::Incomplete);
        }

        let mut buf: &[u8] = &self.payload[..self.payload_len];
        let settings = Settings::decode(&mut buf).map_err(ControlStreamError::Settings)?;
        for id in [
            SettingId::QPACK_MAX_TABLE_CAPACITY,
            SettingId::QPACK_BLOCKED_STREAMS,
        ] {
            if let Some(value) = settings.get(id) {
                if value != 0 {
                    return Err(ControlStreamError::QpackNotSupported(id, value));
                }
            }
        }
        Ok(ControlRead::Complete(settings))
    }
}

impl Default for ControlStreamReader {
    fn default() -> Self {
        Self::new()
    }
}
