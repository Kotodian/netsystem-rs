//! Control stream state (RFC 9114 Section 6.2.1): the first frame on the
//! control stream must be SETTINGS; a second SETTINGS frame is a connection
//! error.
//!
//! Mirrors VPP `HTTP_CONN_F_EXPECT_PEER_SETTINGS` in
//! `third_party/vpp/src/plugins/http/http3/http3.c` (`http3_stream_read_settings`
//! ~1548 and the control-stream frame loop ~1620).

use super::error::ErrorCode;
use super::frame::FrameType;

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
        ControlStreamState { expect_settings: true }
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

/// Control-stream ordering errors (RFC 9114 Section 6.2.1).
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ControlStreamError {
    /// The first frame on the control stream was not SETTINGS.
    MissingSettings,
    /// A second SETTINGS frame was received.
    DuplicateSettings,
}

impl ControlStreamError {
    /// The connection error code to send.
    pub const fn error_code(self) -> ErrorCode {
        match self {
            ControlStreamError::MissingSettings => ErrorCode::MissingSettings,
            ControlStreamError::DuplicateSettings => ErrorCode::FrameUnexpected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http3::proto::error::ErrorCode;

    #[test]
    fn first_frame_must_be_settings() {
        let mut state = ControlStreamState::new();
        assert_eq!(state.on_frame(FrameType::DATA), Err(ControlStreamError::MissingSettings));
        let mut state = ControlStreamState::new();
        assert_eq!(state.on_frame(FrameType::HEADERS), Err(ControlStreamError::MissingSettings));
        let mut state = ControlStreamState::new();
        assert_eq!(state.on_frame(FrameType::GOAWAY), Err(ControlStreamError::MissingSettings));
        let mut state = ControlStreamState::new();
        assert_eq!(state.on_frame(FrameType::MAX_PUSH_ID), Err(ControlStreamError::MissingSettings));
    }

    #[test]
    fn settings_then_any_frame_is_fine() {
        let mut state = ControlStreamState::new();
        assert!(state.on_frame(FrameType::SETTINGS).is_ok());
        assert!(state.on_frame(FrameType::GOAWAY).is_ok());
        assert!(state.on_frame(FrameType::MAX_PUSH_ID).is_ok());
        assert!(state.on_frame(FrameType::SETTINGS).is_err());
    }

    #[test]
    fn second_settings_frame_is_a_connection_error() {
        let mut state = ControlStreamState::new();
        state.on_frame(FrameType::SETTINGS).unwrap();
        assert_eq!(state.on_frame(FrameType::SETTINGS), Err(ControlStreamError::DuplicateSettings));
    }

    #[test]
    fn errors_map_to_connection_error_codes() {
        assert_eq!(ControlStreamError::MissingSettings.error_code(), ErrorCode::MissingSettings);
        assert_eq!(ControlStreamError::DuplicateSettings.error_code(), ErrorCode::FrameUnexpected);
    }

    #[test]
    fn default_state_also_expects_settings() {
        let mut state = ControlStreamState::default();
        assert_eq!(state.on_frame(FrameType::DATA), Err(ControlStreamError::MissingSettings));
    }
}
