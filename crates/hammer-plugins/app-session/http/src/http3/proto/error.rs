//! HTTP/3 application error codes (RFC 9114 Section 8.1, QPACK error codes
//! from RFC 9204 Section 3.2).
//!
//! Values mirror `foreach_http3_errors` in
//! `third_party/vpp/src/plugins/http/http3/http3.h` and
//! `third_party/h3/h3/src/error/codes.rs`.

use std::fmt;

/// An HTTP/3 application error code, sent on the QUIC connection when the
/// connection is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Datagram or capsule parse error (RFC 9297).
    DatagramError,
    /// No error: close without signaling an error.
    NoError,
    /// Peer violated the protocol without a more specific code.
    GeneralProtocolError,
    /// Internal error in the HTTP stack.
    InternalError,
    /// Peer created a stream that will not be accepted.
    StreamCreationError,
    /// A stream required by the connection was closed or reset.
    ClosedCriticalStream,
    /// A frame was received that is not permitted in the current state or on
    /// the current stream.
    FrameUnexpected,
    /// A frame that fails layout requirements or has an invalid size.
    FrameError,
    /// Peer behavior that might generate excessive load.
    ExcessiveLoad,
    /// A Stream ID or Push ID used incorrectly.
    IdError,
    /// An error in the payload of a SETTINGS frame.
    SettingsError,
    /// No SETTINGS frame received at the start of the control stream.
    MissingSettings,
    /// A server rejected a request without application processing.
    RequestRejected,
    /// The request or its response is cancelled.
    RequestCancelled,
    /// The stream terminated without a fully-formed request.
    RequestIncomplete,
    /// An HTTP message is malformed.
    MessageError,
    /// The TCP connection for a CONNECT request was reset.
    ConnectError,
    /// The request cannot be served over HTTP/3.
    VersionFallback,
    /// The decoder failed to interpret an encoded field section.
    QpackDecompressionFailed,
    /// The decoder failed to interpret an encoder instruction.
    QpackEncoderStreamError,
    /// The encoder failed to interpret a decoder instruction.
    QpackDecoderStreamError,
}

impl ErrorCode {
    /// Numerical value per RFC 9114 Section 8.1 / RFC 9204 Section 3.2.
    pub const fn value(self) -> u64 {
        match self {
            ErrorCode::DatagramError => 0x0033,
            ErrorCode::NoError => 0x0100,
            ErrorCode::GeneralProtocolError => 0x0101,
            ErrorCode::InternalError => 0x0102,
            ErrorCode::StreamCreationError => 0x0103,
            ErrorCode::ClosedCriticalStream => 0x0104,
            ErrorCode::FrameUnexpected => 0x0105,
            ErrorCode::FrameError => 0x0106,
            ErrorCode::ExcessiveLoad => 0x0107,
            ErrorCode::IdError => 0x0108,
            ErrorCode::SettingsError => 0x0109,
            ErrorCode::MissingSettings => 0x010a,
            ErrorCode::RequestRejected => 0x010b,
            ErrorCode::RequestCancelled => 0x010c,
            ErrorCode::RequestIncomplete => 0x010d,
            ErrorCode::MessageError => 0x010e,
            ErrorCode::ConnectError => 0x010f,
            ErrorCode::VersionFallback => 0x0110,
            ErrorCode::QpackDecompressionFailed => 0x0200,
            ErrorCode::QpackEncoderStreamError => 0x0201,
            ErrorCode::QpackDecoderStreamError => 0x0202,
        }
    }

    /// Look up a code by its numerical value.
    pub fn from_value(value: u64) -> Option<ErrorCode> {
        Some(match value {
            0x0033 => ErrorCode::DatagramError,
            0x0100 => ErrorCode::NoError,
            0x0101 => ErrorCode::GeneralProtocolError,
            0x0102 => ErrorCode::InternalError,
            0x0103 => ErrorCode::StreamCreationError,
            0x0104 => ErrorCode::ClosedCriticalStream,
            0x0105 => ErrorCode::FrameUnexpected,
            0x0106 => ErrorCode::FrameError,
            0x0107 => ErrorCode::ExcessiveLoad,
            0x0108 => ErrorCode::IdError,
            0x0109 => ErrorCode::SettingsError,
            0x010a => ErrorCode::MissingSettings,
            0x010b => ErrorCode::RequestRejected,
            0x010c => ErrorCode::RequestCancelled,
            0x010d => ErrorCode::RequestIncomplete,
            0x010e => ErrorCode::MessageError,
            0x010f => ErrorCode::ConnectError,
            0x0110 => ErrorCode::VersionFallback,
            0x0200 => ErrorCode::QpackDecompressionFailed,
            0x0201 => ErrorCode::QpackEncoderStreamError,
            0x0202 => ErrorCode::QpackDecoderStreamError,
            _ => return None,
        })
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ErrorCode::DatagramError => "DATAGRAM_ERROR",
            ErrorCode::NoError => "NO_ERROR",
            ErrorCode::GeneralProtocolError => "GENERAL_PROTOCOL_ERROR",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::StreamCreationError => "STREAM_CREATION_ERROR",
            ErrorCode::ClosedCriticalStream => "CLOSED_CRITICAL_STREAM",
            ErrorCode::FrameUnexpected => "FRAME_UNEXPECTED",
            ErrorCode::FrameError => "FRAME_ERROR",
            ErrorCode::ExcessiveLoad => "EXCESSIVE_LOAD",
            ErrorCode::IdError => "ID_ERROR",
            ErrorCode::SettingsError => "SETTINGS_ERROR",
            ErrorCode::MissingSettings => "MISSING_SETTINGS",
            ErrorCode::RequestRejected => "REQUEST_REJECTED",
            ErrorCode::RequestCancelled => "REQUEST_CANCELLED",
            ErrorCode::RequestIncomplete => "REQUEST_INCOMPLETE",
            ErrorCode::MessageError => "MESSAGE_ERROR",
            ErrorCode::ConnectError => "CONNECT_ERROR",
            ErrorCode::VersionFallback => "VERSION_FALLBACK",
            ErrorCode::QpackDecompressionFailed => "QPACK_DECOMPRESSION_FAILED",
            ErrorCode::QpackEncoderStreamError => "QPACK_ENCODER_STREAM_ERROR",
            ErrorCode::QpackDecoderStreamError => "QPACK_DECODER_STREAM_ERROR",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::ErrorCode;

    #[test]
    fn values_match_rfc_9114_and_vpp_http3_h() {
        assert_eq!(ErrorCode::DatagramError.value(), 0x0033);
        assert_eq!(ErrorCode::NoError.value(), 0x0100);
        assert_eq!(ErrorCode::GeneralProtocolError.value(), 0x0101);
        assert_eq!(ErrorCode::InternalError.value(), 0x0102);
        assert_eq!(ErrorCode::StreamCreationError.value(), 0x0103);
        assert_eq!(ErrorCode::ClosedCriticalStream.value(), 0x0104);
        assert_eq!(ErrorCode::FrameUnexpected.value(), 0x0105);
        assert_eq!(ErrorCode::FrameError.value(), 0x0106);
        assert_eq!(ErrorCode::ExcessiveLoad.value(), 0x0107);
        assert_eq!(ErrorCode::IdError.value(), 0x0108);
        assert_eq!(ErrorCode::SettingsError.value(), 0x0109);
        assert_eq!(ErrorCode::MissingSettings.value(), 0x010a);
        assert_eq!(ErrorCode::RequestRejected.value(), 0x010b);
        assert_eq!(ErrorCode::RequestCancelled.value(), 0x010c);
        assert_eq!(ErrorCode::RequestIncomplete.value(), 0x010d);
        assert_eq!(ErrorCode::MessageError.value(), 0x010e);
        assert_eq!(ErrorCode::ConnectError.value(), 0x010f);
        assert_eq!(ErrorCode::VersionFallback.value(), 0x0110);
        assert_eq!(ErrorCode::QpackDecompressionFailed.value(), 0x0200);
        assert_eq!(ErrorCode::QpackEncoderStreamError.value(), 0x0201);
        assert_eq!(ErrorCode::QpackDecoderStreamError.value(), 0x0202);
    }

    #[test]
    fn from_value_round_trip() {
        assert_eq!(ErrorCode::from_value(0x0106), Some(ErrorCode::FrameError));
        assert_eq!(
            ErrorCode::from_value(0x0200),
            Some(ErrorCode::QpackDecompressionFailed)
        );
        assert_eq!(ErrorCode::from_value(0x1), None);
        assert_eq!(ErrorCode::from_value(0x7fff_ffff), None);
    }

    #[test]
    fn display_names_follow_vpp_format() {
        assert_eq!(ErrorCode::NoError.to_string(), "NO_ERROR");
        assert_eq!(ErrorCode::MissingSettings.to_string(), "MISSING_SETTINGS");
        assert_eq!(
            ErrorCode::QpackDecompressionFailed.to_string(),
            "QPACK_DECOMPRESSION_FAILED"
        );
    }
}
