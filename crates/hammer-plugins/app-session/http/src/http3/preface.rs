//! Local control-stream preface encoding (RFC 9114 Section 6.2.1): the
//! control stream type varint followed by the SETTINGS frame, the first
//! bytes the endpoint writes on its control stream.
//!
//! Mirrors the preface VPP writes in `http3_conn_init`
//! (`third_party/vpp/src/plugins/http/http3/http3.c`, ~241: stream type
//! first, then the settings frame) via `http3_frame_settings_write`
//! (`third_party/vpp/src/plugins/http/http3/frame.c`, ~152).

use bytes::BufMut;

use crate::http3::proto::coding::Encode;
use crate::http3::proto::frame::{Settings, SettingsError};
use crate::http3::proto::stream::StreamType;

/// Bytes of the encoded local control preface: a 1-byte control stream
/// type varint plus the zero-capacity QPACK SETTINGS frame (2-byte frame
/// header + two 2-byte setting entries).
pub(crate) const CONTROL_PREFACE_LEN: usize = 7;

/// Encode the local control-stream preface into a fixed-size stack buffer:
/// `StreamType::CONTROL` followed by the zero-capacity QPACK SETTINGS
/// frame. O(1) time and space; the only failure is a SETTINGS encoding
/// error, which the bounded entries exclude.
pub(crate) fn encode_control_preface() -> Result<[u8; CONTROL_PREFACE_LEN], SettingsError> {
    let mut buf = [0u8; CONTROL_PREFACE_LEN];
    let mut w = &mut buf[..];
    StreamType::CONTROL.encode(&mut w);
    Settings::qpack_zero_capacity()?.encode(&mut w)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;
    use crate::http3::proto::coding::Decode;
    use crate::http3::proto::frame::Frame;

    /// The exact preface VPP writes for its default settings: stream type
    /// varint 0x00, SETTINGS frame (type 0x04, length 0x04) advertising
    /// QPACK_MAX_TABLE_CAPACITY=0 (0x01 0x00) and QPACK_BLOCKED_STREAMS=0
    /// (0x07 0x00).
    #[test]
    fn control_preface_golden_bytes() {
        assert_eq!(
            encode_control_preface().unwrap(),
            [0x00, 0x04, 0x04, 0x01, 0x00, 0x07, 0x00],
        );
    }

    #[test]
    fn control_preface_deterministic() {
        assert_eq!(
            encode_control_preface().unwrap(),
            encode_control_preface().unwrap(),
        );
    }

    /// The fixed stack buffer is exactly the encoded length: single-byte
    /// stream type, single-byte frame type and length, two 2-byte entries.
    #[test]
    fn control_preface_fixed_capacity() {
        let preface = encode_control_preface().unwrap();
        assert_eq!(preface.len(), CONTROL_PREFACE_LEN);
        assert_eq!(CONTROL_PREFACE_LEN, 7);
    }

    /// The golden bytes parse as a control stream type followed by exactly
    /// one zero-capacity SETTINGS frame, with no trailing bytes.
    #[test]
    fn control_preface_round_trip() {
        let preface = encode_control_preface().unwrap();
        let mut read: &[u8] = &preface;
        assert_eq!(StreamType::decode(&mut read).unwrap(), StreamType::CONTROL);
        let frame = Frame::decode(&mut read).unwrap();
        assert_eq!(
            frame,
            Frame::Settings(Settings::qpack_zero_capacity().unwrap())
        );
        assert!(!read.has_remaining());
    }
}
