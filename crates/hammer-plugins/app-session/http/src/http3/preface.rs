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
