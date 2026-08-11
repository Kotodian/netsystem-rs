//! Stream type classification (RFC 9114 Section 6.2) and stream identifiers
//! (RFC 9114 Section 3.3, RFC 9000 Section 3.4).
//!
//! Adapted from `third_party/h3/h3/src/proto/stream.rs` and the unidirectional
//! stream classification switch in `third_party/vpp/src/plugins/http/http3/http3.c`
//! (`http3_stream_transport_rx_unknown_type`, ~line 1680).

use std::fmt;

use bytes::{Buf, BufMut};

use super::coding::{Decode, Encode};
use super::varint::{UnexpectedEnd, VarInt, VarIntBoundsExceeded};

/// A unidirectional stream type (RFC 9114 Section 6.2). Bidi request streams
/// carry no stream type; they are classified via `StreamId`.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct StreamType(VarInt);

impl StreamType {
    pub const CONTROL: StreamType = StreamType(VarInt::from_u32(0x00));
    pub const PUSH: StreamType = StreamType(VarInt::from_u32(0x01));
    pub const ENCODER: StreamType = StreamType(VarInt::from_u32(0x02));
    pub const DECODER: StreamType = StreamType(VarInt::from_u32(0x03));

    pub const MAX_ENCODED_SIZE: usize = VarInt::MAX_SIZE;

    /// The raw stream type value.
    pub fn value(&self) -> u64 {
        self.0.into_inner()
    }

    /// Build a `StreamType` from a varint value, failing if it exceeds the
    /// varint range.
    pub fn from_value(value: u64) -> Result<Self, VarIntBoundsExceeded> {
        Ok(StreamType(VarInt::from_u64(value)?))
    }

    /// Classify an unknown uni stream, mirroring the switch in VPP
    /// `http3_stream_transport_rx_unknown_type` (http3.c ~1680): control,
    /// QPACK encoder/decoder, and push are known; anything else is drained.
    pub fn classify(&self) -> StreamCategory {
        match self.value() {
            0x00 => StreamCategory::Control,
            0x01 => StreamCategory::Push,
            0x02 => StreamCategory::QpackEncoder,
            0x03 => StreamCategory::QpackDecoder,
            other => StreamCategory::Unknown(other),
        }
    }
}

impl Decode for StreamType {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, UnexpectedEnd> {
        Ok(StreamType(VarInt::decode(buf)?))
    }
}

impl Encode for StreamType {
    fn encode<W: BufMut>(&self, buf: &mut W) {
        self.0.encode(buf);
    }
}

impl fmt::Display for StreamType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.classify() {
            StreamCategory::Control => "control-stream",
            StreamCategory::Push => "push-stream",
            StreamCategory::QpackEncoder => "qpack-encoder-stream",
            StreamCategory::QpackDecoder => "qpack-decoder-stream",
            StreamCategory::Unknown(_) => "unknown-stream-type",
        };
        f.write_str(name)
    }
}

/// Classification of a unidirectional stream type.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StreamCategory {
    /// Control stream (type 0x00): carries SETTINGS, GOAWAY, MAX_PUSH_ID.
    Control,
    /// Push stream (type 0x01): not supported in this slice.
    Push,
    /// QPACK encoder stream (type 0x02): drained only.
    QpackEncoder,
    /// QPACK decoder stream (type 0x03): drained only.
    QpackDecoder,
    /// Any other type: must be ignored.
    Unknown(u64),
}

/// Identifier for a QUIC stream (RFC 9000 Section 3.4): two low bits encode
/// initiator and directionality.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StreamId(VarInt);

impl StreamId {
    /// Is this a client-initiated bidi (request) stream?
    pub fn is_request(&self) -> bool {
        self.dir() == Dir::Bi && self.initiator() == Side::Client
    }

    /// Is this a server-initiated uni (push) stream?
    pub fn is_push(&self) -> bool {
        self.dir() == Dir::Uni && self.initiator() == Side::Server
    }

    pub(crate) fn initiator(self) -> Side {
        if self.0.into_inner() & 0x1 == 0 {
            Side::Client
        } else {
            Side::Server
        }
    }

    /// The stream index, excluding initiator and direction bits.
    pub fn index(self) -> u64 {
        self.0.into_inner() >> 2
    }

    fn dir(self) -> Dir {
        if self.0.into_inner() & 0x2 == 0 {
            Dir::Bi
        } else {
            Dir::Uni
        }
    }
}

impl TryFrom<u64> for StreamId {
    type Error = VarIntBoundsExceeded;

    fn try_from(v: u64) -> Result<Self, Self::Error> {
        Ok(StreamId(VarInt::from_u64(v)?))
    }
}

impl From<VarInt> for StreamId {
    fn from(v: VarInt) -> Self {
        StreamId(v)
    }
}

impl From<StreamId> for VarInt {
    fn from(v: StreamId) -> Self {
        v.0
    }
}

impl Encode for StreamId {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.0.encode(buf);
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let initiator = match self.initiator() {
            Side::Client => "client",
            Side::Server => "server",
        };
        let dir = match self.dir() {
            Dir::Uni => "uni",
            Dir::Bi => "bi",
        };
        write!(f, "{} {}directional stream {}", initiator, dir, self.index())
    }
}

/// Which side of a connection initiated the stream.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Side {
    Client = 0,
    Server = 1,
}

/// Whether data flows in both directions or only from the initiator.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum Dir {
    Bi = 0,
    Uni = 1,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_type_values() {
        assert_eq!(StreamType::CONTROL.value(), 0x00);
        assert_eq!(StreamType::PUSH.value(), 0x01);
        assert_eq!(StreamType::ENCODER.value(), 0x02);
        assert_eq!(StreamType::DECODER.value(), 0x03);
    }

    #[test]
    fn classification_control_qpack_push_unknown() {
        assert_eq!(StreamType::CONTROL.classify(), StreamCategory::Control);
        assert_eq!(StreamType::ENCODER.classify(), StreamCategory::QpackEncoder);
        assert_eq!(StreamType::DECODER.classify(), StreamCategory::QpackDecoder);
        assert_eq!(StreamType::PUSH.classify(), StreamCategory::Push);
        assert_eq!(
            StreamType::from_value(0x2a).unwrap().classify(),
            StreamCategory::Unknown(0x2a)
        );
        assert!(StreamType::from_value(1 << 62).is_err());
    }

    #[test]
    fn stream_type_codec() {
        for (raw, expected) in [
            (0x00, vec![0x00]),
            (0x01, vec![0x01]),
            (0x02, vec![0x02]),
            (0x03, vec![0x03]),
            (0x40, vec![0x40, 0x40]),
        ] {
            let ty = StreamType::from_value(raw).unwrap();
            let mut out = Vec::new();
            ty.encode(&mut out);
            assert_eq!(out, expected);
            let mut read: &[u8] = &out;
            assert_eq!(StreamType::decode(&mut read), Ok(ty));
        }
    }

    #[test]
    fn truncated_stream_type_errors() {
        let mut empty: &[u8] = &[];
        assert!(StreamType::decode(&mut empty).is_err());
        let mut partial: &[u8] = &[0b01 << 6];
        assert!(StreamType::decode(&mut partial).is_err());
    }

    #[test]
    fn stream_id_bits() {
        // Client-initiated bidi stream: index 0 -> 0
        let id = StreamId::try_from(0u64).unwrap();
        assert!(id.is_request());
        assert!(!id.is_push());
        assert_eq!(id.index(), 0);

        // Server-initiated uni stream (push): index 0 -> 0b111
        let push = StreamId::try_from(7u64).unwrap();
        assert!(!push.is_request());
        assert!(push.is_push());

        // Client uni (control/qpack), index 3 -> (3 << 2) | 0b10
        let ctrl = StreamId::try_from(14u64).unwrap();
        assert!(!ctrl.is_request());
        assert!(!ctrl.is_push());
        assert_eq!(ctrl.index(), 3);

        assert_eq!(VarInt::from(StreamId::try_from(0u64).unwrap()), VarInt::from_u32(0));
    }

    #[test]
    fn stream_id_bounds() {
        let max = VarInt::MAX.into_inner();
        assert!(StreamId::try_from(max).is_ok());
        assert!(StreamId::try_from(max + 1).is_err());
    }

    #[test]
    fn stream_id_from_varint_and_back() {
        let id = StreamId::from(VarInt::from_u32(4));
        let var: VarInt = id.into();
        assert_eq!(var, VarInt::from_u32(4));
    }

    #[test]
    fn stream_id_encode_matches_varint() {
        let id = StreamId::try_from(14u64).unwrap();
        let mut out = Vec::new();
        id.encode(&mut out);
        let mut read: &[u8] = &out;
        assert_eq!(VarInt::decode(&mut read).unwrap().into_inner(), 14);
    }
}
