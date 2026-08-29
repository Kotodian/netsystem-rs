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
        write!(
            f,
            "{} {}directional stream {}",
            initiator,
            dir,
            self.index()
        )
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
