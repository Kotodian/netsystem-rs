//! VPP HTTP FIFO ABI type mirrors.
//!
//! Byte-for-byte mirrors of the HTTP message/header ABI that VPP apps place
//! into session FIFOs. Every type here maps to a C type in
//! `third_party/vpp/src/plugins/http/http.h`; `#[repr(C)]` layouts match the
//! C `sizeof`/`offsetof` on the VPP build target (x86_64, LP64). Sizes and
//! field offsets are pinned by `crate::http_common::tests::layout_matches_vpp`
//! against values verified with a C compiler probe.
//!
//! Hammer difference: VPP memcpys these structs between memory and FIFO with
//! native (little-endian on x86_64) byte order; Hammer encodes and decodes
//! explicit little-endian bytes (see `crate::http_common::codec`), so the
//! on-FIFO bytes are identical. The `repr(C)` mirrors below exist to pin the
//! layout, not as memcpy carriers.

/// `http_msg_type_t` — `enum http_msg_type_` (plain C int, 4 bytes),
/// `http.h:87-91`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgType {
    Request = 0,
    Reply = 1,
}

impl TryFrom<u32> for MsgType {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, u32> {
        match value {
            0 => Ok(Self::Request),
            1 => Ok(Self::Reply),
            _ => Err(value),
        }
    }
}

/// `http_req_method_t` — `enum http_req_method_ : u8`, `http.h:80-85`,
/// order from `foreach_http_method` (`http.h:72-78`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReqMethod {
    Get = 0,
    Post = 1,
    Put = 2,
    Connect = 3,
    ConnectUdp = 4,
    /// Internal-use method; accepted by the codec for structural fidelity.
    Unknown = 5,
}

impl TryFrom<u8> for ReqMethod {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Get),
            1 => Ok(Self::Post),
            2 => Ok(Self::Put),
            3 => Ok(Self::Connect),
            4 => Ok(Self::ConnectUdp),
            5 => Ok(Self::Unknown),
            _ => Err(value),
        }
    }
}

/// `http_msg_data_type_t` — plain C int (4 bytes), `http.h:395-401`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgDataType {
    Inline = 0,
    Ptr = 1,
    Streaming = 2,
    NTypes = 3,
}

impl TryFrom<u32> for MsgDataType {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, u32> {
        match value {
            0 => Ok(Self::Inline),
            1 => Ok(Self::Ptr),
            2 => Ok(Self::Streaming),
            3 => Ok(Self::NTypes),
            _ => Err(value),
        }
    }
}

/// `http_url_scheme_t` — `enum http_url_scheme_ : u8`, `http.h:412-418`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlScheme {
    Http = 0,
    Https = 1,
    Masque = 2,
    Unknown = 3,
}

impl TryFrom<u8> for UrlScheme {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Http),
            1 => Ok(Self::Https),
            2 => Ok(Self::Masque),
            3 => Ok(Self::Unknown),
            _ => Err(value),
        }
    }
}

/// `http_upgrade_proto_t` — `enum http_upgrade_proto_ : u8`, `http.h:386-392`,
/// order from `foreach_http_upgrade_proto` (`http.h:379-383`).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeProto {
    Na = 0,
    ConnectUdp = 1,
    ConnectIp = 2,
    WebSocket = 3,
}

impl TryFrom<u8> for UpgradeProto {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            0 => Ok(Self::Na),
            1 => Ok(Self::ConnectUdp),
            2 => Ok(Self::ConnectIp),
            3 => Ok(Self::WebSocket),
            _ => Err(value),
        }
    }
}

bitflags::bitflags! {
    /// `http_field_line_flags_t` — `enum http_field_line_flags_ : u16`,
    /// `http.h:252-258`. The constants keep the exact ABI bits;
    /// `from_bits_retain` is the wire read (unknown/future VPP bits are
    /// retained), `.bits()` is the wire write.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FieldLineFlags: u16 {
        const INTERNAL = 1 << 0;
        const NEVER_INDEX = 1 << 1;
        /// Implied on every custom-name header entry by the app-side writer
        /// (`http_add_custom_header2`, `http.h:1152`).
        const CUSTOM_NAME = 1 << 2;
        const HOP_BY_HOP = 1 << 3;
    }
}

/// Known header name indices, positional values of `http_header_name_t`
/// (`enum http_header_name_ : u16`, `http.h:370-377`) over
/// `foreach_http_header_name` (`http.h:260-375`). Subset used by the publish
/// seam; the encoder accepts any `u16` index, as VPP does.
pub mod header_name {
    pub const ACCEPT_ENCODING: u16 = 1;
    pub const ACCEPT_LANGUAGE: u16 = 2;
    pub const ACCEPT: u16 = 4;
    pub const CONNECTION: u16 = 30;
    pub const CONTENT_LENGTH: u16 = 35;
    pub const CONTENT_TYPE: u16 = 39;
    pub const HOST: u16 = 52;
    pub const USER_AGENT: u16 = 87;
}

/// A known regular-field name of the publish seam, bound to its positional
/// `http_header_name_t` index. `TryFrom<&[u8]>` is the single Rust-native
/// name-to-ABI conversion: VPP matches the lowercased name column of
/// `foreach_http_header_name` (`http.h:260-375`); any other name is not a
/// `HeaderName` and its field publishes as `Custom`. O(1), zero allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeaderName(u16);

impl TryFrom<&[u8]> for HeaderName {
    type Error = ();

    fn try_from(name: &[u8]) -> Result<Self, ()> {
        match name {
            b"accept" => Ok(Self(header_name::ACCEPT)),
            b"accept-encoding" => Ok(Self(header_name::ACCEPT_ENCODING)),
            b"accept-language" => Ok(Self(header_name::ACCEPT_LANGUAGE)),
            b"connection" => Ok(Self(header_name::CONNECTION)),
            b"content-length" => Ok(Self(header_name::CONTENT_LENGTH)),
            b"content-type" => Ok(Self(header_name::CONTENT_TYPE)),
            b"host" => Ok(Self(header_name::HOST)),
            b"user-agent" => Ok(Self(header_name::USER_AGENT)),
            _ => Err(()),
        }
    }
}

impl From<HeaderName> for u16 {
    fn from(name: HeaderName) -> u16 {
        name.0
    }
}

/// `http_msg_data_t` — 80 bytes on x86_64 LP64, `http.h:426-444`. Field
/// order/widths match the C struct exactly; offsets pinned by
/// `layout_matches_vpp`. `headers_ctx` is a C `uword` (8 bytes); it is
/// server-side scratch on receive and written as 0 by the app publish path.
#[repr(C)]
pub struct MsgData {
    pub data_type: MsgDataType,
    pub len: u64,
    pub scheme: UrlScheme,
    pub target_authority_offset: u32,
    pub target_authority_len: u32,
    pub target_path_offset: u32,
    pub target_path_len: u32,
    pub target_query_offset: u32,
    pub target_query_len: u32,
    pub headers_offset: u32,
    pub headers_len: u32,
    pub body_offset: u32,
    pub body_len: u64,
    pub headers_ctx: u64,
    pub upgrade_proto: UpgradeProto,
}

/// `http_msg_t` — 88 bytes on x86_64 LP64, `http.h:446-455`.
///
/// Hammer difference: the C `union { http_req_method_t method_type;
/// http_status_code_t code; }` at offset 4 is modeled as a plain `u32`
/// (`method_or_code`) — the low byte holds the method for requests, the
/// whole word the status code for replies. The on-FIFO bytes are identical;
/// Rust unions would need `unsafe` to read, which this seam avoids.
#[repr(C)]
pub struct HttpMsg {
    pub msg_type: MsgType,
    pub method_or_code: u32,
    pub data: MsgData,
}
