//! Hammer HTTP plugin.
//!
//! This slice owns only the synchronous HTTP/3 protocol primitives under
//! `http3::proto`, aligned with `third_party/vpp/src/plugins/http/http3/`
//! and `third_party/h3/h3/src/proto`, plus the VPP HTTP FIFO ABI codec under
//! `http_common` (message/header types and checked encode/decode for
//! publishing one request). The VPP-aligned HTTP transport (listener nesting,
//! worker contexts, FIFO transfer, SessionTransport) is a later slice;
//! nothing here registers plugin lifecycle hooks yet.

mod http3;
mod http_common;
