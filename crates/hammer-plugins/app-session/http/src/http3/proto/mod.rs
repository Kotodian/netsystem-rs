//! Synchronous HTTP/3 wire primitives.
//!
//! References:
//! - RFC 9114 (HTTP/3): frames, settings, stream types, error codes.
//! - `third_party/vpp/src/plugins/http/http3/{http3.h,http3.c,frame.h,frame.c}`
//! - `third_party/h3/h3/src/{proto,error}` (read-only protocol reference)
//!
//! This slice is synchronous and stateless at the connection level: no QPACK
//! (the `qpack` module is the next slice and is not declared here yet), no
//! dynamic table, no blocked field sections, no push machinery. Encoded field
//! sections are carried as opaque bytes.

pub mod coding;
pub mod control;
pub mod error;
pub mod frame;
pub mod headers;
pub mod push;
pub mod stream;
pub mod varint;
