//! HTTP/3 engine.
//!
//! `proto` holds the synchronous wire primitives (frames, settings, stream
//! classification, field-section validation). QPACK and the connection,
//! request-stream, and control-stream state machines are later slices.

pub mod proto;
