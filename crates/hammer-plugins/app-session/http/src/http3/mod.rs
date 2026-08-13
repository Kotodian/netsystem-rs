//! HTTP/3 engine.
//!
//! `proto` holds the synchronous wire primitives (frames, settings, stream
//! classification, field-section validation). `request` holds the
//! request-stream frame-ordering state machine. QPACK and the connection
//! and control-stream state machines are later slices.

pub mod proto;

pub(crate) mod preface;
pub(crate) mod request;
pub(crate) mod request_fields;
pub(crate) mod request_frame_reader;
