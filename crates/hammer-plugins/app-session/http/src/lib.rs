//! Hammer HTTP plugin.
//!
//! This slice owns only the synchronous HTTP/3 protocol primitives under
//! `http3::proto`, aligned with `third_party/vpp/src/plugins/http/http3/`
//! and `third_party/h3/h3/src/proto`. The VPP-aligned HTTP transport
//! (listener nesting, worker contexts, FIFO ABI, SessionTransport) is a later
//! slice; nothing here registers plugin lifecycle hooks yet.

mod http3;
