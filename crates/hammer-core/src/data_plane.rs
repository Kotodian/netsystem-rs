//! Compatibility facade for the packet-graph ABI.
//!
//! Canonical graph and buffer values live in their owner modules. This facade
//! keeps the established `data_plane` path available while callers migrate to
//! `hammer_core::{graph, buffer}`.

pub use crate::buffer::*;
pub use crate::graph::*;
