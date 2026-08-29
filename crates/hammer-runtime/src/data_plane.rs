//! Runtime data-plane facade.
//!
//! The execution context lives in the `data_plane::main` owner module. This
//! path remains as the stable crate-local module while the implementation is
//! split by VPP-style responsibility.

#[path = "data_plane/main.rs"]
mod main;

pub use main::{DataPlaneBufferConfig, DataPlaneMain};
