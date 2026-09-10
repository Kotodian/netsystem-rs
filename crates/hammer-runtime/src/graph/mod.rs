//! VPP `vlib_node_main` node registration entry.
//!
//! Node metadata is collected by linkme into `NodeEntry` slices (VPP
//! `VLIB_REGISTER_NODE`). `DataPlaneMain::init_graph` walks them and calls
//! each entry's `init` fn; `NodeMain::resolve_named_next_nodes` links by
//! name (VPP `vlib_node_main_init`).
//!
//! Graph contents come from declarations installed by the thread-zero runtime
//! before normal initialization begins.

mod fanout;

pub use crate::NodeEntry;
