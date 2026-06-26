//! VPP `vlib_node_main` node registration entry + graph-node trait.
//!
//! Node metadata is collected by linkme into `NodeEntry` slices (VPP
//! `VLIB_REGISTER_NODE`). `DataPlaneRuntime::init_graph` walks them and calls
//! `GraphNode::init`; `NodeRuntime::resolve_named_next_nodes` links by name
//! (VPP `vlib_node_main_init`).

pub use hammer_adapter::{GraphNode, NodeEntry};
