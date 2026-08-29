//! VPP `vlib_node_main` node registration entry.
//!
//! Node metadata is collected by linkme into `NodeEntry` slices (VPP
//! `VLIB_REGISTER_NODE`). `DataPlaneMain::init_graph` walks them and calls
//! each entry's `init` fn; `NodeRuntime::resolve_named_next_nodes` links by
//! name (VPP `vlib_node_main_init`).
//!
//! Graph *contents* come from plugins (and runtime builtins). The host only
//! installs the filtered catalog — see [`install::install_packet_graph`].

mod fanout;
pub(crate) mod install;

pub use crate::NodeEntry;
pub use install::install_packet_graph;
