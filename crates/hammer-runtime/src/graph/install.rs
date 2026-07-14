//! Host installs the packet graph from plugin (and builtin) `GRAPH_NODES`.
//!
//! Plugins contribute nodes via `#[graph_node]` / cdylib inventory; this init
//! only filters by `loaded_plugins` and calls `init_graph`. It is not a
//! service-owned graph catalog.

use std::sync::Arc;

use hammer_component_macros::init_function;
use hammer_core::config::Config;
use hammer_core::data_plane::NodeHandle;
use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;

use crate::engine::Engine;
use crate::node::{GRAPH_NODES, NodeEntry};
use crate::plugin::filter_by_plugin;

#[init_function(name = "install_packet_graph", runs_after = ["memory_init"])]
pub fn install_packet_graph(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    let handle = NodeHandle::new(config.worker.handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    let loaded = engine.loaded_plugins();
    let filtered: Vec<NodeEntry> = filter_by_plugin(&GRAPH_NODES[..], loaded, |entry| entry.plugin)
        .into_iter()
        .copied()
        .collect();
    engine.runtime.init_graph(0, &filtered)?;
    Ok(())
}
