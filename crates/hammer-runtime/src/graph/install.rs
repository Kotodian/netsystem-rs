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

use crate::engine::Engine;

#[init_function(name = "install_packet_graph", runs_after = ["memory_init"])]
pub fn install_packet_graph(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    let handle = NodeHandle::new(config.worker.handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    let entries = engine.plugin_main().graph_nodes();
    let functions = engine.plugin_main().node_functions();
    engine
        .runtime
        .init_graph_with_node_functions(0, &entries, &functions)?;
    Ok(())
}
