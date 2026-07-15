//! Host installs the packet graph from the process-wide registration authority.
//!
//! Plugins and builtins contribute nodes through the same constructor-published
//! link-image inventories. This is not a service-owned graph catalog.

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

    let entries = crate::registration::graph_nodes();
    let functions = crate::registration::node_functions();
    engine
        .runtime
        .init_graph_with_node_functions(0, &entries, &functions)?;
    Ok(())
}
