//! Host installs the packet graph from the process-wide registration authority.
//!
//! Plugins and builtins contribute nodes through the same PluginMain-owned
//! link-image inventories. This is not a service-owned graph catalog.

use crate::error::RuntimeResult;
use hammer_component_macros::init_function;
use hammer_core::data_plane::NodeHandle;

use crate::engine::Engine;

#[init_function(name = "install_packet_graph")]
pub fn install_packet_graph(engine: &mut Engine) -> RuntimeResult<()> {
    let handle = NodeHandle::new(engine.worker_config().handoff.node_handle);
    engine.runtime.set_handoff_node_handle(handle);

    let entries = engine.plugin_main().graph_nodes();
    let functions = engine.plugin_main().node_functions();
    engine
        .runtime
        .init_graph_with_node_functions(&entries, &functions)?;
    engine.install_graph_stats()?;
    Ok(())
}
