//! Host installs the packet graph from the process-wide registration authority.
//!
//! Plugins and builtins contribute nodes through the same PluginMain-owned
//! link-image inventories. This is not a service-owned graph catalog.

use crate::error::RuntimeResult;
use hammer_component_macros::init_function;

use crate::global_main::GlobalMain;

#[init_function(name = "install_packet_graph")]
pub fn install_packet_graph(engine: &mut GlobalMain) -> RuntimeResult<()> {
    let entries = engine.plugin_main().graph_nodes();
    let functions = engine.plugin_main().node_functions();
    engine
        .main
        .init_graph_with_node_functions(&entries, &functions)?;
    Ok(())
}
