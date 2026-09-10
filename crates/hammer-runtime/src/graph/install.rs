//! Host installs the packet graph from the process-wide registration authority.
//!
//! Plugins and builtins contribute nodes through the same PluginMain-owned
//! link-image inventories. This is not a service-owned graph catalog.

use crate::error::RuntimeResult;

pub fn install_packet_graph<'entry, 'function>(
    main: &mut crate::DataPlaneMain,
    entries: impl Clone + Iterator<Item = &'entry crate::node::NodeEntry>,
    functions: impl Clone + Iterator<Item = &'function crate::node::NodeFunctionRegistration>,
) -> RuntimeResult<()> {
    main.init_graph_from_declarations(entries, functions)
}
