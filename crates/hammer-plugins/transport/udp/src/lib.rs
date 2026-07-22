//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

pub mod input;
mod wire;

hammer_component_macros::declare_plugin!(
    name = "udp",
    load_after = ["ip"],
    init_functions = [],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [input::__UDP_GRAPH_NODE_UDP_INPUT_NODE],
    node_functions = [],
    process_nodes = [],
);

pub use input::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace};
