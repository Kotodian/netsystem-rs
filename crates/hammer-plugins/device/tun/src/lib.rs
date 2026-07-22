//! Dynamic `tun` device-driver plugin (`libhammer_plugin_tun`).

mod tun;

pub use tun::*;

hammer_component_macros::declare_plugin!(
    name = "tun",
    load_after = ["ip"],
    init_functions = [],
    config_functions = [tun::__CONFIG_FN_TUN_CONFIG],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [tun::__INIT_FN_TUN_WORKER_INIT],
    graph_nodes = [
        tun::__TUN_GRAPH_NODE_TUN_INPUT_DRIVER_NODE,
        tun::__TUN_GRAPH_NODE_TUN_OUTPUT_DRIVER_NODE,
    ],
    node_functions = [],
    process_nodes = [],
);
