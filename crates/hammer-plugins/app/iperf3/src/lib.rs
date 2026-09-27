//! Builtin iperf3 TCP server.

mod config;
mod main;
mod protocol;

pub use config::Iperf3Config;
pub use main::{Iperf3Main, ListenerRole, SessionPhase};
pub use protocol::{
    ControlAction, ControlParameters, ControlParser, ControlPhase, ControlState,
    Iperf3ProtocolError,
};

hammer_component_macros::declare_plugin!(
    name = "iperf3",
    load_after = ["session", "tcp"],
    init_functions = [__INIT_FN_IPERF3_INIT],
    config_functions = [__CONFIG_FN_IPERF3_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    api_init_functions = [],
    graph_nodes = [],
    node_functions = [],
    process_nodes = []
);
