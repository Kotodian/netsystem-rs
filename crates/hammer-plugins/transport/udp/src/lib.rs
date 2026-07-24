//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

use std::sync::OnceLock;

use abi_stable::RRef;
use hammer_runtime::{Engine, IpOutput_CTO, RuntimeError, RuntimeResult};

pub mod input;
mod wire;

type IpOutputFunctions = RRef<'static, IpOutput_CTO<'static, 'static>>;

static IP_OUTPUT: OnceLock<IpOutputFunctions> = OnceLock::new();

hammer_component_macros::declare_plugin!(
    name = "udp",
    load_after = ["ip"],
    init_functions = [__INIT_FN_UDP_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [],
    graph_nodes = [input::__UDP_GRAPH_NODE_UDP_INPUT_NODE],
    node_functions = [],
    process_nodes = [],
);

#[hammer_component_macros::init_function(
    name = "udp_init",
    runs_before = ["install_packet_graph"]
)]
fn init_udp(engine: &mut Engine) -> RuntimeResult<()> {
    let output = engine
        .plugin_main()
        .plugin("ip")?
        .ip_output()
        .into_option()
        .ok_or_else(|| RuntimeError::lifecycle("udp initialization", "IP output is unavailable"))?;
    IP_OUTPUT.set(output).map_err(|_| {
        RuntimeError::lifecycle("udp initialization", "IP output is already initialized")
    })
}

pub use input::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace};
