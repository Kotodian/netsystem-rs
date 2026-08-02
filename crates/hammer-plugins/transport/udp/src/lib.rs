//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

use std::sync::OnceLock;

use abi_stable::{
    RRef,
    sabi_trait::TD_Opaque,
    std_types::{RBoxError, RErr, ROk, RResult},
};
use hammer_core::data_plane::NodeId;
use hammer_runtime::plugin::UdpLocal_CTO;
use hammer_runtime::{Engine, IpOutput_CTO, RuntimeResult, UdpLocal};

pub mod input;
mod wire;

type IpOutputFunctions = RRef<'static, IpOutput_CTO<'static, 'static>>;

static IP_OUTPUT: OnceLock<IpOutputFunctions> = OnceLock::new();

struct UdpLocalService;

impl UdpLocal for UdpLocalService {
    fn register_dst_port(
        &self,
        version: UdpIpVersion,
        port: u16,
        node: NodeId,
    ) -> RResult<(), RBoxError> {
        match input::register_dst_port(version, port, node) {
            Ok(()) => ROk(()),
            Err(error) => RErr(RBoxError::new(error)),
        }
    }

    fn unregister_dst_port(
        &self,
        version: UdpIpVersion,
        port: u16,
        node: NodeId,
    ) -> RResult<(), RBoxError> {
        match input::unregister_dst_port(version, port, node) {
            Ok(()) => ROk(()),
            Err(error) => RErr(RBoxError::new(error)),
        }
    }
}

static UDP_LOCAL: UdpLocal_CTO<'static, 'static> =
    UdpLocal_CTO::from_const(&UdpLocalService, TD_Opaque);

hammer_component_macros::declare_plugin!(
    name = "udp",
    load_after = ["ip"],
    udp_local = &UDP_LOCAL,
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
        .ok_or(input::UdpControlError::IpOutputUnavailable)?;
    IP_OUTPUT
        .set(output)
        .map_err(|_| input::UdpControlError::IpOutputAlreadyInitialized)?;
    Ok(())
}

pub use hammer_runtime::UdpIpVersion;
pub use input::{
    UdpControlError, UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace,
};

#[cfg(test)]
mod tests {
    use hammer_core::data_plane::BufferFrame;
    use hammer_runtime::{
        DataPlaneRuntime, InternalNode, Node, NodeResult, RuntimeError, RuntimeRegistry,
    };

    use super::*;

    struct UdpConsumerNode;

    impl Node for UdpConsumerNode {
        fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }
    }

    impl InternalNode for UdpConsumerNode {}

    #[test]
    fn udp_local_capability_calls_destination_port_operations() {
        let runtime = DataPlaneRuntime::new(Default::default());
        let consumer = runtime.nodes().register_internal(UdpConsumerNode);
        let owner = runtime.nodes().register_internal(UdpConsumerNode);
        let other = runtime.nodes().register_internal(UdpConsumerNode);
        let control = UdpInputControlPlane::new().with_nodes(runtime.nodes().clone());
        input::install_registration_for_test(&control, consumer).expect("install UDP control");

        let mut engine = Engine::new(runtime, RuntimeRegistry::new());
        engine.install_current();
        let module = plugin_module();
        let local = module
            .udp_local()
            .into_option()
            .expect("UDP capability export")
            .get();

        local
            .register_dst_port(UdpIpVersion::V4, 443, owner)
            .into_result()
            .expect("register destination port through capability");
        local
            .register_dst_port(UdpIpVersion::V4, 443, owner)
            .into_result()
            .expect("share destination port through capability");
        let error = local
            .register_dst_port(UdpIpVersion::V4, 443, other)
            .into_result()
            .expect_err("different owner must conflict");
        let error = error
            .downcast_ref::<RuntimeError>()
            .expect("runtime error across capability");
        let RuntimeError::Subsystem { source, .. } = error else {
            panic!("expected UDP subsystem error");
        };
        assert!(source.downcast_ref::<UdpControlError>().is_some());

        local
            .unregister_dst_port(UdpIpVersion::V4, 443, owner)
            .into_result()
            .expect("release shared destination port");
        local
            .unregister_dst_port(UdpIpVersion::V4, 443, owner)
            .into_result()
            .expect("release final destination port");
        local
            .unregister_dst_port(UdpIpVersion::V4, 443, owner)
            .into_result()
            .expect_err("final release removes destination mapping");

        Engine::uninstall_current();
    }
}
