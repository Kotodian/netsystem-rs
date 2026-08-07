//! Dynamic `udp` plugin (`libhammer_plugin_udp`).

use abi_stable::StableAbi;
use hammer_core::data_plane::NodeId;
use hammer_runtime::RuntimeResult;

pub mod input;
mod wire;

#[derive(Debug, Clone, Copy, PartialEq, Eq, StableAbi, serde::Deserialize, serde::Serialize)]
#[repr(u8)]
pub enum UdpIpVersion {
    V4 = 0,
    V6 = 1,
}

pub fn register_dst_port(version: UdpIpVersion, port: u16, node: NodeId) -> RuntimeResult<()> {
    input::register_dst_port(version, port, node)
}

pub fn unregister_dst_port(version: UdpIpVersion, port: u16, node: NodeId) -> RuntimeResult<()> {
    input::unregister_dst_port(version, port, node)
}

hammer_component_macros::declare_plugin!(
    name = "udp",
    load_after = ["ip"],
    init_functions = [worker::__INIT_FN_UDP_INIT],
    config_functions = [],
    early_config_functions = [],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [worker::__INIT_FN_UDP_WORKER_INIT],
    graph_nodes = [
        input::__UDP_GRAPH_NODE_UDP_INPUT_NODE,
        output::__UDP_WORKER_GRAPH_NODE_UDP_OUTPUT_NODE,
    ],
    node_functions = [],
    process_nodes = [],
    session_transports = [worker::__SESSION_TRANSPORT_UDP_WORKER],
);

pub use input::{
    UdpControlError, UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputNode, UdpInputTrace,
};
pub use output::{UdpOutputNext, UdpOutputNode};
pub use worker::UdpWorker;

mod connection;
pub(crate) mod lookup;
pub mod output;
pub(crate) mod worker;

#[cfg(test)]
mod tests {
    use hammer_core::data_plane::BufferFrame;
    use hammer_runtime::{
        DataPlaneRuntime, Engine, InternalNode, Node, NodeResult, RuntimeError, RuntimeRegistry,
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
        let barrier = engine.worker_barrier();
        barrier.sync(|| {
            register_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("register destination port through plugin API");
            register_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("share destination port through plugin API");
            let error = register_dst_port(UdpIpVersion::V4, 443, other)
                .expect_err("different owner must conflict");
            let RuntimeError::Subsystem { source, .. } = error else {
                panic!("expected UDP subsystem error");
            };
            assert!(source.downcast_ref::<UdpControlError>().is_some());

            unregister_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("release shared destination port");
            unregister_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("release final destination port");
            unregister_dst_port(UdpIpVersion::V4, 443, owner)
                .expect_err("final release removes destination mapping");
        });

        Engine::uninstall_current();
    }
}
