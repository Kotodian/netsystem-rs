use std::mem::transmute;

use hammer_core::config::Config;
use hammer_core::data_plane::{BufferFrame, NodeId, NodeRegistration};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{
    DataPlaneRuntime, Engine, InternalNode, Node, NodeResult, new_worker_runtime,
};
use hammer_service::data_plane::DropNode;
use hammer_service::device::DeviceMain;
use hammer_service::interface::{
    InterfaceOutputControlPlane, install_worker_interface_output_runtime,
};
use hammer_service::opaque::NetworkOpaque;

#[derive(Clone, Copy)]
struct TxSinkNode;

impl Node for TxSinkNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

impl InternalNode for TxSinkNode {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("device-output-runtime-test-sink", 0)
    }
}

#[test]
fn worker_output_runtimes_only_install_their_assigned_tx_queues() {
    let runtime = new_worker_runtime(&Config::default()).expect("runtime");
    let engine = Engine::new(runtime, RuntimeRegistry::new());
    let _ = engine.runtime.nodes().register_internal(DropNode::new());
    let sink = engine.runtime.nodes().register_internal(TxSinkNode);
    let output = engine
        .runtime
        .nodes()
        .register_internal(InterfaceOutputControlPlane::new().node());

    let devices = DeviceMain::new();
    devices
        .register_tx_queue(11, 1, 0, hammer_runtime::DataWorkerId::new(0), sink)
        .expect("single-worker TX queue");
    devices
        .register_tx_queue(12, 2, 0, hammer_runtime::DataWorkerId::new(0), sink)
        .expect("shared TX queue");
    devices
        .assign_tx_queue_to_worker(2, 0, hammer_runtime::DataWorkerId::new(1))
        .expect("share TX queue");

    let mut first_worker = engine.spawn(1).expect("first worker");
    let mut second_worker = engine.spawn(2).expect("second worker");
    install_worker_interface_output_runtime(&mut first_worker, &devices)
        .expect("first runtime");
    install_worker_interface_output_runtime(&mut second_worker, &devices)
        .expect("second runtime");

    dispatch_to_interface(&first_worker.runtime, output, 11);
    assert_eq!(node_vectors(&first_worker.runtime, sink), Some(1));
    dispatch_to_interface(&second_worker.runtime, output, 11);
    assert_eq!(node_vectors(&second_worker.runtime, sink), None);

    dispatch_to_interface(&first_worker.runtime, output, 12);
    assert_eq!(node_vectors(&first_worker.runtime, sink), Some(2));
    dispatch_to_interface(&second_worker.runtime, output, 12);
    assert_eq!(node_vectors(&second_worker.runtime, sink), Some(1));
}

fn dispatch_to_interface(runtime: &DataPlaneRuntime, output: NodeId, interface_index: u32) {
    let mut frame = runtime.buffers().get_next_frame(output).expect("frame");
    let index = runtime
        .alloc_index_with_bytes(&[0x45, 0, 0, 20])
        .expect("packet");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
        // SAFETY: NetworkOpaque is the established view of the primary opaque
        // region and its compile-time layout check guarantees that it fits.
        let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        network.sw_if_index[1] = interface_index;
    }
    frame.push_index(index).expect("push packet");
    runtime.put_next_frame(frame).expect("schedule frame");
    runtime.run_ready_nodes().expect("run output");
}

fn node_vectors(runtime: &DataPlaneRuntime, node: NodeId) -> Option<u64> {
    runtime
        .nodes()
        .node_runtime_stats_snapshot()
        .into_iter()
        .find(|stats| stats.node_id == node)
        .map(|stats| stats.vectors)
        .filter(|vectors| *vectors > 0)
}
