use hammer_adapter::{DataPlaneRuntime, InternalNode, Node, NodeResult, NodeRuntimeData};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpSegmentFlags;
use hammer_service::transport::tcp::{
    TcpNodeError, TcpOutputError,
    output::{TcpOutputNext, TcpOutputNode},
};
use std::sync::{Arc, Mutex, OnceLock};

struct BlackholeNode;

impl Node for BlackholeNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut hammer_adapter::BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
    fn node_process(&self) -> hammer_adapter::NodeProcessFn {
        |_: &DataPlaneRuntime,
         _: hammer_adapter::NodeRuntimeData,
         _: &mut hammer_adapter::BufferFrame| NodeResult::drop()
    }
    fn node_runtime_data(&self) -> CoreResult<hammer_adapter::NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

impl InternalNode for BlackholeNode {}

fn setup_output() -> (DataPlaneRuntime, hammer_adapter::NodeId) {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let drop = runtime.nodes().register_internal(BlackholeNode);
    let lookup = runtime.nodes().register_internal(BlackholeNode);
    let output = runtime
        .nodes()
        .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, lookup)));
    (runtime, output)
}

#[test]
fn bad_tcp_header_increments_tcp_output_counter() {
    let (runtime, output) = setup_output();
    let code = TcpOutputError::NoTcpHeader.code();
    let before = runtime.node_error_count(output, code).unwrap_or(0);
    let index = runtime.alloc_index_with_bytes(b"hello").expect("buffer");
    let frame = runtime.alloc_frame_index().expect("frame");
    runtime
        .get_frame_mut(frame)
        .expect("frame")
        .push_index(index)
        .expect("push");
    assert!(runtime.schedule_frame(output, frame).expect("schedule"));
    let _ = runtime.run_ready_nodes().expect("run");
    let after = runtime.node_error_count(output, code).unwrap_or(0);
    assert_eq!(
        after - before,
        1,
        "counter must increment for bad TCP header"
    );
}
