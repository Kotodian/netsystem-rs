use hammer_adapter::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, InternalNode, Node, NodeResult, NodeRuntimeData,
};
use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::TcpSegmentFlags;
use hammer_service::transport::tcp::{
    TcpNodeError, TcpOutputError,
    output::{TcpOutputNext, TcpOutputNode},
};
use std::sync::{Arc, Mutex, OnceLock};

fn test_runtime_configured(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_capacity: usize,
    frame_slots: usize,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_capacity,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

struct BlackholeNode;

impl Node for BlackholeNode {
    fn process(
        &mut self,
        _: &DataPlaneRuntime,
        _: &mut hammer_core::data_plane::BufferFrame,
    ) -> NodeResult {
        NodeResult::drop()
    }
    fn node_process(&self) -> hammer_adapter::NodeProcessFn {
        |_: &DataPlaneRuntime,
         _: hammer_adapter::NodeRuntimeData,
         _: &mut hammer_core::data_plane::BufferFrame| NodeResult::drop()
    }
    fn node_runtime_data(&self) -> CoreResult<hammer_adapter::NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

impl InternalNode for BlackholeNode {}

fn setup_output() -> (DataPlaneRuntime, hammer_core::data_plane::NodeId) {
    let runtime = test_runtime_configured(2048, 16, 8, 8);
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
    let mut frame = runtime.buffers().get_next_frame(output).expect("frame");
    frame.push_index(index).expect("push");
    runtime.put_next_frame(frame).expect("schedule");
    let _ = runtime.run_ready_nodes().expect("run");
    let after = runtime.node_error_count(output, code).unwrap_or(0);
    assert_eq!(
        after - before,
        1,
        "counter must increment for bad TCP header"
    );
}
