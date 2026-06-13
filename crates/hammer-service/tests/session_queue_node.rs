use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_service::session::protocol::tcp::node::TcpSessionQueueNode;

#[test]
fn session_queue_node_runs_as_empty_frame_driver() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let worker = DataWorkerId::new(0);
    let node = TcpSessionQueueNode::new(worker).expect("build tcp session queue node");
    let driver = runtime.nodes().register_driver(node);

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule session queue node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
}
