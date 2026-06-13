use hammer_adapter::{DataPlaneRuntime, DataWorkerId};
use hammer_service::session::SessionQueueNode;

#[test]
fn session_queue_node_runs_as_empty_frame_driver() {
    let runtime = DataPlaneRuntime::with_capacities(64, 8, 4, 8);
    let driver = runtime
        .nodes()
        .register_driver(SessionQueueNode::new(DataWorkerId::new(0)));

    runtime
        .schedule_empty_frame(driver)
        .expect("schedule session queue node");

    assert_eq!(runtime.run_ready_nodes().expect("run ready nodes"), 1);
}
