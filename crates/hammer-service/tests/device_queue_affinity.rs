use hammer_core::data_plane::{BufferFrame, NodeRegistration};
use hammer_runtime::config::Worker;
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeResult};
use hammer_service::data_plane::DropNode;
use hammer_service::device::{DeviceMain, DriverScheduleMode};
use hammer_service::interface::InterfaceOutputNode;

#[derive(Clone, Copy)]
struct TxSink;

impl Node for TxSink {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

impl InternalNode for TxSink {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("device-queue-affinity-sink", 0)
    }
}

#[test]
fn tx_queue_affinity_is_published_by_interface_and_owner() {
    let runtime = Worker::default().create_runtime().expect("runtime");
    runtime.nodes().register_internal(DropNode::new());
    let sink = runtime.nodes().register_internal(TxSink);
    runtime
        .nodes()
        .register_internal(InterfaceOutputNode);
    let devices = DeviceMain::new(runtime.nodes().clone());
    let first_worker = DataWorkerId::new(0);
    let second_worker = DataWorkerId::new(1);
    let device = devices
        .register_device(9, sink, sink)
        .expect("register device");

    devices
        .register_rx_queue(
            device.instance,
            0,
            first_worker,
            DriverScheduleMode::Interrupt,
        )
        .expect("register RX queue");
    devices
        .register_tx_queue(device.instance, 0, first_worker)
        .expect("register TX queue");
    devices
        .assign_tx_queue_to_worker(device.instance, 0, second_worker)
        .expect("share TX queue with second worker");

    let tx_queue = devices
        .tx_queues_for_interface(9)
        .pop()
        .expect("interface TX queue");
    assert_eq!(tx_queue.device_instance, device.instance);
    assert_eq!(tx_queue.queue_id, 0);
    assert!(tx_queue.is_assigned_to(first_worker));
    assert!(tx_queue.is_assigned_to(second_worker));
    assert!(tx_queue.is_shared());
    assert!(devices.tx_queues_for_interface(10).is_empty());
    assert_eq!(
        devices.tx_queues_for_worker(first_worker),
        vec![tx_queue.clone()]
    );
    assert_eq!(devices.tx_queues_for_worker(second_worker), vec![tx_queue]);
    assert!(
        devices
            .tx_queues_for_worker(DataWorkerId::new(2))
            .is_empty()
    );
}

#[test]
fn device_main_allocates_instances_and_rejects_unregistered_queues() {
    let runtime = Worker::default().create_runtime().expect("runtime");
    runtime.nodes().register_internal(DropNode::new());
    let sink = runtime.nodes().register_internal(TxSink);
    runtime
        .nodes()
        .register_internal(InterfaceOutputNode);
    let devices = DeviceMain::new(runtime.nodes().clone());
    assert!(
        devices
            .register_rx_queue(0, 0, DataWorkerId::new(0), DriverScheduleMode::Poll)
            .is_err()
    );
    assert!(
        devices
            .register_tx_queue(0, 0, DataWorkerId::new(0))
            .is_err()
    );

    let first = devices
        .register_device(1, sink, sink)
        .expect("register first device");
    let second = devices
        .register_device(2, sink, sink)
        .expect("register second device");
    assert_eq!(first.instance, 0);
    assert_eq!(second.instance, 1);
    assert_eq!(devices.device(first.instance), Some(first));
    devices
        .register_tx_queue(first.instance, 0, DataWorkerId::new(0))
        .expect("register first TX queue");
}
