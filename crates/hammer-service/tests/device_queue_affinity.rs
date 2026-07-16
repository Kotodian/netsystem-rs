use hammer_core::data_plane::NodeId;
use hammer_runtime::DataWorkerId;
use hammer_service::device::{DeviceMain, DriverScheduleMode};

#[test]
fn tx_queue_affinity_is_published_by_interface_and_owner() {
    let devices = DeviceMain::new();
    let first_worker = DataWorkerId::new(0);
    let second_worker = DataWorkerId::new(1);

    devices
        .register_rx_queue(4, 0, first_worker, DriverScheduleMode::Interrupt)
        .expect("register RX queue");
    devices
        .register_tx_queue(9, 4, 0, first_worker, NodeId::new(12))
        .expect("register TX queue");
    devices
        .assign_tx_queue_to_worker(4, 0, second_worker)
        .expect("share TX queue with second worker");

    let tx_queue = devices.tx_queues_for_interface(9).pop().expect("interface TX queue");
    assert_eq!(tx_queue.device_instance, 4);
    assert_eq!(tx_queue.queue_id, 0);
    assert_eq!(tx_queue.output_node, NodeId::new(12));
    assert_eq!(tx_queue.assigned_workers(), &[first_worker, second_worker]);
    assert!(tx_queue.is_shared());
    assert!(devices.tx_queues_for_interface(10).is_empty());
    assert_eq!(devices.tx_queues_for_worker(first_worker), vec![tx_queue.clone()]);
    assert_eq!(devices.tx_queues_for_worker(second_worker), vec![tx_queue]);
    assert!(devices.tx_queues_for_worker(DataWorkerId::new(2)).is_empty());
}
