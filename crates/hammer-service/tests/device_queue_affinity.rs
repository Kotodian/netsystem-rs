use hammer_core::data_plane::NodeId;
use hammer_runtime::DataWorkerId;
use hammer_service::device::{DeviceMain, DriverScheduleMode};

#[test]
fn tx_queue_affinity_is_published_by_interface_and_owner() {
    let devices = DeviceMain::new();
    let first_worker = DataWorkerId::new(0);
    let second_worker = DataWorkerId::new(1);
    let device = devices
        .register_device(9, NodeId::new(11), NodeId::new(12))
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
        .register_tx_queue(9, device.instance, 0, first_worker, NodeId::new(12))
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
    assert_eq!(tx_queue.output_node, NodeId::new(12));
    assert_eq!(tx_queue.assigned_workers(), &[first_worker, second_worker]);
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
    let devices = DeviceMain::new();
    assert!(
        devices
            .register_rx_queue(0, 0, DataWorkerId::new(0), DriverScheduleMode::Poll)
            .is_err()
    );

    let first = devices
        .register_device(1, NodeId::new(2), NodeId::new(3))
        .expect("register first device");
    let second = devices
        .register_device(2, NodeId::new(4), NodeId::new(5))
        .expect("register second device");
    assert_eq!(first.instance, 0);
    assert_eq!(second.instance, 1);
    assert_eq!(devices.device(first.instance), Some(first));
    assert!(
        devices
            .register_tx_queue(2, first.instance, 0, DataWorkerId::new(0), NodeId::new(5))
            .is_err()
    );
}
