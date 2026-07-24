use std::mem::transmute;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use hammer_core::data_plane::{BufferFrame, NodeRegistration};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, InternalNode, Node, NodeResult, RuntimeRegistry,
    TraceControlPlane, TraceInputPolicy, TracePolicy, config::Worker, spawn::DataRuntime,
};
use hammer_service::device::DeviceMain;
use hammer_service::interface::{
    InterfaceControlPlane, InterfaceMtu, InterfaceMtuKind, InterfaceOutputNode,
    InterfaceOutputTrace,
};
use hammer_service::opaque::NetworkOpaque;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

#[derive(Clone, Copy)]
struct InterfaceTxSinkNode;

impl Node for InterfaceTxSinkNode {
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

impl InternalNode for InterfaceTxSinkNode {
    fn node_registration(&self) -> NodeRegistration {
        NodeRegistration::next("interface-test-tx", 0)
    }
}

fn test_runtime() -> DataPlaneRuntime {
    Worker::default()
        .create_runtime()
        .expect("create worker runtime")
}

#[test]
fn control_plane_publishes_interfaces_and_addresses_through_handle() {
    let control = InterfaceControlPlane::new();
    let handle = control.handle();

    let tun0 = control.register_interface("tun0").expect("register tun0");
    let tun1 = control.register_interface("tun1").expect("register tun1");
    let tun0_again = control
        .register_interface("tun0")
        .expect("register tun0 again");

    assert_eq!(tun0, 0);
    assert_eq!(tun1, 1);
    assert_eq!(tun0_again, tun0);
    assert_eq!(handle.interface_index("tun0"), Some(tun0));
    assert_eq!(handle.interface_index("tun1"), Some(tun1));
    assert_eq!(handle.interface_name(tun0), Some("tun0".to_owned()));

    let v4 = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 24).unwrap());
    let v6 = IpNet::V6(Ipv6Net::new(Ipv6Addr::LOCALHOST, 128).unwrap());
    let v4_index = control.add_address(tun0, v4).expect("add IPv4 address");
    let v6_index = control.add_address(tun0, v6).expect("add IPv6 address");
    let v4_again = control
        .add_address(tun0, v4)
        .expect("add duplicate IPv4 address");

    assert_eq!(v4_index, 0);
    assert_eq!(v6_index, 1);
    assert_eq!(v4_again, v4_index);
    assert_eq!(handle.interface_addresses(tun0), vec![v4, v6]);
    assert_eq!(handle.interface_address_index(tun0, v4), Some(v4_index));
    assert_eq!(handle.interface_address_index(tun0, v6), Some(v6_index));

    assert!(
        control
            .remove_address(tun0, v4)
            .expect("remove IPv4 address")
    );

    assert_eq!(handle.interface_addresses(tun0), vec![v6]);
    assert_eq!(handle.interface_address_index(tun0, v4), None);
}

#[test]
fn control_plane_rejects_addresses_for_missing_interfaces() {
    let control = InterfaceControlPlane::new();
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 24).unwrap());

    let err = control
        .add_address(99, address)
        .expect_err("missing interface should be rejected");

    assert!(err.to_string().contains("interface 99 is not registered"));
}

#[test]
fn control_plane_publishes_interface_mtu_updates_through_handle() {
    let control = InterfaceControlPlane::new();
    let handle = control.handle();
    let tun0 = control.register_interface("tun0").expect("register tun0");

    assert_eq!(handle.interface_mtu(tun0), Some(InterfaceMtu::default()));

    let mtu = InterfaceMtu::new(9000, 1500, 1280, 0);
    control.set_mtu(tun0, mtu).expect("set interface MTU");

    assert_eq!(handle.interface_mtu(tun0), Some(mtu));

    control
        .set_protocol_mtu(tun0, InterfaceMtuKind::Ip6, 1452)
        .expect("set IPv6 MTU");

    let mtu = handle.interface_mtu(tun0).expect("interface MTU");
    assert_eq!(mtu.l3(), 9000);
    assert_eq!(mtu.ip4(), 1500);
    assert_eq!(mtu.ip6(), 1452);
    assert_eq!(mtu.mpls(), 0);
}

#[test]
fn control_plane_rejects_mtu_updates_for_missing_interfaces() {
    let control = InterfaceControlPlane::new();
    let err = control
        .set_protocol_mtu(99, InterfaceMtuKind::L3, 1500)
        .expect_err("missing interface should be rejected");

    assert!(err.to_string().contains("interface 99 is not registered"));
}

#[test]
fn interface_mtu_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "interface-mtu-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let control = InterfaceControlPlane::new().with_data_plane_barrier(barrier.clone());
    let handle = control.handle();
    let tun0 = control.register_interface("tun0").expect("register tun0");

    control
        .set_protocol_mtu(tun0, InterfaceMtuKind::L3, 9000)
        .expect("set L3 MTU");

    assert_eq!(handle.interface_mtu(tun0).map(|mtu| mtu.l3()), Some(9000));
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn interface_output_dispatches_to_registered_tx_node() {
    let engine = Engine::new(test_runtime(), RuntimeRegistry::new());
    let _ = engine
        .runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let tx = engine
        .runtime
        .nodes()
        .register_internal(InterfaceTxSinkNode);
    let output_node = engine
        .runtime
        .nodes()
        .register_internal(InterfaceOutputNode);
    let devices = DeviceMain::new(engine.runtime.nodes().clone());
    let device = devices.register_device(0, tx, tx).expect("register device");
    devices
        .register_tx_queue(device.instance, 0, DataWorkerId::new(0))
        .expect("register TX queue");
    let worker = engine.spawn(1).expect("worker");
    devices.install_worker_output_runtime(DataWorkerId::new(0));
    let runtime = &worker.runtime;
    let trace = TraceControlPlane::new(8);
    trace.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: output_node,
            count: 1,
        }]
        .into(),
    });
    runtime.set_trace_control(Some(trace.handle()));
    let mut frame = runtime
        .buffers()
        .get_next_frame(output_node)
        .expect("alloc frame");
    let packet = ipv4_packet([10, 0, 0, 1], [198, 51, 100, 7], b"interface-output");
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        let opaque = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        *opaque = NetworkOpaque::default();
        opaque.sw_if_index[1] = 0;
    }
    runtime
        .try_mark_trace(output_node, index)
        .expect("mark packet");
    frame.push_index(index).expect("push packet");

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        runtime
            .nodes()
            .node_runtime_stats_snapshot()
            .into_iter()
            .find(|stats| stats.node_id == tx)
            .map(|stats| stats.vectors),
        Some(1)
    );
    assert_eq!(trace.drain_completed(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, output_node);
    assert_eq!(records[0].entries.len(), 1);
    assert_eq!(records[0].entries[0].node_name, Some("interface-output"));
    assert!(!records[0].entries[0].payload_bytes.is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn interface_output_drops_missing_egress_or_tx_mapping() {
    let engine = Engine::new(test_runtime(), RuntimeRegistry::new());
    let _ = engine
        .runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let tx = engine
        .runtime
        .nodes()
        .register_internal(InterfaceTxSinkNode);
    let output_node = engine
        .runtime
        .nodes()
        .register_internal(InterfaceOutputNode);
    let devices = DeviceMain::new(engine.runtime.nodes().clone());
    let device = devices.register_device(7, tx, tx).expect("register device");
    devices
        .register_tx_queue(device.instance, 0, DataWorkerId::new(0))
        .expect("register TX queue");
    let worker = engine.spawn(1).expect("worker");
    devices.install_worker_output_runtime(DataWorkerId::new(0));
    let runtime = &worker.runtime;
    let mut frame = runtime
        .buffers()
        .get_next_frame(output_node)
        .expect("alloc frame");
    push_packet_with_egress(runtime, &mut frame, None, b"no-egress");
    push_packet_with_egress(runtime, &mut frame, Some(99), b"no-tx");

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn push_packet_with_egress(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    egress_interface: Option<u32>,
    payload: &[u8],
) {
    let packet = ipv4_packet([10, 0, 0, 1], [198, 51, 100, 7], payload);
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
    let opaque = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
    *opaque = NetworkOpaque::default();
    if let Some(egress_interface) = egress_interface {
        opaque.sw_if_index[1] = egress_interface;
    }
    frame.push_index(index).expect("push packet");
}

fn ipv4_packet(source: [u8; 4], destination: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let total_len = 20 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 59;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    let checksum = ipv4_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet[20..].copy_from_slice(payload);
    packet
}

fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
