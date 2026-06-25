use std::mem::transmute;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use hammer_adapter::{
    DataPlaneRuntime, NetworkOpaque, TraceControlPlane, TraceInputPolicy, TracePolicy,
};
use hammer_runtime::spawn::DataRuntime;
use hammer_service::interface::{
    InterfaceConnectedRouteControl, InterfaceControlPlane, InterfaceMtu, InterfaceMtuKind,
    InterfaceOutputControlPlane, InterfaceOutputTrace,
};
use hammer_service::net::{
    AdjacencyRewriteNode, DpoType, FibTableBuilder, IpLocalControlPlane, IpLocalNext,
    IpLookupControlPlane,
};
use hammer_service::tun::{MemoryTunDevice, TunOutputDriverNode, TunOutputTrace};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

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
    let tun0 = control.register_interface("tun0").expect("register tun0");

    control
        .set_protocol_mtu(tun0, InterfaceMtuKind::L3, 9000)
        .expect("set L3 MTU");

    assert_eq!(barrier.sync_count(), 2);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn interface_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "interface-control-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let lookup_table =
        IpLookupControlPlane::new(FibTableBuilder::new(hammer_adapter::NodeId::new(0)).build());
    let control = InterfaceControlPlane::new()
        .with_data_plane_barrier(barrier.clone())
        .with_connected_routes(InterfaceConnectedRouteControl::new(
            lookup_table.table_handle(),
            hammer_adapter::NodeId::new(0),
            hammer_adapter::NodeId::new(1),
        ));
    let tun0 = control.register_interface("tun0").expect("register tun0");
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 24).unwrap());

    control.add_address(tun0, address).expect("add address");
    control
        .remove_address(tun0, address)
        .expect("remove address");

    assert_eq!(barrier.sync_count(), 3);
    assert!(
        lookup_table
            .table_handle()
            .table()
            .lookup_ip4(Ipv4Addr::new(10, 0, 0, 1), 0)
            .is_none()
    );
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn interface_address_publish_installs_receive_route_in_fib() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let drop = runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let lookup_control = IpLookupControlPlane::new(FibTableBuilder::new(drop).build());
    let adjacency_rewrite = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(lookup_control.table_handle()));
    let output_control = InterfaceOutputControlPlane::new();
    let interface_output = runtime.nodes().register_internal(output_control.node());
    let local_control =
        IpLocalControlPlane::new(IpLocalNext::nodes(drop, drop, drop, drop, drop, drop));
    runtime.nodes().register_internal(local_control.node());
    let receive = runtime
        .nodes()
        .register_internal(local_control.receive_node());
    let lookup = runtime.nodes().register_internal(lookup_control.node());
    let control = InterfaceControlPlane::new().with_connected_routes(
        InterfaceConnectedRouteControl::new(lookup_control.table_handle(), drop, receive)
            .with_connected_adjacency(adjacency_rewrite, interface_output),
    );
    let tun0 = control.register_interface("tun0").expect("register tun0");
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 255, 0, 1), 24).unwrap());

    control.add_address(tun0, address).expect("add address");

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_packet([10, 255, 0, 2], [10, 255, 0, 1], b"receive");
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));
    assert!(runtime.run_ready_nodes().expect("run nodes") >= 2);
    let stats = runtime.nodes().node_runtime_stats_snapshot();
    assert_eq!(
        stats
            .iter()
            .find(|row| row.node_id == receive)
            .map(|row| row.vectors),
        Some(1)
    );
    let lookup_stats = runtime
        .nodes()
        .node_runtime_stats_snapshot()
        .into_iter()
        .find(|row| row.node_id == lookup)
        .expect("lookup stats");
    assert_eq!(lookup_stats.vectors, 1);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);

    let connected = lookup_control
        .table_handle()
        .table()
        .lookup_ip4(Ipv4Addr::new(10, 255, 0, 2), 0)
        .expect("connected route lookup");
    assert_eq!(connected.dpo.kind(), DpoType::ADJACENCY);
    assert_eq!(connected.dpo.next(), adjacency_rewrite);
    let adjacency = lookup_control
        .table_handle()
        .table()
        .adjacency(connected.dpo.adjacency_index().expect("adjacency index"))
        .expect("adjacency entry");
    assert_eq!(adjacency.egress_interface, Some(tun0));
    assert_eq!(adjacency.next, interface_output);

    control
        .remove_address(tun0, address)
        .expect("remove address");
    let missing = lookup_control
        .table_handle()
        .table()
        .lookup_ip4(Ipv4Addr::new(10, 255, 0, 1), 0);
    assert!(missing.is_none());
}

#[test]
fn interface_output_dispatches_to_registered_tx_node() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let output_device = MemoryTunDevice::new();
    let tx = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(output_device.output()));
    let output_control = InterfaceOutputControlPlane::new();
    output_control.register_tx(7, tx).expect("register tx node");
    let output_node = runtime.nodes().register_internal(output_control.node());
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
    runtime.set_trace_control(Some(trace.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_packet([10, 0, 0, 1], [198, 51, 100, 7], b"interface-output");
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        let opaque = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        opaque.sw_if_index[1] = 7;
    }
    runtime
        .try_mark_trace(output_node, index)
        .expect("mark packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");

    assert!(
        runtime
            .schedule_frame(output_node, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(output_device.drain_output(), vec![packet]);
    assert_eq!(trace.drain_completed(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, output_node);
    assert_eq!(records[0].entries.len(), 2);
    assert_eq!(
        records[0].entries[0].node_name,
        Some("interface-output-node")
    );
    assert_eq!(
        InterfaceOutputTrace::decode(&records[0].entries[0].payload_bytes)
            .expect("interface output trace"),
        InterfaceOutputTrace {
            egress_interface: Some(7),
            tx_node: Some(tx),
            error: None,
            next: Some(tx),
        }
    );
    assert_eq!(
        records[0].entries[1].node_name,
        Some("tun-output-driver-node")
    );
    assert_eq!(
        TunOutputTrace::decode(&records[0].entries[1].payload_bytes).expect("tun output trace"),
        TunOutputTrace {
            mode: hammer_service::tun::TunDriverMode::Tun,
            pending: 1,
        }
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn interface_output_drops_missing_egress_or_tx_mapping() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let output_device = MemoryTunDevice::new();
    let output_control = InterfaceOutputControlPlane::new();
    let output_node = runtime.nodes().register_internal(output_control.node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet_with_egress(&runtime, frame, None, b"no-egress");
    push_packet_with_egress(&runtime, frame, Some(99), b"no-tx");

    assert!(
        runtime
            .schedule_frame(output_node, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert!(output_device.drain_output().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn interface_output_tx_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "interface-output-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let tx = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let output_control =
        InterfaceOutputControlPlane::new().with_data_plane_barrier(barrier.clone());

    output_control.register_tx(7, tx).expect("register tx");
    output_control.unregister_tx(7).expect("unregister tx");

    assert_eq!(barrier.sync_count(), 2);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

fn push_packet_with_egress(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    egress_interface: Option<u32>,
    payload: &[u8],
) {
    let packet = ipv4_packet([10, 0, 0, 1], [198, 51, 100, 7], payload);
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    if let Some(egress_interface) = egress_interface {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        let opaque = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        opaque.sw_if_index[1] = egress_interface;
    }
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
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
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
