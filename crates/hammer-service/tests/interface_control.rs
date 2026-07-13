use std::mem::transmute;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use hammer_core::config::Config;
use hammer_runtime::{DataPlaneRuntime, TraceControlPlane, TraceInputPolicy, TracePolicy};
use hammer_runtime::{new_worker_runtime, spawn::DataRuntime};
use hammer_service::interface::{
    InterfaceConnectedRouteControl, InterfaceControlPlane, InterfaceMtu, InterfaceMtuKind,
    InterfaceOutputControlPlane, InterfaceOutputTrace,
};
use hammer_service::net::{
    AdjacencyRewriteNode, DpoType, FibTableBuilder, IpLocalControlPlane, IpLocalNext,
    IpLookupControlPlane, NetworkOpaque,
};
use hammer_service::tun::{MemoryTunDevice, TunMain, TunOutputDriverNode, TunOutputTrace};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

fn test_runtime(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_pool_size: usize,
) -> DataPlaneRuntime {
    let mut config = Config::default();
    config.worker.buffer.slot_bytes = buffer_slot_capacity;
    config.worker.buffer.slots_per_numa = buffer_slots;
    config.worker.buffer.frame_pool_size = frame_pool_size;
    new_worker_runtime(&config).expect("create worker runtime")
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
fn interface_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "interface-control-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let lookup_table = IpLookupControlPlane::new(FibTableBuilder::new(u16::MAX).build());
    let control = InterfaceControlPlane::new()
        .with_data_plane_barrier(barrier.clone())
        .with_connected_routes(InterfaceConnectedRouteControl::new(
            lookup_table.table_handle(),
            0,
            1,
        ));
    let tun0 = control.register_interface("tun0").expect("register tun0");
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 24).unwrap());

    control.add_address(tun0, address).expect("add address");
    control
        .remove_address(tun0, address)
        .expect("remove address");

    assert!(control.handle().interface_addresses(tun0).is_empty());
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
    let runtime = test_runtime(2048, 8, 4);
    let drop = runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let lookup_control = IpLookupControlPlane::new(FibTableBuilder::new(u16::MAX).build());
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
    let drop_slot = runtime
        .nodes()
        .add_node_next_slot(lookup, drop)
        .expect("drop next");
    let receive_slot = runtime
        .nodes()
        .add_node_next_slot(lookup, receive)
        .expect("receive next");
    let rewrite_slot = runtime
        .nodes()
        .add_node_next_slot(lookup, adjacency_rewrite)
        .expect("rewrite next");
    let output_slot = runtime
        .nodes()
        .add_node_next_slot(adjacency_rewrite, interface_output)
        .expect("output next");
    let control = InterfaceControlPlane::new().with_connected_routes(
        InterfaceConnectedRouteControl::new(lookup_control.table_handle(), drop_slot, receive_slot)
            .with_connected_adjacency(rewrite_slot, output_slot),
    );
    let tun0 = control.register_interface("tun0").expect("register tun0");
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 255, 0, 1), 24).unwrap());

    control.add_address(tun0, address).expect("add address");

    let mut frame = runtime
        .buffers()
        .get_next_frame(lookup)
        .expect("alloc frame");
    let packet = ipv4_packet([10, 255, 0, 2], [10, 255, 0, 1], b"receive");
    let index = runtime
        .alloc_index_with_bytes(&packet)
        .expect("alloc packet");
    {
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
        let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        network.set_packet_cursor(
            hammer_core::data_plane::BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, 20)
                .with_transport_header(20, 8)
                .with_transport_payload_offset(28),
        );
        let ip = network.ip_mut();
        ip.set_ip_version(Some(4));
        ip.set_ip_protocol(Some(59));
    }
    frame.push_index(index).expect("push packet");

    runtime.put_next_frame(frame).expect("schedule");
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
    assert_eq!(connected.dpo.next(), rewrite_slot);
    let adjacency = lookup_control
        .table_handle()
        .table()
        .adjacency(connected.dpo.adjacency_index().expect("adjacency index"))
        .expect("adjacency entry");
    assert_eq!(adjacency.egress_interface, Some(tun0));
    assert_eq!(adjacency.next, output_slot);

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
    let runtime = test_runtime(2048, 8, 4);
    let _ = runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let output_device = MemoryTunDevice::new();
    let tun_main = TunMain::default_main();
    let tx = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new_with_main(
            tun_main.clone(),
            output_device.output(),
        ));
    let mut output_control = InterfaceOutputControlPlane::new().with_nodes(runtime.nodes().clone());
    let output_node = runtime.nodes().register_internal(output_control.node());
    output_control
        .attach_consumer(output_node)
        .expect("attach interface output");
    let tx_slot = output_control.register_tx(7, tx).expect("register tx node");
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
        opaque.sw_if_index[1] = 7;
    }
    runtime
        .try_mark_trace(output_node, index)
        .expect("mark packet");
    frame.push_index(index).expect("push packet");

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(output_device.drain_output(), vec![packet]);
    assert_eq!(trace.drain_completed(), 1);
    let records = trace.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, output_node);
    assert_eq!(records[0].entries.len(), 2);
    assert_eq!(records[0].entries[0].node_name, Some("interface-output"));
    assert_eq!(
        InterfaceOutputTrace::decode(&records[0].entries[0].payload_bytes)
            .expect("interface output trace"),
        InterfaceOutputTrace {
            egress_interface: Some(7),
            tx_next: Some(tx_slot),
            error: None,
            next: Some(tx_slot),
        }
    );
    assert_eq!(records[0].entries[1].node_name, Some("tun-output-driver"));
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
    let runtime = test_runtime(2048, 8, 4);
    let _ = runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let output_device = MemoryTunDevice::new();
    let mut output_control = InterfaceOutputControlPlane::new().with_nodes(runtime.nodes().clone());
    let output_node = runtime.nodes().register_internal(output_control.node());
    output_control
        .attach_consumer(output_node)
        .expect("attach interface output");
    let mut frame = runtime
        .buffers()
        .get_next_frame(output_node)
        .expect("alloc frame");
    push_packet_with_egress(&runtime, &mut frame, None, b"no-egress");
    push_packet_with_egress(&runtime, &mut frame, Some(99), b"no-tx");

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(output_device.drain_output().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn interface_output_tx_updates_run_through_configured_runtime_data_plane_barrier() {
    let data_runtime =
        DataRuntime::new(1, "interface-output-barrier-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = test_runtime(2048, 8, 4);
    let _ = runtime
        .nodes()
        .register_internal(hammer_service::data_plane::DropNode::new());
    let device = MemoryTunDevice::new();
    let tx = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let mut output_control = InterfaceOutputControlPlane::new()
        .with_data_plane_barrier(barrier.clone())
        .with_nodes(runtime.nodes().clone());
    let output_node = runtime.nodes().register_internal(output_control.node());
    output_control
        .attach_consumer(output_node)
        .expect("attach interface output");
    let output_handle = output_control.handle();

    let tx_slot = output_control.register_tx(7, tx).expect("register tx");
    assert_eq!(output_handle.tx_slot(7), Some(tx_slot));
    output_control.unregister_tx(7).expect("unregister tx");

    assert_eq!(output_handle.tx_slot(7), None);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn interface_output_and_tun_output_avoid_packet_copy_shortcuts() {
    let interface_source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/interface.rs"))
            .expect("read interface source");
    let tun_source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/tun/mod.rs"))
            .expect("read TUN source");

    for forbidden in ["chain_bytes", "copy_packet", "batch_cursor"] {
        assert!(
            !interface_source.contains(forbidden),
            "interface-output must not use {forbidden}"
        );
        assert!(
            !tun_source.contains(forbidden),
            "TUN output must not use {forbidden}"
        );
    }
}

fn push_packet_with_egress(
    runtime: &DataPlaneRuntime,
    frame: &mut hammer_core::data_plane::BufferFrame,
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
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
