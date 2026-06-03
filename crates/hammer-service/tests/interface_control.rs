use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use hammer_runtime::spawn::DataRuntime;
use hammer_service::interface::{InterfaceControlPlane, InterfaceMtu, InterfaceMtuKind};
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
    let control = InterfaceControlPlane::new().with_data_plane_barrier(barrier.clone());
    let tun0 = control.register_interface("tun0").expect("register tun0");
    let address = IpNet::V4(Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 24).unwrap());

    control.add_address(tun0, address).expect("add address");
    control
        .remove_address(tun0, address)
        .expect("remove address");

    assert_eq!(barrier.sync_count(), 3);
    data_runtime.shutdown_timeout(Duration::from_secs(1));
}
