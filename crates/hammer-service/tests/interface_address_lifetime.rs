use hammer_runtime::ThreadMain;
use hammer_service::interface::{InterfaceError, InterfaceMain};
use ipnet::IpNet;

#[test]
fn address_removal_preserves_other_interface_addresses() {
    hammer_runtime::config::Memory::default()
        .ensure_main_heap()
        .unwrap();
    hammer_core::buffer::BufferMain::new(64, 1024, &[0], 2, hammer_infra::PageSize::Default)
        .unwrap();
    ThreadMain::new().unwrap();
    let interfaces = InterfaceMain::new();
    let ingress = interfaces.register_hardware_interface(0, 1, 0, 0).unwrap();
    let egress = interfaces.register_hardware_interface(0, 2, 0, 0).unwrap();
    let ingress_sw = interfaces.hardware_interface(ingress).unwrap().sw_if_index;
    let egress_sw = interfaces.hardware_interface(egress).unwrap().sw_if_index;
    let ip4: IpNet = "192.0.2.1/24".parse().unwrap();
    let ip6: IpNet = "2001:db8::1/64".parse().unwrap();
    let peer: IpNet = "198.51.100.1/24".parse().unwrap();

    // VPP ip_interface_address_add/del preserve surviving pool identities.
    // test_ip4.py/test_ip6.py also retain a second address after deleting one.
    let ip4_index = interfaces.add_address(ingress_sw, ip4).unwrap();
    let peer_index = interfaces.add_address(egress_sw, peer).unwrap();
    let ip6_index = interfaces.add_address(ingress_sw, ip6).unwrap();
    assert_eq!(interfaces.add_address(ingress_sw, ip6).unwrap(), ip6_index);
    assert_eq!(interfaces.interface_addresses(ingress_sw), [ip4, ip6]);
    assert!(interfaces.remove_address(ingress_sw, ip4).unwrap());
    assert!(!interfaces.remove_address(ingress_sw, ip4).unwrap());
    assert_eq!(interfaces.interface_address(ip4_index), None);
    assert_eq!(interfaces.interface_address(peer_index), Some(peer));
    assert_eq!(interfaces.interface_address(ip6_index), Some(ip6));
    assert_eq!(
        interfaces.software_interface(ingress_sw).unwrap().addresses,
        [ip6_index]
    );
    assert_eq!(
        interfaces.interface_address_index(ingress_sw, ip6),
        Some(ip6_index)
    );

    let replacement_index = interfaces.add_address(egress_sw, ip4).unwrap();
    assert_eq!(interfaces.interface_address(replacement_index), Some(ip4));
    assert_eq!(interfaces.interface_addresses(egress_sw), [peer, ip4]);
    assert_eq!(interfaces.interface_addresses(ingress_sw), [ip6]);
    assert!(matches!(
        interfaces.add_address(u32::MAX, ip6),
        Err(InterfaceError::NotRegistered {
            interface_index: u32::MAX
        })
    ));
    assert_eq!(interfaces.interface_address_index(u32::MAX, ip6), None);

    interfaces.delete_hardware_interface(egress).unwrap();
    assert_eq!(interfaces.interface_address(peer_index), None);
    assert_eq!(interfaces.interface_address(replacement_index), None);
    assert_eq!(interfaces.interface_address(ip6_index), Some(ip6));
    assert_eq!(interfaces.interface_addresses(ingress_sw), [ip6]);
    interfaces.delete_hardware_interface(ingress).unwrap();
    assert_eq!(interfaces.interface_address(ip6_index), None);
}
