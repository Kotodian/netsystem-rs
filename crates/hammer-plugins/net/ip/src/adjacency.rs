use std::mem::{align_of, offset_of, size_of};
use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::data_plane::{Frame, NodeId, NodeNext};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_runtime::{DataPlaneMain, Node, NodeRuntime, RuntimeResult};
use hammer_service::feature::FeatureMain;
use hammer_service::interface::{
    InterfaceCallbackRegistration, InterfaceMain, InterfaceMtuKind, InterfaceResult,
};
use hammer_service::net::adj::{Adjacency, FibProtocol, FibProtocolId};
use hammer_service::net::adj_glean::AdjacencyGleanMain;
use hammer_service::net::adj_nbr::AdjacencyNeighborMain;
use hammer_service::net::fib_node::{FibNodeList, FibNodeOperations, FibWalkReason};
use hammer_service::net::rewrite::REWRITE_HAS_FEATURES;
use hammer_service::net::{DpoId, DpoProto, DpoType, NetMain};
use hammer_service::opaque::{NetworkFlags, NetworkOpaque};
use ipnet::{Ipv4Net, Ipv6Net};
use rand_09::RngCore;
use zerocopy::FromBytes;

use crate::interface::{ip4_source_address, ip6_multicast_adjacency, ip6_source_address};
use crate::lookup::{IP4_MAIN, IP6_MAIN, IpSecondaryOpaque};
use crate::protocol::icmp::IcmpErrorMetadata;
use crate::protocol::ip::{Ipv4Header, Ipv4MtuAction, Ipv6Header, ipv4_mtu_check};
use hammer_service::net::adj::{AdjacencyIndex, AdjacencyLookupNext, AdjacencyMain};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IpNetLink {
    Ip4,
    Ip6,
}

impl IpNetLink {
    fn dpo_protocol(self) -> DpoProto {
        match self {
            Self::Ip4 => DpoProto::IP4,
            Self::Ip6 => DpoProto::IP6,
        }
    }

    fn mtu_kind(self) -> InterfaceMtuKind {
        match self {
            Self::Ip4 => InterfaceMtuKind::Ip4,
            Self::Ip6 => InterfaceMtuKind::Ip6,
        }
    }

    fn build_rewrite(
        self,
        interfaces: &InterfaceMain,
        sw_if_index: u32,
        destination: Option<&[u8]>,
        output: &mut [u8],
    ) -> usize {
        let ethernet_type = match self {
            Self::Ip4 => 0x0800,
            Self::Ip6 => 0x86dd,
        };
        interfaces.build_rewrite(sw_if_index, Some(ethernet_type), destination, output)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IpFibPrefix {
    length: u16,
    protocol: u8,
    reserved: u8,
    address: Ip46Address,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Ip4Address {
    reserved: [u8; 12],
    address: [u8; 4],
}

#[repr(C)]
#[derive(Clone, Copy)]
union Ip46Address {
    ip4: Ip4Address,
    ip6: [u8; 16],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IpGleanSubtype {
    connected: IpFibPrefix,
    source: Ip46Address,
    reserved: [u8; 12],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IpNeighborSubtype {
    next_hop: Ip46Address,
    reserved: [u8; 32],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub union IpAdjacency {
    glean: IpGleanSubtype,
    neighbor: IpNeighborSubtype,
}

#[derive(Clone, Copy)]
pub struct Ip4FibProtocol;

#[derive(Clone, Copy)]
pub struct Ip6FibProtocol;

impl FibProtocol for Ip4FibProtocol {
    type Address = Ipv4Addr;
    type Prefix = Ipv4Net;
    type AdjacencySubtype = IpAdjacency;
    type Link = IpNetLink;

    const ID: FibProtocolId = FibProtocolId::IP4;
    const DPO_PROTOCOL: DpoProto = DpoProto::IP4;

    fn prefix_address(prefix: Ipv4Net) -> Ipv4Addr {
        prefix.network()
    }
    fn zero_address() -> Ipv4Addr {
        Ipv4Addr::UNSPECIFIED
    }
    fn glean_subtype(connected: Ipv4Net) -> Self::AdjacencySubtype {
        let address = Ip46Address {
            ip4: Ip4Address {
                reserved: [0; 12],
                address: connected.network().octets(),
            },
        };
        IpAdjacency {
            glean: IpGleanSubtype {
                connected: IpFibPrefix {
                    length: u16::from(connected.prefix_len()),
                    protocol: 0,
                    reserved: 0,
                    address,
                },
                source: address,
                reserved: [0; 12],
            },
        }
    }
    fn neighbor_subtype(next_hop: Ipv4Addr) -> Self::AdjacencySubtype {
        IpAdjacency {
            neighbor: IpNeighborSubtype {
                next_hop: Ip46Address {
                    ip4: Ip4Address {
                        reserved: [0; 12],
                        address: next_hop.octets(),
                    },
                },
                reserved: [0; 32],
            },
        }
    }
    fn glean_prefix(subtype: &Self::AdjacencySubtype) -> Ipv4Net {
        let connected = unsafe { subtype.glean.connected };
        assert_eq!(connected.protocol, 0, "IP4 glean subtype required");
        Ipv4Net::new(
            Ipv4Addr::from(unsafe { connected.address.ip4.address }),
            connected.length as u8,
        )
        .expect("saved IP4 connected prefix is valid")
    }
    fn neighbor_address(subtype: &Self::AdjacencySubtype) -> Ipv4Addr {
        let next_hop = unsafe { subtype.neighbor.next_hop };
        Ipv4Addr::from(unsafe { next_hop.ip4.address })
    }
    fn set_glean_source(subtype: &mut Self::AdjacencySubtype, source: Ipv4Addr) {
        subtype.glean.source = Ip46Address {
            ip4: Ip4Address {
                reserved: [0; 12],
                address: source.octets(),
            },
        }
    }
    fn dpo_protocol(link: IpNetLink) -> DpoProto {
        link.dpo_protocol()
    }
    fn mtu_kind(link: IpNetLink) -> InterfaceMtuKind {
        link.mtu_kind()
    }
    fn build_rewrite(
        interfaces: &InterfaceMain,
        sw_if_index: u32,
        link: IpNetLink,
        destination: Option<&[u8]>,
        output: &mut [u8],
    ) -> usize {
        link.build_rewrite(interfaces, sw_if_index, destination, output)
    }
}

impl FibProtocol for Ip6FibProtocol {
    type Address = Ipv6Addr;
    type Prefix = Ipv6Net;
    type AdjacencySubtype = IpAdjacency;
    type Link = IpNetLink;

    const ID: FibProtocolId = FibProtocolId::IP6;
    const DPO_PROTOCOL: DpoProto = DpoProto::IP6;

    fn prefix_address(prefix: Ipv6Net) -> Ipv6Addr {
        prefix.network()
    }
    fn zero_address() -> Ipv6Addr {
        Ipv6Addr::UNSPECIFIED
    }
    fn glean_subtype(connected: Ipv6Net) -> Self::AdjacencySubtype {
        let address = Ip46Address {
            ip6: connected.network().octets(),
        };
        IpAdjacency {
            glean: IpGleanSubtype {
                connected: IpFibPrefix {
                    length: u16::from(connected.prefix_len()),
                    protocol: 1,
                    reserved: 0,
                    address,
                },
                source: address,
                reserved: [0; 12],
            },
        }
    }
    fn neighbor_subtype(next_hop: Ipv6Addr) -> Self::AdjacencySubtype {
        IpAdjacency {
            neighbor: IpNeighborSubtype {
                next_hop: Ip46Address {
                    ip6: next_hop.octets(),
                },
                reserved: [0; 32],
            },
        }
    }
    fn glean_prefix(subtype: &Self::AdjacencySubtype) -> Ipv6Net {
        let connected = unsafe { subtype.glean.connected };
        assert_eq!(connected.protocol, 1, "IP6 glean subtype required");
        Ipv6Net::new(
            Ipv6Addr::from(unsafe { connected.address.ip6 }),
            connected.length as u8,
        )
        .expect("saved IP6 connected prefix is valid")
    }
    fn neighbor_address(subtype: &Self::AdjacencySubtype) -> Ipv6Addr {
        Ipv6Addr::from(unsafe { subtype.neighbor.next_hop.ip6 })
    }
    fn set_glean_source(subtype: &mut Self::AdjacencySubtype, source: Ipv6Addr) {
        subtype.glean.source = Ip46Address {
            ip6: source.octets(),
        }
    }
    fn dpo_protocol(link: IpNetLink) -> DpoProto {
        link.dpo_protocol()
    }
    fn mtu_kind(link: IpNetLink) -> InterfaceMtuKind {
        link.mtu_kind()
    }
    fn build_rewrite(
        interfaces: &InterfaceMain,
        sw_if_index: u32,
        link: IpNetLink,
        destination: Option<&[u8]>,
        output: &mut [u8],
    ) -> usize {
        link.build_rewrite(interfaces, sw_if_index, destination, output)
    }
}

const _: () = assert!(size_of::<IpFibPrefix>() == 20);
const _: () = assert!(offset_of!(IpFibPrefix, address) == 4);
const _: () = assert!(size_of::<Ip46Address>() == 16);
const _: () = assert!(size_of::<IpAdjacency>() == 48);
const _: () = assert!(size_of::<Adjacency<Ip4FibProtocol>>() == 256);
const _: () = assert!(size_of::<Adjacency<Ip6FibProtocol>>() == 256);
const _: () = assert!(align_of::<Adjacency<Ip4FibProtocol>>() == 64);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, rewrite_header) == 64);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, rewrite_data) == 76);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, delegates) == 192);
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, node_index) == 200);

macro_rules! adjacency_node_operations {
    ($family:ident, $lock:ident, $unlock:ident, $children:ident, $set_children:ident, $last_lock:ident) => {
        fn $lock(index: u32) {
            let family = $family
                .get()
                .expect("IP family Main exists before adjacency references");
            unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner installed before DPO use")
                .node_lock(AdjacencyIndex(index));
        }

        fn $unlock(index: u32) -> bool {
            let family = $family
                .get()
                .expect("IP family Main exists before adjacency references");
            unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner installed before DPO use")
                .node_unlock(AdjacencyIndex(index))
        }

        fn $children(index: u32) -> FibNodeList {
            let family = $family
                .get()
                .expect("IP family Main exists before adjacency references");
            unsafe { &*family.adjacency.get() }
                .as_ref()
                .expect("adjacency owner installed before DPO use")
                .children(AdjacencyIndex(index))
        }

        fn $set_children(index: u32, children: FibNodeList) {
            let family = $family
                .get()
                .expect("IP family Main exists before adjacency references");
            unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner installed before DPO use")
                .set_children(AdjacencyIndex(index), children);
        }

        fn $last_lock(index: u32) {
            let net = NetMain::global().expect("adjacency retirement requires the network Main");
            let family = $family
                .get()
                .expect("IP family Main exists before adjacency retirement");
            let adjacency = unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner installed before DPO use");
            let index = AdjacencyIndex(index);
            net.adjacency_delegates_mut().deleted(adjacency, index);
            match adjacency.get(index).lookup_next {
                AdjacencyLookupNext::Glean => unsafe { &mut *family.glean.get() }
                    .as_mut()
                    .expect("glean owner installed before DPO use")
                    .remove(adjacency, index),
                AdjacencyLookupNext::Incomplete
                | AdjacencyLookupNext::Rewrite
                | AdjacencyLookupNext::Broadcast => unsafe { &mut *family.neighbor.get() }
                    .as_mut()
                    .expect("neighbor owner installed before DPO use")
                    .remove(adjacency, index),
                AdjacencyLookupNext::Multicast => {}
            }
            adjacency.release(index);
        }
    };
}

adjacency_node_operations!(
    IP4_MAIN,
    ip4_adj_lock,
    ip4_adj_unlock,
    ip4_adj_children,
    ip4_adj_set_children,
    ip4_adj_last_lock
);
adjacency_node_operations!(
    IP6_MAIN,
    ip6_adj_lock,
    ip6_adj_unlock,
    ip6_adj_children,
    ip6_adj_set_children,
    ip6_adj_last_lock
);

macro_rules! adjacency_interface_callbacks {
    ($family:ident, $glean_admin:ident, $neighbor_admin:ident, $glean_delete:ident, $neighbor_delete:ident, $neighbor_mtu:ident) => {
        fn $glean_admin(
            main: &mut DataPlaneMain,
            _: &InterfaceMain,
            sw_if_index: u32,
            is_up: bool,
        ) -> InterfaceResult<()> {
            let family = $family
                .get()
                .expect("IP family Main exists before interface callbacks");
            if unsafe { &*family.adjacency.get() }.is_none() {
                return Ok(());
            }
            let net = NetMain::global().expect("network Main exists before interface callbacks");
            let glean = unsafe { &*family.glean.get() }
                .as_ref()
                .expect("glean owner is installed");
            let adjacency = unsafe { &*family.adjacency.get() }
                .as_ref()
                .expect("adjacency owner is installed");
            let reason = if is_up {
                FibWalkReason::INTERFACE_UP
            } else {
                FibWalkReason::INTERFACE_DOWN
            };
            glean.interface_walk(
                main,
                &mut net.fib_nodes_mut(),
                adjacency,
                sw_if_index,
                reason,
            );
            Ok(())
        }

        fn $neighbor_admin(
            main: &mut DataPlaneMain,
            _: &InterfaceMain,
            sw_if_index: u32,
            is_up: bool,
        ) -> InterfaceResult<()> {
            let family = $family
                .get()
                .expect("IP family Main exists before interface callbacks");
            if unsafe { &*family.adjacency.get() }.is_none() {
                return Ok(());
            }
            let net = NetMain::global().expect("network Main exists before interface callbacks");
            let neighbor = unsafe { &*family.neighbor.get() }
                .as_ref()
                .expect("neighbor owner is installed");
            let adjacency = unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner is installed");
            let reason = if is_up {
                FibWalkReason::INTERFACE_UP
            } else {
                FibWalkReason::INTERFACE_DOWN
            };
            neighbor.interface_walk(
                main,
                &mut net.fib_nodes_mut(),
                adjacency,
                sw_if_index,
                reason,
            );
            Ok(())
        }

        fn $glean_delete(
            main: &mut DataPlaneMain,
            _: &InterfaceMain,
            sw_if_index: u32,
            is_create: bool,
        ) -> InterfaceResult<()> {
            if !is_create {
                let family = $family
                    .get()
                    .expect("IP family Main exists before interface callbacks");
                if unsafe { &*family.adjacency.get() }.is_none() {
                    return Ok(());
                }
                let net =
                    NetMain::global().expect("network Main exists before interface callbacks");
                let glean = unsafe { &*family.glean.get() }
                    .as_ref()
                    .expect("glean owner is installed");
                let adjacency = unsafe { &*family.adjacency.get() }
                    .as_ref()
                    .expect("adjacency owner is installed");
                glean.interface_walk(
                    main,
                    &mut net.fib_nodes_mut(),
                    adjacency,
                    sw_if_index,
                    FibWalkReason::INTERFACE_DELETE,
                );
            }
            Ok(())
        }

        fn $neighbor_delete(
            main: &mut DataPlaneMain,
            _: &InterfaceMain,
            sw_if_index: u32,
            is_create: bool,
        ) -> InterfaceResult<()> {
            if !is_create {
                let family = $family
                    .get()
                    .expect("IP family Main exists before interface callbacks");
                if unsafe { &*family.adjacency.get() }.is_none() {
                    return Ok(());
                }
                let net =
                    NetMain::global().expect("network Main exists before interface callbacks");
                let neighbor = unsafe { &*family.neighbor.get() }
                    .as_ref()
                    .expect("neighbor owner is installed");
                let adjacency = unsafe { &mut *family.adjacency.get() }
                    .as_mut()
                    .expect("adjacency owner is installed");
                neighbor.interface_walk(
                    main,
                    &mut net.fib_nodes_mut(),
                    adjacency,
                    sw_if_index,
                    FibWalkReason::INTERFACE_DELETE,
                );
            }
            Ok(())
        }

        fn $neighbor_mtu(main: &mut DataPlaneMain, _: &InterfaceMain, sw_if_index: u32) {
            let family = $family
                .get()
                .expect("IP family Main exists before interface callbacks");
            let net = NetMain::global().expect("network Main exists before interface callbacks");
            let neighbor = unsafe { &*family.neighbor.get() }
                .as_ref()
                .expect("neighbor owner is installed");
            let adjacency = unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("adjacency owner is installed");
            neighbor.interface_walk(
                main,
                &mut net.fib_nodes_mut(),
                adjacency,
                sw_if_index,
                FibWalkReason::ADJ_MTU,
            );
        }
    };
}

adjacency_interface_callbacks!(
    IP4_MAIN,
    ip4_glean_admin,
    ip4_neighbor_admin,
    ip4_glean_delete,
    ip4_neighbor_delete,
    ip4_neighbor_mtu
);
adjacency_interface_callbacks!(
    IP6_MAIN,
    ip6_glean_admin,
    ip6_neighbor_admin,
    ip6_glean_delete,
    ip6_neighbor_delete,
    ip6_neighbor_mtu
);

pub(crate) const ADJ_ADMIN_UP_DOWN_CALLBACKS: [InterfaceCallbackRegistration; 4] = [
    InterfaceCallbackRegistration {
        callback: ip4_glean_admin,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip6_glean_admin,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip4_neighbor_admin,
        priority: 1,
    },
    InterfaceCallbackRegistration {
        callback: ip6_neighbor_admin,
        priority: 1,
    },
];

pub(crate) const ADJ_SW_INTERFACE_CALLBACKS: [InterfaceCallbackRegistration; 4] = [
    InterfaceCallbackRegistration {
        callback: ip4_glean_delete,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip6_glean_delete,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip4_neighbor_delete,
        priority: 1,
    },
    InterfaceCallbackRegistration {
        callback: ip6_neighbor_delete,
        priority: 1,
    },
];

pub(crate) fn ip_output_feature_update(arc_index: u8, sw_if_index: u32) {
    let ip4_arc = unsafe {
        (*IP4_MAIN
            .get()
            .expect("IP4 Main exists before Feature updates")
            .lookup_main
            .get())
        .output_feature_arc_index
    };
    let ip6_arc = unsafe {
        (*IP6_MAIN
            .get()
            .expect("IP6 Main exists before Feature updates")
            .lookup_main
            .get())
        .output_feature_arc_index
    };
    if arc_index == ip4_arc {
        update_adjacency_output_config(sw_if_index, DpoProto::IP4);
    }
    if arc_index == ip6_arc {
        update_adjacency_output_config(sw_if_index, DpoProto::IP6);
    }
}

pub(crate) fn update_adjacency_output_config(sw_if_index: u32, protocol: DpoProto) {
    let features = hammer_service::feature::FeatureMain::global()
        .expect("FeatureMain exists before adjacency Feature updates");
    let (arc_index, indices) = match protocol {
        DpoProto::IP4 => {
            let main = IP4_MAIN.get().expect("IP4 Main exists");
            let arc = unsafe { (*main.lookup_main.get()).output_feature_arc_index };
            let mut indices = unsafe { &*main.neighbor.get() }
                .as_ref()
                .expect("IP4 neighbor owner exists")
                .indices(sw_if_index);
            indices.extend(
                unsafe { &*main.glean.get() }
                    .as_ref()
                    .expect("IP4 glean owner exists")
                    .indices(sw_if_index),
            );
            (arc, indices)
        }
        DpoProto::IP6 => {
            let main = IP6_MAIN.get().expect("IP6 Main exists");
            let arc = unsafe { (*main.lookup_main.get()).output_feature_arc_index };
            let mut indices = unsafe { &*main.neighbor.get() }
                .as_ref()
                .expect("IP6 neighbor owner exists")
                .indices(sw_if_index);
            indices.extend(
                unsafe { &*main.glean.get() }
                    .as_ref()
                    .expect("IP6 glean owner exists")
                    .indices(sw_if_index),
            );
            if let Some(index) = ip6_multicast_adjacency(sw_if_index) {
                indices.push(index);
            }
            (arc, indices)
        }
        _ => panic!("IP output Feature update requires IP4 or IP6"),
    };
    if arc_index == u8::MAX {
        return;
    }
    let config_index = features
        .feature_config_index(arc_index, sw_if_index)
        .expect("Feature update names a live interface");
    let has_features = features.has_features(arc_index, sw_if_index);
    match protocol {
        DpoProto::IP4 => {
            let adjacency =
                unsafe { &mut *IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
                    .as_mut()
                    .expect("IP4 adjacency owner exists");
            for index in indices {
                let object = adjacency.get_mut(index);
                object.config_index = config_index.unwrap_or(u32::MAX);
                if has_features {
                    object.rewrite_header.flags |= REWRITE_HAS_FEATURES;
                } else {
                    object.rewrite_header.flags &= !REWRITE_HAS_FEATURES;
                }
            }
        }
        DpoProto::IP6 => {
            let adjacency =
                unsafe { &mut *IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
                    .as_mut()
                    .expect("IP6 adjacency owner exists");
            for index in indices {
                let object = adjacency.get_mut(index);
                object.config_index = config_index.unwrap_or(u32::MAX);
                if has_features {
                    object.rewrite_header.flags |= REWRITE_HAS_FEATURES;
                } else {
                    object.rewrite_header.flags &= !REWRITE_HAS_FEATURES;
                }
            }
        }
        _ => unreachable!(),
    }
}

fn adj_dpo_lock(dpo: DpoId) {
    match dpo.proto() {
        DpoProto::IP4 => ip4_adj_lock(dpo.index()),
        DpoProto::IP6 => ip6_adj_lock(dpo.index()),
        _ => panic!("ADJ DPO protocol must be IP4 or IP6"),
    }
}

fn adj_dpo_unlock(dpo: DpoId) {
    match dpo.proto() {
        DpoProto::IP4 => {
            if ip4_adj_unlock(dpo.index()) {
                ip4_adj_last_lock(dpo.index());
            }
        }
        DpoProto::IP6 => {
            if ip6_adj_unlock(dpo.index()) {
                ip6_adj_last_lock(dpo.index());
            }
        }
        _ => panic!("ADJ DPO protocol must be IP4 or IP6"),
    }
}

fn adj_dpo_mtu(dpo: DpoId) -> u16 {
    match dpo.proto() {
        DpoProto::IP4 => unsafe { &*IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP4 adjacency owner installed")
            .mtu(AdjacencyIndex(dpo.index())),
        DpoProto::IP6 => unsafe { &*IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP6 adjacency owner installed")
            .mtu(AdjacencyIndex(dpo.index())),
        _ => panic!("ADJ DPO protocol must be IP4 or IP6"),
    }
}

fn adj_dpo_urpf(dpo: DpoId) -> u32 {
    match dpo.proto() {
        DpoProto::IP4 => unsafe { &*IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP4 adjacency owner installed")
            .urpf(AdjacencyIndex(dpo.index())),
        DpoProto::IP6 => unsafe { &*IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP6 adjacency owner installed")
            .urpf(AdjacencyIndex(dpo.index())),
        _ => panic!("ADJ DPO protocol must be IP4 or IP6"),
    }
}

fn adj_dpo_format(dpo: DpoId, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
        formatter,
        "adjacency {} protocol {} class {}",
        dpo.index(),
        dpo.proto().get(),
        dpo.class().get()
    )
}

fn adj_dpo_memory() -> (usize, usize, usize) {
    let ip4 = unsafe { &*IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
        .as_ref()
        .expect("IP4 adjacency owner installed")
        .memory();
    let ip6 = unsafe { &*IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
        .as_ref()
        .expect("IP6 adjacency owner installed")
        .memory();
    (ip4.0, ip4.1 + ip6.1, ip4.2 + ip6.2)
}

#[derive(hammer_component_macros::DpoClass)]
#[dpo_class(dpo_type = DpoType::ADJACENCY, nodes = caller, lock = adj_dpo_lock, unlock = adj_dpo_unlock, mtu = adj_dpo_mtu, urpf = adj_dpo_urpf, format = adj_dpo_format, memory = adj_dpo_memory)]
struct IpCompleteAdjacency;

#[derive(hammer_component_macros::DpoClass)]
#[dpo_class(dpo_type = DpoType::ADJACENCY_INCOMPLETE, nodes = caller, lock = adj_dpo_lock, unlock = adj_dpo_unlock, mtu = adj_dpo_mtu, urpf = adj_dpo_urpf, format = adj_dpo_format)]
struct IpIncompleteAdjacency;

#[derive(hammer_component_macros::DpoClass)]
#[dpo_class(dpo_type = DpoType::ADJACENCY_GLEAN, nodes = caller, lock = adj_dpo_lock, unlock = adj_dpo_unlock, mtu = adj_dpo_mtu, urpf = adj_dpo_urpf, format = adj_dpo_format)]
struct IpGleanAdjacency;

#[derive(hammer_component_macros::DpoClass)]
#[dpo_class(dpo_type = DpoType::ADJACENCY_MCAST, nodes = caller, lock = adj_dpo_lock, unlock = adj_dpo_unlock, mtu = adj_dpo_mtu, urpf = adj_dpo_urpf, format = adj_dpo_format)]
struct IpMulticastAdjacency;

fn register_adjacencies(
    ip4_glean: NodeId,
    ip6_glean: NodeId,
    ip4_incomplete: NodeId,
    ip6_incomplete: NodeId,
    ip4_rewrite: NodeId,
    ip6_rewrite: NodeId,
    ip4_rewrite_mcast: NodeId,
    ip6_rewrite_mcast: NodeId,
) {
    let net = NetMain::global().expect("adjacency graph requires the network Main");
    let mut nodes = net.fib_nodes_mut();
    let ip4_type = nodes.register_type(
        "ip4-adjacency",
        FibNodeOperations::new(
            ip4_adj_lock,
            ip4_adj_unlock,
            ip4_adj_children,
            ip4_adj_set_children,
            ip4_adj_last_lock,
        ),
    );
    let ip6_type = nodes.register_type(
        "ip6-adjacency",
        FibNodeOperations::new(
            ip6_adj_lock,
            ip6_adj_unlock,
            ip6_adj_children,
            ip6_adj_set_children,
            ip6_adj_last_lock,
        ),
    );
    drop(nodes);
    let ip4 = IP4_MAIN
        .get()
        .expect("IP4 Main exists before graph registration");
    let ip6 = IP6_MAIN
        .get()
        .expect("IP6 Main exists before graph registration");
    assert!(
        unsafe { &mut *ip4.adjacency.get() }
            .replace(AdjacencyMain::new(ip4_type))
            .is_none()
    );
    assert!(
        unsafe { &mut *ip4.glean.get() }
            .replace(AdjacencyGleanMain::new(ip4_glean))
            .is_none()
    );
    assert!(
        unsafe { &mut *ip4.neighbor.get() }
            .replace(AdjacencyNeighborMain::new(ip4_incomplete, ip4_rewrite))
            .is_none()
    );
    assert!(
        unsafe { &mut *ip6.adjacency.get() }
            .replace(AdjacencyMain::new(ip6_type))
            .is_none()
    );
    assert!(
        unsafe { &mut *ip6.glean.get() }
            .replace(AdjacencyGleanMain::new(ip6_glean))
            .is_none()
    );
    assert!(
        unsafe { &mut *ip6.neighbor.get() }
            .replace(AdjacencyNeighborMain::new(ip6_incomplete, ip6_rewrite))
            .is_none()
    );
    assert_eq!(
        IpCompleteAdjacency::register_dpo_class(
            net,
            &[
                (DpoProto::IP4, &[ip4_rewrite]),
                (DpoProto::IP6, &[ip6_rewrite])
            ]
        )
        .expect("complete ADJ class registration"),
        DpoType::ADJACENCY
    );
    assert_eq!(
        IpIncompleteAdjacency::register_dpo_class(
            net,
            &[
                (DpoProto::IP4, &[ip4_incomplete]),
                (DpoProto::IP6, &[ip6_incomplete])
            ]
        )
        .expect("incomplete ADJ class registration"),
        DpoType::ADJACENCY_INCOMPLETE
    );
    assert_eq!(
        IpGleanAdjacency::register_dpo_class(
            net,
            &[(DpoProto::IP4, &[ip4_glean]), (DpoProto::IP6, &[ip6_glean])]
        )
        .expect("glean ADJ class registration"),
        DpoType::ADJACENCY_GLEAN
    );
    assert_eq!(
        IpMulticastAdjacency::register_dpo_class(
            net,
            &[
                (DpoProto::IP4, &[ip4_rewrite_mcast]),
                (DpoProto::IP6, &[ip6_rewrite_mcast])
            ]
        )
        .expect("multicast ADJ class registration"),
        DpoType::ADJACENCY_MCAST
    );
    net.interface_main().register_mtu_callback(ip4_neighbor_mtu);
    net.interface_main().register_mtu_callback(ip6_neighbor_mtu);
}

#[hammer_component_macros::node_next]
enum Ip4ResolutionNext {
    #[next("ip4-drop")]
    Drop,
}

#[hammer_component_macros::node_next]
enum Ip6ResolutionNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-rewrite-mcast")]
    ReplyTx,
}

#[hammer_component_macros::node_next]
enum Ip4RewriteNext {
    #[next("ip4-drop")]
    Drop,
    #[next("ip4-icmp-error")]
    IcmpError,
    #[next("ip4-frag")]
    Fragment,
}

#[hammer_component_macros::node_next]
enum Ip6RewriteNext {
    #[next("ip6-drop")]
    Drop,
    #[next("ip6-icmp-error")]
    IcmpError,
    #[next("ip6-frag")]
    Fragment,
}

#[hammer_component_macros::node_next]
enum Ip4FragmentNext {
    #[next("ip4-rewrite")]
    Rewrite,
    #[next("ip4-drop")]
    Drop,
}

#[hammer_component_macros::node_next]
enum Ip6FragmentNext {
    #[next("ip6-rewrite")]
    Rewrite,
    #[next("ip6-drop")]
    Drop,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_glean, role = internal, name = "ip4-glean", next = Ip4ResolutionNext)]
pub struct Ip4GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_glean, role = internal, name = "ip6-glean", next = Ip6ResolutionNext)]
pub struct Ip6GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_incomplete, role = internal, name = "ip4-arp", next = Ip4ResolutionNext)]
pub struct Ip4IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_incomplete, role = internal, name = "ip6-discover-neighbor", next = Ip6ResolutionNext)]
pub struct Ip6IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_rewrite, role = internal, name = "ip4-rewrite", next = Ip4RewriteNext)]
pub struct Ip4RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_rewrite, role = internal, name = "ip6-rewrite", next = Ip6RewriteNext)]
pub struct Ip6RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_rewrite_mcast,
    role = internal,
    name = "ip4-rewrite-mcast",
    sibling_of = Ip4RewriteNode,
)]
struct Ip4RewriteMcastNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_rewrite_mcast,
    role = internal,
    name = "ip6-rewrite-mcast",
    sibling_of = Ip6RewriteNode,
)]
struct Ip6RewriteMcastNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    kind = internal,
    name = "ip4-frag",
    next = Ip4FragmentNext,
)]
struct Ip4FragmentNode;

#[hammer_component_macros::graph_node(
    graph = ip,
    kind = internal,
    name = "ip6-frag",
    next = Ip6FragmentNext,
)]
struct Ip6FragmentNode;

fn register_ip4_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4GleanNode::new(), &Ip4ResolutionNext::NEXT_NAMES)
}
fn register_ip6_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6GleanNode::new(), &Ip6ResolutionNext::NEXT_NAMES)
}
fn register_ip4_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4IncompleteNode::new(),
        &Ip4ResolutionNext::NEXT_NAMES,
    )
}
fn register_ip6_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6IncompleteNode::new(),
        &Ip6ResolutionNext::NEXT_NAMES,
    )
}
fn register_ip4_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4RewriteNode::new(), &Ip4RewriteNext::NEXT_NAMES)
}
fn register_ip6_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6RewriteNode::new(), &Ip6RewriteNext::NEXT_NAMES)
}

fn register_ip4_rewrite_mcast(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal(Ip4RewriteMcastNode::new())
}

fn register_ip6_rewrite_mcast(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip6RewriteMcastNode::new())?;
    let nodes = runtime.nodes();
    register_adjacencies(
        nodes
            .node_by_name("ip4-glean")
            .expect("IP4 glean node precedes ADJ class registration"),
        nodes
            .node_by_name("ip6-glean")
            .expect("IP6 glean node precedes ADJ class registration"),
        nodes
            .node_by_name("ip4-arp")
            .expect("IP4 incomplete node precedes ADJ class registration"),
        nodes
            .node_by_name("ip6-discover-neighbor")
            .expect("IP6 incomplete node precedes ADJ class registration"),
        nodes
            .node_by_name("ip4-rewrite")
            .expect("IP4 rewrite node precedes ADJ class registration"),
        nodes
            .node_by_name("ip6-rewrite")
            .expect("IP6 rewrite node precedes ADJ class registration"),
        nodes
            .node_by_name("ip4-rewrite-mcast")
            .expect("IP4 multicast rewrite precedes ADJ class registration"),
        node,
    );
    Ok(node)
}

fn rewrite_ip4_buffer(runtime: &mut DataPlaneMain, buffer_index: u32) -> u16 {
    let dpo = hammer_core::buffer_opaque!(
        runtime.buffer(buffer_index) => crate::lookup::IpSecondaryOpaque
    )
    .lookup
    .forwarding;
    assert!(
        matches!(dpo.class(), DpoType::ADJACENCY | DpoType::ADJACENCY_MCAST),
        "IP4 rewrite requires a complete adjacency"
    );
    assert_eq!(dpo.proto(), DpoProto::IP4, "IP4 rewrite protocol mismatch");
    let adjacency = unsafe { &*IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
        .as_ref()
        .expect("IP4 adjacency owner installed")
        .get(AdjacencyIndex(dpo.index()));
    let header = adjacency.rewrite_header;
    let rewrite_data = adjacency.rewrite_data;
    let config_index = adjacency.config_index;
    assert!(
        usize::from(header.data_bytes) <= rewrite_data.len(),
        "IP4 adjacency rewrite length fits its storage"
    );

    let locally_originated = hammer_core::buffer_opaque!(
        runtime.buffer(buffer_index) => NetworkOpaque
    )
    .flags
    .contains(NetworkFlags::LOCALLY_ORIGINATED);
    let (packet_len, dont_fragment, ttl_expired) = {
        let packet = runtime.buffer_mut(buffer_index).current_mut();
        let packet_capacity = packet.len();
        let (ip_header, _) =
            Ipv4Header::mut_from_prefix(packet).expect("IP4 rewrite receives an IPv4 header");
        assert_eq!(
            ip_header.version(),
            4,
            "IP4 rewrite receives an IPv4 header"
        );
        let header_len = ip_header.header_len();
        assert!(
            header_len >= 20 && packet_capacity >= header_len,
            "IP4 header is contiguous"
        );
        let mut ttl_expired = false;
        if !locally_originated {
            assert_ne!(ip_header.ttl(), 0, "IP4 input rejects zero TTL");
            ip_header.set_ttl(ip_header.ttl() - 1);
            ip_header.set_checksum(0);
            ttl_expired = ip_header.ttl() == 0;
        }
        (
            ip_header.total_len() as u16,
            ip_header.dont_fragment(),
            ttl_expired,
        )
    };
    if !locally_originated {
        let packet = runtime.buffer_mut(buffer_index).current_mut();
        let header_len = Ipv4Header::ref_from_prefix(packet)
            .expect("IP4 header remains contiguous")
            .0
            .header_len();
        let checksum = internet_checksum(&packet[..header_len]);
        Ipv4Header::mut_from_prefix(packet)
            .expect("IP4 header remains contiguous")
            .0
            .set_checksum(checksum);
    }
    if ttl_expired {
        IcmpErrorMetadata::ipv4_time_exceeded().write(hammer_core::buffer_opaque!(
            mut runtime.buffer_mut(buffer_index) => crate::lookup::IpSecondaryOpaque
        ));
        hammer_core::buffer_opaque!(mut runtime.buffer_mut(buffer_index) => NetworkOpaque)
            .sw_if_index[1] = u32::MAX;
        return NodeNext::slot(Ip4RewriteNext::IcmpError);
    }
    let ttl_decremented = !locally_originated;
    match ipv4_mtu_check(packet_len, header.max_l3_packet_bytes, dont_fragment) {
        Ipv4MtuAction::Ok => {}
        action => {
            if ttl_decremented {
                let packet = runtime.buffer_mut(buffer_index).current_mut();
                let header_len = {
                    let (header, _) =
                        Ipv4Header::mut_from_prefix(packet).expect("IP4 header remains contiguous");
                    header.set_ttl(header.ttl() + 1);
                    header.set_checksum(0);
                    header.header_len()
                };
                let checksum = internet_checksum(&packet[..header_len]);
                Ipv4Header::mut_from_prefix(packet)
                    .expect("IP4 header remains contiguous")
                    .0
                    .set_checksum(checksum);
            }
            return match action {
                Ipv4MtuAction::Fragment { .. } => NodeNext::slot(Ip4RewriteNext::Fragment),
                Ipv4MtuAction::IcmpFragNeeded { mtu } => {
                    IcmpErrorMetadata::ipv4_destination_unreachable(4, u32::from(mtu)).write(
                        hammer_core::buffer_opaque!(
                            mut runtime.buffer_mut(buffer_index) => crate::lookup::IpSecondaryOpaque
                        ),
                    );
                    NodeNext::slot(Ip4RewriteNext::IcmpError)
                }
                Ipv4MtuAction::Ok => unreachable!(),
            };
        }
    }

    let buffer = runtime.buffer_mut(buffer_index);
    buffer
        .push_uninit(header.data_bytes as u8)
        .copy_from_slice(&rewrite_data[..usize::from(header.data_bytes)]);
    hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).sw_if_index[1] = header.sw_if_index;
    if header.flags & REWRITE_HAS_FEATURES == 0 {
        return header.next_index;
    }
    let arc_index = unsafe {
        (*IP4_MAIN.get().expect("IP4 Main exists").lookup_main.get()).output_feature_arc_index
    };
    FeatureMain::global()
        .expect("FeatureMain exists before IP4 rewrite")
        .start_feature_arc_at_config(arc_index, config_index, buffer)
}

fn rewrite_ip6_buffer(runtime: &mut DataPlaneMain, buffer_index: u32) -> u16 {
    let dpo = hammer_core::buffer_opaque!(
        runtime.buffer(buffer_index) => crate::lookup::IpSecondaryOpaque
    )
    .lookup
    .forwarding;
    assert!(
        matches!(dpo.class(), DpoType::ADJACENCY | DpoType::ADJACENCY_MCAST),
        "IP6 rewrite requires a complete adjacency"
    );
    assert_eq!(dpo.proto(), DpoProto::IP6, "IP6 rewrite protocol mismatch");
    let adjacency = unsafe { &*IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
        .as_ref()
        .expect("IP6 adjacency owner installed")
        .get(AdjacencyIndex(dpo.index()));
    let header = adjacency.rewrite_header;
    let rewrite_data = adjacency.rewrite_data;
    let config_index = adjacency.config_index;
    assert!(
        usize::from(header.data_bytes) <= rewrite_data.len(),
        "IP6 adjacency rewrite length fits its storage"
    );

    let locally_originated = hammer_core::buffer_opaque!(
        runtime.buffer(buffer_index) => NetworkOpaque
    )
    .flags
    .contains(NetworkFlags::LOCALLY_ORIGINATED);
    let (packet_len, multicast_suffix, hop_limit_expired) = {
        let packet = runtime.buffer_mut(buffer_index).current_mut();
        let (ip_header, _) =
            Ipv6Header::mut_from_prefix(packet).expect("IP6 rewrite receives an IPv6 header");
        assert_eq!(
            ip_header.version(),
            6,
            "IP6 rewrite receives an IPv6 header"
        );
        let destination = ip_header.destination().octets();
        let mut hop_limit_expired = false;
        if !locally_originated {
            assert_ne!(ip_header.hop_limit(), 0, "IP6 input rejects zero Hop Limit");
            ip_header.set_hop_limit(ip_header.hop_limit() - 1);
            hop_limit_expired = ip_header.hop_limit() == 0;
        }
        (
            ip_header.payload_len().saturating_add(40),
            [
                destination[12],
                destination[13],
                destination[14],
                destination[15],
            ],
            hop_limit_expired,
        )
    };
    if hop_limit_expired {
        IcmpErrorMetadata::ipv6_time_exceeded().write(hammer_core::buffer_opaque!(
            mut runtime.buffer_mut(buffer_index) => crate::lookup::IpSecondaryOpaque
        ));
        hammer_core::buffer_opaque!(mut runtime.buffer_mut(buffer_index) => NetworkOpaque)
            .sw_if_index[1] = u32::MAX;
        return NodeNext::slot(Ip6RewriteNext::IcmpError);
    }
    if header.max_l3_packet_bytes >= 1280 && packet_len > usize::from(header.max_l3_packet_bytes) {
        if !locally_originated {
            let packet = runtime.buffer_mut(buffer_index).current_mut();
            let hop_limit = Ipv6Header::ref_from_prefix(packet)
                .expect("IP6 header remains contiguous")
                .0
                .hop_limit();
            Ipv6Header::mut_from_prefix(packet)
                .expect("IP6 header remains contiguous")
                .0
                .set_hop_limit(hop_limit + 1);
            IcmpErrorMetadata::ipv6_packet_too_big(u32::from(header.max_l3_packet_bytes)).write(
                hammer_core::buffer_opaque!(
                    mut runtime.buffer_mut(buffer_index) => crate::lookup::IpSecondaryOpaque
                ),
            );
            return NodeNext::slot(Ip6RewriteNext::IcmpError);
        }
        return NodeNext::slot(Ip6RewriteNext::Fragment);
    }

    let buffer = runtime.buffer_mut(buffer_index);
    buffer
        .push_uninit(header.data_bytes as u8)
        .copy_from_slice(&rewrite_data[..usize::from(header.data_bytes)]);
    if dpo.class() == DpoType::ADJACENCY_MCAST && header.dst_mcast_offset != 0 {
        let start = usize::from(header.data_bytes)
            .checked_sub(usize::from(header.dst_mcast_offset))
            .expect("IP6 multicast rewrite offset is within rewrite data");
        let destination = &mut buffer.current_mut()[start..start + multicast_suffix.len()];
        for (byte, suffix) in destination.iter_mut().zip(multicast_suffix) {
            *byte |= suffix;
        }
    }
    hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).sw_if_index[1] = header.sw_if_index;
    if header.flags & REWRITE_HAS_FEATURES == 0 {
        return header.next_index;
    }
    let arc_index = unsafe {
        (*IP6_MAIN.get().expect("IP6 Main exists").lookup_main.get()).output_feature_arc_index
    };
    FeatureMain::global()
        .expect("FeatureMain exists before IP6 rewrite")
        .start_feature_arc_at_config(arc_index, config_index, buffer)
}

macro_rules! impl_ip4_resolution_node {
    ($node:ident, $class:expr, $glean:expr) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let count = frame.len();
                for &head in frame.vector_args() {
                    let dpo = hammer_core::buffer_opaque!(
                        runtime.buffer(head) => IpSecondaryOpaque
                    )
                    .lookup
                    .forwarding;
                    assert_eq!(dpo.proto(), DpoProto::IP4, "IP4 resolution protocol mismatch");
                    assert_eq!(dpo.class(), $class, "IP4 resolution DPO class mismatch");
                    let (sw_if_index, output_next, target) = {
                        let adjacency = unsafe {
                            &*IP4_MAIN
                                .get()
                                .expect("IP4 Main exists before resolution")
                                .adjacency
                                .get()
                        }
                        .as_ref()
                        .expect("IP4 adjacency owner exists before resolution")
                        .get(AdjacencyIndex(dpo.index()));
                        let target = if $glean {
                            let packet = runtime.buffer(head).current();
                            assert!(
                                packet.len() >= 20 && packet[0] >> 4 == 4,
                                "IP4 glean receives an IPv4 header"
                            );
                            Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19])
                        } else {
                            Ip4FibProtocol::neighbor_address(&adjacency.subtype)
                        };
                        (
                            adjacency.rewrite_header.sw_if_index,
                            adjacency.rewrite_header.next_index,
                            target,
                        )
                    };
                    if let Some(source) = ip4_source_address(sw_if_index, target) {
                        let interfaces = NetMain::global()
                            .expect("IP4 resolution requires the network Main")
                            .interface_main();
                        let mut rewrite = [0_u8; 116];
                        let rewrite_len = interfaces.build_rewrite(
                            sw_if_index,
                            Some(0x0806),
                            None,
                            &mut rewrite,
                        );
                        if rewrite_len == 14 {
                            let mut allocated = [0_u32; 1];
                            if runtime.buffer_alloc(&mut allocated) == 1 {
                                let probe = allocated[0];
                                runtime.buffer_chain_init(probe);
                                let packet = runtime.buffer_mut(probe).put_uninit(42);
                                packet[..14].copy_from_slice(&rewrite[..14]);
                                packet[14..16].copy_from_slice(&1_u16.to_be_bytes());
                                packet[16..18].copy_from_slice(&0x0800_u16.to_be_bytes());
                                packet[18] = 6;
                                packet[19] = 4;
                                packet[20..22].copy_from_slice(&1_u16.to_be_bytes());
                                packet[22..28].copy_from_slice(&rewrite[6..12]);
                                packet[28..32].copy_from_slice(&source.octets());
                                packet[32..38].fill(0);
                                packet[38..42].copy_from_slice(&target.octets());
                                let mut network = NetworkOpaque::default();
                                network.sw_if_index[1] = sw_if_index;
                                network.flags = NetworkFlags::LOCALLY_ORIGINATED;
                                *hammer_core::buffer_opaque!(
                                    mut runtime.buffer_mut(probe) => NetworkOpaque
                                ) = network;
                                let vectors_left = {
                                    let (vectors, _) = runtime.get_next_frame::<u32, ()>(
                                        node_runtime,
                                        u32::from(output_next),
                                    );
                                    vectors[0] = probe;
                                    vectors.len() - 1
                                };
                                runtime.put_next_frame(
                                    node_runtime,
                                    u32::from(output_next),
                                    vectors_left,
                                );
                            }
                        }
                    }
                    let drop_next = NodeNext::slot(Ip4ResolutionNext::Drop);
                    let vectors_left = {
                        let (vectors, _) = runtime.get_next_frame::<u32, ()>(
                            node_runtime,
                            u32::from(drop_next),
                        );
                        vectors[0] = head;
                        vectors.len() - 1
                    };
                    runtime.put_next_frame(
                        node_runtime,
                        u32::from(drop_next),
                        vectors_left,
                    );
                }
                count
            }

            fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
                Ok(self.runtime_data)
            }
        }
    };
}

macro_rules! impl_ip6_resolution_node {
    ($node:ident, $class:expr, $glean:expr) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let count = frame.len();
                for &head in frame.vector_args() {
                    let dpo = hammer_core::buffer_opaque!(
                        runtime.buffer(head) => IpSecondaryOpaque
                    )
                    .lookup
                    .forwarding;
                    assert_eq!(dpo.proto(), DpoProto::IP6, "IP6 resolution protocol mismatch");
                    assert_eq!(dpo.class(), $class, "IP6 resolution DPO class mismatch");
                    let (sw_if_index, target) = {
                        let adjacency = unsafe {
                            &*IP6_MAIN
                                .get()
                                .expect("IP6 Main exists before resolution")
                                .adjacency
                                .get()
                        }
                        .as_ref()
                        .expect("IP6 adjacency owner exists before resolution")
                        .get(AdjacencyIndex(dpo.index()));
                        let target = if $glean {
                            let packet = runtime.buffer(head).current();
                            assert!(
                                packet.len() >= 40 && packet[0] >> 4 == 6,
                                "IP6 glean receives an IPv6 header"
                            );
                            Ipv6Addr::from(<[u8; 16]>::try_from(&packet[24..40]).unwrap())
                        } else {
                            Ip6FibProtocol::neighbor_address(&adjacency.subtype)
                        };
                        (adjacency.rewrite_header.sw_if_index, target)
                    };
                    if let (Some(source), Some(multicast_index)) = (
                        ip6_source_address(sw_if_index, target),
                        ip6_multicast_adjacency(sw_if_index),
                    ) {
                        let net = NetMain::global()
                            .expect("IP6 resolution requires the network Main");
                        let interfaces = net.interface_main();
                        let software = interfaces
                            .software_interface(sw_if_index)
                            .expect("IP6 resolution interface remains live");
                        let primary = interfaces
                            .software_interface(software.sup_sw_if_index)
                            .expect("IP6 resolution super-interface remains live");
                        let hardware = interfaces.hardware_interface(
                            software
                                .hw_if_index
                                .or(primary.hw_if_index)
                                .expect("IP6 resolution requires hardware"),
                        );
                        if let Ok(mac) = <[u8; 6]>::try_from(hardware.hw_address.as_slice()) {
                            let mut allocated = [0_u32; 1];
                            if runtime.buffer_alloc(&mut allocated) == 1 {
                                let probe = allocated[0];
                                runtime.buffer_chain_init(probe);
                                let mut destination = [0_u8; 16];
                                destination[0] = 0xff;
                                destination[1] = 0x02;
                                destination[11] = 0x01;
                                destination[12] = 0xff;
                                destination[13..].copy_from_slice(&target.octets()[13..]);
                                let packet = runtime.buffer_mut(probe).put_uninit(72);
                                packet[0] = 0x60;
                                packet[1..4].fill(0);
                                packet[4..6].copy_from_slice(&32_u16.to_be_bytes());
                                packet[6] = 58;
                                packet[7] = 255;
                                packet[8..24].copy_from_slice(&source.octets());
                                packet[24..40].copy_from_slice(&destination);
                                packet[40] = 135;
                                packet[41] = 0;
                                packet[42..48].fill(0);
                                packet[48..64].copy_from_slice(&target.octets());
                                packet[64] = 1;
                                packet[65] = 1;
                                packet[66..72].copy_from_slice(&mac);
                                let pseudo_length = 32_u32.to_be_bytes();
                                let pseudo_protocol = [0, 0, 0, 58];
                                let checksum = internet_checksum_parts(&[
                                    &packet[8..24],
                                    &packet[24..40],
                                    &pseudo_length,
                                    &pseudo_protocol,
                                    &packet[40..],
                                ]);
                                packet[42..44].copy_from_slice(&checksum.to_be_bytes());
                                let mut network = NetworkOpaque::default();
                                network.sw_if_index[1] = sw_if_index;
                                network.flags = NetworkFlags::LOCALLY_ORIGINATED;
                                network.ip_mut().set_packet_len(72);
                                *hammer_core::buffer_opaque!(
                                    mut runtime.buffer_mut(probe) => NetworkOpaque
                                ) = network;
                                let multicast_dpo = unsafe {
                                    &*IP6_MAIN
                                        .get()
                                        .expect("IP6 Main exists before resolution")
                                        .adjacency
                                        .get()
                                }
                                .as_ref()
                                .expect("IP6 adjacency owner exists before resolution")
                                .dpo(&net.dpo_main(), multicast_index);
                                hammer_core::buffer_opaque!(
                                    mut runtime.buffer_mut(probe) => IpSecondaryOpaque
                                )
                                .lookup
                                .forwarding = multicast_dpo;
                                let reply_next = NodeNext::slot(Ip6ResolutionNext::ReplyTx);
                                let vectors_left = {
                                    let (vectors, _) = runtime.get_next_frame::<u32, ()>(
                                        node_runtime,
                                        u32::from(reply_next),
                                    );
                                    vectors[0] = probe;
                                    vectors.len() - 1
                                };
                                runtime.put_next_frame(
                                    node_runtime,
                                    u32::from(reply_next),
                                    vectors_left,
                                );
                            }
                        }
                    }
                    let drop_next = NodeNext::slot(Ip6ResolutionNext::Drop);
                    let vectors_left = {
                        let (vectors, _) = runtime.get_next_frame::<u32, ()>(
                            node_runtime,
                            u32::from(drop_next),
                        );
                        vectors[0] = head;
                        vectors.len() - 1
                    };
                    runtime.put_next_frame(
                        node_runtime,
                        u32::from(drop_next),
                        vectors_left,
                    );
                }
                count
            }

            fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
                Ok(self.runtime_data)
            }
        }
    };
}

impl_ip4_resolution_node!(Ip4GleanNode, DpoType::ADJACENCY_GLEAN, true);
impl_ip4_resolution_node!(Ip4IncompleteNode, DpoType::ADJACENCY_INCOMPLETE, false);
impl_ip6_resolution_node!(Ip6GleanNode, DpoType::ADJACENCY_GLEAN, true);
impl_ip6_resolution_node!(Ip6IncompleteNode, DpoType::ADJACENCY_INCOMPLETE, false);

macro_rules! impl_rewrite_node {
    ($node:ident, $rewrite:ident) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let count = frame.len();
                hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
                    $rewrite(runtime, index)
                });
                count
            }
        }
    };
    ($node:ident, $rewrite:ident, runtime_data) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let count = frame.len();
                hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
                    $rewrite(runtime, index)
                });
                count
            }

            fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
                Ok(self.runtime_data)
            }
        }
    };
}

impl_rewrite_node!(Ip4RewriteNode, rewrite_ip4_buffer, runtime_data);
impl_rewrite_node!(Ip6RewriteNode, rewrite_ip6_buffer, runtime_data);
impl_rewrite_node!(Ip4RewriteMcastNode, rewrite_ip4_buffer, runtime_data);
impl_rewrite_node!(Ip6RewriteMcastNode, rewrite_ip6_buffer, runtime_data);

impl Node for Ip4FragmentNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let count = frame.len();
        for &head in frame.vector_args() {
            let dpo = hammer_core::buffer_opaque!(runtime.buffer(head) => IpSecondaryOpaque)
                .lookup
                .forwarding;
            let mtu = usize::from(adj_dpo_mtu(dpo));
            let mut network = *hammer_core::buffer_opaque!(runtime.buffer(head) => NetworkOpaque);
            let secondary = *hammer_core::buffer_opaque!(runtime.buffer(head) => IpSecondaryOpaque);
            let chain_len = runtime
                .chain(head)
                .map(|buffer| buffer.current_len())
                .sum::<usize>();
            let first = runtime.buffer(head).current();
            let mut header = [0_u8; 60];
            let mut valid = first.len() >= 20 && first[0] >> 4 == 4;
            let header_len = if valid {
                usize::from(first[0] & 0x0f) * 4
            } else {
                0
            };
            valid &= (20..=header.len()).contains(&header_len) && first.len() >= header_len;
            let packet_len = if valid {
                usize::from(u16::from_be_bytes([first[2], first[3]]))
            } else {
                0
            };
            valid &= packet_len >= header_len && packet_len <= chain_len;
            let max_payload = mtu
                .checked_sub(header_len)
                .map(|bytes| bytes & !7)
                .unwrap_or(0);
            valid &= max_payload != 0;
            if valid {
                header[..header_len].copy_from_slice(&first[..header_len]);
            }
            let flags_offset = if valid {
                u16::from_be_bytes([header[6], header[7]])
            } else {
                0
            };
            valid &= flags_offset & 0x4000 == 0;
            let original_offset = usize::from(flags_offset & 0x1fff) * 8;
            let original_more = flags_offset & 0x2000 != 0;
            let fragment_id = if original_offset != 0 || original_more {
                u16::from_be_bytes([header[4], header[5]])
            } else {
                runtime.random().next_u32() as u16
            };
            let payload_len = packet_len.saturating_sub(header_len);
            let mut fragments = Vec::new();
            let mut payload_offset = 0usize;
            while valid && payload_offset < payload_len {
                let fragment_payload_len = (payload_len - payload_offset).min(max_payload);
                let mut allocated = [0_u32; 1];
                if runtime.buffer_alloc(&mut allocated) != 1 {
                    valid = false;
                    break;
                }
                let fragment = allocated[0];
                runtime.buffer_chain_init(fragment);
                fragments.push(fragment);
                let more = payload_offset + fragment_payload_len < payload_len || original_more;
                let offset = (original_offset + payload_offset) / 8;
                let fragment_flags_offset = u16::try_from(offset)
                    .expect("IPv4 fragment offset fits its header")
                    | if more { 0x2000 } else { 0 };
                {
                    let bytes = runtime.buffer_mut(fragment).put_uninit(
                        u16::try_from(header_len).expect("IPv4 header length fits u16"),
                    );
                    bytes.copy_from_slice(&header[..header_len]);
                    bytes[2..4].copy_from_slice(
                        &u16::try_from(header_len + fragment_payload_len)
                            .expect("IPv4 fragment length fits u16")
                            .to_be_bytes(),
                    );
                    bytes[4..6].copy_from_slice(&fragment_id.to_be_bytes());
                    bytes[6..8].copy_from_slice(&fragment_flags_offset.to_be_bytes());
                    bytes[10..12].fill(0);
                    let checksum = internet_checksum(bytes);
                    bytes[10..12].copy_from_slice(&checksum.to_be_bytes());
                }

                let mut source_index = Some(head);
                let mut source_skip = header_len + payload_offset;
                while let Some(index) = source_index {
                    let source = runtime.buffer(index);
                    if source_skip < source.current_len() {
                        break;
                    }
                    source_skip -= source.current_len();
                    source_index = source.next_buffer_slot();
                }
                let mut remaining = fragment_payload_len;
                let mut tail = fragment;
                while remaining != 0 {
                    let Some(index) = source_index else {
                        valid = false;
                        break;
                    };
                    let (source, copied, next) = {
                        let source = runtime.buffer(index);
                        let copied = remaining.min(source.current_len() - source_skip);
                        (
                            unsafe { source.current().as_ptr().add(source_skip) },
                            copied,
                            source.next_buffer_slot(),
                        )
                    };
                    let source = unsafe { std::slice::from_raw_parts(source, copied) };
                    if runtime.buffer_chain_append_data_with_alloc(fragment, &mut tail, source)
                        != copied
                    {
                        valid = false;
                        break;
                    }
                    remaining -= copied;
                    source_skip = 0;
                    source_index = next;
                }
                if !valid {
                    break;
                }
                network
                    .ip_mut()
                    .set_packet_len((header_len + fragment_payload_len) as u32);
                *hammer_core::buffer_opaque!(mut runtime.buffer_mut(fragment) => NetworkOpaque) =
                    network;
                *hammer_core::buffer_opaque!(mut runtime.buffer_mut(fragment) => IpSecondaryOpaque) =
                    secondary;
                payload_offset += fragment_payload_len;
            }

            if valid && payload_offset == payload_len && !fragments.is_empty() {
                runtime.buffer_free_one(head);
                for fragment in fragments {
                    let next = NodeNext::slot(Ip4FragmentNext::Rewrite);
                    let vectors_left = {
                        let (vectors, _) =
                            runtime.get_next_frame::<u32, ()>(node_runtime, u32::from(next));
                        vectors[0] = fragment;
                        vectors.len() - 1
                    };
                    runtime.put_next_frame(node_runtime, u32::from(next), vectors_left);
                }
            } else {
                runtime.buffer_free(&fragments);
                let next = NodeNext::slot(Ip4FragmentNext::Drop);
                let vectors_left = {
                    let (vectors, _) =
                        runtime.get_next_frame::<u32, ()>(node_runtime, u32::from(next));
                    vectors[0] = head;
                    vectors.len() - 1
                };
                runtime.put_next_frame(node_runtime, u32::from(next), vectors_left);
            }
        }
        count
    }
}

impl Node for Ip6FragmentNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let count = frame.len();
        for &head in frame.vector_args() {
            let dpo = hammer_core::buffer_opaque!(runtime.buffer(head) => IpSecondaryOpaque)
                .lookup
                .forwarding;
            let mtu = usize::from(adj_dpo_mtu(dpo));
            let mut network = *hammer_core::buffer_opaque!(runtime.buffer(head) => NetworkOpaque);
            let secondary = *hammer_core::buffer_opaque!(runtime.buffer(head) => IpSecondaryOpaque);
            let chain_len = runtime
                .chain(head)
                .map(|buffer| buffer.current_len())
                .sum::<usize>();
            let first = runtime.buffer(head).current();
            let mut header = [0_u8; 48];
            let mut valid = first.len() >= 40 && first[0] >> 4 == 6 && first[6] != 44;
            let payload_len = if valid {
                usize::from(u16::from_be_bytes([first[4], first[5]]))
            } else {
                0
            };
            valid &= payload_len + 40 <= chain_len;
            let max_payload = mtu
                .checked_sub(header.len())
                .map(|bytes| bytes & !7)
                .unwrap_or(0);
            valid &= max_payload != 0;
            if valid {
                header[..40].copy_from_slice(&first[..40]);
            }
            let next_header = header[6];
            let identification = runtime.random().next_u32();
            let mut fragments = Vec::new();
            let mut payload_offset = 0usize;
            while valid && payload_offset < payload_len {
                let fragment_payload_len = (payload_len - payload_offset).min(max_payload);
                let mut allocated = [0_u32; 1];
                if runtime.buffer_alloc(&mut allocated) != 1 {
                    valid = false;
                    break;
                }
                let fragment = allocated[0];
                runtime.buffer_chain_init(fragment);
                fragments.push(fragment);
                let more = payload_offset + fragment_payload_len < payload_len;
                let offset_more = u16::try_from(payload_offset)
                    .expect("IPv6 fragment offset fits its header")
                    | u16::from(more);
                header[4..6].copy_from_slice(
                    &u16::try_from(fragment_payload_len + 8)
                        .expect("IPv6 fragment payload length fits u16")
                        .to_be_bytes(),
                );
                header[6] = 44;
                header[40] = next_header;
                header[41] = 0;
                header[42..44].copy_from_slice(&offset_more.to_be_bytes());
                header[44..48].copy_from_slice(&identification.to_be_bytes());
                runtime
                    .buffer_mut(fragment)
                    .put_uninit(48)
                    .copy_from_slice(&header);

                let mut source_index = Some(head);
                let mut source_skip = 40 + payload_offset;
                while let Some(index) = source_index {
                    let source = runtime.buffer(index);
                    if source_skip < source.current_len() {
                        break;
                    }
                    source_skip -= source.current_len();
                    source_index = source.next_buffer_slot();
                }
                let mut remaining = fragment_payload_len;
                let mut tail = fragment;
                while remaining != 0 {
                    let Some(index) = source_index else {
                        valid = false;
                        break;
                    };
                    let (source, copied, next) = {
                        let source = runtime.buffer(index);
                        let copied = remaining.min(source.current_len() - source_skip);
                        (
                            unsafe { source.current().as_ptr().add(source_skip) },
                            copied,
                            source.next_buffer_slot(),
                        )
                    };
                    let source = unsafe { std::slice::from_raw_parts(source, copied) };
                    if runtime.buffer_chain_append_data_with_alloc(fragment, &mut tail, source)
                        != copied
                    {
                        valid = false;
                        break;
                    }
                    remaining -= copied;
                    source_skip = 0;
                    source_index = next;
                }
                if !valid {
                    break;
                }
                network
                    .ip_mut()
                    .set_packet_len((48 + fragment_payload_len) as u32);
                *hammer_core::buffer_opaque!(mut runtime.buffer_mut(fragment) => NetworkOpaque) =
                    network;
                *hammer_core::buffer_opaque!(mut runtime.buffer_mut(fragment) => IpSecondaryOpaque) =
                    secondary;
                payload_offset += fragment_payload_len;
            }

            if valid && payload_offset == payload_len && !fragments.is_empty() {
                runtime.buffer_free_one(head);
                for fragment in fragments {
                    let next = NodeNext::slot(Ip6FragmentNext::Rewrite);
                    let vectors_left = {
                        let (vectors, _) =
                            runtime.get_next_frame::<u32, ()>(node_runtime, u32::from(next));
                        vectors[0] = fragment;
                        vectors.len() - 1
                    };
                    runtime.put_next_frame(node_runtime, u32::from(next), vectors_left);
                }
            } else {
                runtime.buffer_free(&fragments);
                let next = NodeNext::slot(Ip6FragmentNext::Drop);
                let vectors_left = {
                    let (vectors, _) =
                        runtime.get_next_frame::<u32, ()>(node_runtime, u32::from(next));
                    vectors[0] = head;
                    vectors.len() - 1
                };
                runtime.put_next_frame(node_runtime, u32::from(next), vectors_left);
            }
        }
        count
    }
}
