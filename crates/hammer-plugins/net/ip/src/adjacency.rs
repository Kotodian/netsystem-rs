use std::mem::{align_of, offset_of, size_of};
use std::net::{Ipv4Addr, Ipv6Addr};

use hammer_core::data_plane::{Frame, NodeId, NodeNext};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, RuntimeResult};
use hammer_service::interface::{
    InterfaceCallbackRegistration, InterfaceMain, InterfaceMtuKind, InterfaceResult,
};
use hammer_service::net::adj::{Adjacency, FibProtocol, FibProtocolId};
use hammer_service::net::adj_glean::AdjacencyGleanMain;
use hammer_service::net::adj_nbr::AdjacencyNeighborMain;
use hammer_service::net::fib_node::{FibNodeList, FibNodeOperations, FibWalkReason};
use hammer_service::net::{DpoId, DpoProto, DpoType, NetMain};
use hammer_service::opaque::NetworkOpaque;
use ipnet::{Ipv4Net, Ipv6Net};

use crate::lookup::{IP4_MAIN, IP6_MAIN};
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
const _: () = assert!(offset_of!(Adjacency<Ip4FibProtocol>, rewrite) == 64);
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
                _ => unsafe { &mut *family.neighbor.get() }
                    .as_mut()
                    .expect("neighbor owner installed before DPO use")
                    .remove(adjacency, index),
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

fn register_adjacencies(
    ip4_glean: NodeId,
    ip6_glean: NodeId,
    ip4_incomplete: NodeId,
    ip6_incomplete: NodeId,
    ip4_rewrite: NodeId,
    ip6_rewrite: NodeId,
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
    net.interface_main().register_mtu_callback(ip4_neighbor_mtu);
    net.interface_main().register_mtu_callback(ip6_neighbor_mtu);
}

#[hammer_component_macros::node_next]
enum Ip4AdjacencyNext {
    #[next("drop")]
    Drop,
    #[next("ip4-punt")]
    Punt,
}

#[hammer_component_macros::node_next]
enum Ip6AdjacencyNext {
    #[next("drop")]
    Drop,
    #[next("ip6-punt")]
    Punt,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_glean, role = internal, name = "ip4-glean", next = Ip4AdjacencyNext)]
pub struct Ip4GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_glean, role = internal, name = "ip6-glean", next = Ip6AdjacencyNext)]
pub struct Ip6GleanNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_incomplete, role = internal, name = "ip4-arp", next = Ip4AdjacencyNext)]
pub struct Ip4IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_incomplete, role = internal, name = "ip6-discover-neighbor", next = Ip6AdjacencyNext)]
pub struct Ip6IncompleteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip4_rewrite, role = internal, name = "ip4-rewrite", next = Ip4AdjacencyNext)]
pub struct Ip4RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(graph = ip, init = register_ip6_rewrite, role = internal, name = "ip6-rewrite", next = Ip6AdjacencyNext)]
pub struct Ip6RewriteNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

fn register_ip4_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4GleanNode::new(), &Ip4AdjacencyNext::NEXT_NAMES)
}
fn register_ip6_glean(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6GleanNode::new(), &Ip6AdjacencyNext::NEXT_NAMES)
}
fn register_ip4_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip4IncompleteNode::new(),
        &Ip4AdjacencyNext::NEXT_NAMES,
    )
}
fn register_ip6_incomplete(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal_with_next_names(
        Ip6IncompleteNode::new(),
        &Ip6AdjacencyNext::NEXT_NAMES,
    )
}
fn register_ip4_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4RewriteNode::new(), &Ip4AdjacencyNext::NEXT_NAMES)
}
fn register_ip6_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Ip6RewriteNode::new(),
        &Ip6AdjacencyNext::NEXT_NAMES,
    )?;
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
        node,
    );
    Ok(node)
}

fn rewrite_packet(runtime: &mut DataPlaneMain, index: u32, proto: DpoProto, drop_next: u16) -> u16 {
    let dpo =
        hammer_core::buffer_opaque!(runtime.buffer(index) => crate::lookup::IpSecondaryOpaque)
            .lookup
            .forwarding;
    assert_eq!(
        dpo.class(),
        DpoType::ADJACENCY,
        "rewrite node requires complete adjacency"
    );
    assert_eq!(dpo.proto(), proto, "rewrite node protocol mismatch");
    let adjacency = match proto {
        DpoProto::IP4 => unsafe { &*IP4_MAIN.get().expect("IP4 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP4 adjacency owner installed")
            .get(AdjacencyIndex(dpo.index()))
            .rewrite
            .clone(),
        DpoProto::IP6 => unsafe { &*IP6_MAIN.get().expect("IP6 Main exists").adjacency.get() }
            .as_ref()
            .expect("IP6 adjacency owner installed")
            .get(AdjacencyIndex(dpo.index()))
            .rewrite
            .clone(),
        _ => panic!("rewrite node protocol must be IP4 or IP6"),
    };
    let buffer = runtime.buffer_mut(index);
    if buffer.current_len() > usize::from(adjacency.max_l3_packet_bytes) {
        return drop_next;
    }
    buffer.advance(-(adjacency.data_bytes as isize));
    buffer.current_mut()[..usize::from(adjacency.data_bytes)]
        .copy_from_slice(&adjacency.data[..usize::from(adjacency.data_bytes)]);
    hammer_core::buffer_opaque!(mut buffer => NetworkOpaque).sw_if_index[1] = adjacency.sw_if_index;
    adjacency.next_index
}

macro_rules! impl_adjacency_node {
    ($node:ident, $proto:expr, $next:ident, unresolved) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let process: NodeProcessFn = |runtime, node_runtime, frame| {
                    let count = frame.len();
                    hammer_runtime::process_frame!(runtime, node_runtime, frame, |_| {
                        NodeNext::slot($next::Punt)
                    });
                    count
                };
                process(runtime, node_runtime, frame)
            }
            fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
                Ok(self.runtime_data)
            }
        }
    };
    ($node:ident, $proto:expr, $next:ident, rewrite) => {
        impl Node for $node {
            fn process(
                runtime: &mut DataPlaneMain,
                node_runtime: &mut NodeRuntime,
                frame: &mut Frame,
            ) -> usize {
                let process: NodeProcessFn = |runtime, node_runtime, frame| {
                    let count = frame.len();
                    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
                        rewrite_packet(runtime, index, $proto, NodeNext::slot($next::Drop))
                    });
                    count
                };
                process(runtime, node_runtime, frame)
            }
            fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
                Ok(self.runtime_data)
            }
        }
    };
}

impl_adjacency_node!(Ip4GleanNode, DpoProto::IP4, Ip4AdjacencyNext, unresolved);
impl_adjacency_node!(Ip6GleanNode, DpoProto::IP6, Ip6AdjacencyNext, unresolved);
impl_adjacency_node!(
    Ip4IncompleteNode,
    DpoProto::IP4,
    Ip4AdjacencyNext,
    unresolved
);
impl_adjacency_node!(
    Ip6IncompleteNode,
    DpoProto::IP6,
    Ip6AdjacencyNext,
    unresolved
);
impl_adjacency_node!(Ip4RewriteNode, DpoProto::IP4, Ip4AdjacencyNext, rewrite);
impl_adjacency_node!(Ip6RewriteNode, DpoProto::IP6, Ip6AdjacencyNext, rewrite);
