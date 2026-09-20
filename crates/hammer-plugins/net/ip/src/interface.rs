use std::cell::UnsafeCell;
use std::fmt;
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;

use hammer_runtime::DataPlaneMain;
use hammer_service::feature::FeatureMain;
use hammer_service::interface::InterfaceMtuKind;
use hammer_service::interface::{InterfaceCallbackRegistration, InterfaceMain, InterfaceResult};
use hammer_service::net::fib::FibEntrySourceBehaviorId;
use hammer_service::net::{
    DpoId, DpoProto, FibEntryFlags, FibSource, LoadBalanceDpo, LoadBalanceFlags, LoadBalancePath,
    NetMain,
};
use ipnet::{Ipv4Net, Ipv6Net};
use rand_09::RngCore;

use crate::adjacency::{Ip4FibProtocol, Ip6FibProtocol, IpNetLink};
use crate::lookup::{IP4_MAIN, IP6_MAIN, IpInterfaceAddress, IpInterfaceAddressKey, IpLookupMain};
use hammer_service::net::adj::{AdjacencyLookupNext, FibProtocol};

pub type IpInterfaceAddressCallback<A> = fn(&mut DataPlaneMain, u32, A, u8, u32, bool);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpInterfaceAddressError {
    code: i32,
}

impl IpInterfaceAddressError {
    const UNSUPPORTED: Self = Self { code: -126 };
    const ADDRESS_LENGTH_MISMATCH: Self = Self { code: -59 };
    const ADDRESS_IN_USE: Self = Self { code: -105 };
    const DUPLICATE_IF_ADDRESS: Self = Self { code: -127 };
    const ADDRESS_NOT_FOUND_FOR_INTERFACE: Self = Self { code: -60 };
    const ADDRESS_NOT_DELETABLE: Self = Self { code: -61 };

    pub const fn code(self) -> i32 {
        self.code
    }
}

impl fmt::Display for IpInterfaceAddressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "IP interface address API status {}", self.code)
    }
}

impl std::error::Error for IpInterfaceAddressError {}

#[derive(Debug, Clone, Copy)]
struct Ip6Link {
    sw_if_index: u32,
    link_local_address: Ipv6Addr,
    locks: u32,
}

struct Ip6LinkMain {
    links_by_sw_if_index: UnsafeCell<Vec<Option<Ip6Link>>>,
}

impl Ip6LinkMain {
    const FIB_SOURCE: FibSource = FibSource::new(14);

    fn new() -> Self {
        Self {
            links_by_sw_if_index: UnsafeCell::new(Vec::new()),
        }
    }

    fn links(&self) -> &[Option<Ip6Link>] {
        // SAFETY: mutation is main-thread-only under the worker barrier.
        unsafe { &*self.links_by_sw_if_index.get() }
    }

    #[allow(clippy::mut_from_ref)]
    fn links_mut(&self) -> &mut Vec<Option<Ip6Link>> {
        // SAFETY: mutation is main-thread-only under the worker barrier.
        unsafe { &mut *self.links_by_sw_if_index.get() }
    }
}

// SAFETY: the owner is mutated only from main-thread lifecycle and control-plane operations.
unsafe impl Sync for Ip6LinkMain {}

static IP6_LINK_MAIN: OnceLock<Ip6LinkMain> = OnceLock::new();

#[hammer_component_macros::init_function(
    name = "ip6_link_init",
    runs_after = ["ip_lookup_init"],
    runs_before = ["net_main_init"]
)]
fn ip6_link_init() -> hammer_runtime::RuntimeResult<()> {
    assert!(
        IP6_LINK_MAIN.set(Ip6LinkMain::new()).is_ok(),
        "IP6 link initialization callback executes once"
    );
    Ok(())
}

#[derive(hammer_component_macros::FibSource)]
#[fib_source(source = Ip6LinkMain::FIB_SOURCE, name = "ip6-nd", priority = 0xc1, behavior = FibEntrySourceBehaviorId::API)]
struct Ip6NdSourceRegistration;

#[hammer_component_macros::init_function(name = "ip6_link_fib_source_init", runs_after = ["net_main_init"])]
fn ip6_link_fib_source_init() -> hammer_runtime::RuntimeResult<()> {
    Ip6NdSourceRegistration::register_fib_source(&mut NetMain::global()?.fib_sources_mut());
    Ok(())
}

pub fn register_ip4_add_del_interface_address_callback(
    callback: IpInterfaceAddressCallback<Ipv4Addr>,
) {
    hammer_runtime::ensure_main_thread()
        .expect("IP4 address callback registration is startup-only");
    let main = IP4_MAIN
        .get()
        .expect("IP4 Main is initialized before callback registration");
    // SAFETY: callback registration completes before Data Workers start.
    unsafe { &mut *main.add_del_interface_address_callbacks.get() }.push(callback);
}

pub fn register_ip6_add_del_interface_address_callback(
    callback: IpInterfaceAddressCallback<Ipv6Addr>,
) {
    hammer_runtime::ensure_main_thread()
        .expect("IP6 address callback registration is startup-only");
    let main = IP6_MAIN
        .get()
        .expect("IP6 Main is initialized before callback registration");
    // SAFETY: callback registration completes before Data Workers start.
    unsafe { &mut *main.add_del_interface_address_callbacks.get() }.push(callback);
}

fn set_interface_lifecycle(
    main: &mut DataPlaneMain,
    lookup: &mut IpLookupMain<impl Copy + Eq + Hash, impl Copy + Eq + Hash, impl Copy>,
    fib_indices: &mut Vec<Option<u32>>,
    enabled: &mut Vec<u8>,
    sw_if_index: u32,
    is_create: bool,
    not_enabled_name: &'static str,
) {
    let position = sw_if_index as usize;
    if fib_indices.len() <= position {
        fib_indices.resize(position + 1, None);
    }
    if enabled.len() <= position {
        enabled.resize(position + 1, 0);
    }
    if is_create {
        fib_indices[position] = Some(0);
        if lookup.unicast_feature_arc_index != u8::MAX && enabled[position] == 0 {
            let features =
                FeatureMain::global().expect("FeatureMain exists after arc registration");
            let feature = features
                .feature_index(lookup.unicast_feature_arc_index, not_enabled_name)
                .expect("not-enabled feature is registered on the family unicast arc");
            features
                .enable_feature(
                    main,
                    lookup.unicast_feature_arc_index,
                    feature,
                    sw_if_index,
                    &[],
                )
                .expect("live interface accepts its default not-enabled feature");
        }
    } else {
        fib_indices[position] = None;
        enabled[position] = 0;
        if lookup.unicast_feature_arc_index != u8::MAX {
            let features =
                FeatureMain::global().expect("FeatureMain exists before interface deletion");
            let feature = features
                .feature_index(lookup.unicast_feature_arc_index, not_enabled_name)
                .expect("not-enabled feature is registered on the family unicast arc");
            features
                .disable_feature(
                    main,
                    lookup.unicast_feature_arc_index,
                    feature,
                    sw_if_index,
                    &[],
                )
                .expect("live interface releases its not-enabled feature");
        }
    }
}

fn ip4_sw_interface_add_del(
    main: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_create: bool,
) -> InterfaceResult<()> {
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main exists before interface lifecycle callbacks");
    if !is_create {
        let addresses = interface_address_indices(
            // SAFETY: interface callbacks run on the main thread under publication ownership.
            unsafe { &*family.lookup_main.get() },
            sw_if_index,
        );
        for index in addresses {
            let address = unsafe { &*family.lookup_main.get() }
                .interface_addresses
                .get(index)
                .copied()
                .expect("enumerated IP4 interface address remains occupied");
            ip4_add_del_interface_address(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                true,
            )
            .expect("IP4 owner deletes an address it just enumerated");
        }
    }
    // SAFETY: this callback owns family control-plane mutation.
    set_interface_lifecycle(
        main,
        unsafe { &mut *family.lookup_main.get() },
        unsafe { &mut *family.fib_index_by_sw_if_index.get() },
        unsafe { &mut *family.ip_enabled_by_sw_if_index.get() },
        sw_if_index,
        is_create,
        "ip4-not-enabled",
    );
    Ok(())
}

fn ip6_sw_interface_add_del(
    main: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_create: bool,
) -> InterfaceResult<()> {
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main exists before interface lifecycle callbacks");
    if !is_create {
        let addresses =
            interface_address_indices(unsafe { &*family.lookup_main.get() }, sw_if_index);
        for index in addresses {
            let address = unsafe { &*family.lookup_main.get() }
                .interface_addresses
                .get(index)
                .copied()
                .expect("enumerated IP6 interface address remains occupied");
            ip6_add_del_interface_address(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                true,
            )
            .expect("IP6 owner deletes an address it just enumerated");
        }
    }
    set_interface_lifecycle(
        main,
        unsafe { &mut *family.lookup_main.get() },
        unsafe { &mut *family.fib_index_by_sw_if_index.get() },
        unsafe { &mut *family.ip_enabled_by_sw_if_index.get() },
        sw_if_index,
        is_create,
        "ip6-not-enabled",
    );
    Ok(())
}

fn ip6_link_sw_interface_add_del(
    main: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_create: bool,
) -> InterfaceResult<()> {
    if !is_create {
        let links = IP6_LINK_MAIN
            .get()
            .expect("IP6 link Main exists before interface lifecycle callbacks")
            .links_mut();
        if links.get(sw_if_index as usize).is_some_and(Option::is_some) {
            links[sw_if_index as usize] = None;
            ip6_sw_interface_enable_disable(main, sw_if_index, false);
        }
    }
    Ok(())
}

fn ip4_sw_interface_admin_up_down(
    main: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_up: bool,
) -> InterfaceResult<()> {
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main exists before admin state callbacks");
    let fib_index = family
        .fib_index(sw_if_index)
        .expect("live interface has an IP4 FIB mapping");
    let lookup = unsafe { &*family.lookup_main.get() };
    for index in interface_address_indices(lookup, sw_if_index) {
        let address = lookup
            .interface_addresses
            .get(index)
            .copied()
            .expect("IP4 interface address chain remains occupied");
        if is_up {
            ip4_add_interface_routes(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                fib_index,
            );
        } else {
            ip4_del_interface_routes(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                fib_index,
            );
        }
    }
    Ok(())
}

fn ip6_sw_interface_admin_up_down(
    main: &mut DataPlaneMain,
    _: &InterfaceMain,
    sw_if_index: u32,
    is_up: bool,
) -> InterfaceResult<()> {
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main exists before admin state callbacks");
    let fib_index = family
        .fib_index(sw_if_index)
        .expect("live interface has an IP6 FIB mapping");
    let lookup = unsafe { &*family.lookup_main.get() };
    for index in interface_address_indices(lookup, sw_if_index) {
        let address = lookup
            .interface_addresses
            .get(index)
            .copied()
            .expect("IP6 interface address chain remains occupied");
        if is_up {
            ip6_add_interface_routes(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                fib_index,
            );
        } else {
            ip6_del_interface_routes(
                main,
                sw_if_index,
                address.address,
                address.address_length,
                fib_index,
            );
        }
    }
    Ok(())
}

pub(crate) const IP_SW_INTERFACE_CALLBACKS: [InterfaceCallbackRegistration; 3] = [
    InterfaceCallbackRegistration {
        callback: ip4_sw_interface_add_del,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip6_sw_interface_add_del,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip6_link_sw_interface_add_del,
        priority: 0,
    },
];

pub(crate) const IP_ADMIN_UP_DOWN_CALLBACKS: [InterfaceCallbackRegistration; 2] = [
    InterfaceCallbackRegistration {
        callback: ip4_sw_interface_admin_up_down,
        priority: 0,
    },
    InterfaceCallbackRegistration {
        callback: ip6_sw_interface_admin_up_down,
        priority: 0,
    },
];

fn interface_route_dpo(
    main: &mut DataPlaneMain,
    proto: DpoProto,
    child: DpoId,
    flags: FibEntryFlags,
) -> DpoId {
    let net = NetMain::global().expect("interface route requires the network Main");
    let mut load_balance = LoadBalanceDpo::new(
        proto,
        &[LoadBalancePath {
            dpo: child,
            path_index: u32::MAX,
            weight: 1,
        }],
        LoadBalanceFlags::empty(),
        0x9f,
    )
    .expect("interface route path is a valid load-balance path");
    load_balance.fib_entry_flags = flags;
    let dpo = net
        .create_load_balance(main, proto, load_balance)
        .expect("interface route DPO registration is installed");
    net.unlock_dpo(child);
    dpo
}

fn ip4_add_interface_routes(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
    fib_index: u32,
) {
    let net = NetMain::global().expect("IP4 interface route requires the network Main");
    let interfaces = net.interface_main();
    let local = interfaces
        .add_or_lock_receive_dpo(sw_if_index, std::net::IpAddr::V4(address))
        .expect("IP4 receive DPO creation is a control-plane invariant")
        .expect("live interface has an IP4 receive DPO");
    let local_dpo = interface_route_dpo(
        main,
        DpoProto::IP4,
        local,
        FibEntryFlags::CONNECTED | FibEntryFlags::LOCAL,
    );
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main exists before interface route publication");
    let host = Ipv4Net::new(address, 32).expect("IPv4 host prefix is valid");
    family
        .fib_table_mut(fib_index)
        .add_route(host, FibSource::INTERFACE, local_dpo)
        .expect("IP4 local interface route publication cannot fail");
    net.unlock_dpo(local_dpo);

    if address_length < 32 {
        let prefix = Ipv4Net::new(address, address_length)
            .expect("IPv4 interface prefix is valid")
            .trunc();
        let first = {
            let lookup = unsafe { &mut *family.lookup_main.get() };
            let source = lookup
                .interface_address_index_by_key
                .get(&IpInterfaceAddressKey { address, fib_index })
                .copied()
                .expect("added IPv4 interface address remains in the pool");
            lookup.lock_interface_prefix(prefix, sw_if_index, source)
        };
        if !first {
            return;
        }
        let mtu = interfaces
            .software_interface(sw_if_index)
            .expect("connected route requires a live interface")
            .mtu
            .get(InterfaceMtuKind::Ip4)
            .min(u32::from(u16::MAX)) as u16;
        let connected = {
            let adjacency = unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("IP4 ADJ is initialized before interface routes");
            let index = if interfaces.is_p2p(sw_if_index) {
                let neighbors = unsafe { &mut *family.neighbor.get() }
                    .as_mut()
                    .expect("IP4 neighbor DB is initialized before interface routes");
                let index = neighbors.add_or_lock(
                    main,
                    adjacency,
                    IpNetLink::Ip4,
                    Ip4FibProtocol::zero_address(),
                    sw_if_index,
                    mtu,
                );
                if adjacency.get(index).lookup_next == AdjacencyLookupNext::Incomplete {
                    neighbors.update_rewrite(
                        main,
                        &mut net.fib_nodes_mut(),
                        adjacency,
                        index,
                        true,
                        &[],
                    );
                }
                index
            } else {
                unsafe { &mut *family.glean.get() }
                    .as_mut()
                    .expect("IP4 glean DB is initialized before interface routes")
                    .add_or_lock(main, adjacency, IpNetLink::Ip4, sw_if_index, prefix, mtu)
            };
            adjacency.dpo(&net.dpo_main(), index)
        };
        let connected_dpo = interface_route_dpo(
            main,
            DpoProto::IP4,
            connected,
            FibEntryFlags::CONNECTED | FibEntryFlags::ATTACHED,
        );
        family
            .fib_table_mut(fib_index)
            .add_route(prefix, FibSource::INTERFACE, connected_dpo)
            .expect("IP4 connected interface route publication cannot fail");
        net.unlock_dpo(connected_dpo);
        if address_length <= 30 {
            let network = prefix.network();
            let broadcast = prefix.broadcast();
            for special in [network, broadcast] {
                if special == address {
                    continue;
                }
                let drop = DpoId::drop(DpoProto::IP4);
                let drop_dpo = interface_route_dpo(
                    main,
                    DpoProto::IP4,
                    drop,
                    FibEntryFlags::DROP | FibEntryFlags::LOOSE_URPF_EXEMPT,
                );
                family
                    .fib_table_mut(fib_index)
                    .add_route(
                        Ipv4Net::new(special, 32).expect("IPv4 special host prefix is valid"),
                        FibSource::INTERFACE,
                        drop_dpo,
                    )
                    .expect("IP4 interface special route publication cannot fail");
                net.unlock_dpo(drop_dpo);
            }
        } else if address_length == 31 {
            let peer = Ipv4Addr::from(u32::from(address) ^ 1);
            let attached = {
                let adjacency = unsafe { &mut *family.adjacency.get() }
                    .as_mut()
                    .expect("IP4 ADJ is initialized before attached host routes");
                let neighbors = unsafe { &mut *family.neighbor.get() }
                    .as_mut()
                    .expect("IP4 neighbor DB is initialized before attached host routes");
                let next_hop = if interfaces.is_p2p(sw_if_index) {
                    Ipv4Addr::UNSPECIFIED
                } else {
                    peer
                };
                let index = neighbors.add_or_lock(
                    main,
                    adjacency,
                    IpNetLink::Ip4,
                    next_hop,
                    sw_if_index,
                    mtu,
                );
                if interfaces.is_p2p(sw_if_index)
                    && adjacency.get(index).lookup_next == AdjacencyLookupNext::Incomplete
                {
                    neighbors.update_rewrite(
                        main,
                        &mut net.fib_nodes_mut(),
                        adjacency,
                        index,
                        true,
                        &[],
                    );
                }
                adjacency.dpo(&net.dpo_main(), index)
            };
            let attached_dpo =
                interface_route_dpo(main, DpoProto::IP4, attached, FibEntryFlags::ATTACHED);
            family
                .fib_table_mut(fib_index)
                .add_route(
                    Ipv4Net::new(peer, 32).expect("IPv4 peer host prefix is valid"),
                    FibSource::INTERFACE,
                    attached_dpo,
                )
                .expect("IP4 interface peer route publication cannot fail");
            net.unlock_dpo(attached_dpo);
        }
    }
}

fn ip4_del_interface_routes(
    _: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
    fib_index: u32,
) {
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main exists before interface route withdrawal");
    let host = Ipv4Net::new(address, 32).expect("IPv4 host prefix is valid");
    family
        .fib_table_mut(fib_index)
        .remove_route(host, FibSource::INTERFACE)
        .expect("IP4 local interface route withdrawal cannot fail");
    if address_length < 32 {
        let prefix = Ipv4Net::new(address, address_length)
            .expect("IPv4 interface prefix is valid")
            .trunc();
        if unsafe { &mut *family.lookup_main.get() }.unlock_interface_prefix(prefix, sw_if_index)
            != Some(true)
        {
            return;
        }
        if address_length <= 30 {
            for special in [prefix.network(), prefix.broadcast()] {
                if special == address {
                    continue;
                }
                family
                    .fib_table_mut(fib_index)
                    .remove_route(
                        Ipv4Net::new(special, 32).expect("IPv4 special host prefix is valid"),
                        FibSource::INTERFACE,
                    )
                    .expect("IP4 interface special route withdrawal cannot fail");
            }
        } else if address_length == 31 {
            let peer = Ipv4Addr::from(u32::from(address) ^ 1);
            family
                .fib_table_mut(fib_index)
                .remove_route(
                    Ipv4Net::new(peer, 32).expect("IPv4 peer host prefix is valid"),
                    FibSource::INTERFACE,
                )
                .expect("IP4 interface peer route withdrawal cannot fail");
        }
        family
            .fib_table_mut(fib_index)
            .remove_route(prefix, FibSource::INTERFACE)
            .expect("IP4 connected interface route withdrawal cannot fail");
    }
}

fn ip6_add_interface_routes(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv6Addr,
    address_length: u8,
    fib_index: u32,
) {
    let net = NetMain::global().expect("IP6 interface route requires the network Main");
    let interfaces = net.interface_main();
    let local = interfaces
        .add_or_lock_receive_dpo(sw_if_index, std::net::IpAddr::V6(address))
        .expect("IP6 receive DPO creation is a control-plane invariant")
        .expect("live interface has an IP6 receive DPO");
    let local_dpo = interface_route_dpo(
        main,
        DpoProto::IP6,
        local,
        FibEntryFlags::CONNECTED | FibEntryFlags::LOCAL,
    );
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main exists before interface route publication");
    let host = Ipv6Net::new(address, 128).expect("IPv6 host prefix is valid");
    family
        .fib_table_mut(fib_index)
        .add_route(host, FibSource::INTERFACE, local_dpo)
        .expect("IP6 local interface route publication cannot fail");
    net.unlock_dpo(local_dpo);

    if address_length < 128 {
        let prefix = Ipv6Net::new(address, address_length)
            .expect("IPv6 interface prefix is valid")
            .trunc();
        let first = unsafe { &mut *family.lookup_main.get() }.lock_interface_prefix(
            prefix,
            sw_if_index,
            (),
        );
        if !first {
            return;
        }
        let mtu = interfaces
            .software_interface(sw_if_index)
            .expect("connected route requires a live interface")
            .mtu
            .get(InterfaceMtuKind::Ip6)
            .min(u32::from(u16::MAX)) as u16;
        let connected = {
            let adjacency = unsafe { &mut *family.adjacency.get() }
                .as_mut()
                .expect("IP6 ADJ is initialized before interface routes");
            let index = if interfaces.is_p2p(sw_if_index) {
                let neighbors = unsafe { &mut *family.neighbor.get() }
                    .as_mut()
                    .expect("IP6 neighbor DB is initialized before interface routes");
                let index = neighbors.add_or_lock(
                    main,
                    adjacency,
                    IpNetLink::Ip6,
                    Ip6FibProtocol::zero_address(),
                    sw_if_index,
                    mtu,
                );
                if adjacency.get(index).lookup_next == AdjacencyLookupNext::Incomplete {
                    neighbors.update_rewrite(
                        main,
                        &mut net.fib_nodes_mut(),
                        adjacency,
                        index,
                        true,
                        &[],
                    );
                }
                index
            } else {
                unsafe { &mut *family.glean.get() }
                    .as_mut()
                    .expect("IP6 glean DB is initialized before interface routes")
                    .add_or_lock(main, adjacency, IpNetLink::Ip6, sw_if_index, prefix, mtu)
            };
            adjacency.dpo(&net.dpo_main(), index)
        };
        let connected_dpo = interface_route_dpo(
            main,
            DpoProto::IP6,
            connected,
            FibEntryFlags::CONNECTED | FibEntryFlags::ATTACHED,
        );
        family
            .fib_table_mut(fib_index)
            .add_route(prefix, FibSource::INTERFACE, connected_dpo)
            .expect("IP6 connected interface route publication cannot fail");
        net.unlock_dpo(connected_dpo);
    }
}

fn ip6_del_interface_routes(
    _: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv6Addr,
    address_length: u8,
    fib_index: u32,
) {
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main exists before interface route withdrawal");
    if address_length < 128 {
        let prefix = Ipv6Net::new(address, address_length)
            .expect("IPv6 interface prefix is valid")
            .trunc();
        if unsafe { &mut *family.lookup_main.get() }.unlock_interface_prefix(prefix, sw_if_index)
            == Some(true)
        {
            family
                .fib_table_mut(fib_index)
                .remove_route(prefix, FibSource::INTERFACE)
                .expect("IP6 connected interface route withdrawal cannot fail");
        }
    }
    let host = Ipv6Net::new(address, 128).expect("IPv6 host prefix is valid");
    family
        .fib_table_mut(fib_index)
        .remove_route(host, FibSource::INTERFACE)
        .expect("IP6 local interface route withdrawal cannot fail");
}

fn interface_address_indices<A, P, S>(lookup: &IpLookupMain<A, P, S>, sw_if_index: u32) -> Vec<u32>
where
    A: Copy + Eq + Hash,
    P: Copy + Eq + Hash,
    S: Copy,
{
    let mut indices = Vec::new();
    let mut current = lookup
        .interface_address_head_by_sw_if_index
        .get(sw_if_index as usize)
        .copied()
        .flatten();
    while let Some(index) = current {
        indices.push(index);
        current = lookup
            .interface_addresses
            .get(index)
            .expect("interface address chain names an occupied slot")
            .next_this_sw_interface;
    }
    indices
}

fn set_ip_interface_enabled<const ZERO_DISABLE_IS_NOOP: bool>(
    main: &mut DataPlaneMain,
    lookup: &IpLookupMain<impl Copy + Eq + Hash, impl Copy + Eq + Hash, impl Copy>,
    enabled: &mut Vec<u8>,
    sw_if_index: u32,
    is_enable: bool,
    not_enabled_name: &'static str,
) {
    let position = sw_if_index as usize;
    if enabled.len() <= position {
        enabled.resize(position + 1, 0);
    }
    if is_enable {
        enabled[position] = enabled[position]
            .checked_add(1)
            .expect("IP interface enable reference count overflow");
        if enabled[position] != 1 {
            return;
        }
    } else {
        if enabled[position] == 0 && ZERO_DISABLE_IS_NOOP {
            return;
        }
        assert_ne!(
            enabled[position], 0,
            "IP4 interface enable reference count underflow"
        );
        enabled[position] -= 1;
        if enabled[position] != 0 {
            return;
        }
    }
    if lookup.unicast_feature_arc_index == u8::MAX {
        return;
    }
    let features = FeatureMain::global().expect("FeatureMain exists before IP enable transitions");
    let feature = features
        .feature_index(lookup.unicast_feature_arc_index, not_enabled_name)
        .expect("not-enabled feature is registered on the family unicast arc");
    if is_enable {
        features
            .disable_feature(
                main,
                lookup.unicast_feature_arc_index,
                feature,
                sw_if_index,
                &[],
            )
            .expect("IP enable removes the not-enabled feature");
    } else {
        features
            .enable_feature(
                main,
                lookup.unicast_feature_arc_index,
                feature,
                sw_if_index,
                &[],
            )
            .expect("IP disable installs the not-enabled feature");
    }
}

pub fn ip4_sw_interface_enable_disable(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    is_enable: bool,
) {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP4 interface enablement requires publication ownership");
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main is initialized before interface enablement");
    set_ip_interface_enabled::<false>(
        main,
        unsafe { &*family.lookup_main.get() },
        unsafe { &mut *family.ip_enabled_by_sw_if_index.get() },
        sw_if_index,
        is_enable,
        "ip4-not-enabled",
    );
}

pub fn ip6_sw_interface_enable_disable(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    is_enable: bool,
) {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP6 interface enablement requires publication ownership");
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main is initialized before interface enablement");
    set_ip_interface_enabled::<true>(
        main,
        unsafe { &*family.lookup_main.get() },
        unsafe { &mut *family.ip_enabled_by_sw_if_index.get() },
        sw_if_index,
        is_enable,
        "ip6-not-enabled",
    );
}

fn supports_addressing(sw_if_index: u32) -> bool {
    sw_if_index
        != hammer_service::net::NetMain::global()
            .expect("network Main exists before IP address mutation")
            .local_interface_sw_index()
}

fn ipv4_prefix_matches(left: Ipv4Addr, right: Ipv4Addr, length: u8) -> bool {
    if length == 0 {
        return true;
    }
    if length > 32 {
        return false;
    }
    let mask = u32::MAX << (32 - u32::from(length));
    (u32::from(left) ^ u32::from(right)) & mask == 0
}

fn ipv6_prefix_matches(left: Ipv6Addr, right: Ipv6Addr, length: u8) -> bool {
    if length == 0 {
        return true;
    }
    if length > 128 {
        return false;
    }
    let mask = u128::MAX << (128 - u32::from(length));
    (u128::from(left) ^ u128::from(right)) & mask == 0
}

fn add_interface_address<A, P, S>(
    lookup: &mut IpLookupMain<A, P, S>,
    key: IpInterfaceAddressKey<A>,
    sw_if_index: u32,
    address_length: u8,
    width: u8,
) -> Result<u32, IpInterfaceAddressError>
where
    A: Copy + Eq + Hash,
    P: Copy + Eq + Hash,
    S: Copy,
{
    if address_length == 0 || address_length > width {
        return Err(IpInterfaceAddressError::ADDRESS_LENGTH_MISMATCH);
    }
    Ok(lookup.add_interface_address(key, sw_if_index, address_length))
}

fn delete_interface_address<A, P, S>(
    lookup: &mut IpLookupMain<A, P, S>,
    key: IpInterfaceAddressKey<A>,
    sw_if_index: u32,
) -> Result<(u32, IpInterfaceAddress<A>), IpInterfaceAddressError>
where
    A: Copy + Eq + Hash,
    P: Copy + Eq + Hash,
    S: Copy,
{
    let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() else {
        return Err(IpInterfaceAddressError::ADDRESS_NOT_FOUND_FOR_INTERFACE);
    };
    let record = lookup
        .interface_addresses
        .get(index)
        .expect("address hash names an occupied pool slot");
    if record.sw_if_index != sw_if_index {
        return Err(IpInterfaceAddressError::ADDRESS_NOT_FOUND_FOR_INTERFACE);
    }
    Ok((index, lookup.remove_interface_address(key, index)))
}

pub fn ip4_add_del_interface_address(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
    is_delete: bool,
) -> Result<(), IpInterfaceAddressError> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP4 interface address mutation requires publication ownership");
    if !supports_addressing(sw_if_index) {
        return Err(IpInterfaceAddressError::UNSUPPORTED);
    }
    let family = IP4_MAIN
        .get()
        .expect("IP4 Main is initialized before address mutation");
    let fib_index = family
        .fib_index(sw_if_index)
        .expect("live interface has an IP4 FIB mapping");
    let key = IpInterfaceAddressKey { address, fib_index };
    let lookup = unsafe { &mut *family.lookup_main.get() };
    let if_address_index = if is_delete {
        delete_interface_address(lookup, key, sw_if_index)?.0
    } else {
        for (_, existing) in lookup.interface_addresses.iter() {
            let existing_fib = family
                .fib_index(existing.sw_if_index)
                .expect("live IP4 address owner has a FIB mapping");
            if existing_fib != fib_index {
                continue;
            }
            if ipv4_prefix_matches(address, existing.address, existing.address_length)
                || ipv4_prefix_matches(existing.address, address, address_length)
            {
                if existing.sw_if_index == sw_if_index
                    && existing.address_length == address_length
                    && existing.address != address
                {
                    continue;
                }
                return Err(IpInterfaceAddressError::ADDRESS_IN_USE);
            }
        }
        if let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() {
            assert!(
                lookup.interface_addresses.get(index).is_some(),
                "address hash names an occupied pool slot"
            );
            return Err(IpInterfaceAddressError::DUPLICATE_IF_ADDRESS);
        }
        add_interface_address(lookup, key, sw_if_index, address_length, 32)?
    };
    ip4_sw_interface_enable_disable(main, sw_if_index, !is_delete);
    let admin_up = NetMain::global()
        .expect("IP4 address mutation requires the network Main")
        .interface_main()
        .software_interface(sw_if_index)
        .is_some_and(|interface| interface.is_admin_up());
    if admin_up {
        if is_delete {
            ip4_del_interface_routes(main, sw_if_index, address, address_length, fib_index);
        } else {
            ip4_add_interface_routes(main, sw_if_index, address, address_length, fib_index);
        }
    }
    let callbacks = unsafe { &*family.add_del_interface_address_callbacks.get() };
    for callback in callbacks {
        callback(
            main,
            sw_if_index,
            address,
            address_length,
            if_address_index,
            is_delete,
        );
    }
    Ok(())
}

fn generated_link_local(main: &mut DataPlaneMain, sw_if_index: u32) -> Ipv6Addr {
    let interfaces = hammer_service::net::NetMain::global()
        .expect("network Main exists before IP6 link enablement")
        .interface_main();
    let mac = interfaces
        .software_interface(sw_if_index)
        .and_then(|interface| interfaces.software_interface(interface.sup_sw_if_index))
        .and_then(|interface| interface.hw_if_index)
        .map(|hw_if_index| &interfaces.hardware_interface(hw_if_index).hw_address)
        .and_then(|address| <[u8; 6]>::try_from(address.as_slice()).ok());
    if let Some(mac) = mac {
        let mut octets = [0; 16];
        octets[0] = 0xfe;
        octets[1] = 0x80;
        octets[8] = mac[0] ^ (1 << 1);
        octets[9] = mac[1];
        octets[10] = mac[2];
        octets[11] = 0xff;
        octets[12] = 0xfe;
        octets[13] = mac[3];
        octets[14] = mac[4];
        octets[15] = mac[5];
        return Ipv6Addr::from(octets);
    }

    let mut identifier = main.random().next_u64();
    identifier &= !(1_u64 << 57);
    if identifier == 0 {
        identifier = u64::from(sw_if_index) + 1;
    }
    Ipv6Addr::from((0xfe80_u128 << 112) | u128::from(identifier))
}

fn ip6_link_enable(main: &mut DataPlaneMain, sw_if_index: u32, address: Option<Ipv6Addr>) {
    let links = IP6_LINK_MAIN
        .get()
        .expect("IP6 link Main is initialized before link enablement")
        .links_mut();
    let position = sw_if_index as usize;
    if links.len() <= position {
        links.resize(position + 1, None);
    }
    if let Some(link) = links[position].as_mut() {
        if let Some(address) = address {
            link.link_local_address = address;
        } else {
            link.locks = link
                .locks
                .checked_add(1)
                .expect("IP6 link reference count overflow");
        }
        return;
    }
    links[position] = Some(Ip6Link {
        sw_if_index,
        link_local_address: address.unwrap_or_else(|| generated_link_local(main, sw_if_index)),
        locks: 1,
    });
    ip6_sw_interface_enable_disable(main, sw_if_index, true);
}

fn ip6_link_disable(main: &mut DataPlaneMain, sw_if_index: u32) {
    let links = IP6_LINK_MAIN
        .get()
        .expect("IP6 link Main is initialized before link disablement")
        .links_mut();
    let Some(link) = links.get_mut(sw_if_index as usize).and_then(Option::as_mut) else {
        return;
    };
    link.locks = link
        .locks
        .checked_sub(1)
        .expect("IP6 link reference count is positive");
    if link.locks == 0 {
        links[sw_if_index as usize] = None;
        ip6_sw_interface_enable_disable(main, sw_if_index, false);
    }
}

pub fn ip6_add_del_interface_address(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv6Addr,
    address_length: u8,
    is_delete: bool,
) -> Result<(), IpInterfaceAddressError> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP6 interface address mutation requires publication ownership");
    if !supports_addressing(sw_if_index) {
        return Err(IpInterfaceAddressError::UNSUPPORTED);
    }
    if address.is_unicast_link_local() {
        if address_length != 128 {
            return Err(IpInterfaceAddressError::ADDRESS_LENGTH_MISMATCH);
        }
        let current = IP6_LINK_MAIN
            .get()
            .expect("IP6 link Main is initialized before address mutation")
            .links()
            .get(sw_if_index as usize)
            .copied()
            .flatten();
        if is_delete {
            return match current {
                Some(link) if link.link_local_address == address => {
                    Err(IpInterfaceAddressError::ADDRESS_NOT_DELETABLE)
                }
                _ => Err(IpInterfaceAddressError::ADDRESS_NOT_FOUND_FOR_INTERFACE),
            };
        }
        ip6_link_enable(main, sw_if_index, Some(address));
        return Ok(());
    }
    let family = IP6_MAIN
        .get()
        .expect("IP6 Main is initialized before address mutation");
    let fib_index = family
        .fib_index(sw_if_index)
        .expect("live interface has an IP6 FIB mapping");
    let key = IpInterfaceAddressKey { address, fib_index };
    let lookup = unsafe { &mut *family.lookup_main.get() };
    let if_address_index = if is_delete {
        delete_interface_address(lookup, key, sw_if_index)?.0
    } else {
        for (_, existing) in lookup.interface_addresses.iter() {
            let existing_fib = family
                .fib_index(existing.sw_if_index)
                .expect("live IP6 address owner has a FIB mapping");
            if existing_fib != fib_index {
                continue;
            }
            if ipv6_prefix_matches(address, existing.address, existing.address_length)
                || ipv6_prefix_matches(existing.address, address, address_length)
            {
                if existing.sw_if_index == sw_if_index
                    && existing.address_length == address_length
                    && existing.address != address
                {
                    continue;
                }
                return Err(IpInterfaceAddressError::DUPLICATE_IF_ADDRESS);
            }
        }
        if let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() {
            assert!(
                lookup.interface_addresses.get(index).is_some(),
                "address hash names an occupied pool slot"
            );
            return Err(IpInterfaceAddressError::DUPLICATE_IF_ADDRESS);
        }
        add_interface_address(lookup, key, sw_if_index, address_length, 128)?
    };
    ip6_sw_interface_enable_disable(main, sw_if_index, !is_delete);
    if !is_delete {
        ip6_link_enable(main, sw_if_index, None);
    }
    let admin_up = NetMain::global()
        .expect("IP6 address mutation requires the network Main")
        .interface_main()
        .software_interface(sw_if_index)
        .is_some_and(|interface| interface.is_admin_up());
    if admin_up {
        if is_delete {
            ip6_del_interface_routes(main, sw_if_index, address, address_length, fib_index);
        } else {
            ip6_add_interface_routes(main, sw_if_index, address, address_length, fib_index);
        }
    }
    let callbacks = unsafe { &*family.add_del_interface_address_callbacks.get() };
    for callback in callbacks {
        callback(
            main,
            sw_if_index,
            address,
            address_length,
            if_address_index,
            is_delete,
        );
    }
    if is_delete {
        ip6_link_disable(main, sw_if_index);
    }
    Ok(())
}

pub(crate) fn ip4_source_address(sw_if_index: u32, destination: Ipv4Addr) -> Option<Ipv4Addr> {
    let lookup = unsafe {
        &*IP4_MAIN
            .get()
            .expect("IP4 Main exists before source address selection")
            .lookup_main
            .get()
    };
    interface_address_indices(lookup, sw_if_index)
        .into_iter()
        .filter_map(|index| lookup.interface_addresses.get(index))
        .max_by_key(|record| (u32::from(record.address) ^ u32::from(destination)).leading_zeros())
        .map(|record| record.address)
}

pub(crate) fn ip6_source_address(sw_if_index: u32, destination: Ipv6Addr) -> Option<Ipv6Addr> {
    if destination.is_unicast_link_local() || destination.segments()[..2] == [0xff02, 0] {
        return IP6_LINK_MAIN
            .get()
            .expect("IP6 link Main exists before source address selection")
            .links()
            .get(sw_if_index as usize)
            .copied()
            .flatten()
            .map(|link| {
                assert_eq!(link.sw_if_index, sw_if_index);
                link.link_local_address
            });
    }
    let lookup = unsafe {
        &*IP6_MAIN
            .get()
            .expect("IP6 Main exists before source address selection")
            .lookup_main
            .get()
    };
    interface_address_indices(lookup, sw_if_index)
        .into_iter()
        .filter_map(|index| lookup.interface_addresses.get(index))
        .max_by_key(|record| (u128::from(record.address) ^ u128::from(destination)).leading_zeros())
        .map(|record| record.address)
}

#[hammer_component_macros::main_loop_enter_function(
    name = "ip_interface_feature_init",
    runs_after = ["feature_arc_init"]
)]
fn ip_interface_feature_init(main: &mut DataPlaneMain) -> hammer_runtime::RuntimeResult<()> {
    let interfaces = hammer_service::net::NetMain::global()?.interface_main();
    for sw_if_index in interfaces.software_interface_indices() {
        let ip4 = IP4_MAIN
            .get()
            .expect("IP4 Main exists before feature catch-up");
        let ip4_lookup = unsafe { &*ip4.lookup_main.get() };
        let ip4_enabled = unsafe { &*ip4.ip_enabled_by_sw_if_index.get() };
        if ip4_enabled.get(sw_if_index as usize).copied().unwrap_or(0) == 0 {
            let features = FeatureMain::global()?;
            let feature = features
                .feature_index(ip4_lookup.unicast_feature_arc_index, "ip4-not-enabled")
                .expect("IP4 not-enabled feature is registered");
            features
                .enable_feature(
                    main,
                    ip4_lookup.unicast_feature_arc_index,
                    feature,
                    sw_if_index,
                    &[],
                )
                .expect("live interface accepts its default IP4 not-enabled feature");
        }
        let ip6 = IP6_MAIN
            .get()
            .expect("IP6 Main exists before feature catch-up");
        let ip6_lookup = unsafe { &*ip6.lookup_main.get() };
        let ip6_enabled = unsafe { &*ip6.ip_enabled_by_sw_if_index.get() };
        if ip6_enabled.get(sw_if_index as usize).copied().unwrap_or(0) == 0 {
            let features = FeatureMain::global()?;
            let feature = features
                .feature_index(ip6_lookup.unicast_feature_arc_index, "ip6-not-enabled")
                .expect("IP6 not-enabled feature is registered");
            features
                .enable_feature(
                    main,
                    ip6_lookup.unicast_feature_arc_index,
                    feature,
                    sw_if_index,
                    &[],
                )
                .expect("live interface accepts its default IP6 not-enabled feature");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hammer_core::data_plane::Buffer;
    use hammer_runtime::{DataPlaneBufferConfig, DataPlaneMain};
    use hammer_service::feature::FeatureMain;
    use hammer_service::interface::{InterfaceMain, SwInterfaceFlags};
    use hammer_service::net::NetMain;

    use super::*;
    use crate::lookup::{Ip4LookupNode, Ip6LookupNode};
    use crate::protocol::ip::IpVersion;
    use crate::punt::{Ip4DropNode, Ip4NotEnabledNode, Ip6DropNode, Ip6NotEnabledNode};
    use crate::{Ip4InputNode, Ip6InputNode};

    fn empty_feature_buffer() -> Buffer {
        // SAFETY: Buffer's fixed-layout fields accept zero. This test only
        // exercises the Feature config cursor and never owns packet storage.
        unsafe { std::mem::MaybeUninit::<Buffer>::zeroed().assume_init() }
    }

    #[test]
    fn interface_enable_fib_and_address_operations_match_vpp() {
        hammer_runtime::ThreadMain::new().unwrap();
        let mut data_plane = DataPlaneMain::new(DataPlaneBufferConfig::default());
        assert!(IP4_MAIN.set(crate::lookup::Ip4Main::new()).is_ok());
        assert!(IP6_MAIN.set(crate::lookup::Ip6Main::new()).is_ok());
        (super::__INIT_FN_IP6_LINK_INIT.func)(&mut data_plane).unwrap();

        let interfaces = Arc::new(InterfaceMain::new());
        interfaces
            .consume_registration_image(&crate::HAMMER_INTERFACE_REGISTRATION_IMAGE)
            .unwrap();
        let net = NetMain::init(&mut data_plane, Arc::clone(&interfaces)).unwrap();
        FeatureMain::init().unwrap();
        let features = FeatureMain::global().unwrap();

        let terminal = hammer_service::data_plane::register_drop(&mut data_plane).unwrap();
        (hammer_service::interface::__SERVICE_GRAPH_NODE_INTERFACE_OUTPUT_NODE.init)(&data_plane)
            .unwrap();
        (crate::ip::input::__IP_GRAPH_NODE_IP4_INPUT_NODE.init)(&data_plane).unwrap();
        (crate::ip::input::__IP_GRAPH_NODE_IP6_INPUT_NODE.init)(&data_plane).unwrap();
        (crate::ip::local::__IP_GRAPH_NODE_IP4_LOCAL_NODE.init)(&data_plane).unwrap();
        (crate::ip::local::__IP_GRAPH_NODE_IP6_LOCAL_NODE.init)(&data_plane).unwrap();
        (crate::ip::local::__IP_GRAPH_NODE_IP4_RECEIVE_NODE.init)(&data_plane).unwrap();
        (crate::ip::local::__IP_GRAPH_NODE_IP6_RECEIVE_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP4_LOOKUP_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP6_LOOKUP_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP4_LOAD_BALANCE_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP6_LOAD_BALANCE_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP4_DROP_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP4_NOT_ENABLED_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP6_DROP_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP6_NOT_ENABLED_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP4_GLEAN_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP6_GLEAN_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP4_INCOMPLETE_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP6_INCOMPLETE_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP4_REWRITE_NODE.init)(&data_plane).unwrap();
        (crate::adjacency::__IP_GRAPH_NODE_IP6_REWRITE_NODE.init)(&data_plane).unwrap();
        data_plane.nodes().resolve_named_next_nodes().unwrap();

        let ip4_arc = Ip4InputNode::register_feature_arc(features, data_plane.nodes()).unwrap();
        let ip6_arc = Ip6InputNode::register_feature_arc(features, data_plane.nodes()).unwrap();
        let ip4_drop_arc = Ip4DropNode::register_feature_arc(features, data_plane.nodes()).unwrap();
        let ip6_drop_arc = Ip6DropNode::register_feature_arc(features, data_plane.nodes()).unwrap();
        unsafe {
            (*IP4_MAIN.get().unwrap().lookup_main.get()).unicast_feature_arc_index = ip4_arc;
            *IP4_MAIN.get().unwrap().drop_feature_arc_index.get() = ip4_drop_arc;
            (*IP6_MAIN.get().unwrap().lookup_main.get()).unicast_feature_arc_index = ip6_arc;
            *IP6_MAIN.get().unwrap().drop_feature_arc_index.get() = ip6_drop_arc;
        }
        Ip4LookupNode::register_feature(features, data_plane.nodes()).unwrap();
        Ip4NotEnabledNode::register_feature(features, data_plane.nodes()).unwrap();
        Ip6LookupNode::register_feature(features, data_plane.nodes()).unwrap();
        Ip6NotEnabledNode::register_feature(features, data_plane.nodes()).unwrap();
        features
            .register_feature(
                Ip4DropNode::FEATURE_ARC_NAME,
                hammer_service::data_plane::DropNode::NODE_NAME,
                terminal,
                &[],
                &[],
            )
            .unwrap();
        features
            .register_feature(
                Ip6DropNode::FEATURE_ARC_NAME,
                hammer_service::data_plane::DropNode::NODE_NAME,
                terminal,
                &[],
                &[],
            )
            .unwrap();
        hammer_service::feature::feature_arc_init(&mut data_plane).unwrap();
        (super::__INIT_FN_IP_INTERFACE_FEATURE_INIT.func)(&mut data_plane).unwrap();

        let hardware = interfaces
            .register_hardware_interface(
                &mut data_plane,
                interfaces.device_class_index("local"),
                1,
                interfaces.hw_class_index("local"),
                0,
            )
            .unwrap();
        let sw_if_index = interfaces.hardware_interface(hardware).sw_if_index();
        let disabled_hardware = interfaces
            .register_hardware_interface(
                &mut data_plane,
                interfaces.device_class_index("local"),
                2,
                interfaces.hw_class_index("local"),
                0,
            )
            .unwrap();
        let disabled_sw_if_index = interfaces
            .hardware_interface(disabled_hardware)
            .sw_if_index();
        assert_eq!(
            crate::lookup::fib_table_get_index_for_sw_if_index(IpVersion::V4, sw_if_index),
            Some(0)
        );
        assert_eq!(
            crate::lookup::fib_table_get_index_for_sw_if_index(IpVersion::V6, sw_if_index),
            Some(0)
        );
        let ip4_not_enabled = features
            .feature_index(ip4_arc, Ip4NotEnabledNode::NODE_NAME)
            .unwrap();
        let ip6_not_enabled = features
            .feature_index(ip6_arc, Ip6NotEnabledNode::NODE_NAME)
            .unwrap();
        assert!(
            features
                .is_feature_enabled(ip4_arc, ip4_not_enabled, sw_if_index)
                .unwrap()
        );
        assert!(
            features
                .is_feature_enabled(ip6_arc, ip6_not_enabled, sw_if_index)
                .unwrap()
        );

        ip4_sw_interface_enable_disable(&mut data_plane, sw_if_index, true);
        ip6_sw_interface_enable_disable(&mut data_plane, sw_if_index, true);
        assert!(
            !features
                .is_feature_enabled(ip4_arc, ip4_not_enabled, sw_if_index)
                .unwrap()
        );
        assert!(
            !features
                .is_feature_enabled(ip6_arc, ip6_not_enabled, sw_if_index)
                .unwrap()
        );

        let ip4_input = data_plane
            .nodes()
            .node_by_name(Ip4InputNode::NODE_NAME)
            .unwrap();
        let ip6_input = data_plane
            .nodes()
            .node_by_name(Ip6InputNode::NODE_NAME)
            .unwrap();
        let ip4_lookup = data_plane
            .nodes()
            .node_by_name(Ip4LookupNode::NODE_NAME)
            .unwrap();
        let ip6_lookup = data_plane
            .nodes()
            .node_by_name(Ip6LookupNode::NODE_NAME)
            .unwrap();
        let ip4_not_enabled_node = data_plane
            .nodes()
            .node_by_name(Ip4NotEnabledNode::NODE_NAME)
            .unwrap();
        let ip6_not_enabled_node = data_plane
            .nodes()
            .node_by_name(Ip6NotEnabledNode::NODE_NAME)
            .unwrap();
        let ip4_lookup_next = data_plane
            .nodes()
            .node_next_slot_for_target(ip4_input, ip4_lookup)
            .unwrap()
            .unwrap();
        let ip6_lookup_next = data_plane
            .nodes()
            .node_next_slot_for_target(ip6_input, ip6_lookup)
            .unwrap()
            .unwrap();
        let mut buffer = empty_feature_buffer();
        let enabled_ip4_next =
            features.start_feature_arc(ip4_arc, sw_if_index, &mut buffer, ip4_lookup_next);
        let disabled_ip4_next =
            features.start_feature_arc(ip4_arc, disabled_sw_if_index, &mut buffer, ip4_lookup_next);
        let enabled_ip6_next =
            features.start_feature_arc(ip6_arc, sw_if_index, &mut buffer, ip6_lookup_next);
        let disabled_ip6_next =
            features.start_feature_arc(ip6_arc, disabled_sw_if_index, &mut buffer, ip6_lookup_next);
        assert_eq!(
            data_plane
                .nodes()
                .node_next_slot(ip4_input, usize::from(enabled_ip4_next))
                .unwrap(),
            ip4_lookup
        );
        assert_eq!(
            data_plane
                .nodes()
                .node_next_slot(ip4_input, usize::from(disabled_ip4_next))
                .unwrap(),
            ip4_not_enabled_node
        );
        assert_eq!(
            data_plane
                .nodes()
                .node_next_slot(ip6_input, usize::from(enabled_ip6_next))
                .unwrap(),
            ip6_lookup
        );
        assert_eq!(
            data_plane
                .nodes()
                .node_next_slot(ip6_input, usize::from(disabled_ip6_next))
                .unwrap(),
            ip6_not_enabled_node
        );

        ip4_sw_interface_enable_disable(&mut data_plane, sw_if_index, false);
        assert!(
            features
                .is_feature_enabled(ip4_arc, ip4_not_enabled, sw_if_index)
                .unwrap()
        );
        ip6_sw_interface_enable_disable(&mut data_plane, sw_if_index, false);
        assert!(
            features
                .is_feature_enabled(ip6_arc, ip6_not_enabled, sw_if_index)
                .unwrap()
        );

        let ip4 = Ipv4Addr::new(192, 0, 2, 1);
        assert_eq!(
            ip4_add_del_interface_address(
                &mut data_plane,
                net.local_interface_sw_index(),
                ip4,
                24,
                false,
            )
            .unwrap_err()
            .code(),
            -126
        );
        assert_eq!(
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 0, false)
                .unwrap_err()
                .code(),
            -59
        );
        ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, false).unwrap();
        let ip4_host = Ipv4Net::new(ip4, 32).unwrap();
        let ip4_connected = Ipv4Net::new(ip4, 24).unwrap().trunc();
        assert_eq!(
            IP4_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip4_host),
            None
        );
        interfaces
            .set_software_flags(&mut data_plane, sw_if_index, SwInterfaceFlags::ADMIN_UP)
            .unwrap();
        assert!(
            IP4_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .forwarding_lookup(ip4)
                .is_some()
        );
        assert!(
            IP4_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip4_connected)
                .is_some()
        );
        interfaces
            .set_software_flags(&mut data_plane, sw_if_index, SwInterfaceFlags::empty())
            .unwrap();
        assert_eq!(
            IP4_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip4_host),
            None
        );
        assert_eq!(
            IP4_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip4_connected),
            None
        );
        assert!(
            unsafe { &*IP4_MAIN.get().unwrap().lookup_main.get() }
                .interface_address_index_by_key
                .contains_key(&IpInterfaceAddressKey {
                    address: ip4,
                    fib_index: 0,
                })
        );
        assert_eq!(
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, false)
                .unwrap_err()
                .code(),
            -105
        );
        ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 31, true).unwrap();
        assert_eq!(
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, true)
                .unwrap_err()
                .code(),
            -60
        );

        let ip6 = "2001:db8::1".parse().unwrap();
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 64, false).unwrap();
        let ip6_host = Ipv6Net::new(ip6, 128).unwrap();
        let ip6_connected = Ipv6Net::new(ip6, 64).unwrap().trunc();
        assert_eq!(
            IP6_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip6_host),
            None
        );
        interfaces
            .set_software_flags(&mut data_plane, sw_if_index, SwInterfaceFlags::ADMIN_UP)
            .unwrap();
        assert!(
            IP6_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .forwarding_lookup(ip6)
                .is_some()
        );
        assert!(
            IP6_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip6_connected)
                .is_some()
        );
        interfaces
            .set_software_flags(&mut data_plane, sw_if_index, SwInterfaceFlags::empty())
            .unwrap();
        assert_eq!(
            IP6_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip6_host),
            None
        );
        assert_eq!(
            IP6_MAIN
                .get()
                .unwrap()
                .fib_table_mut(0)
                .lookup_exact(ip6_connected),
            None
        );
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 64, false)
                .unwrap_err()
                .code(),
            -127
        );
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 1, true).unwrap();

        let link_local = "fe80::1".parse().unwrap();
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 64, false,)
                .unwrap_err()
                .code(),
            -59
        );
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 128, false)
            .unwrap();
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 128, true,)
                .unwrap_err()
                .code(),
            -61
        );

        interfaces
            .delete_hardware_interface(&mut data_plane, hardware)
            .unwrap();
        interfaces
            .delete_hardware_interface(&mut data_plane, disabled_hardware)
            .unwrap();
        assert_eq!(
            crate::lookup::fib_table_get_index_for_sw_if_index(IpVersion::V4, sw_if_index),
            None
        );
        assert_eq!(
            crate::lookup::fib_table_get_index_for_sw_if_index(IpVersion::V6, sw_if_index),
            None
        );
        assert_eq!(net.local_interface_sw_index(), 0);
    }
}
