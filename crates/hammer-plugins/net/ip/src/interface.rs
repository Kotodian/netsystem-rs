use std::cell::UnsafeCell;
use std::fmt;
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;

use hammer_runtime::DataPlaneMain;
use hammer_service::feature::FeatureMain;
use hammer_service::interface::{InterfaceCallbackRegistration, InterfaceMain, InterfaceResult};
use rand_09::RngCore;

use crate::lookup::{IP4_MAIN, IP6_MAIN, IpInterfaceAddress, IpInterfaceAddressKey, IpLookupMain};

pub type IpInterfaceAddressCallback<A> = fn(&mut DataPlaneMain, u32, A, u8, u32, bool);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpInterfaceAddressError<A> {
    Unsupported {
        sw_if_index: u32,
    },
    AddressLengthMismatch {
        address: A,
        address_length: u8,
    },
    AddressInUse {
        address: A,
        conflicting_sw_if_index: u32,
    },
    DuplicateInterfaceAddress {
        address: A,
        existing_sw_if_index: u32,
    },
    AddressNotFoundForInterface {
        sw_if_index: u32,
        address: A,
    },
    AddressNotDeletable {
        sw_if_index: u32,
        address: A,
    },
}

impl<A: fmt::Debug> fmt::Display for IpInterfaceAddressError<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { sw_if_index } => {
                write!(
                    formatter,
                    "interface {sw_if_index} does not support IP addressing"
                )
            }
            Self::AddressLengthMismatch {
                address,
                address_length,
            } => write!(
                formatter,
                "address {address:?}/{address_length} has an invalid length"
            ),
            Self::AddressInUse {
                address,
                conflicting_sw_if_index,
            } => write!(
                formatter,
                "address {address:?} conflicts with interface {conflicting_sw_if_index}"
            ),
            Self::DuplicateInterfaceAddress {
                address,
                existing_sw_if_index,
            } => write!(
                formatter,
                "address {address:?} already exists on interface {existing_sw_if_index}"
            ),
            Self::AddressNotFoundForInterface {
                sw_if_index,
                address,
            } => write!(
                formatter,
                "address {address:?} was not found on interface {sw_if_index}"
            ),
            Self::AddressNotDeletable {
                sw_if_index,
                address,
            } => write!(
                formatter,
                "address {address:?} on interface {sw_if_index} is not deletable"
            ),
        }
    }
}

impl<A: fmt::Debug> std::error::Error for IpInterfaceAddressError<A> {}

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
    lookup: &mut IpLookupMain<impl Copy + Eq + Hash>,
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

fn interface_address_indices<A>(lookup: &IpLookupMain<A>, sw_if_index: u32) -> Vec<u32>
where
    A: Copy + Eq + Hash,
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
    lookup: &IpLookupMain<impl Copy + Eq + Hash>,
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

fn add_interface_address<A>(
    lookup: &mut IpLookupMain<A>,
    key: IpInterfaceAddressKey<A>,
    sw_if_index: u32,
    address_length: u8,
    width: u8,
) -> Result<u32, IpInterfaceAddressError<A>>
where
    A: Copy + Eq + Hash,
{
    if address_length == 0 || address_length > width {
        return Err(IpInterfaceAddressError::AddressLengthMismatch {
            address: key.address,
            address_length,
        });
    }
    Ok(lookup.add_interface_address(key, sw_if_index, address_length))
}

fn delete_interface_address<A>(
    lookup: &mut IpLookupMain<A>,
    key: IpInterfaceAddressKey<A>,
    sw_if_index: u32,
) -> Result<(u32, IpInterfaceAddress<A>), IpInterfaceAddressError<A>>
where
    A: Copy + Eq + Hash,
{
    let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() else {
        return Err(IpInterfaceAddressError::AddressNotFoundForInterface {
            sw_if_index,
            address: key.address,
        });
    };
    let record = lookup
        .interface_addresses
        .get(index)
        .expect("address hash names an occupied pool slot");
    if record.sw_if_index != sw_if_index {
        return Err(IpInterfaceAddressError::AddressNotFoundForInterface {
            sw_if_index,
            address: key.address,
        });
    }
    Ok((index, lookup.remove_interface_address(key, index)))
}

pub fn ip4_add_del_interface_address(
    main: &mut DataPlaneMain,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
    is_delete: bool,
) -> Result<(), IpInterfaceAddressError<Ipv4Addr>> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP4 interface address mutation requires publication ownership");
    if !supports_addressing(sw_if_index) {
        return Err(IpInterfaceAddressError::Unsupported { sw_if_index });
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
                return Err(IpInterfaceAddressError::AddressInUse {
                    address,
                    conflicting_sw_if_index: existing.sw_if_index,
                });
            }
        }
        if let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() {
            let existing = lookup
                .interface_addresses
                .get(index)
                .expect("address hash names an occupied pool slot");
            return Err(IpInterfaceAddressError::DuplicateInterfaceAddress {
                address,
                existing_sw_if_index: existing.sw_if_index,
            });
        }
        add_interface_address(lookup, key, sw_if_index, address_length, 32)?
    };
    ip4_sw_interface_enable_disable(main, sw_if_index, !is_delete);
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
) -> Result<(), IpInterfaceAddressError<Ipv6Addr>> {
    hammer_runtime::ensure_main_thread_with_barrier()
        .expect("IP6 interface address mutation requires publication ownership");
    if !supports_addressing(sw_if_index) {
        return Err(IpInterfaceAddressError::Unsupported { sw_if_index });
    }
    if address.is_unicast_link_local() {
        if address_length != 128 {
            return Err(IpInterfaceAddressError::AddressLengthMismatch {
                address,
                address_length,
            });
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
                    Err(IpInterfaceAddressError::AddressNotDeletable {
                        sw_if_index,
                        address,
                    })
                }
                _ => Err(IpInterfaceAddressError::AddressNotFoundForInterface {
                    sw_if_index,
                    address,
                }),
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
                return Err(IpInterfaceAddressError::DuplicateInterfaceAddress {
                    address,
                    existing_sw_if_index: existing.sw_if_index,
                });
            }
        }
        if let Some(index) = lookup.interface_address_index_by_key.get(&key).copied() {
            let existing = lookup
                .interface_addresses
                .get(index)
                .expect("address hash names an occupied pool slot");
            return Err(IpInterfaceAddressError::DuplicateInterfaceAddress {
                address,
                existing_sw_if_index: existing.sw_if_index,
            });
        }
        add_interface_address(lookup, key, sw_if_index, address_length, 128)?
    };
    ip6_sw_interface_enable_disable(main, sw_if_index, !is_delete);
    if !is_delete {
        ip6_link_enable(main, sw_if_index, None);
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
    use hammer_service::interface::InterfaceMain;
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
        (crate::ip::input::__IP_GRAPH_NODE_IP4_INPUT_NODE.init)(&data_plane).unwrap();
        (crate::ip::input::__IP_GRAPH_NODE_IP6_INPUT_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP4_LOOKUP_NODE.init)(&data_plane).unwrap();
        (crate::lookup::__IP_GRAPH_NODE_IP6_LOOKUP_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP4_DROP_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP4_NOT_ENABLED_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP6_DROP_NODE.init)(&data_plane).unwrap();
        (crate::punt::__IP_GRAPH_NODE_IP6_NOT_ENABLED_NODE.init)(&data_plane).unwrap();

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
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 0, false),
            Err(IpInterfaceAddressError::AddressLengthMismatch {
                address: ip4,
                address_length: 0,
            })
        );
        ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, false).unwrap();
        assert_eq!(
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, false),
            Err(IpInterfaceAddressError::AddressInUse {
                address: ip4,
                conflicting_sw_if_index: sw_if_index,
            })
        );
        ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 31, true).unwrap();
        assert_eq!(
            ip4_add_del_interface_address(&mut data_plane, sw_if_index, ip4, 24, true),
            Err(IpInterfaceAddressError::AddressNotFoundForInterface {
                sw_if_index,
                address: ip4,
            })
        );

        let ip6 = "2001:db8::1".parse().unwrap();
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 64, false).unwrap();
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 64, false),
            Err(IpInterfaceAddressError::DuplicateInterfaceAddress {
                address: ip6,
                existing_sw_if_index: sw_if_index,
            })
        );
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, ip6, 1, true).unwrap();

        let link_local = "fe80::1".parse().unwrap();
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 64, false,),
            Err(IpInterfaceAddressError::AddressLengthMismatch {
                address: link_local,
                address_length: 64,
            })
        );
        ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 128, false)
            .unwrap();
        assert_eq!(
            ip6_add_del_interface_address(&mut data_plane, sw_if_index, link_local, 128, true,),
            Err(IpInterfaceAddressError::AddressNotDeletable {
                sw_if_index,
                address: link_local,
            })
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
