use ipnet::{Ipv4Net, Ipv6Net};
use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::hash::Hash;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;
use std::time::Instant;

use hammer_service::net::throttle::Throttle;

use hammer_core::data_plane::{BufferPacketCursor, Frame, NodeId, NodeNext};
use hammer_infra::pool::Pool;
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, RuntimeResult};
use hammer_service::net::adj::AdjacencyMain;
use hammer_service::net::adj_glean::AdjacencyGleanMain;
use hammer_service::net::adj_nbr::AdjacencyNeighborMain;
use hammer_service::net::{DpoId, DpoProto, DpoType, NetMain};
use hammer_service::opaque::NetworkOpaque;

use crate::adjacency::{Ip4FibProtocol, Ip6FibProtocol};
use crate::fib::{Ip4FibTable, Ip6FibTable};
use crate::interface::IpInterfaceAddressCallback;
use crate::protocol::ip::{IpProtocol, IpVersion, Ipv4Header, Ipv6Header};
use zerocopy::FromBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IpInterfaceAddressKey<A> {
    pub(crate) address: A,
    pub(crate) fib_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IpInterfaceAddress<A> {
    pub(crate) address: A,
    pub(crate) address_length: u8,
    pub(crate) sw_if_index: u32,
    pub(crate) next_this_sw_interface: Option<u32>,
    pub(crate) prev_this_sw_interface: Option<u32>,
}

pub(crate) struct IpInterfacePrefix<P, S> {
    pub(crate) prefix: P,
    pub(crate) sw_if_index: u32,
    pub(crate) reference_count: u32,
    pub(crate) source: S,
}

pub(crate) struct IpLookupMain<A, P, S> {
    pub(crate) interface_addresses: Pool<IpInterfaceAddress<A>>,
    pub(crate) interface_address_index_by_key: HashMap<IpInterfaceAddressKey<A>, u32>,
    pub(crate) interface_address_head_by_sw_if_index: Vec<Option<u32>>,
    pub(crate) interface_prefixes: Pool<IpInterfacePrefix<P, S>>,
    pub(crate) interface_prefix_index_by_key: HashMap<(P, u32), u32>,
    pub(crate) unicast_feature_arc_index: u8,
    pub(crate) output_feature_arc_index: u8,
    pub(crate) local_next_by_ip_protocol: [u16; 256],
}

impl<A, P, S> IpLookupMain<A, P, S>
where
    A: Copy + Eq + Hash,
    P: Copy + Eq + Hash,
    S: Copy,
{
    fn new(punt_next: u16) -> Self {
        Self {
            interface_addresses: Pool::new(),
            interface_address_index_by_key: HashMap::new(),
            interface_address_head_by_sw_if_index: Vec::new(),
            interface_prefixes: Pool::new(),
            interface_prefix_index_by_key: HashMap::new(),
            unicast_feature_arc_index: u8::MAX,
            output_feature_arc_index: u8::MAX,
            local_next_by_ip_protocol: [punt_next; 256],
        }
    }

    pub(crate) fn add_interface_address(
        &mut self,
        key: IpInterfaceAddressKey<A>,
        sw_if_index: u32,
        address_length: u8,
    ) -> u32 {
        let position = sw_if_index as usize;
        if self.interface_address_head_by_sw_if_index.len() <= position {
            self.interface_address_head_by_sw_if_index
                .resize(position + 1, None);
        }
        let mut previous = None;
        let mut current = self.interface_address_head_by_sw_if_index[position];
        while let Some(index) = current {
            previous = current;
            current = self
                .interface_addresses
                .get(index)
                .expect("interface address chain names an occupied slot")
                .next_this_sw_interface;
        }
        let index = self.interface_addresses.insert(IpInterfaceAddress {
            address: key.address,
            address_length,
            sw_if_index,
            next_this_sw_interface: None,
            prev_this_sw_interface: previous,
        });
        if let Some(previous) = previous {
            self.interface_addresses
                .get_mut(previous)
                .expect("interface address chain tail remains occupied")
                .next_this_sw_interface = Some(index);
        } else {
            self.interface_address_head_by_sw_if_index[position] = Some(index);
        }
        assert!(
            self.interface_address_index_by_key
                .insert(key, index)
                .is_none(),
            "validated interface address key is unique"
        );
        index
    }

    pub(crate) fn remove_interface_address(
        &mut self,
        key: IpInterfaceAddressKey<A>,
        index: u32,
    ) -> IpInterfaceAddress<A> {
        let address = self
            .interface_addresses
            .remove(index)
            .expect("interface address database names an occupied slot");
        if let Some(previous) = address.prev_this_sw_interface {
            self.interface_addresses
                .get_mut(previous)
                .expect("previous interface address remains occupied")
                .next_this_sw_interface = address.next_this_sw_interface;
        } else {
            self.interface_address_head_by_sw_if_index[address.sw_if_index as usize] =
                address.next_this_sw_interface;
        }
        if let Some(next) = address.next_this_sw_interface {
            self.interface_addresses
                .get_mut(next)
                .expect("next interface address remains occupied")
                .prev_this_sw_interface = address.prev_this_sw_interface;
        }
        assert_eq!(
            self.interface_address_index_by_key.remove(&key),
            Some(index)
        );
        address
    }

    pub(crate) fn lock_interface_prefix(&mut self, prefix: P, sw_if_index: u32, source: S) -> bool {
        if let Some(&index) = self
            .interface_prefix_index_by_key
            .get(&(prefix, sw_if_index))
        {
            let record = self
                .interface_prefixes
                .get_mut(index)
                .expect("interface prefix DB names a live record");
            record.reference_count = record
                .reference_count
                .checked_add(1)
                .expect("interface prefix reference count overflow");
            return false;
        }
        let index = self.interface_prefixes.insert(IpInterfacePrefix {
            prefix,
            sw_if_index,
            reference_count: 1,
            source,
        });
        assert!(
            self.interface_prefix_index_by_key
                .insert((prefix, sw_if_index), index)
                .is_none()
        );
        true
    }

    pub(crate) fn unlock_interface_prefix(&mut self, prefix: P, sw_if_index: u32) -> Option<bool> {
        let Some(&index) = self
            .interface_prefix_index_by_key
            .get(&(prefix, sw_if_index))
        else {
            tracing::warn!(
                sw_if_index,
                "interface prefix record was not found for route withdrawal"
            );
            return None;
        };
        let record = self
            .interface_prefixes
            .get_mut(index)
            .expect("interface prefix DB names a live record");
        record.reference_count = record
            .reference_count
            .checked_sub(1)
            .expect("interface prefix reference count underflow");
        if record.reference_count != 0 {
            return Some(false);
        }
        self.interface_prefix_index_by_key
            .remove(&(prefix, sw_if_index));
        self.interface_prefixes
            .remove(index)
            .expect("last prefix reference owns a live record");
        Some(true)
    }
}

pub struct Ip4Main {
    pub(crate) adjacency: UnsafeCell<Option<AdjacencyMain<Ip4FibProtocol>>>,
    pub(crate) glean: UnsafeCell<Option<AdjacencyGleanMain<Ip4FibProtocol>>>,
    pub(crate) neighbor: UnsafeCell<Option<AdjacencyNeighborMain<Ip4FibProtocol>>>,
    pub(crate) local_feature_arc_index: UnsafeCell<u8>,
    pub(crate) punt_feature_arc_index: UnsafeCell<u8>,
    pub(crate) drop_feature_arc_index: UnsafeCell<u8>,
    pub(crate) lookup_main: UnsafeCell<IpLookupMain<Ipv4Addr, Ipv4Net, u32>>,
    pub(crate) fib_index_by_sw_if_index: UnsafeCell<Vec<Option<u32>>>,
    pub(crate) ip_enabled_by_sw_if_index: UnsafeCell<Vec<u8>>,
    pub(crate) add_del_interface_address_callbacks:
        UnsafeCell<Vec<IpInterfaceAddressCallback<Ipv4Addr>>>,
    pub(crate) icmp_throttle: Vec<RefCell<Throttle>>,
    pub(crate) clock_origin: Instant,
    pub(crate) unicast_tables: UnsafeCell<Vec<Ip4FibTable>>,
}

impl Ip4Main {
    pub fn new() -> Self {
        Self {
            adjacency: UnsafeCell::new(None),
            glean: UnsafeCell::new(None),
            neighbor: UnsafeCell::new(None),
            local_feature_arc_index: UnsafeCell::new(u8::MAX),
            punt_feature_arc_index: UnsafeCell::new(u8::MAX),
            drop_feature_arc_index: UnsafeCell::new(u8::MAX),
            lookup_main: UnsafeCell::new(IpLookupMain::new(NodeNext::slot(
                crate::local::Ip4LocalNext::Punt,
            ))),
            fib_index_by_sw_if_index: UnsafeCell::new(Vec::new()),
            ip_enabled_by_sw_if_index: UnsafeCell::new(Vec::new()),
            add_del_interface_address_callbacks: UnsafeCell::new(Vec::new()),
            icmp_throttle: Vec::new(),
            clock_origin: Instant::now(),
            unicast_tables: UnsafeCell::new(vec![Ip4FibTable::new(Default::default())]),
        }
    }

    #[inline(always)]
    pub fn fib_index(&self, sw_if_index: u32) -> Option<u32> {
        // SAFETY: main-thread mutation is published under the worker barrier.
        unsafe { &*self.fib_index_by_sw_if_index.get() }
            .get(sw_if_index as usize)
            .copied()
            .flatten()
    }

    #[inline(always)]
    pub fn forwarding_dpo(&self, fib_index: u32, address: Ipv4Addr) -> Option<DpoId> {
        // SAFETY: route publication occurs under the worker barrier; packet
        // lookup only reads the published table outside that scope.
        unsafe { &*self.unicast_tables.get() }
            .get(fib_index as usize)
            .and_then(|table| table.forwarding_lookup(address))
    }

    pub(crate) fn fib_table_mut(&self, fib_index: u32) -> &mut Ip4FibTable {
        // SAFETY: callers hold the main-thread publication scope.
        unsafe { &mut *self.unicast_tables.get() }
            .get_mut(fib_index as usize)
            .expect("IP4 FIB mapping names an installed table")
    }
}

pub struct Ip6Main {
    pub(crate) adjacency: UnsafeCell<Option<AdjacencyMain<Ip6FibProtocol>>>,
    pub(crate) glean: UnsafeCell<Option<AdjacencyGleanMain<Ip6FibProtocol>>>,
    pub(crate) neighbor: UnsafeCell<Option<AdjacencyNeighborMain<Ip6FibProtocol>>>,
    pub(crate) local_feature_arc_index: UnsafeCell<u8>,
    pub(crate) punt_feature_arc_index: UnsafeCell<u8>,
    pub(crate) drop_feature_arc_index: UnsafeCell<u8>,
    pub(crate) lookup_main: UnsafeCell<IpLookupMain<Ipv6Addr, Ipv6Net, ()>>,
    pub(crate) fib_index_by_sw_if_index: UnsafeCell<Vec<Option<u32>>>,
    pub(crate) ip_enabled_by_sw_if_index: UnsafeCell<Vec<u8>>,
    pub(crate) add_del_interface_address_callbacks:
        UnsafeCell<Vec<IpInterfaceAddressCallback<Ipv6Addr>>>,
    pub(crate) icmp_throttle: Vec<RefCell<Throttle>>,
    pub(crate) clock_origin: Instant,
    pub(crate) unicast_tables: UnsafeCell<Vec<Ip6FibTable>>,
}

impl Ip6Main {
    pub fn new() -> Self {
        Self {
            adjacency: UnsafeCell::new(None),
            glean: UnsafeCell::new(None),
            neighbor: UnsafeCell::new(None),
            local_feature_arc_index: UnsafeCell::new(u8::MAX),
            punt_feature_arc_index: UnsafeCell::new(u8::MAX),
            drop_feature_arc_index: UnsafeCell::new(u8::MAX),
            lookup_main: UnsafeCell::new(IpLookupMain::new(NodeNext::slot(
                crate::local::Ip6LocalNext::Punt,
            ))),
            fib_index_by_sw_if_index: UnsafeCell::new(Vec::new()),
            ip_enabled_by_sw_if_index: UnsafeCell::new(Vec::new()),
            add_del_interface_address_callbacks: UnsafeCell::new(Vec::new()),
            icmp_throttle: Vec::new(),
            clock_origin: Instant::now(),
            unicast_tables: UnsafeCell::new(vec![Ip6FibTable::new(Default::default())]),
        }
    }

    #[inline(always)]
    pub fn fib_index(&self, sw_if_index: u32) -> Option<u32> {
        // SAFETY: main-thread mutation is published under the worker barrier.
        unsafe { &*self.fib_index_by_sw_if_index.get() }
            .get(sw_if_index as usize)
            .copied()
            .flatten()
    }

    #[inline(always)]
    pub fn forwarding_dpo(&self, fib_index: u32, address: Ipv6Addr) -> Option<DpoId> {
        // SAFETY: route publication occurs under the worker barrier; packet
        // lookup only reads the published table outside that scope.
        unsafe { &*self.unicast_tables.get() }
            .get(fib_index as usize)
            .and_then(|table| table.forwarding_lookup(address))
    }

    pub(crate) fn fib_table_mut(&self, fib_index: u32) -> &mut Ip6FibTable {
        // SAFETY: callers hold the main-thread publication scope.
        unsafe { &mut *self.unicast_tables.get() }
            .get_mut(fib_index as usize)
            .expect("IP6 FIB mapping names an installed table")
    }
}

pub static IP4_MAIN: OnceLock<Ip4Main> = OnceLock::new();
pub static IP6_MAIN: OnceLock<Ip6Main> = OnceLock::new();

// SAFETY: local protocol slots are changed only by the main thread before
// workers start or while the worker barrier is held. Workers borrow slots
// only synchronously during a node invocation; all other state is immutable
// or explicitly worker-owned.
unsafe impl Sync for Ip4Main {}
unsafe impl Sync for Ip6Main {}

#[inline(always)]
pub fn fib_table_get_index_for_sw_if_index(version: IpVersion, sw_if_index: u32) -> Option<u32> {
    match version {
        IpVersion::V4 => IP4_MAIN
            .get()
            .expect("IP4 Main is initialized before FIB selection")
            .fib_index(sw_if_index),
        IpVersion::V6 => IP6_MAIN
            .get()
            .expect("IP6 Main is initialized before FIB selection")
            .fib_index(sw_if_index),
    }
}

#[hammer_component_macros::init_function(
    name = "ip_lookup_init",
    runs_after = ["interface_main_init"],
    runs_before = ["net_main_init"]
)]
fn init_lookup() -> RuntimeResult<()> {
    assert!(
        IP4_MAIN.get().is_none() && IP6_MAIN.get().is_none(),
        "IP lookup initialization callback executes once"
    );
    let mut ip4 = Ip4Main::new();
    let mut ip6 = Ip6Main::new();
    ip4.icmp_throttle = (0..=hammer_runtime::config::worker::worker_count())
        .map(|_| RefCell::new(Throttle::new(std::time::Duration::from_micros(10))))
        .collect();
    ip6.icmp_throttle = (0..=hammer_runtime::config::worker::worker_count())
        .map(|_| RefCell::new(Throttle::new(std::time::Duration::from_millis(1))))
        .collect();
    assert!(
        IP4_MAIN.set(ip4).is_ok(),
        "IP4 Main remains uninitialized after lifecycle preflight"
    );
    assert!(
        IP6_MAIN.set(ip6).is_ok(),
        "IP6 Main remains uninitialized after lifecycle preflight"
    );
    Ok(())
}

#[hammer_component_macros::node_next]
enum Ip4LookupNext {
    #[next("drop")]
    Drop,
    #[next("ip4-load-balance")]
    LoadBalance,
}

#[hammer_component_macros::node_next]
enum Ip6LookupNext {
    #[next("drop")]
    Drop,
    #[next("ip6-load-balance")]
    LoadBalance,
}

#[hammer_component_macros::node_next]
enum Ip4InterfaceRxNext {
    #[next("drop")]
    Drop,
    #[next("ip4-input")]
    Input,
}

#[hammer_component_macros::node_next]
enum Ip6InterfaceRxNext {
    #[next("drop")]
    Drop,
    #[next("ip6-input")]
    Input,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_interface_rx,
    role = internal,
    name = "interface-rx-dpo-ip4",
    next = Ip4InterfaceRxNext,
)]
pub struct Ip4InterfaceRxNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_interface_rx,
    role = internal,
    name = "interface-rx-dpo-ip6",
    next = Ip6InterfaceRxNext,
)]
pub struct Ip6InterfaceRxNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

fn register_ip4_interface_rx(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Ip4InterfaceRxNode::new(),
        &Ip4InterfaceRxNext::NEXT_NAMES,
    )?;
    register_interface_rx_class(DpoProto::IP4, node).map_err(|source| {
        hammer_runtime::RuntimeError::GraphNodeInitialization {
            node: "interface-rx-dpo-ip4",
            source: Box::new(source),
        }
    })?;
    Ok(node)
}

fn register_ip6_interface_rx(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime.nodes().try_register_internal_with_next_names(
        Ip6InterfaceRxNode::new(),
        &Ip6InterfaceRxNext::NEXT_NAMES,
    )?;
    register_interface_rx_class(DpoProto::IP6, node).map_err(|source| {
        hammer_runtime::RuntimeError::GraphNodeInitialization {
            node: "interface-rx-dpo-ip6",
            source: Box::new(source),
        }
    })?;
    Ok(node)
}

fn register_interface_rx_class(
    proto: DpoProto,
    node: NodeId,
) -> Result<DpoType, hammer_service::net::DpoError> {
    use hammer_service::net::InterfaceRxDpo;
    let net = NetMain::global()?;
    let install_operations = [DpoProto::IP4, DpoProto::IP6]
        .into_iter()
        .all(|proto| net.dpo_main().nodes(DpoType::INTERFACE_RX, proto).is_none());
    net.register_dpo(
        Some(DpoType::INTERFACE_RX),
        &[(proto, &[node])],
        install_operations.then_some((InterfaceRxDpo::lock, InterfaceRxDpo::unlock)),
        None,
        None,
        None,
        None,
        install_operations.then_some(InterfaceRxDpo::format),
        install_operations.then_some(InterfaceRxDpo::memory),
    )
}

impl Node for Ip4InterfaceRxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, node_runtime, frame| {
            let processed_vectors = frame.len();
            process_interface_rx(runtime, node_runtime, frame, DpoProto::IP4);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6InterfaceRxNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = |runtime, node_runtime, frame| {
            let processed_vectors = frame.len();
            process_interface_rx(runtime, node_runtime, frame, DpoProto::IP6);
            processed_vectors
        };
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

fn process_interface_rx(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    proto: DpoProto,
) {
    let net = NetMain::global().expect("interface RX graph requires its installed owner");
    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
        let buffer = runtime.buffer_mut(index);
        // SAFETY: DPO lookup/stack execution initialized this IP-owned overlay;
        // the mutable buffer borrow excludes concurrent metadata access.
        let forwarding = hammer_core::buffer_opaque!(buffer => IpSecondaryOpaque)
            .lookup
            .forwarding;
        assert_eq!(
            forwarding.proto(),
            proto,
            "RX DPO reached the wrong protocol node"
        );
        let sw_if_index = net
            .rx_dpo_interface(forwarding)
            .expect("published interface RX DPO remains retained during packet processing");
        // SAFETY: NetworkOpaque is the asserted packet ABI overlay, and the
        // packet is exclusively owned by this node while changing its RX fact.
        let opaque = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
        opaque.sw_if_index[0] = sw_if_index;
        if proto == DpoProto::IP4 {
            NodeNext::slot(Ip4InterfaceRxNext::Input)
        } else {
            NodeNext::slot(Ip6InterfaceRxNext::Input)
        }
    });
}

#[derive(Clone, Copy)]
#[repr(C)]
#[derive(zerocopy::FromBytes, zerocopy::IntoBytes, zerocopy::Immutable)]
pub(crate) struct LookupMetadata {
    fib_index: u32,
    padding: [u8; 4],
    pub(crate) forwarding: DpoId,
    flow_hash: u32,
    lookup_count: u8,
    reserved: [u8; 3],
}

// Lookup occupies the initial bytes; the ICMP request is the final u64,
// matching the existing IP producer/consumer contract over opaque2[14].
#[hammer_component_macros::buffer_opaque(secondary)]
#[derive(Clone, Copy)]
pub struct IpSecondaryOpaque {
    pub(crate) lookup: LookupMetadata,
    reserved: [u8; 48 - core::mem::size_of::<LookupMetadata>()],
    pub(crate) icmp_error: u64,
}

const _: () = assert!(core::mem::size_of::<IpSecondaryOpaque>() == 56);
const _: () = assert!(core::mem::offset_of!(IpSecondaryOpaque, lookup) == 0);
const _: () = assert!(core::mem::offset_of!(IpSecondaryOpaque, icmp_error) == 48);

impl Default for LookupMetadata {
    fn default() -> Self {
        Self {
            fib_index: u32::MAX,
            padding: [0; 4],
            reserved: [0; 3],
            forwarding: DpoId::INVALID,
            flow_hash: 0,
            lookup_count: 0,
        }
    }
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_lookup,
    role = internal,
    name = "ip4-lookup",
    next = Ip4LookupNext,
)]
#[hammer_component_macros::feature(arc = crate::ip::input::Ip4InputNode)]
pub struct Ip4LookupNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_lookup,
    role = internal,
    name = "ip6-lookup",
    next = Ip6LookupNext,
)]
#[hammer_component_macros::feature(arc = crate::ip::input::Ip6InputNode)]
pub struct Ip6LookupNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip4_load_balance,
    role = internal,
    name = "ip4-load-balance",
    sibling_of = Ip4LookupNode,
)]
pub struct Ip4LoadBalanceNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip6_load_balance,
    role = internal,
    name = "ip6-load-balance",
    sibling_of = Ip6LookupNode,
)]
pub struct Ip6LoadBalanceNode {
    #[node(default = NodeRuntime::empty())]
    runtime_data: NodeRuntime,
}

fn register_ip4_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip4LookupNode::new(), &Ip4LookupNext::NEXT_NAMES)
}

fn register_ip6_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Ip6LookupNode::new(), &Ip6LookupNext::NEXT_NAMES)
}

fn register_ip4_load_balance(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip4LoadBalanceNode::new())?;
    let net = NetMain::global()?;
    let install_operations = net
        .dpo_main()
        .nodes(DpoType::LOAD_BALANCE, DpoProto::IP6)
        .is_none();
    net.register_dpo(
        Some(DpoType::LOAD_BALANCE),
        &[(DpoProto::IP4, &[node][..])],
        install_operations.then_some((
            hammer_service::net::LoadBalanceDpo::lock,
            hammer_service::net::LoadBalanceDpo::unlock,
        )),
        None,
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::mtu),
        None,
        None,
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::format),
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::memory),
    )
    .map_err(
        |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
            node: "ip4-load-balance",
            source: Box::new(source),
        },
    )?;
    Ok(node)
}

fn register_ip6_load_balance(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = runtime
        .nodes()
        .try_register_internal(Ip6LoadBalanceNode::new())?;
    let net = NetMain::global()?;
    let install_operations = net
        .dpo_main()
        .nodes(DpoType::LOAD_BALANCE, DpoProto::IP4)
        .is_none();
    net.register_dpo(
        Some(DpoType::LOAD_BALANCE),
        &[(DpoProto::IP6, &[node][..])],
        install_operations.then_some((
            hammer_service::net::LoadBalanceDpo::lock,
            hammer_service::net::LoadBalanceDpo::unlock,
        )),
        None,
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::mtu),
        None,
        None,
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::format),
        install_operations.then_some(hammer_service::net::LoadBalanceDpo::memory),
    )
    .map_err(
        |source| hammer_runtime::RuntimeError::GraphNodeInitialization {
            node: "ip6-load-balance",
            source: Box::new(source),
        },
    )?;
    Ok(node)
}

impl Node for Ip4LookupNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = process_lookup_v4;
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6LookupNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = process_lookup_v6;
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip4LoadBalanceNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = process_load_balance_v4;
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

impl Node for Ip6LoadBalanceNode {
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = process_load_balance_v6;
        process(runtime, node_runtime, frame)
    }

    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(self.runtime_data)
    }
}

fn process_lookup_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    version: IpVersion,
) {
    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
        lookup_index(runtime, index, version)
    })
}

const IP_FLOW_HASH_SRC_ADDR: u16 = 1 << 0;
const IP_FLOW_HASH_DST_ADDR: u16 = 1 << 1;
const IP_FLOW_HASH_SRC_PORT: u16 = 1 << 2;
const IP_FLOW_HASH_DST_PORT: u16 = 1 << 3;
const IP_FLOW_HASH_PROTO: u16 = 1 << 4;
const IP_FLOW_HASH_REVERSE_SRC_DST: u16 = 1 << 5;
const IP_FLOW_HASH_SYMMETRIC: u16 = 1 << 6;
const IP_FLOW_HASH_FLOW_LABEL: u16 = 1 << 7;
const IP_FLOW_HASH_GTPV1_TEID: u16 = 1 << 8;
const GTPV1_PORT_BE: u16 = 2152u16.to_be();

#[inline(always)]
fn hash_v3_mix32(mut a: u32, mut b: u32, mut c: u32) -> u32 {
    a = a.wrapping_sub(c) ^ c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c) ^ c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a) ^ a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b) ^ b.rotate_left(4);
    b = b.wrapping_add(a);

    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    c
}

#[inline(always)]
fn hash_mix64(mut a: u64, mut b: u64, mut c: u64) -> u64 {
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 43);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 9);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 8);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 38);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 23);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 5);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 35);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 49);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 11);
    a = a.wrapping_sub(b).wrapping_sub(c) ^ (c >> 12);
    b = b.wrapping_sub(c).wrapping_sub(a) ^ (a << 18);
    c = c.wrapping_sub(a).wrapping_sub(b) ^ (b >> 22);
    c
}

#[inline(always)]
fn transport_ports(packet: &[u8], offset: usize, protocol: IpProtocol) -> (u16, u16) {
    if !matches!(protocol, IpProtocol::Tcp | IpProtocol::Udp) {
        return (0, 0);
    }
    let Some(bytes) = packet.get(offset..offset.saturating_add(4)) else {
        return (0, 0);
    };
    (
        u16::from_ne_bytes([bytes[0], bytes[1]]),
        u16::from_ne_bytes([bytes[2], bytes[3]]),
    )
}

#[inline(always)]
fn ip4_flow_hash(
    packet: &[u8],
    header: &Ipv4Header,
    cursor: BufferPacketCursor,
    config: u16,
) -> u32 {
    let source = u32::from_ne_bytes(header.source().octets());
    let destination = u32::from_ne_bytes(header.destination().octets());
    let protocol = IpProtocol::from(header.protocol());
    let (source_port, destination_port) =
        transport_ports(packet, cursor.transport_header_offset(), protocol);
    let transport_destination_port = destination_port;
    let gtp_teid =
        if config & IP_FLOW_HASH_GTPV1_TEID != 0 && transport_destination_port == GTPV1_PORT_BE {
            packet
                .get(
                    cursor.transport_header_offset().saturating_add(8)
                        ..cursor.transport_header_offset().saturating_add(12),
                )
                .map(|bytes| u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                .unwrap_or(0)
        } else {
            0
        };
    let source = if config & IP_FLOW_HASH_SRC_ADDR != 0 {
        source
    } else {
        0
    };
    let destination = if config & IP_FLOW_HASH_DST_ADDR != 0 {
        destination
    } else {
        0
    };
    let mut a = source;
    let mut b = destination;
    let mut source_port = if config & IP_FLOW_HASH_SRC_PORT != 0 {
        source_port
    } else {
        0
    };
    let mut destination_port = if config & IP_FLOW_HASH_DST_PORT != 0 {
        destination_port
    } else {
        0
    };
    if config & IP_FLOW_HASH_REVERSE_SRC_DST != 0 {
        (a, b) = (b, a);
        (source_port, destination_port) = (destination_port, source_port);
    }
    if config & IP_FLOW_HASH_SYMMETRIC != 0 {
        if b < a {
            (a, b) = (b, a);
        }
        if destination_port < source_port {
            (source_port, destination_port) = (destination_port, source_port);
        }
    }
    if config & IP_FLOW_HASH_PROTO != 0 {
        b ^= u32::from(u8::from(protocol));
    }
    let c = (u32::from(destination_port) << 16) | u32::from(source_port);
    a ^= gtp_teid;
    hash_v3_mix32(a, b, c)
}

#[inline(always)]
fn ip6_flow_hash(
    packet: &[u8],
    header: &Ipv6Header,
    cursor: BufferPacketCursor,
    protocol: IpProtocol,
    config: u16,
) -> u32 {
    let source = header.source().octets();
    let destination = header.destination().octets();
    let (source_port, destination_port) =
        transport_ports(packet, cursor.transport_header_offset(), protocol);
    let transport_destination_port = destination_port;
    let source = if config & IP_FLOW_HASH_SRC_ADDR != 0 {
        u64::from_ne_bytes(source[..8].try_into().unwrap())
            ^ u64::from_ne_bytes(source[8..].try_into().unwrap())
    } else {
        0
    };
    let destination = if config & IP_FLOW_HASH_DST_ADDR != 0 {
        u64::from_ne_bytes(destination[..8].try_into().unwrap())
            ^ u64::from_ne_bytes(destination[8..].try_into().unwrap())
    } else {
        0
    };
    let mut a = source;
    let mut b = destination;
    let mut source_port = if config & IP_FLOW_HASH_SRC_PORT != 0 {
        source_port
    } else {
        0
    };
    let mut destination_port = if config & IP_FLOW_HASH_DST_PORT != 0 {
        destination_port
    } else {
        0
    };
    if config & IP_FLOW_HASH_REVERSE_SRC_DST != 0 {
        (a, b) = (b, a);
        (source_port, destination_port) = (destination_port, source_port);
    }
    if config & IP_FLOW_HASH_SYMMETRIC != 0 {
        if b < a {
            (a, b) = (b, a);
        }
        if destination_port < source_port {
            (source_port, destination_port) = (destination_port, source_port);
        }
    }
    if config & IP_FLOW_HASH_PROTO != 0 {
        b ^= u64::from(u8::from(protocol));
    }
    let mut c = (u64::from(destination_port) << 16) | u64::from(source_port);
    if config & IP_FLOW_HASH_FLOW_LABEL != 0 {
        c ^= u64::from(header.flow_label());
    }
    if config & IP_FLOW_HASH_GTPV1_TEID != 0
        && transport_destination_port == GTPV1_PORT_BE
        && let Some(bytes) = packet.get(
            cursor.transport_header_offset().saturating_add(8)
                ..cursor.transport_header_offset().saturating_add(12),
        )
    {
        a ^= u64::from(u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
    }
    c = hash_mix64(a, b, c);
    c as u32
}

fn process_lookup_v4(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    process_lookup_frame(runtime, node_runtime, frame, IpVersion::V4);
    processed_vectors
}

fn process_load_balance_v4(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    process_load_balance_frame(runtime, node_runtime, frame, IpVersion::V4);
    processed_vectors
}

fn process_load_balance_v6(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    process_load_balance_frame(runtime, node_runtime, frame, IpVersion::V6);
    processed_vectors
}

fn process_load_balance_frame(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    version: IpVersion,
) {
    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
        load_balance_index(runtime, index, version)
    })
}

#[inline(always)]
fn load_balance_index(runtime: &mut DataPlaneMain, index: u32, version: IpVersion) -> u16 {
    let drop_next = match version {
        IpVersion::V4 => NodeNext::slot(Ip4LookupNext::Drop),
        IpVersion::V6 => NodeNext::slot(Ip6LookupNext::Drop),
    };
    let buffer = runtime.buffer_mut(index);
    let (cursor, protocol) = {
        let opaque = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
        (
            opaque.packet_cursor(),
            opaque.ip().ip_protocol().map(IpProtocol::from),
        )
    };
    let metadata = &mut hammer_core::buffer_opaque!(mut buffer => IpSecondaryOpaque).lookup;
    const MAX_LOOKUPS_PER_PACKET: u8 = 4;
    if metadata.lookup_count >= MAX_LOOKUPS_PER_PACKET {
        return drop_next;
    }
    metadata.lookup_count += 1;
    let current = metadata.forwarding;
    let flow_hash = metadata.flow_hash;
    let expected_proto = match version {
        IpVersion::V4 => DpoProto::IP4,
        IpVersion::V6 => DpoProto::IP6,
    };
    if current.class() != DpoType::LOAD_BALANCE || current.proto() != expected_proto {
        return drop_next;
    }
    let Ok(net) = hammer_service::net::NetMain::global() else {
        return drop_next;
    };
    let mut next_flow_hash = None;
    let selected = net.select_load_balance(current, |bucket_count, flow_hash_config| {
        let hash = if bucket_count <= 1 {
            0
        } else if flow_hash != 0 {
            flow_hash >> 1
        } else {
            match version {
                IpVersion::V4 => {
                    let bytes = buffer.current().get(cursor.network_header_offset()..)?;
                    let (header, _) = Ipv4Header::ref_from_prefix(bytes).ok()?;
                    ip4_flow_hash(buffer.current(), header, cursor, flow_hash_config)
                }
                IpVersion::V6 => {
                    let bytes = buffer.current().get(cursor.network_header_offset()..)?;
                    let (header, _) = Ipv6Header::ref_from_prefix(bytes).ok()?;
                    let protocol = protocol?;
                    ip6_flow_hash(buffer.current(), header, cursor, protocol, flow_hash_config)
                }
            }
        };
        if bucket_count > 1 {
            next_flow_hash = Some(hash);
        }
        Some(hash)
    });
    let metadata = &mut hammer_core::buffer_opaque!(mut buffer => IpSecondaryOpaque).lookup;
    if let Some(hash) = next_flow_hash {
        metadata.flow_hash = hash;
    }
    let Some(selected) = selected else {
        return drop_next;
    };
    metadata.forwarding = selected;
    if selected.class() == DpoType::LOAD_BALANCE {
        match version {
            IpVersion::V4 => NodeNext::slot(Ip4LookupNext::LoadBalance),
            IpVersion::V6 => NodeNext::slot(Ip6LookupNext::LoadBalance),
        }
    } else {
        selected.next()
    }
}

fn process_lookup_v6(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    process_lookup_frame(runtime, node_runtime, frame, IpVersion::V6);
    processed_vectors
}

#[inline(always)]
fn lookup_index(runtime: &mut DataPlaneMain, index: u32, version: IpVersion) -> u16 {
    let drop_next = match version {
        IpVersion::V4 => NodeNext::slot(Ip4LookupNext::Drop),
        IpVersion::V6 => NodeNext::slot(Ip6LookupNext::Drop),
    };
    let buffer = runtime.buffer_mut(index);
    let opaque = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
    let fib_index = opaque
        .ip()
        .fib_index_override()
        .or_else(|| opaque.ip().fib_index())
        .or_else(|| match version {
            IpVersion::V4 => IP4_MAIN
                .get()
                .and_then(|main| main.fib_index(opaque.sw_if_index[0])),
            IpVersion::V6 => IP6_MAIN
                .get()
                .and_then(|main| main.fib_index(opaque.sw_if_index[0])),
        });
    let Some(fib_index) = fib_index else {
        return drop_next;
    };
    let cursor = opaque.packet_cursor();
    let forwarding = match version {
        IpVersion::V4 => buffer
            .current()
            .get(cursor.network_header_offset()..)
            .and_then(|bytes| Ipv4Header::ref_from_prefix(bytes).ok())
            .and_then(|(header, _)| {
                IP4_MAIN
                    .get()
                    .and_then(|main| main.forwarding_dpo(fib_index, header.destination()))
            }),
        IpVersion::V6 => buffer
            .current()
            .get(cursor.network_header_offset()..)
            .and_then(|bytes| Ipv6Header::ref_from_prefix(bytes).ok())
            .and_then(|(header, _)| {
                IP6_MAIN
                    .get()
                    .and_then(|main| main.forwarding_dpo(fib_index, header.destination()))
            }),
    };
    let metadata = &mut hammer_core::buffer_opaque!(mut buffer => IpSecondaryOpaque).lookup;
    *metadata = LookupMetadata {
        fib_index,
        padding: [0; 4],
        reserved: [0; 3],
        forwarding: forwarding.unwrap_or(DpoId::INVALID),
        flow_hash: 0,
        lookup_count: 0,
    };
    match forwarding {
        Some(dpo) if dpo.class() == DpoType::LOAD_BALANCE => {
            load_balance_index(runtime, index, version)
        }
        Some(dpo) => dpo.next(),
        None => drop_next,
    }
}
