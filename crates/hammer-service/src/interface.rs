use std::cell::UnsafeCell;
use std::fmt;
use std::mem::transmute;
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwapOption;
use hammer_core::config::Config;
use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_core::forwarding::{AdjacencyRewrite, DpoId, DpoProto, FibTableBuilder};
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::map::{FlatHashKey, FlatHashTable};
use hammer_infra::vec::Vec;
use hammer_runtime::DataPlaneBarrierHandle;
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace,
};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::data_plane::set_index_node_error_code;
use crate::net::fib::FibTableHandle;
use crate::opaque::NetworkOpaque;
use crate::trace::codec::{TraceDecodeCursor, put_option_u16, put_option_u32};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterfaceMtu {
    values: [u32; InterfaceMtuKind::COUNT],
}

impl InterfaceMtu {
    #[inline]
    pub const fn new(l3: u32, ip4: u32, ip6: u32, mpls: u32) -> Self {
        Self {
            values: [l3, ip4, ip6, mpls],
        }
    }

    #[inline]
    pub fn get(&self, kind: InterfaceMtuKind) -> u32 {
        self.values[kind.slot()]
    }

    #[inline]
    pub fn set(&mut self, kind: InterfaceMtuKind, value: u32) {
        self.values[kind.slot()] = value;
    }

    #[inline]
    pub fn l3(&self) -> u32 {
        self.get(InterfaceMtuKind::L3)
    }

    #[inline]
    pub fn ip4(&self) -> u32 {
        self.get(InterfaceMtuKind::Ip4)
    }

    #[inline]
    pub fn ip6(&self) -> u32 {
        self.get(InterfaceMtuKind::Ip6)
    }

    #[inline]
    pub fn mpls(&self) -> u32 {
        self.get(InterfaceMtuKind::Mpls)
    }
}

impl Default for InterfaceMtu {
    #[inline]
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceMtuKind {
    L3,
    Ip4,
    Ip6,
    Mpls,
}

impl InterfaceMtuKind {
    const COUNT: usize = 4;

    #[inline]
    const fn slot(self) -> usize {
        match self {
            Self::L3 => 0,
            Self::Ip4 => 1,
            Self::Ip6 => 2,
            Self::Mpls => 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InterfaceControlHandle {
    inner: Arc<InterfaceStateSlot>,
}

impl InterfaceControlHandle {
    #[inline]
    fn new(inner: Arc<InterfaceStateSlot>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn interface_index(&self, name: &str) -> Option<u32> {
        self.inner.state().interface_index(name)
    }

    #[inline]
    pub fn interface_name(&self, index: u32) -> Option<String> {
        self.inner
            .state()
            .interface_name(index)
            .map(ToOwned::to_owned)
    }

    #[inline]
    pub fn interface_addresses(&self, index: u32) -> Vec<IpNet> {
        self.inner.state().interface_addresses(index)
    }

    #[inline]
    pub fn interface_mtu(&self, index: u32) -> Option<InterfaceMtu> {
        self.inner.state().interface_mtu(index)
    }

    #[inline]
    pub fn interface_address_index(&self, interface_index: u32, address: IpNet) -> Option<u32> {
        self.inner
            .state()
            .interface_address_index(interface_index, address)
    }
}

pub struct InterfaceControlPlane {
    inner: Arc<InterfaceStateSlot>,
    barrier: Option<DataPlaneBarrierHandle>,
    connected_routes: Option<InterfaceConnectedRouteControl>,
}

impl Default for InterfaceControlPlane {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl InterfaceControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InterfaceStateSlot::new(InterfaceState::default())),
            barrier: None,
            connected_routes: None,
        }
    }

    #[inline]
    pub fn with_data_plane_barrier(mut self, barrier: DataPlaneBarrierHandle) -> Self {
        self.barrier = Some(barrier);
        self
    }

    #[inline]
    pub fn with_connected_routes(mut self, routes: InterfaceConnectedRouteControl) -> Self {
        self.connected_routes = Some(routes);
        self
    }

    #[inline]
    pub fn handle(&self) -> InterfaceControlHandle {
        InterfaceControlHandle::new(Arc::clone(&self.inner))
    }

    pub fn register_interface(&self, name: impl Into<String>) -> CoreResult<u32> {
        self.register_interface_with_mtu(name, InterfaceMtu::default())
    }

    pub fn register_interface_with_mtu(
        &self,
        name: impl Into<String>,
        mtu: InterfaceMtu,
    ) -> CoreResult<u32> {
        let name = name.into();
        if name.is_empty() {
            return Err(CoreError::internal("interface name is empty"));
        }
        let mut index = None;
        self.synchronize(|| {
            let current = self.inner.state();
            if let Some(current_index) = current.interface_index(&name) {
                index = Some(current_index);
                return Ok(());
            }
            let mut next = InterfaceState::clone(&current);
            let next_index = next.interfaces.len() as u32;
            next.interfaces.push(InterfaceRecord {
                name: name.clone(),
                addresses: Vec::new(),
                mtu,
            });
            index = Some(next_index);
            self.publish(next)?;
            Ok(())
        })?;
        index.ok_or_else(|| CoreError::internal("interface registration did not publish an index"))
    }

    pub fn set_mtu(&self, interface_index: u32, mtu: InterfaceMtu) -> CoreResult<()> {
        self.ensure_interface(interface_index)?;
        self.synchronize(|| {
            let current = self.inner.state();
            let mut next = InterfaceState::clone(current);
            let interface = next.interface_mut(interface_index).ok_or_else(|| {
                CoreError::internal(format!("interface {interface_index} is not registered"))
            })?;
            interface.mtu = mtu;
            self.publish(next)?;
            Ok(())
        })
    }

    pub fn set_protocol_mtu(
        &self,
        interface_index: u32,
        kind: InterfaceMtuKind,
        value: u32,
    ) -> CoreResult<()> {
        self.ensure_interface(interface_index)?;
        self.synchronize(|| {
            let current = self.inner.state();
            let mut next = InterfaceState::clone(current);
            let interface = next.interface_mut(interface_index).ok_or_else(|| {
                CoreError::internal(format!("interface {interface_index} is not registered"))
            })?;
            interface.mtu.set(kind, value);
            self.publish(next)?;
            Ok(())
        })
    }

    pub fn add_address(&self, interface_index: u32, address: IpNet) -> CoreResult<u32> {
        self.ensure_interface(interface_index)?;
        let mut address_index = None;
        self.synchronize(|| {
            let current = self.inner.state();
            if let Some(current_index) = current.interface_address_index(interface_index, address) {
                address_index = Some(current_index);
                return Ok(());
            }
            let mut next = InterfaceState::clone(&current);
            let index = next.addresses.len() as u32;
            next.address_to_index
                .insert(InterfaceAddressKey::new(interface_index, address), index);
            next.addresses.push(InterfaceAddressRecord {
                interface_index,
                address,
                removed: false,
            });
            next.interfaces[interface_index as usize]
                .addresses
                .push(index);
            address_index = Some(index);
            self.publish(next)?;
            Ok(())
        })?;
        address_index
            .ok_or_else(|| CoreError::internal("interface address did not publish an index"))
    }

    pub fn remove_address(&self, interface_index: u32, address: IpNet) -> CoreResult<bool> {
        self.ensure_interface(interface_index)?;
        let mut removed = false;
        self.synchronize(|| {
            let current = self.inner.state();
            let Some(address_index) = current.interface_address_index(interface_index, address)
            else {
                return Ok(());
            };
            let mut next = InterfaceState::clone(&current);
            if let Some(address) = next.addresses.get_mut(address_index as usize) {
                address.removed = true;
            }
            if let Some(interface) = next.interfaces.get_mut(interface_index as usize) {
                let addresses = interface
                    .addresses
                    .iter()
                    .copied()
                    .filter(|index| *index != address_index)
                    .collect::<Vec<_>>();
                interface.addresses = addresses;
            }
            next.rebuild_address_index();
            removed = true;
            self.publish(next)?;
            Ok(())
        })?;
        Ok(removed)
    }

    #[inline]
    fn ensure_interface(&self, interface_index: u32) -> CoreResult<()> {
        if self.inner.state().interface(interface_index).is_some() {
            Ok(())
        } else {
            Err(CoreError::internal(format!(
                "interface {interface_index} is not registered"
            )))
        }
    }

    #[inline]
    fn synchronize<R>(&self, operation: impl FnOnce() -> CoreResult<R>) -> CoreResult<R> {
        if let Some(barrier) = &self.barrier {
            barrier.synchronize(operation)
        } else {
            operation()
        }
    }

    #[inline]
    fn publish(&self, state: InterfaceState) -> CoreResult<()> {
        if let Some(routes) = &self.connected_routes {
            routes.publish_state(&state)?;
        }
        self.inner.replace_after_barrier(state);
        Ok(())
    }
}

/// Process-level interface control plane (VPP-style main). Filled by
/// `interface_init` from `[[network.interface]]` only — no device open.
pub static INTERFACE_MAIN: ArcSwapOption<InterfaceControlPlane> = ArcSwapOption::const_empty();

pub fn reset_interface_main_for_test() {
    INTERFACE_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let plane = init_interface(reg.require::<Config>()?)?;
    reg.set(plane);
    Ok(())
}

#[hammer_component_macros::init_function(name = "interface_init")]
fn init_interface(config: Arc<Config>) -> HammerResult<Arc<InterfaceControlPlane>> {
    let plane = InterfaceControlPlane::new();
    for iface in &config.network.interface {
        let mtu = InterfaceMtu::new(iface.mtu.l3, iface.mtu.ip4, iface.mtu.ip6, iface.mtu.mpls);
        let index = plane.register_interface_with_mtu(iface.name.clone(), mtu)?;
        for address in &iface.address {
            plane.add_address(index, *address)?;
        }
    }
    let plane = Arc::new(plane);
    INTERFACE_MAIN.store(Some(Arc::clone(&plane)));
    Ok(plane)
}

#[derive(Debug, Clone)]
pub struct InterfaceConnectedRouteControl {
    table: FibTableHandle,
    drop_next: u16,
    receive_next: u16,
    connected_nexts: Option<InterfaceConnectedNexts>,
}

impl InterfaceConnectedRouteControl {
    #[inline]
    pub fn new(table: FibTableHandle, drop_next: u16, receive_next: u16) -> Self {
        Self {
            table,
            drop_next,
            receive_next,
            connected_nexts: None,
        }
    }

    #[inline]
    pub fn with_connected_adjacency(
        mut self,
        adjacency_rewrite_next: u16,
        interface_output_next: u16,
    ) -> Self {
        self.connected_nexts = Some(InterfaceConnectedNexts {
            adjacency_rewrite_next,
            interface_output_next,
        });
        self
    }

    fn publish_state(&self, state: &InterfaceState) -> CoreResult<()> {
        let mut builder = FibTableBuilder::<u16>::new(self.drop_next);
        for record in state.addresses.iter().filter(|record| !record.removed) {
            self.add_address_routes(&mut builder, record);
        }
        self.table.replace_after_barrier(builder.build());
        Ok(())
    }

    fn add_address_routes(
        &self,
        builder: &mut FibTableBuilder<u16>,
        record: &InterfaceAddressRecord,
    ) {
        match record.address {
            IpNet::V4(address) => {
                let receive = Ipv4Net::new(address.addr(), 32).expect("IPv4 host prefix");
                builder.add_ip4_route_dpo(
                    receive,
                    DpoId::<u16>::receive(DpoProto::IP4, self.receive_next),
                );
                if address.prefix_len() < 32 {
                    let prefix =
                        Ipv4Net::new(address.network(), address.prefix_len()).expect("IPv4 prefix");
                    self.add_connected_route(
                        builder,
                        IpNet::V4(prefix),
                        DpoProto::IP4,
                        record.interface_index,
                    );
                }
            }
            IpNet::V6(address) => {
                let receive = Ipv6Net::new(address.addr(), 128).expect("IPv6 host prefix");
                builder.add_ip6_route_dpo(
                    receive,
                    DpoId::<u16>::receive(DpoProto::IP6, self.receive_next),
                );
                if address.prefix_len() < 128 {
                    let prefix =
                        Ipv6Net::new(address.network(), address.prefix_len()).expect("IPv6 prefix");
                    self.add_connected_route(
                        builder,
                        IpNet::V6(prefix),
                        DpoProto::IP6,
                        record.interface_index,
                    );
                }
            }
        }
    }

    fn add_connected_route(
        &self,
        builder: &mut FibTableBuilder<u16>,
        prefix: IpNet,
        proto: DpoProto,
        interface_index: u32,
    ) {
        let Some(nexts) = self.connected_nexts else {
            return;
        };
        let dpo = builder.add_interface_adjacency_dpo(
            proto,
            interface_index,
            AdjacencyRewrite::empty(),
            nexts.adjacency_rewrite_next,
            nexts.interface_output_next,
        );
        builder.add_route_dpo(prefix, dpo);
    }
}

#[derive(Debug, Clone, Copy)]
struct InterfaceConnectedNexts {
    adjacency_rewrite_next: u16,
    interface_output_next: u16,
}

struct InterfaceStateSlot {
    state: UnsafeCell<InterfaceState>,
}

impl InterfaceStateSlot {
    #[inline]
    fn new(state: InterfaceState) -> Self {
        Self {
            state: UnsafeCell::new(state),
        }
    }

    #[inline]
    fn state(&self) -> &InterfaceState {
        // SAFETY: Interface state writes are serialized by the runtime
        // data-plane barrier before publication. Driver/data-plane reads use
        // immutable references while workers are running.
        unsafe { &*self.state.get() }
    }

    #[inline]
    fn replace_after_barrier(&self, state: InterfaceState) {
        // SAFETY: callers replace state either while the runtime data-plane
        // barrier is held, or during single-threaded setup in tests.
        unsafe {
            *self.state.get() = state;
        }
    }
}

impl fmt::Debug for InterfaceStateSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InterfaceStateSlot").finish_non_exhaustive()
    }
}

unsafe impl Send for InterfaceStateSlot {}
unsafe impl Sync for InterfaceStateSlot {}

#[derive(Debug, Clone, Default)]
struct InterfaceState {
    interfaces: Vec<InterfaceRecord>,
    addresses: Vec<InterfaceAddressRecord>,
    address_to_index: FlatHashTable<InterfaceAddressKey, u32>,
}

impl InterfaceState {
    #[inline]
    fn interface(&self, index: u32) -> Option<&InterfaceRecord> {
        self.interfaces.get(index as usize)
    }

    #[inline]
    fn interface_mut(&mut self, index: u32) -> Option<&mut InterfaceRecord> {
        self.interfaces.get_mut(index as usize)
    }

    #[inline]
    fn interface_index(&self, name: &str) -> Option<u32> {
        self.interfaces
            .iter()
            .position(|interface| interface.name == name)
            .and_then(|index| u32::try_from(index).ok())
    }

    #[inline]
    fn interface_name(&self, index: u32) -> Option<&str> {
        self.interface(index)
            .map(|interface| interface.name.as_str())
    }

    #[inline]
    fn interface_mtu(&self, index: u32) -> Option<InterfaceMtu> {
        self.interface(index).map(|interface| interface.mtu)
    }

    fn interface_addresses(&self, index: u32) -> Vec<IpNet> {
        let Some(interface) = self.interface(index) else {
            return Vec::new();
        };
        interface
            .addresses
            .iter()
            .filter_map(|address_index| self.addresses.get(*address_index as usize))
            .filter(|record| !record.removed)
            .map(|record| record.address)
            .collect()
    }

    #[inline]
    fn interface_address_index(&self, interface_index: u32, address: IpNet) -> Option<u32> {
        self.address_to_index
            .lookup(&InterfaceAddressKey::new(interface_index, address))
    }

    fn rebuild_address_index(&mut self) {
        let mut address_to_index = FlatHashTable::with_capacity(self.addresses.len().max(1));
        for (index, address) in self.addresses.iter().enumerate() {
            if address.removed {
                continue;
            }
            let index = u32::try_from(index).expect("interface address index fits u32");
            address_to_index.insert(
                InterfaceAddressKey::new(address.interface_index, address.address),
                index,
            );
        }
        self.address_to_index = address_to_index;
    }
}

#[derive(Debug, Clone)]
struct InterfaceRecord {
    name: String,
    addresses: Vec<u32>,
    mtu: InterfaceMtu,
}

#[derive(Debug, Clone)]
struct InterfaceAddressRecord {
    interface_index: u32,
    address: IpNet,
    removed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct InterfaceAddressKey {
    interface_index: u32,
    address_family: u8,
    prefix_len: u8,
    address_hi: u64,
    address_lo: u64,
}

impl InterfaceAddressKey {
    #[inline]
    fn new(interface_index: u32, address: IpNet) -> Self {
        let (address_family, prefix_len, address_hi, address_lo) = match address {
            IpNet::V4(address) => {
                let value = u32::from(address.addr());
                (4, address.prefix_len(), 0, u64::from(value))
            }
            IpNet::V6(address) => {
                let value = u128::from(address.addr());
                (6, address.prefix_len(), (value >> 64) as u64, value as u64)
            }
        };
        Self {
            interface_index,
            address_family,
            prefix_len,
            address_hi,
            address_lo,
        }
    }
}

impl FlatHashKey for InterfaceAddressKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        let mut value = u128::from(self.interface_index);
        value ^= u128::from(self.address_family) << 32;
        value ^= u128::from(self.prefix_len) << 40;
        value ^= u128::from(self.address_hi) << 64;
        value ^= u128::from(self.address_lo);
        value.hash_key()
    }
}

struct InterfaceOutputStateSlot {
    state: UnsafeCell<InterfaceOutputState>,
}

impl InterfaceOutputStateSlot {
    #[inline]
    fn new(state: InterfaceOutputState) -> Self {
        Self {
            state: UnsafeCell::new(state),
        }
    }

    #[inline]
    fn state(&self) -> &InterfaceOutputState {
        // SAFETY: Interface output map writes are serialized by the runtime
        // data-plane barrier before publication. Data-plane nodes only read an
        // immutable snapshot while workers are running.
        unsafe { &*self.state.get() }
    }

    #[inline]
    fn replace_after_barrier(&self, state: InterfaceOutputState) {
        // SAFETY: callers replace state either under the runtime barrier or
        // during single-threaded graph setup in tests.
        unsafe {
            *self.state.get() = state;
        }
    }
}

impl fmt::Debug for InterfaceOutputStateSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InterfaceOutputStateSlot")
            .finish_non_exhaustive()
    }
}

unsafe impl Send for InterfaceOutputStateSlot {}
unsafe impl Sync for InterfaceOutputStateSlot {}

#[derive(Debug, Clone)]
pub struct InterfaceOutputHandle {
    inner: Arc<InterfaceOutputStateSlot>,
}

impl InterfaceOutputHandle {
    #[inline]
    fn new(inner: Arc<InterfaceOutputStateSlot>) -> Self {
        Self { inner }
    }

    #[inline]
    pub fn tx_slot(&self, interface_index: u32) -> Option<u16> {
        self.inner.state().tx_slot(interface_index)
    }

    #[inline]
    pub fn drop_slot(&self) -> Option<u16> {
        self.inner.state().drop_slot
    }
}

pub struct InterfaceOutputControlPlane {
    inner: Arc<InterfaceOutputStateSlot>,
    barrier: Option<DataPlaneBarrierHandle>,
    nodes: Option<hammer_runtime::node::NodeRuntime>,
    consumer: Option<NodeId>,
}

impl Default for InterfaceOutputControlPlane {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl InterfaceOutputControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InterfaceOutputStateSlot::new(
                InterfaceOutputState::default(),
            )),
            barrier: None,
            nodes: None,
            consumer: None,
        }
    }

    #[inline]
    pub fn with_data_plane_barrier(mut self, barrier: DataPlaneBarrierHandle) -> Self {
        self.barrier = Some(barrier);
        self
    }

    #[inline]
    pub fn with_nodes(mut self, nodes: hammer_runtime::node::NodeRuntime) -> Self {
        self.nodes = Some(nodes);
        self
    }

    #[inline]
    pub fn handle(&self) -> InterfaceOutputHandle {
        InterfaceOutputHandle::new(Arc::clone(&self.inner))
    }

    #[inline]
    pub fn node(&self) -> InterfaceOutputNode {
        InterfaceOutputNode::new(self.handle())
    }

    /// Wire Drop into the interface-output local-next table.
    pub fn attach_consumer(&mut self, consumer: NodeId) -> CoreResult<()> {
        let nodes = self
            .nodes
            .as_ref()
            .ok_or_else(|| CoreError::internal("interface output attach requires node runtime"))?;
        let drop = nodes
            .node_by_name("drop")
            .ok_or_else(|| CoreError::internal("interface output attach requires drop node"))?;
        let drop_slot = nodes.add_node_next_slot(consumer, drop)?;
        self.consumer = Some(consumer);
        self.synchronize(|| {
            let current = self.inner.state();
            let mut next = InterfaceOutputState::clone(current);
            next.drop_slot = Some(drop_slot);
            self.publish(next);
            Ok(())
        })
    }

    pub fn register_tx(&self, interface_index: u32, node: NodeId) -> CoreResult<u16> {
        let consumer = self.consumer.ok_or_else(|| {
            CoreError::internal("interface output register_tx requires attach_consumer")
        })?;
        let nodes = self.nodes.as_ref().ok_or_else(|| {
            CoreError::internal("interface output register_tx requires node runtime")
        })?;
        let slot = nodes.add_node_next_slot(consumer, node)?;
        self.synchronize(|| {
            let current = self.inner.state();
            let mut next = InterfaceOutputState::clone(current);
            next.set_tx_slot(interface_index, Some(slot));
            self.publish(next);
            Ok(slot)
        })
    }

    pub fn unregister_tx(&self, interface_index: u32) -> CoreResult<bool> {
        let mut removed = false;
        self.synchronize(|| {
            let current = self.inner.state();
            removed = current.tx_slot(interface_index).is_some();
            if !removed {
                return Ok(());
            }
            let mut next = InterfaceOutputState::clone(current);
            next.set_tx_slot(interface_index, None);
            self.publish(next);
            Ok(())
        })?;
        Ok(removed)
    }

    #[inline]
    fn synchronize<R>(&self, operation: impl FnOnce() -> CoreResult<R>) -> CoreResult<R> {
        if let Some(barrier) = &self.barrier {
            barrier.synchronize(operation)
        } else {
            operation()
        }
    }

    #[inline]
    fn publish(&self, state: InterfaceOutputState) {
        self.inner.replace_after_barrier(state);
    }
}

#[derive(Debug, Clone, Default)]
struct InterfaceOutputState {
    drop_slot: Option<u16>,
    tx_slots: Vec<Option<u16>>,
}

impl InterfaceOutputState {
    #[inline]
    fn tx_slot(&self, interface_index: u32) -> Option<u16> {
        self.tx_slots
            .get(interface_index as usize)
            .copied()
            .flatten()
    }

    #[inline]
    fn set_tx_slot(&mut self, interface_index: u32, slot: Option<u16>) {
        let index = interface_index as usize;
        while self.tx_slots.len() <= index {
            self.tx_slots.push(None);
        }
        self.tx_slots[index] = slot;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceOutputNodeError {
    MissingEgressInterface,
    MissingTxNode,
}

impl InterfaceOutputNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        match self {
            Self::MissingEgressInterface => 1,
            Self::MissingTxNode => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceOutputTrace {
    pub egress_interface: Option<u32>,
    pub tx_next: Option<u16>,
    pub error: Option<u16>,
    pub next: Option<u16>,
}

impl InterfaceOutputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            egress_interface: cursor.read_option_u32()?,
            tx_next: cursor.read_option_u16()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_option_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for InterfaceOutputTrace {
    #[inline]
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_u32(out, self.egress_interface);
        put_option_u16(out, self.tx_next);
        put_option_u16(out, self.error);
        put_option_u16(out, self.next);
    }
}

fn format_interface_output_trace(bytes: &[u8]) -> String {
    match InterfaceOutputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("InterfaceOutputTrace invalid={bytes:?}"),
    }
}

#[hammer_component_macros::node]
pub struct InterfaceOutputNode {
    #[node(default = register_interface_output_runtime(output.clone()))]
    runtime_data: NodeRuntimeData,
    output: InterfaceOutputHandle,
}

impl InterfaceOutputNode {
    #[inline(always)]
    fn tx_for_index(
        output: &InterfaceOutputHandle,
        runtime: &DataPlaneRuntime,
        index: Index,
        drop_next: u16,
    ) -> CoreResult<u16> {
        let interface_index = {
            let buffer = runtime.get_buffer(index)?;
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
            network.sw_if_index[1]
        };
        if interface_index == 0 {
            set_index_node_error_code(
                runtime,
                index,
                InterfaceOutputNodeError::MissingEgressInterface.code(),
            )?;
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: None,
                    tx_next: None,
                    error: Some(InterfaceOutputNodeError::MissingEgressInterface.code()),
                    next: Some(drop_next),
                },
            );
            return Ok(drop_next);
        }
        let Some(tx) = output.tx_slot(interface_index) else {
            set_index_node_error_code(
                runtime,
                index,
                InterfaceOutputNodeError::MissingTxNode.code(),
            )?;
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: Some(interface_index),
                    tx_next: None,
                    error: Some(InterfaceOutputNodeError::MissingTxNode.code()),
                    next: Some(drop_next),
                },
            );
            return Ok(drop_next);
        };
        let _ = add_packet_trace!(
            runtime,
            index,
            InterfaceOutputTrace {
                egress_interface: Some(interface_index),
                tx_next: Some(tx),
                error: None,
                next: Some(tx),
            },
        );
        Ok(tx)
    }
}

impl Node for InterfaceOutputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        interface_output_process_frame(runtime, frame, &self.output)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_interface_output_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        interface_output_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for InterfaceOutputNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
    }
}

#[derive(Clone)]
struct InterfaceOutputRuntime {
    output: InterfaceOutputHandle,
}

fn interface_output_runtimes() -> &'static Mutex<Vec<InterfaceOutputRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<InterfaceOutputRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_interface_output_runtime(output: InterfaceOutputHandle) -> NodeRuntimeData {
    let mut runtimes = interface_output_runtimes()
        .lock()
        .expect("interface output runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(InterfaceOutputRuntime { output });
    NodeRuntimeData::from_usize(slot).expect("interface output runtime slot overflow")
}

fn interface_output_runtime(data: NodeRuntimeData) -> CoreResult<InterfaceOutputRuntime> {
    let slot = data.usize_word(0)?;
    interface_output_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("interface output runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("interface output runtime slot is invalid"))
}

fn interface_output_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match interface_output_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    interface_output_process_frame(runtime, frame, &state.output)
}

#[inline(always)]
fn interface_output_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    output: &InterfaceOutputHandle,
) -> NodeResult {
    let Some(drop_next) = output.drop_slot() else {
        return NodeResult::drop();
    };
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match InterfaceOutputNode::tx_for_index(output, runtime, index, drop_next) {
            Ok(slot) => slot,
            Err(_) => drop_next,
        }
    })
}

#[cfg(test)]
mod init_tests {
    use super::*;
    use hammer_core::config::Config;
    use hammer_core::config::loader::parse_config;
    use hammer_core::registry::RuntimeRegistry;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn interface_init_publishes_configured_interfaces() {
        reset_interface_main_for_test();
        let cfg = parse_config(
            r#"
[[network.interface]]
name = "utun9"
address = ["10.0.0.1/30"]
mtu = { l3 = 1500, ip4 = 1500, ip6 = 1500, mpls = 1500 }
"#,
        )
        .expect("parse");
        let registry = RuntimeRegistry::new();
        registry.set::<Config>(Arc::new(cfg));
        init(registry.as_ref()).expect("interface_init");

        let main = INTERFACE_MAIN.load();
        let plane = main.as_deref().expect("interface main");
        let handle = plane.handle();
        let index = handle.interface_index("utun9").expect("name");
        assert_eq!(handle.interface_name(index).as_deref(), Some("utun9"));
        assert_eq!(
            handle.interface_mtu(index),
            Some(InterfaceMtu::new(1500, 1500, 1500, 1500))
        );
        assert_eq!(
            handle.interface_addresses(index),
            hammer_infra::vec![IpNet::from_str("10.0.0.1/30").expect("cidr")]
        );
    }
}
