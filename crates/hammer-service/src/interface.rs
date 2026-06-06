use std::cell::UnsafeCell;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData, PacketTrace, TraceFormatter,
    add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::forwarding::AdjacencyRewrite;
use hammer_infra::map::{FlatHashKey, FlatHashTable};
use hammer_infra::vec::Vec;
use hammer_runtime::DataPlaneBarrierHandle;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

use crate::data_plane::set_index_node_error_code;
use crate::net::{DpoId, DpoProto, FibTableBuilder, FibTableHandle};
use crate::trace::codec::{TraceDecodeCursor, put_option_node, put_option_u16, put_option_u32};

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

#[derive(Debug, Clone)]
pub struct InterfaceConnectedRouteControl {
    table: FibTableHandle,
    drop_next: NodeId,
    receive_next: NodeId,
    connected_nexts: Option<InterfaceConnectedNexts>,
}

impl InterfaceConnectedRouteControl {
    #[inline]
    pub fn new(table: FibTableHandle, drop_next: NodeId, receive_next: NodeId) -> Self {
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
        adjacency_rewrite_next: NodeId,
        interface_output_next: NodeId,
    ) -> Self {
        self.connected_nexts = Some(InterfaceConnectedNexts {
            adjacency_rewrite_next,
            interface_output_next,
        });
        self
    }

    fn publish_state(&self, state: &InterfaceState) -> CoreResult<()> {
        let mut builder = FibTableBuilder::new(self.drop_next);
        for record in state.addresses.iter().filter(|record| !record.removed) {
            self.add_address_routes(&mut builder, record);
        }
        self.table.replace_after_barrier(builder.build());
        Ok(())
    }

    fn add_address_routes(&self, builder: &mut FibTableBuilder, record: &InterfaceAddressRecord) {
        match record.address {
            IpNet::V4(address) => {
                let receive = Ipv4Net::new(address.addr(), 32).expect("IPv4 host prefix");
                builder
                    .add_ip4_route_dpo(receive, DpoId::receive(DpoProto::IP4, self.receive_next));
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
                builder
                    .add_ip6_route_dpo(receive, DpoId::receive(DpoProto::IP6, self.receive_next));
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
        builder: &mut FibTableBuilder,
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
    adjacency_rewrite_next: NodeId,
    interface_output_next: NodeId,
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
    pub fn tx_node(&self, interface_index: u32) -> Option<NodeId> {
        self.inner.state().tx_node(interface_index)
    }
}

pub struct InterfaceOutputControlPlane {
    inner: Arc<InterfaceOutputStateSlot>,
    barrier: Option<DataPlaneBarrierHandle>,
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
        }
    }

    #[inline]
    pub fn with_data_plane_barrier(mut self, barrier: DataPlaneBarrierHandle) -> Self {
        self.barrier = Some(barrier);
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

    pub fn register_tx(&self, interface_index: u32, node: NodeId) -> CoreResult<()> {
        self.synchronize(|| {
            let current = self.inner.state();
            let mut next = InterfaceOutputState::clone(current);
            next.set_tx_node(interface_index, Some(node));
            self.publish(next);
            Ok(())
        })
    }

    pub fn unregister_tx(&self, interface_index: u32) -> CoreResult<bool> {
        let mut removed = false;
        self.synchronize(|| {
            let current = self.inner.state();
            removed = current.tx_node(interface_index).is_some();
            if !removed {
                return Ok(());
            }
            let mut next = InterfaceOutputState::clone(current);
            next.set_tx_node(interface_index, None);
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

#[derive(Debug, Clone, Default)]
struct InterfaceOutputState {
    tx_nodes: Vec<Option<NodeId>>,
}

impl InterfaceOutputState {
    #[inline]
    fn tx_node(&self, interface_index: u32) -> Option<NodeId> {
        self.tx_nodes
            .get(interface_index as usize)
            .copied()
            .flatten()
    }

    #[inline]
    fn set_tx_node(&mut self, interface_index: u32, node: Option<NodeId>) {
        let slot = interface_index as usize;
        while self.tx_nodes.len() <= slot {
            self.tx_nodes.push(None);
        }
        self.tx_nodes[slot] = node;
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
    pub tx_node: Option<NodeId>,
    pub error: Option<u16>,
    pub next: Option<NodeId>,
}

impl InterfaceOutputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            egress_interface: cursor.read_option_u32()?,
            tx_node: cursor.read_option_node()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_option_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for InterfaceOutputTrace {
    #[inline]
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_u32(out, self.egress_interface);
        put_option_node(out, self.tx_node);
        put_option_u16(out, self.error);
        put_option_node(out, self.next);
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
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl InterfaceOutputNode {
    #[inline(always)]
    fn tx_for_index(
        output: &InterfaceOutputHandle,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<Option<NodeId>> {
        let metadata = runtime.metadata(index)?;
        let Some(interface_index) = metadata.egress_interface else {
            set_index_node_error_code(
                runtime,
                index,
                InterfaceOutputNodeError::MissingEgressInterface.code(),
            )?;
            add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: None,
                    tx_node: None,
                    error: Some(InterfaceOutputNodeError::MissingEgressInterface.code()),
                    next: None,
                },
            )?;
            runtime.free_index(index);
            return Ok(None);
        };
        let Some(tx) = output.tx_node(interface_index) else {
            set_index_node_error_code(
                runtime,
                index,
                InterfaceOutputNodeError::MissingTxNode.code(),
            )?;
            add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: Some(interface_index),
                    tx_node: None,
                    error: Some(InterfaceOutputNodeError::MissingTxNode.code()),
                    next: None,
                },
            )?;
            runtime.free_index(index);
            return Ok(None);
        };
        add_packet_trace!(
            runtime,
            index,
            InterfaceOutputTrace {
                egress_interface: Some(interface_index),
                tx_node: Some(tx),
                error: None,
                next: Some(tx),
            },
        )?;
        Ok(Some(tx))
    }
}

impl Node for InterfaceOutputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut next_frames = NodeNextFrames::default();
        let mut current_next = self.cached_next;
        let mut last_next = None;
        let result =
            frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
                let Some(node) = Self::tx_for_index(&self.output, runtime, index)? else {
                    return Ok(None);
                };
                last_next = Some(node);
                match current_next {
                    Some(current) if current == node => Ok(Some(index)),
                    Some(_) => {
                        next_frames.enqueue(runtime, node, index)?;
                        Ok(None)
                    }
                    None => {
                        current_next = Some(node);
                        Ok(Some(index))
                    }
                }
            });
        if let Err(err) = result {
            next_frames.free(runtime);
            return Err(err);
        }

        next_frames.schedule(runtime)?;
        if let Some(node) = last_next {
            self.cached_next = Some(node);
        }
        if frame.has_pending()
            && let Some(node) = current_next
        {
            Ok(NodeResult::next_current(node))
        } else {
            Ok(NodeResult::drop())
        }
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
        NodeRegistration::next("interface-output-node", 0)
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
) -> CoreResult<NodeResult> {
    let state = interface_output_runtime(data)?;
    let mut next_frames = NodeNextFrames::default();
    let mut current_next = None;
    let result = frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        let Some(node) = InterfaceOutputNode::tx_for_index(&state.output, runtime, index)? else {
            return Ok(None);
        };
        match current_next {
            Some(current) if current == node => Ok(Some(index)),
            Some(_) => {
                next_frames.enqueue(runtime, node, index)?;
                Ok(None)
            }
            None => {
                current_next = Some(node);
                Ok(Some(index))
            }
        }
    });
    if let Err(err) = result {
        next_frames.free(runtime);
        return Err(err);
    }

    next_frames.schedule(runtime)?;
    if frame.has_pending()
        && let Some(node) = current_next
    {
        Ok(NodeResult::next_current(node))
    } else {
        Ok(NodeResult::drop())
    }
}
