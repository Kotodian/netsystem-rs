use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::fmt;
use std::mem::transmute;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::data_plane::{BufferFrame, Index, NodeId, NodeRegistration};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, InternalNode, Node, NodeProcessFn, NodeRuntimeData,
    add_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use ipnet::IpNet;

use crate::device::DeviceTxQueue;
use crate::opaque::NetworkOpaque;

pub const DEFAULT_INTERFACE_MTU: u32 = 9_000;

/// One generic interface declaration embedded by a device plugin's own
/// configuration section.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterfaceConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub address: Vec<IpNet>,
    #[serde(default)]
    pub mtu: InterfaceConfigMtu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct InterfaceConfigMtu {
    pub l3: u32,
    pub ip4: u32,
    pub ip6: u32,
    pub mpls: u32,
}

impl Default for InterfaceConfigMtu {
    fn default() -> Self {
        Self {
            l3: DEFAULT_INTERFACE_MTU,
            ip4: DEFAULT_INTERFACE_MTU,
            ip6: DEFAULT_INTERFACE_MTU,
            mpls: DEFAULT_INTERFACE_MTU,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InterfaceError {
    #[error("interface name is empty")]
    NameEmpty,
    #[error("interface index space is exhausted at {interface_count} interfaces")]
    IndexSpaceExhausted { interface_count: usize },
    #[error("interface {interface_index} is not registered")]
    NotRegistered { interface_index: u32 },
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

pub type InterfaceResult<T> = Result<T, InterfaceError>;

impl From<InterfaceError> for RuntimeError {
    fn from(error: InterfaceError) -> Self {
        match error {
            InterfaceError::Runtime(error) => error,
            other => Self::subsystem("interface", other),
        }
    }
}

impl InterfaceConfig {
    fn validate(&self) -> RuntimeResult<()> {
        if self.name.is_empty() {
            return Err(RuntimeError::config_validation(
                "interface.name must be non-empty",
            ));
        }
        let mtu = self.mtu;
        if mtu.l3 == 0 || mtu.ip4 == 0 || mtu.ip6 == 0 || mtu.mpls == 0 {
            return Err(RuntimeError::config_validation(
                "interface.mtu values must be non-zero",
            ));
        }
        Ok(())
    }
}

/// Materialize generic interface state from one device plugin's declarations.
///
/// Each driver owns where its declarations appear in TOML. The service owns
/// the common schema, validation, and interface state installation so another
/// driver can use the same implementation without sharing TUN configuration.
pub fn configure_interfaces(
    interfaces: &[InterfaceConfig],
) -> RuntimeResult<Arc<InterfaceControlPlane>> {
    let mut names = std::collections::HashSet::with_capacity(interfaces.len());
    for interface in interfaces {
        interface.validate()?;
        if !names.insert(interface.name.as_str()) {
            return Err(RuntimeError::config_validation(format!(
                "duplicate interface name: {}",
                interface.name
            )));
        }
    }

    let plane = INTERFACE_MAIN
        .load_full()
        .unwrap_or_else(|| Arc::new(InterfaceControlPlane::new()));
    for interface in interfaces {
        let mtu = InterfaceMtu::new(
            interface.mtu.l3,
            interface.mtu.ip4,
            interface.mtu.ip6,
            interface.mtu.mpls,
        );
        let index = plane.register_interface_with_mtu(interface.name.clone(), mtu)?;
        for address in &interface.address {
            plane.add_address(index, *address)?;
        }
    }
    INTERFACE_MAIN.store(Some(Arc::clone(&plane)));
    Ok(plane)
}

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
        }
    }

    #[inline]
    pub fn handle(&self) -> InterfaceControlHandle {
        InterfaceControlHandle::new(Arc::clone(&self.inner))
    }

    pub fn register_interface(&self, name: impl Into<String>) -> InterfaceResult<u32> {
        self.register_interface_with_mtu(name, InterfaceMtu::default())
    }

    pub fn register_interface_with_mtu(
        &self,
        name: impl Into<String>,
        mtu: InterfaceMtu,
    ) -> InterfaceResult<u32> {
        let name = name.into();
        if name.is_empty() {
            return Err(InterfaceError::NameEmpty);
        }
        let current = self.inner.state();
        if let Some(current_index) = current.interface_index(&name) {
            return Ok(current_index);
        }
        let mut next = InterfaceState::clone(&current);
        let interface_count = next.interfaces.len();
        let next_index = u32::try_from(interface_count)
            .map_err(|_| InterfaceError::IndexSpaceExhausted { interface_count })?;
        if next_index == u32::MAX {
            return Err(InterfaceError::IndexSpaceExhausted { interface_count });
        }
        next.interfaces.push(InterfaceRecord {
            name,
            addresses: Vec::new(),
            mtu,
        });
        self.publish(next);
        Ok(next_index)
    }

    pub fn set_mtu(&self, interface_index: u32, mtu: InterfaceMtu) -> InterfaceResult<()> {
        self.ensure_interface(interface_index)?;
        let current = self.inner.state();
        let mut next = InterfaceState::clone(current);
        let interface = next
            .interface_mut(interface_index)
            .ok_or(InterfaceError::NotRegistered { interface_index })?;
        interface.mtu = mtu;
        self.publish(next);
        Ok(())
    }

    pub fn set_protocol_mtu(
        &self,
        interface_index: u32,
        kind: InterfaceMtuKind,
        value: u32,
    ) -> InterfaceResult<()> {
        self.ensure_interface(interface_index)?;
        let current = self.inner.state();
        let mut next = InterfaceState::clone(current);
        let interface = next
            .interface_mut(interface_index)
            .ok_or(InterfaceError::NotRegistered { interface_index })?;
        interface.mtu.set(kind, value);
        self.publish(next);
        Ok(())
    }

    pub fn add_address(&self, interface_index: u32, address: IpNet) -> InterfaceResult<u32> {
        self.ensure_interface(interface_index)?;
        let current = self.inner.state();
        if let Some(current_index) = current.interface_address_index(interface_index, address) {
            return Ok(current_index);
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
        self.publish(next);
        Ok(index)
    }

    pub fn remove_address(&self, interface_index: u32, address: IpNet) -> InterfaceResult<bool> {
        self.ensure_interface(interface_index)?;
        let current = self.inner.state();
        let Some(address_index) = current.interface_address_index(interface_index, address) else {
            return Ok(false);
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
        self.publish(next);
        Ok(true)
    }

    #[inline]
    fn ensure_interface(&self, interface_index: u32) -> InterfaceResult<()> {
        if self.inner.state().interface(interface_index).is_some() {
            Ok(())
        } else {
            Err(InterfaceError::NotRegistered { interface_index })
        }
    }

    #[inline]
    fn publish(&self, state: InterfaceState) {
        if let Some(barrier) = hammer_runtime::barrier::global() {
            let inner = Arc::clone(&self.inner);
            barrier.sync(|| inner.publish(state));
        } else {
            self.inner.publish(state);
        }
    }
}

/// Process-level interface control plane (VPP-style main). Device plugins
/// supply their own configuration sections; service owns the generic state.
pub static INTERFACE_MAIN: ArcSwapOption<InterfaceControlPlane> = ArcSwapOption::const_empty();

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
    fn publish(&self, state: InterfaceState) {
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
    address_to_index: HashMap<InterfaceAddressKey, u32>,
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
            .get(&InterfaceAddressKey::new(interface_index, address))
            .copied()
    }

    fn rebuild_address_index(&mut self) {
        let mut address_to_index = HashMap::with_capacity(self.addresses.len());
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

thread_local! {
    static INTERFACE_OUTPUT_STATE: RefCell<InterfaceOutputState> =
        RefCell::new(InterfaceOutputState::default());
}

impl InterfaceOutputNode {
    pub(crate) fn install_worker_queues(worker: DataWorkerId, tx_queues: &[DeviceTxQueue]) {
        let mut state = InterfaceOutputState::default();
        for queue in tx_queues {
            state.drop_slot = Some(queue.drop_slot);
            if !queue.is_assigned_to(worker) {
                continue;
            }
            state.set_tx_slot(queue.interface_index, Some(queue.output_slot));
        }

        INTERFACE_OUTPUT_STATE.with(|worker_state| {
            worker_state.replace(state);
        });
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

/// Trace-only output diagnostics. Missing interface or TX graph state is an
/// owner/resource failure, not a node business-error counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum InterfaceOutputTraceError {
    MissingEgressInterface,
    MissingTxNode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct InterfaceOutputTrace {
    pub egress_interface: Option<u32>,
    pub tx_next: Option<u16>,
    pub error: Option<InterfaceOutputTraceError>,
    pub next: Option<u16>,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = register_interface_output_graph,
    name = "interface-output",
)]
#[derive(Debug, Clone, Copy)]
pub struct InterfaceOutputNode;

fn register_interface_output_graph(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal(InterfaceOutputNode)
}

impl InterfaceOutputNode {
    #[inline(always)]
    fn tx_for_index(
        output: &InterfaceOutputState,
        runtime: &DataPlaneMain,
        index: Index,
        drop_next: u16,
    ) -> RuntimeResult<u16> {
        let interface_index = {
            let buffer = runtime.get_buffer(index)?;
            let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
            network.sw_if_index[1]
        };
        if interface_index == u32::MAX {
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: None,
                    tx_next: None,
                    error: Some(InterfaceOutputTraceError::MissingEgressInterface),
                    next: Some(drop_next),
                },
            );
            return Ok(drop_next);
        }
        let Some(tx) = output.tx_slot(interface_index) else {
            let _ = add_packet_trace!(
                runtime,
                index,
                InterfaceOutputTrace {
                    egress_interface: Some(interface_index),
                    tx_next: None,
                    error: Some(InterfaceOutputTraceError::MissingTxNode),
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
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        INTERFACE_OUTPUT_STATE.with(|state| {
            let state = state.borrow();
            interface_output_process_frame(runtime, frame, &state)
        })
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        interface_output_process
    }
}

impl InternalNode for InterfaceOutputNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        Some(NodeRegistration::next("interface-output", 0))
    }
}

fn interface_output_process(
    runtime: &DataPlaneMain,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
    INTERFACE_OUTPUT_STATE.with(|state| {
        let state = state.borrow();
        interface_output_process_frame(runtime, frame, &state)
    })
}

#[inline(always)]
fn interface_output_process_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    output: &InterfaceOutputState,
) -> () {
    let Some(drop_next) = output.drop_slot else {
        return ();
    };
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match InterfaceOutputNode::tx_for_index(output, runtime, index, drop_next) {
            Ok(slot) => slot,
            Err(_) => drop_next,
        }
    })
}
