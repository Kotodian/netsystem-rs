use std::cell::UnsafeCell;
use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU64;
use std::sync::{Arc, OnceLock};

use crate::wire::UdpHeader;
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId, SecondaryOpaque};
use hammer_infra::bitmap::Bitmap;
use hammer_infra::checksum::internet_checksum_parts;
use hammer_infra::sparse_vec::SparseVec;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneRuntime, Engine, Node, NodeProcessFn, NodeResult, NodeRuntimeData, RuntimeError,
    TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_service::data_plane::set_index_node_error;
use hammer_service::opaque::NetworkOpaque;

use crate::UdpIpVersion;

const UDP_HEADER_LEN: usize = 8;
const UDP_PORT_COUNT: usize = u16::MAX as usize + 1;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<NonZeroU64>,
    reserved: [u64; 6],
}

const _: () = assert!(size_of::<IcmpErrorOpaque>() == size_of::<SecondaryOpaque>());

#[hammer_component_macros::node_next]
pub enum UdpInputNext {
    #[next("drop")]
    Drop,
    #[next("drop")]
    Punt,
    #[next("drop")]
    IcmpError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum UdpInputError {
    BadLength,
    WrongProtocol,
    UnknownPort,
    BadChecksum,
    SessionMissing,
    FifoFull,
    FifoNoMemory,
    WrongWorker,
}

impl hammer_runtime::node::NodeErrorCode for UdpInputError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl UdpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[hammer_component_macros::runtime_error(subsystem = "udp")]
#[derive(Debug, thiserror::Error)]
pub enum UdpControlError {
    #[error("UDP input consumer is not attached")]
    ConsumerNotAttached,
    #[error("UDP input node runtime is unavailable")]
    NodeRuntimeUnavailable,
    #[error("UDP input runtime registry is poisoned")]
    RuntimeRegistryPoisoned,
    #[error("UDP input runtime slot {slot} is not registered")]
    RuntimeSlotInvalid { slot: usize },
    #[error("UDP header at offset {offset} is truncated or out of range")]
    HeaderOutOfRange { offset: usize },
    #[error(
        "UDP destination port {port} for {version:?} is owned by node {owner:?}, not node {requested_owner:?}"
    )]
    PortConflict {
        version: UdpIpVersion,
        port: u16,
        owner: NodeId,
        requested_owner: NodeId,
    },
    #[error("UDP destination port {port} for {version:?} is not registered")]
    PortNotRegistered { version: UdpIpVersion, port: u16 },
    #[error(
        "UDP destination port {port} for {version:?} is owned by node {owner:?}, not node {requested_owner:?}"
    )]
    PortOwnerMismatch {
        version: UdpIpVersion,
        port: u16,
        owner: NodeId,
        requested_owner: NodeId,
    },
    #[error("UDP destination port {port} for {version:?} reference count is exhausted")]
    PortReferenceCountOverflow { version: UdpIpVersion, port: u16 },
    #[error("UDP destination-port control is not initialized")]
    PortControlNotInitialized,
    #[error("UDP destination-port control is already initialized")]
    PortControlAlreadyInitialized,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct UdpInputTrace {
    pub version: Option<UdpIpVersion>,
    pub protocol: Option<UdpIpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub error: Option<u16>,
    pub next: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum UdpIpProtocol {
    Icmpv4,
    Tcp,
    Udp,
    Icmpv6,
    Other(u8),
}

pub struct UdpInputControlPlane {
    inner: Arc<UdpInputSnapshotCell>,
    nodes: Option<hammer_runtime::node::NodeRuntime>,
    consumer: Option<NodeId>,
}

impl UdpInputControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(UdpInputSnapshotCell::new()),
            nodes: None,
            consumer: None,
        }
    }

    #[inline]
    pub fn with_nodes(mut self, nodes: hammer_runtime::node::NodeRuntime) -> Self {
        self.nodes = Some(nodes);
        self
    }

    pub fn attach_consumer(&mut self, consumer: NodeId) -> RuntimeResult<()> {
        if self.nodes.is_none() {
            return Err(UdpControlError::NodeRuntimeUnavailable.into());
        }
        self.consumer = Some(consumer);
        Ok(())
    }

    #[inline]
    pub fn register_dst_port(
        &self,
        version: UdpIpVersion,
        port: u16,
        node: NodeId,
    ) -> RuntimeResult<u16> {
        self.ensure_control_barrier()?;
        let consumer = self.consumer.ok_or(UdpControlError::ConsumerNotAttached)?;
        let nodes = self
            .nodes
            .as_ref()
            .ok_or(UdpControlError::NodeRuntimeUnavailable)?;
        self.register_dst_port_with_graph(version, port, node, nodes, consumer)
    }

    /// Compatibility entry point for the original IPv4-only control surface.
    #[inline]
    pub fn register_port(&self, port: u16, node: NodeId) -> RuntimeResult<u16> {
        self.register_dst_port(UdpIpVersion::V4, port, node)
    }

    #[inline]
    pub fn register_punt_port(&self, port: u16) -> RuntimeResult<()> {
        self.ensure_control_barrier()?;
        self.mutate_snapshot(|snapshot| snapshot.register_punt_port(port))
    }

    #[inline]
    pub fn unregister_dst_port(
        &self,
        version: UdpIpVersion,
        port: u16,
        node: NodeId,
    ) -> RuntimeResult<()> {
        self.ensure_control_barrier()?;
        let current = self.inner.get();
        let registration = current
            .registration(version, port)
            .ok_or(UdpControlError::PortNotRegistered { version, port })?;
        if registration.node != node {
            return Err(UdpControlError::PortOwnerMismatch {
                version,
                port,
                owner: registration.node,
                requested_owner: node,
            }
            .into());
        }
        self.mutate_snapshot(|snapshot| snapshot.release_port(version, port, node))
    }

    fn register_dst_port_with_graph(
        &self,
        version: UdpIpVersion,
        port: u16,
        node: NodeId,
        nodes: &hammer_runtime::node::NodeRuntime,
        consumer: NodeId,
    ) -> RuntimeResult<u16> {
        let current = self.inner.get();
        if let Some(registration) = current.registration(version, port) {
            if registration.node != node {
                return Err(UdpControlError::PortConflict {
                    version,
                    port,
                    owner: registration.node,
                    requested_owner: node,
                }
                .into());
            }
            if registration.references == u32::MAX {
                return Err(UdpControlError::PortReferenceCountOverflow { version, port }.into());
            }
            let slot = registration.slot;
            self.mutate_snapshot(|snapshot| snapshot.share_port(version, port, node))?;
            return Ok(slot);
        }

        let slot = nodes.add_node_next_slot(consumer, node)?;
        self.mutate_snapshot(|snapshot| snapshot.register_port(version, port, node, slot))?;
        Ok(slot)
    }

    #[inline]
    pub fn unregister_port(&self, port: u16) -> RuntimeResult<()> {
        self.ensure_control_barrier()?;
        self.mutate_snapshot(|snapshot| snapshot.unregister_port(port))
    }

    fn mutate_snapshot(&self, operation: impl FnOnce(&mut UdpInputSnapshot)) -> RuntimeResult<()> {
        self.ensure_control_barrier()?;
        let snapshot = self.inner.get_mut();
        operation(snapshot);
        Ok(())
    }

    fn ensure_control_barrier(&self) -> RuntimeResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()
    }

    #[inline]
    pub fn node(&self) -> UdpInputNode {
        UdpInputNode::new(UdpInputSnapshotHandle::new(Arc::clone(&self.inner)))
    }
}

/// Main-thread UDP capability state retained by the plugin ABI.
///
/// The caller must be the main/control engine and hold the worker barrier when
/// Data Workers exist. UDP only retains the existing port table and the UDP
/// input node identity; it does not retain a barrier or create another
/// publication protocol.
struct UdpLocalRegistration {
    inner: Arc<UdpInputSnapshotCell>,
    consumer: NodeId,
}

static UDP_LOCAL_REGISTRATION: OnceLock<UdpLocalRegistration> = OnceLock::new();

pub(crate) fn is_external_port_registered(version: UdpIpVersion, port: u16) -> bool {
    let Some(registration) = UDP_LOCAL_REGISTRATION.get() else {
        return false;
    };
    registration
        .inner
        .get()
        .registration(version, port)
        .is_some()
}

pub(crate) fn register_dst_port(
    version: UdpIpVersion,
    port: u16,
    node: NodeId,
) -> RuntimeResult<()> {
    let registration = UDP_LOCAL_REGISTRATION
        .get()
        .ok_or(UdpControlError::PortControlNotInitialized)?;
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let result = Engine::with_current(|engine| {
        let control = UdpInputControlPlane {
            inner: Arc::clone(&registration.inner),
            nodes: Some(engine.runtime.nodes().clone()),
            consumer: Some(registration.consumer),
        };
        control.register_dst_port(version, port, node).map(|_| ())
    })
    .ok_or(RuntimeError::ControlRequiresMainThread)?;
    result
}

pub(crate) fn unregister_dst_port(
    version: UdpIpVersion,
    port: u16,
    node: NodeId,
) -> RuntimeResult<()> {
    let registration = UDP_LOCAL_REGISTRATION
        .get()
        .ok_or(UdpControlError::PortControlNotInitialized)?;
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let result = Engine::with_current(|_| {
        let control = UdpInputControlPlane {
            inner: Arc::clone(&registration.inner),
            nodes: None,
            consumer: None,
        };
        control.unregister_dst_port(version, port, node)
    })
    .ok_or(RuntimeError::ControlRequiresMainThread)?;
    result
}

#[cfg(test)]
pub(super) fn install_registration_for_test(
    control: &UdpInputControlPlane,
    consumer: NodeId,
) -> RuntimeResult<()> {
    UDP_LOCAL_REGISTRATION
        .set(UdpLocalRegistration {
            inner: Arc::clone(&control.inner),
            consumer,
        })
        .map_err(|_| UdpControlError::PortControlAlreadyInitialized.into())
}

#[derive(Clone)]
struct UdpInputSnapshot {
    v4: UdpPortTable,
    v6: UdpPortTable,
}

impl UdpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            v4: UdpPortTable::new(),
            v6: UdpPortTable::new(),
        }
    }

    #[inline(always)]
    fn table(&self, version: UdpIpVersion) -> &UdpPortTable {
        match version {
            UdpIpVersion::V4 => &self.v4,
            UdpIpVersion::V6 => &self.v6,
        }
    }

    #[inline(always)]
    fn table_mut(&mut self, version: UdpIpVersion) -> &mut UdpPortTable {
        match version {
            UdpIpVersion::V4 => &mut self.v4,
            UdpIpVersion::V6 => &mut self.v6,
        }
    }

    #[inline(always)]
    fn register_port(&mut self, version: UdpIpVersion, port: u16, node: NodeId, slot: u16) {
        self.table_mut(version).register_port(port, node, slot);
    }

    #[inline(always)]
    fn share_port(&mut self, version: UdpIpVersion, port: u16, node: NodeId) {
        self.table_mut(version).share_port(port, node);
    }

    #[inline(always)]
    fn release_port(&mut self, version: UdpIpVersion, port: u16, node: NodeId) {
        self.table_mut(version).release_port(port, node);
    }

    #[inline(always)]
    fn register_punt_port(&mut self, port: u16) {
        self.v4.register_punt_port(port);
        self.v6.register_punt_port(port);
    }

    #[inline(always)]
    fn unregister_port(&mut self, port: u16) {
        self.v4.unregister_port(port);
        self.v6.unregister_port(port);
    }

    #[inline(always)]
    fn action(&self, version: UdpIpVersion, port: u16) -> UdpPortAction {
        self.table(version).action(port)
    }

    #[inline(always)]
    fn registration(&self, version: UdpIpVersion, port: u16) -> Option<UdpPortRegistration> {
        self.table(version).registration(port)
    }
}

#[derive(Clone)]
struct UdpPortTable {
    dispatch: Bitmap<u16>,
    punt: Bitmap<u16>,
    next_by_dst_port: SparseVec<u16>,
    registrations: SparseVec<UdpPortRegistration>,
}

impl UdpPortTable {
    #[inline]
    fn new() -> Self {
        Self {
            dispatch: Bitmap::with_capacity(UDP_PORT_COUNT),
            punt: Bitmap::with_capacity(UDP_PORT_COUNT),
            next_by_dst_port: SparseVec::with_index_bits(u16::BITS as u8),
            registrations: SparseVec::with_index_bits(u16::BITS as u8),
        }
    }

    #[inline(always)]
    fn register_port(&mut self, port: u16, node: NodeId, slot: u16) {
        debug_assert!(self.registration(port).is_none());
        self.punt.clear(port);
        self.dispatch.set(port);
        self.next_by_dst_port.insert(port as usize, slot);
        let registration = UdpPortRegistration {
            node,
            slot,
            references: 1,
        };
        self.registrations.insert(port as usize, registration);
    }

    #[inline(always)]
    fn share_port(&mut self, port: u16, node: NodeId) {
        let Some(registration) = self.registrations.get_mut(port as usize) else {
            debug_assert!(false, "validated UDP port must remain registered");
            return;
        };
        debug_assert_eq!(registration.node, node);
        registration.references = registration
            .references
            .checked_add(1)
            .expect("validated UDP reference count must not overflow");
    }

    #[inline(always)]
    fn release_port(&mut self, port: u16, node: NodeId) {
        let Some(registration) = self.registrations.get(port as usize) else {
            debug_assert!(false, "validated UDP port must remain registered");
            return;
        };
        debug_assert_eq!(registration.node, node);
        if registration.references > 1 {
            self.registrations
                .get_mut(port as usize)
                .expect("validated UDP registration remains present")
                .references -= 1;
            return;
        }
        self.registrations.remove(port as usize);
        self.next_by_dst_port.remove(port as usize);
        self.dispatch.clear(port);
    }

    #[inline(always)]
    fn register_punt_port(&mut self, port: u16) {
        self.remove_registration(port);
        self.dispatch.clear(port);
        self.punt.set(port);
    }

    #[inline(always)]
    fn unregister_port(&mut self, port: u16) {
        self.remove_registration(port);
        self.dispatch.clear(port);
        self.punt.clear(port);
    }

    #[inline(always)]
    fn action(&self, port: u16) -> UdpPortAction {
        if self.dispatch.is_set(port) {
            UdpPortAction::Dispatch(
                self.next_by_dst_port
                    .get(port as usize)
                    .copied()
                    .unwrap_or(0),
            )
        } else if self.punt.is_set(port) {
            UdpPortAction::Punt
        } else {
            UdpPortAction::IcmpError
        }
    }

    #[inline(always)]
    fn registration(&self, port: u16) -> Option<UdpPortRegistration> {
        self.registrations.get(port as usize).copied()
    }

    #[inline(always)]
    fn remove_registration(&mut self, port: u16) {
        self.registrations.remove(port as usize);
        self.next_by_dst_port.remove(port as usize);
    }
}

#[derive(Debug, Clone, Copy)]
enum UdpPortAction {
    IcmpError,
    Punt,
    Dispatch(u16),
}

#[derive(Debug, Clone, Copy)]
struct UdpPortRegistration {
    node: NodeId,
    slot: u16,
    references: u32,
}

struct UdpInputSnapshotCell {
    value: UnsafeCell<UdpInputSnapshot>,
}

// SAFETY: control mutation is serialized by the WorkerBarrier; packet workers
// only hold shared reads between barrier acknowledgements.
unsafe impl Send for UdpInputSnapshotCell {}
unsafe impl Sync for UdpInputSnapshotCell {}

impl UdpInputSnapshotCell {
    #[inline]
    fn new() -> Self {
        Self {
            value: UnsafeCell::new(UdpInputSnapshot::new()),
        }
    }

    #[inline]
    fn get(&self) -> &UdpInputSnapshot {
        // SAFETY: callers must not overlap this read with `get_mut`.
        unsafe { &*self.value.get() }
    }

    #[allow(clippy::mut_from_ref)]
    #[inline]
    fn get_mut(&self) -> &mut UdpInputSnapshot {
        // SAFETY: callers hold the control-thread/barrier mutation contract.
        unsafe { &mut *self.value.get() }
    }
}

#[derive(Clone)]
struct UdpInputSnapshotHandle {
    inner: Arc<UdpInputSnapshotCell>,
}

impl UdpInputSnapshotHandle {
    #[inline]
    fn new(inner: Arc<UdpInputSnapshotCell>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> &UdpInputSnapshot {
        self.inner.get()
    }
}

fn register_udp_input_runtime(snapshot: UdpInputSnapshotHandle) -> NodeRuntimeData {
    NodeRuntimeData::from_usize(Arc::as_ptr(&snapshot.inner) as usize)
        .expect("UDP input snapshot pointer must fit runtime data")
}

fn udp_input_runtime(data: NodeRuntimeData) -> RuntimeResult<&'static UdpInputSnapshotCell> {
    let pointer = data.usize_word(0)? as *const UdpInputSnapshotCell;
    if pointer.is_null() {
        return Err(UdpControlError::RuntimeSlotInvalid { slot: 0 }.into());
    }
    // SAFETY: the cell is retained by the process-global UDP registration for
    // the lifetime of every installed graph node.
    Ok(unsafe { &*pointer })
}

fn udp_input_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let snapshot = match udp_input_runtime(data) {
        Ok(state) => state.get(),
        Err(_) => return NodeResult::drop(),
    };
    udp_input_process_frame(runtime, frame, snapshot)
}

#[hammer_component_macros::graph_node(
    graph = udp,
    name = "udp-input",
    next = UdpInputNext,
    init = register_udp_input,
    role = internal,
)]
pub struct UdpInputNode {
    #[node(default = register_udp_input_runtime(snapshot.clone()))]
    runtime_data: NodeRuntimeData,
    snapshot: UdpInputSnapshotHandle,
}

fn register_udp_input(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let control = UdpInputControlPlane::new();
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(control.node(), &UdpInputNext::NEXT_NAMES)?;
    hammer_plugin_ip::register_protocol(17, node)?;
    UDP_LOCAL_REGISTRATION
        .set(UdpLocalRegistration {
            inner: Arc::clone(&control.inner),
            consumer: node,
        })
        .map_err(|_| UdpControlError::PortControlAlreadyInitialized)?;
    Ok(node)
}

impl Node for UdpInputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let snapshot = self.snapshot.load();
        udp_input_process_frame(runtime, frame, &snapshot)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(UdpInputTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        udp_input_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

#[inline(always)]
fn udp_input_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    snapshot: &UdpInputSnapshot,
) -> NodeResult {
    let drop_slot = UdpInputNext::Drop.slot() as u16;
    let width = runtime.preferred_frame_batch_width();
    let mut nexts = Vec::with_capacity(frame.len());
    let _ = frame.rewrite_indices_batched(width, |index| {
        match next_slot_for_index(runtime, index, snapshot) {
            Ok(Some(slot)) => {
                nexts.push(slot);
                Ok(Some(index))
            }
            Ok(None) => Ok(None),
            Err(_) => {
                nexts.push(drop_slot);
                Ok(Some(index))
            }
        }
    });
    if !nexts.is_empty() {
        runtime.enqueue_to_next(frame, nexts.as_slice());
    }
    NodeResult::drop()
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    snapshot: &UdpInputSnapshot,
) -> RuntimeResult<Option<u16>> {
    let (version, protocol, source_port, destination_port, cursor, local, remote, payload_len) = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
        let cursor = network.packet_cursor();
        let ip = network.ip();
        let version = match ip.ip_version() {
            Some(4) => UdpIpVersion::V4,
            Some(6) => UdpIpVersion::V6,
            _ => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        let protocol = match ip.ip_protocol() {
            Some(17) => UdpIpProtocol::Udp,
            Some(1) => UdpIpProtocol::Icmpv4,
            Some(6) => UdpIpProtocol::Tcp,
            Some(58) => UdpIpProtocol::Icmpv6,
            Some(value) => UdpIpProtocol::Other(value),
            None => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        if protocol != UdpIpProtocol::Udp {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::WrongProtocol,
                Some(version),
                Some(protocol),
                None,
                None,
            );
        }
        if !valid_udp_cursor(cursor) {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        }

        let header = match read_udp_header(current, cursor.transport_header_offset()) {
            Ok(header) => header,
            Err(_) => {
                drop(buffer);
                return resolve_drop_error(
                    runtime,
                    index,
                    UdpInputError::BadLength,
                    None,
                    None,
                    None,
                    None,
                );
            }
        };
        let source_port = header.source_port();
        let destination_port = header.destination_port();
        let udp_len = header.length();
        if !valid_udp_len(
            cursor.transport_header_offset(),
            cursor.packet_len(),
            udp_len,
        ) {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        }
        let Some(datagram_end) = cursor.transport_header_offset().checked_add(udp_len) else {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        };
        let Some(datagram) = current.get(cursor.transport_header_offset()..datagram_end) else {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::BadLength,
                None,
                None,
                None,
                None,
            );
        };
        if !udp_checksum_is_valid(current, cursor, version, header.checksum(), datagram) {
            drop(buffer);
            return resolve_drop_error(
                runtime,
                index,
                UdpInputError::BadChecksum,
                Some(version),
                Some(protocol),
                Some(source_port),
                Some(destination_port),
            );
        }
        let (local, remote) = udp_socket_addrs(
            current,
            cursor.network_header_offset(),
            version,
            source_port,
            destination_port,
        )
        .ok_or(UdpControlError::HeaderOutOfRange {
            offset: cursor.network_header_offset(),
        })?;
        let payload_len = header.length().checked_sub(UDP_HEADER_LEN).ok_or(
            UdpControlError::HeaderOutOfRange {
                offset: cursor.transport_header_offset(),
            },
        )?;
        (
            version,
            protocol,
            source_port,
            destination_port,
            cursor,
            local,
            remote,
            payload_len,
        )
    };

    refresh_udp_cursor(runtime, index, cursor)?;

    if let Some(main) = crate::worker::UDP_MAIN.get() {
        let payload_offset = cursor
            .transport_header_offset()
            .checked_add(UDP_HEADER_LEN)
            .ok_or(UdpControlError::HeaderOutOfRange {
                offset: cursor.transport_header_offset(),
            })?;
        let return_node = runtime
            .current_node()
            .ok_or(UdpControlError::NodeRuntimeUnavailable)?;
        match main.deliver_datagram(
            runtime,
            index,
            local,
            remote,
            payload_offset,
            payload_len,
            false,
            return_node,
        ) {
            Ok(crate::worker::UdpDelivery::Delivered) => {
                clear_success_metadata(runtime, index)?;
                let slot = UdpInputNext::Drop.slot() as u16;
                add_packet_trace!(
                    runtime,
                    index,
                    UdpInputTrace {
                        version: Some(version),
                        protocol: Some(protocol),
                        source_port: Some(source_port),
                        destination_port: Some(destination_port),
                        error: None,
                        next: slot,
                    },
                )?;
                return Ok(Some(slot));
            }
            Ok(crate::worker::UdpDelivery::FifoFull) => {
                return resolve_drop_error(
                    runtime,
                    index,
                    UdpInputError::FifoFull,
                    Some(version),
                    Some(protocol),
                    Some(source_port),
                    Some(destination_port),
                );
            }
            Ok(crate::worker::UdpDelivery::MigrationQueued) => {
                return Ok(None);
            }
            Ok(crate::worker::UdpDelivery::WrongWorker) => {
                return resolve_drop_error(
                    runtime,
                    index,
                    UdpInputError::WrongWorker,
                    Some(version),
                    Some(protocol),
                    Some(source_port),
                    Some(destination_port),
                );
            }
            Ok(crate::worker::UdpDelivery::NotUdp) => {}
            Err(error) => {
                return resolve_udp_delivery_error(
                    runtime,
                    index,
                    error,
                    version,
                    protocol,
                    source_port,
                    destination_port,
                );
            }
        }
    }

    match snapshot.action(version, destination_port) {
        UdpPortAction::Dispatch(slot) => {
            clear_success_metadata(runtime, index)?;
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(version),
                    protocol: Some(protocol),
                    source_port: Some(source_port),
                    destination_port: Some(destination_port),
                    error: None,
                    next: slot,
                },
            )?;
            Ok(Some(slot))
        }
        UdpPortAction::Punt => {
            clear_success_metadata(runtime, index)?;
            let slot = UdpInputNext::Punt.slot() as u16;
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(version),
                    protocol: Some(protocol),
                    source_port: Some(source_port),
                    destination_port: Some(destination_port),
                    error: None,
                    next: slot,
                },
            )?;
            Ok(Some(slot))
        }
        UdpPortAction::IcmpError => resolve_unknown_port(
            runtime,
            index,
            version,
            protocol,
            source_port,
            destination_port,
        ),
    }
}

#[inline(always)]
fn udp_socket_addrs(
    packet: &[u8],
    network_header_offset: usize,
    version: UdpIpVersion,
    source_port: u16,
    destination_port: u16,
) -> Option<(SocketAddr, SocketAddr)> {
    match version {
        UdpIpVersion::V4 => {
            let source = packet.get(network_header_offset + 12..network_header_offset + 16)?;
            let destination = packet.get(network_header_offset + 16..network_header_offset + 20)?;
            Some((
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(
                        destination[0],
                        destination[1],
                        destination[2],
                        destination[3],
                    )),
                    destination_port,
                ),
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(source[0], source[1], source[2], source[3])),
                    source_port,
                ),
            ))
        }
        UdpIpVersion::V6 => {
            let source = packet.get(network_header_offset + 8..network_header_offset + 24)?;
            let destination = packet.get(network_header_offset + 24..network_header_offset + 40)?;
            Some((
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(destination).ok()?)),
                    destination_port,
                ),
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(source).ok()?)),
                    source_port,
                ),
            ))
        }
    }
}

#[inline(always)]
fn resolve_drop_error(
    runtime: &DataPlaneRuntime,
    index: Index,
    error: UdpInputError,
    version: Option<UdpIpVersion>,
    protocol: Option<UdpIpProtocol>,
    source_port: Option<u16>,
    destination_port: Option<u16>,
) -> RuntimeResult<Option<u16>> {
    set_index_node_error(runtime, index, error)?;
    let slot = UdpInputNext::Drop.slot() as u16;
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version,
            protocol,
            source_port,
            destination_port,
            error: Some(error.code()),
            next: slot,
        },
    )?;
    Ok(Some(slot))
}

#[inline(always)]
fn resolve_udp_delivery_error(
    runtime: &DataPlaneRuntime,
    index: Index,
    error: RuntimeError,
    version: UdpIpVersion,
    protocol: UdpIpProtocol,
    source_port: u16,
    destination_port: u16,
) -> RuntimeResult<Option<u16>> {
    let input_error = match &error {
        RuntimeError::Subsystem { source, .. }
            if source
                .downcast_ref::<crate::worker::UdpTransportError>()
                .is_some() =>
        {
            UdpInputError::SessionMissing
        }
        _ => UdpInputError::FifoNoMemory,
    };
    resolve_drop_error(
        runtime,
        index,
        input_error,
        Some(version),
        Some(protocol),
        Some(source_port),
        Some(destination_port),
    )
}

#[inline(always)]
fn resolve_unknown_port(
    runtime: &DataPlaneRuntime,
    index: Index,
    version: UdpIpVersion,
    protocol: UdpIpProtocol,
    source_port: u16,
    destination_port: u16,
) -> RuntimeResult<Option<u16>> {
    set_index_node_error(runtime, index, UdpInputError::UnknownPort)?;
    {
        let mut buffer = runtime.get_buffer_mut(index)?;
        let opaque = unsafe { transmute::<_, &mut IcmpErrorOpaque>(buffer.opaque2_mut()) };
        opaque.icmp_error = port_unreachable_metadata(version);
    }
    let slot = UdpInputNext::IcmpError.slot() as u16;
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version: Some(version),
            protocol: Some(protocol),
            source_port: Some(source_port),
            destination_port: Some(destination_port),
            error: Some(UdpInputError::UnknownPort.code()),
            next: slot,
        },
    )?;
    Ok(Some(slot))
}

fn clear_success_metadata(runtime: &DataPlaneRuntime, index: Index) -> RuntimeResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    let opaque = unsafe { transmute::<_, &mut IcmpErrorOpaque>(buffer.opaque2_mut()) };
    opaque.icmp_error = None;
    Ok(())
}

#[inline(always)]
fn valid_udp_cursor(cursor: hammer_core::data_plane::BufferPacketCursor) -> bool {
    let transport_header_offset = cursor.transport_header_offset();
    let packet_len = cursor.packet_len();
    cursor.packet_len() != 0
        && transport_header_offset
            .checked_add(UDP_HEADER_LEN)
            .is_some_and(|end| end <= packet_len && u16::try_from(end).is_ok())
}

#[inline(always)]
fn valid_udp_len(transport_header_offset: usize, packet_len: usize, udp_len: usize) -> bool {
    udp_len >= UDP_HEADER_LEN
        && transport_header_offset
            .checked_add(udp_len)
            .is_some_and(|end| end <= packet_len)
}

#[inline(always)]
fn udp_checksum_is_valid(
    packet: &[u8],
    cursor: BufferPacketCursor,
    version: UdpIpVersion,
    checksum: u16,
    datagram: &[u8],
) -> bool {
    let network_header_offset = cursor.network_header_offset();
    match version {
        UdpIpVersion::V4 => {
            if checksum == 0 {
                return true;
            }
            let (Some(source), Some(destination)) = (
                packet.get(network_header_offset + 12..network_header_offset + 16),
                packet.get(network_header_offset + 16..network_header_offset + 20),
            ) else {
                return false;
            };
            let length = (datagram.len() as u16).to_be_bytes();
            internet_checksum_parts(&[source, destination, &[0, 17], &length, datagram]) == 0
        }
        UdpIpVersion::V6 => {
            if checksum == 0 {
                return false;
            }
            let (Some(source), Some(destination)) = (
                packet.get(network_header_offset + 8..network_header_offset + 24),
                packet.get(network_header_offset + 24..network_header_offset + 40),
            ) else {
                return false;
            };
            let length = (datagram.len() as u32).to_be_bytes();
            internet_checksum_parts(&[source, destination, &length, &[0, 0, 0, 17], datagram]) == 0
        }
    }
}

fn refresh_udp_cursor(
    runtime: &DataPlaneRuntime,
    index: Index,
    cursor: BufferPacketCursor,
) -> RuntimeResult<()> {
    let transport_header_offset = cursor.transport_header_offset();
    let transport_payload_offset = transport_header_offset.checked_add(UDP_HEADER_LEN).ok_or(
        UdpControlError::HeaderOutOfRange {
            offset: transport_header_offset,
        },
    )?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(cursor.packet_len())
            .with_network_header(cursor.network_header_offset(), cursor.network_header_len())
            .with_transport_header(transport_header_offset, UDP_HEADER_LEN)
            .with_transport_payload_offset(transport_payload_offset),
    );
    Ok(())
}

#[inline(always)]
fn port_unreachable_metadata(version: UdpIpVersion) -> Option<NonZeroU64> {
    match version {
        UdpIpVersion::V4 => {
            NonZeroU64::new((1u64 << 63) | (4u64 << 48) | (3u64 << 40) | (3u64 << 32))
        }
        UdpIpVersion::V6 => {
            NonZeroU64::new((1u64 << 63) | (6u64 << 48) | (1u64 << 40) | (4u64 << 32))
        }
    }
}

#[inline(always)]
fn read_udp_header(packet: &[u8], offset: usize) -> RuntimeResult<UdpHeader> {
    let end = offset
        .checked_add(size_of::<UdpHeader>())
        .ok_or(UdpControlError::HeaderOutOfRange { offset })?;
    let bytes = packet
        .get(offset..end)
        .ok_or(UdpControlError::HeaderOutOfRange { offset })?;
    // SAFETY: `bytes` has exactly the size of `UdpHeader`; unaligned reads are
    // valid because network headers may start at arbitrary buffer offsets.
    Ok(unsafe { bytes.as_ptr().cast::<UdpHeader>().read_unaligned() })
}

#[cfg(test)]
mod control_tests {
    use hammer_core::data_plane::BufferFrame;
    use hammer_runtime::{
        DataPlaneRuntime, Engine, InternalNode, Node, NodeResult, RuntimeRegistry,
    };

    use super::*;

    struct UdpConsumerNode;

    impl Node for UdpConsumerNode {
        fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }
    }

    impl InternalNode for UdpConsumerNode {}

    fn control() -> (UdpInputControlPlane, NodeId, NodeId) {
        let runtime = DataPlaneRuntime::new(Default::default());
        let consumer = runtime.nodes().register_internal(UdpConsumerNode);
        let owner = runtime.nodes().register_internal(UdpConsumerNode);
        let control = UdpInputControlPlane::new().with_nodes(runtime.nodes().clone());
        let mut control = control;
        control
            .attach_consumer(consumer)
            .expect("attach UDP input consumer");
        (control, owner, consumer)
    }

    fn with_test_engine<R>(operation: impl FnOnce() -> R) -> R {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(Default::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let result = operation();
        Engine::uninstall_current();
        result
    }

    #[test]
    fn register_dst_port_rejects_missing_main_engine() {
        let (control, owner, _) = control();
        let error = control
            .register_dst_port(UdpIpVersion::V4, 443, owner)
            .expect_err("UDP port registration requires the main Engine");

        assert!(matches!(
            error,
            hammer_runtime::RuntimeError::ControlRequiresMainThread
        ));
    }

    #[test]
    fn unregister_rejects_a_different_owner_without_mutating_registration() {
        let (control, owner, other) = control();
        with_test_engine(|| {
            control
                .register_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("register UDP port");

            let error = control
                .unregister_dst_port(UdpIpVersion::V4, 443, other)
                .expect_err("owner mismatch must fail");
            let hammer_runtime::RuntimeError::Subsystem { source, .. } = error else {
                panic!("expected UDP subsystem error");
            };
            assert!(matches!(
                source.downcast_ref::<UdpControlError>(),
                Some(UdpControlError::PortOwnerMismatch {
                    version: UdpIpVersion::V4,
                    port: 443,
                    owner: registered,
                    requested_owner,
                }) if *registered == owner && *requested_owner == other
            ));
            assert!(
                control
                    .unregister_dst_port(UdpIpVersion::V4, 443, owner)
                    .is_ok()
            );
        });
    }

    #[test]
    fn unregister_reports_missing_registration() {
        let (control, owner, _) = control();
        with_test_engine(|| {
            let error = control
                .unregister_dst_port(UdpIpVersion::V6, 443, owner)
                .expect_err("missing UDP port must fail");
            let hammer_runtime::RuntimeError::Subsystem { source, .. } = error else {
                panic!("expected UDP subsystem error");
            };
            assert!(matches!(
                source.downcast_ref::<UdpControlError>(),
                Some(UdpControlError::PortNotRegistered {
                    version: UdpIpVersion::V6,
                    port: 443,
                })
            ));
        });
    }

    #[test]
    fn registration_rejects_reference_count_overflow() {
        let (control, owner, _) = control();
        with_test_engine(|| {
            control
                .register_dst_port(UdpIpVersion::V4, 443, owner)
                .expect("register UDP port");
            control
                .mutate_snapshot(|snapshot| {
                    let table = snapshot.table_mut(UdpIpVersion::V4);
                    table
                        .registrations
                        .get_mut(443)
                        .expect("registered UDP port must retain registration metadata")
                        .references = u32::MAX;
                })
                .expect("force UDP port registration overflow");

            let error = control
                .register_dst_port(UdpIpVersion::V4, 443, owner)
                .expect_err("reference count overflow must fail");
            let hammer_runtime::RuntimeError::Subsystem { source, .. } = error else {
                panic!("expected UDP subsystem error");
            };
            assert!(matches!(
                source.downcast_ref::<UdpControlError>(),
                Some(UdpControlError::PortReferenceCountOverflow {
                    version: UdpIpVersion::V4,
                    port: 443,
                })
            ));
        });
    }

    #[test]
    fn final_release_clears_dispatch_without_reclaiming_graph_slot() {
        let mut table = UdpPortTable::new();
        let owner = NodeId::new(7);
        table.register_port(443, owner, 19);

        assert!(matches!(table.action(443), UdpPortAction::Dispatch(19)));
        table.release_port(443, owner);

        assert!(matches!(table.action(443), UdpPortAction::IcmpError));
        assert_eq!(table.registrations.get(443).map(|entry| entry.slot), None);
        assert_eq!(table.next_by_dst_port.get(443), None);
    }
}
