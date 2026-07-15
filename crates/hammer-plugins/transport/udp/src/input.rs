use std::mem::{size_of, transmute};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, SecondaryOpaque,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_core::protocol::transport::UdpHeader;
use hammer_core::protocol::wire::read_header;
use hammer_runtime::{
    DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace,
};

use hammer_core::protocol::ip::{IpProtocol, IpVersion};
use hammer_service::data_plane::set_index_node_error_code;
use hammer_service::opaque::NetworkOpaque;
use hammer_service::trace::codec::{
    TraceDecodeCursor, put_option_ip_protocol, put_option_ip_version, put_option_u16, put_u16,
};

const UDP_HEADER_LEN: usize = 8;
const UDP_PORT_COUNT: usize = u16::MAX as usize + 1;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<IcmpErrorMetadata>,
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
pub enum UdpInputError {
    BadLength,
    WrongProtocol,
    UnknownPort,
}

impl UdpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub error: Option<u16>,
    pub next: u16,
}

impl UdpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            source_port: cursor.read_option_u16()?,
            destination_port: cursor.read_option_u16()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for UdpInputTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_u16(out, self.source_port);
        put_option_u16(out, self.destination_port);
        put_option_u16(out, self.error);
        put_u16(out, self.next);
    }
}

fn format_udp_input_trace(bytes: &[u8]) -> String {
    match UdpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("UdpInputTrace invalid={bytes:?}"),
    }
}

pub struct UdpInputControlPlane {
    inner: Arc<ArcSwap<UdpInputSnapshot>>,
    next: [NodeId; UdpInputNext::COUNT],
    nodes: Option<hammer_runtime::node::NodeRuntime>,
    consumer: Option<NodeId>,
}

impl UdpInputControlPlane {
    #[inline]
    pub fn new(nexts: [NodeId; UdpInputNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(UdpInputSnapshot::new())),
            next: nexts,
            nodes: None,
            consumer: None,
        }
    }

    #[inline]
    pub fn with_nodes(mut self, nodes: hammer_runtime::node::NodeRuntime) -> Self {
        self.nodes = Some(nodes);
        self
    }

    pub fn attach_consumer(&mut self, consumer: NodeId) -> CoreResult<()> {
        if self.nodes.is_none() {
            return Err(CoreError::internal(
                "udp input attach requires node runtime",
            ));
        }
        self.consumer = Some(consumer);
        Ok(())
    }

    #[inline]
    pub fn register_port(&self, port: u16, node: NodeId) -> CoreResult<u16> {
        let consumer = self.consumer.ok_or_else(|| {
            CoreError::internal("udp input register_port requires attach_consumer")
        })?;
        let nodes = self
            .nodes
            .as_ref()
            .ok_or_else(|| CoreError::internal("udp input register_port requires node runtime"))?;
        let slot = nodes.add_node_next_slot(consumer, node)?;
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_port(port, slot);
            next
        });
        Ok(slot)
    }

    #[inline]
    pub fn register_punt_port(&self, port: u16) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_punt_port(port);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn unregister_port(&self, port: u16) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.unregister_port(port);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> UdpInputNode {
        UdpInputNode::new(
            UdpInputSnapshotHandle::new(Arc::clone(&self.inner)),
            self.next,
        )
    }
}

#[derive(Clone)]
struct UdpInputSnapshot {
    ports: Box<[UdpPortAction]>,
}

impl UdpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            ports: vec![UdpPortAction::IcmpError; UDP_PORT_COUNT].into_boxed_slice(),
        }
    }

    #[inline(always)]
    fn register_port(&mut self, port: u16, next: u16) {
        self.ports[port as usize] = UdpPortAction::Dispatch(next);
    }

    #[inline(always)]
    fn register_punt_port(&mut self, port: u16) {
        self.ports[port as usize] = UdpPortAction::Punt;
    }

    #[inline(always)]
    fn unregister_port(&mut self, port: u16) {
        self.ports[port as usize] = UdpPortAction::IcmpError;
    }

    #[inline(always)]
    fn action(&self, port: u16) -> UdpPortAction {
        self.ports[port as usize].clone()
    }
}

#[derive(Debug, Clone, Copy)]
enum UdpPortAction {
    IcmpError,
    Punt,
    Dispatch(u16),
}

#[derive(Clone)]
struct UdpInputSnapshotHandle {
    inner: Arc<ArcSwap<UdpInputSnapshot>>,
}

impl UdpInputSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<UdpInputSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<UdpInputSnapshot>> {
        self.inner.load()
    }
}

#[derive(Clone)]
struct UdpInputRuntime {
    snapshot: UdpInputSnapshotHandle,
}

fn udp_input_runtimes() -> &'static Mutex<Vec<UdpInputRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<UdpInputRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_udp_input_runtime(snapshot: UdpInputSnapshotHandle) -> NodeRuntimeData {
    let mut runtimes = udp_input_runtimes()
        .lock()
        .expect("UDP input runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(UdpInputRuntime { snapshot });
    NodeRuntimeData::from_usize(slot).expect("UDP input runtime slot overflow")
}

fn udp_input_runtime(data: NodeRuntimeData) -> CoreResult<UdpInputRuntime> {
    let slot = data.usize_word(0)?;
    udp_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("UDP input runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("UDP input runtime slot is invalid"))
}

fn udp_input_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match udp_input_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    let snapshot = state.snapshot.load();
    udp_input_process_frame(runtime, frame, &snapshot)
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

fn register_udp_input(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    let control = UdpInputControlPlane::new([NodeId::new(0); UdpInputNext::COUNT]);
    runtime
        .nodes()
        .try_register_internal_with_next_names(control.node(), &UdpInputNext::NEXT_NAMES)
}

impl Node for UdpInputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let snapshot = self.snapshot.load();
        udp_input_process_frame(runtime, frame, &snapshot)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_udp_input_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        udp_input_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
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
    let mut nexts = [drop_slot; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut count = 0usize;
    for index in frame.iter_indices() {
        let slot = match next_slot_for_index(runtime, *index, snapshot) {
            Ok(Some(slot)) => slot,
            _ => drop_slot,
        };
        nexts[count] = slot;
        count += 1;
    }
    runtime.enqueue_to_next(frame, &nexts[..count]);
    NodeResult::drop()
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    snapshot: &UdpInputSnapshot,
) -> CoreResult<Option<u16>> {
    let (version, protocol, source_port, destination_port) = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
        let cursor = network.packet_cursor();
        let ip = network.ip();
        let version = match ip.ip_version() {
            Some(4) => IpVersion::V4,
            Some(6) => IpVersion::V6,
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
            Some(17) => IpProtocol::Udp,
            Some(1) => IpProtocol::Icmpv4,
            Some(6) => IpProtocol::Tcp,
            Some(58) => IpProtocol::Icmpv6,
            Some(value) => IpProtocol::Other(value),
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
        if protocol != IpProtocol::Udp {
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

        let header = match read_header::<UdpHeader>(current, cursor.transport_header_offset()) {
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
        if !valid_udp_len(
            cursor.transport_header_offset(),
            cursor.packet_len(),
            header.length(),
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
        (version, protocol, source_port, destination_port)
    };

    match snapshot.action(destination_port) {
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
fn resolve_drop_error(
    runtime: &DataPlaneRuntime,
    index: Index,
    error: UdpInputError,
    version: Option<IpVersion>,
    protocol: Option<IpProtocol>,
    source_port: Option<u16>,
    destination_port: Option<u16>,
) -> CoreResult<Option<u16>> {
    set_index_node_error_code(runtime, index, error.code())?;
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
fn resolve_unknown_port(
    runtime: &DataPlaneRuntime,
    index: Index,
    version: IpVersion,
    protocol: IpProtocol,
    source_port: u16,
    destination_port: u16,
) -> CoreResult<Option<u16>> {
    set_index_node_error_code(runtime, index, UdpInputError::UnknownPort.code())?;
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

fn clear_success_metadata(runtime: &DataPlaneRuntime, index: Index) -> CoreResult<()> {
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
        && cursor.transport_header_len() >= UDP_HEADER_LEN
        && cursor.transport_payload_offset() >= transport_header_offset + UDP_HEADER_LEN
        && transport_header_offset
            .checked_add(UDP_HEADER_LEN)
            .is_some_and(|end| end <= packet_len)
}

#[inline(always)]
fn valid_udp_len(transport_header_offset: usize, packet_len: usize, udp_len: usize) -> bool {
    udp_len >= UDP_HEADER_LEN
        && transport_header_offset
            .checked_add(udp_len)
            .is_some_and(|end| end <= packet_len)
}

#[inline(always)]
fn port_unreachable_metadata(version: IpVersion) -> Option<IcmpErrorMetadata> {
    match version {
        IpVersion::V4 => Some(IcmpErrorMetadata::ipv4_destination_unreachable(3, 0)),
        IpVersion::V6 => Some(IcmpErrorMetadata::ipv6_port_unreachable()),
    }
}
