use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextStorage, NodeProcessFn,
    NodeResult, NodeRuntimeData, NodeVectorDispatch, PacketTrace, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_infra::boxed::Slice;
use hammer_runtime::app::{AppContext, AppFlowId};

use crate::app::{AppIngressRegistry, AppIngressTarget};
use crate::data_plane::set_index_node_error_code;
use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
};

const UDP_HEADER_LEN: usize = 8;
const UDP_PORT_COUNT: usize = u16::MAX as usize + 1;

#[hammer_component_macros::node_next]
pub enum UdpInputNext {
    Drop,
    Punt,
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
    pub next: NodeId,
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
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for UdpInputTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_u16(out, self.source_port);
        put_option_u16(out, self.destination_port);
        put_option_u16(out, self.error);
        put_node(out, self.next);
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
}

impl UdpInputControlPlane {
    #[inline]
    pub fn new(nexts: [NodeId; UdpInputNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(UdpInputSnapshot::new())),
            next: nexts,
        }
    }

    #[inline]
    pub fn register_port(&self, port: u16, node: NodeId) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_port(port, node);
            next
        });
        Ok(())
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
    pub fn register_app(&self, port: u16, registration: UdpAppRegistration) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = UdpInputSnapshot::clone(current);
            next.register_app(port, registration.clone());
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
    ports: Slice<UdpPortAction>,
    app_registry: AppIngressRegistry<u16>,
}

impl UdpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            ports: Slice::from_elem(UDP_PORT_COUNT, UdpPortAction::IcmpError),
            app_registry: AppIngressRegistry::new(),
        }
    }

    #[inline(always)]
    fn register_port(&mut self, port: u16, node: NodeId) {
        self.ports[port as usize] = UdpPortAction::Dispatch(node);
    }

    #[inline(always)]
    fn register_punt_port(&mut self, port: u16) {
        self.ports[port as usize] = UdpPortAction::Punt;
    }

    #[inline(always)]
    fn register_app(&mut self, port: u16, registration: UdpAppRegistration) {
        self.ports[port as usize] = UdpPortAction::App;
        self.app_registry.insert(port, registration.into_target());
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

#[derive(Debug, Clone)]
enum UdpPortAction {
    IcmpError,
    Punt,
    Dispatch(NodeId),
    App,
}

#[derive(Clone)]
pub struct UdpAppRegistration {
    target: AppIngressTarget,
}

impl UdpAppRegistration {
    #[inline]
    pub fn new(app: AppContext, flow: AppFlowId) -> Self {
        Self {
            target: AppIngressTarget::new(app, flow),
        }
    }

    #[inline]
    fn into_target(self) -> AppIngressTarget {
        self.target
    }

    #[inline]
    fn target(&self) -> &AppIngressTarget {
        &self.target
    }
}

impl std::fmt::Debug for UdpAppRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpAppRegistration")
            .field("flow", &self.target().flow().value())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
enum UdpInputNextKey<'a> {
    Punt(&'a [NodeId; UdpInputNext::COUNT]),
    IcmpError(&'a [NodeId; UdpInputNext::COUNT]),
}

impl NodeNextStorage<UdpInputNextKey<'_>> for UdpInputSnapshot {
    #[inline(always)]
    fn next(&self, key: UdpInputNextKey<'_>) -> NodeId {
        match key {
            UdpInputNextKey::Punt(next) => next[UdpInputNext::Punt.slot()],
            UdpInputNextKey::IcmpError(next) => next[UdpInputNext::IcmpError.slot()],
        }
    }
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

#[hammer_component_macros::node(role = internal, next = UdpInputNext)]
pub struct UdpInputNode {
    #[node(default = register_udp_input_runtime(snapshot.clone()))]
    runtime_data: NodeRuntimeData,
    snapshot: UdpInputSnapshotHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for UdpInputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| next_node_for_index(runtime, index, &snapshot, &next),
        )?;
        self.cached_next = cached_next;
        Ok(result)
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
) -> CoreResult<NodeResult> {
    let state = udp_input_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = UdpInputNode::runtime_nexts(runtime)?;
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        next_node_for_index(runtime, index, &snapshot, &next)
    })?;
    Ok(result)
}

#[derive(Debug, Clone, Copy)]
struct ParsedUdpInput {
    version: IpVersion,
    protocol: IpProtocol,
    source_port: u16,
    destination_port: u16,
}

#[derive(Debug, Clone, Copy)]
struct UdpInputTraceContext {
    version: Option<IpVersion>,
    protocol: Option<IpProtocol>,
    source_port: Option<u16>,
    destination_port: Option<u16>,
}

impl UdpInputTraceContext {
    #[inline(always)]
    const fn empty() -> Self {
        Self {
            version: None,
            protocol: None,
            source_port: None,
            destination_port: None,
        }
    }

    #[inline(always)]
    const fn protocol(version: IpVersion, protocol: IpProtocol) -> Self {
        Self {
            version: Some(version),
            protocol: Some(protocol),
            source_port: None,
            destination_port: None,
        }
    }
}

#[inline(always)]
fn next_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    snapshot: &UdpInputSnapshot,
    next: &[NodeId; UdpInputNext::COUNT],
) -> CoreResult<Option<NodeId>> {
    let parsed = match parse_udp_input(runtime, index)? {
        Ok(parsed) => parsed,
        Err(UdpInputParseError::BadLength) => {
            return resolve_drop_error(
                runtime,
                index,
                next,
                UdpInputError::BadLength,
                UdpInputTraceContext::empty(),
            );
        }
        Err(UdpInputParseError::WrongProtocol { version, protocol }) => {
            return resolve_drop_error(
                runtime,
                index,
                next,
                UdpInputError::WrongProtocol,
                UdpInputTraceContext::protocol(version, protocol),
            );
        }
    };

    match snapshot.action(parsed.destination_port) {
        UdpPortAction::Dispatch(node) => {
            clear_success_metadata(runtime, index)?;
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    source_port: Some(parsed.source_port),
                    destination_port: Some(parsed.destination_port),
                    error: None,
                    next: node,
                },
            )?;
            Ok(Some(node))
        }
        UdpPortAction::Punt => {
            clear_success_metadata(runtime, index)?;
            let resolved = NodeNextStorage::next(snapshot, UdpInputNextKey::Punt(next));
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    source_port: Some(parsed.source_port),
                    destination_port: Some(parsed.destination_port),
                    error: None,
                    next: resolved,
                },
            )?;
            Ok(Some(resolved))
        }
        UdpPortAction::App => {
            let registration = snapshot
                .app_registry
                .get(&parsed.destination_port)
                .ok_or_else(|| {
                    CoreError::internal(format!(
                        "UDP app registration missing for port {}",
                        parsed.destination_port
                    ))
                })?;
            clear_success_metadata(runtime, index)?;
            dispatch_udp_input_to_app(runtime, index, &registration)?;
            add_packet_trace!(
                runtime,
                index,
                UdpInputTrace {
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    source_port: Some(parsed.source_port),
                    destination_port: Some(parsed.destination_port),
                    error: None,
                    next: NodeId::new(0),
                },
            )?;
            Ok(None)
        }
        UdpPortAction::IcmpError => resolve_unknown_port(runtime, index, next, parsed, snapshot),
    }
}

#[inline(always)]
fn resolve_drop_error(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; UdpInputNext::COUNT],
    error: UdpInputError,
    trace: UdpInputTraceContext,
) -> CoreResult<Option<NodeId>> {
    set_index_node_error_code(runtime, index, error.code())?;
    let resolved = NodeNextStorage::next(next, UdpInputNext::Drop);
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version: trace.version,
            protocol: trace.protocol,
            source_port: trace.source_port,
            destination_port: trace.destination_port,
            error: Some(error.code()),
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn resolve_unknown_port(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; UdpInputNext::COUNT],
    parsed: ParsedUdpInput,
    snapshot: &UdpInputSnapshot,
) -> CoreResult<Option<NodeId>> {
    set_index_node_error_code(runtime, index, UdpInputError::UnknownPort.code())?;
    runtime.with_metadata_mut(index, |metadata| {
        metadata.icmp_error = port_unreachable_metadata(parsed.version);
    })?;
    let resolved = NodeNextStorage::next(snapshot, UdpInputNextKey::IcmpError(next));
    add_packet_trace!(
        runtime,
        index,
        UdpInputTrace {
            version: Some(parsed.version),
            protocol: Some(parsed.protocol),
            source_port: Some(parsed.source_port),
            destination_port: Some(parsed.destination_port),
            error: Some(UdpInputError::UnknownPort.code()),
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn clear_success_metadata(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    buffer.metadata_mut().icmp_error = None;
    Ok(())
}

#[inline(always)]
fn dispatch_udp_input_to_app(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    target: &AppIngressTarget,
) -> CoreResult<()> {
    target.post_recv_cqe(runtime, index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpInputParseError {
    BadLength,
    WrongProtocol {
        version: IpVersion,
        protocol: IpProtocol,
    },
}

#[inline(always)]
fn parse_udp_input(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<Result<ParsedUdpInput, UdpInputParseError>> {
    let (version, protocol, source_port, destination_port) = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        let cursor = buffer.packet_cursor();
        let parsed =
            match parse_ip_packet_with_chain_len(current, buffer.total_len_not_including_first()) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(Err(UdpInputParseError::BadLength)),
            };
        if parsed.protocol != IpProtocol::Udp {
            return Ok(Err(UdpInputParseError::WrongProtocol {
                version: parsed.version,
                protocol: parsed.protocol,
            }));
        }
        if parsed.input_error != IpInputError::None
            || !valid_udp_cursor(parsed.packet_len, parsed.transport_header_offset, cursor)
        {
            return Ok(Err(UdpInputParseError::BadLength));
        }

        let Some(header) = current.get(
            cursor.transport_header_offset()..cursor.transport_header_offset() + UDP_HEADER_LEN,
        ) else {
            return Ok(Err(UdpInputParseError::BadLength));
        };
        let source_port = u16::from_be_bytes([header[0], header[1]]);
        let destination_port = u16::from_be_bytes([header[2], header[3]]);
        let udp_len = u16::from_be_bytes([header[4], header[5]]) as usize;
        if !valid_udp_len(
            cursor.transport_header_offset(),
            cursor.packet_len(),
            udp_len,
        ) {
            return Ok(Err(UdpInputParseError::BadLength));
        }
        (
            parsed.version,
            parsed.protocol,
            source_port,
            destination_port,
        )
    };

    Ok(Ok(ParsedUdpInput {
        version,
        protocol,
        source_port,
        destination_port,
    }))
}

#[inline(always)]
fn valid_udp_cursor(
    packet_len: usize,
    transport_header_offset: usize,
    cursor: hammer_adapter::BufferPacketCursor,
) -> bool {
    cursor.packet_len() == packet_len
        && cursor.transport_header_offset() == transport_header_offset
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
