use std::mem::transmute;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};

use crate::protocol::wire::read_header;
use arc_swap::ArcSwap;
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_runtime::{
    DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData, TraceFormatter, add_packet_trace,
    format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::data_plane::{FeatureArcStartHandle, set_index_node_error};
use hammer_service::opaque::NetworkOpaque;

use super::{IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket, ip_header};

const IP_PROTOCOL_ICMP: u8 = 1;
const IP_PROTOCOL_TCP: u8 = 6;
const IP_PROTOCOL_UDP: u8 = 17;
const IP_PROTOCOL_ICMP6: u8 = 58;
const TCP_HEADER_MIN_LEN: usize = 20;
const ICMP_HEADER_MIN_LEN: usize = 4;

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct IcmpHeader {
    icmp_type: u8,
    code: u8,
    checksum: [u8; 2],
}

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpLocalArc {
    LocalForward,
}

#[hammer_component_macros::node_next]
pub enum IpLocalNext {
    #[next("drop")]
    Drop,
    #[next("drop")]
    Punt,
    #[next("icmp-input")]
    Icmp,
    #[next("ip-reassembly")]
    Reassembly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IpLocalError {
    BadLength,
    BadTransportHeader,
    BadChecksum,
    UnknownProtocol,
}

impl hammer_runtime::node::NodeErrorCode for IpLocalError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IpLocalTraceStage {
    Head,
    Receive,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IpLocalTrace {
    pub stage: IpLocalTraceStage,
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub transport_header_len: usize,
    pub error: Option<u16>,
    pub next: u16,
}

impl IpLocalError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Clone)]
pub struct IpLocalControlPlane {
    inner: Arc<ArcSwap<IpLocalState>>,
}

impl IpLocalControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(IpLocalState::new())),
        }
    }

    #[inline]
    pub fn node(&self) -> IpLocalNode {
        IpLocalNode::new(IpLocalStateHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn receive_node(&self) -> IpReceiveNode {
        IpReceiveNode::new(IpLocalStateHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    fn publish_protocol_slot(&self, protocol: u8, slot: u16) {
        self.inner.rcu(|current| {
            let mut next = IpLocalState::clone(current);
            next.protocol_nexts[protocol as usize] = Some(slot);
            next
        });
    }

    #[inline]
    pub fn unregister_protocol(&self, protocol: u8) -> RuntimeResult<()> {
        self.inner.rcu(|current| {
            let mut next = IpLocalState::clone(current);
            next.protocol_nexts[protocol as usize] = None;
            next
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct IpLocalState {
    protocol_nexts: Box<[Option<u16>; 256]>,
}

impl IpLocalState {
    #[inline]
    fn new() -> Self {
        Self {
            protocol_nexts: Box::new([None; 256]),
        }
    }

    #[inline(always)]
    fn protocol_next_slot(&self, protocol: IpProtocol) -> u16 {
        self.protocol_nexts[ip_protocol_number(protocol) as usize]
            .unwrap_or_else(|| default_protocol_slot(protocol))
    }

    #[inline(always)]
    fn punt_slot(&self) -> u16 {
        NodeNext::slot(IpLocalNext::Punt)
    }

    #[inline(always)]
    fn drop_slot(&self) -> u16 {
        NodeNext::slot(IpLocalNext::Drop)
    }

    #[inline(always)]
    fn reassembly_slot(&self) -> u16 {
        NodeNext::slot(IpLocalNext::Reassembly)
    }
}

#[derive(Clone)]
struct IpLocalStateHandle {
    inner: Arc<ArcSwap<IpLocalState>>,
}

impl IpLocalStateHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<IpLocalState>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<IpLocalState>> {
        self.inner.load()
    }
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip_local,
    role = internal,
    next = IpLocalNext,
    start_arc = IpLocalArc,
)]
pub struct IpLocalNode {
    #[node(default = register_ip_local_runtime(state.clone(), None))]
    runtime_data: NodeRuntimeData,
    state: IpLocalStateHandle,
}

#[hammer_component_macros::graph_node(
    graph = ip,
    init = register_ip_receive,
    role = internal,
    sibling_of = IpLocalNode,
    start_arc = IpLocalArc,
)]
pub struct IpReceiveNode {
    #[node(default = register_ip_local_runtime(state.clone(), None))]
    runtime_data: NodeRuntimeData,
    state: IpLocalStateHandle,
}

static PENDING_IP_LOCAL_CONTROL: OnceLock<IpLocalControlPlane> = OnceLock::new();
static IP_LOCAL_PROTOCOL_REGISTRATION: OnceLock<(IpLocalControlPlane, NodeId)> = OnceLock::new();

pub(crate) fn register_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    let Some((control, consumer)) = IP_LOCAL_PROTOCOL_REGISTRATION.get() else {
        return Err(crate::ip::IpControlError::NodeRuntimeUnavailable {
            operation: crate::ip::IpControlOperation::IpProtocolRegistration,
        }
        .into());
    };
    let slot = nodes.add_node_next_slot(*consumer, node)?;
    control.publish_protocol_slot(protocol, slot);
    Ok(())
}

pub(crate) fn register_ip4_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    register_protocol(nodes, protocol, node)
}

pub(crate) fn register_ip6_protocol(
    nodes: &hammer_runtime::node::NodeRuntime,
    protocol: u8,
    node: NodeId,
) -> RuntimeResult<()> {
    register_protocol(nodes, protocol, node)
}

fn register_ip_local(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let control = IpLocalControlPlane::new();
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(control.node(), &IpLocalNext::NEXT_NAMES)?;
    if IP_LOCAL_PROTOCOL_REGISTRATION
        .set((control.clone(), node))
        .is_err()
    {
        panic!("IP local protocol registration initialized twice");
    }
    if PENDING_IP_LOCAL_CONTROL.set(control).is_err() {
        panic!("IP local control initialized twice");
    }
    Ok(node)
}

fn register_ip_receive(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let control = PENDING_IP_LOCAL_CONTROL.get().cloned().ok_or(
        crate::ip::IpControlError::NodeRuntimeUnavailable {
            operation: crate::ip::IpControlOperation::IpReceiveRegistration,
        },
    )?;
    runtime
        .nodes()
        .try_register_internal(control.receive_node())
}

impl Node for IpLocalNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        let state = self.state.load();
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        process_frame(
            runtime,
            frame,
            &state,
            LocalStage::Head,
            feature_arc.as_ref(),
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_local_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        sync_ip_local_runtime(self.runtime_data, self.state.clone(), feature_arc)?;
        Ok(self.runtime_data)
    }
}

impl Node for IpReceiveNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        let state = self.state.load();
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        process_frame(
            runtime,
            frame,
            &state,
            LocalStage::Receive,
            feature_arc.as_ref(),
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLocalTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_receive_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        sync_ip_local_runtime(self.runtime_data, self.state.clone(), feature_arc)?;
        Ok(self.runtime_data)
    }
}

/// Per-instance state held in the global IP-local registry and shared by
/// `IpLocalNode` (Head stage) and `IpReceiveNode` (Receive stage). The stage
/// is a fixed constant per node and passed by each node's process fn, so the
/// registry only stores the `IpLocalStateHandle` + feature-arc start handle.
///
/// Mirrors the `OnceLock<Mutex<Vec<...>>>` + `NodeRuntimeData::from_usize`
/// pattern used by the sibling migrated nodes (`IpLookupNode`, `IcmpInputNode`,
/// `InterfaceOutputNode`): word 0 of [`NodeRuntimeData`] is the registry slot.
#[derive(Clone)]
struct IpLocalRuntime {
    state: IpLocalStateHandle,
    feature_arc: Option<FeatureArcStartHandle>,
}

fn ip_local_runtimes() -> &'static Mutex<Vec<IpLocalRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<IpLocalRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_ip_local_runtime(
    state: IpLocalStateHandle,
    feature_arc: Option<FeatureArcStartHandle>,
) -> NodeRuntimeData {
    let mut runtimes = ip_local_runtimes()
        .lock()
        .expect("IP local runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IpLocalRuntime { state, feature_arc });
    NodeRuntimeData::from_usize(slot).expect("IP local runtime slot overflow")
}

fn sync_ip_local_runtime(
    data: NodeRuntimeData,
    state: IpLocalStateHandle,
    feature_arc: Option<FeatureArcStartHandle>,
) -> RuntimeResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_local_runtimes().lock().map_err(|_| {
        crate::ip::IpControlError::RuntimeRegistryPoisoned {
            registry: crate::ip::IpRuntimeRegistry::IpLocal,
        }
    })?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or(crate::ip::IpControlError::RuntimeSlotInvalid {
            registry: crate::ip::IpRuntimeRegistry::IpLocal,
            slot,
        })?;
    runtime.state = state;
    runtime.feature_arc = feature_arc;
    Ok(())
}

fn ip_local_runtime(data: NodeRuntimeData) -> RuntimeResult<IpLocalRuntime> {
    let slot = data.usize_word(0)?;
    ip_local_runtimes()
        .lock()
        .map_err(|_| crate::ip::IpControlError::RuntimeRegistryPoisoned {
            registry: crate::ip::IpRuntimeRegistry::IpLocal,
        })?
        .get(slot)
        .cloned()
        .ok_or_else(|| {
            crate::ip::IpControlError::RuntimeSlotInvalid {
                registry: crate::ip::IpRuntimeRegistry::IpLocal,
                slot,
            }
            .into()
        })
}

fn ip_local_process(runtime: &DataPlaneMain, data: NodeRuntimeData, frame: &mut BufferFrame) -> () {
    let state = match ip_local_runtime(data) {
        Ok(state) => state,
        Err(_) => return (),
    };
    let snapshot = state.state.load();
    let feature_arc = state.feature_arc.as_ref();
    process_frame(runtime, frame, &snapshot, LocalStage::Head, feature_arc)
}

fn ip_receive_process(
    runtime: &DataPlaneMain,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
    let state = match ip_local_runtime(data) {
        Ok(state) => state,
        Err(_) => return (),
    };
    let snapshot = state.state.load();
    let feature_arc = state.feature_arc.as_ref();
    process_frame(runtime, frame, &snapshot, LocalStage::Receive, feature_arc)
}

#[derive(Debug, Clone, Copy)]
enum LocalStage {
    Head,
    Receive,
}

impl LocalStage {
    #[inline(always)]
    fn is_head_of_feature_arc(self) -> bool {
        matches!(self, Self::Head | Self::Receive)
    }

    #[inline(always)]
    fn trace_stage(self) -> IpLocalTraceStage {
        match self {
            Self::Head => IpLocalTraceStage::Head,
            Self::Receive => IpLocalTraceStage::Receive,
        }
    }
}

#[inline(always)]
fn process_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    state: &IpLocalState,
    stage: LocalStage,
    feature_arc: Option<&FeatureArcStartHandle>,
) -> () {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match process_index(runtime, index, state, stage, feature_arc) {
            Ok(slot) => slot,
            Err(_) => state.drop_slot(),
        }
    })
}

#[inline(always)]
fn process_index(
    runtime: &DataPlaneMain,
    index: Index,
    state: &IpLocalState,
    stage: LocalStage,
    feature_arc: Option<&FeatureArcStartHandle>,
) -> RuntimeResult<u16> {
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let parsed = match ip_header(current, network.packet_cursor()) {
        Ok(parsed) => parsed,
        Err(_) => {
            drop(buffer);
            set_index_node_error(runtime, index, IpLocalError::BadLength)?;
            let resolved = state.drop_slot();
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: None,
                    protocol: None,
                    transport_header_len: 0,
                    error: Some(IpLocalError::BadLength.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };
    match parsed.input_target {
        IpInputTarget::Drop | IpInputTarget::IcmpError | IpInputTarget::Options => {
            let error = error_for_input(parsed.input_error);
            drop(buffer);
            set_index_node_error(runtime, index, error)?;
            let resolved = state.drop_slot();
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: parsed.transport_header_len,
                    error: Some(error.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
        IpInputTarget::Reassembly => {
            drop(buffer);
            refresh_basic_metadata(runtime, index, &parsed, None)?;
            let resolved = state.reassembly_slot();
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: parsed.transport_header_len,
                    error: None,
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
        IpInputTarget::Punt | IpInputTarget::Lookup | IpInputTarget::LookupMulticast => {}
    }

    let first_len = current.len().min(parsed.packet_len);
    let packet = current
        .get(..first_len)
        .ok_or_else(|| RuntimeError::from(crate::protocol::ip::IpInputError::BadLength))?;
    let transport = match packet.get(parsed.transport_header_offset..parsed.packet_len) {
        Some(transport) => transport,
        None => {
            drop(buffer);
            set_index_node_error(runtime, index, IpLocalError::BadLength)?;
            let resolved = state.drop_slot();
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: 0,
                    error: Some(IpLocalError::BadLength.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };

    let transport_len = match validate_transport(packet, transport, &parsed, stage) {
        Ok(transport_len) => transport_len,
        Err(error) => {
            drop(buffer);
            set_index_node_error(runtime, index, error)?;
            let resolved = state.drop_slot();
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: 0,
                    error: Some(error.code()),
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    };
    drop(buffer);
    refresh_basic_metadata(runtime, index, &parsed, transport_len)?;

    if stage.is_head_of_feature_arc() {
        if let Some(feature_arc) = feature_arc {
            let default_slot = state.protocol_next_slot(parsed.protocol);
            let interface_index = {
                let buffer = runtime.get_buffer(index)?;
                let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
                network.sw_if_index[0]
            };
            let resolved = if interface_index == u32::MAX {
                default_slot
            } else {
                feature_arc.start_for_interface_or(runtime, index, interface_index, default_slot)
            };
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: transport_len.unwrap_or_default(),
                    error: None,
                    next: resolved,
                },
            );
            return Ok(resolved);
        }
    }

    let resolved = state.protocol_next_slot(parsed.protocol);
    let error = if resolved == state.punt_slot() && matches!(parsed.protocol, IpProtocol::Other(_))
    {
        set_index_node_error(runtime, index, IpLocalError::UnknownProtocol)?;
        Some(IpLocalError::UnknownProtocol.code())
    } else {
        None
    };
    let _ = add_packet_trace!(
        runtime,
        index,
        IpLocalTrace {
            stage: stage.trace_stage(),
            version: Some(parsed.version),
            protocol: Some(parsed.protocol),
            transport_header_len: transport_len.unwrap_or_default(),
            error,
            next: resolved,
        },
    );
    Ok(resolved)
}

#[inline(always)]
fn validate_transport(
    packet: &[u8],
    transport: &[u8],
    parsed: &ParsedIpPacket,
    stage: LocalStage,
) -> Result<Option<usize>, IpLocalError> {
    match parsed.protocol {
        IpProtocol::Tcp => {
            let header_len = tcp_header_len(transport)?;
            if stage.is_head_of_feature_arc()
                && l4_checksum(packet, parsed, IP_PROTOCOL_TCP, transport) != 0
            {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(header_len))
        }
        // UDP owns its header, length, and checksum validation in the UDP input
        // node. IP establishes only the IP packet cursor before protocol dispatch.
        IpProtocol::Udp => Ok(None),
        IpProtocol::Icmpv4 => {
            read_header::<IcmpHeader>(transport, 0)
                .map_err(|_| IpLocalError::BadTransportHeader)?;
            if matches!(stage, LocalStage::Head) && internet_checksum(transport) != 0 {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Icmpv6 => {
            read_header::<IcmpHeader>(transport, 0)
                .map_err(|_| IpLocalError::BadTransportHeader)?;
            if matches!(stage, LocalStage::Head)
                && l4_checksum(packet, parsed, IP_PROTOCOL_ICMP6, transport) != 0
            {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Other(_) => Ok(None),
    }
}

#[inline(always)]
fn tcp_header_len(transport: &[u8]) -> Result<usize, IpLocalError> {
    let data_offset = *transport.get(12).ok_or(IpLocalError::BadTransportHeader)?;
    let header_len = usize::from(data_offset >> 4) * 4;
    if header_len < TCP_HEADER_MIN_LEN || transport.len() < header_len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(header_len)
}

#[inline(always)]
fn refresh_basic_metadata(
    runtime: &DataPlaneMain,
    index: Index,
    parsed: &ParsedIpPacket,
    transport_header_len: Option<usize>,
) -> RuntimeResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let transport_header_len = transport_header_len.unwrap_or_default();
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(parsed.packet_len)
            .with_network_header(parsed.network_header_offset, parsed.network_header_len)
            .with_transport_header(parsed.transport_header_offset, transport_header_len)
            .with_transport_payload_offset(parsed.transport_header_offset + transport_header_len),
    );
    Ok(())
}

#[inline(always)]
fn error_for_input(error: IpInputError) -> IpLocalError {
    match error {
        IpInputError::BadChecksum => IpLocalError::BadChecksum,
        _ => IpLocalError::BadLength,
    }
}

#[inline(always)]
fn ip_protocol_number(protocol: IpProtocol) -> u8 {
    match protocol {
        IpProtocol::Icmpv4 => IP_PROTOCOL_ICMP,
        IpProtocol::Tcp => IP_PROTOCOL_TCP,
        IpProtocol::Udp => IP_PROTOCOL_UDP,
        IpProtocol::Icmpv6 => IP_PROTOCOL_ICMP6,
        IpProtocol::Other(value) => value,
    }
}

#[inline(always)]
fn default_protocol_slot(protocol: IpProtocol) -> u16 {
    match protocol {
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => NodeNext::slot(IpLocalNext::Icmp),
        IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Other(_) => {
            NodeNext::slot(IpLocalNext::Punt)
        }
    }
}

#[inline(always)]
fn l4_checksum(_packet: &[u8], parsed: &ParsedIpPacket, protocol: u8, segment: &[u8]) -> u16 {
    match parsed.version {
        IpVersion::V4 => match (parsed.source, parsed.destination) {
            (IpAddr::V4(source), IpAddr::V4(destination)) => internet_checksum_parts(&[
                &source.octets(),
                &destination.octets(),
                &[0, protocol],
                &(segment.len() as u16).to_be_bytes(),
                segment,
            ]),
            _ => return 1,
        },
        IpVersion::V6 => match (parsed.source, parsed.destination) {
            (IpAddr::V6(source), IpAddr::V6(destination)) => internet_checksum_parts(&[
                &source.octets(),
                &destination.octets(),
                &(segment.len() as u32).to_be_bytes(),
                &[0, 0, 0, protocol],
                segment,
            ]),
            _ => return 1,
        },
    }
}
