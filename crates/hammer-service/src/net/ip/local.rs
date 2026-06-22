use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, Node, NodeId, NodeNextStorage,
    NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch, PacketTrace, SocksAddr,
    TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::data_plane::{FeatureArcStartHandle, set_index_node_error_code};
use crate::net::{DpoType, FibLookupResult, FibTableHandle};

use super::{IpInputError, IpInputTarget, IpProtocol, IpVersion, ParsedIpPacket};
use super::{network_for_protocol, parse_ip_packet_with_chain_len};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
    put_u8, put_usize,
};

const IP_PROTOCOL_ICMP: u8 = 1;
const IP_PROTOCOL_TCP: u8 = 6;
const IP_PROTOCOL_UDP: u8 = 17;
const IP_PROTOCOL_ICMP6: u8 = 58;
const TCP_HEADER_MIN_LEN: usize = 20;
const UDP_HEADER_LEN: usize = 8;
const ICMP_HEADER_MIN_LEN: usize = 4;

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IpLocalArc {
    LocalForward,
}

#[hammer_component_macros::node_next]
pub enum IpLocalNext {
    Drop,
    Punt,
    Tcp,
    Udp,
    Icmp,
    Reassembly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpLocalError {
    BadLength,
    BadTransportHeader,
    BadChecksum,
    SourceCheckFailed,
    UnknownProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpLocalTraceStage {
    Head,
    Receive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpLocalTrace {
    pub stage: IpLocalTraceStage,
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub transport_header_len: usize,
    pub error: Option<u16>,
    pub next: NodeId,
}

impl IpLocalTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let stage = match cursor.read_u8()? {
            0 => IpLocalTraceStage::Head,
            1 => IpLocalTraceStage::Receive,
            _ => return None,
        };
        let trace = Self {
            stage,
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            transport_header_len: cursor.read_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpLocalTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_u8(
            out,
            match self.stage {
                IpLocalTraceStage::Head => 0,
                IpLocalTraceStage::Receive => 1,
            },
        );
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_usize(out, self.transport_header_len);
        put_option_u16(out, self.error);
        put_node(out, self.next);
    }
}

fn format_ip_local_trace(bytes: &[u8]) -> String {
    match IpLocalTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IpLocalTrace invalid={bytes:?}"),
    }
}

impl IpLocalError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone)]
pub enum IpLocalSourceCheck {
    Disabled,
    ReverseFib(FibTableHandle),
}

impl Default for IpLocalSourceCheck {
    #[inline]
    fn default() -> Self {
        Self::Disabled
    }
}

pub struct IpLocalControlPlane {
    inner: Arc<ArcSwap<IpLocalState>>,
    next: [NodeId; IpLocalNext::COUNT],
}

impl IpLocalControlPlane {
    #[inline]
    pub fn new(next: [NodeId; IpLocalNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(IpLocalState::new())),
            next,
        }
    }

    #[inline]
    pub fn with_source_check(self, source_check: IpLocalSourceCheck) -> Self {
        self.publish_source_check(source_check);
        self
    }

    #[inline]
    pub fn node(&self) -> IpLocalNode {
        IpLocalNode::new(IpLocalStateHandle::new(Arc::clone(&self.inner)), self.next)
    }

    #[inline]
    pub fn receive_node(&self) -> IpReceiveNode {
        IpReceiveNode::new(IpLocalStateHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn register_protocol(&self, protocol: u8, node: NodeId) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IpLocalState::clone(current);
            next.protocol_nexts[protocol as usize] = Some(node);
            next
        });
        Ok(())
    }

    #[inline]
    pub fn unregister_protocol(&self, protocol: u8) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IpLocalState::clone(current);
            next.protocol_nexts[protocol as usize] = None;
            next
        });
        Ok(())
    }

    #[inline]
    pub fn publish_source_check(&self, source_check: IpLocalSourceCheck) {
        self.inner.rcu(|current| {
            let mut next = IpLocalState::clone(current);
            next.source_check = source_check.clone();
            next
        });
    }
}

#[derive(Debug, Clone)]
struct IpLocalState {
    protocol_nexts: Box<[Option<NodeId>; 256]>,
    source_check: IpLocalSourceCheck,
}

impl IpLocalState {
    #[inline]
    fn new() -> Self {
        Self {
            protocol_nexts: Box::new([None; 256]),
            source_check: IpLocalSourceCheck::Disabled,
        }
    }

    #[inline(always)]
    fn protocol_next(&self, next: &[NodeId; IpLocalNext::COUNT], protocol: IpProtocol) -> NodeId {
        NodeNextStorage::next(self, IpLocalNextKey::Protocol { next, protocol })
    }

    #[inline(always)]
    fn punt_next(&self, next: &[NodeId; IpLocalNext::COUNT]) -> NodeId {
        NodeNextStorage::next(self, IpLocalNextKey::Punt(next))
    }

    #[inline(always)]
    fn drop_next(&self, next: &[NodeId; IpLocalNext::COUNT]) -> NodeId {
        NodeNextStorage::next(self, IpLocalNextKey::Drop(next))
    }

    #[inline(always)]
    fn reassembly_next(&self, next: &[NodeId; IpLocalNext::COUNT]) -> NodeId {
        NodeNextStorage::next(self, IpLocalNextKey::Reassembly(next))
    }
}

#[derive(Debug, Clone, Copy)]
enum IpLocalNextKey<'a> {
    Drop(&'a [NodeId; IpLocalNext::COUNT]),
    Punt(&'a [NodeId; IpLocalNext::COUNT]),
    Reassembly(&'a [NodeId; IpLocalNext::COUNT]),
    Protocol {
        next: &'a [NodeId; IpLocalNext::COUNT],
        protocol: IpProtocol,
    },
}

impl NodeNextStorage<IpLocalNextKey<'_>> for IpLocalState {
    #[inline(always)]
    fn next(&self, key: IpLocalNextKey<'_>) -> NodeId {
        match key {
            IpLocalNextKey::Drop(next) => next[IpLocalNext::Drop.slot()],
            IpLocalNextKey::Punt(next) => next[IpLocalNext::Punt.slot()],
            IpLocalNextKey::Reassembly(next) => next[IpLocalNext::Reassembly.slot()],
            IpLocalNextKey::Protocol { next, protocol } => self.protocol_nexts
                [ip_protocol_number(protocol) as usize]
                .unwrap_or_else(|| default_protocol_next(next, protocol)),
        }
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

#[hammer_component_macros::node(role = internal, next = IpLocalNext, start_arc = IpLocalArc)]
pub struct IpLocalNode {
    #[node(default = register_ip_local_runtime(state.clone()))]
    runtime_data: NodeRuntimeData,
    state: IpLocalStateHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

#[hammer_component_macros::node(role = internal, sibling_of = IpLocalNode, start_arc = IpLocalArc)]
pub struct IpReceiveNode {
    #[node(default = register_ip_local_runtime(state.clone()))]
    runtime_data: NodeRuntimeData,
    state: IpLocalStateHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for IpLocalNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let state = self.state.load();
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        let next = Self::runtime_nexts(runtime)?;
        process_frame(
            runtime,
            frame,
            &state,
            next,
            LocalStage::Head,
            feature_arc.as_ref(),
            &mut self.cached_next,
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_local_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_local_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_ip_local_runtime(
            self.runtime_data,
            self.feature_arc.as_ref().map(|arc| arc.start_handle()),
        )?;
        Ok(self.runtime_data)
    }
}

impl Node for IpReceiveNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let state = self.state.load();
        let feature_arc = self.feature_arc.as_ref().map(|arc| arc.start_handle());
        let next = Self::runtime_nexts(runtime)?;
        process_frame(
            runtime,
            frame,
            &state,
            next,
            LocalStage::Receive,
            feature_arc.as_ref(),
            &mut self.cached_next,
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_local_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_receive_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_ip_local_runtime(
            self.runtime_data,
            self.feature_arc.as_ref().map(|arc| arc.start_handle()),
        )?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct IpLocalRuntime {
    state: IpLocalStateHandle,
    feature_arc: Option<FeatureArcStartHandle>,
}

fn ip_local_runtimes() -> &'static Mutex<Vec<IpLocalRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<IpLocalRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_ip_local_runtime(state: IpLocalStateHandle) -> NodeRuntimeData {
    let mut runtimes = ip_local_runtimes()
        .lock()
        .expect("IP local runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IpLocalRuntime {
        state,
        feature_arc: None,
    });
    NodeRuntimeData::from_usize(slot).expect("IP local runtime slot overflow")
}

fn sync_ip_local_runtime(
    data: NodeRuntimeData,
    feature_arc: Option<FeatureArcStartHandle>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = ip_local_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP local runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("IP local runtime slot is invalid"))?;
    runtime.feature_arc = feature_arc;
    Ok(())
}

fn ip_local_runtime(data: NodeRuntimeData) -> CoreResult<IpLocalRuntime> {
    let slot = data.usize_word(0)?;
    ip_local_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP local runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("IP local runtime slot is invalid"))
}

fn ip_local_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = ip_local_runtime(data)?;
    let feature_arc = state.feature_arc.clone();
    let state = state.state.load();
    let mut cached_next = None;
    process_frame(
        runtime,
        frame,
        &state,
        IpLocalNode::runtime_nexts(runtime)?,
        LocalStage::Head,
        feature_arc.as_ref(),
        &mut cached_next,
    )
}

fn ip_receive_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = ip_local_runtime(data)?;
    let feature_arc = state.feature_arc.clone();
    let state = state.state.load();
    let mut cached_next = None;
    process_frame(
        runtime,
        frame,
        &state,
        IpReceiveNode::runtime_nexts(runtime)?,
        LocalStage::Receive,
        feature_arc.as_ref(),
        &mut cached_next,
    )
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
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    state: &IpLocalState,
    next: [NodeId; IpLocalNext::COUNT],
    stage: LocalStage,
    feature_arc: Option<&FeatureArcStartHandle>,
    cached_next: &mut Option<NodeId>,
) -> CoreResult<NodeResult> {
    let (result, next_cache) =
        NodeVectorDispatch::new(*cached_next).route_frame_index(runtime, frame, |index| {
            Ok(Some(process_index(
                runtime,
                index,
                state,
                &next,
                stage,
                feature_arc,
            )?))
        })?;
    *cached_next = next_cache;
    Ok(result)
}

#[inline(always)]
fn process_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    state: &IpLocalState,
    next: &[NodeId; IpLocalNext::COUNT],
    stage: LocalStage,
    feature_arc: Option<&FeatureArcStartHandle>,
) -> CoreResult<NodeId> {
    let packet = packet_bytes(runtime, index)?;
    let parsed = match parse_ip_packet_with_chain_len(&packet, 0) {
        Ok(parsed) => parsed,
        Err(_) => {
            set_index_node_error_code(runtime, index, IpLocalError::BadLength.code())?;
            let resolved = state.drop_next(next);
            add_packet_trace!(
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
            )?;
            return Ok(resolved);
        }
    };
    match parsed.input_target {
        IpInputTarget::Drop | IpInputTarget::IcmpError | IpInputTarget::Options => {
            let error = error_for_input(parsed.input_error).code();
            set_index_node_error_code(runtime, index, error)?;
            let resolved = state.drop_next(next);
            add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: parsed.transport_header_len,
                    error: Some(error),
                    next: resolved,
                },
            )?;
            return Ok(resolved);
        }
        IpInputTarget::Reassembly => {
            refresh_basic_metadata(runtime, index, &parsed, None)?;
            let resolved = state.reassembly_next(next);
            add_packet_trace!(
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
            )?;
            return Ok(resolved);
        }
        IpInputTarget::Punt | IpInputTarget::Lookup | IpInputTarget::LookupMulticast => {}
    }

    let packet = packet
        .get(..parsed.packet_len)
        .ok_or_else(|| CoreError::internal("invalid local packet length"))?;
    let transport = match packet.get(parsed.transport_header_offset..parsed.packet_len) {
        Some(transport) => transport,
        None => {
            set_index_node_error_code(runtime, index, IpLocalError::BadLength.code())?;
            let resolved = state.drop_next(next);
            add_packet_trace!(
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
            )?;
            return Ok(resolved);
        }
    };

    let transport_len = match validate_transport(packet, transport, &parsed, stage) {
        Ok(transport_len) => transport_len,
        Err(error) => {
            set_index_node_error_code(runtime, index, error.code())?;
            let resolved = state.drop_next(next);
            add_packet_trace!(
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
            )?;
            return Ok(resolved);
        }
    };
    refresh_basic_metadata(runtime, index, &parsed, transport_len)?;

    if stage.is_head_of_feature_arc() {
        if !source_check_passes(state, &parsed) {
            set_index_node_error_code(runtime, index, IpLocalError::SourceCheckFailed.code())?;
            let resolved = state.drop_next(next);
            add_packet_trace!(
                runtime,
                index,
                IpLocalTrace {
                    stage: stage.trace_stage(),
                    version: Some(parsed.version),
                    protocol: Some(parsed.protocol),
                    transport_header_len: transport_len.unwrap_or_default(),
                    error: Some(IpLocalError::SourceCheckFailed.code()),
                    next: resolved,
                },
            )?;
            return Ok(resolved);
        }
        if let Some(feature_arc) = feature_arc {
            let next = state.protocol_next(next, parsed.protocol);
            let interface_index = {
                let buffer = runtime.get_buffer(index)?;
                let network = unsafe {
                    std::mem::transmute::<
                        &hammer_adapter::PrimaryOpaque,
                        &hammer_adapter::NetworkOpaque,
                    >(buffer.opaque())
                };
                network.sw_if_index[0]
            };
            let resolved = if interface_index == 0 {
                next
            } else {
                feature_arc.start_for_interface_or(interface_index, next)
            };
            add_packet_trace!(
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
            )?;
            return Ok(resolved);
        }
    }

    let resolved = state.protocol_next(next, parsed.protocol);
    if resolved == state.punt_next(next) && matches!(parsed.protocol, IpProtocol::Other(_)) {
        set_index_node_error_code(runtime, index, IpLocalError::UnknownProtocol.code())?;
    }
    let error =
        if resolved == state.punt_next(next) && matches!(parsed.protocol, IpProtocol::Other(_)) {
            Some(IpLocalError::UnknownProtocol.code())
        } else {
            None
        };
    add_packet_trace!(
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
    )?;
    Ok(resolved)
}

#[inline(always)]
fn packet_bytes(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<Vec<u8>> {
    let buffer = runtime.get_buffer(index)?;
    let packet_len = buffer.current_len() + buffer.total_len_not_including_first();
    drop(buffer);
    let mut packet = runtime.copy_current_chain(index)?;
    if packet.len() > packet_len {
        packet.truncate(packet_len);
    }
    Ok(packet.into_iter().collect())
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
        IpProtocol::Udp => {
            let datagram_len = udp_datagram_len(transport)?;
            let datagram = &transport[..datagram_len];
            if stage.is_head_of_feature_arc() {
                let checksum = u16::from_be_bytes([transport[6], transport[7]]);
                match parsed.version {
                    IpVersion::V4 if checksum == 0 => {}
                    IpVersion::V6 if checksum == 0 => return Err(IpLocalError::BadChecksum),
                    _ if l4_checksum(packet, parsed, IP_PROTOCOL_UDP, datagram) != 0 => {
                        return Err(IpLocalError::BadChecksum);
                    }
                    _ => {}
                }
            }
            Ok(Some(UDP_HEADER_LEN))
        }
        IpProtocol::Icmpv4 => {
            if transport.len() < ICMP_HEADER_MIN_LEN {
                return Err(IpLocalError::BadTransportHeader);
            }
            if matches!(stage, LocalStage::Head) && internet_checksum(transport) != 0 {
                return Err(IpLocalError::BadChecksum);
            }
            Ok(Some(ICMP_HEADER_MIN_LEN))
        }
        IpProtocol::Icmpv6 => {
            if transport.len() < ICMP_HEADER_MIN_LEN {
                return Err(IpLocalError::BadTransportHeader);
            }
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
    if transport.len() < TCP_HEADER_MIN_LEN {
        return Err(IpLocalError::BadTransportHeader);
    }
    let header_len = ((transport[12] >> 4) as usize) * 4;
    if header_len < TCP_HEADER_MIN_LEN || transport.len() < header_len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(header_len)
}

#[inline(always)]
fn udp_datagram_len(transport: &[u8]) -> Result<usize, IpLocalError> {
    if transport.len() < UDP_HEADER_LEN {
        return Err(IpLocalError::BadTransportHeader);
    }
    let len = u16::from_be_bytes([transport[4], transport[5]]) as usize;
    if len < UDP_HEADER_LEN || transport.len() < len {
        return Err(IpLocalError::BadTransportHeader);
    }
    Ok(len)
}

#[inline(always)]
fn refresh_basic_metadata(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    parsed: &ParsedIpPacket,
    transport_header_len: Option<usize>,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let (source_port, destination_port) = match parsed.protocol {
        IpProtocol::Tcp | IpProtocol::Udp => {
            let current = buffer.current();
            let transport = current
                .get(parsed.transport_header_offset..parsed.packet_len)
                .ok_or_else(|| CoreError::internal("invalid transport packet cursor"))?;
            let source_port = u16::from_be_bytes([transport[0], transport[1]]);
            let destination_port = u16::from_be_bytes([transport[2], transport[3]]);
            (source_port, destination_port)
        }
        _ => (0, 0),
    };
    let transport_header_len = transport_header_len.unwrap_or_default();
    buffer.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(parsed.packet_len)
            .with_network_header(parsed.network_header_offset, parsed.network_header_len)
            .with_transport_header(parsed.transport_header_offset, transport_header_len)
            .with_transport_payload_offset(parsed.transport_header_offset + transport_header_len),
    );
    Ok(())
}

#[inline(always)]
fn source_check_passes(state: &IpLocalState, parsed: &ParsedIpPacket) -> bool {
    let IpLocalSourceCheck::ReverseFib(handle) = &state.source_check else {
        return true;
    };
    let fib = handle.table();
    let result = match parsed.source {
        IpAddr::V4(source) => fib.lookup_ip4(source, 0),
        IpAddr::V6(source) => fib.lookup_ip6(source, 0),
    };
    result.is_some_and(source_lookup_result_is_usable)
}

#[inline(always)]
fn source_lookup_result_is_usable(result: FibLookupResult) -> bool {
    !matches!(
        result.dpo.kind(),
        DpoType::DROP | DpoType::PUNT | DpoType::RECEIVE
    )
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
fn default_protocol_next(next: &[NodeId; IpLocalNext::COUNT], protocol: IpProtocol) -> NodeId {
    match protocol {
        IpProtocol::Tcp => next[IpLocalNext::Tcp.slot()],
        IpProtocol::Udp => next[IpLocalNext::Udp.slot()],
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => next[IpLocalNext::Icmp.slot()],
        IpProtocol::Other(_) => next[IpLocalNext::Punt.slot()],
    }
}

#[inline(always)]
fn l4_checksum(_packet: &[u8], parsed: &ParsedIpPacket, protocol: u8, segment: &[u8]) -> u16 {
    match parsed.version {
        IpVersion::V4 => {
            let mut pseudo = Vec::with_capacity(12 + segment.len());
            match (parsed.source, parsed.destination) {
                (IpAddr::V4(source), IpAddr::V4(destination)) => {
                    pseudo.extend_from_slice(&source.octets());
                    pseudo.extend_from_slice(&destination.octets());
                }
                _ => return 1,
            }
            pseudo.push(0);
            pseudo.push(protocol);
            pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
            pseudo.extend_from_slice(segment);
            internet_checksum(&pseudo)
        }
        IpVersion::V6 => {
            let mut pseudo = Vec::with_capacity(40 + segment.len());
            match (parsed.source, parsed.destination) {
                (IpAddr::V6(source), IpAddr::V6(destination)) => {
                    pseudo.extend_from_slice(&source.octets());
                    pseudo.extend_from_slice(&destination.octets());
                }
                _ => return 1,
            }
            pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
            pseudo.extend_from_slice(&[0, 0, 0, protocol]);
            pseudo.extend_from_slice(segment);
            internet_checksum(&pseudo)
        }
    }
}

#[inline(always)]
fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use hammer_adapter::{NodeId, NodeNextStorage};

    use super::*;

    #[test]
    fn ip_local_state_returns_protocol_or_control_next() {
        let drop = NodeId::new(1);
        let punt = NodeId::new(2);
        let tcp = NodeId::new(3);
        let udp = NodeId::new(4);
        let icmp = NodeId::new(5);
        let reassembly = NodeId::new(6);
        let next = IpLocalNext::nodes(drop, punt, tcp, udp, icmp, reassembly);
        let state = IpLocalState::new();

        assert_eq!(
            NodeNextStorage::next(
                &state,
                IpLocalNextKey::Protocol {
                    next: &next,
                    protocol: IpProtocol::Tcp
                }
            ),
            tcp
        );
        assert_eq!(
            NodeNextStorage::next(
                &state,
                IpLocalNextKey::Protocol {
                    next: &next,
                    protocol: IpProtocol::Other(99)
                }
            ),
            punt
        );
        assert_eq!(
            NodeNextStorage::next(&state, IpLocalNextKey::Drop(&next)),
            drop
        );
        assert_eq!(
            NodeNextStorage::next(&state, IpLocalNextKey::Reassembly(&next)),
            reassembly
        );
    }
}
