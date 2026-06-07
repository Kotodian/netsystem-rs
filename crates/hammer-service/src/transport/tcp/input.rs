use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId, Node, NodeHandle,
    NodeId, NodeNextStorage, NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
    PacketTrace, RouteMetadata, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::data_plane::set_index_node_error_code;
use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
    put_u8,
};

use super::{
    TcpDispatchTable, TcpInputError, TcpInputFlags, TcpInputNext, TcpLookupKind, TcpLookupSnapshot,
    TcpLookupValue, TcpState, TcpV4ConnectionKey, TcpV4ListenerKey, TcpV6ConnectionKey,
    TcpV6ListenerKey,
};

const TCP_HEADER_MIN_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpInputHandoff {
    tcp_input: NodeHandle,
    worker: DataWorkerId,
}

impl TcpInputHandoff {
    #[inline]
    pub fn new(tcp_input: NodeHandle, worker: DataWorkerId) -> Self {
        Self { tcp_input, worker }
    }

    #[inline]
    fn current_worker(self) -> DataWorkerId {
        self.worker
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub flags: u8,
    pub error: Option<u16>,
    pub next: NodeId,
}

impl TcpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            version: cursor.read_option_ip_version()?,
            protocol: cursor.read_option_ip_protocol()?,
            source_port: cursor.read_option_u16()?,
            destination_port: cursor.read_option_u16()?,
            flags: cursor.read_u8()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for TcpInputTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_u16(out, self.source_port);
        put_option_u16(out, self.destination_port);
        put_u8(out, self.flags);
        put_option_u16(out, self.error);
        put_node(out, self.next);
    }
}

fn format_tcp_input_trace(bytes: &[u8]) -> String {
    match TcpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("TcpInputTrace invalid={bytes:?}"),
    }
}

#[derive(Debug, Clone)]
struct TcpInputSnapshot {
    lookup: TcpLookupSnapshot,
    dispatch: TcpDispatchTable,
}

impl TcpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            lookup: TcpLookupSnapshot::default(),
            dispatch: TcpDispatchTable::default(),
        }
    }
}

#[derive(Clone)]
struct TcpInputSnapshotHandle {
    inner: Arc<ArcSwap<TcpInputSnapshot>>,
}

impl TcpInputSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<TcpInputSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<TcpInputSnapshot>> {
        self.inner.load()
    }
}

pub struct TcpInputControlPlane {
    inner: Arc<ArcSwap<TcpInputSnapshot>>,
    next: [NodeId; TcpInputNext::COUNT],
}

impl TcpInputControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpInputNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpInputSnapshot::new())),
            next,
        }
    }

    #[inline]
    pub fn publish_lookup(&self, lookup: TcpLookupSnapshot) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = TcpInputSnapshot::clone(current);
            next.lookup = lookup.clone();
            next
        });
        Ok(())
    }

    #[inline]
    pub fn publish_dispatch(&self, dispatch: TcpDispatchTable) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = TcpInputSnapshot::clone(current);
            next.dispatch = dispatch.clone();
            next
        });
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> TcpInputNode {
        TcpInputNode::new(
            TcpInputSnapshotHandle::new(Arc::clone(&self.inner)),
            self.next,
        )
    }
}

#[hammer_component_macros::node(role = internal, next = TcpInputNext)]
pub struct TcpInputNode {
    #[node(default = register_tcp_input_runtime(snapshot.clone()))]
    runtime_data: NodeRuntimeData,
    snapshot: TcpInputSnapshotHandle,
    #[node(default)]
    handoff: Option<TcpInputHandoff>,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl TcpInputNode {
    #[inline]
    pub fn with_handoff(mut self, handoff: TcpInputHandoff) -> Self {
        self.handoff = Some(handoff);
        self
    }
}

impl Node for TcpInputNode {
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
            |index| next_node_for_index(runtime, index, &snapshot, &next, self.handoff),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_input_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_input_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_input_runtime(self.runtime_data, self.handoff)?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct TcpInputRuntime {
    snapshot: TcpInputSnapshotHandle,
    handoff: Option<TcpInputHandoff>,
}

fn tcp_input_runtimes() -> &'static Mutex<Vec<TcpInputRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<TcpInputRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_tcp_input_runtime(snapshot: TcpInputSnapshotHandle) -> NodeRuntimeData {
    let mut runtimes = tcp_input_runtimes()
        .lock()
        .expect("TCP input runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(TcpInputRuntime {
        snapshot,
        handoff: None,
    });
    NodeRuntimeData::from_usize(slot).expect("TCP input runtime slot overflow")
}

fn tcp_input_runtime(data: NodeRuntimeData) -> CoreResult<TcpInputRuntime> {
    let slot = data.usize_word(0)?;
    tcp_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("TCP input runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))
}

fn sync_tcp_input_runtime(
    data: NodeRuntimeData,
    handoff: Option<TcpInputHandoff>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = tcp_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("TCP input runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))?;
    runtime.handoff = handoff;
    Ok(())
}

fn tcp_input_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_input_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpInputNode::runtime_nexts(runtime)?;
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        next_node_for_index(runtime, index, &snapshot, &next, state.handoff)
    })?;
    Ok(result)
}

#[derive(Debug, Clone, Copy)]
struct ParsedTcpInput {
    version: IpVersion,
    protocol: IpProtocol,
    source_ip: IpAddr,
    destination_ip: IpAddr,
    source_port: u16,
    destination_port: u16,
    flags: TcpInputFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpInputParseError {
    BadLength,
    WrongProtocol {
        version: IpVersion,
        protocol: IpProtocol,
    },
}

#[inline(always)]
fn next_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    snapshot: &TcpInputSnapshot,
    next: &[NodeId; TcpInputNext::COUNT],
    handoff: Option<TcpInputHandoff>,
) -> CoreResult<Option<NodeId>> {
    let parsed = match parse_tcp_input(runtime, index)? {
        Ok(parsed) => parsed,
        Err(TcpInputParseError::BadLength) => {
            return resolve_error_next(
                runtime,
                index,
                next,
                TcpInputNext::Drop,
                TcpInputError::BadLength,
                None,
                None,
                0,
            );
        }
        Err(TcpInputParseError::WrongProtocol { version, protocol }) => {
            return resolve_error_next(
                runtime,
                index,
                next,
                TcpInputNext::Drop,
                TcpInputError::WrongProtocol,
                Some(version),
                Some(protocol),
                0,
            );
        }
    };

    let lookup = lookup_for_packet(runtime.metadata(index)?, &snapshot.lookup, &parsed);
    if let Some(owner) = established_owner(lookup)
        && let Some(handoff) = handoff
        && owner != handoff.current_worker()
    {
        runtime.handoff_index(owner, handoff.tcp_input, index)?;
        return Ok(None);
    }
    let state = match lookup {
        Some(value) if value.kind == TcpLookupKind::EstablishedConnection => TcpState::Established,
        _ => TcpState::Listen,
    };
    let entry = snapshot.dispatch.entry(state, parsed.flags);
    if let Some(error) = entry.error {
        return resolve_error_next(
            runtime,
            index,
            next,
            entry.next,
            error,
            Some(parsed.version),
            Some(parsed.protocol),
            parsed.flags.bits(),
        );
    }
    clear_success_metadata(runtime, index)?;
    let resolved = NodeNextStorage::next(next, entry.next);
    add_packet_trace!(
        runtime,
        index,
        TcpInputTrace {
            version: Some(parsed.version),
            protocol: Some(parsed.protocol),
            source_port: Some(parsed.source_port),
            destination_port: Some(parsed.destination_port),
            flags: parsed.flags.bits(),
            error: None,
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn resolve_error_next(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; TcpInputNext::COUNT],
    next_key: TcpInputNext,
    error: TcpInputError,
    version: Option<IpVersion>,
    protocol: Option<IpProtocol>,
    flags: u8,
) -> CoreResult<Option<NodeId>> {
    set_index_node_error_code(runtime, index, error.code())?;
    let resolved = NodeNextStorage::next(next, next_key);
    add_packet_trace!(
        runtime,
        index,
        TcpInputTrace {
            version,
            protocol,
            source_port: None,
            destination_port: None,
            flags,
            error: Some(error.code()),
            next: resolved,
        },
    )?;
    Ok(Some(resolved))
}

#[inline(always)]
fn established_owner(lookup: Option<TcpLookupValue>) -> Option<DataWorkerId> {
    match lookup {
        Some(value) if value.kind == TcpLookupKind::EstablishedConnection => {
            Some(value.owner_worker)
        }
        _ => None,
    }
}

#[inline(always)]
fn clear_success_metadata(runtime: &DataPlaneRuntime, index: BufferIndex) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.clear_node_error();
    buffer.metadata_mut().icmp_error = None;
    Ok(())
}

#[inline(always)]
fn parse_tcp_input(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<Result<ParsedTcpInput, TcpInputParseError>> {
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    let cursor = buffer.packet_cursor();
    let parsed =
        match parse_ip_packet_with_chain_len(current, buffer.total_len_not_including_first()) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(Err(TcpInputParseError::BadLength)),
        };
    if parsed.protocol != IpProtocol::Tcp {
        return Ok(Err(TcpInputParseError::WrongProtocol {
            version: parsed.version,
            protocol: parsed.protocol,
        }));
    }
    if parsed.input_error != IpInputError::None || !valid_tcp_cursor(cursor) {
        return Ok(Err(TcpInputParseError::BadLength));
    }
    let packet = current
        .get(..parsed.packet_len)
        .ok_or_else(|| CoreError::internal("invalid TCP packet length"))?;
    let source_ip = source_ip(parsed.version, packet)?;
    let destination_ip = destination_ip(parsed.version, packet)?;
    let header = packet
        .get(cursor.transport_header_offset()..)
        .ok_or_else(|| CoreError::internal("missing TCP header"))?;
    if header.len() < TCP_HEADER_MIN_LEN {
        return Ok(Err(TcpInputParseError::BadLength));
    }
    let source_port = u16::from_be_bytes([header[0], header[1]]);
    let destination_port = u16::from_be_bytes([header[2], header[3]]);
    let header_len = ((header[12] >> 4) as usize) * 4;
    if header_len < TCP_HEADER_MIN_LEN
        || cursor.transport_header_offset() + header_len > cursor.packet_len()
    {
        return Ok(Err(TcpInputParseError::BadLength));
    }
    let flags = tcp_flags_from_byte(header[13]);
    Ok(Ok(ParsedTcpInput {
        version: parsed.version,
        protocol: parsed.protocol,
        source_ip,
        destination_ip,
        source_port,
        destination_port,
        flags,
    }))
}

#[inline(always)]
fn valid_tcp_cursor(cursor: BufferPacketCursor) -> bool {
    cursor.packet_len() >= cursor.transport_header_offset() + TCP_HEADER_MIN_LEN
}

#[inline(always)]
fn tcp_flags_from_byte(flags: u8) -> TcpInputFlags {
    let mut parsed = TcpInputFlags::empty();
    if flags & 0x01 != 0 {
        parsed |= TcpInputFlags::FIN;
    }
    if flags & 0x02 != 0 {
        parsed |= TcpInputFlags::SYN;
    }
    if flags & 0x04 != 0 {
        parsed |= TcpInputFlags::RST;
    }
    if flags & 0x10 != 0 {
        parsed |= TcpInputFlags::ACK;
    }
    parsed
}

#[inline(always)]
fn source_ip(version: IpVersion, packet: &[u8]) -> CoreResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let source = packet
                .get(12..16)
                .ok_or_else(|| CoreError::internal("missing IPv4 source"))?;
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        IpVersion::V6 => {
            let source = packet
                .get(8..24)
                .ok_or_else(|| CoreError::internal("missing IPv6 source"))?;
            let bytes: [u8; 16] = source
                .try_into()
                .map_err(|_| CoreError::internal("invalid IPv6 source length"))?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn destination_ip(version: IpVersion, packet: &[u8]) -> CoreResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let destination = packet
                .get(16..20)
                .ok_or_else(|| CoreError::internal("missing IPv4 destination"))?;
            Ok(Ipv4Addr::new(
                destination[0],
                destination[1],
                destination[2],
                destination[3],
            )
            .into())
        }
        IpVersion::V6 => {
            let destination = packet
                .get(24..40)
                .ok_or_else(|| CoreError::internal("missing IPv6 destination"))?;
            let bytes: [u8; 16] = destination
                .try_into()
                .map_err(|_| CoreError::internal("invalid IPv6 destination length"))?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn lookup_for_packet(
    metadata: RouteMetadata,
    snapshot: &TcpLookupSnapshot,
    parsed: &ParsedTcpInput,
) -> Option<TcpLookupValue> {
    let local = metadata
        .destination
        .as_ref()
        .map(|addr| (addr.host, addr.port))
        .unwrap_or((parsed.destination_ip, parsed.destination_port));
    let remote = metadata
        .source
        .as_ref()
        .map(|addr| (addr.host, addr.port))
        .unwrap_or((parsed.source_ip, parsed.source_port));
    match (local.0, remote.0, parsed.version) {
        (IpAddr::V4(local_addr), IpAddr::V4(remote_addr), IpVersion::V4) => snapshot.lookup_v4(
            TcpV4ConnectionKey::new(0, local_addr, local.1, remote_addr, remote.1),
            TcpV4ListenerKey::new(0, local_addr, local.1),
        ),
        (IpAddr::V6(local_addr), IpAddr::V6(remote_addr), IpVersion::V6) => snapshot.lookup_v6(
            TcpV6ConnectionKey::new(0, local_addr, local.1, remote_addr, remote.1),
            TcpV6ListenerKey::new(0, local_addr, local.1),
        ),
        _ => None,
    }
}
