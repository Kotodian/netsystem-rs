use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use crate::data_plane::set_index_node_error_code;
use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
    put_u8,
};
use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId, Node, NodeHandle,
    NodeId, NodeNextStorage, NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
    PacketTrace, RouteMetadata, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpSegmentFlags, TcpSegmentParseError, TcpSegmentView};

use crate::session::SessionQueueHandle;

use super::session::TcpSessionQueue;
use super::{
    TcpInputError, TcpInputFlags, TcpInputNext, TcpIpv4ListenerAddress, TcpIpv6ListenerAddress,
    TcpLookupSnapshot, TcpLookupValue, TcpSessionProtocol, TcpV4ListenerKey, TcpV6ListenerKey,
};

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
}

impl TcpInputSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            lookup: TcpLookupSnapshot::default(),
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
    session_queue: Option<SessionQueueHandle>,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl TcpInputNode {
    #[inline]
    pub fn with_handoff(mut self, handoff: TcpInputHandoff) -> Self {
        self.handoff = Some(handoff);
        self
    }

    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
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
            |index| {
                next_node_for_index(
                    runtime,
                    index,
                    &snapshot,
                    &next,
                    self.handoff,
                    self.session_queue,
                )
            },
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
        sync_tcp_input_runtime(self.runtime_data, self.handoff, self.session_queue)?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct TcpInputRuntime {
    snapshot: TcpInputSnapshotHandle,
    handoff: Option<TcpInputHandoff>,
    session_queue: Option<SessionQueueHandle>,
}

thread_local! {
    static TCP_INPUT_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpInputRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_tcp_input_runtime(snapshot: TcpInputSnapshotHandle) -> NodeRuntimeData {
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpInputRuntime {
            snapshot,
            handoff: None,
            session_queue: None,
        });
        NodeRuntimeData::from_usize(slot).expect("TCP input runtime slot overflow")
    })
}

fn tcp_input_runtime(data: NodeRuntimeData) -> CoreResult<TcpInputRuntime> {
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))
    })
}

fn sync_tcp_input_runtime(
    data: NodeRuntimeData,
    handoff: Option<TcpInputHandoff>,
    session_queue: Option<SessionQueueHandle>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))?;
        runtime.handoff = handoff;
        runtime.session_queue = session_queue;
        Ok(())
    })
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
        next_node_for_index(
            runtime,
            index,
            &snapshot,
            &next,
            state.handoff,
            state.session_queue,
        )
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
    session_queue: Option<SessionQueueHandle>,
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

    if let Some(session) = session_input_entry(session_queue, &parsed)? {
        if let Some(handoff) = handoff
            && session.owner != handoff.current_worker()
        {
            runtime.handoff_index(session.owner, handoff.tcp_input, index)?;
            return Ok(None);
        }
        return resolve_success_next(
            runtime,
            index,
            next,
            session.next,
            parsed.version,
            parsed.protocol,
            parsed.source_port,
            parsed.destination_port,
            parsed.flags.bits(),
        );
    }

    let metadata = runtime.metadata(index)?;
    let lookup = lookup_for_packet(&metadata, &snapshot.lookup, &parsed);
    let entry = tcp_listener_input_entry(parsed.flags);
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
    let Some(_listener) = lookup else {
        return resolve_error_next(
            runtime,
            index,
            next,
            TcpInputNext::Punt,
            TcpInputError::ConnectionClosed,
            Some(parsed.version),
            Some(parsed.protocol),
            parsed.flags.bits(),
        );
    };
    resolve_success_next(
        runtime,
        index,
        next,
        entry.next,
        parsed.version,
        parsed.protocol,
        parsed.source_port,
        parsed.destination_port,
        parsed.flags.bits(),
    )
}

#[inline(always)]
fn resolve_success_next(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: &[NodeId; TcpInputNext::COUNT],
    next_key: TcpInputNext,
    version: IpVersion,
    protocol: IpProtocol,
    source_port: u16,
    destination_port: u16,
    flags: u8,
) -> CoreResult<Option<NodeId>> {
    clear_success_metadata(runtime, index)?;
    let resolved = NodeNextStorage::next(next, next_key);
    add_packet_trace!(
        runtime,
        index,
        TcpInputTrace {
            version: Some(version),
            protocol: Some(protocol),
            source_port: Some(source_port),
            destination_port: Some(destination_port),
            flags,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpInputEntry {
    next: TcpInputNext,
    error: Option<TcpInputError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpSessionInputEntry {
    owner: DataWorkerId,
    next: TcpInputNext,
}

fn session_input_entry(
    session_queue: Option<SessionQueueHandle>,
    parsed: &ParsedTcpInput,
) -> CoreResult<Option<TcpSessionInputEntry>> {
    let Some(handle) = session_queue else {
        return Ok(None);
    };
    let local = SocketAddr::new(parsed.destination_ip, parsed.destination_port);
    let remote = SocketAddr::new(parsed.source_ip, parsed.source_port);
    TcpSessionProtocol::with_queue(handle, |runtime: &mut TcpSessionQueue| {
        let session_id = runtime
            .session_id_by_tuple(local, remote)
            .or_else(|| runtime.pending_id_by_tuple(local, remote));
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let Some(connection) = runtime.session_state(session_id) else {
            return Ok(None);
        };
        Ok(Some(TcpSessionInputEntry {
            owner: connection.owner_worker(),
            next: connection.next_node(),
        }))
    })
}

#[inline(always)]
fn tcp_listener_input_entry(flags: TcpInputFlags) -> TcpInputEntry {
    if flags == TcpInputFlags::SYN {
        return TcpInputEntry {
            next: TcpInputNext::Listen,
            error: None,
        };
    }
    if flags == TcpInputFlags::ACK {
        return TcpInputEntry {
            next: TcpInputNext::Reset,
            error: Some(TcpInputError::AckInvalid),
        };
    }
    TcpInputEntry {
        next: TcpInputNext::Punt,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use hammer_adapter::{DataPlaneHandoff, DataPlaneRuntime, DataWorkerId, NodeHandle, NodeId};
    use hammer_core::protocol::tcp::TcpConnectionId;

    use crate::transport::tcp::connection::TcpConnectionState;
    use crate::transport::tcp::session::TcpSessionQueue;
    use crate::transport::tcp::{TcpInputFlags, TcpInputHandoff, TcpInputNext, TcpSessionProtocol};

    use super::{
        ParsedTcpInput, TcpInputSnapshot, TcpInputSnapshotHandle, TcpSessionInputEntry,
        next_node_for_index, parse_tcp_input, register_tcp_input_runtime, session_input_entry,
    };
    use crate::net::ip::{IpProtocol, IpVersion};
    use crate::session::node::SessionQueueHandle;

    #[test]
    fn tcp_input_runtime_registry_is_isolated_per_thread() {
        let main_snapshot =
            TcpInputSnapshotHandle::new(Arc::new(ArcSwap::from_pointee(TcpInputSnapshot::new())));
        let main_runtime = register_tcp_input_runtime(main_snapshot)
            .usize_word(0)
            .expect("main runtime slot");
        assert_eq!(main_runtime, 0);

        let worker_runtime = std::thread::spawn(|| {
            let worker_snapshot = TcpInputSnapshotHandle::new(Arc::new(ArcSwap::from_pointee(
                TcpInputSnapshot::new(),
            )));
            register_tcp_input_runtime(worker_snapshot)
                .usize_word(0)
                .expect("worker runtime slot")
        })
        .join()
        .expect("worker thread joins");

        assert_eq!(worker_runtime, 0);
    }

    #[test]
    fn tcp_input_routes_existing_established_tuple_to_established_node() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(0), 50_044);

        let entry = session_input_entry(Some(handle), &parsed_input(50_044))
            .expect("session lookup")
            .expect("existing tcp session");

        assert_eq!(
            entry,
            TcpSessionInputEntry {
                owner: DataWorkerId::new(0),
                next: TcpInputNext::Established,
            }
        );
    }

    #[test]
    fn tcp_input_existing_session_entry_keeps_owner_for_handoff_decision() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(1), 50_055);

        let entry = session_input_entry(Some(handle), &parsed_input(50_055))
            .expect("session lookup")
            .expect("existing tcp session");

        assert_eq!(
            entry,
            TcpSessionInputEntry {
                owner: DataWorkerId::new(1),
                next: TcpInputNext::Established,
            }
        );
    }

    #[test]
    fn tcp_input_routes_pending_syn_sent_tuple_to_syn_sent_node() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let worker = DataWorkerId::new(0);
        let handle = TcpSessionProtocol::register_queue(worker, runtime.packet_buffers().clone())
            .expect("register tcp queue");
        let local_port = 50_077;
        let local = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            local_port,
        );
        let remote = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            443,
        );
        let session_id = TcpSessionProtocol::connect(handle, local, remote).expect("connect");

        let entry = session_input_entry(Some(handle), &parsed_input(local_port))
            .expect("session lookup")
            .expect("pending tcp session");

        assert_eq!(
            entry,
            TcpSessionInputEntry {
                owner: worker,
                next: TcpInputNext::SynSent,
            }
        );
        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            assert_eq!(queue.session_id_by_tuple(local, remote), None);
            assert_eq!(queue.pending_id_by_tuple(local, remote), Some(session_id));
            Ok(())
        })
        .expect("inspect pending session");
    }

    #[test]
    fn tcp_input_handoffs_existing_session_to_owner_worker() {
        const TCP_INPUT_HANDLE: NodeHandle = NodeHandle::new(44);

        let handoff = DataPlaneHandoff::new(2, 8);
        let runtime = DataPlaneRuntime::with_handoff(
            DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
            DataWorkerId::new(0),
            handoff.worker(DataWorkerId::new(0)),
        );
        let handle = install_tcp_session(&runtime, DataWorkerId::new(1), 50_066);
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let packet = tcp_packet(
            Ipv4Addr::new(198, 51, 100, 50_066u16 as u8),
            443,
            Ipv4Addr::new(192, 0, 2, 50_066u16 as u8),
            50_066,
        );
        let index = runtime
            .alloc_index_with_bytes(Default::default(), &packet)
            .expect("alloc packet");
        stamp_tcp_cursor(&runtime, index, &packet);
        assert!(
            parse_tcp_input(&runtime, index)
                .expect("parse tcp input")
                .is_ok()
        );
        runtime
            .get_frame_mut(frame)
            .expect("frame mut")
            .push_index(index)
            .expect("push packet");

        let next = TcpInputNext::nodes(
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
            NodeId::new(6),
            NodeId::new(7),
        );
        let selected = next_node_for_index(
            &runtime,
            index,
            &TcpInputSnapshot::new(),
            &next,
            Some(TcpInputHandoff::new(TCP_INPUT_HANDLE, DataWorkerId::new(0))),
            Some(handle),
        )
        .expect("next node");

        assert_eq!(selected, None);
        assert_eq!(
            runtime
                .handoff_source_worker(index)
                .expect("handoff source"),
            Some(DataWorkerId::new(0))
        );
        runtime.free_frame_index(frame).expect("free frame");
    }

    fn install_tcp_session(
        runtime: &DataPlaneRuntime,
        owner: DataWorkerId,
        local_port: u16,
    ) -> SessionQueueHandle {
        let local = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            local_port,
        );
        let remote = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            443,
        );
        let connection = TcpConnectionState::established_for_test(
            Some(TcpConnectionId::new(u64::from(local_port))),
            owner,
            local_port,
            Some(local),
            remote,
        );
        TcpSessionProtocol::register_queue_with_connection_for_test(
            owner,
            runtime.packet_buffers().clone(),
            connection,
        )
        .expect("register test queue")
    }

    fn parsed_input(local_port: u16) -> ParsedTcpInput {
        ParsedTcpInput {
            version: IpVersion::V4,
            protocol: IpProtocol::Tcp,
            source_ip: IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            destination_ip: IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            source_port: 443,
            destination_port: local_port,
            flags: TcpInputFlags::ACK,
        }
    }

    fn tcp_packet(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
    ) -> std::vec::Vec<u8> {
        let mut packet = std::vec![0u8; 40];
        packet[0] = 0x45;
        packet[2..4].copy_from_slice(&40u16.to_be_bytes());
        packet[8] = 64;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&source.octets());
        packet[16..20].copy_from_slice(&destination.octets());
        packet[20..22].copy_from_slice(&source_port.to_be_bytes());
        packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
        packet[32] = 0x50;
        packet[33] = TcpInputFlags::ACK.bits();
        let tcp_checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
        packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
        let ip_checksum = internet_checksum(&packet[..20]);
        packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        packet
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut pseudo = std::vec::Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
        pseudo.extend_from_slice(&source.octets());
        pseudo.extend_from_slice(&destination.octets());
        pseudo.push(0);
        pseudo.push(protocol);
        pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
        pseudo.extend_from_slice(segment);
        internet_checksum(&pseudo)
    }

    fn internet_checksum(bytes: &[u8]) -> u16 {
        let mut sum = 0u32;
        for chunk in bytes.chunks(2) {
            let word = match chunk {
                [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
                [hi] => u16::from_be_bytes([*hi, 0]) as u32,
                _ => unreachable!(),
            };
            sum += word;
            while sum > 0xffff {
                sum = (sum & 0xffff) + (sum >> 16);
            }
        }
        !(sum as u16)
    }

    fn stamp_tcp_cursor(
        runtime: &DataPlaneRuntime,
        buffer: hammer_adapter::BufferIndex,
        packet: &[u8],
    ) {
        let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
        runtime
            .get_buffer_mut(buffer)
            .expect("buffer mut")
            .set_packet_cursor(
                hammer_adapter::BufferPacketCursor::new()
                    .with_packet_len(packet.len())
                    .with_network_header(0, header_len)
                    .with_transport_header(header_len, 20)
                    .with_transport_payload_offset(header_len + 20),
            );
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
    let first_len = current.len().min(parsed.packet_len);
    let packet = current
        .get(..first_len)
        .ok_or_else(|| CoreError::internal("invalid TCP packet length"))?;
    let source_ip = source_ip(parsed.version, packet)?;
    let destination_ip = destination_ip(parsed.version, packet)?;
    let transport = packet
        .get(cursor.transport_header_offset()..first_len)
        .ok_or_else(|| CoreError::internal("missing TCP header"))?;
    let segment = match TcpSegmentView::parse(transport) {
        Ok(segment) => segment,
        Err(
            TcpSegmentParseError::ShortHeader
            | TcpSegmentParseError::BadDataOffset
            | TcpSegmentParseError::InvalidSlice,
        ) => return Ok(Err(TcpInputParseError::BadLength)),
    };
    Ok(Ok(ParsedTcpInput {
        version: parsed.version,
        protocol: parsed.protocol,
        source_ip,
        destination_ip,
        source_port: segment.source_port(),
        destination_port: segment.destination_port(),
        flags: tcp_input_flags(segment.flags()),
    }))
}

#[inline(always)]
fn valid_tcp_cursor(cursor: BufferPacketCursor) -> bool {
    cursor.packet_len() >= cursor.transport_header_offset()
}

#[inline(always)]
fn tcp_input_flags(flags: TcpSegmentFlags) -> TcpInputFlags {
    let mut parsed = TcpInputFlags::empty();
    if flags.contains(TcpSegmentFlags::FIN) {
        parsed |= TcpInputFlags::FIN;
    }
    if flags.contains(TcpSegmentFlags::SYN) {
        parsed |= TcpInputFlags::SYN;
    }
    if flags.contains(TcpSegmentFlags::RST) {
        parsed |= TcpInputFlags::RST;
    }
    if flags.contains(TcpSegmentFlags::ACK) {
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
    metadata: &RouteMetadata,
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
        (IpAddr::V4(local_addr), IpAddr::V4(_remote_addr), IpVersion::V4) => snapshot
            .lookup_listener::<TcpIpv4ListenerAddress>(
            TcpV4ListenerKey::new(0, local_addr, local.1),
        ),
        (IpAddr::V6(local_addr), IpAddr::V6(_remote_addr), IpVersion::V6) => snapshot
            .lookup_listener::<TcpIpv6ListenerAddress>(
            TcpV6ListenerKey::new(0, local_addr, local.1),
        ),
        _ => None,
    }
}
