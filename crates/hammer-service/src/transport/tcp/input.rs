use std::cell::RefCell;
use std::mem::size_of;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use crate::data_plane::set_buffer_node_error_code;
use crate::net::ip::{IpInputError, IpProtocol, IpVersion, parse_ip_packet_with_chain_len};
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_ip_protocol, put_option_ip_version, put_option_u16,
    put_u16,
};
use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, DataWorkerId,
    Node, NodeHandle, NodeId, NodeNextStorage, NodeProcessFn, NodeResult, NodeRuntimeData,
    PacketTrace, SecondaryOpaque, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_core::protocol::tcp::{TcpError, TcpInputFlags, TcpSegmentFlags};
use hammer_infra::vec::Vec;

use super::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpLookupSnapshot, TcpLookupValue,
    TcpV4ListenerKey, TcpV6ListenerKey,
};
use super::segment::{parse_tcp_wire_header, tcp_wire_flags};
use super::{TcpInputNext, TcpQueue, write_session_route_opaque};
use super::{tcp_worker_state, tcp_worker_state_mut};
use crate::session::SessionId;
use crate::transport::congestion::CongestionController;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<IcmpErrorMetadata>,
    reserved: [u64; 6],
}

const _: () = assert!(size_of::<IcmpErrorOpaque>() == size_of::<SecondaryOpaque>());

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpInputTrace {
    pub version: Option<IpVersion>,
    pub protocol: Option<IpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub flags: u16,
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
            flags: cursor.read_u16()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for TcpInputTrace {
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
        put_option_ip_version(out, self.version);
        put_option_ip_protocol(out, self.protocol);
        put_option_u16(out, self.source_port);
        put_option_u16(out, self.destination_port);
        put_u16(out, self.flags);
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

#[derive(Clone)]
pub struct TcpInputControlPlane {
    inner: Arc<ArcSwap<TcpLookupSnapshot>>,
}

impl TcpInputControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpLookupSnapshot::default())),
        }
    }

    #[inline]
    pub fn publish_lookup(&self, lookup: TcpLookupSnapshot) -> CoreResult<()> {
        self.inner.store(Arc::new(lookup));
        Ok(())
    }

    #[inline]
    pub(crate) fn lookup_listener(&self, local: SocketAddr) -> Option<TcpLookupValue> {
        let snapshot = self.inner.load();
        match local.ip() {
            IpAddr::V4(local_addr) => snapshot.lookup_listener::<TcpIpv4ListenerAddress>(
                TcpV4ListenerKey::new(0, local_addr, local.port()),
            ),
            IpAddr::V6(local_addr) => snapshot.lookup_listener::<TcpIpv6ListenerAddress>(
                TcpV6ListenerKey::new(0, local_addr, local.port()),
            ),
        }
    }

    #[inline]
    pub(crate) fn node<C>(
        &self,
        next: [NodeId; TcpInputNext::COUNT],
        session_queue: Option<TcpQueue<C>>,
        handoff: Option<(NodeHandle, DataWorkerId)>,
    ) -> TcpInputNode<C>
    where
        C: CongestionController + 'static,
    {
        let mut node = TcpInputNode::<C>::new(Arc::clone(&self.inner), next);
        node.session_queue = session_queue;
        if let Some((handoff, worker)) = handoff {
            node.handoff = Some(handoff);
            node.handoff_worker = Some(worker);
        }
        node
    }
}

#[hammer_component_macros::node(role = internal, next = TcpInputNext)]
pub struct TcpInputNode<C: CongestionController + 'static> {
    #[node(default = register_tcp_input_runtime(snapshot.clone()))]
    runtime_data: NodeRuntimeData,
    snapshot: Arc<ArcSwap<TcpLookupSnapshot>>,
    #[node(default)]
    handoff: Option<NodeHandle>,
    #[node(default)]
    handoff_worker: Option<DataWorkerId>,
    #[node(default)]
    session_queue: Option<TcpQueue<C>>,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl<C> Node for TcpInputNode<C>
where
    C: CongestionController + 'static,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let mut traces = Vec::new();
        let mut handoffs = Vec::new();
        hammer_adapter::node_route_frame_cached!(
            self,
            runtime,
            frame,
            |batch, indices| prefetch_tcp_input(batch, indices, &snapshot, self.session_queue),
            |batch, indices, nexts| {
                for (offset, index) in indices.iter().copied().enumerate() {
                    let parsed = parse_tcp_input_buffer(batch.buffer(index)?)?;
                    nexts[offset] = next_node_for_index_with_batch(
                        runtime,
                        batch,
                        index,
                        parsed,
                        &snapshot,
                        &next,
                        self.handoff,
                        self.handoff_worker,
                        self.session_queue,
                        &mut traces,
                        &mut handoffs,
                    )?;
                }
                Ok(())
            },
            {
                for (worker, target, index) in handoffs {
                    runtime.handoff_index(worker, target, index)?;
                }
                for (index, trace) in traces {
                    add_packet_trace!(runtime, index, trace)?;
                }
            }
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_input_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_input_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_input_runtime(
            self.runtime_data,
            self.handoff,
            self.handoff_worker,
            self.session_queue,
        )?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct TcpInputRuntime {
    snapshot: Arc<ArcSwap<TcpLookupSnapshot>>,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
    session_queue: Option<NodeRuntimeData>,
}

thread_local! {
    static TCP_INPUT_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpInputRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_tcp_input_runtime(snapshot: Arc<ArcSwap<TcpLookupSnapshot>>) -> NodeRuntimeData {
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpInputRuntime {
            snapshot,
            handoff: None,
            handoff_worker: None,
            session_queue: None,
        });
        NodeRuntimeData::from_usize(slot).expect("TCP input runtime slot overflow")
    })
}

fn tcp_input_runtime(data: NodeRuntimeData) -> CoreResult<TcpInputRuntime>
where
{
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))
    })
}

fn sync_tcp_input_runtime<C>(
    data: NodeRuntimeData,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
    session_queue: Option<TcpQueue<C>>,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP input runtime slot is invalid"))?;
        runtime.handoff = handoff;
        runtime.handoff_worker = handoff_worker;
        runtime.session_queue = session_queue.map(TcpQueue::runtime_data);
        Ok(())
    })
}

fn tcp_input_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let state = tcp_input_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpInputNode::<C>::runtime_nexts(runtime)?;
    let session_queue = state.session_queue.map(TcpQueue::<C>::new);
    let mut traces = Vec::new();
    let mut handoffs = Vec::new();
    hammer_adapter::node_route_frame_static!(
        None,
        runtime,
        frame,
        |batch, indices| prefetch_tcp_input(batch, indices, &snapshot, session_queue),
        |batch, indices, nexts| {
            for (offset, index) in indices.iter().copied().enumerate() {
                let parsed = parse_tcp_input_buffer(batch.buffer(index)?)?;
                nexts[offset] = next_node_for_index_with_batch(
                    runtime,
                    batch,
                    index,
                    parsed,
                    &snapshot,
                    &next,
                    state.handoff,
                    state.handoff_worker,
                    session_queue,
                    &mut traces,
                    &mut handoffs,
                )?;
            }
            Ok(())
        },
        {
            for (worker, target, index) in handoffs {
                runtime.handoff_index(worker, target, index)?;
            }
            for (index, trace) in traces {
                add_packet_trace!(runtime, index, trace)?;
            }
        }
    )
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
fn next_node_for_index_with_batch<C>(
    runtime: &DataPlaneRuntime,
    batch: &mut BufferBatchMut<'_>,
    index: BufferIndex,
    parsed: Result<
        (IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags),
        TcpInputParseError,
    >,
    snapshot: &TcpLookupSnapshot,
    next: &[NodeId; TcpInputNext::COUNT],
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
    session_queue: Option<TcpQueue<C>>,
    traces: &mut Vec<(BufferIndex, TcpInputTrace)>,
    handoffs: &mut Vec<(DataWorkerId, NodeHandle, BufferIndex)>,
) -> CoreResult<Option<NodeId>>
where
    C: CongestionController + 'static,
{
    let traced = batch.buffer(index)?.trace_handle().is_some();
    let (version, protocol, local, remote, flags) = match parsed {
        Ok(parsed) => parsed,
        Err(TcpInputParseError::BadLength) => {
            return resolve_error_next_with_batch(
                runtime,
                batch,
                index,
                next,
                TcpInputNext::Drop,
                TcpError::Length,
                None,
                None,
                0,
                traced,
                traces,
            );
        }
        Err(TcpInputParseError::WrongProtocol { version, protocol }) => {
            return resolve_error_next_with_batch(
                runtime,
                batch,
                index,
                next,
                TcpInputNext::Drop,
                TcpError::Dispatch,
                Some(version),
                Some(protocol),
                0,
                traced,
                traces,
            );
        }
    };
    let source_port = remote.port();
    let destination_port = local.port();

    let (session_route, listener_pending) =
        session_or_listener_pending_input_entry(session_queue, local, remote, flags)?;
    if let Some((session_id, owner, session_next)) = session_route {
        let resolved = NodeNextStorage::next(next, session_next);
        {
            let buffer = batch.buffer_mut(index)?;
            buffer.clear_node_error();
            write_session_route_opaque(buffer.opaque2_mut(), session_id, owner, session_next);
            if let (Some(_), Some(current_worker)) = (handoff, handoff_worker)
                && owner != current_worker
            {
                buffer.set_current_config(resolved);
                buffer.set_handoff_source_worker(current_worker);
            }
        }
        if let (Some(target), Some(current_worker)) = (handoff, handoff_worker)
            && owner != current_worker
        {
            handoffs.push((owner, target, index));
            if traced {
                traces.push((
                    index,
                    TcpInputTrace {
                        version: Some(version),
                        protocol: Some(protocol),
                        source_port: Some(source_port),
                        destination_port: Some(destination_port),
                        flags: u16::from(flags.bits()),
                        error: None,
                        next: resolved,
                    },
                ));
            }
            return Ok(None);
        }
        return resolve_success_next_with_trace(
            index,
            next,
            session_next,
            version,
            protocol,
            source_port,
            destination_port,
            u16::from(flags.bits()),
            traced,
            traces,
        );
    }

    if listener_pending {
        {
            let buffer = batch.buffer_mut(index)?;
            buffer.clear_node_error();
            buffer.opaque2_mut().clear();
        }
        return resolve_success_next_with_trace(
            index,
            next,
            TcpInputNext::Listen,
            version,
            protocol,
            source_port,
            destination_port,
            u16::from(flags.bits()),
            traced,
            traces,
        );
    }

    let lookup = lookup_for_packet(snapshot, local, remote);
    let (listener_next, listener_error) = tcp_listener_input_entry(flags);
    if let Some(error) = listener_error {
        return resolve_error_next_with_batch(
            runtime,
            batch,
            index,
            next,
            listener_next,
            error,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
            traces,
        );
    }
    let Some(_listener) = lookup else {
        return resolve_error_next_with_batch(
            runtime,
            batch,
            index,
            next,
            TcpInputNext::Punt,
            TcpError::ConnectionClosed,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
            traces,
        );
    };
    {
        let buffer = batch.buffer_mut(index)?;
        buffer.clear_node_error();
        buffer.opaque2_mut().clear();
    }
    resolve_success_next_with_trace(
        index,
        next,
        listener_next,
        version,
        protocol,
        source_port,
        destination_port,
        u16::from(flags.bits()),
        traced,
        traces,
    )
}

#[inline(always)]
fn resolve_success_next_with_trace(
    index: BufferIndex,
    next: &[NodeId; TcpInputNext::COUNT],
    next_key: TcpInputNext,
    version: IpVersion,
    protocol: IpProtocol,
    source_port: u16,
    destination_port: u16,
    flags: u16,
    traced: bool,
    traces: &mut Vec<(BufferIndex, TcpInputTrace)>,
) -> CoreResult<Option<NodeId>> {
    let resolved = NodeNextStorage::next(next, next_key);
    if traced {
        traces.push((
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
        ));
    }
    Ok(Some(resolved))
}

#[inline(always)]
fn resolve_error_next_with_batch(
    runtime: &DataPlaneRuntime,
    batch: &mut BufferBatchMut<'_>,
    index: BufferIndex,
    next: &[NodeId; TcpInputNext::COUNT],
    next_key: TcpInputNext,
    error: TcpError,
    version: Option<IpVersion>,
    protocol: Option<IpProtocol>,
    flags: u16,
    traced: bool,
    traces: &mut Vec<(BufferIndex, TcpInputTrace)>,
) -> CoreResult<Option<NodeId>> {
    {
        let buffer = batch.buffer_mut(index)?;
        set_buffer_node_error_code(runtime, buffer, error as u16)?;
    }
    let resolved = NodeNextStorage::next(next, next_key);
    if traced {
        traces.push((
            index,
            TcpInputTrace {
                version,
                protocol,
                source_port: None,
                destination_port: None,
                flags,
                error: Some(error as u16),
                next: resolved,
            },
        ));
    }
    Ok(Some(resolved))
}

#[inline(always)]
fn session_or_listener_pending_input_entry<C>(
    session_queue: Option<TcpQueue<C>>,
    local: SocketAddr,
    remote: SocketAddr,
    flags: TcpInputFlags,
) -> CoreResult<(Option<(SessionId, DataWorkerId, TcpInputNext)>, bool)>
where
    C: CongestionController + 'static,
{
    let Some(_) = session_queue else {
        return Ok((None, false));
    };
    let (route, listener_pending) = tcp_worker_state_mut().input_route(
        local,
        remote,
        flags.contains(TcpInputFlags::ACK) && !flags.contains(TcpInputFlags::RST),
    );
    if let Some(route) = route {
        return Ok((Some(route), false));
    }
    Ok((None, listener_pending))
}

#[inline(always)]
fn tcp_listener_input_entry(flags: TcpInputFlags) -> (TcpInputNext, Option<TcpError>) {
    if flags == TcpInputFlags::SYN {
        return (TcpInputNext::Listen, None);
    }
    if flags.contains(TcpInputFlags::RST) {
        return (TcpInputNext::Drop, None);
    }
    if flags.contains(TcpInputFlags::ACK) {
        return (TcpInputNext::Reset, Some(TcpError::AckInvalid));
    }
    if flags.contains(TcpInputFlags::SYN) {
        return (TcpInputNext::Reset, Some(TcpError::AckInvalid));
    }
    (TcpInputNext::Reset, Some(TcpError::ConnectionClosed))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use hammer_adapter::{
        BufferFrame, DataPlaneHandoff, DataPlaneRuntime, DataWorkerId, InternalNode, Node,
        NodeHandle, NodeProcessFn, NodeResult, NodeRuntimeData,
    };
    use hammer_core::error::CoreResult;
    use hammer_core::protocol::tcp::TcpConnectionId;

    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::lookup::TcpLookupSnapshot;
    use crate::transport::tcp::{
        SessionId, TcpConnection, TcpInputFlags, TcpInputNext, TcpQueue, TcpSessionDriver,
    };

    #[derive(Clone, Copy)]
    struct BlackholeNode;

    impl Node for BlackholeNode {
        fn process(
            &mut self,
            _runtime: &DataPlaneRuntime,
            frame: &mut BufferFrame,
        ) -> CoreResult<NodeResult> {
            frame.drain_pending();
            Ok(NodeResult::drop())
        }

        fn node_process(&self) -> NodeProcessFn {
            blackhole_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(NodeRuntimeData::default())
        }
    }

    impl InternalNode for BlackholeNode {}

    fn blackhole_process(
        _: &DataPlaneRuntime,
        _: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.drain_pending();
        Ok(NodeResult::drop())
    }

    macro_rules! register_tcp_input_test_nexts {
        ($runtime:expr) => {{
            let drop = $runtime.nodes().register_internal(BlackholeNode);
            let listen = $runtime.nodes().register_internal(BlackholeNode);
            let rcv_process = $runtime.nodes().register_internal(BlackholeNode);
            let syn_sent = $runtime.nodes().register_internal(BlackholeNode);
            let established = $runtime.nodes().register_internal(BlackholeNode);
            let reset = $runtime.nodes().register_internal(BlackholeNode);
            (
                drop,
                listen,
                rcv_process,
                syn_sent,
                established,
                reset,
                TcpInputNext::nodes(
                    drop,
                    listen,
                    rcv_process,
                    syn_sent,
                    established,
                    reset,
                    drop,
                ),
            )
        }};
    }

    use super::{
        TcpInputControlPlane, register_tcp_input_runtime, session_or_listener_pending_input_entry,
        tcp_worker_state,
    };
    use crate::transport::tcp::{
        TcpWorkerOwnedState, connect_tcp_session, publish_tcp_connection, set_tcp_worker_state,
    };
    #[test]
    fn tcp_input_runtime_registry_is_isolated_per_thread() {
        let main_snapshot = Arc::new(ArcSwap::from_pointee(TcpLookupSnapshot::default()));
        let main_runtime = register_tcp_input_runtime(main_snapshot)
            .usize_word(0)
            .expect("main runtime slot");
        assert_eq!(main_runtime, 0);

        let worker_runtime = std::thread::spawn(|| {
            let worker_snapshot = Arc::new(ArcSwap::from_pointee(TcpLookupSnapshot::default()));
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
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(0), 50_044);
        let session_id = route_session_id(handle, 50_044);

        let local = parsed_local(50_044);
        let remote = parsed_remote(50_044);
        let (entry, listener_pending) = session_or_listener_pending_input_entry(
            Some(handle),
            local,
            remote,
            TcpInputFlags::ACK,
        )
        .expect("session lookup");
        let entry = entry.expect("existing tcp session");
        assert!(!listener_pending);

        assert_eq!(
            entry,
            (session_id, DataWorkerId::new(0), TcpInputNext::Established)
        );
    }

    #[test]
    fn tcp_input_existing_session_entry_keeps_owner_for_handoff_decision() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(1), 50_055);
        let session_id = route_session_id(handle, 50_055);

        let local = parsed_local(50_055);
        let remote = parsed_remote(50_055);
        let (entry, listener_pending) = session_or_listener_pending_input_entry(
            Some(handle),
            local,
            remote,
            TcpInputFlags::ACK,
        )
        .expect("session lookup");
        let entry = entry.expect("existing tcp session");
        assert!(!listener_pending);

        assert_eq!(
            entry,
            (session_id, DataWorkerId::new(1), TcpInputNext::Established)
        );
    }

    #[test]
    fn tcp_input_routes_pending_syn_sent_tuple_to_syn_sent_node() {
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let worker = DataWorkerId::new(0);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let handle = crate::session::node::register_session_queue(
            TcpSessionDriver::<BbrController>::new(worker, runtime.packet_buffers().clone()),
        )
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
        let session_id = {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            connect_tcp_session(&mut queue, local, remote).expect("connect")
        };

        let local = parsed_local(local_port);
        let remote = parsed_remote(local_port);
        let (entry, listener_pending) = session_or_listener_pending_input_entry(
            Some(handle),
            local,
            remote,
            TcpInputFlags::ACK,
        )
        .expect("session lookup");
        let entry = entry.expect("pending tcp session");
        assert!(!listener_pending);

        assert_eq!(entry, (session_id, worker, TcpInputNext::SynSent));
        {
            assert_eq!(
                tcp_worker_state().session_route_by_tuple(local, remote),
                None
            );
            assert_eq!(
                tcp_worker_state().pending_route_by_tuple(local, remote),
                Some((session_id, worker, TcpInputNext::SynSent))
            );
        }
    }

    #[test]
    fn tcp_input_handoffs_existing_session_to_owner_worker() {
        const HANDOFF_HANDLE: NodeHandle = NodeHandle::new(44);

        let handoff = DataPlaneHandoff::new(2, 8);
        let runtime = DataPlaneRuntime::with_handoff(
            DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
            DataWorkerId::new(0),
            handoff.worker(DataWorkerId::new(0)),
        );
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(1), 50_066);
        let frame = runtime.alloc_frame_index().expect("alloc frame");
        let packet = tcp_packet(
            Ipv4Addr::new(198, 51, 100, 50_066u16 as u8),
            443,
            Ipv4Addr::new(192, 0, 2, 50_066u16 as u8),
            50_066,
        );
        let index = runtime
            .alloc_index_with_bytes(&packet)
            .expect("alloc packet");
        stamp_tcp_cursor(&runtime, index, &packet);
        runtime
            .get_frame_mut(frame)
            .expect("frame mut")
            .push_index(index)
            .expect("push packet");

        let control = TcpInputControlPlane::new();
        let (_, _, _, _, _, reset, nexts) = register_tcp_input_test_nexts!(runtime);
        let node = runtime.nodes().register_internal(control.node(
            nexts,
            Some(handle),
            Some((HANDOFF_HANDLE, DataWorkerId::new(0))),
        ));

        assert!(runtime.schedule_frame(node, frame).expect("schedule"));
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);

        assert_eq!(runtime.current_config(index).expect("handoff next"), reset);
        assert_eq!(
            runtime
                .handoff_source_worker(index)
                .expect("handoff source"),
            Some(DataWorkerId::new(0))
        );
    }

    #[test]
    fn tcp_input_preserves_session_route_in_opaque_for_follow_on_nodes() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let handle = install_tcp_session(&runtime, DataWorkerId::new(0), 50_088);
        let session_id = route_session_id(handle, 50_088);
        let packet = tcp_packet(
            Ipv4Addr::new(198, 51, 100, 50_088u16 as u8),
            443,
            Ipv4Addr::new(192, 0, 2, 50_088u16 as u8),
            50_088,
        );
        let index = runtime
            .alloc_index_with_bytes(&packet)
            .expect("alloc packet");
        stamp_tcp_cursor(&runtime, index, &packet);
        let frame = runtime.alloc_frame_index().expect("frame");
        runtime
            .get_frame_mut(frame)
            .expect("frame mut")
            .push_index(index)
            .expect("push packet");
        let control = TcpInputControlPlane::new();
        let (_, _, _, _, _, _, nexts) = register_tcp_input_test_nexts!(runtime);
        let node = runtime
            .nodes()
            .register_internal(control.node(nexts, Some(handle), None));

        assert!(runtime.schedule_frame(node, frame).expect("schedule"));
        assert!(runtime.run_ready_nodes().expect("run input") >= 1);

        assert_eq!(
            crate::transport::tcp::read_session_id(&runtime, index).expect("read session id"),
            Some(session_id)
        );
    }

    fn install_tcp_session(
        runtime: &DataPlaneRuntime,
        owner: DataWorkerId,
        local_port: u16,
    ) -> TcpQueue<BbrController> {
        let local = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            local_port,
        );
        let remote = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            443,
        );
        let connection = TcpConnection::<BbrController>::established_for_test(
            Some(TcpConnectionId::new(u64::from(local_port))),
            owner,
            local_port,
            Some(local),
            remote,
        );
        {
            let mut driver =
                TcpSessionDriver::<BbrController>::new(owner, runtime.packet_buffers().clone());
            let session_id = driver
                .insert_session_with_id(|_| connection.clone())
                .expect("insert session");
            publish_tcp_connection(&mut driver, session_id).expect("refresh route");
            crate::session::node::register_session_queue(driver).expect("register test queue")
        }
    }

    fn parsed_local(local_port: u16) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            local_port,
        )
    }

    fn parsed_remote(local_port: u16) -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            443,
        )
    }

    fn route_session_id(_: TcpQueue<BbrController>, local_port: u16) -> SessionId {
        let local = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, local_port as u8)),
            local_port,
        );
        let remote = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, local_port as u8)),
            443,
        );
        tcp_worker_state()
            .session_route_by_tuple(local, remote)
            .map(|(session_id, _, _)| session_id)
            .expect("session route exists")
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
        internet_checksum_parts(&[
            &source.octets(),
            &destination.octets(),
            &[0, protocol],
            &(segment.len() as u16).to_be_bytes(),
            segment,
        ])
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

    fn internet_checksum_parts(parts: &[&[u8]]) -> u16 {
        let mut sum = 0u32;
        let mut high = None;
        for part in parts {
            let mut index = 0usize;
            if let Some(hi) = high.take() {
                if let Some(&lo) = part.first() {
                    sum += u16::from_be_bytes([hi, lo]) as u32;
                    while sum > 0xffff {
                        sum = (sum & 0xffff) + (sum >> 16);
                    }
                    index = 1;
                } else {
                    high = Some(hi);
                    continue;
                }
            }
            let mut chunks = part[index..].chunks_exact(2);
            for chunk in &mut chunks {
                sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
                while sum > 0xffff {
                    sum = (sum & 0xffff) + (sum >> 16);
                }
            }
            if let [hi] = chunks.remainder() {
                high = Some(*hi);
            }
        }
        if let Some(hi) = high {
            sum += u16::from_be_bytes([hi, 0]) as u32;
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

#[cfg(test)]
pub(crate) fn stamp_session_route_for_test(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_id: SessionId,
    owner: DataWorkerId,
    next: TcpInputNext,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    write_session_route_opaque(buffer.opaque2_mut(), session_id, owner, next);
    Ok(())
}

#[inline(always)]
fn parse_tcp_input_buffer(
    buffer: &hammer_adapter::Buffer,
) -> CoreResult<
    Result<(IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags), TcpInputParseError>,
> {
    parse_tcp_input_parts(
        buffer.current(),
        buffer.total_len_not_including_first(),
        buffer.packet_cursor(),
    )
}

#[inline(always)]
fn prefetch_tcp_input(
    batch: &mut BufferBatchMut<'_>,
    indices: &[BufferIndex],
    lookup: &TcpLookupSnapshot,
    session_queue: Option<TcpQueue<impl CongestionController + 'static>>,
) {
    for index in indices.iter().copied() {
        batch.prefetch_read(index);
        let _ = batch.with_buffer(index, |buffer| {
            prefetch_lookup_for_buffer(lookup, buffer);
            prefetch_session_route_for_buffer(session_queue, buffer);
        });
    }
}

#[inline(always)]
fn parse_tcp_input_parts(
    current: &[u8],
    total_len_not_including_first: usize,
    cursor: BufferPacketCursor,
) -> CoreResult<
    Result<(IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags), TcpInputParseError>,
> {
    let parsed = match parse_ip_packet_with_chain_len(current, total_len_not_including_first) {
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
    let Some(packet) = current.get(..first_len) else {
        return Ok(Err(TcpInputParseError::BadLength));
    };
    let source_ip = source_ip(parsed.version, packet)?;
    let destination_ip = destination_ip(parsed.version, packet)?;
    let Some(transport) = packet.get(cursor.transport_header_offset()..first_len) else {
        return Ok(Err(TcpInputParseError::BadLength));
    };
    let segment = match parse_tcp_wire_header(transport) {
        Ok(segment) => segment,
        Err(_) => return Ok(Err(TcpInputParseError::BadLength)),
    };
    Ok(Ok((
        parsed.version,
        parsed.protocol,
        SocketAddr::new(destination_ip, segment.destination_port()),
        SocketAddr::new(source_ip, segment.source_port()),
        tcp_input_flags(tcp_wire_flags(segment)),
    )))
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
            let Some(source) = packet.get(12..16) else {
                return Err(TcpError::Length.into());
            };
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        IpVersion::V6 => {
            let Some(source) = packet.get(8..24) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = source.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn destination_ip(version: IpVersion, packet: &[u8]) -> CoreResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let Some(destination) = packet.get(16..20) else {
                return Err(TcpError::Length.into());
            };
            Ok(Ipv4Addr::new(
                destination[0],
                destination[1],
                destination[2],
                destination[3],
            )
            .into())
        }
        IpVersion::V6 => {
            let Some(destination) = packet.get(24..40) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = destination.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn lookup_for_packet(
    snapshot: &TcpLookupSnapshot,
    local: SocketAddr,
    remote: SocketAddr,
) -> Option<TcpLookupValue> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_addr), IpAddr::V4(_)) => snapshot
            .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                local_addr,
                local.port(),
            )),
        (IpAddr::V6(local_addr), IpAddr::V6(_)) => snapshot
            .lookup_listener::<TcpIpv6ListenerAddress>(TcpV6ListenerKey::new(
                0,
                local_addr,
                local.port(),
            )),
        _ => None,
    }
}

#[inline(always)]
fn prefetch_lookup_for_buffer(snapshot: &TcpLookupSnapshot, buffer: &hammer_adapter::Buffer) {
    let cursor = buffer.packet_cursor();
    if !valid_tcp_cursor(cursor) {
        return;
    }
    let current = buffer.current();
    let packet_len = cursor.packet_len().min(current.len());
    let Some(packet) = current.get(..packet_len) else {
        return;
    };
    let destination_port = tcp_destination_port(buffer);
    match packet.first().map(|first| first >> 4) {
        Some(4) if packet.len() >= 20 => {
            let local_addr = Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]);
            snapshot.prefetch_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                local_addr,
                destination_port,
            ));
        }
        Some(6) if packet.len() >= 40 => {
            let local_addr = Ipv6Addr::from([
                packet[24], packet[25], packet[26], packet[27], packet[28], packet[29], packet[30],
                packet[31], packet[32], packet[33], packet[34], packet[35], packet[36], packet[37],
                packet[38], packet[39],
            ]);
            snapshot.prefetch_listener::<TcpIpv6ListenerAddress>(TcpV6ListenerKey::new(
                0,
                local_addr,
                destination_port,
            ));
        }
        _ => {}
    }
}

#[inline(always)]
fn prefetch_session_route_for_buffer<C>(
    session_queue: Option<TcpQueue<C>>,
    buffer: &hammer_adapter::Buffer,
) where
    C: CongestionController + 'static,
{
    let Some(_) = session_queue else {
        return;
    };
    let cursor = buffer.packet_cursor();
    if !valid_tcp_cursor(cursor) {
        return;
    }
    let current = buffer.current();
    let packet_len = cursor.packet_len().min(current.len());
    let Some(packet) = current.get(..packet_len) else {
        return;
    };
    let (source_ip, destination_ip) = match packet.first().map(|first| first >> 4) {
        Some(4) if packet.len() >= 20 => (
            IpAddr::V4(Ipv4Addr::new(
                packet[12], packet[13], packet[14], packet[15],
            )),
            IpAddr::V4(Ipv4Addr::new(
                packet[16], packet[17], packet[18], packet[19],
            )),
        ),
        Some(6) if packet.len() >= 40 => (
            IpAddr::V6(Ipv6Addr::from([
                packet[8], packet[9], packet[10], packet[11], packet[12], packet[13], packet[14],
                packet[15], packet[16], packet[17], packet[18], packet[19], packet[20], packet[21],
                packet[22], packet[23],
            ])),
            IpAddr::V6(Ipv6Addr::from([
                packet[24], packet[25], packet[26], packet[27], packet[28], packet[29], packet[30],
                packet[31], packet[32], packet[33], packet[34], packet[35], packet[36], packet[37],
                packet[38], packet[39],
            ])),
        ),
        _ => return,
    };
    let local = SocketAddr::new(destination_ip, tcp_destination_port(buffer));
    let remote = SocketAddr::new(source_ip, tcp_source_port(buffer));
    tcp_worker_state().prefetch_tuple(local, remote);
}

#[inline(always)]
fn tcp_source_port(buffer: &hammer_adapter::Buffer) -> u16 {
    let transport = buffer.packet_cursor().transport_header_offset();
    let current = buffer.current();
    current
        .get(transport..transport + 2)
        .map(|port| u16::from_be_bytes([port[0], port[1]]))
        .unwrap_or(0)
}

#[inline(always)]
fn tcp_destination_port(buffer: &hammer_adapter::Buffer) -> u16 {
    let transport = buffer.packet_cursor().transport_header_offset();
    let current = buffer.current();
    current
        .get(transport + 2..transport + 4)
        .map(|port| u16::from_be_bytes([port[0], port[1]]))
        .unwrap_or(0)
}
