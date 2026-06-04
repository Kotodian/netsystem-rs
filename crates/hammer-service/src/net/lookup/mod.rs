use std::cell::UnsafeCell;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;

use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime,
    ForwardingMetadata, InternalNode, Network, Node, NodeId, NodeNextFrames, NodeNextVectorEnqueue,
    NodeRegistration, NodeResult, PacketTrace, RouteMetadata, TraceFormatter, unlikely,
};
use hammer_core::error::{CoreResult, HammerResult};
use hammer_core::forwarding::{
    Adjacency as CoreAdjacency, DpoId as CoreDpoId, FibEntry as CoreFibEntry,
    FibLookupResult as CoreFibLookupResult, FibTable as CoreFibTable,
    FibTableBuilder as CoreFibTableBuilder, LoadBalance as CoreLoadBalance,
};
pub use hammer_core::forwarding::{
    AdjacencyIndex, Dpo, DpoClass, DpoProto, DpoStackRegistry, DpoType, DpoTypeRegistry,
    FibRouteDpoError, LoadBalanceError, LoadBalanceIndex,
};
use hammer_core::protocol::ip::{
    IpProtocol, IpVersion, ParsedIpPacket, parse_ip_packet_with_chain_len,
};
use hammer_runtime::{ControlThreadHandle, DataPlaneBarrierHandle};

use crate::data_plane::set_index_node_error_code;
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_dpo_type, put_option_node, put_option_u16,
    put_option_u32, put_u32, put_usize,
};

pub type Adjacency = CoreAdjacency<NodeId>;
pub type DpoId = CoreDpoId<NodeId>;
pub type FibEntry = CoreFibEntry<NodeId>;
pub type FibLookupResult = CoreFibLookupResult<NodeId>;
pub type FibTable = CoreFibTable<NodeId>;
pub type FibTableBuilder = CoreFibTableBuilder<NodeId>;
pub type LoadBalance = CoreLoadBalance<NodeId>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpLookupTrace {
    pub fib_index: u32,
    pub route_dpo_type: Option<DpoType>,
    pub route_dpo_index: Option<u32>,
    pub load_balance_index: Option<u32>,
    pub bucket_index: Option<u16>,
    pub dpo_type: Option<DpoType>,
    pub dpo_index: Option<u32>,
    pub next: NodeId,
}

impl IpLookupTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            fib_index: cursor.read_u32()?,
            route_dpo_type: cursor.read_option_dpo_type()?,
            route_dpo_index: cursor.read_option_u32()?,
            load_balance_index: cursor.read_option_u32()?,
            bucket_index: cursor.read_option_u16()?,
            dpo_type: cursor.read_option_dpo_type()?,
            dpo_index: cursor.read_option_u32()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpLookupTrace {
    #[inline]
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_u32(out, self.fib_index);
        put_option_dpo_type(out, self.route_dpo_type);
        put_option_u32(out, self.route_dpo_index);
        put_option_u32(out, self.load_balance_index);
        put_option_u16(out, self.bucket_index);
        put_option_dpo_type(out, self.dpo_type);
        put_option_u32(out, self.dpo_index);
        put_node(out, self.next);
    }
}

fn format_ip_lookup_trace(bytes: &[u8]) -> String {
    match IpLookupTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IpLookupTrace invalid={bytes:?}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyRewriteTrace {
    pub dpo_index: Option<u32>,
    pub egress_interface: Option<u32>,
    pub rewrite_len: usize,
    pub error: Option<u16>,
    pub next: Option<NodeId>,
}

impl AdjacencyRewriteTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            dpo_index: cursor.read_option_u32()?,
            egress_interface: cursor.read_option_u32()?,
            rewrite_len: cursor.read_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_option_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for AdjacencyRewriteTrace {
    #[inline]
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_u32(out, self.dpo_index);
        put_option_u32(out, self.egress_interface);
        put_usize(out, self.rewrite_len);
        put_option_u16(out, self.error);
        put_option_node(out, self.next);
    }
}

fn format_adjacency_rewrite_trace(bytes: &[u8]) -> String {
    match AdjacencyRewriteTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("AdjacencyRewriteTrace invalid={bytes:?}"),
    }
}

#[derive(Clone)]
pub struct FibTableHandle {
    inner: Arc<FibTableSlot>,
}

struct FibTableSlot {
    table: UnsafeCell<FibTable>,
}

impl FibTableHandle {
    #[inline]
    pub fn new(table: FibTable) -> Self {
        Self {
            inner: Arc::new(FibTableSlot::new(table)),
        }
    }

    #[inline]
    pub fn table(&self) -> &FibTable {
        self.inner.table()
    }

    #[inline]
    fn replace_after_barrier(&self, table: FibTable) {
        self.inner.replace_after_barrier(table);
    }
}

impl fmt::Debug for FibTableHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FibTableHandle").finish_non_exhaustive()
    }
}

impl FibTableSlot {
    #[inline]
    fn new(table: FibTable) -> Self {
        Self {
            table: UnsafeCell::new(table),
        }
    }

    #[inline]
    fn table(&self) -> &FibTable {
        // SAFETY: FIB table writes are serialized by the runtime data-plane
        // barrier before publication. Data-plane nodes only take immutable
        // references while workers are running.
        unsafe { &*self.table.get() }
    }

    #[inline]
    fn replace_after_barrier(&self, table: FibTable) {
        // SAFETY: callers replace the table either while the runtime
        // data-plane barrier is held, or during single-threaded graph setup in
        // tests before packets are processed.
        unsafe {
            *self.table.get() = table;
        }
    }
}

unsafe impl Send for FibTableSlot {}
unsafe impl Sync for FibTableSlot {}

pub struct IpLookupControlPlane {
    table: FibTableHandle,
    control_handle: Option<Arc<ControlThreadHandle>>,
    barrier: Option<DataPlaneBarrierHandle>,
}

impl IpLookupControlPlane {
    #[inline]
    pub fn new(table: FibTable) -> Self {
        Self {
            table: FibTableHandle::new(table),
            control_handle: None,
            barrier: None,
        }
    }

    #[inline]
    pub fn from_handle(table: FibTableHandle) -> Self {
        Self {
            table,
            control_handle: None,
            barrier: None,
        }
    }

    #[inline]
    pub fn with_control_handle(mut self, control_handle: Arc<ControlThreadHandle>) -> Self {
        self.control_handle = Some(control_handle);
        self
    }

    #[inline]
    pub fn with_barrier(mut self, barrier: DataPlaneBarrierHandle) -> Self {
        self.barrier = Some(barrier);
        self
    }

    #[inline]
    pub fn table_handle(&self) -> FibTableHandle {
        self.table.clone()
    }

    #[inline]
    pub fn node(&self) -> IpLookupNode {
        IpLookupNode::new(self.table_handle())
    }

    #[inline]
    pub fn publish(&self, table: FibTable) -> HammerResult<()> {
        let table_handle = self.table.clone();
        let barrier = self.barrier.clone();
        let publish = move || {
            if let Some(barrier) = barrier {
                barrier.synchronize(|| {
                    table_handle.replace_after_barrier(table);
                    Ok(())
                })
            } else {
                table_handle.replace_after_barrier(table);
                Ok(())
            }
        };
        if let Some(control_handle) = &self.control_handle {
            control_handle.call(publish)??;
        } else {
            publish()?;
        }
        Ok(())
    }
}

#[hammer_component_macros::node]
pub struct IpLookupNode {
    table: FibTableHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl IpLookupNode {
    #[inline(always)]
    fn process_index_with_batch(
        &self,
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        index: BufferIndex,
        traces: &mut std::vec::Vec<(BufferIndex, IpLookupTrace)>,
    ) -> CoreResult<NodeId> {
        let buffer = batch.buffer_mut(index)?;
        let traced = buffer.trace_mark().is_some();
        let parsed = match packet_from_cached_metadata(
            buffer.metadata(),
            buffer.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        )
        .or_else(|| {
            parse_ip_packet_with_chain_len(buffer.current(), buffer.total_len_not_including_first())
                .ok()
        }) {
            Some(parsed) => parsed,
            None => {
                buffer.metadata_mut().forwarding = None;
                let next = table.drop_next();
                if unlikely(traced) {
                    traces.push((
                        index,
                        IpLookupTrace {
                            fib_index: 0,
                            route_dpo_type: None,
                            route_dpo_index: None,
                            load_balance_index: None,
                            bucket_index: None,
                            dpo_type: None,
                            dpo_index: None,
                            next,
                        },
                    ));
                }
                return Ok(next);
            }
        };
        let result = table
            .lookup_packet(&parsed)
            .unwrap_or_else(|| FibLookupResult::terminal(table.drop_dpo(parsed.version)));
        buffer.metadata_mut().forwarding = Some(ForwardingMetadata {
            fib_index: 0,
            route_dpo_type: result.route_dpo.kind(),
            route_dpo_index: result.route_dpo.forwarding_index(),
            load_balance_index: result.forwarding_load_balance_index(),
            bucket_index: result.forwarding_bucket_index(),
            dpo_type: result.dpo.kind(),
            dpo_index: result.dpo.forwarding_index(),
        });
        if unlikely(traced) {
            traces.push((
                index,
                IpLookupTrace {
                    fib_index: 0,
                    route_dpo_type: Some(result.route_dpo.kind()),
                    route_dpo_index: Some(result.route_dpo.forwarding_index()),
                    load_balance_index: Some(result.forwarding_load_balance_index()),
                    bucket_index: Some(result.forwarding_bucket_index()),
                    dpo_type: Some(result.dpo.kind()),
                    dpo_index: Some(result.dpo.forwarding_index()),
                    next: result.dpo.next(),
                },
            ));
        }
        Ok(result.dpo.next())
    }

    #[inline(always)]
    fn prefetch_index_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        index: BufferIndex,
    ) {
        batch.prefetch_read(index);
        let Ok(buffer) = batch.buffer(index) else {
            return;
        };
        let Some(parsed) = packet_from_cached_metadata(
            buffer.metadata(),
            buffer.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        ) else {
            return;
        };
        table.prefetch_packet(&parsed);
    }

    #[inline(always)]
    fn prefetch_range_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
    ) {
        if offset >= indices.len() {
            return;
        }
        let end = (offset + width).min(indices.len());
        for index in indices[offset..end].iter().copied() {
            Self::prefetch_index_with_batch(batch, table, index);
        }
    }

    #[inline(always)]
    fn prefetch_indices_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        indices: &[BufferIndex],
    ) {
        for index in indices.iter().copied() {
            Self::prefetch_index_with_batch(batch, table, index);
        }
    }

    #[inline(always)]
    fn process_indices_with_batch(
        &self,
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        indices: &[BufferIndex],
        nexts: &mut [NodeId; 4],
        start_offset: usize,
        traces: &mut std::vec::Vec<(BufferIndex, IpLookupTrace)>,
    ) -> CoreResult<()> {
        for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
            nexts[offset] = self.process_index_with_batch(batch, table, index, traces)?;
        }
        Ok(())
    }
}

impl<G> Node<G> for IpLookupNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let table = self.table.table();
        let indices = frame.pending_indices();
        let Some(first) = indices.first().copied() else {
            return Ok(NodeResult::drop());
        };
        let width = frame_batch_width(runtime);
        let mut traces = std::vec::Vec::new();
        let first_next = {
            let mut batch = runtime.buffer_batch_mut();
            Self::prefetch_range_with_batch(&mut batch, &table, indices, 0, width);
            self.process_index_with_batch(&mut batch, &table, first, &mut traces)?
        };
        let cached_next = self.cached_next.unwrap_or(first_next);
        let mut first_chunk = true;
        let (result, cached_next) = NodeNextVectorEnqueue::new(cached_next)
            .enqueue_frame_with_buffer_batch_chunks(
                runtime,
                frame,
                |batch, indices| {
                    Self::prefetch_indices_with_batch(batch, &table, indices);
                },
                |batch, indices, nexts| {
                    let start_offset = if first_chunk {
                        first_chunk = false;
                        nexts[0] = first_next;
                        1
                    } else {
                        0
                    };
                    self.process_indices_with_batch(
                        batch,
                        &table,
                        indices,
                        nexts,
                        start_offset,
                        &mut traces,
                    )
                },
            )?;
        for (index, trace) in traces {
            runtime.add_trace(index, trace)?;
        }
        self.cached_next = Some(cached_next);
        Ok(result)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_lookup_trace)
    }
}

impl<G> InternalNode<G> for IpLookupNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("ip-lookup-node", 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyRewriteNodeError {
    MissingForwarding,
    WrongDpo,
    MissingAdjacency,
}

impl AdjacencyRewriteNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        match self {
            Self::MissingForwarding => 1,
            Self::WrongDpo => 2,
            Self::MissingAdjacency => 3,
        }
    }
}

#[hammer_component_macros::node]
pub struct AdjacencyRewriteNode {
    table: FibTableHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl AdjacencyRewriteNode {
    #[inline(always)]
    fn next_for_index<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<Option<NodeId>> {
        let metadata = runtime.metadata(index)?;
        let traced = runtime.get_buffer(index)?.trace_mark().is_some();
        let Some(forwarding) = metadata.forwarding else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingForwarding.code(),
            )?;
            if unlikely(traced) {
                runtime.add_trace(
                    index,
                    AdjacencyRewriteTrace {
                        dpo_index: None,
                        egress_interface: None,
                        rewrite_len: 0,
                        error: Some(AdjacencyRewriteNodeError::MissingForwarding.code()),
                        next: None,
                    },
                )?;
            }
            runtime.free_index(index);
            return Ok(None);
        };
        if forwarding.dpo_type != DpoType::ADJACENCY {
            set_index_node_error_code(runtime, index, AdjacencyRewriteNodeError::WrongDpo.code())?;
            if unlikely(traced) {
                runtime.add_trace(
                    index,
                    AdjacencyRewriteTrace {
                        dpo_index: Some(forwarding.dpo_index),
                        egress_interface: None,
                        rewrite_len: 0,
                        error: Some(AdjacencyRewriteNodeError::WrongDpo.code()),
                        next: None,
                    },
                )?;
            }
            runtime.free_index(index);
            return Ok(None);
        }
        let Some(adjacency) = self
            .table
            .table()
            .adjacency(AdjacencyIndex::new(forwarding.dpo_index))
        else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingAdjacency.code(),
            )?;
            if unlikely(traced) {
                runtime.add_trace(
                    index,
                    AdjacencyRewriteTrace {
                        dpo_index: Some(forwarding.dpo_index),
                        egress_interface: None,
                        rewrite_len: 0,
                        error: Some(AdjacencyRewriteNodeError::MissingAdjacency.code()),
                        next: None,
                    },
                )?;
            }
            runtime.free_index(index);
            return Ok(None);
        };
        let rewrite_len = adjacency.rewrite.as_slice().len();
        let egress_interface = adjacency.egress_interface;
        let next = adjacency.next;
        apply_adjacency_rewrite(runtime, index, adjacency)?;
        if unlikely(traced) {
            runtime.add_trace(
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface,
                    rewrite_len,
                    error: None,
                    next: Some(next),
                },
            )?;
        }
        Ok(Some(next))
    }
}

impl<G> Node<G> for AdjacencyRewriteNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut next_frames = NodeNextFrames::default();
        let mut current_next = self.cached_next;
        let mut last_next = None;
        let result =
            frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
                let Some(node) = self.next_for_index(runtime, index)? else {
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
        Some(format_adjacency_rewrite_trace)
    }
}

impl<G> InternalNode<G> for AdjacencyRewriteNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("adjacency-rewrite-node", 0)
    }
}

#[inline(always)]
fn apply_adjacency_rewrite<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    adjacency: Adjacency,
) -> CoreResult<()> {
    let rewrite = adjacency.rewrite.as_slice();
    if !rewrite.is_empty() {
        if runtime.current_data(index)? >= rewrite.len() {
            runtime.prepend(index, rewrite)?;
        } else {
            let packet = runtime.copy_current_chain(index)?;
            runtime.truncate_chain(index, 0)?;
            runtime.append(index, rewrite)?;
            runtime.append(index, &packet)?;
        }
    }

    let mut buffer = runtime.get_buffer_mut(index)?;
    if !rewrite.is_empty() {
        let cursor = buffer.packet_cursor();
        buffer.set_packet_cursor(shift_packet_cursor(cursor, rewrite.len()));
    }
    let metadata = buffer.metadata_mut();
    metadata.egress_interface = adjacency.egress_interface;
    if !rewrite.is_empty() {
        metadata.tap_ethernet = None;
    }
    Ok(())
}

#[inline(always)]
fn shift_packet_cursor(cursor: BufferPacketCursor, delta: usize) -> BufferPacketCursor {
    if cursor.packet_len() == 0 {
        return cursor;
    }
    BufferPacketCursor::new()
        .with_packet_len(cursor.packet_len() + delta)
        .with_network_header(
            cursor.network_header_offset() + delta,
            cursor.network_header_len(),
        )
        .with_transport_header(
            cursor.transport_header_offset() + delta,
            cursor.transport_header_len(),
        )
        .with_transport_payload_offset(cursor.transport_payload_offset() + delta)
}

#[inline(always)]
fn frame_batch_width<G>(runtime: &DataPlaneRuntime<G>) -> usize {
    match runtime.preferred_frame_batch_width() {
        hammer_adapter::FrameBatchWidth::Quad => 4,
        hammer_adapter::FrameBatchWidth::Pair => 2,
    }
}

#[inline(always)]
fn packet_from_cached_metadata(
    metadata: &RouteMetadata,
    cursor: BufferPacketCursor,
    current_ptr: *const u8,
    current_len: usize,
    tail_len: usize,
) -> Option<ParsedIpPacket> {
    if cursor.packet_len() == 0 || current_ptr.is_null() {
        return None;
    }
    let chain_len = current_len.checked_add(tail_len)?;
    if cursor.network_header_offset() > current_len || cursor.packet_len() > chain_len {
        return None;
    }
    let source = metadata.source.as_ref()?.host;
    let destination = metadata.destination.as_ref()?.host;
    let version = match (source, destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => IpVersion::V4,
        (IpAddr::V6(_), IpAddr::V6(_)) => IpVersion::V6,
        _ => return None,
    };
    Some(ParsedIpPacket {
        version,
        protocol: protocol_from_network(metadata.network, version),
        input_target: hammer_core::protocol::ip::IpInputTarget::Lookup,
        input_error: hammer_core::protocol::ip::IpInputError::None,
        source,
        destination,
        packet_len: cursor.packet_len(),
        network_header_offset: cursor.network_header_offset(),
        network_header_len: cursor.network_header_len(),
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
    })
}

#[inline(always)]
fn protocol_from_network(network: Network, version: IpVersion) -> IpProtocol {
    match network {
        Network::Tcp => IpProtocol::Tcp,
        Network::Udp => IpProtocol::Udp,
        Network::Icmp => match version {
            IpVersion::V4 => IpProtocol::Icmpv4,
            IpVersion::V6 => IpProtocol::Icmpv6,
        },
    }
}
