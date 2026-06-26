use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferBatchMut, BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime,
    ForwardingMetadata, InternalNode, Node, NodeId, NodeProcessFn, NodeRegistration, NodeResult,
    NodeRuntimeData, PacketTrace, SecondaryOpaque, TapEthernetMetadata, TraceFormatter,
    add_packet_trace, unlikely,
};
use hammer_core::config::RouteAction;
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_core::forwarding::{
    Adjacency as CoreAdjacency, DpoId as CoreDpoId, FibEntry as CoreFibEntry,
    FibLookupResult as CoreFibLookupResult, FibTable as CoreFibTable,
    FibTableBuilder as CoreFibTableBuilder, LoadBalance as CoreLoadBalance,
};
pub use hammer_core::forwarding::{
    AdjacencyIndex, Dpo, DpoClass, DpoProto, DpoStackRegistry, DpoType, DpoTypeRegistry,
    FibRouteDpoError, LoadBalanceError, LoadBalanceIndex,
};
use hammer_core::protocol::icmp::IcmpErrorMetadata;
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

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LookupOpaque {
    tap_ethernet: Option<TapEthernetMetadata>,
    icmp_error: Option<IcmpErrorMetadata>,
    forwarding: Option<ForwardingMetadata>,
}

const _: () = assert!(size_of::<LookupOpaque>() == size_of::<SecondaryOpaque>());

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
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
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
    fn encode_trace(&self, out: &mut hammer_infra::vec::Vec<u8>) {
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
    pub(crate) fn replace_after_barrier(&self, table: FibTable) {
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

pub(crate) fn assemble_ip_lookup_node(
    runtime: &hammer_adapter::NodeRuntime,
    _worker_id: usize,
    _: &(),
) -> CoreResult<NodeId> {
    let drop = crate::packet_graph::graph_node(runtime, "drop")?;
    let mut builder = FibTableBuilder::new(drop);
    crate::packet_graph::with_boot(|boot| {
        for route in boot.routes.iter() {
            if let RouteAction::Drop = route
                .action()
                .map_err(|err| CoreError::internal(err.to_string()))?
            {
                builder.add_drop_route(route.prefix);
            }
        }
        runtime.try_register_internal(IpLookupControlPlane::new(builder.build()).node())
    })
}

#[hammer_component_macros::graph_node(
    graph = service,
    assemble = crate::net::lookup::assemble_ip_lookup_node,
)]
#[hammer_component_macros::node]
pub struct IpLookupNode {
    #[node(default = register_ip_lookup_runtime(table.clone()))]
    runtime_data: NodeRuntimeData,
    table: FibTableHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl IpLookupNode {
    #[inline(always)]
    fn prefetch_buffer_with_batch(batch: &mut BufferBatchMut<'_>, index: BufferIndex) {
        batch.prefetch_header(index);
    }

    #[inline(always)]
    fn cached_packet_for_index(
        batch: &mut BufferBatchMut<'_>,
        index: BufferIndex,
    ) -> Option<ParsedIpPacket> {
        let buffer = batch.buffer(index).ok()?;
        packet_from_cached_metadata(
            unsafe { transmute::<_, &hammer_adapter::NetworkOpaque>(buffer.opaque()) },
            buffer.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        )
    }

    #[inline(always)]
    fn prefetch_lookup_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        index: BufferIndex,
    ) {
        let Some(parsed) = Self::cached_packet_for_index(batch, index) else {
            return;
        };
        table.prefetch_packet(&parsed);
    }

    #[inline(always)]
    fn process_index_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        index: BufferIndex,
        traces: &mut std::vec::Vec<(BufferIndex, IpLookupTrace)>,
    ) -> CoreResult<NodeId> {
        let parsed = Self::cached_packet_for_index(batch, index);
        let (traced, parsed) = {
            let buffer = batch.buffer(index)?;
            let traced = buffer.trace_handle().is_some();
            let parsed = parsed.or_else(|| {
                parse_ip_packet_with_chain_len(
                    buffer.current(),
                    buffer.total_len_not_including_first(),
                )
                .ok()
            });
            (traced, parsed)
        };
        let parsed = match parsed {
            Some(parsed) => parsed,
            None => {
                let buffer = batch.buffer_mut(index)?;
                let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
                opaque.forwarding = None;
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
        let buffer = batch.buffer_mut(index)?;
        let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
        opaque.forwarding = Some(ForwardingMetadata {
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
    fn prefetch_lookup_range_with_batch(
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
            Self::prefetch_lookup_with_batch(batch, table, index);
        }
    }

    #[inline(always)]
    fn prefetch_buffer_range_with_batch(
        batch: &mut BufferBatchMut<'_>,
        indices: &[BufferIndex],
        offset: usize,
        width: usize,
    ) {
        if offset >= indices.len() {
            return;
        }
        let end = (offset + width).min(indices.len());
        for index in indices[offset..end].iter().copied() {
            Self::prefetch_buffer_with_batch(batch, index);
        }
    }

    #[inline(always)]
    fn prefetch_buffer_indices_with_batch(batch: &mut BufferBatchMut<'_>, indices: &[BufferIndex]) {
        for index in indices.iter().copied() {
            Self::prefetch_buffer_with_batch(batch, index);
        }
    }

    #[inline(always)]
    fn prefetch_lookup_indices_with_batch(
        batch: &mut BufferBatchMut<'_>,
        table: &FibTable,
        indices: &[BufferIndex],
    ) {
        for index in indices.iter().copied() {
            Self::prefetch_lookup_with_batch(batch, table, index);
        }
    }
}

impl Node for IpLookupNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
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
            Self::prefetch_buffer_range_with_batch(&mut batch, indices, 0, width);
            Self::prefetch_lookup_range_with_batch(&mut batch, &table, indices, 0, width);
            Self::process_index_with_batch(&mut batch, &table, first, &mut traces)?
        };
        let mut first_chunk = true;
        hammer_adapter::node_route_frame_cached!(
            self,
            runtime,
            frame,
            |batch, indices| {
                Self::prefetch_buffer_indices_with_batch(batch, indices);
                Self::prefetch_lookup_indices_with_batch(batch, &table, indices);
            },
            |batch, indices, nexts| {
                let start_offset = if first_chunk {
                    first_chunk = false;
                    nexts[0] = Some(first_next);
                    1
                } else {
                    0
                };
                for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
                    nexts[offset] = Some(Self::process_index_with_batch(
                        batch,
                        &table,
                        index,
                        &mut traces,
                    )?);
                }
                Ok(())
            },
            {
                for (index, trace) in traces {
                    add_packet_trace!(runtime, index, trace)?;
                }
            }
        )
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip_lookup_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_lookup_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for IpLookupNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
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
    #[node(default = register_adjacency_rewrite_runtime(table.clone()))]
    runtime_data: NodeRuntimeData,
    table: FibTableHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl AdjacencyRewriteNode {
    #[inline(always)]
    fn next_for_index(
        table: &FibTableHandle,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<Option<NodeId>> {
        let forwarding = {
            let buffer = runtime.get_buffer(index)?;
            let opaque = unsafe { transmute::<_, &LookupOpaque>(buffer.opaque2()) };
            opaque.forwarding
        };
        let Some(forwarding) = forwarding else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingForwarding.code(),
            )?;
            add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: None,
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::MissingForwarding.code()),
                    next: None,
                },
            )?;
            runtime.free_index(index);
            return Ok(None);
        };
        if forwarding.dpo_type != DpoType::ADJACENCY {
            set_index_node_error_code(runtime, index, AdjacencyRewriteNodeError::WrongDpo.code())?;
            add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::WrongDpo.code()),
                    next: None,
                },
            )?;
            runtime.free_index(index);
            return Ok(None);
        }
        let Some(adjacency) = table
            .table()
            .adjacency(AdjacencyIndex::new(forwarding.dpo_index))
        else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingAdjacency.code(),
            )?;
            add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::MissingAdjacency.code()),
                    next: None,
                },
            )?;
            runtime.free_index(index);
            return Ok(None);
        };
        let rewrite_len = adjacency.rewrite.as_slice().len();
        let egress_interface = adjacency.egress_interface;
        let next = adjacency.next;
        apply_adjacency_rewrite(runtime, index, adjacency)?;
        add_packet_trace!(
            runtime,
            index,
            AdjacencyRewriteTrace {
                dpo_index: Some(forwarding.dpo_index),
                egress_interface,
                rewrite_len,
                error: None,
                next: Some(next),
            },
        )?;
        Ok(Some(next))
    }
}

impl Node for AdjacencyRewriteNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        hammer_adapter::node_route_frame_index_cached!(self, runtime, frame, |index| {
            Self::next_for_index(&self.table, runtime, index)
        })
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_adjacency_rewrite_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        adjacency_rewrite_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for AdjacencyRewriteNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
    }
}

#[derive(Clone)]
struct LookupRuntime {
    table: FibTableHandle,
}

#[derive(Clone)]
struct AdjacencyRewriteRuntime {
    table: FibTableHandle,
}

fn lookup_runtimes() -> &'static Mutex<Vec<LookupRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<LookupRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn adjacency_rewrite_runtimes() -> &'static Mutex<Vec<AdjacencyRewriteRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<AdjacencyRewriteRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_ip_lookup_runtime(table: FibTableHandle) -> NodeRuntimeData {
    let mut runtimes = lookup_runtimes()
        .lock()
        .expect("IP lookup runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(LookupRuntime { table });
    NodeRuntimeData::from_usize(slot).expect("IP lookup runtime slot overflow")
}

fn register_adjacency_rewrite_runtime(table: FibTableHandle) -> NodeRuntimeData {
    let mut runtimes = adjacency_rewrite_runtimes()
        .lock()
        .expect("adjacency rewrite runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(AdjacencyRewriteRuntime { table });
    NodeRuntimeData::from_usize(slot).expect("adjacency rewrite runtime slot overflow")
}

fn ip_lookup_runtime(data: NodeRuntimeData) -> CoreResult<LookupRuntime> {
    let slot = data.usize_word(0)?;
    lookup_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("IP lookup runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("IP lookup runtime slot is invalid"))
}

fn adjacency_rewrite_runtime(data: NodeRuntimeData) -> CoreResult<AdjacencyRewriteRuntime> {
    let slot = data.usize_word(0)?;
    adjacency_rewrite_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("adjacency rewrite runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("adjacency rewrite runtime slot is invalid"))
}

fn ip_lookup_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = ip_lookup_runtime(data)?;
    let table = state.table.table();
    let indices = frame.pending_indices();
    let Some(first) = indices.first().copied() else {
        return Ok(NodeResult::drop());
    };
    let width = frame_batch_width(runtime);
    let mut traces = std::vec::Vec::new();
    let first_next = {
        let mut batch = runtime.buffer_batch_mut();
        IpLookupNode::prefetch_buffer_range_with_batch(&mut batch, indices, 0, width);
        IpLookupNode::prefetch_lookup_range_with_batch(&mut batch, &table, indices, 0, width);
        IpLookupNode::process_index_with_batch(&mut batch, &table, first, &mut traces)?
    };
    let mut first_chunk = true;
    hammer_adapter::node_route_frame_static!(
        Some(first_next),
        runtime,
        frame,
        |batch, indices| {
            IpLookupNode::prefetch_buffer_indices_with_batch(batch, indices);
            IpLookupNode::prefetch_lookup_indices_with_batch(batch, &table, indices);
        },
        |batch, indices, nexts| {
            let start_offset = if first_chunk {
                first_chunk = false;
                nexts[0] = Some(first_next);
                1
            } else {
                0
            };
            for (offset, index) in indices.iter().copied().enumerate().skip(start_offset) {
                nexts[offset] = Some(IpLookupNode::process_index_with_batch(
                    batch,
                    &table,
                    index,
                    &mut traces,
                )?);
            }
            Ok(())
        },
        {
            for (index, trace) in traces {
                add_packet_trace!(runtime, index, trace)?;
            }
        }
    )
}

fn adjacency_rewrite_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = adjacency_rewrite_runtime(data)?;
    hammer_adapter::node_route_frame_index_static!(None, runtime, frame, |index| {
        AdjacencyRewriteNode::next_for_index(&state.table, runtime, index)
    })
}

#[inline(always)]
fn apply_adjacency_rewrite(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    adjacency: Adjacency,
) -> CoreResult<()> {
    let rewrite = adjacency.rewrite.as_slice();
    let mut buffer = runtime.get_buffer_mut(index)?;
    if !rewrite.is_empty() {
        buffer.advance(
            -isize::try_from(rewrite.len())
                .map_err(|_| CoreError::internal("adjacency rewrite length exceeds isize"))?,
        )?;
        buffer.current_mut()[..rewrite.len()].copy_from_slice(rewrite);
    }
    if !rewrite.is_empty() {
        let cursor = buffer.packet_cursor();
        buffer.set_packet_cursor(shift_packet_cursor(cursor, rewrite.len()));
    }
    unsafe { transmute::<_, &mut hammer_adapter::NetworkOpaque>(buffer.opaque_mut()) }
        .sw_if_index[1] = adjacency.egress_interface.unwrap_or(0);
    if !rewrite.is_empty() {
        let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
        opaque.tap_ethernet = None;
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
fn frame_batch_width(runtime: &DataPlaneRuntime) -> usize {
    match runtime.preferred_frame_batch_width() {
        hammer_adapter::FrameBatchWidth::Quad => 4,
        hammer_adapter::FrameBatchWidth::Pair => 2,
    }
}

#[inline(always)]
fn packet_from_cached_metadata(
    network: &hammer_adapter::NetworkOpaque,
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
    let version = match network.ip().ip_version()? {
        4 => IpVersion::V4,
        6 => IpVersion::V6,
        _ => return None,
    };
    let protocol = match network.ip().ip_protocol()? {
        1 => IpProtocol::Icmpv4,
        6 => IpProtocol::Tcp,
        17 => IpProtocol::Udp,
        58 => IpProtocol::Icmpv6,
        other => IpProtocol::Other(other),
    };
    let (source, destination) = match version {
        IpVersion::V4 => {
            let source_offset = cursor.network_header_offset() + 12;
            let destination_offset = cursor.network_header_offset() + 16;
            if destination_offset + 4 > current_len {
                return None;
            }
            (
                IpAddr::V4(Ipv4Addr::new(
                    unsafe { *current_ptr.add(source_offset) },
                    unsafe { *current_ptr.add(source_offset + 1) },
                    unsafe { *current_ptr.add(source_offset + 2) },
                    unsafe { *current_ptr.add(source_offset + 3) },
                )),
                IpAddr::V4(Ipv4Addr::new(
                    unsafe { *current_ptr.add(destination_offset) },
                    unsafe { *current_ptr.add(destination_offset + 1) },
                    unsafe { *current_ptr.add(destination_offset + 2) },
                    unsafe { *current_ptr.add(destination_offset + 3) },
                )),
            )
        }
        IpVersion::V6 => {
            let source_offset = cursor.network_header_offset() + 8;
            let destination_offset = cursor.network_header_offset() + 24;
            if destination_offset + 16 > current_len {
                return None;
            }
            let mut source_bytes = [0u8; 16];
            let mut destination_bytes = [0u8; 16];
            for (index, byte) in source_bytes.iter_mut().enumerate() {
                *byte = unsafe { *current_ptr.add(source_offset + index) };
            }
            for (index, byte) in destination_bytes.iter_mut().enumerate() {
                *byte = unsafe { *current_ptr.add(destination_offset + index) };
            }
            (
                IpAddr::V6(Ipv6Addr::from(source_bytes)),
                IpAddr::V6(Ipv6Addr::from(destination_bytes)),
            )
        }
    };
    Some(ParsedIpPacket {
        version,
        protocol,
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
