use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwapOption;
use hammer_adapter::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace, unlikely,
};
use hammer_core::config::{Config, Route, RouteAction};
use hammer_core::data_plane::{
    BufferFrame, BufferIndex, BufferPacketCursor, NodeId, NodeRegistration, SecondaryOpaque,
};
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
use hammer_core::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{ControlThreadHandle, DataPlaneBarrierHandle};

use crate::data_plane::set_index_node_error_code;
use crate::net::{ForwardingMetadata, NetworkOpaque, TapEthernetMetadata};
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

/// IP-lookup subsystem control-plane main (VPP `ip_main_t`).
///
/// Owns the static routes read from config. The FIB table is built in two
/// phases so that graph registration is order-independent:
///   1. `register_node` runs inside `DataPlaneRuntime::init_graph` as the
///      `ip-lookup` init fn. linkme `SERVICE_GRAPH_NODES` section order is
///      non-deterministic per build, so the `drop` node may not be registered
///      yet at this point. The node is therefore registered with a placeholder
///      table whose `drop` next-node id is [`DROP_PLACEHOLDER`].
///   2. `wire_drop` runs after `init_graph` has registered every node, resolves
///      `drop` by name, rebuilds the real FIB table, and swaps it into every
///      per-worker `IpLookupNode` handle via `FibTableHandle::replace_after_barrier`.
/// No packet flows between the two phases (graph setup completes before the
/// data plane starts), so the placeholder never reaches the hot path.
pub struct IpMain {
    routes: Arc<[Route]>,
}

/// Sentinel `drop` next-node id used by [`IpMain::register_node`] when the real
/// `drop` node is not registered yet. Replaced by [`IpMain::wire_drop`] before
/// any packet is processed.
const DROP_PLACEHOLDER: NodeId = NodeId::new(u32::MAX);

impl IpMain {
    pub fn new(routes: Arc<[Route]>) -> Self {
        Self { routes }
    }

    fn build_table(&self, drop: NodeId) -> CoreResult<FibTable> {
        let mut builder = FibTableBuilder::new(drop);
        for route in self.routes.iter() {
            if let RouteAction::Drop = route
                .action()
                .map_err(|err| CoreError::internal(err.to_string()))?
            {
                builder.add_drop_route(route.prefix);
            }
        }
        Ok(builder.build())
    }

    /// Build and register this worker's `IpLookupNode` with a placeholder
    /// FIB table. The real table is installed by [`wire_drop`] after every
    /// graph node has registered. `IpLookupNode` is not congestion-typed, so
    /// this is plain (no `with_congestion!`).
    pub fn register_node(&self, rt: &DataPlaneRuntime) -> CoreResult<NodeId> {
        let placeholder = self.build_table(DROP_PLACEHOLDER)?;
        let node = IpLookupControlPlane::new(placeholder).node();
        rt.nodes().try_register_internal(node)
    }

    /// Resolve the `drop` node by name and install the real FIB table on every
    /// per-worker `IpLookupNode` handle. Must run after `init_graph` has
    /// registered all nodes, so registration order across the linkme slice does
    /// not matter.
    pub fn wire_drop(&self, rt: &DataPlaneRuntime) -> CoreResult<()> {
        let drop = rt
            .nodes()
            .node_by_name("drop")
            .ok_or_else(|| CoreError::internal("drop node not registered"))?;
        let table = self.build_table(drop)?;
        let runtimes = lookup_runtimes()
            .lock()
            .map_err(|_| CoreError::internal("IP lookup runtime registry poisoned"))?;
        for runtime in runtimes.iter() {
            runtime.table.replace_after_barrier(table.clone());
        }
        Ok(())
    }
}

pub static IP_MAIN: ArcSwapOption<IpMain> = ArcSwapOption::const_empty();

#[cfg(test)]
pub(crate) fn reset_for_test() {
    IP_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<Config>()?;
    let routes = Arc::from(config.network.route.clone().into_boxed_slice());
    IP_MAIN.store(Some(Arc::new(IpMain::new(routes))));
    Ok(())
}

#[hammer_component_macros::init_function(name = "ip_init", runs_after = ["transport_init", "tcp_init"])]
fn init_ip(engine: &mut hammer_runtime::Engine) -> HammerResult<()> {
    init(&engine.registry)
}

pub fn register_ip_lookup(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    IP_MAIN
        .load()
        .as_deref()
        .ok_or_else(|| CoreError::internal("ip main not initialized"))?
        .register_node(runtime)
}

/// Resolve the `drop` next-node and install the real FIB table on every
/// per-worker `IpLookupNode`. Call after `DataPlaneRuntime::init_graph` has
/// registered all graph nodes for this worker.
pub fn wire_ip_lookup_drop(runtime: &DataPlaneRuntime) -> CoreResult<()> {
    IP_MAIN
        .load()
        .as_deref()
        .ok_or_else(|| CoreError::internal("ip main not initialized"))?
        .wire_drop(runtime)
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::net::lookup::register_ip_lookup
)]
pub struct IpLookupNode {
    #[node(default = register_ip_lookup_runtime(table.clone()))]
    runtime_data: NodeRuntimeData,
    table: FibTableHandle,
}

impl IpLookupNode {
    #[inline(always)]
    fn cached_packet_for_index(
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> Option<ParsedIpPacket> {
        let buffer = runtime.get_buffer(index).ok()?;
        packet_from_cached_metadata(
            unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) },
            unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }.packet_cursor(),
            buffer.current_ptr(),
            buffer.current_len(),
            buffer.total_len_not_including_first(),
        )
    }

    #[inline(always)]
    fn process_index(runtime: &DataPlaneRuntime, table: &FibTable, index: BufferIndex) -> NodeId {
        let parsed = Self::cached_packet_for_index(runtime, index);
        let traced = runtime
            .get_buffer(index)
            .expect("buffer")
            .trace_handle()
            .is_some();
        let parsed = match parsed {
            Some(parsed) => parsed,
            None => {
                let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
                let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
                opaque.forwarding = None;
                let next = table.drop_next();
                drop(buffer);
                if unlikely(traced) {
                    let _ = add_packet_trace!(
                        runtime,
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
                    );
                }
                return next;
            }
        };
        let result = table
            .lookup_packet(&parsed)
            .unwrap_or_else(|| FibLookupResult::terminal(table.drop_dpo(parsed.version)));
        let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
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
        drop(buffer);
        if unlikely(traced) {
            let _ = add_packet_trace!(
                runtime,
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
            );
        }
        result.dpo.next()
    }
}

impl Node for IpLookupNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let table = self.table.table();
        ip_lookup_process_frame(runtime, frame, &table)
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
}

impl AdjacencyRewriteNode {
    #[inline(always)]
    fn next_for_index(
        table: &FibTableHandle,
        runtime: &DataPlaneRuntime,
        index: BufferIndex,
    ) -> Option<NodeId> {
        let forwarding = {
            let buffer = runtime.get_buffer(index).expect("buffer");
            let opaque = unsafe { transmute::<_, &LookupOpaque>(buffer.opaque2()) };
            opaque.forwarding
        };
        let Some(forwarding) = forwarding else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingForwarding.code(),
            )
            .ok();
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: None,
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::MissingForwarding.code()),
                    next: None,
                },
            );
            return None;
        };
        if forwarding.dpo_type != DpoType::ADJACENCY {
            set_index_node_error_code(runtime, index, AdjacencyRewriteNodeError::WrongDpo.code())
                .ok();
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::WrongDpo.code()),
                    next: None,
                },
            );
            return None;
        }
        let Some(adjacency) = table
            .table()
            .adjacency(AdjacencyIndex::new(forwarding.dpo_index))
        else {
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingAdjacency.code(),
            )
            .ok();
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface: None,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::MissingAdjacency.code()),
                    next: None,
                },
            );
            return None;
        };
        let rewrite_len = adjacency.rewrite.as_slice().len();
        let egress_interface = adjacency.egress_interface;
        let next = adjacency.next;
        apply_adjacency_rewrite(runtime, index, adjacency).expect("adjacency rewrite");
        let _ = add_packet_trace!(
            runtime,
            index,
            AdjacencyRewriteTrace {
                dpo_index: Some(forwarding.dpo_index),
                egress_interface,
                rewrite_len,
                error: None,
                next: Some(next),
            },
        );
        Some(next)
    }
}

impl Node for AdjacencyRewriteNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        adjacency_rewrite_process_frame(runtime, frame, &self.table)
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
) -> NodeResult {
    let state = ip_lookup_runtime(data).expect("ip lookup runtime");
    let table = state.table.table();
    ip_lookup_process_frame(runtime, frame, &table)
}

fn ip_lookup_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    table: &FibTable,
) -> NodeResult {
    hammer_adapter::process_frame!(runtime, frame, |index| {
        IpLookupNode::process_index(runtime, table, index)
    })
}

fn adjacency_rewrite_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = adjacency_rewrite_runtime(data).expect("adjacency rewrite runtime");
    adjacency_rewrite_process_frame(runtime, frame, &state.table)
}

fn adjacency_rewrite_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    table: &FibTableHandle,
) -> NodeResult {
    let _ = frame.retain_indices_batched_with_prefetch(
        runtime.preferred_frame_batch_width(),
        |index| {
            runtime.prefetch_header(index);
            runtime.prefetch_write(index);
        },
        |index| {
            let Some(next) = AdjacencyRewriteNode::next_for_index(table, runtime, index) else {
                return Ok(true);
            };
            let mut next_frame = match runtime.buffers().get_next_frame(next) {
                Ok(frame) => frame,
                Err(_) => return Ok(true),
            };
            if next_frame.push_index(index).is_err() {
                return Ok(true);
            }
            let _ = runtime.put_next_frame(next_frame);
            Ok(false)
        },
    );
    NodeResult::drop()
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
        let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        network.set_packet_cursor(shift_packet_cursor(network.packet_cursor(), rewrite.len()));
    }
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.sw_if_index[1] =
        adjacency.egress_interface.unwrap_or(0);
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
fn packet_from_cached_metadata(
    network: &NetworkOpaque,
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
