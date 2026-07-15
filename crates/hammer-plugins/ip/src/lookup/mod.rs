use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwapOption;
use hammer_core::config::{Config, Route, RouteAction};
use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext, NodeRegistration, SecondaryOpaque,
};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_core::forwarding::{
    Adjacency, AdjacencyIndex, DpoProto, DpoType, FibLookupResult, FibTable, FibTableBuilder,
};
use hammer_core::protocol::icmp::IcmpErrorMetadata;
use hammer_core::protocol::ip::{IpProtocol, IpVersion, ParsedIpPacket};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{ControlThreadHandle, DataPlaneBarrierHandle};
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace, unlikely,
};

use hammer_service::data_plane::set_index_node_error_code;
use hammer_service::net::fib::FibTableHandle;
use hammer_service::opaque::{ForwardingMetadata, NetworkOpaque, TapEthernetMetadata};
use hammer_service::trace::codec::{
    TraceDecodeCursor, put_option_dpo_type, put_option_u16, put_option_u32, put_u16, put_u32,
    put_usize,
};

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
    pub next: u16,
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
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IpLookupTrace {
    #[inline]
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_u32(out, self.fib_index);
        put_option_dpo_type(out, self.route_dpo_type);
        put_option_u32(out, self.route_dpo_index);
        put_option_u32(out, self.load_balance_index);
        put_option_u16(out, self.bucket_index);
        put_option_dpo_type(out, self.dpo_type);
        put_option_u32(out, self.dpo_index);
        put_u16(out, self.next);
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
    pub next: Option<u16>,
}

impl AdjacencyRewriteTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            dpo_index: cursor.read_option_u32()?,
            egress_interface: cursor.read_option_u32()?,
            rewrite_len: cursor.read_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_option_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for AdjacencyRewriteTrace {
    #[inline]
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_u32(out, self.dpo_index);
        put_option_u32(out, self.egress_interface);
        put_usize(out, self.rewrite_len);
        put_option_u16(out, self.error);
        put_option_u16(out, self.next);
    }
}

fn format_adjacency_rewrite_trace(bytes: &[u8]) -> String {
    match AdjacencyRewriteTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("AdjacencyRewriteTrace invalid={bytes:?}"),
    }
}

pub struct IpLookupControlPlane {
    table: FibTableHandle,
    control_handle: Option<Arc<ControlThreadHandle>>,
    barrier: Option<DataPlaneBarrierHandle>,
}

impl IpLookupControlPlane {
    #[inline]
    pub fn new(table: FibTable<u16>) -> Self {
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
    pub fn node(&self, next: [NodeId; IpLookupNext::COUNT]) -> IpLookupNode {
        IpLookupNode::new(self.table_handle(), next)
    }

    #[inline]
    pub fn publish(&self, table: FibTable<u16>) -> HammerResult<()> {
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

#[hammer_component_macros::node_next]
pub enum IpLookupNext {
    #[next("drop")]
    Drop,
}

/// IP-lookup subsystem control-plane main (VPP `ip_main_t`).
///
/// Owns the static routes read from config. FIB DPOs store `ip-lookup` local
/// next slots; Graph Runtime resolves the declared next names after every node
/// is registered.
pub struct IpMain {
    routes: Arc<[Route]>,
}

impl IpMain {
    pub fn new(routes: Arc<[Route]>) -> Self {
        Self { routes }
    }

    fn build_table(&self) -> CoreResult<FibTable<u16>> {
        let mut builder = FibTableBuilder::<u16>::new(NodeNext::slot(IpLookupNext::Drop));
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

    pub fn register_node(&self, rt: &DataPlaneRuntime) -> CoreResult<NodeId> {
        let table = self.build_table()?;
        let node = IpLookupControlPlane::new(table).node(IpLookupNext::nodes(NodeId::new(0)));
        rt.nodes()
            .try_register_internal_with_next_names(node, &IpLookupNext::NEXT_NAMES)
    }
}

pub static IP_MAIN: ArcSwapOption<IpMain> = ArcSwapOption::const_empty();

pub fn reset_for_test() {
    IP_MAIN.store(None);
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<Config>()?;
    let routes = Arc::<[_]>::from(config.network.route.as_slice());
    IP_MAIN.store(Some(Arc::new(IpMain::new(routes))));
    hammer_service::net::pmtu::publish_path_mtu_cache(
        hammer_service::net::pmtu::PathMtuCache::new(),
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "ip_init",
    runs_before = ["install_packet_graph"]
)]
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

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::lookup::register_ip_lookup,
    next = IpLookupNext,
)]
pub struct IpLookupNode {
    #[node(default = register_ip_lookup_runtime(table.clone()))]
    runtime_data: NodeRuntimeData,
    table: FibTableHandle,
}

impl IpLookupNode {
    #[inline(always)]
    fn cached_packet_for_index(runtime: &DataPlaneRuntime, index: Index) -> Option<ParsedIpPacket> {
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
    fn process_index(runtime: &DataPlaneRuntime, table: &FibTable<u16>, index: Index) -> u16 {
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
            .unwrap_or_else(|| FibLookupResult::<u16>::terminal(table.drop_dpo(parsed.version)));
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
        NodeRegistration::next(Self::NODE_NAME, IpLookupNext::COUNT)
    }

    #[inline]
    fn node_initial_nexts(&self) -> &[NodeId] {
        &self.next
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyRewriteNodeError {
    MissingForwarding,
    WrongDpo,
    MissingAdjacency,
    MtuExceeded,
}

impl AdjacencyRewriteNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        match self {
            Self::MissingForwarding => 1,
            Self::WrongDpo => 2,
            Self::MissingAdjacency => 3,
            Self::MtuExceeded => 4,
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
    /// Configure the fanout slot used when DF + MTU exceed (VPP icmp-error next).
    pub fn configure_icmp_error_next(runtime_data: NodeRuntimeData, next: u16) {
        let Ok(slot) = runtime_data.usize_word(0) else {
            return;
        };
        let Ok(mut runtimes) = adjacency_rewrite_runtimes().lock() else {
            return;
        };
        if let Some(runtime) = runtimes.get_mut(slot) {
            runtime.icmp_error_next = Some(next);
        }
    }

    /// Configure the fanout slot used when non-DF + MTU exceed (VPP fragment next).
    pub fn configure_fragment_next(runtime_data: NodeRuntimeData, next: u16) {
        let Ok(slot) = runtime_data.usize_word(0) else {
            return;
        };
        let Ok(mut runtimes) = adjacency_rewrite_runtimes().lock() else {
            return;
        };
        if let Some(runtime) = runtimes.get_mut(slot) {
            runtime.fragment_next = Some(next);
        }
    }

    #[inline]
    pub fn runtime_data_handle(&self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline(always)]
    fn next_for_index(
        table: &FibTableHandle,
        icmp_error_next: Option<u16>,
        fragment_next: Option<u16>,
        runtime: &DataPlaneRuntime,
        index: Index,
    ) -> Option<u16> {
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

        if let Some(next) =
            adjacency_mtu_divert(runtime, index, &adjacency, icmp_error_next, fragment_next)
        {
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface: adjacency.egress_interface,
                    rewrite_len: 0,
                    error: Some(AdjacencyRewriteNodeError::MtuExceeded.code()),
                    next: Some(next),
                },
            );
            return Some(next);
        }

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
        let state =
            adjacency_rewrite_runtime(self.runtime_data).expect("adjacency rewrite runtime");
        adjacency_rewrite_process_frame(
            runtime,
            frame,
            &state.table,
            state.icmp_error_next,
            state.fragment_next,
        )
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
    /// Fanout slot for ICMP Frag-Needed when DF + MTU exceeded (VPP
    /// `IP4_REWRITE_NEXT_ICMP_ERROR`). `None` drops the packet.
    icmp_error_next: Option<u16>,
    /// Fanout slot for fragmentation when non-DF + MTU exceeded (VPP
    /// `IP4_REWRITE_NEXT_FRAGMENT`). `None` drops the packet.
    fragment_next: Option<u16>,
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
    runtimes.push(AdjacencyRewriteRuntime {
        table,
        icmp_error_next: None,
        fragment_next: None,
    });
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
    table: &FibTable<u16>,
) -> NodeResult {
    let count = frame.len();
    if count == 0 {
        return NodeResult::drop();
    }
    let mut nexts = Vec::with_capacity(count);
    for &index in frame.indices() {
        nexts.push(IpLookupNode::process_index(runtime, table, index));
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    NodeResult::drop()
}

fn adjacency_rewrite_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = adjacency_rewrite_runtime(data).expect("adjacency rewrite runtime");
    adjacency_rewrite_process_frame(
        runtime,
        frame,
        &state.table,
        state.icmp_error_next,
        state.fragment_next,
    )
}

fn adjacency_rewrite_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    table: &FibTableHandle,
    icmp_error_next: Option<u16>,
    fragment_next: Option<u16>,
) -> NodeResult {
    let indices: Vec<_> = frame.indices().iter().copied().collect();
    frame.discard_prefix(frame.len());
    let mut success_indices = Vec::with_capacity(indices.len());
    let mut success_nexts = Vec::with_capacity(indices.len());
    let mut failed = Vec::new();
    for index in indices {
        match AdjacencyRewriteNode::next_for_index(
            table,
            icmp_error_next,
            fragment_next,
            runtime,
            index,
        ) {
            Some(next) => {
                success_indices.push(index);
                success_nexts.push(next);
            }
            None => failed.push(index),
        }
    }
    for index in success_indices {
        frame
            .push_index(index)
            .expect("adjacency rewrite success fits production frame");
    }
    if !success_nexts.is_empty() {
        runtime.enqueue_to_next(frame, success_nexts.as_slice());
    }
    for index in failed {
        let _ = frame.push_index(index);
    }
    NodeResult::drop()
}

/// VPP `ip4_mtu_check` at rewrite: DF + oversize → ICMP Frag-Needed next;
/// non-DF + oversize → fragment next.
/// Returns `Some(next)` when the packet is diverted (no rewrite applied).
#[inline(always)]
fn adjacency_mtu_divert(
    runtime: &DataPlaneRuntime,
    index: Index,
    adjacency: &Adjacency<u16>,
    icmp_error_next: Option<u16>,
    fragment_next: Option<u16>,
) -> Option<u16> {
    if adjacency.proto != DpoProto::IP4 {
        return None;
    }
    let action = {
        let buffer = runtime.get_buffer(index).ok()?;
        let current = buffer.current();
        if current.len() < 20 {
            return None;
        }
        let header_total = u16::from_be_bytes([current[2], current[3]]);
        let packet_len = if header_total != 0 {
            header_total
        } else {
            u16::try_from(current.len()).unwrap_or(u16::MAX)
        };
        let dont_fragment = hammer_core::protocol::ip::read_ipv4_flags_fragment(current)
            .is_some_and(|flags| flags & hammer_core::protocol::ip::IPV4_FLAG_DONT_FRAGMENT != 0);
        hammer_core::protocol::ip::ipv4_mtu_check(
            packet_len,
            adjacency.max_l3_packet_bytes,
            dont_fragment,
        )
    };
    match action {
        hammer_core::protocol::ip::Ipv4MtuAction::Ok => None,
        hammer_core::protocol::ip::Ipv4MtuAction::IcmpFragNeeded { mtu } => {
            let next = icmp_error_next?;
            let mut buffer = runtime.get_buffer_mut(index).ok()?;
            let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
            opaque.icmp_error = Some(IcmpErrorMetadata::ipv4_destination_unreachable(
                4,
                u32::from(mtu),
            ));
            Some(next)
        }
        hammer_core::protocol::ip::Ipv4MtuAction::Fragment { .. } => fragment_next,
    }
}

#[inline(always)]
fn apply_adjacency_rewrite(
    runtime: &DataPlaneRuntime,
    index: Index,
    adjacency: Adjacency<u16>,
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
