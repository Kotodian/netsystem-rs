use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, OnceLock};

use crate::forwarding::{
    Adjacency, AdjacencyIndex, DpoProto, DpoType, FibLookupResult, FibTable, FibTableHandle,
    ForwardingMetadata,
};
use crate::protocol::ip::{
    IPV4_FLAG_DONT_FRAGMENT, IpInputError, IpInputTarget, IpProtocol, IpVersion, Ipv4MtuAction,
    ParsedIpPacket, ipv4_mtu_check, read_ipv4_flags_fragment,
};
use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext, NodeRegistration, SecondaryOpaque,
};
use hammer_runtime::{
    DataPlaneMain, InternalNode, Node, NodeProcessFn, NodeRuntimeData, TraceFormatter,
    add_packet_trace, format_packet_trace, unlikely,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::data_plane::set_index_node_error;
use hammer_service::opaque::{NetworkOpaque, TapEthernetMetadata};

use crate::config::NetworkIpConfig;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LookupOpaque {
    tap_ethernet: Option<TapEthernetMetadata>,
    icmp_error: Option<IcmpErrorMetadata>,
    forwarding: Option<ForwardingMetadata>,
}

#[derive(Clone, Copy)]
#[repr(transparent)]
struct IcmpErrorMetadata(NonZeroU64);

impl IcmpErrorMetadata {
    #[inline]
    fn ipv4_destination_unreachable(code: u8, data: u32) -> Self {
        let packed =
            (1u64 << 63) | (4u64 << 48) | (3u64 << 40) | (u64::from(code) << 32) | u64::from(data);
        Self(NonZeroU64::new(packed).expect("ICMP error metadata is presence-tagged"))
    }
}

const _: () = assert!(size_of::<LookupOpaque>() == size_of::<SecondaryOpaque>());

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AdjacencyRewriteTrace {
    pub dpo_index: Option<u32>,
    pub egress_interface: Option<u32>,
    pub rewrite_len: usize,
    pub error: Option<u16>,
    pub next: Option<u16>,
}

pub struct IpLookupControlPlane {
    table: FibTableHandle,
}

impl IpLookupControlPlane {
    #[inline]
    pub fn new(table: FibTable) -> Self {
        Self {
            table: FibTableHandle::new(table),
        }
    }

    #[inline]
    pub fn table_handle(&self) -> FibTableHandle {
        self.table.clone()
    }

    #[inline]
    pub fn node(&self) -> IpLookupNode {
        IpLookupNode::new(self.table_handle())
    }
}

#[hammer_component_macros::node_next]
pub enum IpLookupNext {
    #[next("drop")]
    Drop,
    #[next("ip-receive")]
    Receive,
    #[next("adjacency-rewrite")]
    AdjacencyRewrite,
}

#[hammer_component_macros::node_next]
pub enum AdjacencyRewriteNext {
    #[next("interface-output")]
    Output,
    #[next("drop")]
    Drop,
}

pub struct IpMain {
    control: OnceLock<Arc<IpLookupControlPlane>>,
}

impl IpMain {
    pub fn new() -> Self {
        Self {
            control: OnceLock::new(),
        }
    }

    fn control_plane(&self) -> RuntimeResult<Arc<IpLookupControlPlane>> {
        if let Some(control) = self.control.get() {
            return Ok(Arc::clone(control));
        }
        let table = FibTable::new(NodeNext::slot(IpLookupNext::Drop));
        let control = Arc::new(IpLookupControlPlane::new(table));
        if self.control.set(Arc::clone(&control)).is_err() {
            return self
                .control
                .get()
                .cloned()
                .ok_or_else(|| IpLookupError::ControlPlaneMissing.into());
        }
        Ok(control)
    }

    pub fn register_node(&self, rt: &DataPlaneMain) -> RuntimeResult<NodeId> {
        let control = self.control_plane()?;
        rt.nodes()
            .try_register_internal_with_next_names(control.node(), &IpLookupNext::NEXT_NAMES)
    }
}

/// Recoverable IP FIB/lookup control-plane failures.
#[hammer_component_macros::runtime_error(subsystem = "ip")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum IpLookupError {
    #[error("IP lookup control plane was not installed")]
    ControlPlaneMissing,
    #[error("adjacency rewrite length {len} exceeds isize")]
    RewriteTooLong { len: usize },
}

pub static IP_MAIN: OnceLock<IpMain> = OnceLock::new();

#[hammer_component_macros::config_function(
    name = "ip_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_ip(config: NetworkIpConfig, _: &mut hammer_runtime::GlobalMain) -> RuntimeResult<()> {
    config.validate()?;
    let main = IpMain::new();
    IP_MAIN
        .set(main)
        .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    crate::pmtu::init_path_mtu();
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "ip_init",
    runs_before = ["install_packet_graph"]
)]
fn init_ip() -> RuntimeResult<()> {
    if IP_MAIN.get().is_none() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "ip" });
    }
    Ok(())
}

pub fn register_ip_lookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    IP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?
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
    fn cached_packet_for_index(runtime: &DataPlaneMain, index: Index) -> Option<ParsedIpPacket> {
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
    fn process_index(runtime: &DataPlaneMain, table: &FibTable, index: Index) -> u16 {
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
            route_dpo_type: result.route_dpo.class(),
            route_dpo_index: result.route_dpo.index(),
            load_balance_index: result.forwarding_load_balance_index(),
            bucket_index: result.forwarding_bucket_index(),
            dpo_type: result.dpo.class(),
            dpo_index: result.dpo.index(),
        });
        drop(buffer);
        if unlikely(traced) {
            let _ = add_packet_trace!(
                runtime,
                index,
                IpLookupTrace {
                    fib_index: 0,
                    route_dpo_type: Some(result.route_dpo.class()),
                    route_dpo_index: Some(result.route_dpo.index()),
                    load_balance_index: Some(result.forwarding_load_balance_index()),
                    bucket_index: Some(result.forwarding_bucket_index()),
                    dpo_type: Some(result.dpo.class()),
                    dpo_index: Some(result.dpo.index()),
                    next: result.dpo.next(),
                },
            );
        }
        result.dpo.next()
    }
}

impl Node for IpLookupNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
        let table = self.table.table();
        ip_lookup_process_frame(runtime, frame, &table)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(IpLookupTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        ip_lookup_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for IpLookupNode {
    #[inline]
    fn node_registration(&self) -> Option<NodeRegistration>
    where
        Self: Sized,
    {
        Some(NodeRegistration::next(Self::NODE_NAME, IpLookupNext::COUNT))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum AdjacencyRewriteNodeError {
    MissingForwarding,
    WrongDpo,
    MissingAdjacency,
    RewriteFailed,
    MtuExceeded,
}

impl hammer_runtime::node::NodeErrorCode for AdjacencyRewriteNodeError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl AdjacencyRewriteNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        match self {
            Self::MissingForwarding => 1,
            Self::WrongDpo => 2,
            Self::MissingAdjacency => 3,
            Self::RewriteFailed => 4,
            Self::MtuExceeded => 5,
        }
    }
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::lookup::register_adjacency_rewrite,
    name = "adjacency-rewrite",
    next = AdjacencyRewriteNext,
    role = internal,
)]
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
        runtime: &DataPlaneMain,
        index: Index,
    ) -> RuntimeResult<Option<u16>> {
        let forwarding = {
            let buffer = runtime.get_buffer(index)?;
            let opaque = unsafe { transmute::<_, &LookupOpaque>(buffer.opaque2()) };
            opaque.forwarding
        };
        let Some(forwarding) = forwarding else {
            set_index_node_error(runtime, index, AdjacencyRewriteNodeError::MissingForwarding)?;
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
            return Ok(None);
        };
        if forwarding.dpo_type != DpoType::ADJACENCY {
            set_index_node_error(runtime, index, AdjacencyRewriteNodeError::WrongDpo)?;
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
            return Ok(None);
        }
        let Some(adjacency) = table
            .table()
            .adjacency(AdjacencyIndex::new(forwarding.dpo_index))
        else {
            set_index_node_error(runtime, index, AdjacencyRewriteNodeError::MissingAdjacency)?;
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
            return Ok(None);
        };

        if let Some(next) =
            adjacency_mtu_divert(runtime, index, &adjacency, icmp_error_next, fragment_next)?
        {
            set_index_node_error(runtime, index, AdjacencyRewriteNodeError::MtuExceeded)?;
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
            return Ok(Some(next));
        }

        let rewrite_len = adjacency.rewrite.as_slice().len();
        let egress_interface = adjacency.egress_interface;
        let next = adjacency.next;
        if apply_adjacency_rewrite(runtime, index, adjacency).is_err() {
            let error = AdjacencyRewriteNodeError::RewriteFailed;
            set_index_node_error(runtime, index, error)?;
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface,
                    rewrite_len: 0,
                    error: Some(error.code()),
                    next: None,
                },
            );
            return Ok(None);
        }
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
        Ok(Some(next))
    }
}

impl Node for AdjacencyRewriteNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneMain, frame: &mut BufferFrame) -> () {
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
        Some(format_packet_trace!(AdjacencyRewriteTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        adjacency_rewrite_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

pub fn register_adjacency_rewrite(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let main = IP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "ip" })?;
    let control = main.control_plane()?;
    runtime.nodes().try_register_internal_with_next_names(
        AdjacencyRewriteNode::new(control.table_handle()),
        &AdjacencyRewriteNext::NEXT_NAMES,
    )
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

fn ip_lookup_runtime(data: NodeRuntimeData) -> RuntimeResult<LookupRuntime> {
    let slot = data.usize_word(0)?;
    lookup_runtimes()
        .lock()
        .map_err(|_| crate::ip::IpControlError::RuntimeRegistryPoisoned {
            registry: crate::ip::IpRuntimeRegistry::IpLookup,
        })?
        .get(slot)
        .cloned()
        .ok_or_else(|| {
            crate::ip::IpControlError::RuntimeSlotInvalid {
                registry: crate::ip::IpRuntimeRegistry::IpLookup,
                slot,
            }
            .into()
        })
}

fn adjacency_rewrite_runtime(data: NodeRuntimeData) -> RuntimeResult<AdjacencyRewriteRuntime> {
    let slot = data.usize_word(0)?;
    adjacency_rewrite_runtimes()
        .lock()
        .map_err(|_| crate::ip::IpControlError::RuntimeRegistryPoisoned {
            registry: crate::ip::IpRuntimeRegistry::AdjacencyRewrite,
        })?
        .get(slot)
        .cloned()
        .ok_or_else(|| {
            crate::ip::IpControlError::RuntimeSlotInvalid {
                registry: crate::ip::IpRuntimeRegistry::AdjacencyRewrite,
                slot,
            }
            .into()
        })
}

fn ip_lookup_process(
    runtime: &DataPlaneMain,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
    let state = ip_lookup_runtime(data).expect("IP lookup runtime");
    let table = state.table.table();
    ip_lookup_process_frame(runtime, frame, &table)
}

fn ip_lookup_process_frame(
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    table: &FibTable,
) -> () {
    let count = frame.len();
    if count == 0 {
        return ();
    }
    let mut nexts = Vec::with_capacity(count);
    for &index in frame.indices() {
        nexts.push(IpLookupNode::process_index(runtime, table, index));
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    ()
}

fn adjacency_rewrite_process(
    runtime: &DataPlaneMain,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
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
    runtime: &DataPlaneMain,
    frame: &mut BufferFrame,
    table: &FibTableHandle,
    icmp_error_next: Option<u16>,
    fragment_next: Option<u16>,
) -> () {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match AdjacencyRewriteNode::next_for_index(
            table,
            icmp_error_next,
            fragment_next,
            runtime,
            index,
        )
        .expect("adjacency rewrite packet must belong to the current Frame")
        {
            Some(next) => next,
            None => NodeNext::slot(AdjacencyRewriteNext::Drop),
        }
    })
}

/// VPP `ip4_mtu_check` at rewrite: DF + oversize → ICMP Frag-Needed next;
/// non-DF + oversize → fragment next.
/// Returns `Some(next)` when the packet is diverted (no rewrite applied).
#[inline(always)]
fn adjacency_mtu_divert(
    runtime: &DataPlaneMain,
    index: Index,
    adjacency: &Adjacency,
    icmp_error_next: Option<u16>,
    fragment_next: Option<u16>,
) -> RuntimeResult<Option<u16>> {
    if adjacency.proto != DpoProto::IP4 {
        return Ok(None);
    }
    let action = {
        let buffer = runtime.get_buffer(index)?;
        let current = buffer.current();
        if current.len() < 20 {
            return Ok(None);
        }
        let header_total = u16::from_be_bytes([current[2], current[3]]);
        let packet_len = if header_total != 0 {
            header_total
        } else {
            u16::try_from(current.len()).unwrap_or(u16::MAX)
        };
        let dont_fragment = read_ipv4_flags_fragment(current)
            .is_some_and(|flags| flags & IPV4_FLAG_DONT_FRAGMENT != 0);
        ipv4_mtu_check(packet_len, adjacency.max_l3_packet_bytes, dont_fragment)
    };
    match action {
        Ipv4MtuAction::Ok => Ok(None),
        Ipv4MtuAction::IcmpFragNeeded { mtu } => {
            let next =
                icmp_error_next.unwrap_or_else(|| NodeNext::slot(AdjacencyRewriteNext::Drop));
            let mut buffer = runtime.get_buffer_mut(index)?;
            let opaque = unsafe { transmute::<_, &mut LookupOpaque>(buffer.opaque2_mut()) };
            opaque.icmp_error = Some(IcmpErrorMetadata::ipv4_destination_unreachable(
                4,
                u32::from(mtu),
            ));
            Ok(Some(next))
        }
        Ipv4MtuAction::Fragment { .. } => {
            Ok(Some(fragment_next.unwrap_or_else(|| {
                NodeNext::slot(AdjacencyRewriteNext::Drop)
            })))
        }
    }
}

#[inline(always)]
fn apply_adjacency_rewrite(
    runtime: &DataPlaneMain,
    index: Index,
    adjacency: Adjacency,
) -> RuntimeResult<()> {
    let rewrite = adjacency.rewrite.as_slice();
    let mut buffer = runtime.get_buffer_mut(index)?;
    if !rewrite.is_empty() {
        buffer.advance(
            -isize::try_from(rewrite.len())
                .map_err(|_| IpLookupError::RewriteTooLong { len: rewrite.len() })?,
        )?;
        buffer.current_mut()[..rewrite.len()].copy_from_slice(rewrite);
    }
    if !rewrite.is_empty() {
        let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
        network.set_packet_cursor(shift_packet_cursor(network.packet_cursor(), rewrite.len()));
    }
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.sw_if_index[1] =
        adjacency.egress_interface.unwrap_or(u32::MAX);
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
        input_target: IpInputTarget::Lookup,
        input_error: IpInputError::None,
        source,
        destination,
        packet_len: cursor.packet_len(),
        network_header_offset: cursor.network_header_offset(),
        network_header_len: cursor.network_header_len(),
        transport_header_offset: cursor.transport_header_offset(),
        transport_header_len: cursor.transport_header_len(),
    })
}
