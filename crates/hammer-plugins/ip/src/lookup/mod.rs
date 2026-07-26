use std::collections::BTreeMap;
use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use crate::forwarding::{
    Adjacency, AdjacencyIndex, AdjacencyRewrite, DpoProto, DpoType, FibLookupResult, FibSource,
    FibTable, FibTableBuilder, FibTableHandle, ForwardingMetadata,
};
use crate::protocol::icmp::IcmpErrorMetadata;
use crate::protocol::ip::{
    IPV4_FLAG_DONT_FRAGMENT, IpInputError, IpInputTarget, IpProtocol, IpVersion, Ipv4MtuAction,
    ParsedIpPacket, ipv4_mtu_check, read_ipv4_flags_fragment,
};
use arc_swap::ArcSwapOption;
use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext, NodeRegistration, SecondaryOpaque,
};
use hammer_runtime::{ControlThreadHandle, DataPlaneBarrierHandle};
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
    TraceFormatter, add_packet_trace, format_packet_trace, unlikely,
};
use hammer_runtime::{RuntimeError, RuntimeResult};

use hammer_service::data_plane::set_index_node_error_code;
use hammer_service::interface::InterfaceControlHandle;
use hammer_service::opaque::{NetworkOpaque, TapEthernetMetadata};

use crate::config::{NetworkIpConfig, Route, RouteAction};

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct LookupOpaque {
    tap_ethernet: Option<TapEthernetMetadata>,
    icmp_error: Option<IcmpErrorMetadata>,
    forwarding: Option<ForwardingMetadata>,
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
    pub fn node(&self) -> IpLookupNode {
        IpLookupNode::new(self.table_handle())
    }

    #[inline]
    pub fn publish(&self, table: FibTable<u16>) -> RuntimeResult<()> {
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

/// IP-lookup subsystem control-plane main (VPP `ip_main_t`).
///
/// Owns all FIB source contributions. FIB DPOs store `ip-lookup` local next
/// slots; source selection and retained fallback state never enter lookup.
pub struct IpMain {
    contributions: Mutex<FibContributions>,
    control: OnceLock<Arc<IpLookupControlPlane>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FibContribution {
    Drop,
    Receive,
    Paths(Vec<FibPath>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FibPath {
    interface_index: u32,
    next_hop: Option<IpAddr>,
}

#[derive(Debug, Clone, Default)]
struct FibContributions {
    by_prefix: BTreeMap<ipnet::IpNet, BTreeMap<FibSource, FibContribution>>,
}

impl FibContributions {
    fn insert(
        &mut self,
        prefix: ipnet::IpNet,
        source: FibSource,
        contribution: FibContribution,
    ) -> RuntimeResult<()> {
        if matches!(&contribution, FibContribution::Paths(paths) if paths.is_empty()) {
            return Err(RuntimeError::config_validation(format!(
                "FIB source {source:?} for {prefix} must contribute at least one path"
            )));
        }
        let sources = self.by_prefix.entry(prefix).or_default();
        match (sources.get_mut(&source), contribution) {
            (None, contribution) => {
                sources.insert(source, contribution);
            }
            (Some(FibContribution::Paths(current)), FibContribution::Paths(mut added)) => {
                current.append(&mut added);
            }
            (Some(FibContribution::Drop), FibContribution::Drop)
            | (Some(FibContribution::Receive), FibContribution::Receive) => {}
            (Some(_), _) => {
                return Err(IpLookupError::FibSourceConflict { prefix }.into());
            }
        }
        Ok(())
    }

    fn remove(&mut self, prefix: ipnet::IpNet, source: FibSource) -> bool {
        let Some(sources) = self.by_prefix.get_mut(&prefix) else {
            return false;
        };
        let removed = sources.remove(&source).is_some();
        let prefix_is_unsourced = sources.is_empty();
        if prefix_is_unsourced {
            self.by_prefix.remove(&prefix);
        }
        removed
    }
}

impl IpMain {
    pub fn new(
        routes: Arc<[Route]>,
        interfaces: Option<InterfaceControlHandle>,
    ) -> RuntimeResult<Self> {
        let mut contributions = FibContributions::default();
        for route in routes.iter() {
            let action = match route.action()? {
                RouteAction::Drop => FibContribution::Drop,
                RouteAction::Adjacency { via, interface } => {
                    let interface_index = Self::configured_interface_index(
                        interfaces.as_ref(),
                        route.prefix,
                        interface.as_str(),
                    )?;
                    match via {
                        Some(next_hop) => {
                            validate_via_family(route.prefix, next_hop)?;
                            FibContribution::Paths(vec![FibPath {
                                interface_index,
                                next_hop: Some(next_hop),
                            }])
                        }
                        None => FibContribution::Paths(vec![FibPath {
                            interface_index,
                            next_hop: None,
                        }]),
                    }
                }
                RouteAction::LoadBalance { via, interface } => {
                    let interface_index = Self::configured_interface_index(
                        interfaces.as_ref(),
                        route.prefix,
                        interface.as_str(),
                    )?;
                    for next_hop in &via {
                        validate_via_family(route.prefix, *next_hop)?;
                    }
                    FibContribution::Paths(
                        via.into_iter()
                            .map(|next_hop| FibPath {
                                interface_index,
                                next_hop: Some(next_hop),
                            })
                            .collect(),
                    )
                }
            };
            contributions.insert(route.prefix, FibSource::Api, action)?;
        }
        if let Some(interfaces) = interfaces.as_ref() {
            Self::add_interface_contributions(&mut contributions, interfaces)?;
        }
        Ok(Self {
            contributions: Mutex::new(contributions),
            control: OnceLock::new(),
        })
    }

    fn build_table(&self) -> RuntimeResult<FibTable<u16>> {
        let contributions = self
            .contributions
            .lock()
            .map_err(|_| IpLookupError::FibContributionsPoisoned)?;
        Self::compile_contributions(&contributions)
    }

    fn compile_contributions(contributions: &FibContributions) -> RuntimeResult<FibTable<u16>> {
        let drop_next = NodeNext::slot(IpLookupNext::Drop);
        let receive_next = NodeNext::slot(IpLookupNext::Receive);
        let rewrite_next = NodeNext::slot(IpLookupNext::AdjacencyRewrite);
        let rewrite_output_next = NodeNext::slot(AdjacencyRewriteNext::Output);
        let mut builder = FibTableBuilder::<u16>::new(drop_next);
        for (prefix, sources) in &contributions.by_prefix {
            let Some((_, action)) = sources.iter().next() else {
                return Err(IpLookupError::FibPrefixEmpty { prefix: *prefix }.into());
            };
            match action {
                FibContribution::Drop => {
                    builder.add_drop_route(*prefix);
                }
                FibContribution::Receive => {
                    builder.add_receive_route(*prefix, receive_next);
                }
                FibContribution::Paths(paths) if paths.len() == 1 => Self::add_adjacency_route(
                    &mut builder,
                    *prefix,
                    paths[0],
                    rewrite_next,
                    rewrite_output_next,
                )?,
                FibContribution::Paths(paths) => Self::add_load_balance_route(
                    &mut builder,
                    *prefix,
                    paths,
                    rewrite_next,
                    rewrite_output_next,
                )?,
            }
        }
        Ok(builder.build())
    }

    fn configured_interface_index(
        interfaces: Option<&InterfaceControlHandle>,
        prefix: ipnet::IpNet,
        interface: &str,
    ) -> RuntimeResult<u32> {
        let interfaces = interfaces.ok_or_else(|| {
            RuntimeError::config_validation(format!(
                "network.route[{prefix}] requires a configured interface"
            ))
        })?;
        interfaces.interface_index(interface).ok_or_else(|| {
            RuntimeError::config_validation(format!(
                "network.route[{prefix}] references unknown interface `{interface}`"
            ))
        })
    }

    fn add_adjacency_route(
        builder: &mut FibTableBuilder<u16>,
        prefix: ipnet::IpNet,
        path: FibPath,
        rewrite_next: u16,
        rewrite_output_next: u16,
    ) -> RuntimeResult<()> {
        let proto = DpoProto::from(match prefix {
            ipnet::IpNet::V4(_) => IpVersion::V4,
            ipnet::IpNet::V6(_) => IpVersion::V6,
        });
        if let Some(next_hop) = path.next_hop {
            validate_via_family(prefix, next_hop)?;
        }
        let dpo = builder.add_interface_adjacency_dpo(
            proto,
            path.interface_index,
            AdjacencyRewrite::empty(),
            rewrite_next,
            rewrite_output_next,
        );
        builder.add_route_dpo(prefix, dpo);
        Ok(())
    }

    fn add_load_balance_route(
        builder: &mut FibTableBuilder<u16>,
        prefix: ipnet::IpNet,
        paths: &[FibPath],
        rewrite_next: u16,
        rewrite_output_next: u16,
    ) -> RuntimeResult<()> {
        for path in paths {
            if let Some(next_hop) = path.next_hop {
                validate_via_family(prefix, next_hop)?;
            }
        }
        let proto = DpoProto::from(match prefix {
            ipnet::IpNet::V4(_) => IpVersion::V4,
            ipnet::IpNet::V6(_) => IpVersion::V6,
        });
        let buckets = paths
            .iter()
            .map(|path| {
                builder.add_interface_adjacency_dpo(
                    proto,
                    path.interface_index,
                    AdjacencyRewrite::empty(),
                    rewrite_next,
                    rewrite_output_next,
                )
            })
            .collect::<Vec<_>>();
        let load_balance = builder
            .try_add_load_balance(proto, buckets)
            .map_err(|error| {
                RuntimeError::config_validation(format!(
                    "FIB paths for {prefix} cannot form a load balance: {error:?}"
                ))
            })?;
        builder.add_route(prefix, load_balance);
        Ok(())
    }

    fn add_interface_contributions(
        contributions: &mut FibContributions,
        interfaces: &InterfaceControlHandle,
    ) -> RuntimeResult<()> {
        let mut interface_index = 0u32;
        while interfaces.interface_name(interface_index).is_some() {
            for address in interfaces.interface_addresses(interface_index) {
                let host = host_prefix(address)?;
                contributions.insert(host, FibSource::Interface, FibContribution::Receive)?;

                if address.prefix_len() == address.max_prefix_len() {
                    continue;
                }
                let connected = address.trunc();
                contributions.insert(
                    connected,
                    FibSource::Interface,
                    FibContribution::Paths(vec![FibPath {
                        interface_index,
                        next_hop: None,
                    }]),
                )?;
            }
            interface_index = interface_index
                .checked_add(1)
                .ok_or(IpLookupError::InterfaceIndexSpaceExhausted)?;
        }
        Ok(())
    }

    pub fn remove_contribution(
        &self,
        prefix: ipnet::IpNet,
        source: FibSource,
    ) -> RuntimeResult<bool> {
        let mut current = self
            .contributions
            .lock()
            .map_err(|_| IpLookupError::FibContributionsPoisoned)?;
        let mut next = current.clone();
        if !next.remove(prefix, source) {
            return Ok(false);
        }
        let table = Self::compile_contributions(&next)?;
        if let Some(control) = self.control.get() {
            control.publish(table)?;
        }
        *current = next;
        Ok(true)
    }

    fn control_plane(&self) -> RuntimeResult<Arc<IpLookupControlPlane>> {
        if let Some(control) = self.control.get() {
            return Ok(Arc::clone(control));
        }
        let control = Arc::new(IpLookupControlPlane::new(self.build_table()?));
        if self.control.set(Arc::clone(&control)).is_err() {
            return self
                .control
                .get()
                .cloned()
                .ok_or_else(|| IpLookupError::ControlPlaneMissing.into());
        }
        Ok(control)
    }

    pub fn register_node(&self, rt: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
        let control = self.control_plane()?;
        rt.nodes()
            .try_register_internal_with_next_names(control.node(), &IpLookupNext::NEXT_NAMES)
    }
}

/// Recoverable IP FIB/lookup control-plane failures.
#[derive(Debug, thiserror::Error)]
pub(crate) enum IpLookupError {
    #[error("FIB prefix {prefix} received incompatible route semantics from one source")]
    FibSourceConflict { prefix: ipnet::IpNet },
    #[error("FIB prefix {prefix} has no source contribution")]
    FibPrefixEmpty { prefix: ipnet::IpNet },
    #[error("interface index space is exhausted")]
    InterfaceIndexSpaceExhausted,
    #[error("IP lookup control plane was not installed")]
    ControlPlaneMissing,
    #[error("FIB contribution table lock is poisoned")]
    FibContributionsPoisoned,
    #[error("host prefix for {address} is invalid")]
    HostPrefixInvalid {
        address: ipnet::IpNet,
        #[source]
        source: ipnet::PrefixLenError,
    },
    #[error("adjacency rewrite length {len} exceeds isize")]
    RewriteTooLong { len: usize },
}

impl From<IpLookupError> for RuntimeError {
    fn from(error: IpLookupError) -> Self {
        Self::subsystem("ip", error)
    }
}

fn host_prefix(address: ipnet::IpNet) -> RuntimeResult<ipnet::IpNet> {
    let host = match address {
        ipnet::IpNet::V4(net) => ipnet::Ipv4Net::new(net.addr(), 32).map(ipnet::IpNet::V4),
        ipnet::IpNet::V6(net) => ipnet::Ipv6Net::new(net.addr(), 128).map(ipnet::IpNet::V6),
    };
    host.map_err(|source| IpLookupError::HostPrefixInvalid { address, source }.into())
}

fn validate_via_family(prefix: ipnet::IpNet, via: IpAddr) -> RuntimeResult<()> {
    let matches = matches!(
        (prefix, via),
        (ipnet::IpNet::V4(_), IpAddr::V4(_)) | (ipnet::IpNet::V6(_), IpAddr::V6(_))
    );
    if matches {
        Ok(())
    } else {
        Err(RuntimeError::config_validation(format!(
            "network.route[{prefix}] next hop `{via}` has a mismatched address family"
        )))
    }
}

pub static IP_MAIN: ArcSwapOption<IpMain> = ArcSwapOption::const_empty();

pub fn reset_for_test() {
    IP_MAIN.store(None);
}

#[hammer_component_macros::config_function(
    name = "ip_config",
    section = "network",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_ip(config: NetworkIpConfig, engine: &mut hammer_runtime::Engine) -> RuntimeResult<()> {
    config.validate()?;
    let routes = Arc::<[_]>::from(config.route);
    let interfaces = engine
        .registry
        .get::<hammer_service::interface::InterfaceControlPlane>()
        .map(|plane| plane.handle());
    let main = Arc::new(IpMain::new(routes, interfaces)?);
    IP_MAIN.store(Some(main));
    hammer_service::net::pmtu::publish_path_mtu_cache(
        hammer_service::net::pmtu::PathMtuCache::new(),
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "ip_init",
    runs_before = ["install_packet_graph"]
)]
fn init_ip() -> RuntimeResult<()> {
    if IP_MAIN.load().is_none() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "ip" });
    }
    Ok(())
}

pub fn register_ip_lookup(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    IP_MAIN
        .load()
        .as_deref()
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
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, IpLookupNext::COUNT)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyRewriteNodeError {
    MissingForwarding,
    WrongDpo,
    MissingAdjacency,
    RewriteFailed,
    MtuExceeded,
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
        runtime: &DataPlaneRuntime,
        index: Index,
    ) -> RuntimeResult<Option<u16>> {
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
            set_index_node_error_code(runtime, index, AdjacencyRewriteNodeError::WrongDpo.code())?;
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
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MissingAdjacency.code(),
            )?;
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
            set_index_node_error_code(
                runtime,
                index,
                AdjacencyRewriteNodeError::MtuExceeded.code(),
            )?;
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
            let error = AdjacencyRewriteNodeError::RewriteFailed.code();
            set_index_node_error_code(runtime, index, error)?;
            let _ = add_packet_trace!(
                runtime,
                index,
                AdjacencyRewriteTrace {
                    dpo_index: Some(forwarding.dpo_index),
                    egress_interface,
                    rewrite_len: 0,
                    error: Some(error),
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

pub fn register_adjacency_rewrite(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let main_guard = IP_MAIN.load();
    let main = main_guard
        .as_ref()
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
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = ip_lookup_runtime(data).expect("IP lookup runtime");
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
    runtime: &DataPlaneRuntime,
    index: Index,
    adjacency: &Adjacency<u16>,
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
    runtime: &DataPlaneRuntime,
    index: Index,
    adjacency: Adjacency<u16>,
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::Via;
    use hammer_service::interface::InterfaceControlPlane;

    fn parsed(destination: &str) -> ParsedIpPacket {
        let destination = destination.parse::<IpAddr>().expect("destination");
        let (version, source) = match destination {
            IpAddr::V4(_) => (IpVersion::V4, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))),
            IpAddr::V6(_) => (IpVersion::V6, IpAddr::V6(Ipv6Addr::LOCALHOST)),
        };
        ParsedIpPacket {
            version,
            protocol: IpProtocol::Tcp,
            input_target: IpInputTarget::Lookup,
            input_error: IpInputError::None,
            source,
            destination,
            packet_len: 40,
            network_header_offset: 0,
            network_header_len: match version {
                IpVersion::V4 => 20,
                IpVersion::V6 => 40,
            },
            transport_header_offset: match version {
                IpVersion::V4 => 20,
                IpVersion::V6 => 40,
            },
            transport_header_len: 20,
        }
    }

    fn interface_with_address() -> (InterfaceControlPlane, u32) {
        let interfaces = InterfaceControlPlane::new();
        let interface = interfaces.register_interface("utun").expect("interface");
        interfaces
            .add_address(interface, "10.66.77.1/30".parse().expect("address"))
            .expect("address registration");
        (interfaces, interface)
    }

    #[test]
    fn interface_address_compiles_local_and_connected_routes() {
        let (interfaces, interface) = interface_with_address();
        let handle = interfaces.handle();
        let main =
            IpMain::new(Arc::<[Route]>::from(Vec::new()), Some(handle)).expect("build IP main");
        let table = main.build_table().expect("build FIB");

        let local = table
            .lookup_packet(&parsed("10.66.77.1"))
            .expect("local route");
        assert_eq!(local.dpo.kind(), DpoType::RECEIVE);

        let connected = table
            .lookup_packet(&parsed("10.66.77.2"))
            .expect("connected route");
        assert_eq!(connected.dpo.kind(), DpoType::ADJACENCY);
        let adjacency = table
            .adjacency(connected.dpo.adjacency_index().expect("adjacency"))
            .expect("adjacency record");
        assert_eq!(adjacency.egress_interface, Some(interface));
    }

    #[test]
    fn interface_source_wins_and_api_source_returns_after_withdrawal() {
        let prefix = "198.51.100.0/24".parse().expect("prefix");
        let mut contributions = FibContributions::default();
        contributions
            .insert(prefix, FibSource::Api, FibContribution::Drop)
            .expect("API contribution");
        contributions
            .insert(
                prefix,
                FibSource::Interface,
                FibContribution::Paths(vec![FibPath {
                    interface_index: 7,
                    next_hop: None,
                }]),
            )
            .expect("interface contribution");

        let table = IpMain::compile_contributions(&contributions).expect("compile FIB");
        let interface = table
            .lookup_packet(&parsed("198.51.100.9"))
            .expect("interface route");
        assert_eq!(interface.dpo.kind(), DpoType::ADJACENCY);
        let adjacency = table
            .adjacency(interface.dpo.adjacency_index().expect("adjacency"))
            .expect("adjacency record");
        assert_eq!(adjacency.egress_interface, Some(7));

        assert!(contributions.remove(prefix, FibSource::Interface));
        let table = IpMain::compile_contributions(&contributions).expect("compile fallback FIB");
        let fallback = table
            .lookup_packet(&parsed("198.51.100.9"))
            .expect("API fallback route");
        assert_eq!(fallback.dpo.kind(), DpoType::DROP);
    }

    #[test]
    fn same_source_paths_compile_to_distinct_ecmp_adjacencies() {
        let prefix = "203.0.113.0/24".parse().expect("prefix");
        let mut contributions = FibContributions::default();
        for (interface_index, next_hop) in [
            (10, "192.0.2.1".parse().expect("first next hop")),
            (11, "192.0.2.2".parse().expect("second next hop")),
        ] {
            contributions
                .insert(
                    prefix,
                    FibSource::Api,
                    FibContribution::Paths(vec![FibPath {
                        interface_index,
                        next_hop: Some(next_hop),
                    }]),
                )
                .expect("path contribution");
        }

        let table = IpMain::compile_contributions(&contributions).expect("compile FIB");
        let route = table
            .lookup_packet(&parsed("203.0.113.9"))
            .expect("ECMP route");
        let load_balance = table
            .load_balance(route.route_dpo.load_balance_index().expect("load balance"))
            .expect("load-balance record");
        assert_eq!(load_balance.bucket_count(), 2);
        let first = load_balance.buckets()[0]
            .adjacency_index()
            .expect("first adjacency");
        let second = load_balance.buckets()[1]
            .adjacency_index()
            .expect("second adjacency");
        assert_ne!(first, second);
        assert_eq!(
            table
                .adjacency(first)
                .expect("first adjacency record")
                .egress_interface,
            Some(10)
        );
        assert_eq!(
            table
                .adjacency(second)
                .expect("second adjacency record")
                .egress_interface,
            Some(11)
        );
    }

    #[test]
    fn empty_path_contribution_is_rejected_without_building_a_load_balance() {
        let prefix = "203.0.113.0/24".parse().expect("prefix");
        let mut contributions = FibContributions::default();
        let error = contributions
            .insert(prefix, FibSource::Api, FibContribution::Paths(Vec::new()))
            .expect_err("empty contribution");
        assert!(matches!(error, RuntimeError::ConfigValidation { .. }));
        assert!(!contributions.by_prefix.contains_key(&prefix));

        contributions
            .by_prefix
            .entry(prefix)
            .or_default()
            .insert(FibSource::Api, FibContribution::Paths(Vec::new()));
        let error = IpMain::compile_contributions(&contributions)
            .expect_err("empty load balance must not compile");
        assert!(matches!(error, RuntimeError::ConfigValidation { .. }));
    }

    #[test]
    fn startup_routes_compile_drop_adjacency_and_ecmp_actions() {
        let (interfaces, _) = interface_with_address();
        let routes = vec![
            Route {
                prefix: "192.0.2.0/24".parse().expect("drop prefix"),
                drop: true,
                via: None,
                interface: String::new(),
            },
            Route {
                prefix: "198.51.100.0/24".parse().expect("adjacency prefix"),
                drop: false,
                via: Some(Via::One("10.66.77.2".parse().expect("next hop"))),
                interface: "utun".to_owned(),
            },
            Route {
                prefix: "203.0.113.0/24".parse().expect("ECMP prefix"),
                drop: false,
                via: Some(Via::Many(vec![
                    "10.66.77.2".parse().expect("first next hop"),
                    "10.66.77.3".parse().expect("second next hop"),
                ])),
                interface: "utun".to_owned(),
            },
        ];
        let handle = interfaces.handle();
        let main = IpMain::new(routes.into(), Some(handle)).expect("build IP main");
        let table = main.build_table().expect("build FIB");

        let drop = table
            .lookup_packet(&parsed("192.0.2.9"))
            .expect("drop route");
        assert_eq!(drop.dpo.kind(), DpoType::DROP);

        let adjacency = table
            .lookup_packet(&parsed("198.51.100.9"))
            .expect("adjacency route");
        assert_eq!(adjacency.dpo.kind(), DpoType::ADJACENCY);

        let ecmp = table
            .lookup_packet(&parsed("203.0.113.9"))
            .expect("ECMP route");
        assert!(ecmp.load_balance().is_some());
        assert_eq!(ecmp.dpo.kind(), DpoType::ADJACENCY);
    }

    #[test]
    fn route_next_hop_must_match_prefix_family() {
        let (interfaces, _) = interface_with_address();
        let routes = vec![Route {
            prefix: "198.51.100.0/24".parse().expect("prefix"),
            drop: false,
            via: Some(Via::One("2001:db8::1".parse().expect("next hop"))),
            interface: "utun".to_owned(),
        }];
        let handle = interfaces.handle();
        let error = IpMain::new(routes.into(), Some(handle))
            .err()
            .expect("family mismatch");
        assert!(error.to_string().contains("mismatched address family"));
    }
}
