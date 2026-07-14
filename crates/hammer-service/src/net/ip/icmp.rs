use std::mem::{size_of, transmute};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeId, NodeNext, SecondaryOpaque,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::{
    IcmpBuildError, IcmpErrorFamily, IcmpErrorMetadata, IcmpGeneratedPacket, IcmpHeader,
    build_echo_reply, build_icmp_error_packet,
};
use hammer_core::protocol::wire::read_header;
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData, PacketTrace,
    TraceFormatter, add_packet_trace,
};

use crate::data_plane::set_index_node_error_code;
use crate::opaque::NetworkOpaque;
use crate::trace::codec::{
    TraceDecodeCursor, put_option_icmp_error_family, put_option_ip_version, put_option_u16,
    put_option_u32, put_option_usize, put_u8, put_u16,
};

use super::{IpInputError, IpProtocol, IpVersion, ip_header};
use hammer_infra::vec::Vec;

const ICMP_HEADER_MIN_LEN: usize = 4;
const ICMP_ECHO_HEADER_LEN: usize = 8;
const ICMP4_ECHO_REPLY: u8 = 0;
const ICMP4_ECHO_REQUEST: u8 = 8;
const ICMP6_ECHO_REQUEST: u8 = 128;
const ICMP6_ECHO_REPLY: u8 = 129;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<IcmpErrorMetadata>,
    reserved: [u64; 6],
}

const _: () = assert!(size_of::<IcmpErrorOpaque>() == size_of::<SecondaryOpaque>());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpInputError {
    BadLength,
    WrongProtocol,
    UnknownType,
    BadCode,
    TooShort,
    HopLimit,
}

impl IcmpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpNodeError {
    BadLength,
    WrongProtocol,
    WrongType,
    BadCode,
    BadChecksum,
    Suppressed,
    MissingMetadata,
    MissingIngressInterface,
    MissingSource,
    UnsupportedFamily,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpInputTrace {
    pub version: Option<IpVersion>,
    pub icmp_type: Option<u8>,
    pub code: Option<u8>,
    pub error: Option<u16>,
    pub next: u16,
}

impl IcmpInputTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let version = cursor.read_option_ip_version()?;
        let icmp_type = if cursor.read_bool()? {
            Some(cursor.read_u8()?)
        } else {
            None
        };
        let code = if cursor.read_bool()? {
            Some(cursor.read_u8()?)
        } else {
            None
        };
        let trace = Self {
            version,
            icmp_type,
            code,
            error: cursor.read_option_u16()?,
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpInputTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_ip_version(out, self.version);
        match self.icmp_type {
            Some(value) => {
                crate::trace::codec::put_bool(out, true);
                put_u8(out, value);
            }
            None => crate::trace::codec::put_bool(out, false),
        }
        match self.code {
            Some(value) => {
                crate::trace::codec::put_bool(out, true);
                put_u8(out, value);
            }
            None => crate::trace::codec::put_bool(out, false),
        }
        put_option_u16(out, self.error);
        put_u16(out, self.next);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpEchoRequestTrace {
    pub generated_len: Option<usize>,
    pub error: Option<u16>,
    pub next: u16,
}

impl IcmpEchoRequestTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            generated_len: cursor.read_option_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpEchoRequestTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_usize(out, self.generated_len);
        put_option_u16(out, self.error);
        put_u16(out, self.next);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpErrorTrace {
    pub family: Option<IcmpErrorFamily>,
    pub ingress_interface: Option<u32>,
    pub local_source_present: bool,
    pub generated_len: Option<usize>,
    pub error: Option<u16>,
    pub next: u16,
}

impl IcmpErrorTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            family: cursor.read_option_icmp_error_family()?,
            ingress_interface: cursor.read_option_u32()?,
            local_source_present: cursor.read_bool()?,
            generated_len: cursor.read_option_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_u16()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpErrorTrace {
    fn encode_trace(&self, out: &mut Vec<u8>) {
        put_option_icmp_error_family(out, self.family);
        put_option_u32(out, self.ingress_interface);
        crate::trace::codec::put_bool(out, self.local_source_present);
        put_option_usize(out, self.generated_len);
        put_option_u16(out, self.error);
        put_u16(out, self.next);
    }
}

fn format_icmp_input_trace(bytes: &[u8]) -> String {
    match IcmpInputTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IcmpInputTrace invalid={bytes:?}"),
    }
}

fn format_icmp_echo_request_trace(bytes: &[u8]) -> String {
    match IcmpEchoRequestTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IcmpEchoRequestTrace invalid={bytes:?}"),
    }
}

fn format_icmp_error_trace(bytes: &[u8]) -> String {
    match IcmpErrorTrace::decode(bytes) {
        Some(trace) => format!("{trace:?}"),
        None => format!("IcmpErrorTrace invalid={bytes:?}"),
    }
}

impl IcmpNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

impl From<IcmpBuildError> for IcmpNodeError {
    #[inline(always)]
    fn from(error: IcmpBuildError) -> Self {
        match error {
            IcmpBuildError::BadLength => Self::BadLength,
            IcmpBuildError::WrongProtocol => Self::WrongProtocol,
            IcmpBuildError::WrongType => Self::WrongType,
            IcmpBuildError::BadCode => Self::BadCode,
            IcmpBuildError::BadChecksum => Self::BadChecksum,
            IcmpBuildError::Suppressed => Self::Suppressed,
            IcmpBuildError::UnsupportedFamily => Self::UnsupportedFamily,
        }
    }
}

pub struct IcmpErrorSourceTable {
    inner: Arc<ArcSwap<IcmpErrorSourceSnapshot>>,
}

impl IcmpErrorSourceTable {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(IcmpErrorSourceSnapshot::new())),
        }
    }

    #[inline]
    pub fn from_sources(sources: impl IntoIterator<Item = (u32, IpAddr)>) -> Self {
        let mut snapshot = IcmpErrorSourceSnapshot::new();
        for (interface_index, source) in sources {
            snapshot.insert(interface_index, source);
        }
        Self {
            inner: Arc::new(ArcSwap::from_pointee(snapshot)),
        }
    }

    #[inline]
    pub fn handle(&self) -> IcmpErrorSourceTableHandle {
        IcmpErrorSourceTableHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn publish(&self, sources: impl IntoIterator<Item = (u32, IpAddr)>) {
        let mut snapshot = IcmpErrorSourceSnapshot::new();
        for (interface_index, source) in sources {
            snapshot.insert(interface_index, source);
        }
        self.inner.store(Arc::new(snapshot));
    }
}

impl Default for IcmpErrorSourceTable {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct IcmpErrorSourceTableHandle {
    inner: Arc<ArcSwap<IcmpErrorSourceSnapshot>>,
}

impl IcmpErrorSourceTableHandle {
    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<IcmpErrorSourceSnapshot>> {
        self.inner.load()
    }
}

#[derive(Debug, Clone)]
struct IcmpErrorSourceSnapshot {
    sources: Vec<IcmpErrorSourceEntry>,
}

impl IcmpErrorSourceSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    fn insert(&mut self, interface_index: u32, source: IpAddr) {
        if self
            .lookup(interface_index, version_for_addr(source))
            .is_some()
        {
            return;
        }
        self.sources.push(IcmpErrorSourceEntry {
            interface_index,
            source,
        });
    }

    #[inline(always)]
    fn lookup(&self, interface_index: u32, version: IpVersion) -> Option<IpAddr> {
        self.sources
            .iter()
            .find(|entry| {
                entry.interface_index == interface_index
                    && version_for_addr(entry.source) == version
            })
            .map(|entry| entry.source)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcmpErrorSourceEntry {
    interface_index: u32,
    source: IpAddr,
}

#[inline(always)]
fn version_for_addr(addr: IpAddr) -> IpVersion {
    match addr {
        IpAddr::V4(_) => IpVersion::V4,
        IpAddr::V6(_) => IpVersion::V6,
    }
}

#[inline(always)]
fn version_for_family(family: IcmpErrorFamily) -> IpVersion {
    match family {
        IcmpErrorFamily::Ipv4 => IpVersion::V4,
        IcmpErrorFamily::Ipv6 => IpVersion::V6,
    }
}

pub struct IcmpInputControlPlane {
    inner: Arc<ArcSwap<IcmpInputSnapshot>>,
    nodes: Option<hammer_runtime::node::NodeRuntime>,
    consumer: Option<NodeId>,
    ip4_default_node: NodeId,
    ip6_default_node: NodeId,
}

impl IcmpInputControlPlane {
    #[inline]
    pub fn new(default_next: NodeId) -> Self {
        Self::with_defaults(default_next, default_next)
    }

    #[inline]
    pub fn with_defaults(ip4_default_next: NodeId, ip6_default_next: NodeId) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(IcmpInputSnapshot::new(
                u16::MAX,
                u16::MAX,
            ))),
            nodes: None,
            consumer: None,
            ip4_default_node: ip4_default_next,
            ip6_default_node: ip6_default_next,
        }
    }

    #[inline]
    pub fn with_nodes(mut self, nodes: hammer_runtime::node::NodeRuntime) -> Self {
        self.nodes = Some(nodes);
        self
    }

    /// Wire default nexts into the ICMP-input local-next table and publish slots.
    pub fn attach_consumer(&mut self, consumer: NodeId) -> CoreResult<()> {
        let nodes = self
            .nodes
            .as_ref()
            .ok_or_else(|| CoreError::internal("icmp input attach requires node runtime"))?;
        let ip4_slot = nodes.add_node_next_slot(consumer, self.ip4_default_node)?;
        let ip6_slot = if self.ip6_default_node == self.ip4_default_node {
            ip4_slot
        } else {
            nodes.add_node_next_slot(consumer, self.ip6_default_node)?
        };
        self.consumer = Some(consumer);
        self.inner
            .store(Arc::new(IcmpInputSnapshot::new(ip4_slot, ip6_slot)));
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> IcmpInputNode {
        IcmpInputNode::new(IcmpInputSnapshotHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn register_type(
        &self,
        version: IpVersion,
        icmp_type: u8,
        node: NodeId,
    ) -> CoreResult<u16> {
        let consumer = self.consumer.ok_or_else(|| {
            CoreError::internal("icmp input register_type requires attach_consumer")
        })?;
        let nodes = self
            .nodes
            .as_ref()
            .ok_or_else(|| CoreError::internal("icmp input register_type requires node runtime"))?;
        let slot = nodes.add_node_next_slot(consumer, node)?;
        self.inner.rcu(|current| {
            let mut next = IcmpInputSnapshot::clone(current);
            next.register_type(version, icmp_type, slot);
            next
        });
        Ok(slot)
    }

    #[inline]
    pub fn unregister_type(&self, version: IpVersion, icmp_type: u8) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IcmpInputSnapshot::clone(current);
            next.unregister_type(version, icmp_type);
            next
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct IcmpInputSnapshot {
    ip4: IcmpInputTable,
    ip6: IcmpInputTable,
}

impl IcmpInputSnapshot {
    #[inline]
    fn new(ip4_default_next: u16, ip6_default_next: u16) -> Self {
        let mut ip4 = IcmpInputTable::new(ip4_default_next);
        ip4.set_spec(ICMP4_ECHO_REPLY, IcmpTypeSpec::echo());
        ip4.set_spec(ICMP4_ECHO_REQUEST, IcmpTypeSpec::echo());

        let mut ip6 = IcmpInputTable::new(ip6_default_next);
        ip6.set_spec(ICMP6_ECHO_REQUEST, IcmpTypeSpec::echo());
        ip6.set_spec(ICMP6_ECHO_REPLY, IcmpTypeSpec::echo());

        Self { ip4, ip6 }
    }

    #[inline(always)]
    fn default_next(&self, version: IpVersion) -> u16 {
        self.table(version).default_next()
    }

    #[inline(always)]
    fn next_for_type(&self, version: IpVersion, icmp_type: u8) -> Option<u16> {
        self.table(version).next_for_type(icmp_type)
    }

    #[inline(always)]
    fn slot_for(&self, version: IpVersion, icmp_type: u8) -> u16 {
        self.next_for_type(version, icmp_type)
            .unwrap_or_else(|| self.default_next(version))
    }

    #[inline(always)]
    fn spec(&self, version: IpVersion, icmp_type: u8) -> IcmpTypeSpec {
        self.table(version).spec(icmp_type)
    }

    #[inline(always)]
    fn register_type(&mut self, version: IpVersion, icmp_type: u8, next: u16) {
        self.table_mut(version).register_type(icmp_type, next);
    }

    #[inline(always)]
    fn unregister_type(&mut self, version: IpVersion, icmp_type: u8) {
        self.table_mut(version).unregister_type(icmp_type);
    }

    #[inline(always)]
    fn table(&self, version: IpVersion) -> &IcmpInputTable {
        match version {
            IpVersion::V4 => &self.ip4,
            IpVersion::V6 => &self.ip6,
        }
    }

    #[inline(always)]
    fn table_mut(&mut self, version: IpVersion) -> &mut IcmpInputTable {
        match version {
            IpVersion::V4 => &mut self.ip4,
            IpVersion::V6 => &mut self.ip6,
        }
    }
}

#[derive(Debug, Clone)]
struct IcmpInputTable {
    default_next: u16,
    entries: [IcmpInputEntry; 256],
}

impl IcmpInputTable {
    #[inline]
    fn new(default_next: u16) -> Self {
        Self {
            default_next,
            entries: [IcmpInputEntry::new(default_next); 256],
        }
    }

    #[inline(always)]
    fn default_next(&self) -> u16 {
        self.default_next
    }

    #[inline(always)]
    fn set_spec(&mut self, icmp_type: u8, spec: IcmpTypeSpec) {
        self.entries[icmp_type as usize].spec = spec;
    }

    #[inline(always)]
    fn register_type(&mut self, icmp_type: u8, next: u16) {
        let entry = &mut self.entries[icmp_type as usize];
        entry.next = next;
        entry.registered = true;
    }

    #[inline(always)]
    fn unregister_type(&mut self, icmp_type: u8) {
        let entry = &mut self.entries[icmp_type as usize];
        entry.next = self.default_next;
        entry.registered = false;
    }

    #[inline(always)]
    fn next_for_type(&self, icmp_type: u8) -> Option<u16> {
        let entry = self.entries[icmp_type as usize];
        entry.registered.then_some(entry.next)
    }

    #[inline(always)]
    fn spec(&self, icmp_type: u8) -> IcmpTypeSpec {
        self.entries[icmp_type as usize].spec
    }
}

#[derive(Debug, Clone, Copy)]
struct IcmpInputEntry {
    next: u16,
    spec: IcmpTypeSpec,
    registered: bool,
}

impl IcmpInputEntry {
    #[inline]
    fn new(default_next: u16) -> Self {
        Self {
            next: default_next,
            spec: IcmpTypeSpec::default(),
            registered: false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IcmpTypeSpec {
    max_code: u8,
    min_len: usize,
    min_hop_limit: u8,
}

impl Default for IcmpTypeSpec {
    #[inline]
    fn default() -> Self {
        Self {
            max_code: u8::MAX,
            min_len: ICMP_HEADER_MIN_LEN,
            min_hop_limit: 0,
        }
    }
}

impl IcmpTypeSpec {
    #[inline]
    fn echo() -> Self {
        Self {
            max_code: 0,
            min_len: ICMP_ECHO_HEADER_LEN,
            min_hop_limit: 0,
        }
    }
}

#[derive(Clone)]
struct IcmpInputSnapshotHandle {
    inner: Arc<ArcSwap<IcmpInputSnapshot>>,
}

impl IcmpInputSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<IcmpInputSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<IcmpInputSnapshot>> {
        self.inner.load()
    }
}

#[hammer_component_macros::node]
pub struct IcmpInputNode {
    #[node(default = register_icmp_input_runtime(snapshot.clone()))]
    runtime_data: NodeRuntimeData,
    snapshot: IcmpInputSnapshotHandle,
}

impl Node for IcmpInputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let snapshot = self.snapshot.load();
        icmp_input_process_frame(runtime, frame, &snapshot)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_icmp_input_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        icmp_input_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for IcmpInputNode {}

#[hammer_component_macros::node_next]
pub enum IcmpEchoRequestNext {
    Lookup,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = IcmpEchoRequestNext)]
pub struct IcmpEchoRequestNode;

impl Node for IcmpEchoRequestNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        icmp_echo_request_process_frame(runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_icmp_echo_request_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        icmp_echo_request_process
    }
}

/// Consume IPv4 ICMP Destination Unreachable / Fragmentation Needed and update
/// the IP-owned path MTU cache (Hammer extension beyond VPP core TCP PMTU).
#[hammer_component_macros::node(role = internal)]
pub struct IcmpPathMtuNode;

impl Node for IcmpPathMtuNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        icmp_path_mtu_process_frame(runtime, frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        icmp_path_mtu_process
    }
}

#[derive(Clone)]
struct IcmpInputRuntime {
    snapshot: IcmpInputSnapshotHandle,
}

fn icmp_input_runtimes() -> &'static Mutex<Vec<IcmpInputRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<IcmpInputRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_icmp_input_runtime(snapshot: IcmpInputSnapshotHandle) -> NodeRuntimeData {
    let mut runtimes = icmp_input_runtimes()
        .lock()
        .expect("ICMP input runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IcmpInputRuntime { snapshot });
    NodeRuntimeData::from_usize(slot).expect("ICMP input runtime slot overflow")
}

fn icmp_input_runtime(data: NodeRuntimeData) -> CoreResult<IcmpInputRuntime> {
    let slot = data.usize_word(0)?;
    icmp_input_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("ICMP input runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("ICMP input runtime slot is invalid"))
}

fn icmp_input_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match icmp_input_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    let snapshot = state.snapshot.load();
    icmp_input_process_frame(runtime, frame, &snapshot)
}

fn icmp_echo_request_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    icmp_echo_request_process_frame(runtime, frame)
}

fn icmp_path_mtu_process(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    icmp_path_mtu_process_frame(runtime, frame)
}

fn icmp_path_mtu_process_frame(runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
    for index in frame.iter_indices() {
        let _ = update_path_mtu_from_index(runtime, *index);
    }
    NodeResult::drop()
}

fn update_path_mtu_from_index(runtime: &DataPlaneRuntime, index: Index) -> CoreResult<()> {
    let packet = collect_current_chain_for_icmp_generation(runtime, index)?;
    let _ = super::pmtu::process_ipv4_icmp_path_mtu_packet(packet.as_ref());
    Ok(())
}

#[hammer_component_macros::node_next]
pub enum IcmpErrorNext {
    Drop,
    Lookup,
}

#[hammer_component_macros::node(role = internal, next = IcmpErrorNext)]
pub struct IcmpErrorNode {
    #[node(default = register_icmp_error_runtime(None))]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    source_table: Option<IcmpErrorSourceTableHandle>,
}

impl IcmpErrorNode {
    #[inline]
    pub fn with_source_table(mut self, source_table: IcmpErrorSourceTableHandle) -> Self {
        sync_icmp_error_runtime(self.runtime_data, Some(source_table.clone()))
            .expect("ICMP error runtime slot");
        self.source_table = Some(source_table);
        self
    }
}

impl Node for IcmpErrorNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        let source_table = self
            .source_table
            .as_ref()
            .map(IcmpErrorSourceTableHandle::load);
        let source_table = source_table.as_deref().map(|arc| &**arc);
        icmp_error_process_frame(runtime, frame, source_table)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_icmp_error_trace)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        icmp_error_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_icmp_error_runtime(self.runtime_data, self.source_table.clone())?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct IcmpErrorRuntime {
    source_table: Option<IcmpErrorSourceTableHandle>,
}

fn icmp_error_runtimes() -> &'static Mutex<Vec<IcmpErrorRuntime>> {
    static RUNTIMES: OnceLock<Mutex<Vec<IcmpErrorRuntime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_icmp_error_runtime(
    source_table: Option<IcmpErrorSourceTableHandle>,
) -> NodeRuntimeData {
    let mut runtimes = icmp_error_runtimes()
        .lock()
        .expect("ICMP error runtime registry poisoned");
    let slot = runtimes.len();
    runtimes.push(IcmpErrorRuntime { source_table });
    NodeRuntimeData::from_usize(slot).expect("ICMP error runtime slot overflow")
}

fn sync_icmp_error_runtime(
    data: NodeRuntimeData,
    source_table: Option<IcmpErrorSourceTableHandle>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    let mut runtimes = icmp_error_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("ICMP error runtime registry poisoned"))?;
    let runtime = runtimes
        .get_mut(slot)
        .ok_or_else(|| CoreError::internal("ICMP error runtime slot is invalid"))?;
    runtime.source_table = source_table;
    Ok(())
}

fn icmp_error_runtime(data: NodeRuntimeData) -> CoreResult<IcmpErrorRuntime> {
    let slot = data.usize_word(0)?;
    icmp_error_runtimes()
        .lock()
        .map_err(|_| CoreError::internal("ICMP error runtime registry poisoned"))?
        .get(slot)
        .cloned()
        .ok_or_else(|| CoreError::internal("ICMP error runtime slot is invalid"))
}

fn icmp_error_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match icmp_error_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    let source_table = state
        .source_table
        .as_ref()
        .map(IcmpErrorSourceTableHandle::load);
    let source_table = source_table.as_deref().map(|arc| &**arc);
    icmp_error_process_frame(runtime, frame, source_table)
}

fn icmp_input_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    snapshot: &IcmpInputSnapshot,
) -> NodeResult {
    let drop_slot = snapshot.default_next(IpVersion::V4);
    let mut nexts = Vec::with_capacity(frame.len());
    for index in frame.iter_indices() {
        let slot = match next_slot_for_index(runtime, *index, snapshot) {
            Ok(slot) => slot,
            Err(_) => drop_slot,
        };
        nexts.push(slot);
    }
    runtime.enqueue_to_next(frame, nexts.as_slice());
    NodeResult::drop()
}

fn icmp_echo_request_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
) -> NodeResult {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match next_for_echo_request_index(runtime, index) {
            Ok(next) => next,
            Err(_) => IcmpEchoRequestNext::Drop,
        }
    })
}

fn icmp_error_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    source_table: Option<&IcmpErrorSourceSnapshot>,
) -> NodeResult {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        match next_for_icmp_error_index(runtime, index, source_table) {
            Ok(next) => next,
            Err(_) => IcmpErrorNext::Drop,
        }
    })
}

#[inline(always)]
fn next_slot_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    snapshot: &IcmpInputSnapshot,
) -> CoreResult<u16> {
    let buffer = runtime.get_buffer(index)?;
    let current = buffer.current();
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let parsed = match ip_header(current, network.packet_cursor()) {
        Ok(parsed) => parsed,
        Err(_) => {
            drop(buffer);
            let error = IcmpInputError::BadLength.code();
            set_index_node_error_code(runtime, index, error)?;
            let next = snapshot.default_next(IpVersion::V4);
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpInputTrace {
                    version: None,
                    icmp_type: None,
                    code: None,
                    error: Some(error),
                    next,
                },
            );
            return Ok(next);
        }
    };
    let version = parsed.version;
    let default_next = snapshot.default_next(version);
    if parsed.input_error != IpInputError::None {
        drop(buffer);
        let error = IcmpInputError::BadLength.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: None,
                code: None,
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    }
    match parsed.protocol {
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => {}
        IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Other(_) => {
            drop(buffer);
            let error = IcmpInputError::WrongProtocol.code();
            set_index_node_error_code(runtime, index, error)?;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpInputTrace {
                    version: Some(version),
                    icmp_type: None,
                    code: None,
                    error: Some(error),
                    next: default_next,
                },
            );
            return Ok(default_next);
        }
    }
    let Some(icmp) =
        current.get(parsed.transport_header_offset..parsed.packet_len.min(current.len()))
    else {
        drop(buffer);
        let error = IcmpInputError::BadLength.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: None,
                code: None,
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    };
    let header = match read_header::<IcmpHeader>(current, parsed.transport_header_offset) {
        Ok(header) => header,
        Err(_) => {
            drop(buffer);
            let error = IcmpInputError::BadLength.code();
            set_index_node_error_code(runtime, index, error)?;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpInputTrace {
                    version: Some(version),
                    icmp_type: None,
                    code: None,
                    error: Some(error),
                    next: default_next,
                },
            );
            return Ok(default_next);
        }
    };
    if icmp.len() < ICMP_HEADER_MIN_LEN {
        drop(buffer);
        let error = IcmpInputError::BadLength.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: None,
                code: None,
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    }

    let icmp_type = header.icmp_type();
    let code = header.code();
    if snapshot.next_for_type(version, icmp_type).is_none() {
        drop(buffer);
        let error = IcmpInputError::UnknownType.code();
        set_index_node_error_code(runtime, index, error)?;
        let next = snapshot.slot_for(version, icmp_type);
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next,
            },
        );
        return Ok(next);
    }
    let spec = snapshot.spec(version, icmp_type);
    if code > spec.max_code {
        drop(buffer);
        let error = IcmpInputError::BadCode.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    }
    if icmp.len() < spec.min_len {
        drop(buffer);
        let error = IcmpInputError::TooShort.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    }
    if version == IpVersion::V6
        && current
            .get(7)
            .is_some_and(|hop_limit| *hop_limit < spec.min_hop_limit)
    {
        drop(buffer);
        let error = IcmpInputError::HopLimit.code();
        set_index_node_error_code(runtime, index, error)?;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        );
        return Ok(default_next);
    }

    drop(buffer);
    runtime.get_buffer_mut(index)?.clear_node_error();
    let next = snapshot.slot_for(version, icmp_type);
    let _ = add_packet_trace!(
        runtime,
        index,
        IcmpInputTrace {
            version: Some(version),
            icmp_type: Some(icmp_type),
            code: Some(code),
            error: None,
            next,
        },
    );
    Ok(next)
}

#[inline(always)]
fn next_for_echo_request_index(
    runtime: &DataPlaneRuntime,
    index: Index,
) -> CoreResult<IcmpEchoRequestNext> {
    let packet = collect_current_chain_for_icmp_generation(runtime, index)?;
    match build_echo_reply(packet.as_ref()) {
        Ok(generated) => {
            let generated_len = generated.packet.len();
            {
                let mut buffer = runtime.get_buffer_mut(index)?;
                buffer.truncate(0)?;
                let dst = buffer.writable_tail_mut();
                dst[..generated.packet.len()].copy_from_slice(&generated.packet);
                buffer.commit_writable_tail(generated.packet.len())?;
            }
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            let next = IcmpEchoRequestNext::Lookup;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    generated_len: Some(generated_len),
                    error: None,
                    next: NodeNext::slot(next),
                },
            );
            Ok(next)
        }
        Err(error) => {
            let error = IcmpNodeError::from(error).code();
            set_index_node_error_code(runtime, index, error)?;
            let next = IcmpEchoRequestNext::Drop;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    generated_len: None,
                    error: Some(error),
                    next: NodeNext::slot(next),
                },
            );
            Ok(next)
        }
    }
}

#[inline(always)]
fn next_for_icmp_error_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    source_table: Option<&IcmpErrorSourceSnapshot>,
) -> CoreResult<IcmpErrorNext> {
    let buffer = runtime.get_buffer(index)?;
    let opaque = unsafe { transmute::<_, &IcmpErrorOpaque>(buffer.opaque2()) };
    let Some(metadata) = opaque.icmp_error else {
        let error = IcmpNodeError::MissingMetadata.code();
        set_index_node_error_code(runtime, index, error)?;
        let next = IcmpErrorNext::Drop;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: None,
                ingress_interface: None,
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: NodeNext::slot(next),
            },
        );
        return Ok(next);
    };
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let interface_index = network.sw_if_index[0];
    if interface_index == 0 {
        let error = IcmpNodeError::MissingIngressInterface.code();
        set_index_node_error_code(runtime, index, error)?;
        let next = IcmpErrorNext::Drop;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: Some(metadata.family()),
                ingress_interface: None,
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: NodeNext::slot(next),
            },
        );
        return Ok(next);
    }
    let Some(local_source) = source_table.and_then(|source_table| {
        source_table.lookup(interface_index, version_for_family(metadata.family()))
    }) else {
        let error = IcmpNodeError::MissingSource.code();
        set_index_node_error_code(runtime, index, error)?;
        let next = IcmpErrorNext::Drop;
        let _ = add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: Some(metadata.family()),
                ingress_interface: Some(interface_index),
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: NodeNext::slot(next),
            },
        );
        return Ok(next);
    };
    let original = collect_current_chain_for_icmp_generation(runtime, index)?;
    match build_icmp_error_packet(original.as_ref(), metadata, local_source) {
        Ok(generated) => {
            let generated_len = generated.packet.len();
            {
                let mut buffer = runtime.get_buffer_mut(index)?;
                buffer.truncate(0)?;
                let dst = buffer.writable_tail_mut();
                dst[..generated.packet.len()].copy_from_slice(&generated.packet);
                buffer.commit_writable_tail(generated.packet.len())?;
            }
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            let next = IcmpErrorNext::Lookup;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpErrorTrace {
                    family: Some(metadata.family()),
                    ingress_interface: Some(interface_index),
                    local_source_present: true,
                    generated_len: Some(generated_len),
                    error: None,
                    next: NodeNext::slot(next),
                },
            );
            Ok(next)
        }
        Err(error) => {
            let error = IcmpNodeError::from(error).code();
            set_index_node_error_code(runtime, index, error)?;
            let next = IcmpErrorNext::Drop;
            let _ = add_packet_trace!(
                runtime,
                index,
                IcmpErrorTrace {
                    family: Some(metadata.family()),
                    ingress_interface: Some(interface_index),
                    local_source_present: true,
                    generated_len: None,
                    error: Some(error),
                    next: NodeNext::slot(next),
                },
            );
            Ok(next)
        }
    }
}

#[inline(always)]
fn collect_current_chain_for_icmp_generation(
    runtime: &DataPlaneRuntime,
    index: Index,
) -> CoreResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chain = runtime.chain(index);
    while let Some(buffer) = chain.next() {
        let buffer = buffer?;
        bytes.extend_from_copy_slice(buffer.current());
    }
    Ok(bytes)
}

#[inline(always)]
fn refresh_generated_icmp_metadata(
    runtime: &DataPlaneRuntime,
    index: Index,
    generated: &IcmpGeneratedPacket,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let packet_len = generated.packet.len();
    buffer.clear_node_error();
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, generated.network_header_len)
            .with_transport_header(generated.transport_header_offset, ICMP_ECHO_HEADER_LEN)
            .with_transport_payload_offset(
                generated.transport_header_offset + ICMP_ECHO_HEADER_LEN,
            ),
    );
    let opaque = unsafe { transmute::<_, &mut IcmpErrorOpaque>(buffer.opaque2_mut()) };
    opaque.icmp_error = None;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn icmp_snapshot_storage_returns_registered_or_default_next() {
        let default: u16 = 1;
        let echo: u16 = 2;
        let mut snapshot = IcmpInputSnapshot::new(default, default);

        snapshot.register_type(IpVersion::V4, ICMP4_ECHO_REQUEST, echo);

        assert_eq!(snapshot.slot_for(IpVersion::V4, ICMP4_ECHO_REQUEST), echo);
        assert_eq!(snapshot.slot_for(IpVersion::V4, 13), default);
    }
}
