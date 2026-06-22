use std::mem::{size_of, transmute};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, InternalNode, NetworkOpaque,
    Node, NodeId, NodeNextStorage, NodeProcessFn, NodeResult, NodeRuntimeData, NodeVectorDispatch,
    PacketTrace, SecondaryOpaque, TraceFormatter, add_packet_trace,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::icmp::{
    IcmpBuildError, IcmpErrorFamily, IcmpErrorMetadata, IcmpGeneratedPacket, build_echo_reply,
    build_icmp_error_packet,
};
use hammer_infra::vec::Vec;

use crate::data_plane::set_index_node_error_code;
use crate::trace::codec::{
    TraceDecodeCursor, put_node, put_option_icmp_error_family, put_option_ip_version,
    put_option_u16, put_option_u32, put_option_usize, put_u8,
};

use super::{IpProtocol, IpVersion, parse_ip_packet_with_chain_len};

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
    pub next: NodeId,
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
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpInputTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
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
        put_node(out, self.next);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpEchoRequestTrace {
    pub generated_len: Option<usize>,
    pub error: Option<u16>,
    pub next: NodeId,
}

impl IcmpEchoRequestTrace {
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut cursor = TraceDecodeCursor::new(bytes);
        let trace = Self {
            generated_len: cursor.read_option_usize()?,
            error: cursor.read_option_u16()?,
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpEchoRequestTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_usize(out, self.generated_len);
        put_option_u16(out, self.error);
        put_node(out, self.next);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcmpErrorTrace {
    pub family: Option<IcmpErrorFamily>,
    pub ingress_interface: Option<u32>,
    pub local_source_present: bool,
    pub generated_len: Option<usize>,
    pub error: Option<u16>,
    pub next: NodeId,
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
            next: cursor.read_node()?,
        };
        cursor.is_empty().then_some(trace)
    }
}

impl PacketTrace for IcmpErrorTrace {
    fn encode_trace(&self, out: &mut std::vec::Vec<u8>) {
        put_option_icmp_error_family(out, self.family);
        put_option_u32(out, self.ingress_interface);
        crate::trace::codec::put_bool(out, self.local_source_present);
        put_option_usize(out, self.generated_len);
        put_option_u16(out, self.error);
        put_node(out, self.next);
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
    sources: hammer_infra::vec::Vec<IcmpErrorSourceEntry>,
}

impl IcmpErrorSourceSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            sources: hammer_infra::vec::Vec::new(),
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
                ip4_default_next,
                ip6_default_next,
            ))),
        }
    }

    #[inline]
    pub fn node(&self) -> IcmpInputNode {
        IcmpInputNode::new(IcmpInputSnapshotHandle::new(Arc::clone(&self.inner)))
    }

    #[inline]
    pub fn register_type(&self, version: IpVersion, icmp_type: u8, node: NodeId) -> CoreResult<()> {
        self.inner.rcu(|current| {
            let mut next = IcmpInputSnapshot::clone(current);
            next.register_type(version, icmp_type, node);
            next
        });
        Ok(())
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
    fn new(ip4_default_next: NodeId, ip6_default_next: NodeId) -> Self {
        let mut ip4 = IcmpInputTable::new(ip4_default_next);
        ip4.set_spec(ICMP4_ECHO_REPLY, IcmpTypeSpec::echo());
        ip4.set_spec(ICMP4_ECHO_REQUEST, IcmpTypeSpec::echo());

        let mut ip6 = IcmpInputTable::new(ip6_default_next);
        ip6.set_spec(ICMP6_ECHO_REQUEST, IcmpTypeSpec::echo());
        ip6.set_spec(ICMP6_ECHO_REPLY, IcmpTypeSpec::echo());

        Self { ip4, ip6 }
    }

    #[inline(always)]
    fn default_next(&self, version: IpVersion) -> NodeId {
        self.table(version).default_next()
    }

    #[inline(always)]
    fn next_for_type(&self, version: IpVersion, icmp_type: u8) -> Option<NodeId> {
        self.table(version).next_for_type(icmp_type)
    }

    #[inline(always)]
    fn spec(&self, version: IpVersion, icmp_type: u8) -> IcmpTypeSpec {
        self.table(version).spec(icmp_type)
    }

    #[inline(always)]
    fn register_type(&mut self, version: IpVersion, icmp_type: u8, node: NodeId) {
        self.table_mut(version).register_type(icmp_type, node);
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

#[derive(Debug, Clone, Copy)]
struct IcmpInputKey {
    version: IpVersion,
    icmp_type: u8,
}

impl NodeNextStorage<IcmpInputKey> for IcmpInputSnapshot {
    #[inline(always)]
    fn next(&self, key: IcmpInputKey) -> NodeId {
        self.next_for_type(key.version, key.icmp_type)
            .unwrap_or_else(|| self.default_next(key.version))
    }
}

#[derive(Debug, Clone)]
struct IcmpInputTable {
    default_next: NodeId,
    entries: [IcmpInputEntry; 256],
}

impl IcmpInputTable {
    #[inline]
    fn new(default_next: NodeId) -> Self {
        Self {
            default_next,
            entries: [IcmpInputEntry::new(default_next); 256],
        }
    }

    #[inline(always)]
    fn default_next(&self) -> NodeId {
        self.default_next
    }

    #[inline(always)]
    fn set_spec(&mut self, icmp_type: u8, spec: IcmpTypeSpec) {
        self.entries[icmp_type as usize].spec = spec;
    }

    #[inline(always)]
    fn register_type(&mut self, icmp_type: u8, node: NodeId) {
        let entry = &mut self.entries[icmp_type as usize];
        entry.next = node;
        entry.registered = true;
    }

    #[inline(always)]
    fn unregister_type(&mut self, icmp_type: u8) {
        let entry = &mut self.entries[icmp_type as usize];
        entry.next = self.default_next;
        entry.registered = false;
    }

    #[inline(always)]
    fn next_for_type(&self, icmp_type: u8) -> Option<NodeId> {
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
    next: NodeId,
    spec: IcmpTypeSpec,
    registered: bool,
}

impl IcmpInputEntry {
    #[inline]
    fn new(default_next: NodeId) -> Self {
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
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for IcmpInputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let snapshot = self.snapshot.load();
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| Ok(Some(next_node_for_index(runtime, index, &snapshot)?)),
        )?;
        self.cached_next = cached_next;
        Ok(result)
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
pub struct IcmpEchoRequestNode {
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for IcmpEchoRequestNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                Ok(Some(next_node_for_echo_request_index(
                    runtime, index, next,
                )?))
            },
        )?;
        self.cached_next = cached_next;
        Ok(result)
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
) -> CoreResult<NodeResult> {
    let state = icmp_input_runtime(data)?;
    let snapshot = state.snapshot.load();
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        Ok(Some(next_node_for_index(runtime, index, &snapshot)?))
    })?;
    Ok(result)
}

fn icmp_echo_request_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = IcmpEchoRequestNode::runtime_nexts(runtime)?;
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        Ok(Some(next_node_for_echo_request_index(
            runtime, index, next,
        )?))
    })?;
    Ok(result)
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
    #[node(default)]
    cached_next: Option<NodeId>,
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
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let source_table = self
            .source_table
            .as_ref()
            .map(IcmpErrorSourceTableHandle::load);
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                let source_table = source_table
                    .as_ref()
                    .map(|source_table| source_table.as_ref());
                Ok(Some(next_node_for_icmp_error_index(
                    runtime,
                    index,
                    next,
                    source_table,
                )?))
            },
        )?;
        self.cached_next = cached_next;
        Ok(result)
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
) -> CoreResult<NodeResult> {
    let state = icmp_error_runtime(data)?;
    let next = IcmpErrorNode::runtime_nexts(runtime)?;
    let source_table = state
        .source_table
        .as_ref()
        .map(IcmpErrorSourceTableHandle::load);
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        let source_table = source_table
            .as_ref()
            .map(|source_table| source_table.as_ref());
        Ok(Some(next_node_for_icmp_error_index(
            runtime,
            index,
            next,
            source_table,
        )?))
    })?;
    Ok(result)
}

#[inline(always)]
fn next_node_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    snapshot: &IcmpInputSnapshot,
) -> CoreResult<NodeId> {
    let packet = runtime.copy_current_chain(index)?;
    let packet = packet.as_ref();
    let parsed = match parse_ip_packet_with_chain_len(packet, 0) {
        Ok(parsed) => parsed,
        Err(_) => {
            let error = IcmpInputError::BadLength.code();
            set_index_node_error_code(runtime, index, error)?;
            let next = snapshot.default_next(IpVersion::V4);
            add_packet_trace!(
                runtime,
                index,
                IcmpInputTrace {
                    version: None,
                    icmp_type: None,
                    code: None,
                    error: Some(error),
                    next,
                },
            )?;
            return Ok(next);
        }
    };
    let version = parsed.version;
    let default_next = snapshot.default_next(version);
    match parsed.protocol {
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => {}
        IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Other(_) => {
            let error = IcmpInputError::WrongProtocol.code();
            set_index_node_error_code(runtime, index, error)?;
            add_packet_trace!(
                runtime,
                index,
                IcmpInputTrace {
                    version: Some(version),
                    icmp_type: None,
                    code: None,
                    error: Some(error),
                    next: default_next,
                },
            )?;
            return Ok(default_next);
        }
    }
    let Some(icmp) = packet.get(parsed.transport_header_offset..parsed.packet_len) else {
        let error = IcmpInputError::BadLength.code();
        set_index_node_error_code(runtime, index, error)?;
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: None,
                code: None,
                error: Some(error),
                next: default_next,
            },
        )?;
        return Ok(default_next);
    };
    if icmp.len() < ICMP_HEADER_MIN_LEN {
        let error = IcmpInputError::BadLength.code();
        set_index_node_error_code(runtime, index, error)?;
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: None,
                code: None,
                error: Some(error),
                next: default_next,
            },
        )?;
        return Ok(default_next);
    }

    let icmp_type = icmp[0];
    let code = icmp[1];
    let key = IcmpInputKey { version, icmp_type };
    if snapshot.next_for_type(version, icmp_type).is_none() {
        let error = IcmpInputError::UnknownType.code();
        set_index_node_error_code(runtime, index, error)?;
        let next = NodeNextStorage::next(snapshot, key);
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next,
            },
        )?;
        return Ok(next);
    }
    let spec = snapshot.spec(version, icmp_type);
    if code > spec.max_code {
        let error = IcmpInputError::BadCode.code();
        set_index_node_error_code(runtime, index, error)?;
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        )?;
        return Ok(default_next);
    }
    if icmp.len() < spec.min_len {
        let error = IcmpInputError::TooShort.code();
        set_index_node_error_code(runtime, index, error)?;
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        )?;
        return Ok(default_next);
    }
    if version == IpVersion::V6
        && packet
            .get(7)
            .is_some_and(|hop_limit| *hop_limit < spec.min_hop_limit)
    {
        let error = IcmpInputError::HopLimit.code();
        set_index_node_error_code(runtime, index, error)?;
        add_packet_trace!(
            runtime,
            index,
            IcmpInputTrace {
                version: Some(version),
                icmp_type: Some(icmp_type),
                code: Some(code),
                error: Some(error),
                next: default_next,
            },
        )?;
        return Ok(default_next);
    }

    runtime.get_buffer_mut(index)?.clear_node_error();
    let next = NodeNextStorage::next(snapshot, key);
    add_packet_trace!(
        runtime,
        index,
        IcmpInputTrace {
            version: Some(version),
            icmp_type: Some(icmp_type),
            code: Some(code),
            error: None,
            next,
        },
    )?;
    Ok(next)
}

#[inline(always)]
fn next_node_for_echo_request_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: [NodeId; IcmpEchoRequestNext::COUNT],
) -> CoreResult<NodeId> {
    let packet = runtime.copy_current_chain(index)?;
    match build_echo_reply(packet.as_ref()) {
        Ok(generated) => {
            let generated_len = generated.packet.len();
            replace_current_chain(runtime, index, &generated.packet)?;
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            let resolved = NodeNextStorage::next(&next, IcmpEchoRequestNext::Lookup);
            add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    generated_len: Some(generated_len),
                    error: None,
                    next: resolved,
                },
            )?;
            Ok(resolved)
        }
        Err(error) => {
            let error = IcmpNodeError::from(error).code();
            set_index_node_error_code(runtime, index, error)?;
            let resolved = NodeNextStorage::next(&next, IcmpEchoRequestNext::Drop);
            add_packet_trace!(
                runtime,
                index,
                IcmpEchoRequestTrace {
                    generated_len: None,
                    error: Some(error),
                    next: resolved,
                },
            )?;
            Ok(resolved)
        }
    }
}

#[inline(always)]
fn next_node_for_icmp_error_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    next: [NodeId; IcmpErrorNext::COUNT],
    source_table: Option<&IcmpErrorSourceSnapshot>,
) -> CoreResult<NodeId> {
    let buffer = runtime.get_buffer(index)?;
    let opaque = unsafe { transmute::<_, &IcmpErrorOpaque>(buffer.opaque2()) };
    let Some(metadata) = opaque.icmp_error else {
        let error = IcmpNodeError::MissingMetadata.code();
        set_index_node_error_code(runtime, index, error)?;
        let resolved = NodeNextStorage::next(&next, IcmpErrorNext::Drop);
        add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: None,
                ingress_interface: None,
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: resolved,
            },
        )?;
        return Ok(resolved);
    };
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let interface_index = network.sw_if_index[0];
    if interface_index == 0 {
        let error = IcmpNodeError::MissingIngressInterface.code();
        set_index_node_error_code(runtime, index, error)?;
        let resolved = NodeNextStorage::next(&next, IcmpErrorNext::Drop);
        add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: Some(metadata.family()),
                ingress_interface: None,
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: resolved,
            },
        )?;
        return Ok(resolved);
    }
    let Some(local_source) = source_table.and_then(|source_table| {
        source_table.lookup(interface_index, version_for_family(metadata.family()))
    }) else {
        let error = IcmpNodeError::MissingSource.code();
        set_index_node_error_code(runtime, index, error)?;
        let resolved = NodeNextStorage::next(&next, IcmpErrorNext::Drop);
        add_packet_trace!(
            runtime,
            index,
            IcmpErrorTrace {
                family: Some(metadata.family()),
                ingress_interface: Some(interface_index),
                local_source_present: false,
                generated_len: None,
                error: Some(error),
                next: resolved,
            },
        )?;
        return Ok(resolved);
    };
    let original = runtime.copy_current_chain(index)?;
    match build_icmp_error_packet(original.as_ref(), metadata, local_source) {
        Ok(generated) => {
            let generated_len = generated.packet.len();
            replace_current_chain(runtime, index, &generated.packet)?;
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            let resolved = NodeNextStorage::next(&next, IcmpErrorNext::Lookup);
            add_packet_trace!(
                runtime,
                index,
                IcmpErrorTrace {
                    family: Some(metadata.family()),
                    ingress_interface: Some(interface_index),
                    local_source_present: true,
                    generated_len: Some(generated_len),
                    error: None,
                    next: resolved,
                },
            )?;
            Ok(resolved)
        }
        Err(error) => {
            let error = IcmpNodeError::from(error).code();
            set_index_node_error_code(runtime, index, error)?;
            let resolved = NodeNextStorage::next(&next, IcmpErrorNext::Drop);
            add_packet_trace!(
                runtime,
                index,
                IcmpErrorTrace {
                    family: Some(metadata.family()),
                    ingress_interface: Some(interface_index),
                    local_source_present: true,
                    generated_len: None,
                    error: Some(error),
                    next: resolved,
                },
            )?;
            Ok(resolved)
        }
    }
}

#[inline(always)]
fn replace_current_chain(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    runtime.truncate_chain(index, 0)?;
    runtime.append(index, packet)
}

#[inline(always)]
fn refresh_generated_icmp_metadata(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    generated: &IcmpGeneratedPacket,
) -> CoreResult<()> {
    let mut buffer = runtime.get_buffer_mut(index)?;
    let packet_len = generated.packet.len();
    buffer.clear_node_error();
    buffer.set_packet_cursor(
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
    use hammer_adapter::{NodeId, NodeNextStorage};

    use super::*;

    #[test]
    fn icmp_snapshot_storage_returns_registered_or_default_next() {
        let default = NodeId::new(1);
        let echo = NodeId::new(2);
        let mut snapshot = IcmpInputSnapshot::new(default, default);

        snapshot.register_type(IpVersion::V4, ICMP4_ECHO_REQUEST, echo);

        assert_eq!(
            NodeNextStorage::next(
                &snapshot,
                IcmpInputKey {
                    version: IpVersion::V4,
                    icmp_type: ICMP4_ECHO_REQUEST,
                },
            ),
            echo
        );
        assert_eq!(
            NodeNextStorage::next(
                &snapshot,
                IcmpInputKey {
                    version: IpVersion::V4,
                    icmp_type: 13,
                },
            ),
            default
        );
    }
}
