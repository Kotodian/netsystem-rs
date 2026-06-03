use std::net::IpAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, BufferPacketCursor, DataPlaneRuntime, InternalNode, Network, Node,
    NodeId, NodeNextStorage, NodeResult, PacketNextResolver, SocksAddr,
    process_cached_rewrite_next, process_cached_speculative_next,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::icmp::{
    IcmpBuildError, IcmpErrorFamily, IcmpGeneratedPacket, build_echo_reply, build_icmp_error_packet,
};

use crate::data_plane::set_index_node_error_code;

use super::{IpProtocol, IpVersion, parse_ip_packet_with_chain_len};

const ICMP_HEADER_MIN_LEN: usize = 4;
const ICMP_ECHO_HEADER_LEN: usize = 8;
const ICMP4_ECHO_REPLY: u8 = 0;
const ICMP4_ECHO_REQUEST: u8 = 8;
const ICMP6_ECHO_REQUEST: u8 = 128;
const ICMP6_ECHO_REPLY: u8 = 129;

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
    snapshot: IcmpInputSnapshotHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

struct IcmpInputNextResolver {
    snapshot: arc_swap::Guard<Arc<IcmpInputSnapshot>>,
}

impl<G> PacketNextResolver<G> for IcmpInputNextResolver {
    #[inline(always)]
    fn next_for_index(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        next_node_for_index(runtime, index, &self.snapshot)
    }
}

impl<G> Node<G> for IcmpInputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let resolver = IcmpInputNextResolver {
            snapshot: self.snapshot.load(),
        };
        process_cached_speculative_next(runtime, frame, &mut self.cached_next, &resolver)
    }
}

impl<G> InternalNode<G> for IcmpInputNode {}

#[hammer_component_macros::node_next]
pub enum IcmpEchoRequestNext {
    Lookup,
    Drop,
}

#[hammer_component_macros::node(next = IcmpEchoRequestNext)]
pub struct IcmpEchoRequestNode {
    #[node(default)]
    cached_next: Option<NodeId>,
}

struct IcmpEchoRequestNextResolver {
    next: [NodeId; IcmpEchoRequestNext::COUNT],
}

impl<G> PacketNextResolver<G> for IcmpEchoRequestNextResolver {
    #[inline(always)]
    fn next_for_index(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        next_node_for_echo_request_index(runtime, index, self.next)
    }
}

impl<G> Node<G> for IcmpEchoRequestNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let resolver = IcmpEchoRequestNextResolver { next: self.next };
        process_cached_rewrite_next(runtime, frame, &mut self.cached_next, &resolver)
    }
}

impl<G> InternalNode<G> for IcmpEchoRequestNode {}

#[hammer_component_macros::node_next]
pub enum IcmpErrorNext {
    Drop,
    Lookup,
}

#[hammer_component_macros::node(next = IcmpErrorNext)]
pub struct IcmpErrorNode {
    #[node(default)]
    source_table: Option<IcmpErrorSourceTableHandle>,
    #[node(default)]
    cached_next: Option<NodeId>,
}

struct IcmpErrorNextResolver {
    next: [NodeId; IcmpErrorNext::COUNT],
    source_table: Option<arc_swap::Guard<Arc<IcmpErrorSourceSnapshot>>>,
}

impl<G> PacketNextResolver<G> for IcmpErrorNextResolver {
    #[inline(always)]
    fn next_for_index(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        let source_table = self
            .source_table
            .as_ref()
            .map(|source_table| source_table.as_ref());
        next_node_for_icmp_error_index(runtime, index, self.next, source_table)
    }
}

impl IcmpErrorNode {
    #[inline]
    pub fn with_source_table(mut self, source_table: IcmpErrorSourceTableHandle) -> Self {
        self.source_table = Some(source_table);
        self
    }
}

impl<G> Node<G> for IcmpErrorNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let resolver = IcmpErrorNextResolver {
            next: self.next,
            source_table: self
                .source_table
                .as_ref()
                .map(IcmpErrorSourceTableHandle::load),
        };
        process_cached_rewrite_next(runtime, frame, &mut self.cached_next, &resolver)
    }
}

impl<G> InternalNode<G> for IcmpErrorNode {}

#[inline(always)]
fn next_node_for_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    snapshot: &IcmpInputSnapshot,
) -> CoreResult<NodeId> {
    let packet = runtime.copy_current_chain(index)?;
    let packet = packet.as_ref();
    let parsed = match parse_ip_packet_with_chain_len(packet, 0) {
        Ok(parsed) => parsed,
        Err(_) => {
            set_index_node_error_code(runtime, index, IcmpInputError::BadLength.code())?;
            return Ok(snapshot.default_next(IpVersion::V4));
        }
    };
    let version = parsed.version;
    let default_next = snapshot.default_next(version);
    match parsed.protocol {
        IpProtocol::Icmpv4 | IpProtocol::Icmpv6 => {}
        IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Other(_) => {
            set_index_node_error_code(runtime, index, IcmpInputError::WrongProtocol.code())?;
            return Ok(default_next);
        }
    }
    let Some(icmp) = packet.get(parsed.transport_header_offset..parsed.packet_len) else {
        set_index_node_error_code(runtime, index, IcmpInputError::BadLength.code())?;
        return Ok(default_next);
    };
    if icmp.len() < ICMP_HEADER_MIN_LEN {
        set_index_node_error_code(runtime, index, IcmpInputError::BadLength.code())?;
        return Ok(default_next);
    }

    let icmp_type = icmp[0];
    let code = icmp[1];
    let key = IcmpInputKey { version, icmp_type };
    if snapshot.next_for_type(version, icmp_type).is_none() {
        set_index_node_error_code(runtime, index, IcmpInputError::UnknownType.code())?;
        return Ok(NodeNextStorage::next(snapshot, key));
    }
    let spec = snapshot.spec(version, icmp_type);
    if code > spec.max_code {
        set_index_node_error_code(runtime, index, IcmpInputError::BadCode.code())?;
        return Ok(default_next);
    }
    if icmp.len() < spec.min_len {
        set_index_node_error_code(runtime, index, IcmpInputError::TooShort.code())?;
        return Ok(default_next);
    }
    if version == IpVersion::V6
        && packet
            .get(7)
            .is_some_and(|hop_limit| *hop_limit < spec.min_hop_limit)
    {
        set_index_node_error_code(runtime, index, IcmpInputError::HopLimit.code())?;
        return Ok(default_next);
    }

    runtime.get_buffer_mut(index)?.clear_node_error();
    Ok(NodeNextStorage::next(snapshot, key))
}

#[inline(always)]
fn next_node_for_echo_request_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    next: [NodeId; IcmpEchoRequestNext::COUNT],
) -> CoreResult<NodeId> {
    let packet = runtime.copy_current_chain(index)?;
    match build_echo_reply(packet.as_ref()) {
        Ok(generated) => {
            replace_current_chain(runtime, index, &generated.packet)?;
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            Ok(NodeNextStorage::next(&next, IcmpEchoRequestNext::Lookup))
        }
        Err(error) => {
            set_index_node_error_code(runtime, index, IcmpNodeError::from(error).code())?;
            Ok(NodeNextStorage::next(&next, IcmpEchoRequestNext::Drop))
        }
    }
}

#[inline(always)]
fn next_node_for_icmp_error_index<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    next: [NodeId; IcmpErrorNext::COUNT],
    source_table: Option<&IcmpErrorSourceSnapshot>,
) -> CoreResult<NodeId> {
    let Some(metadata) = runtime.with_metadata(index, |metadata| metadata.icmp_error)? else {
        set_index_node_error_code(runtime, index, IcmpNodeError::MissingMetadata.code())?;
        return Ok(NodeNextStorage::next(&next, IcmpErrorNext::Drop));
    };
    let Some(interface_index) =
        runtime.with_metadata(index, |metadata| metadata.ingress_interface)?
    else {
        set_index_node_error_code(
            runtime,
            index,
            IcmpNodeError::MissingIngressInterface.code(),
        )?;
        return Ok(NodeNextStorage::next(&next, IcmpErrorNext::Drop));
    };
    let Some(local_source) = source_table.and_then(|source_table| {
        source_table.lookup(interface_index, version_for_family(metadata.family()))
    }) else {
        set_index_node_error_code(runtime, index, IcmpNodeError::MissingSource.code())?;
        return Ok(NodeNextStorage::next(&next, IcmpErrorNext::Drop));
    };
    let original = runtime.copy_current_chain(index)?;
    match build_icmp_error_packet(original.as_ref(), metadata, local_source) {
        Ok(generated) => {
            replace_current_chain(runtime, index, &generated.packet)?;
            refresh_generated_icmp_metadata(runtime, index, &generated)?;
            Ok(NodeNextStorage::next(&next, IcmpErrorNext::Lookup))
        }
        Err(error) => {
            set_index_node_error_code(runtime, index, IcmpNodeError::from(error).code())?;
            Ok(NodeNextStorage::next(&next, IcmpErrorNext::Drop))
        }
    }
}

#[inline(always)]
fn replace_current_chain<G>(
    runtime: &DataPlaneRuntime<G>,
    index: BufferIndex,
    packet: &[u8],
) -> CoreResult<()> {
    runtime.truncate_chain(index, 0)?;
    runtime.append(index, packet)
}

#[inline(always)]
fn refresh_generated_icmp_metadata<G>(
    runtime: &DataPlaneRuntime<G>,
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
    let metadata = buffer.metadata_mut();
    metadata.network = Network::Icmp;
    metadata.source = Some(SocksAddr::ip(generated.source, 0));
    metadata.destination = Some(SocksAddr::ip(generated.destination, 0));
    metadata.icmp_error = None;
    Ok(())
}

#[cfg(test)]
mod tests {
    use hammer_adapter::{DataPlaneRuntime, NodeNextStorage};

    use super::*;
    use crate::data_plane::DropNode;

    #[test]
    fn icmp_snapshot_storage_returns_registered_or_default_next() {
        let runtime = DataPlaneRuntime::<DropNode>::with_capacities(8, 2, 1, 1);
        let default = runtime.nodes().register_internal(DropNode::new());
        let echo = runtime.nodes().register_internal(DropNode::new());
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
