use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextEnqueue,
    NodeResult,
};
use hammer_core::error::CoreResult;

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
}

impl<G> Node<G> for IcmpInputNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let Some(first) = frame.pending_indices().first().copied() else {
            return Ok(NodeResult::drop());
        };
        let snapshot = self.snapshot.load();
        let first_next = next_node_for_index(runtime, first, &snapshot)?;
        NodeNextEnqueue::new(first_next).validate_frame_with_first_next(
            runtime,
            frame,
            first,
            first_next,
            |index| next_node_for_index(runtime, index, &snapshot),
        )
    }
}

impl<G> InternalNode<G> for IcmpInputNode {}

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
    let Some(next) = snapshot.next_for_type(version, icmp_type) else {
        set_index_node_error_code(runtime, index, IcmpInputError::UnknownType.code())?;
        return Ok(default_next);
    };
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
    Ok(next)
}
