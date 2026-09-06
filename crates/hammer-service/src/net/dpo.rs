use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::slice;

use hammer_core::data_plane::NodeId;
use hammer_runtime::{DataPlaneMain, RuntimeError};

use super::fib::FibEntryFlags;

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct DpoProto(u8);

impl DpoProto {
    pub const IP4: Self = Self(0);
    pub const IP6: Self = Self(1);
    pub const NONE: Self = Self(7);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct DpoType(u8);

impl DpoType {
    pub const INVALID: Self = Self(0);
    pub const DROP: Self = Self(1);
    pub const PUNT: Self = Self(3);
    pub const LOAD_BALANCE: Self = Self(4);
    pub const REPLICATE: Self = Self(5);
    pub const ADJACENCY: Self = Self(6);
    pub const ADJACENCY_INCOMPLETE: Self = Self(7);
    pub const ADJACENCY_MIDCHAIN: Self = Self(8);
    pub const ADJACENCY_GLEAN: Self = Self(9);
    pub const ADJACENCY_MCAST: Self = Self(10);
    pub const ADJACENCY_MCAST_MIDCHAIN: Self = Self(11);
    pub const RECEIVE: Self = Self(12);
    pub const LOOKUP: Self = Self(13);
    // Values 14..18 and 21..29 are reserved for plugin-owned DPO classes that
    // are not part of protocol-neutral net service. Keep VPP's numeric layout
    // so interface DPO identities remain interoperable with the class key
    // space.
    pub const INTERFACE_RX: Self = Self(19);
    pub const INTERFACE_TX: Self = Self(20);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct DpoId(u64);

impl PartialEq for DpoId {
    fn eq(&self, other: &Self) -> bool {
        self.class() == other.class() && self.index() == other.index()
    }
}

impl Eq for DpoId {}

impl std::hash::Hash for DpoId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.class().hash(state);
        self.index().hash(state);
    }
}

impl DpoId {
    pub const INVALID: Self = Self::with_next(DpoType::INVALID, DpoProto::NONE, u32::MAX, 0);

    const fn new(dpo_type: DpoType, proto: DpoProto, index: u32) -> Self {
        Self::with_next(dpo_type, proto, index, 0)
    }

    const fn with_next(dpo_type: DpoType, proto: DpoProto, index: u32, next: u16) -> Self {
        Self(
            dpo_type.get() as u64
                | ((proto.get() as u64) << 8)
                | ((next as u64) << 16)
                | ((index as u64) << 32),
        )
    }

    pub const fn drop(proto: DpoProto) -> Self {
        Self::new(DpoType::DROP, proto, proto.get() as u32)
    }

    pub const fn punt(proto: DpoProto) -> Self {
        Self::new(DpoType::PUNT, proto, 1)
    }

    pub const fn receive(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::RECEIVE, proto, index)
    }

    pub const fn adjacency(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::ADJACENCY, proto, index)
    }

    pub const fn load_balance(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::LOAD_BALANCE, proto, index)
    }

    pub const fn replicate(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::REPLICATE, proto, index)
    }

    pub const fn lookup(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::LOOKUP, proto, index)
    }

    pub const fn interface_rx(proto: DpoProto, index: u32) -> Self {
        Self::new(DpoType::INTERFACE_RX, proto, index)
    }

    pub const fn interface_tx(proto: DpoProto, sw_if_index: u32) -> Self {
        Self::new(DpoType::INTERFACE_TX, proto, sw_if_index)
    }

    pub const fn class(self) -> DpoType {
        DpoType::new(self.0 as u8)
    }

    pub const fn proto(self) -> DpoProto {
        DpoProto::new((self.0 >> 8) as u8)
    }

    pub const fn index(self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub const fn next(self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// Returns whether this identity refers to an initialized DPO object.
    /// The check follows VPP's `dpo_id_is_valid`: the invalid class and the
    /// invalid pool index are both reserved sentinels.
    pub const fn is_valid(self) -> bool {
        self.class().get() != DpoType::INVALID.get() && self.index() != u32::MAX
    }

    // The graph slot is written only after DpoMain has resolved a registered edge.
    const fn stack(self, next: u16) -> Self {
        Self::with_next(self.class(), self.proto(), self.index(), next)
    }
}

const _: () = assert!(size_of::<DpoId>() == 8);
const _: () = assert!(align_of::<DpoId>() == 8);

#[derive(Debug, thiserror::Error)]
pub enum DpoError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("DPO class registration repeats protocol {proto}")]
    DuplicateProtocol { proto: u8 },
    #[error("DPO class registration cannot use protocol {proto}")]
    InvalidProtocol { proto: u8 },
    #[error("DPO class {dpo_type} has no node for protocol {proto}")]
    NodeMissing { dpo_type: u8, proto: u8 },
    #[error("failed to add DPO graph edge from node {child:?} to parent {parent:?}")]
    GraphEdgeAdd {
        child: NodeId,
        parent: NodeId,
        #[source]
        source: RuntimeError,
    },
    #[error("load-balance protocol {actual} does not match requested protocol {expected}")]
    ProtocolMismatch { actual: u8, expected: u8 },
    #[error("DPO class {actual} does not support operation for class {expected}")]
    TypeMismatch { actual: u8, expected: u8 },
    #[error("DPO object {dpo_type}:{index} is not present")]
    ObjectMissing { dpo_type: u8, index: u32 },
    #[error("DPO bucket count must be zero or a power of two")]
    InvalidBucketCount,
    #[error("a zero-bucket DPO class {dpo_type} cannot be published")]
    EmptyDpoPublication { dpo_type: u8 },
    #[error("DPO bucket storage does not match its bucket count")]
    InvalidBucketStorage,
    #[error("DPO type key space is exhausted at {next_type}")]
    TypeKeySpaceExhausted { next_type: u8 },
    #[error("DPO builtin type {dpo_type} is outside the reserved range")]
    InvalidBuiltinType { dpo_type: u8 },
}

impl From<DpoError> for RuntimeError {
    fn from(error: DpoError) -> Self {
        match error {
            DpoError::Runtime(error) => error,
            error => RuntimeError::Subsystem {
                subsystem: "DPO",
                source: Box::new(error),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct DpoMain {
    // These tables mirror VPP's dpo_nodes[type][proto] and dpo_edges
    // vectors. Empty node lists are valid for instance-dependent classes.
    nodes: Vec<Vec<Vec<NodeId>>>,
    edges: Vec<Vec<Vec<Vec<u16>>>>,
    pub(super) locks: Vec<Option<fn(DpoId)>>,
    pub(super) unlocks: Vec<Option<fn(DpoId)>>,
    next_type: u8,
}

impl Default for DpoMain {
    fn default() -> Self {
        Self::new()
    }
}

// VPP allocates plugin classes from DPO_LAST (30). The unused values below
// are deliberately reserved; service net does not define the business classes
// that occupy them in VPP.
const DYNAMIC_TYPE_START: u8 = 30;
const DPO_PROTO_COUNT: u8 = 6;
const NO_EDGE: u16 = u16::MAX;

impl DpoMain {
    pub const fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            locks: Vec::new(),
            unlocks: Vec::new(),
            next_type: DYNAMIC_TYPE_START,
        }
    }

    fn node_slot_mut(&mut self, dpo_type: DpoType, proto: DpoProto) -> &mut Vec<NodeId> {
        let type_index = usize::from(dpo_type.get());
        let proto_index = usize::from(proto.get());
        if self.nodes.len() <= type_index {
            self.nodes.resize_with(type_index + 1, Vec::new);
        }
        if self.nodes[type_index].len() <= proto_index {
            self.nodes[type_index].resize_with(proto_index + 1, Vec::new);
        }
        &mut self.nodes[type_index][proto_index]
    }

    fn edge_slot_mut(
        &mut self,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoType,
        parent_proto: DpoProto,
    ) -> &mut u16 {
        let child_index = usize::from(child.get());
        let child_proto_index = usize::from(child_proto.get());
        let parent_index = usize::from(parent.get());
        let parent_proto_index = usize::from(parent_proto.get());
        if self.edges.len() <= child_index {
            self.edges.resize_with(child_index + 1, Vec::new);
        }
        if self.edges[child_index].len() <= child_proto_index {
            self.edges[child_index].resize_with(child_proto_index + 1, Vec::new);
        }
        if self.edges[child_index][child_proto_index].len() <= parent_index {
            self.edges[child_index][child_proto_index].resize_with(parent_index + 1, Vec::new);
        }
        if self.edges[child_index][child_proto_index][parent_index].len() <= parent_proto_index {
            self.edges[child_index][child_proto_index][parent_index]
                .resize(parent_proto_index + 1, NO_EDGE);
        }
        &mut self.edges[child_index][child_proto_index][parent_index][parent_proto_index]
    }

    fn edge_slot(
        &self,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoType,
        parent_proto: DpoProto,
    ) -> Option<u16> {
        self.edges
            .get(usize::from(child.get()))?
            .get(usize::from(child_proto.get()))?
            .get(usize::from(parent.get()))?
            .get(usize::from(parent_proto.get()))
            .copied()
            .filter(|next| *next != NO_EDGE)
    }

    fn validate_protocol(proto: DpoProto) -> Result<(), DpoError> {
        // VPP's dpo_proto_t has six packet-graph protocols (0..=5) and
        // reserves 7 as DPO_PROTO_NONE. Values outside that space are not
        // extension points: they would create graph-table entries with no
        // corresponding protocol semantics.
        (proto.get() < DPO_PROTO_COUNT)
            .then_some(())
            .ok_or(DpoError::InvalidProtocol { proto: proto.get() })
    }

    /// Class/node metadata is worker-visible after the graph starts. Startup
    /// registration is allowed before workers exist; live registration must
    /// already be inside the process barrier, just like VPP's dpo registry
    /// mutation is serialized with worker graph access.
    fn require_registration_scope() -> Result<(), DpoError> {
        let Some(barrier) = hammer_runtime::barrier::global() else {
            return Ok(());
        };
        if barrier.worker_count() == 0 {
            return Ok(());
        }
        hammer_runtime::ensure_main_thread_with_barrier().map_err(DpoError::Runtime)
    }

    /// Allocates one class key and binds the originating graph nodes for each
    /// data-path protocol. The node list is the Rust equivalent of VPP's
    /// NULL-terminated `dpo_nodes[type][proto]` list.
    pub fn register_new_type(
        &mut self,
        nodes: &[(DpoProto, &[NodeId])],
        locks: Option<(fn(DpoId), fn(DpoId))>,
    ) -> Result<DpoType, DpoError> {
        Self::require_registration_scope()?;
        for (position, (proto, _)) in nodes.iter().enumerate() {
            Self::validate_protocol(*proto)?;
            if nodes[..position].iter().any(|(other, _)| other == proto) {
                return Err(DpoError::DuplicateProtocol { proto: proto.get() });
            }
        }
        let dpo_type = DpoType::new(self.next_type);
        self.next_type = self
            .next_type
            .checked_add(1)
            .ok_or(DpoError::TypeKeySpaceExhausted {
                next_type: self.next_type,
            })?;
        for (proto, node_ids) in nodes {
            *self.node_slot_mut(dpo_type, *proto) = node_ids.to_vec();
        }
        let index = usize::from(dpo_type.get());
        self.locks.resize(index + 1, None);
        self.unlocks.resize(index + 1, None);
        if let Some((lock, unlock)) = locks {
            self.locks[index] = Some(lock);
            self.unlocks[index] = Some(unlock);
        }
        Ok(dpo_type)
    }

    pub fn nodes(&self, dpo_type: DpoType, proto: DpoProto) -> Option<&[NodeId]> {
        self.nodes
            .get(usize::from(dpo_type.get()))?
            .get(usize::from(proto.get()))
            .map(Vec::as_slice)
    }

    /// Builds an identity for a class/protocol that this registry has
    /// registered. The concrete class owner still validates the pool index;
    /// the registry only prevents an unregistered class/protocol pair from
    /// entering a forwarding object.
    pub fn identity(
        &self,
        dpo_type: DpoType,
        proto: DpoProto,
        index: u32,
    ) -> Result<DpoId, DpoError> {
        self.nodes(dpo_type, proto).ok_or(DpoError::NodeMissing {
            dpo_type: dpo_type.get(),
            proto: proto.get(),
        })?;
        Ok(DpoId::new(dpo_type, proto, index))
    }

    pub fn node(&self, dpo_type: DpoType, proto: DpoProto) -> Option<NodeId> {
        self.nodes(dpo_type, proto)
            .and_then(|node_ids| node_ids.first().copied())
    }

    /// Returns a previously resolved graph edge without creating topology.
    /// This is the read-only counterpart of VPP's
    /// `dpo_get_next_node_by_type_and_proto`.
    pub fn next_node(
        &self,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoType,
        parent_proto: DpoProto,
    ) -> Option<u16> {
        self.edge_slot(child, child_proto, parent, parent_proto)
    }

    pub fn register_builtin(
        &mut self,
        dpo_type: DpoType,
        nodes: &[(DpoProto, &[NodeId])],
    ) -> Result<(), DpoError> {
        Self::require_registration_scope()?;
        if dpo_type.get() >= DYNAMIC_TYPE_START || dpo_type == DpoType::INVALID {
            return Err(DpoError::InvalidBuiltinType {
                dpo_type: dpo_type.get(),
            });
        }
        for (position, (proto, _)) in nodes.iter().enumerate() {
            Self::validate_protocol(*proto)?;
            if nodes[..position].iter().any(|(other, _)| other == proto)
                || self.nodes(dpo_type, *proto).is_some()
            {
                return Err(DpoError::DuplicateProtocol { proto: proto.get() });
            }
        }
        for (proto, node_ids) in nodes {
            *self.node_slot_mut(dpo_type, *proto) = node_ids.to_vec();
        }
        Ok(())
    }

    /// Stacks a concrete child node on parent nodes resolved by the concrete
    /// parent owner. This is the instance-dependent counterpart of `stack`:
    /// Interface TX, Interface RX, and other DPOs whose next node depends on
    /// their object identity must supply these nodes instead of using the
    /// class-wide `dpo_nodes[type][proto]` table.
    pub fn stack_from_node(
        &mut self,
        runtime: &mut DataPlaneMain,
        child_node: NodeId,
        parent: DpoId,
        parent_nodes: &[NodeId],
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if !parent.is_valid() {
            return Err(DpoError::ObjectMissing {
                dpo_type: parent.class().get(),
                index: parent.index(),
            });
        }
        if parent_nodes.is_empty() {
            return Err(DpoError::NodeMissing {
                dpo_type: parent.class().get(),
                proto: parent.proto().get(),
            });
        }
        let mut needs_barrier = false;
        for &parent_node in parent_nodes {
            if runtime
                .nodes()
                .node_next_slot_for_target(child_node, parent_node)
                .map_err(DpoError::Runtime)?
                .is_none()
            {
                needs_barrier = true;
            }
        }
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if needs_barrier
            && workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            return hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.stack_from_node_inner(runtime, child_node, parent, parent_nodes)
            });
        }
        self.stack_from_node_inner(runtime, child_node, parent, parent_nodes)
    }

    fn stack_from_node_inner(
        &mut self,
        runtime: &DataPlaneMain,
        child_node: NodeId,
        parent: DpoId,
        parent_nodes: &[NodeId],
    ) -> Result<DpoId, DpoError> {
        let edges: Vec<_> = parent_nodes
            .iter()
            .copied()
            .map(|parent_node| (child_node, parent_node))
            .collect();
        let slots = runtime
            .nodes()
            .add_node_next_slots(&edges)
            .map_err(|source| {
                let (child, parent) = edges[0];
                DpoError::GraphEdgeAdd {
                    child,
                    parent,
                    source,
                }
            })?;
        Ok(parent.stack(*slots.last().expect("non-empty parent node list")))
    }

    pub fn stack(
        &mut self,
        runtime: &mut DataPlaneMain,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoId,
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        if !parent.is_valid() {
            return Err(DpoError::ObjectMissing {
                dpo_type: parent.class().get(),
                index: parent.index(),
            });
        }
        let child_nodes = self
            .nodes(child, child_proto)
            .ok_or(DpoError::NodeMissing {
                dpo_type: child.get(),
                proto: child_proto.get(),
            })?
            .to_vec();
        let parent_nodes = self
            .nodes(parent.class(), parent.proto())
            .ok_or(DpoError::NodeMissing {
                dpo_type: parent.class().get(),
                proto: parent.proto().get(),
            })?
            .to_vec();
        if child_nodes.is_empty() || parent_nodes.is_empty() {
            return Err(DpoError::NodeMissing {
                dpo_type: child.get(),
                proto: child_proto.get(),
            });
        }
        if let Some(next) = self.edge_slot(child, child_proto, parent.class(), parent.proto()) {
            return Ok(parent.stack(next));
        }

        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            return hammer_runtime::worker_thread_barrier_sync!(runtime, {
                self.stack_inner(
                    runtime,
                    child,
                    child_proto,
                    parent,
                    child_nodes,
                    parent_nodes,
                )
            });
        }
        self.stack_inner(
            runtime,
            child,
            child_proto,
            parent,
            child_nodes,
            parent_nodes,
        )
    }

    fn stack_inner(
        &mut self,
        runtime: &DataPlaneMain,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoId,
        child_nodes: Vec<NodeId>,
        parent_nodes: Vec<NodeId>,
    ) -> Result<DpoId, DpoError> {
        let edges: Vec<_> = child_nodes
            .iter()
            .flat_map(|child_node| {
                parent_nodes
                    .iter()
                    .map(move |parent_node| (*child_node, *parent_node))
            })
            .collect();
        let slots = runtime
            .nodes()
            .add_node_next_slots(&edges)
            .map_err(|source| {
                let (child, parent) = edges[0];
                DpoError::GraphEdgeAdd {
                    child,
                    parent,
                    source,
                }
            })?;
        let next = slots[0];
        for &slot in &slots[1..] {
            assert_eq!(
                next, slot,
                "DPO sibling graph edges must resolve to one slot"
            );
        }
        *self.edge_slot_mut(child, child_proto, parent.class(), parent.proto()) = next;
        Ok(parent.stack(next))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveDpo<A> {
    pub sw_if_index: u32,
    pub address: A,
    pub lock_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupInput {
    SourceAddress,
    DestinationAddress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupTable {
    FromInputInterface,
    Configured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupCast {
    Unicast,
    Multicast,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupDpo {
    pub fib_index: u32,
    pub proto: DpoProto,
    pub input: LookupInput,
    pub table: LookupTable,
    pub cast: LookupCast,
    pub lock_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyDpo<A, R> {
    pub config_index: u32,
    pub lock_count: u32,
    pub sw_if_index: u32,
    pub next_hop: Option<A>,
    pub rewrite: R,
    pub child: Option<DpoId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceRxDpo {
    pub sw_if_index: u32,
    pub proto: DpoProto,
    pub lock_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceTxDpo {
    pub sw_if_index: u32,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct ReplicateFlags: u8 {
        const HAS_LOCAL = 1 << 0;
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct ReplicateDpo {
    bucket_count: u16,
    pub proto: DpoProto,
    pub flags: ReplicateFlags,
    pub(super) lock_count: u32,
    overflow: Option<NonNull<DpoId>>,
    pub inline_buckets: [DpoId; 4],
    pub(super) locked_buckets: u16,
}

const _: () = assert!(size_of::<ReplicateDpo>() == 64);
const _: () = assert!(align_of::<ReplicateDpo>() == 64);

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct LoadBalanceFlags: u8 {
        const USES_MAP = 1 << 0;
        const STICKY = 1 << 1;
    }
}

#[repr(C, align(64))]
#[derive(Debug)]
pub struct LoadBalanceDpo {
    bucket_count: u16,
    bucket_mask: u16,
    pub proto: DpoProto,
    pub flags: LoadBalanceFlags,
    pub fib_entry_flags: FibEntryFlags,
    pub(super) lock_count: u32,
    pub map_index: u32,
    pub urpf_index: u32,
    pub flow_hash_config: u16,
    pub(super) locked_buckets: u16,
    overflow: Option<NonNull<DpoId>>,
    pub inline_buckets: [DpoId; 4],
}

const _: () = assert!(size_of::<LoadBalanceDpo>() == 64);
const _: () = assert!(align_of::<LoadBalanceDpo>() == 64);

impl LoadBalanceDpo {
    pub const INLINE_BUCKETS: usize = 4;
    pub const MAX_BUCKETS: usize = 8192;

    #[inline(always)]
    pub const fn bucket_count(&self) -> u16 {
        self.bucket_count
    }

    #[inline(always)]
    pub const fn bucket_mask(&self) -> u16 {
        self.bucket_mask
    }

    #[inline(always)]
    pub const fn flow_hash_config(&self) -> u16 {
        self.flow_hash_config
    }

    pub(crate) fn publish_root(&mut self) {
        assert!(
            self.lock_count < u32::MAX,
            "load-balance reference count overflow"
        );
        self.lock_count += 1;
    }

    pub(crate) fn withdraw_root(&mut self) -> bool {
        assert!(
            self.lock_count != 0,
            "load-balance reference count underflow"
        );
        self.lock_count -= 1;
        self.lock_count == 0
    }

    pub fn new(
        proto: DpoProto,
        buckets: &[DpoId],
        flags: LoadBalanceFlags,
        flow_hash_config: u16,
    ) -> Result<Self, DpoError> {
        Self::validate_bucket_count(buckets.len())?;
        let (bucket_count, bucket_mask, inline_buckets, overflow) = Self::bucket_storage(buckets);
        Ok(Self {
            bucket_count,
            bucket_mask,
            proto,
            flags,
            fib_entry_flags: FibEntryFlags::empty(),
            // Detached objects acquire their first root when the owner
            // publishes the returned identity into forwarding state.
            lock_count: 0,
            map_index: u32::MAX,
            urpf_index: u32::MAX,
            flow_hash_config,
            locked_buckets: 0,
            inline_buckets,
            overflow,
        })
    }

    pub(crate) fn validate_bucket_count(count: usize) -> Result<(), DpoError> {
        if (count != 0 && !count.is_power_of_two()) || count > Self::MAX_BUCKETS {
            return Err(DpoError::InvalidBucketCount);
        }
        Ok(())
    }

    pub(crate) fn validate_storage(&self) -> Result<(), DpoError> {
        Self::validate_bucket_count(usize::from(self.bucket_count))?;
        let has_overflow = self.bucket_count as usize > Self::INLINE_BUCKETS;
        if has_overflow != self.overflow.is_some() {
            return Err(DpoError::InvalidBucketStorage);
        }
        Ok(())
    }

    fn bucket_storage(
        buckets: &[DpoId],
    ) -> (
        u16,
        u16,
        [DpoId; Self::INLINE_BUCKETS],
        Option<NonNull<DpoId>>,
    ) {
        let mut inline_buckets = [DpoId::INVALID; Self::INLINE_BUCKETS];
        for (slot, bucket) in buckets.iter().take(Self::INLINE_BUCKETS).enumerate() {
            inline_buckets[slot] = *bucket;
        }
        let overflow = if buckets.len() > Self::INLINE_BUCKETS {
            allocate_bucket_storage(&buckets[Self::INLINE_BUCKETS..])
        } else {
            None
        };
        (
            buckets.len() as u16,
            buckets.len().saturating_sub(1) as u16,
            inline_buckets,
            overflow,
        )
    }

    pub(crate) fn replace_buckets(&mut self, buckets: &[DpoId]) -> Result<(), DpoError> {
        assert_eq!(
            self.locked_buckets, 0,
            "owning bucket replacement requires an owner transaction"
        );
        Self::validate_bucket_count(buckets.len())?;
        let (bucket_count, bucket_mask, inline_buckets, overflow) = Self::bucket_storage(buckets);
        self.release_overflow();
        self.inline_buckets = inline_buckets;
        self.overflow = overflow;
        self.bucket_count = bucket_count;
        self.bucket_mask = bucket_mask;
        Ok(())
    }

    #[inline(always)]
    pub fn select_bucket(&self, hash: u32) -> Option<DpoId> {
        if self.bucket_count == 0 {
            return None;
        }
        let bucket = (hash & u32::from(self.bucket_mask)) as usize;
        if bucket < Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[bucket]);
        }
        self.overflow_slice()
            .get(bucket - Self::INLINE_BUCKETS)
            .copied()
    }

    #[inline(always)]
    pub(crate) fn bucket_mut(&mut self, index: usize) -> Option<&mut DpoId> {
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if index < Self::INLINE_BUCKETS {
            return Some(&mut self.inline_buckets[index]);
        }
        self.overflow_slice_mut()
            .get_mut(index - Self::INLINE_BUCKETS)
    }

    #[inline(always)]
    fn overflow_slice(&self) -> &[DpoId] {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        let Some(pointer) = self.overflow else {
            return &[];
        };
        // SAFETY: `pointer` owns `length` initialized DPO identities until the
        // next replacement or this object's Drop implementation.
        unsafe { slice::from_raw_parts(pointer.as_ptr(), length) }
    }

    #[inline(always)]
    fn overflow_slice_mut(&mut self) -> &mut [DpoId] {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        let Some(pointer) = self.overflow else {
            return &mut [];
        };
        // SAFETY: `&mut self` guarantees unique access to the owned allocation.
        unsafe { slice::from_raw_parts_mut(pointer.as_ptr(), length) }
    }

    fn release_overflow(&mut self) {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        if let Some(pointer) = self.overflow.take() {
            // SAFETY: the pointer and length were created together by
            // `allocate_bucket_storage` and have not been released before.
            unsafe { release_bucket_storage(pointer, length) };
        }
    }
}

impl Drop for LoadBalanceDpo {
    fn drop(&mut self) {
        while self.locked_buckets != 0 {
            self.locked_buckets -= 1;
            let child = self
                .select_bucket(u32::from(self.locked_buckets))
                .expect("locked bucket must be present");
            super::NetMain::global()
                .expect("DPO owner must outlive its objects")
                .unlock_dpo(child);
        }
        self.release_overflow();
    }
}

impl PartialEq for LoadBalanceDpo {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_count == other.bucket_count
            && self.bucket_mask == other.bucket_mask
            && self.proto == other.proto
            && self.flags == other.flags
            && self.fib_entry_flags == other.fib_entry_flags
            && self.lock_count == other.lock_count
            && self.map_index == other.map_index
            && self.urpf_index == other.urpf_index
            && self.flow_hash_config == other.flow_hash_config
            && self.inline_buckets == other.inline_buckets
            && self.overflow_slice() == other.overflow_slice()
    }
}

impl Eq for LoadBalanceDpo {}

impl ReplicateDpo {
    pub const INLINE_BUCKETS: usize = 4;
    pub const MAX_BUCKETS: usize = 1024;

    #[inline(always)]
    pub const fn bucket_count(&self) -> u16 {
        self.bucket_count
    }

    pub(crate) fn publish_root(&mut self) {
        assert!(
            self.lock_count < u32::MAX,
            "replicate reference count overflow"
        );
        self.lock_count += 1;
    }

    pub(crate) fn withdraw_root(&mut self) -> bool {
        assert!(self.lock_count != 0, "replicate reference count underflow");
        self.lock_count -= 1;
        self.lock_count == 0
    }

    pub fn new(
        proto: DpoProto,
        buckets: &[DpoId],
        flags: ReplicateFlags,
    ) -> Result<Self, DpoError> {
        Self::validate_bucket_count(buckets.len())?;
        let (bucket_count, inline_buckets, overflow) = Self::bucket_storage(buckets);
        Ok(Self {
            bucket_count,
            proto,
            flags,
            // Detached objects acquire their first root when the owner
            // publishes the returned identity into forwarding state.
            lock_count: 0,
            inline_buckets,
            overflow,
            locked_buckets: 0,
        })
    }

    pub(crate) fn validate_bucket_count(count: usize) -> Result<(), DpoError> {
        (count <= Self::MAX_BUCKETS)
            .then_some(())
            .ok_or(DpoError::InvalidBucketCount)
    }

    pub(crate) fn validate_storage(&self) -> Result<(), DpoError> {
        Self::validate_bucket_count(usize::from(self.bucket_count))?;
        let has_overflow = self.bucket_count as usize > Self::INLINE_BUCKETS;
        if has_overflow != self.overflow.is_some() {
            return Err(DpoError::InvalidBucketStorage);
        }
        Ok(())
    }

    fn bucket_storage(
        buckets: &[DpoId],
    ) -> (u16, [DpoId; Self::INLINE_BUCKETS], Option<NonNull<DpoId>>) {
        let mut inline_buckets = [DpoId::INVALID; Self::INLINE_BUCKETS];
        for (slot, bucket) in buckets.iter().take(Self::INLINE_BUCKETS).enumerate() {
            inline_buckets[slot] = *bucket;
        }
        let overflow = (buckets.len() > Self::INLINE_BUCKETS)
            .then(|| allocate_bucket_storage(&buckets[Self::INLINE_BUCKETS..]));
        (
            u16::try_from(buckets.len()).expect("replicate bucket count fits u16"),
            inline_buckets,
            overflow.flatten(),
        )
    }

    pub(crate) fn replace_buckets(&mut self, buckets: &[DpoId]) -> Result<(), DpoError> {
        assert_eq!(
            self.locked_buckets, 0,
            "owning bucket replacement requires an owner transaction"
        );
        Self::validate_bucket_count(buckets.len())?;
        let (bucket_count, inline_buckets, overflow) = Self::bucket_storage(buckets);
        self.flags.set(
            ReplicateFlags::HAS_LOCAL,
            buckets
                .iter()
                .any(|bucket| bucket.class() == DpoType::RECEIVE),
        );
        self.release_overflow();
        self.inline_buckets = inline_buckets;
        self.overflow = overflow;
        self.bucket_count = bucket_count;
        Ok(())
    }

    pub(crate) fn bucket_mut(&mut self, index: usize) -> Option<&mut DpoId> {
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if index < Self::INLINE_BUCKETS {
            return Some(&mut self.inline_buckets[index]);
        }
        self.overflow_slice_mut()
            .get_mut(index - Self::INLINE_BUCKETS)
    }

    #[inline(always)]
    pub fn bucket(&self, index: u16) -> Option<DpoId> {
        let index = usize::from(index);
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if index < Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[index]);
        }
        self.overflow_slice()
            .get(index - Self::INLINE_BUCKETS)
            .copied()
    }

    #[inline(always)]
    fn overflow_slice(&self) -> &[DpoId] {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        let Some(pointer) = self.overflow else {
            return &[];
        };
        // SAFETY: `pointer` owns `length` initialized DPO identities until the
        // next replacement or this object's Drop implementation.
        unsafe { slice::from_raw_parts(pointer.as_ptr(), length) }
    }

    #[inline(always)]
    fn overflow_slice_mut(&mut self) -> &mut [DpoId] {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        let Some(pointer) = self.overflow else {
            return &mut [];
        };
        // SAFETY: `&mut self` guarantees unique access to the owned allocation.
        unsafe { slice::from_raw_parts_mut(pointer.as_ptr(), length) }
    }

    fn release_overflow(&mut self) {
        let length = usize::from(self.bucket_count).saturating_sub(Self::INLINE_BUCKETS);
        if let Some(pointer) = self.overflow.take() {
            // SAFETY: the pointer and length were created together by
            // `allocate_bucket_storage` and have not been released before.
            unsafe { release_bucket_storage(pointer, length) };
        }
    }
}

impl Drop for ReplicateDpo {
    fn drop(&mut self) {
        while self.locked_buckets != 0 {
            self.locked_buckets -= 1;
            let child = self
                .bucket(self.locked_buckets)
                .expect("locked bucket must be present");
            super::NetMain::global()
                .expect("DPO owner must outlive its objects")
                .unlock_dpo(child);
        }
        self.release_overflow();
    }
}

impl PartialEq for ReplicateDpo {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_count == other.bucket_count
            && self.proto == other.proto
            && self.flags == other.flags
            && self.lock_count == other.lock_count
            && self.inline_buckets == other.inline_buckets
            && self.overflow_slice() == other.overflow_slice()
    }
}

impl Eq for ReplicateDpo {}

fn allocate_bucket_storage(buckets: &[DpoId]) -> Option<NonNull<DpoId>> {
    if buckets.is_empty() {
        return None;
    }
    let layout = bucket_layout(buckets.len());
    // SAFETY: `layout` describes the exact contiguous allocation requested.
    let pointer = NonNull::new(unsafe { alloc(layout).cast::<DpoId>() })
        .unwrap_or_else(|| handle_alloc_error(layout));
    // SAFETY: the allocation is valid for `buckets.len()` DPO identities and
    // the source and destination do not overlap.
    unsafe {
        pointer
            .as_ptr()
            .copy_from_nonoverlapping(buckets.as_ptr(), buckets.len());
    }
    Some(pointer)
}

/// # Safety
/// `pointer` must have been returned by `allocate_bucket_storage` for exactly
/// `length` initialized `DpoId` values and must not have been released before.
unsafe fn release_bucket_storage(pointer: NonNull<DpoId>, length: usize) {
    let layout = bucket_layout(length);
    // SAFETY: guaranteed by this function's safety contract.
    unsafe { dealloc(pointer.cast::<u8>().as_ptr(), layout) };
}

fn bucket_layout(length: usize) -> Layout {
    let size = size_of::<DpoId>()
        .checked_mul(length)
        .expect("bucket allocation size fits");
    Layout::from_size_align(size, 64).expect("bucket allocation alignment is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_round_trips_without_object_storage() {
        let id = DpoId::with_next(DpoType::new(7), DpoProto::IP6, 42, 9);
        assert_eq!(size_of::<DpoId>(), 8);
        assert_eq!(align_of::<DpoId>(), 8);
        assert_eq!(id.class(), DpoType::new(7));
        assert_eq!(id.proto(), DpoProto::IP6);
        assert_eq!(id.index(), 42);
        assert_eq!(id.next(), 9);
        assert!(id.is_valid());
        assert!(!DpoId::INVALID.is_valid());
    }

    #[test]
    fn class_registration_and_stack_are_monotonic() {
        let mut main = DpoMain::new();
        let first = main
            .register_new_type(&[(DpoProto::IP4, &[NodeId::new(10)][..])], None)
            .expect("first class");
        let second = main
            .register_new_type(&[(DpoProto::IP6, &[NodeId::new(11)][..])], None)
            .expect("second class");
        assert_eq!(first.get(), 30);
        assert_eq!(second.get(), first.get() + 1);
        assert_eq!(
            main.nodes(first, DpoProto::IP4),
            Some(&[NodeId::new(10)][..])
        );
        assert_eq!(
            main.nodes(second, DpoProto::IP6),
            Some(&[NodeId::new(11)][..])
        );
    }

    #[test]
    fn builtin_nodes_and_overflow_buckets_are_owner_supplied() {
        let mut main = DpoMain::new();
        let node = NodeId::new(2);
        main.register_builtin(DpoType::DROP, &[(DpoProto::IP4, &[node][..])])
            .expect("builtin node");
        assert_eq!(main.node(DpoType::DROP, DpoProto::IP4), Some(node));
        let buckets: [DpoId; 8] =
            std::array::from_fn(|index| DpoId::adjacency(DpoProto::IP4, index as u32));
        let load_balance =
            LoadBalanceDpo::new(DpoProto::IP4, &buckets, LoadBalanceFlags::empty(), 0x9f).unwrap();
        assert_eq!(load_balance.select_bucket(6), Some(buckets[6]));
        assert_eq!(load_balance.overflow.unwrap().as_ptr() as usize % 64, 0);

        let single = LoadBalanceDpo::new(
            DpoProto::IP4,
            &buckets[..1],
            LoadBalanceFlags::empty(),
            0x9f,
        )
        .unwrap();
        assert_eq!(single.select_bucket(u32::MAX), Some(buckets[0]));

        let empty =
            LoadBalanceDpo::new(DpoProto::IP4, &[], LoadBalanceFlags::empty(), 0x9f).unwrap();
        assert_eq!(empty.bucket_count(), 0);
        assert_eq!(empty.select_bucket(u32::MAX), None);

        let empty_replicate =
            ReplicateDpo::new(DpoProto::IP4, &[], ReplicateFlags::empty()).unwrap();
        assert_eq!(empty_replicate.bucket_count(), 0);
        assert_eq!(empty_replicate.bucket(0), None);
    }
}
