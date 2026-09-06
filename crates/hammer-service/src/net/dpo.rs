use std::mem::{align_of, size_of};
use std::ptr::NonNull;
use std::slice;

use hammer_core::data_plane::NodeId;
use hammer_infra::align::CACHE_LINE;
use hammer_infra::heap_boxed::{allocate, deallocate};
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
    pub const PUNT: Self = Self(2);
    pub const LOAD_BALANCE: Self = Self(3);
    pub const REPLICATE: Self = Self(4);
    pub const ADJACENCY: Self = Self(5);
    pub const ADJACENCY_INCOMPLETE: Self = Self(6);
    pub const ADJACENCY_MIDCHAIN: Self = Self(7);
    pub const ADJACENCY_GLEAN: Self = Self(8);
    pub const ADJACENCY_MCAST: Self = Self(9);
    pub const ADJACENCY_MCAST_MIDCHAIN: Self = Self(10);
    pub const RECEIVE: Self = Self(11);
    pub const LOOKUP: Self = Self(12);
    pub const INTERFACE_RX: Self = Self(13);
    pub const INTERFACE_TX: Self = Self(14);

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

impl std::fmt::Display for DpoId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "[@{}]: ", self.next())?;
        if !self.is_valid() {
            return formatter.write_str("unset");
        }
        // Worker diagnostics and formatting during registry mutation remain
        // identity-only. Neither case may touch control-plane borrow state.
        let operation = if hammer_runtime::ensure_main_thread().is_ok() {
            super::NET_MAIN.get().and_then(|main| {
                main.dpo_main.try_borrow().ok().and_then(|registry| {
                    registry
                        .formats
                        .get(usize::from(self.class().get()))
                        .copied()
                        .flatten()
                })
            })
        } else {
            None
        };
        match operation {
            Some(format) => format(*self, formatter),
            None => write!(
                formatter,
                "class {} object {} protocol {}",
                self.class().get(),
                self.index(),
                self.proto().get()
            ),
        }
    }
}

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
    #[error("DPO class {dpo_type} operations are already installed")]
    ClassAlreadyRegistered { dpo_type: u8 },
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
    #[error("DPO bucket count must be zero or a power of two")]
    InvalidBucketCount,
    #[error("a zero-bucket DPO class {dpo_type} cannot be published")]
    EmptyDpoPublication { dpo_type: u8 },
    #[error("DPO bucket storage does not match its bucket count")]
    InvalidBucketStorage,
    #[error("DPO type key space is exhausted at {next_type}")]
    TypeKeySpaceExhausted { next_type: u16 },
}

#[derive(Debug)]
pub struct DpoMain {
    // These tables mirror VPP's dpo_nodes[type][proto] and dpo_edges
    // vectors. Empty node lists are valid for instance-dependent classes.
    nodes: Vec<Vec<Option<Vec<NodeId>>>>,
    edges: Vec<Vec<Vec<Vec<Option<u16>>>>>,
    registered: Vec<bool>,
    pub(super) locks: Vec<Option<fn(DpoId)>>,
    pub(super) unlocks: Vec<Option<fn(DpoId)>>,
    next_nodes: Vec<Option<fn(DpoId) -> Vec<NodeId>>>,
    pub(super) mtus: Vec<Option<fn(DpoId) -> u16>>,
    pub(super) urpfs: Vec<Option<fn(DpoId) -> u32>>,
    pub(super) interposes: Vec<Option<fn(DpoId, DpoId) -> Result<DpoId, DpoError>>>,
    pub(super) formats: Vec<Option<fn(DpoId, &mut std::fmt::Formatter<'_>) -> std::fmt::Result>>,
    pub(super) memory: Vec<Option<fn() -> (usize, usize, usize)>>,
    next_type: u16,
}

impl Default for DpoMain {
    fn default() -> Self {
        Self::new()
    }
}

const DPO_PROTO_COUNT: u8 = 6;

impl DpoMain {
    pub const fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            registered: Vec::new(),
            locks: Vec::new(),
            unlocks: Vec::new(),
            next_nodes: Vec::new(),
            mtus: Vec::new(),
            urpfs: Vec::new(),
            interposes: Vec::new(),
            formats: Vec::new(),
            memory: Vec::new(),
            next_type: DpoType::INTERFACE_TX.get() as u16 + 1,
        }
    }

    fn node_slot_mut(&mut self, dpo_type: DpoType, proto: DpoProto) -> &mut Vec<NodeId> {
        let type_index = usize::from(dpo_type.get());
        let proto_index = usize::from(proto.get());
        if self.nodes.len() <= type_index {
            self.nodes.resize_with(type_index + 1, Vec::new);
        }
        if self.nodes[type_index].len() <= proto_index {
            self.nodes[type_index].resize_with(proto_index + 1, || None);
        }
        self.nodes[type_index][proto_index].get_or_insert_with(Vec::new)
    }

    fn edge_slot_mut(
        &mut self,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoType,
        parent_proto: DpoProto,
    ) -> &mut Option<u16> {
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
                .resize(parent_proto_index + 1, None);
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
            .flatten()
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

    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        dpo_type: Option<DpoType>,
        nodes: &[(DpoProto, &[NodeId])],
        locks: Option<(fn(DpoId), fn(DpoId))>,
        next_nodes: Option<fn(DpoId) -> Vec<NodeId>>,
        mtu: Option<fn(DpoId) -> u16>,
        urpf: Option<fn(DpoId) -> u32>,
        interpose: Option<fn(DpoId, DpoId) -> Result<DpoId, DpoError>>,
        format: Option<fn(DpoId, &mut std::fmt::Formatter<'_>) -> std::fmt::Result>,
        memory: Option<fn() -> (usize, usize, usize)>,
    ) -> Result<DpoType, DpoError> {
        Self::require_registration_scope()?;
        let allocate = dpo_type.is_none();
        let dpo_type = match dpo_type {
            Some(dpo_type) => dpo_type,
            None => {
                let mut next_type = self.next_type;
                while self
                    .registered
                    .get(usize::from(next_type))
                    .copied()
                    .unwrap_or(false)
                {
                    next_type += 1;
                }
                DpoType::new(
                    u8::try_from(next_type)
                        .map_err(|_| DpoError::TypeKeySpaceExhausted { next_type })?,
                )
            }
        };
        assert_ne!(
            dpo_type,
            DpoType::INVALID,
            "a DPO owner cannot register the invalid identity class"
        );
        let index = usize::from(dpo_type.get());
        if self.registered.get(index).copied().unwrap_or(false)
            && (locks.is_some()
                || next_nodes.is_some()
                || mtu.is_some()
                || urpf.is_some()
                || interpose.is_some()
                || format.is_some()
                || memory.is_some())
        {
            return Err(DpoError::ClassAlreadyRegistered {
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
        let length = self.registered.len().max(index + 1);
        self.registered.resize(length, false);
        self.locks.resize(length, None);
        self.unlocks.resize(length, None);
        self.next_nodes.resize(length, None);
        self.mtus.resize(length, None);
        self.urpfs.resize(length, None);
        self.interposes.resize(length, None);
        self.formats.resize(length, None);
        self.memory.resize(length, None);
        self.registered[index] = true;
        // Later protocol initializers may append node bindings, never replace
        // the operations used by already-published identities of this class.
        self.next_nodes[index] = next_nodes.or(self.next_nodes[index]);
        self.mtus[index] = mtu.or(self.mtus[index]);
        self.urpfs[index] = urpf.or(self.urpfs[index]);
        self.interposes[index] = interpose.or(self.interposes[index]);
        self.formats[index] = format.or(self.formats[index]);
        self.memory[index] = memory.or(self.memory[index]);
        if let Some((lock, unlock)) = locks {
            self.locks[index] = Some(lock);
            self.unlocks[index] = Some(unlock);
        }
        if allocate {
            self.next_type = u16::from(dpo_type.get()) + 1;
        }
        Ok(dpo_type)
    }

    pub fn nodes(&self, dpo_type: DpoType, proto: DpoProto) -> Option<&[NodeId]> {
        self.nodes
            .get(usize::from(dpo_type.get()))?
            .get(usize::from(proto.get()))
            .and_then(Option::as_deref)
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
        Self::validate_protocol(proto)?;
        let class = usize::from(dpo_type.get());
        if !self.registered.get(class).copied().unwrap_or(false)
            || (self.nodes(dpo_type, proto).is_none()
                && self.next_nodes.get(class).is_none_or(Option::is_none))
        {
            return Err(DpoError::NodeMissing {
                dpo_type: dpo_type.get(),
                proto: proto.get(),
            });
        }
        Ok(DpoId::new(dpo_type, proto, index))
    }

    pub fn node(&self, dpo_type: DpoType, proto: DpoProto) -> Option<NodeId> {
        self.nodes(dpo_type, proto)
            .and_then(|node_ids| node_ids.first().copied())
    }

    fn nodes_for_dpo(&self, dpo: DpoId) -> Result<Vec<NodeId>, DpoError> {
        self.identity(dpo.class(), dpo.proto(), dpo.index())?;
        let nodes = match self.next_nodes[usize::from(dpo.class().get())] {
            Some(resolve) => resolve(dpo),
            None => self
                .nodes(dpo.class(), dpo.proto())
                .unwrap_or_default()
                .to_vec(),
        };
        if nodes.is_empty() {
            return Err(DpoError::NodeMissing {
                dpo_type: dpo.class().get(),
                proto: dpo.proto().get(),
            });
        }
        Ok(nodes)
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

    /// Stacks a concrete child node using the parent's registered node resolver.
    pub fn stack_from_node(
        &mut self,
        runtime: &mut DataPlaneMain,
        child_node: NodeId,
        parent: DpoId,
    ) -> Result<DpoId, DpoError> {
        hammer_runtime::ensure_main_thread()?;
        let parent_nodes = self.nodes_for_dpo(parent)?;
        let mut needs_barrier = false;
        for &parent_node in &parent_nodes {
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
                self.stack_from_node_inner(runtime, child_node, parent, &parent_nodes)
            });
        }
        self.stack_from_node_inner(runtime, child_node, parent, &parent_nodes)
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
            .add_node_next_slots(&edges, &[])
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
        let mut parents = [parent];
        self.stack_buckets(runtime, child, child_proto, &mut parents)?;
        Ok(parents[0])
    }

    /// Publishes all missing bucket edges in one graph transaction. Neither
    /// identities nor the edge cache change if graph validation fails.
    pub(crate) fn stack_buckets(
        &mut self,
        runtime: &mut DataPlaneMain,
        child: DpoType,
        child_proto: DpoProto,
        parents: &mut [DpoId],
    ) -> Result<(), DpoError> {
        hammer_runtime::ensure_main_thread()?;
        let child_nodes = self
            .nodes(child, child_proto)
            .ok_or(DpoError::NodeMissing {
                dpo_type: child.get(),
                proto: child_proto.get(),
            })?
            .to_vec();
        if child_nodes.is_empty() {
            return Err(DpoError::NodeMissing {
                dpo_type: child.get(),
                proto: child_proto.get(),
            });
        }
        let mut edges = Vec::new();
        let mut bindings = Vec::new();
        for &parent in parents.iter() {
            self.identity(parent.class(), parent.proto(), parent.index())?;
            if self
                .edge_slot(child, child_proto, parent.class(), parent.proto())
                .is_some()
                || bindings
                    .iter()
                    .any(|(class, proto, _)| *class == parent.class() && *proto == parent.proto())
            {
                continue;
            }
            let parent_nodes = self.nodes_for_dpo(parent)?;
            let start = edges.len();
            for &child_node in &child_nodes {
                edges.extend(
                    parent_nodes
                        .iter()
                        .map(|&parent_node| (child_node, parent_node)),
                );
            }
            bindings.push((parent.class(), parent.proto(), start..edges.len()));
        }
        let publish = |runtime: &DataPlaneMain| -> Result<(), DpoError> {
            if !edges.is_empty() {
                let shared_slots: Vec<_> =
                    bindings.iter().map(|(_, _, range)| range.clone()).collect();
                let slots = runtime
                    .nodes()
                    .add_node_next_slots(&edges, &shared_slots)
                    .map_err(|source| {
                        let (child, parent) = edges[0];
                        DpoError::GraphEdgeAdd {
                            child,
                            parent,
                            source,
                        }
                    })?;
                for (class, proto, range) in &bindings {
                    let next = slots[range.start];
                    *self.edge_slot_mut(child, child_proto, *class, *proto) = Some(next);
                }
            }
            for parent in parents {
                let next = self
                    .edge_slot(child, child_proto, parent.class(), parent.proto())
                    .expect("validated bucket edge was committed");
                *parent = parent.stack(next);
            }
            Ok(())
        };
        let workers_running =
            hammer_runtime::barrier::global().is_some_and(|barrier| barrier.worker_count() != 0);
        if !edges.is_empty()
            && workers_running
            && !hammer_runtime::barrier::global().is_some_and(|barrier| barrier.is_pending())
        {
            return hammer_runtime::worker_thread_barrier_sync!(runtime, { publish(runtime) });
        }
        publish(runtime)
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

#[derive(Debug, PartialEq, Eq)]
pub struct InterfaceRxDpo {
    pub(crate) sw_if_index: u32,
    pub(crate) proto: DpoProto,
    pub(crate) lock_count: u32,
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

/// A resolved FIB path supplied to the load-balance owner. The identity is
/// borrowed logically: publishing buckets acquires their own references.
#[derive(Debug, Clone, Copy)]
pub struct LoadBalancePath {
    pub dpo: DpoId,
    pub path_index: u32,
    pub weight: u32,
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
    pub(super) urpf_index: u32,
    pub flow_hash_config: u16,
    pub(super) locked_buckets: u16,
    overflow: Option<NonNull<DpoId>>,
    pub inline_buckets: [DpoId; 4],
}

const _: () = assert!(size_of::<LoadBalanceDpo>() == 64);
const _: () = assert!(align_of::<LoadBalanceDpo>() == 64);

impl LoadBalanceDpo {
    pub fn format(dpo: DpoId, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let main = super::NetMain::global().expect("load-balance owner must be initialized");
        let Some(object) = main.load_balance(dpo.index()) else {
            return write!(formatter, "load-balance {} absent", dpo.index());
        };
        write!(
            formatter,
            "load-balance {} buckets {} locks {} hash {:#x}",
            dpo.index(),
            object.bucket_count(),
            object.lock_count,
            object.flow_hash_config()
        )?;
        for index in 0..object.bucket_count() {
            write!(
                formatter,
                "\n  bucket {index}: {:?}",
                object.select_bucket(u32::from(index))
            )?;
        }
        Ok(())
    }

    pub fn memory() -> (usize, usize, usize) {
        let main = super::NetMain::global().expect("load-balance owner must be initialized");
        let pool = main.load_balances();
        (size_of::<Self>(), pool.len(), pool.capacity())
    }

    pub fn lock(dpo: DpoId) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("load-balance reference acquisition requires the publication scope");
        let main = super::NetMain::global().expect("load-balance owner must be initialized");
        let mut pool = main.load_balances_mut();
        let object = pool
            .get_mut(dpo.index())
            .expect("referenced load-balance must exist");
        assert_eq!(object.proto, dpo.proto());
        object.lock_count = object
            .lock_count
            .checked_add(1)
            .expect("load-balance reference count overflow");
    }

    pub fn unlock(dpo: DpoId) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("load-balance retirement requires the publication scope");
        let main = super::NetMain::global().expect("load-balance owner must be initialized");
        let mut pool = main.load_balances_mut();
        let object = pool
            .get_mut(dpo.index())
            .expect("locked load-balance must exist");
        assert_eq!(object.proto, dpo.proto());
        object.lock_count = object
            .lock_count
            .checked_sub(1)
            .expect("load-balance reference count underflow");
        if object.lock_count == 0 {
            let object = pool.remove(dpo.index());
            drop(pool);
            drop(object);
        }
    }

    pub fn mtu(dpo: DpoId) -> u16 {
        let main = super::NetMain::global().expect("load-balance owner must be initialized");
        let object = main
            .load_balance(dpo.index())
            .expect("referenced load-balance must exist");
        (0..object.bucket_count()).fold(u16::MAX, |mtu, index| {
            let child = object
                .select_bucket(u32::from(index))
                .expect("bucket count bounds the index");
            mtu.min(main.dpo_mtu(child))
        })
    }

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

    pub fn new(
        proto: DpoProto,
        paths: &[LoadBalancePath],
        flags: LoadBalanceFlags,
        flow_hash_config: u16,
    ) -> Result<Self, DpoError> {
        let mut paths = paths.to_vec();
        if paths.is_empty() {
            paths.push(LoadBalancePath {
                dpo: DpoId::drop(proto),
                path_index: u32::MAX,
                weight: 1,
            });
        }
        match paths.len() {
            1 => paths[0].weight = 1,
            2 => {
                // VPP's two-path fast sort is descending; the general sort
                // is ascending. Bucket order matters to existing flows.
                if paths[0].weight < paths[1].weight {
                    paths.swap(0, 1);
                }
                if paths[0].weight == paths[1].weight {
                    paths[0].weight = 1;
                    paths[1].weight = 1;
                }
            }
            _ => paths.sort_by_key(|path| path.weight),
        }
        let mut total_weight: u64 = paths.iter().map(|path| u64::from(path.weight)).sum();
        if total_weight == 0 {
            paths.iter_mut().for_each(|path| path.weight = 1);
            total_weight = paths.len() as u64;
        }
        let mut bucket_count = paths.len().min(Self::MAX_BUCKETS).next_power_of_two();
        let mut counts = vec![0; paths.len()];
        loop {
            let scale = bucket_count as f64 / total_weight as f64;
            let mut remaining = bucket_count;
            let mut error = 0.0;
            let mut retained = 0;
            counts.fill(0);
            for (path, count) in paths.iter().zip(&mut counts) {
                let exact = f64::from(path.weight) * scale;
                *count = (exact.round() as usize).min(remaining);
                remaining -= *count;
                error += (exact - *count as f64).abs();
                if *count == 0 {
                    error = bucket_count as f64;
                    break;
                }
                retained += 1;
            }
            // The vendored implementation uses 0.1, despite its 1% comment.
            if error <= 0.1 * bucket_count as f64 || bucket_count == Self::MAX_BUCKETS {
                // VPP truncates at the first zero allocation. Never turn an
                // empty prefix into forwarding through that rejected path.
                if retained == 0 {
                    return Err(DpoError::InvalidBucketCount);
                }
                paths.truncate(retained);
                counts.truncate(retained);
                counts[0] += remaining;
                break;
            }
            bucket_count *= 2;
        }
        let live_paths: Vec<_> = paths
            .iter()
            .zip(&counts)
            .filter(|(path, count)| **count != 0 && path.dpo.class() != DpoType::DROP)
            .map(|(path, _)| path.dpo)
            .collect();
        let mut live = live_paths.iter().cycle();
        let mut buckets = Vec::with_capacity(bucket_count);
        for (path, count) in paths.iter().zip(counts) {
            for _ in 0..count {
                let child = if flags.contains(LoadBalanceFlags::STICKY)
                    && path.dpo.class() == DpoType::DROP
                {
                    live.next().copied().unwrap_or(path.dpo)
                } else {
                    path.dpo
                };
                buckets.push(child);
            }
        }
        Self::validate_bucket_count(buckets.len())?;
        let (bucket_count, bucket_mask, inline_buckets, overflow) = Self::bucket_storage(&buckets);
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
        let overflow = if buckets.len() > Self::INLINE_BUCKETS {
            let pointer = allocate::<DpoId, CACHE_LINE>(buckets.len());
            // SAFETY: infra allocated exactly this many contiguous slots;
            // the borrowed source cannot overlap the new allocation.
            unsafe {
                pointer
                    .as_ptr()
                    .copy_from_nonoverlapping(buckets.as_ptr(), buckets.len());
            }
            Some(pointer)
        } else {
            inline_buckets[..buckets.len()].copy_from_slice(buckets);
            None
        };
        (
            buckets.len() as u16,
            buckets.len().saturating_sub(1) as u16,
            inline_buckets,
            overflow,
        )
    }

    #[inline(always)]
    pub fn select_bucket(&self, hash: u32) -> Option<DpoId> {
        if self.bucket_count == 0 {
            return None;
        }
        let bucket = (hash & u32::from(self.bucket_mask)) as usize;
        if usize::from(self.bucket_count) <= Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[bucket]);
        }
        self.overflow_slice().get(bucket).copied()
    }

    #[inline(always)]
    pub(crate) fn bucket_mut(&mut self, index: usize) -> Option<&mut DpoId> {
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if usize::from(self.bucket_count) <= Self::INLINE_BUCKETS {
            return Some(&mut self.inline_buckets[index]);
        }
        self.overflow_slice_mut().get_mut(index)
    }

    #[inline(always)]
    fn overflow_slice(&self) -> &[DpoId] {
        let length = usize::from(self.bucket_count);
        let Some(pointer) = self.overflow else {
            return &[];
        };
        // SAFETY: `pointer` owns `length` initialized DPO identities until the
        // next replacement or this object's Drop implementation.
        unsafe { slice::from_raw_parts(pointer.as_ptr(), length) }
    }

    #[inline(always)]
    fn overflow_slice_mut(&mut self) -> &mut [DpoId] {
        let length = usize::from(self.bucket_count);
        let Some(pointer) = self.overflow else {
            return &mut [];
        };
        // SAFETY: `&mut self` guarantees unique access to the owned allocation.
        unsafe { slice::from_raw_parts_mut(pointer.as_ptr(), length) }
    }

    fn release_overflow(&mut self) {
        let length = usize::from(self.bucket_count);
        if let Some(pointer) = self.overflow.take() {
            // SAFETY: this owner allocated these slots from the Main Heap
            // with the same alignment and length. DpoId is Copy; child
            // references are released separately before freeing storage.
            unsafe { deallocate::<DpoId, CACHE_LINE>(pointer, length) };
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
        if self.urpf_index != u32::MAX {
            super::NetMain::global()
                .expect("FIB owner must outlive its load-balance references")
                .unlock_urpf_list(self.urpf_index);
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
    pub fn format(dpo: DpoId, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let main = super::NetMain::global().expect("replicate owner must be initialized");
        let Some(object) = main.replicate(dpo.index()) else {
            return write!(formatter, "replicate {} absent", dpo.index());
        };
        write!(
            formatter,
            "replicate {} buckets {} locks {}",
            dpo.index(),
            object.bucket_count(),
            object.lock_count
        )?;
        for index in 0..object.bucket_count() {
            write!(formatter, "\n  bucket {index}: {:?}", object.bucket(index))?;
        }
        Ok(())
    }

    pub fn memory() -> (usize, usize, usize) {
        let main = super::NetMain::global().expect("replicate owner must be initialized");
        let pool = main.replicates();
        (size_of::<Self>(), pool.len(), pool.capacity())
    }

    pub fn lock(dpo: DpoId) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("replicate reference acquisition requires the publication scope");
        let main = super::NetMain::global().expect("replicate owner must be initialized");
        let mut pool = main.replicates_mut();
        let object = pool
            .get_mut(dpo.index())
            .expect("referenced replicate must exist");
        assert_eq!(object.proto, dpo.proto());
        object.lock_count = object
            .lock_count
            .checked_add(1)
            .expect("replicate reference count overflow");
    }

    pub fn unlock(dpo: DpoId) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("replicate retirement requires the publication scope");
        let main = super::NetMain::global().expect("replicate owner must be initialized");
        let mut pool = main.replicates_mut();
        let object = pool
            .get_mut(dpo.index())
            .expect("locked replicate must exist");
        assert_eq!(object.proto, dpo.proto());
        object.lock_count = object
            .lock_count
            .checked_sub(1)
            .expect("replicate reference count underflow");
        if object.lock_count == 0 {
            let object = pool.remove(dpo.index());
            drop(pool);
            drop(object);
        }
    }

    pub const INLINE_BUCKETS: usize = 4;
    pub const MAX_BUCKETS: usize = 1024;

    #[inline(always)]
    pub const fn bucket_count(&self) -> u16 {
        self.bucket_count
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
        let overflow = if buckets.len() > Self::INLINE_BUCKETS {
            let pointer = allocate::<DpoId, CACHE_LINE>(buckets.len());
            // SAFETY: infra allocated exactly this many contiguous slots;
            // the borrowed source cannot overlap the new allocation.
            unsafe {
                pointer
                    .as_ptr()
                    .copy_from_nonoverlapping(buckets.as_ptr(), buckets.len());
            }
            Some(pointer)
        } else {
            inline_buckets[..buckets.len()].copy_from_slice(buckets);
            None
        };
        (
            u16::try_from(buckets.len()).expect("replicate bucket count fits u16"),
            inline_buckets,
            overflow,
        )
    }

    pub(crate) fn bucket_mut(&mut self, index: usize) -> Option<&mut DpoId> {
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if usize::from(self.bucket_count) <= Self::INLINE_BUCKETS {
            return Some(&mut self.inline_buckets[index]);
        }
        self.overflow_slice_mut().get_mut(index)
    }

    #[inline(always)]
    pub fn bucket(&self, index: u16) -> Option<DpoId> {
        let index = usize::from(index);
        if index >= usize::from(self.bucket_count) {
            return None;
        }
        if usize::from(self.bucket_count) <= Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[index]);
        }
        self.overflow_slice().get(index).copied()
    }

    #[inline(always)]
    fn overflow_slice(&self) -> &[DpoId] {
        let length = usize::from(self.bucket_count);
        let Some(pointer) = self.overflow else {
            return &[];
        };
        // SAFETY: `pointer` owns `length` initialized DPO identities until the
        // next replacement or this object's Drop implementation.
        unsafe { slice::from_raw_parts(pointer.as_ptr(), length) }
    }

    #[inline(always)]
    fn overflow_slice_mut(&mut self) -> &mut [DpoId] {
        let length = usize::from(self.bucket_count);
        let Some(pointer) = self.overflow else {
            return &mut [];
        };
        // SAFETY: `&mut self` guarantees unique access to the owned allocation.
        unsafe { slice::from_raw_parts_mut(pointer.as_ptr(), length) }
    }

    fn release_overflow(&mut self) {
        let length = usize::from(self.bucket_count);
        if let Some(pointer) = self.overflow.take() {
            // SAFETY: this owner allocated these slots from the Main Heap
            // with the same alignment and length. DpoId is Copy; child
            // references are released separately before freeing storage.
            unsafe { deallocate::<DpoId, CACHE_LINE>(pointer, length) };
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
