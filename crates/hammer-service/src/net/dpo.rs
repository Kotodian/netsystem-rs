use std::mem::{align_of, size_of};

use hammer_core::data_plane::NodeId;
use hammer_runtime::{DataPlaneMain, RuntimeError};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoId(u64);

impl DpoId {
    pub const INVALID: Self = Self::with_next(DpoType::INVALID, DpoProto::NONE, u32::MAX, 0);

    pub const fn new(dpo_type: DpoType, proto: DpoProto, index: u32) -> Self {
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
    #[error("load-balance bucket count must be a non-zero power of two")]
    InvalidBucketCount,
}

#[derive(Debug, Clone)]
pub struct DpoMain {
    // These tables mirror VPP's dpo_nodes[type][proto] and dpo_edges
    // vectors. Empty node lists are valid for instance-dependent classes.
    nodes: Vec<Vec<Vec<NodeId>>>,
    edges: Vec<Vec<Vec<Vec<u16>>>>,
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
const NO_EDGE: u16 = u16::MAX;

impl DpoMain {
    pub const fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
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
        (proto != DpoProto::NONE)
            .then_some(())
            .ok_or(DpoError::InvalidProtocol { proto: proto.get() })
    }

    /// Class/node metadata is worker-visible after the graph starts. Startup
    /// registration is allowed before workers exist; live registration must
    /// already be inside the process barrier, just like VPP's dpo registry
    /// mutation is serialized with worker graph access.
    fn require_registration_scope() -> Result<(), DpoError> {
        if hammer_runtime::barrier::global()
            .is_some_and(|barrier| barrier.worker_count() != 0 && !barrier.is_pending())
        {
            return Err(DpoError::Runtime(
                RuntimeError::ControlRequiresWorkerBarrier,
            ));
        }
        Ok(())
    }

    pub fn register_new_type(
        &mut self,
        nodes: &[(DpoProto, &[NodeId])],
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
            .expect("DPO dynamic class space exhausted");
        for (proto, node_ids) in nodes {
            *self.node_slot_mut(dpo_type, *proto) = node_ids.to_vec();
        }
        Ok(dpo_type)
    }

    pub fn nodes(&self, dpo_type: DpoType, proto: DpoProto) -> Option<&[NodeId]> {
        self.nodes
            .get(usize::from(dpo_type.get()))?
            .get(usize::from(proto.get()))
            .map(Vec::as_slice)
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
        assert!(
            dpo_type.get() < DYNAMIC_TYPE_START && dpo_type != DpoType::INVALID,
            "builtin DPO class key must be in the reserved class range"
        );
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

    /// Equivalent to VPP's `dpo_stack`: add every child-originating node to
    /// every parent node, cache the common edge slot, and return the parent
    /// identity with that slot installed.
    pub fn stack_from_node(
        &mut self,
        runtime: &mut DataPlaneMain,
        child_node: NodeId,
        parent: DpoId,
        parent_nodes: &[NodeId],
    ) -> Result<DpoId, DpoError> {
        if parent_nodes.is_empty() {
            return Err(DpoError::NodeMissing {
                dpo_type: parent.class().get(),
                proto: parent.proto().get(),
            });
        }
        let needs_barrier = parent_nodes.iter().any(|&parent_node| {
            runtime
                .nodes()
                .node_next_slot_for_target(child_node, parent_node)
                .expect("validated graph node lookup cannot fail")
                .is_none()
        });
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupDpo {
    pub fib_index: u32,
    pub proto: DpoProto,
    pub input: LookupInput,
    pub table: LookupTable,
    pub cast: LookupCast,
    pub lock_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyDpo<A, R> {
    pub config_index: u32,
    pub lock_count: u32,
    pub sw_if_index: u32,
    pub next_hop: Option<A>,
    pub rewrite: R,
    pub child: Option<DpoId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterfaceRxDpo {
    pub sw_if_index: u32,
    pub proto: DpoProto,
    pub lock_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateDpo {
    pub bucket_count: u16,
    pub proto: DpoProto,
    pub flags: ReplicateFlags,
    pub lock_count: u32,
    pub inline_buckets: [DpoId; 4],
    overflow_buckets: Box<[DpoId]>,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct LoadBalanceFlags: u8 {
        const USES_MAP = 1 << 0;
        const STICKY = 1 << 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadBalanceDpo {
    pub bucket_count: u16,
    pub bucket_mask: u16,
    pub proto: DpoProto,
    pub flags: LoadBalanceFlags,
    pub lock_count: u32,
    pub map_index: u32,
    pub urpf_index: u32,
    pub flow_hash_config: u16,
    pub inline_buckets: [DpoId; 4],
    pub(crate) overflow_buckets: Box<[DpoId]>,
}

impl LoadBalanceDpo {
    pub const INLINE_BUCKETS: usize = 4;
    pub const MAX_BUCKETS: usize = 8192;

    pub fn new(
        proto: DpoProto,
        buckets: &[DpoId],
        flags: LoadBalanceFlags,
        flow_hash_config: u16,
    ) -> Result<Self, DpoError> {
        if buckets.is_empty()
            || !buckets.len().is_power_of_two()
            || buckets.len() > Self::MAX_BUCKETS
        {
            return Err(DpoError::InvalidBucketCount);
        }
        let first = buckets
            .first()
            .copied()
            .unwrap_or(DpoId::drop(DpoProto::IP4));
        let mut inline_buckets = [first; 4];
        for (slot, bucket) in buckets.iter().take(4).enumerate() {
            inline_buckets[slot] = *bucket;
        }
        let overflow_buckets: Box<[DpoId]> = if buckets.len() > Self::INLINE_BUCKETS {
            buckets[Self::INLINE_BUCKETS..].to_vec().into_boxed_slice()
        } else {
            Box::new([])
        };
        Ok(Self {
            bucket_count: buckets.len() as u16,
            bucket_mask: (buckets.len() - 1) as u16,
            proto,
            flags,
            lock_count: 0,
            map_index: u32::MAX,
            urpf_index: u32::MAX,
            flow_hash_config,
            inline_buckets,
            overflow_buckets,
        })
    }

    #[inline(always)]
    pub fn select_bucket(&self, hash: u32) -> Option<DpoId> {
        let bucket = (hash & u32::from(self.bucket_mask)) as usize;
        if bucket < Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[bucket]);
        }
        self.overflow_buckets
            .get(bucket - Self::INLINE_BUCKETS)
            .copied()
    }
}

impl ReplicateDpo {
    pub const INLINE_BUCKETS: usize = 4;
    pub const MAX_BUCKETS: usize = 1024;

    pub fn new(
        proto: DpoProto,
        buckets: &[DpoId],
        flags: ReplicateFlags,
    ) -> Result<Self, DpoError> {
        if buckets.is_empty() || buckets.len() > Self::MAX_BUCKETS {
            return Err(DpoError::InvalidBucketCount);
        }
        let first = buckets[0];
        let mut inline_buckets = [first; Self::INLINE_BUCKETS];
        for (slot, bucket) in buckets.iter().take(Self::INLINE_BUCKETS).enumerate() {
            inline_buckets[slot] = *bucket;
        }
        let overflow_buckets = if buckets.len() > Self::INLINE_BUCKETS {
            buckets[Self::INLINE_BUCKETS..].to_vec().into_boxed_slice()
        } else {
            Box::new([])
        };
        Ok(Self {
            bucket_count: u16::try_from(buckets.len()).expect("replicate bucket count fits u16"),
            proto,
            flags,
            lock_count: 0,
            inline_buckets,
            overflow_buckets,
        })
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
        self.overflow_buckets
            .get(index - Self::INLINE_BUCKETS)
            .copied()
    }
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
            .register_new_type(&[(DpoProto::IP4, &[NodeId::new(10)][..])])
            .expect("first class");
        let second = main
            .register_new_type(&[(DpoProto::IP6, &[NodeId::new(11)][..])])
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

        let single = LoadBalanceDpo::new(
            DpoProto::IP4,
            &buckets[..1],
            LoadBalanceFlags::empty(),
            0x9f,
        )
        .unwrap();
        assert_eq!(single.select_bucket(u32::MAX), Some(buckets[0]));
    }
}
