use std::mem::{align_of, size_of};

use hammer_core::data_plane::NodeId;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoProto(u8);

impl DpoProto {
    pub const IP4: Self = Self(0);
    pub const IP6: Self = Self(1);
    pub const NONE: Self = Self(u8::MAX);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoType(u8);

impl DpoType {
    pub const INVALID: Self = Self(u8::MAX);
    pub const DROP: Self = Self(0);
    pub const PUNT: Self = Self(1);
    pub const RECEIVE: Self = Self(2);
    pub const LOAD_BALANCE: Self = Self(3);
    pub const FIRST_REGISTERED: u8 = 4;

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
    pub const INVALID: Self = Self::new(DpoType::INVALID, DpoProto::NONE, u32::MAX, u16::MAX);
    pub(crate) const fn new(dpo_type: DpoType, proto: DpoProto, index: u32, next: u16) -> Self {
        Self(
            index as u64
                | ((next as u64) << 32)
                | ((proto.get() as u64) << 48)
                | ((dpo_type.get() as u64) << 56),
        )
    }

    pub(crate) const fn drop(proto: DpoProto, next: u16) -> Self {
        Self::new(DpoType::DROP, proto, 0, next)
    }

    pub(crate) const fn punt(proto: DpoProto, next: u16) -> Self {
        Self::new(DpoType::PUNT, proto, 0, next)
    }

    pub(crate) const fn receive(proto: DpoProto, index: u32, next: u16) -> Self {
        Self::new(DpoType::RECEIVE, proto, index, next)
    }

    pub(crate) const fn load_balance(proto: DpoProto, index: u32, next: u16) -> Self {
        Self::new(DpoType::LOAD_BALANCE, proto, index, next)
    }

    pub const fn class(self) -> DpoType {
        DpoType::new((self.0 >> 56) as u8)
    }

    pub const fn proto(self) -> DpoProto {
        DpoProto::new((self.0 >> 48) as u8)
    }

    pub const fn index(self) -> u32 {
        self.0 as u32
    }

    pub const fn next(self) -> u16 {
        (self.0 >> 32) as u16
    }

    pub const fn stack(self, next: u16) -> Self {
        Self::new(self.class(), self.proto(), self.index(), next)
    }
}

const _: () = assert!(size_of::<DpoId>() == 8);
const _: () = assert!(align_of::<DpoId>() == 8);

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum DpoError {
    #[error("DPO class registration requires at least one protocol node")]
    EmptyClass,
    #[error("DPO class registration repeats protocol {proto}")]
    DuplicateProtocol { proto: u8 },
    #[error("DPO class registry exhausted")]
    ClassExhausted,
    #[error("DPO class {dpo_type} has no node for protocol {proto}")]
    NodeMissing { dpo_type: u8, proto: u8 },
    #[error("DPO stack edge is already registered with a different node")]
    StackEdgeConflict,
    #[error("load-balance bucket count must be a non-zero power of two")]
    InvalidBucketCount,
}

#[derive(Debug, Clone)]
struct DpoClassState {
    nodes: Vec<(DpoProto, NodeId)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DpoStackKey {
    child: DpoType,
    child_proto: DpoProto,
    parent: DpoType,
    parent_proto: DpoProto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DpoStackEdge {
    key: DpoStackKey,
    next: u16,
}

#[derive(Debug, Clone, Default)]
pub struct DpoMain {
    classes: Vec<DpoClassState>,
    builtins: Vec<(DpoType, DpoProto, NodeId)>,
    edges: Vec<DpoStackEdge>,
}

impl DpoMain {
    pub const fn new() -> Self {
        Self {
            classes: Vec::new(),
            builtins: Vec::new(),
            edges: Vec::new(),
        }
    }

    pub fn register_new_type(&mut self, nodes: &[(DpoProto, NodeId)]) -> Result<DpoType, DpoError> {
        if nodes.is_empty() {
            return Err(DpoError::EmptyClass);
        }
        for (position, (proto, _)) in nodes.iter().enumerate() {
            if nodes[..position].iter().any(|(other, _)| other == proto) {
                return Err(DpoError::DuplicateProtocol { proto: proto.get() });
            }
        }
        let value = DpoType::FIRST_REGISTERED
            .checked_add(u8::try_from(self.classes.len()).map_err(|_| DpoError::ClassExhausted)?)
            .ok_or(DpoError::ClassExhausted)?;
        if value == DpoType::INVALID.get() {
            return Err(DpoError::ClassExhausted);
        }
        self.classes.push(DpoClassState {
            nodes: nodes.to_vec(),
        });
        Ok(DpoType::new(value))
    }

    pub fn node(&self, dpo_type: DpoType, proto: DpoProto) -> Option<NodeId> {
        if dpo_type.get() < DpoType::FIRST_REGISTERED {
            return self.builtins.iter().find_map(|(class, candidate, node)| {
                (*class == dpo_type && *candidate == proto).then_some(*node)
            });
        }
        self.classes
            .get(usize::from(dpo_type.get() - DpoType::FIRST_REGISTERED))?
            .nodes
            .iter()
            .find_map(|(candidate, node)| (*candidate == proto).then_some(*node))
    }

    pub fn register_builtin_node(
        &mut self,
        dpo_type: DpoType,
        proto: DpoProto,
        node: NodeId,
    ) -> Result<(), DpoError> {
        if dpo_type.get() >= DpoType::FIRST_REGISTERED || dpo_type == DpoType::INVALID {
            return Err(DpoError::NodeMissing {
                dpo_type: dpo_type.get(),
                proto: proto.get(),
            });
        }
        if self.node(dpo_type, proto).is_some() {
            return Err(DpoError::DuplicateProtocol { proto: proto.get() });
        }
        self.builtins.push((dpo_type, proto, node));
        Ok(())
    }

    pub fn register_stack_edge(
        &mut self,
        child: DpoType,
        child_proto: DpoProto,
        parent: DpoId,
        next: u16,
    ) -> Result<(), DpoError> {
        if self.node(child, child_proto).is_none() {
            return Err(DpoError::NodeMissing {
                dpo_type: child.get(),
                proto: child_proto.get(),
            });
        }
        if self.node(parent.class(), parent.proto()).is_none() {
            return Err(DpoError::NodeMissing {
                dpo_type: parent.class().get(),
                proto: parent.proto().get(),
            });
        }
        let key = DpoStackKey {
            child,
            child_proto,
            parent: parent.class(),
            parent_proto: parent.proto(),
        };
        if let Some(edge) = self.edges.iter_mut().find(|edge| edge.key == key) {
            if edge.next != next {
                return Err(DpoError::StackEdgeConflict);
            }
        } else {
            self.edges.push(DpoStackEdge { key, next });
        }
        Ok(())
    }

    pub fn stack(&self, child: DpoType, child_proto: DpoProto, parent: DpoId) -> Option<DpoId> {
        let key = DpoStackKey {
            child,
            child_proto,
            parent: parent.class(),
            parent_proto: parent.proto(),
        };
        self.edges
            .iter()
            .find_map(|edge| (edge.key == key).then_some(parent.stack(edge.next)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropDpo {
    pub id: DpoId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PuntDpo {
    pub id: DpoId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveDpo<A> {
    pub sw_if_index: u32,
    pub address: A,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupDpo {
    pub fib_index: u32,
    pub table_id: u32,
    pub input_sw_if_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyDpo<A, R> {
    pub sw_if_index: u32,
    pub next_hop: Option<A>,
    pub rewrite: R,
    pub child: Option<DpoId>,
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct LoadBalanceFlags: u8 {
        const USES_MAP = 1 << 0;
        const STICKY = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadBalanceDpo {
    pub lock_count: u16,
    pub bucket_count: u16,
    pub bucket_mask: u16,
    pub flags: LoadBalanceFlags,
    pub inline_buckets: [DpoId; 4],
    pub overflow_index: u32,
}

impl LoadBalanceDpo {
    pub const INLINE_BUCKETS: usize = 4;
    pub const NO_OVERFLOW: u32 = u32::MAX;

    pub fn new(buckets: &[DpoId], flags: LoadBalanceFlags) -> Result<Self, DpoError> {
        if buckets.is_empty()
            || !buckets.len().is_power_of_two()
            || buckets.len() > u16::MAX as usize
        {
            return Err(DpoError::InvalidBucketCount);
        }
        let first = buckets
            .first()
            .copied()
            .unwrap_or(DpoId::drop(DpoProto::IP4, 0));
        let mut inline_buckets = [first; 4];
        for (slot, bucket) in buckets.iter().take(4).enumerate() {
            inline_buckets[slot] = *bucket;
        }
        Ok(Self {
            lock_count: 0,
            bucket_count: buckets.len() as u16,
            bucket_mask: (buckets.len() - 1) as u16,
            flags,
            inline_buckets,
            overflow_index: if buckets.len() > 4 {
                0
            } else {
                Self::NO_OVERFLOW
            },
        })
    }

    pub fn select_inline(&self, hash: usize) -> DpoId {
        self.inline_buckets[hash & usize::from(self.bucket_mask.min(3))]
    }

    pub fn select(&self, hash: usize, overflow: &[DpoId]) -> Option<DpoId> {
        let bucket = hash & usize::from(self.bucket_mask);
        if bucket < Self::INLINE_BUCKETS {
            return Some(self.inline_buckets[bucket]);
        }
        overflow.get(bucket).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_round_trips_without_object_storage() {
        let id = DpoId::new(DpoType::new(7), DpoProto::IP6, 42, 9);
        assert_eq!(size_of::<DpoId>(), 8);
        assert_eq!(align_of::<DpoId>(), 8);
        assert_eq!(id.class(), DpoType::new(7));
        assert_eq!(id.proto(), DpoProto::IP6);
        assert_eq!(id.index(), 42);
        assert_eq!(id.next(), 9);
        assert_eq!(id.stack(11).next(), 11);
    }

    #[test]
    fn class_registration_and_stack_are_monotonic() {
        let mut main = DpoMain::new();
        let first = main
            .register_new_type(&[(DpoProto::IP4, NodeId::new(10))])
            .expect("first class");
        let second = main
            .register_new_type(&[(DpoProto::IP6, NodeId::new(11))])
            .expect("second class");
        assert_eq!(second.get(), first.get() + 1);
        let parent = DpoId::new(first, DpoProto::IP4, 3, 4);
        main.register_stack_edge(second, DpoProto::IP6, parent, 12)
            .expect("stack edge");
        assert_eq!(
            main.stack(second, DpoProto::IP6, parent).unwrap().next(),
            12
        );
    }

    #[test]
    fn builtin_nodes_and_overflow_buckets_are_owner_supplied() {
        let mut main = DpoMain::new();
        let node = NodeId::new(2);
        main.register_builtin_node(DpoType::DROP, DpoProto::IP4, node)
            .expect("builtin node");
        assert_eq!(main.node(DpoType::DROP, DpoProto::IP4), Some(node));
        let buckets = [DpoId::drop(DpoProto::IP4, 0); 8];
        let load_balance = LoadBalanceDpo::new(&buckets, LoadBalanceFlags::empty()).unwrap();
        assert_eq!(load_balance.select(6, &buckets), Some(buckets[6]));
    }
}
