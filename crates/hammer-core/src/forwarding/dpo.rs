use crate::protocol::ip::IpVersion;

use super::load_balance::LoadBalanceIndex;
use hammer_infra::vec::Vec;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoProto(u8);

impl DpoProto {
    #[inline(always)]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u8 {
        self.0
    }

    pub const IP4: Self = Self(0);
    pub const IP6: Self = Self(1);
    pub const MPLS: Self = Self(2);
    pub const ETHERNET: Self = Self(3);
    pub const BIER: Self = Self(4);
    pub const NSH: Self = Self(5);

    #[inline(always)]
    pub const fn from_ip_version(version: IpVersion) -> Self {
        match version {
            IpVersion::V4 => Self::IP4,
            IpVersion::V6 => Self::IP6,
        }
    }

    #[inline(always)]
    pub const fn ip_version(self) -> Option<IpVersion> {
        match self {
            Self::IP4 => Some(IpVersion::V4),
            Self::IP6 => Some(IpVersion::V6),
            Self::MPLS | Self::ETHERNET | Self::BIER | Self::NSH => None,
            _ => None,
        }
    }
}

impl From<IpVersion> for DpoProto {
    #[inline(always)]
    fn from(value: IpVersion) -> Self {
        Self::from_ip_version(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AdjacencyIndex(u32);

impl AdjacencyIndex {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u32 {
        self.0
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone)]
pub struct DpoTypeRegistry {
    next: u16,
}

impl Default for DpoTypeRegistry {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl DpoTypeRegistry {
    #[inline]
    pub const fn new() -> Self {
        Self {
            next: DpoType::FIRST_REGISTERED,
        }
    }

    #[inline]
    pub fn register(&mut self) -> Option<DpoType> {
        if self.next < DpoType::FIRST_REGISTERED {
            return None;
        }
        let dpo_type = DpoType::new(self.next);
        self.next = self.next.checked_add(1).unwrap_or_default();
        Some(dpo_type)
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DpoType(u16);

pub type DpoClass = DpoType;

impl DpoType {
    #[inline(always)]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn get(self) -> u16 {
        self.0
    }

    #[inline(always)]
    pub const fn is_builtin(self) -> bool {
        self.0 < Self::FIRST_REGISTERED
    }

    #[inline(always)]
    pub const fn is_terminal(self) -> bool {
        self.0 == Self::DROP.0 || self.0 == Self::PUNT.0 || self.0 == Self::RECEIVE.0
    }

    pub const DROP: Self = Self(0);
    pub const PUNT: Self = Self(1);
    pub const ADJACENCY: Self = Self(2);
    pub const RECEIVE: Self = Self(3);
    pub const LOAD_BALANCE: Self = Self(4);
    pub const FIRST_REGISTERED: u16 = 5;
}

pub trait DpoKind: Copy {
    type Index: Copy;

    fn dpo_type(self) -> DpoType;
    fn encode_index(index: Self::Index) -> u32;
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dpo<N> {
    next: N,
    index: u32,
    dpo_type: DpoType,
    proto: DpoProto,
}

pub type DpoId<N> = Dpo<N>;

impl<N> Dpo<N> {
    #[inline(always)]
    pub const fn drop(proto: DpoProto, next: N) -> Self {
        Self {
            next,
            index: 0,
            dpo_type: DpoType::DROP,
            proto,
        }
    }

    #[inline(always)]
    pub const fn punt(proto: DpoProto, next: N) -> Self {
        Self {
            next,
            index: 0,
            dpo_type: DpoType::PUNT,
            proto,
        }
    }

    #[inline(always)]
    pub const fn receive(proto: DpoProto, next: N) -> Self {
        Self {
            next,
            index: 0,
            dpo_type: DpoType::RECEIVE,
            proto,
        }
    }

    #[inline(always)]
    pub const fn adjacency(proto: DpoProto, adjacency: AdjacencyIndex, next: N) -> Self {
        Self {
            next,
            index: adjacency.get(),
            dpo_type: DpoType::ADJACENCY,
            proto,
        }
    }

    #[inline(always)]
    pub const fn load_balance(proto: DpoProto, load_balance: LoadBalanceIndex, next: N) -> Self {
        Self {
            next,
            index: load_balance.get(),
            dpo_type: DpoType::LOAD_BALANCE,
            proto,
        }
    }

    #[inline(always)]
    pub const fn new(proto: DpoProto, dpo_type: DpoType, index: u32, next: N) -> Self {
        Self {
            next,
            index,
            dpo_type,
            proto,
        }
    }

    #[inline(always)]
    pub fn typed<K: DpoKind>(proto: DpoProto, kind: K, index: K::Index, next: N) -> Self {
        Self::new(proto, kind.dpo_type(), K::encode_index(index), next)
    }

    /// Stack a parent DPO onto a precomputed graph next.
    ///
    /// This mirrors VPP's `dpo_stack`: the DPO keeps the parent
    /// type/protocol/index identity while `next` becomes the child-to-parent
    /// graph edge selected by the control plane.
    #[inline(always)]
    pub fn stack(parent: Self, next: N) -> Self {
        Self {
            next,
            index: parent.index,
            dpo_type: parent.dpo_type,
            proto: parent.proto,
        }
    }

    #[inline(always)]
    pub const fn proto(&self) -> DpoProto {
        self.proto
    }

    #[inline(always)]
    pub fn kind(&self) -> DpoType {
        self.dpo_type
    }

    #[inline(always)]
    pub fn class(&self) -> DpoClass {
        self.dpo_type
    }

    #[inline(always)]
    pub fn adjacency_index(&self) -> Option<AdjacencyIndex> {
        if self.dpo_type == DpoType::ADJACENCY {
            Some(AdjacencyIndex::new(self.index))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn load_balance_index(&self) -> Option<LoadBalanceIndex> {
        if self.dpo_type == DpoType::LOAD_BALANCE {
            Some(LoadBalanceIndex::new(self.index))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn forwarding_index(&self) -> u32 {
        if self.dpo_type.is_terminal() {
            0
        } else {
            self.index
        }
    }
}

impl<N: Copy> Dpo<N> {
    #[inline(always)]
    pub fn next(&self) -> N {
        self.next
    }
}

#[derive(Debug, Clone)]
pub struct DpoStackRegistry<N> {
    edges: Vec<DpoStackEdge<N>>,
}

impl<N> Default for DpoStackRegistry<N> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<N> DpoStackRegistry<N> {
    #[inline]
    pub const fn new() -> Self {
        Self { edges: Vec::new() }
    }

    #[inline]
    pub fn register(
        &mut self,
        child: DpoClass,
        child_proto: DpoProto,
        parent: DpoClass,
        parent_proto: DpoProto,
        next: N,
    ) {
        let key = DpoStackKey {
            child,
            child_proto,
            parent,
            parent_proto,
        };
        if let Some(edge) = self.edges.iter_mut().find(|edge| edge.key == key) {
            edge.next = next;
            return;
        }
        self.edges.push(DpoStackEdge { key, next });
    }
}

impl<N: Copy> DpoStackRegistry<N> {
    #[inline]
    pub fn stack(&self, child: DpoClass, child_proto: DpoProto, parent: Dpo<N>) -> Option<Dpo<N>> {
        let key = DpoStackKey {
            child,
            child_proto,
            parent: parent.class(),
            parent_proto: parent.proto(),
        };
        let next = self
            .edges
            .iter()
            .find(|edge| edge.key == key)
            .map(|edge| edge.next)?;
        Some(Dpo::stack(parent, next))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DpoStackKey {
    child: DpoClass,
    child_proto: DpoProto,
    parent: DpoClass,
    parent_proto: DpoProto,
}

#[derive(Debug, Clone)]
struct DpoStackEdge<N> {
    key: DpoStackKey,
    next: N,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjacencyRewriteError {
    TooLarge { len: usize, max: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdjacencyRewrite {
    len: u8,
    bytes: [u8; Self::MAX_LEN],
}

impl AdjacencyRewrite {
    pub const MAX_LEN: usize = 64;

    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            len: 0,
            bytes: [0; Self::MAX_LEN],
        }
    }

    #[inline]
    pub fn try_new(bytes: &[u8]) -> Result<Self, AdjacencyRewriteError> {
        if bytes.len() > Self::MAX_LEN {
            return Err(AdjacencyRewriteError::TooLarge {
                len: bytes.len(),
                max: Self::MAX_LEN,
            });
        }

        let mut rewrite = Self::empty();
        rewrite.bytes[..bytes.len()].copy_from_slice(bytes);
        rewrite.len = bytes.len() as u8;
        Ok(rewrite)
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for AdjacencyRewrite {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency<N> {
    pub next: N,
    pub proto: DpoProto,
    pub egress_interface: Option<u32>,
    pub rewrite: AdjacencyRewrite,
    /// Operational L3 MTU in bytes — VPP `rewrite_header.max_l3_packet_bytes`.
    pub max_l3_packet_bytes: u16,
}

/// Default Ethernet L3 MTU used when interface MTU has not been applied yet.
pub const DEFAULT_ADJACENCY_L3_MTU: u16 = 1_500;

impl<N> Adjacency<N> {
    /// VPP `adj_nbr_set_mtu`: `path_mtu == 0` restores `link_mtu`; otherwise
    /// clamps to `min(link_mtu, path_mtu)`.
    #[inline]
    pub fn set_path_mtu(&mut self, link_mtu: u16, path_mtu: u16) {
        if path_mtu == 0 {
            self.max_l3_packet_bytes = link_mtu;
        } else {
            self.max_l3_packet_bytes = link_mtu.min(path_mtu);
        }
    }
}
