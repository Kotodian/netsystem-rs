use crate::protocol::ip::IpVersion;

use super::load_balance::LoadBalanceIndex;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CustomDpoType(u16);

impl CustomDpoType {
    #[inline(always)]
    pub const fn new(value: u16) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    #[inline(always)]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct CustomDpoRegistry {
    next: u16,
}

impl Default for CustomDpoRegistry {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl CustomDpoRegistry {
    #[inline]
    pub const fn new() -> Self {
        Self { next: 1 }
    }

    #[inline]
    pub fn register(&mut self) -> Option<CustomDpoType> {
        let dpo_type = CustomDpoType::new(self.next)?;
        self.next = self.next.checked_add(1).unwrap_or_default();
        Some(dpo_type)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CustomDpoIndex(u32);

impl CustomDpoIndex {
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpoType {
    Drop = 0,
    Punt = 1,
    Adjacency = 2,
    Receive = 3,
    Custom = 4,
    LoadBalance = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DpoClass {
    dpo_type: DpoType,
    custom_type: u16,
}

impl DpoClass {
    #[inline(always)]
    pub const fn builtin(dpo_type: DpoType) -> Option<Self> {
        match dpo_type {
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::LoadBalance => Some(Self {
                dpo_type,
                custom_type: 0,
            }),
            DpoType::Custom => None,
        }
    }

    #[inline(always)]
    pub const fn custom(custom_type: CustomDpoType) -> Self {
        Self {
            dpo_type: DpoType::Custom,
            custom_type: custom_type.get(),
        }
    }

    #[inline(always)]
    pub const fn dpo_type(self) -> DpoType {
        self.dpo_type
    }

    #[inline(always)]
    pub const fn custom_type(self) -> Option<CustomDpoType> {
        match self.dpo_type {
            DpoType::Custom => CustomDpoType::new(self.custom_type),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::LoadBalance => None,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dpo<N> {
    next: N,
    index: u32,
    custom_type: u16,
    dpo_type: DpoType,
    proto: IpVersion,
}

pub type DpoId<N> = Dpo<N>;

impl<N> Dpo<N> {
    #[inline(always)]
    pub const fn drop(proto: IpVersion, next: N) -> Self {
        Self {
            next,
            index: 0,
            custom_type: 0,
            dpo_type: DpoType::Drop,
            proto,
        }
    }

    #[inline(always)]
    pub const fn punt(proto: IpVersion, next: N) -> Self {
        Self {
            next,
            index: 0,
            custom_type: 0,
            dpo_type: DpoType::Punt,
            proto,
        }
    }

    #[inline(always)]
    pub const fn receive(proto: IpVersion, next: N) -> Self {
        Self {
            next,
            index: 0,
            custom_type: 0,
            dpo_type: DpoType::Receive,
            proto,
        }
    }

    #[inline(always)]
    pub const fn adjacency(proto: IpVersion, adjacency: AdjacencyIndex, next: N) -> Self {
        Self {
            next,
            index: adjacency.get(),
            custom_type: 0,
            dpo_type: DpoType::Adjacency,
            proto,
        }
    }

    #[inline(always)]
    pub const fn load_balance(proto: IpVersion, load_balance: LoadBalanceIndex, next: N) -> Self {
        Self {
            next,
            index: load_balance.get(),
            custom_type: 0,
            dpo_type: DpoType::LoadBalance,
            proto,
        }
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
            custom_type: parent.custom_type,
            dpo_type: parent.dpo_type,
            proto: parent.proto,
        }
    }

    #[inline(always)]
    pub const fn custom(
        proto: IpVersion,
        custom_type: CustomDpoType,
        custom_index: CustomDpoIndex,
        next: N,
    ) -> Self {
        Self {
            next,
            index: custom_index.get(),
            custom_type: custom_type.get(),
            dpo_type: DpoType::Custom,
            proto,
        }
    }

    #[inline(always)]
    pub const fn proto(&self) -> IpVersion {
        self.proto
    }

    #[inline(always)]
    pub fn kind(&self) -> DpoType {
        self.dpo_type
    }

    #[inline(always)]
    pub fn class(&self) -> DpoClass {
        match self.dpo_type {
            DpoType::Custom => DpoClass::custom(
                CustomDpoType::new(self.custom_type).expect("custom DPO stores non-zero type"),
            ),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::LoadBalance => DpoClass::builtin(self.dpo_type).expect("built-in DPO class"),
        }
    }

    #[inline(always)]
    pub fn adjacency_index(&self) -> Option<AdjacencyIndex> {
        match self.dpo_type {
            DpoType::Adjacency => Some(AdjacencyIndex::new(self.index)),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Receive
            | DpoType::Custom
            | DpoType::LoadBalance => None,
        }
    }

    #[inline(always)]
    pub fn load_balance_index(&self) -> Option<LoadBalanceIndex> {
        match self.dpo_type {
            DpoType::LoadBalance => Some(LoadBalanceIndex::new(self.index)),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::Custom => None,
        }
    }

    #[inline(always)]
    pub fn custom_type(&self) -> Option<CustomDpoType> {
        match self.dpo_type {
            DpoType::Custom => CustomDpoType::new(self.custom_type),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::LoadBalance => None,
        }
    }

    #[inline(always)]
    pub fn custom_index(&self) -> Option<CustomDpoIndex> {
        match self.dpo_type {
            DpoType::Custom => Some(CustomDpoIndex::new(self.index)),
            DpoType::Drop
            | DpoType::Punt
            | DpoType::Adjacency
            | DpoType::Receive
            | DpoType::LoadBalance => None,
        }
    }

    #[inline(always)]
    pub fn forwarding_index(&self) -> u32 {
        match self.dpo_type {
            DpoType::Adjacency | DpoType::Custom | DpoType::LoadBalance => self.index,
            DpoType::Drop | DpoType::Punt | DpoType::Receive => 0,
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
    pub fn register(&mut self, child: DpoClass, parent: DpoClass, proto: IpVersion, next: N) {
        let key = DpoStackKey {
            child,
            parent,
            proto,
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
    pub fn stack(&self, child: DpoClass, parent: Dpo<N>) -> Option<Dpo<N>> {
        let key = DpoStackKey {
            child,
            parent: parent.class(),
            proto: parent.proto(),
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
    parent: DpoClass,
    proto: IpVersion,
}

#[derive(Debug, Clone)]
struct DpoStackEdge<N> {
    key: DpoStackKey,
    next: N,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency<N> {
    pub next: N,
    pub proto: IpVersion,
}
