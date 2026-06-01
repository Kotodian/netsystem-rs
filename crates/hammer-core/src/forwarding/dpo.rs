use crate::protocol::ip::IpVersion;

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
    pub fn adjacency_index(&self) -> Option<AdjacencyIndex> {
        match self.dpo_type {
            DpoType::Adjacency => Some(AdjacencyIndex::new(self.index)),
            DpoType::Drop | DpoType::Punt | DpoType::Receive | DpoType::Custom => None,
        }
    }

    #[inline(always)]
    pub fn custom_type(&self) -> Option<CustomDpoType> {
        match self.dpo_type {
            DpoType::Custom => CustomDpoType::new(self.custom_type),
            DpoType::Drop | DpoType::Punt | DpoType::Adjacency | DpoType::Receive => None,
        }
    }

    #[inline(always)]
    pub fn custom_index(&self) -> Option<CustomDpoIndex> {
        match self.dpo_type {
            DpoType::Custom => Some(CustomDpoIndex::new(self.index)),
            DpoType::Drop | DpoType::Punt | DpoType::Adjacency | DpoType::Receive => None,
        }
    }

    #[inline(always)]
    pub fn forwarding_index(&self) -> u32 {
        match self.dpo_type {
            DpoType::Adjacency | DpoType::Custom => self.index,
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

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency<N> {
    pub next: N,
    pub proto: IpVersion,
}
