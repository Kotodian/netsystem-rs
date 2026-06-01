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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpoType {
    Drop = 0,
    Punt = 1,
    Adjacency = 2,
    Receive = 3,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dpo<N> {
    next: N,
    index: u32,
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
            dpo_type: DpoType::Drop,
            proto,
        }
    }

    #[inline(always)]
    pub const fn punt(proto: IpVersion, next: N) -> Self {
        Self {
            next,
            index: 0,
            dpo_type: DpoType::Punt,
            proto,
        }
    }

    #[inline(always)]
    pub const fn receive(proto: IpVersion, next: N) -> Self {
        Self {
            next,
            index: 0,
            dpo_type: DpoType::Receive,
            proto,
        }
    }

    #[inline(always)]
    pub const fn adjacency(proto: IpVersion, adjacency: AdjacencyIndex, next: N) -> Self {
        Self {
            next,
            index: adjacency.get(),
            dpo_type: DpoType::Adjacency,
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
            DpoType::Drop | DpoType::Punt | DpoType::Receive => None,
        }
    }

    #[inline(always)]
    pub fn forwarding_index(&self) -> u32 {
        self.adjacency_index()
            .map(AdjacencyIndex::get)
            .unwrap_or_default()
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
