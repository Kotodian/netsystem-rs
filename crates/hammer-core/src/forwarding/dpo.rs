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
pub struct DpoId<N> {
    pub next: N,
    pub index: u32,
    pub dpo_type: DpoType,
    pub proto: IpVersion,
}

impl<N> DpoId<N> {
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
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adjacency<N> {
    pub next: N,
    pub proto: IpVersion,
}
