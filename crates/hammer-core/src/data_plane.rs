#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    #[inline(always)]
    pub const fn new(slot: u32) -> Self {
        Self(slot)
    }

    #[inline]
    pub fn slot(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeHandle(u32);

impl NodeHandle {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Plain,
    Driver,
    Internal,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NodeState {
    Disabled,
    #[default]
    Polling,
    Interrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistration {
    Plain,
    Next {
        name: &'static str,
        next_count: usize,
    },
    Sibling {
        name: &'static str,
        sibling_of: &'static str,
    },
}

impl NodeRegistration {
    #[inline]
    pub const fn next(name: &'static str, next_count: usize) -> Self {
        Self::Next { name, next_count }
    }

    #[inline]
    pub const fn sibling_of(name: &'static str, sibling_of: &'static str) -> Self {
        Self::Sibling { name, sibling_of }
    }

    #[inline]
    pub fn name(self) -> Option<&'static str> {
        match self {
            Self::Plain => None,
            Self::Next { name, .. } | Self::Sibling { name, .. } => Some(name),
        }
    }
}

pub const MAX_NODE_NEXT_SLOTS: usize = 16;

pub trait NodeNext: Copy + Eq {
    const COUNT: usize;

    fn slot(self) -> usize;
}

pub trait NodeNextStorage<K> {
    fn next(&self, key: K) -> NodeId;
}

impl<K, const N: usize> NodeNextStorage<K> for [NodeId; N]
where
    K: NodeNext,
{
    #[inline(always)]
    fn next(&self, key: K) -> NodeId {
        self[key.slot()]
    }
}

impl NodeNextStorage<()> for NodeId {
    #[inline(always)]
    fn next(&self, _key: ()) -> NodeId {
        *self
    }
}
