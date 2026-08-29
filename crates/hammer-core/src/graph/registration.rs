#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRegistration {
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
    pub const fn name(self) -> &'static str {
        match self {
            Self::Next { name, .. } | Self::Sibling { name, .. } => name,
        }
    }
}
