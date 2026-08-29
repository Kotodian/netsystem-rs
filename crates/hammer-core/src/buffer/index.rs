/// Copyable data-plane pool identity. Pools construct it; Frame or another
/// domain owner retains release responsibility. Copying does not alter
/// reference counts.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index {
    pub(super) pool_id: u64,
    pub(super) slot: u32,
    pub(super) generation: u32,
}

const _: () = assert!(core::mem::size_of::<Index>() == 16);

impl Index {
    pub fn pool_id(self) -> u64 {
        self.pool_id
    }

    pub fn slot(self) -> u32 {
        self.slot
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}
