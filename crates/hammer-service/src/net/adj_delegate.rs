use std::fmt;

use hammer_infra::pool::Pool;

use super::adj::{AdjacencyIndex, AdjacencyMain, FibProtocol};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AdjacencyDelegateType(u8);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdjacencyDelegateList(u64);

impl AdjacencyDelegateList {
    pub const NONE: Self = Self(u64::MAX);
}

#[derive(Clone, Copy, Debug)]
pub struct AdjacencyDelegate {
    pub adjacency: AdjacencyIndex,
    pub delegate_type: AdjacencyDelegateType,
    pub provider_index: u32,
}

#[derive(Clone, Copy)]
pub struct AdjacencyDelegateOperations {
    pub format: Option<fn(AdjacencyDelegate, &mut fmt::Formatter<'_>) -> fmt::Result>,
    pub created: Option<fn(AdjacencyIndex)>,
    pub modified: Option<fn(AdjacencyDelegate)>,
    pub deleted: Option<fn(AdjacencyDelegate)>,
}

#[derive(Default)]
pub struct AdjacencyDelegateMain {
    operations: Vec<AdjacencyDelegateOperations>,
    lists: Pool<Vec<AdjacencyDelegate>>,
}

impl AdjacencyDelegateMain {
    pub fn register(&mut self, operations: AdjacencyDelegateOperations) -> AdjacencyDelegateType {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("delegate registration requires publication ownership");
        let index = u8::try_from(self.operations.len()).expect("delegate type space exhausted");
        self.operations.push(operations);
        AdjacencyDelegateType(index)
    }

    pub fn add<P: FibProtocol>(
        &mut self,
        adjacency: &mut AdjacencyMain<P>,
        index: AdjacencyIndex,
        delegate_type: AdjacencyDelegateType,
        provider_index: u32,
    ) -> bool {
        assert!(
            self.operations.get(delegate_type.0 as usize).is_some(),
            "delegate type must be registered"
        );
        let handle = adjacency.get(index).delegates;
        let list = if handle == u64::MAX {
            let handle = self.lists.insert(Vec::new());
            adjacency.get_mut(index).delegates = handle.into();
            handle
        } else {
            u32::try_from(handle).expect("delegate list index is representable")
        };
        let entries = self.lists.get_mut(list).expect("delegate list must exist");
        match entries.binary_search_by_key(&delegate_type, |entry| entry.delegate_type) {
            Ok(_) => false,
            Err(position) => {
                entries.insert(
                    position,
                    AdjacencyDelegate {
                        adjacency: index,
                        delegate_type,
                        provider_index,
                    },
                );
                true
            }
        }
    }

    pub fn remove<P: FibProtocol>(
        &mut self,
        adjacency: &mut AdjacencyMain<P>,
        index: AdjacencyIndex,
        delegate_type: AdjacencyDelegateType,
    ) {
        let handle = adjacency.get(index).delegates;
        assert_ne!(handle, u64::MAX, "removed delegate must exist");
        let list = u32::try_from(handle).expect("delegate list index is representable");
        let entries = self.lists.get_mut(list).expect("delegate list must exist");
        let position = entries
            .binary_search_by_key(&delegate_type, |entry| entry.delegate_type)
            .expect("removed delegate must exist");
        entries.remove(position);
        if entries.is_empty() {
            self.lists
                .remove(list)
                .expect("empty delegate list must exist");
            adjacency.get_mut(index).delegates = u64::MAX;
        }
    }

    pub fn created(&self, index: AdjacencyIndex) {
        for operations in &self.operations {
            if let Some(created) = operations.created {
                created(index);
            }
        }
    }

    pub fn modified<P: FibProtocol>(&self, adjacency: &AdjacencyMain<P>, index: AdjacencyIndex) {
        let handle = adjacency.get(index).delegates;
        if handle == u64::MAX {
            return;
        }
        let list = u32::try_from(handle).expect("delegate list index is representable");
        for &entry in self.lists.get(list).expect("delegate list must exist") {
            if let Some(modified) = self.operations[entry.delegate_type.0 as usize].modified {
                modified(entry);
            }
        }
    }

    pub fn deleted<P: FibProtocol>(
        &mut self,
        adjacency: &mut AdjacencyMain<P>,
        index: AdjacencyIndex,
    ) {
        let handle = adjacency.get(index).delegates;
        if handle == u64::MAX {
            return;
        }
        let list = u32::try_from(handle).expect("delegate list index is representable");
        let entries = self.lists.remove(list).expect("delegate list must exist");
        adjacency.get_mut(index).delegates = u64::MAX;
        for entry in entries {
            if let Some(deleted) = self.operations[entry.delegate_type.0 as usize].deleted {
                deleted(entry);
            }
        }
    }
}
