use std::fmt;

use crate::error::{DataPlaneError, PacketGraphResult};

pub(crate) const HAMMER_MAX_NUMA_NODES: usize = 32;

pub(crate) struct StaticNumaTable<T, const N: usize> {
    entries: [Option<T>; N],
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for StaticNumaTable<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T: Clone, const N: usize> Clone for StaticNumaTable<T, N> {
    fn clone(&self) -> Self {
        Self {
            entries: std::array::from_fn(|index| self.entries[index].clone()),
        }
    }
}

impl<T, const N: usize> StaticNumaTable<T, N> {
    pub(crate) fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
        }
    }

    pub(crate) fn insert(&mut self, numa_node: u32, value: T) -> PacketGraphResult<()> {
        let index = usize::try_from(numa_node)
            .map_err(|_| DataPlaneError::NumaNodeDoesNotFitUsize { numa_node })?;
        let slot = self.entries.get_mut(index).ok_or(
            DataPlaneError::NumaNodeExceedsStaticMemoryTable {
                numa_node,
                capacity: N,
            },
        )?;
        if slot.is_some() {
            return Err(DataPlaneError::DuplicateNumaMemoryEntry { numa_node }.into());
        }
        *slot = Some(value);
        Ok(())
    }

    pub(crate) fn get(&self, numa_node: u32) -> Option<&T> {
        let index = usize::try_from(numa_node).ok()?;
        self.entries.get(index).and_then(Option::as_ref)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (u32, &T)> + '_ {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                let value = value.as_ref()?;
                let numa_node = u32::try_from(index).ok()?;
                Some((numa_node, value))
            })
    }
}

impl<T, const N: usize> Default for StaticNumaTable<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
