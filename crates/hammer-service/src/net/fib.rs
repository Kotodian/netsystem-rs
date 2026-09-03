use std::cmp::Reverse;
use std::collections::BTreeMap;

use super::dpo::DpoId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FibSourceBehavior {
    Drop,
    Api,
    Simple,
    RecursiveResolution,
    Interface,
    Interpose,
    Adjacency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FibSource {
    pub id: u8,
    pub priority: u8,
    pub behavior: FibSourceBehavior,
}

impl FibSource {
    pub const API: Self = Self {
        id: 0,
        priority: 0x80,
        behavior: FibSourceBehavior::Api,
    };

    pub const INTERFACE: Self = Self {
        id: 1,
        priority: 0x03,
        behavior: FibSourceBehavior::Interface,
    };
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibEntryFlags: u16 {
        const CONNECTED = 1 << 0;
        const ATTACHED = 1 << 1;
        const DROP = 1 << 2;
        const EXCLUSIVE = 1 << 3;
        const IMPORT = 1 << 4;
        const LOCAL = 1 << 5;
        const MULTICAST = 1 << 6;
        const LOOSE_URPF_EXEMPT = 1 << 7;
        const NO_ATTACHED_EXPORT = 1 << 8;
        const COVERED_INHERIT = 1 << 9;
        const INTERPOSE = 1 << 10;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibEntrySrcFlags: u16 {
        const ADDED = 1 << 0;
        const CONTRIBUTING = 1 << 1;
        const ACTIVE = 1 << 2;
        const STALE = 1 << 3;
        const INHERITED = 1 << 4;
        const PROVIDES_GLEAN = 1 << 5;
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct FibPathListFlags: u8 {
        const SHARED = 1 << 0;
        const DROP = 1 << 1;
        const LOCAL = 1 << 2;
        const EXCLUSIVE = 1 << 3;
        const RESOLVED = 1 << 4;
        const LOOPED = 1 << 5;
        const POPULAR = 1 << 6;
        const NO_URPF = 1 << 7;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibPath<N, F> {
    pub sw_if_index: u32,
    pub table_id: u32,
    pub rpf_id: u32,
    pub weight: u8,
    pub preference: u8,
    pub flags: F,
    pub next_hop: N,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibPathExt<N, F, E> {
    pub path: FibPath<N, F>,
    pub path_index: u32,
    pub data: E,
}

#[derive(Debug, Clone)]
pub struct FibPathExtList<N, F, E> {
    entries: Vec<FibPathExt<N, F, E>>,
}

impl<N, F, E> Default for FibPathExtList<N, F, E> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<N, F, E> FibPathExtList<N, F, E> {
    pub fn insert(&mut self, extension: FibPathExt<N, F, E>) {
        if let Some(current) = self
            .entries
            .iter_mut()
            .find(|current| current.path_index == extension.path_index)
        {
            *current = extension;
        } else {
            self.entries.push(extension);
        }
    }

    pub fn remove(&mut self, path_index: u32) -> Option<FibPathExt<N, F, E>> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.path_index == path_index)?;
        Some(self.entries.remove(position))
    }

    pub fn find(&self, path_index: u32) -> Option<&FibPathExt<N, F, E>> {
        self.entries
            .iter()
            .find(|entry| entry.path_index == path_index)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &FibPathExt<N, F, E>> {
        self.entries.iter()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Debug, Clone)]
pub struct FibEntrySrc<N, F, SourceData, PathExt> {
    pub path_exts: FibPathExtList<N, F, PathExt>,
    pub path_list: Option<u32>,
    pub entry_flags: FibEntryFlags,
    pub source: FibSource,
    pub flags: FibEntrySrcFlags,
    pub ref_count: u8,
    pub cover: Option<(u32, u32)>,
    pub interpose_dpo: Option<DpoId>,
    pub source_data: SourceData,
}

impl<N, F, SourceData, PathExt> FibEntrySrc<N, F, SourceData, PathExt> {
    pub fn new(source: FibSource, source_data: SourceData) -> Self {
        Self {
            path_exts: FibPathExtList::default(),
            path_list: None,
            entry_flags: FibEntryFlags::empty(),
            source,
            flags: FibEntrySrcFlags::ADDED,
            ref_count: 1,
            cover: None,
            interpose_dpo: None,
            source_data,
        }
    }

    pub fn add_reference(&mut self) -> bool {
        let Some(next) = self.ref_count.checked_add(1) else {
            return false;
        };
        self.ref_count = next;
        true
    }

    pub fn remove_reference(&mut self) -> bool {
        if self.ref_count == 0 {
            return false;
        }
        self.ref_count -= 1;
        self.ref_count != 0
    }
}

#[derive(Debug, Clone)]
pub struct FibPathList<N, F> {
    pub paths: Box<[FibPath<N, F>]>,
    pub key_flags: FibPathListFlags,
    pub flags: FibPathListFlags,
    pub source_count: u32,
    pub child_count: u32,
    pub children: Vec<(u16, u32)>,
    pub urpf_index: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FibEntry {
    pub flags: FibEntryFlags,
    pub sources: Vec<(FibSource, u32)>,
    pub forwarding: Option<DpoId>,
}

pub trait FibTableBackend {
    type Prefix: Copy + Ord;
    type PacketAddress: Copy;
    type NextHop: Clone;
    type PathFlags: Copy;
    type Error;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32>;
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32>;
    fn less_specific(&self, prefix: Self::Prefix) -> Option<u32>;
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn remove_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<u32>;
    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error>;
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error>;
    fn project_forwarding(
        &mut self,
        entry: &FibEntry,
        source: FibSource,
    ) -> Result<Option<DpoId>, Self::Error>;
}

#[derive(Debug)]
pub struct FibTable<P, B>
where
    P: Copy + Ord,
    B: FibTableBackend<Prefix = P>,
{
    backend: B,
    entries: Vec<FibEntry>,
    prefixes: BTreeMap<P, u32>,
    source_references: BTreeMap<(u32, FibSource), u8>,
}

impl<P, B> FibTable<P, B>
where
    P: Copy + Ord,
    B: FibTableBackend<Prefix = P>,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            entries: Vec::new(),
            prefixes: BTreeMap::new(),
            source_references: BTreeMap::new(),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    pub fn entry(&self, index: u32) -> Option<&FibEntry> {
        self.entries.get(index as usize)
    }

    pub fn lookup(&self, prefix: P) -> Option<u32> {
        self.backend.lookup(prefix)
    }

    pub fn lookup_exact(&self, prefix: P) -> Option<u32> {
        self.backend.lookup_exact(prefix)
    }

    pub fn forwarding_lookup(&self, address: B::PacketAddress) -> Option<u32> {
        self.backend.forwarding_lookup(address)
    }

    pub fn add_source(&mut self, prefix: P, source: FibSource) -> Result<u32, FibError> {
        let entry = if let Some(index) = self.prefixes.get(&prefix).copied() {
            index
        } else {
            let index = u32::try_from(self.entries.len()).map_err(|_| FibError::BackendRejected)?;
            self.backend
                .insert_entry(prefix, index)
                .map_err(|_| FibError::BackendRejected)?;
            self.prefixes.insert(prefix, index);
            self.entries.push(FibEntry {
                flags: FibEntryFlags::empty(),
                sources: Vec::new(),
                forwarding: None,
            });
            index
        };
        let key = (entry, source);
        let references = self.source_references.entry(key).or_insert(0);
        *references = references
            .checked_add(1)
            .ok_or(FibError::ReferenceCountOverflow)?;
        if *references == 1 {
            self.entries[entry as usize].sources.push((source, entry));
            self.entries[entry as usize]
                .sources
                .sort_by_key(|(candidate, _)| Reverse(candidate.priority));
        }
        Ok(entry)
    }

    pub fn winner_source(&self, prefix: P) -> Option<FibSource> {
        let entry = self.prefixes.get(&prefix).copied()?;
        self.entries
            .get(entry as usize)?
            .sources
            .first()
            .map(|(source, _)| *source)
    }

    pub fn remove_source(&mut self, prefix: P, source: FibSource) -> Result<bool, FibError> {
        let Some(entry) = self.prefixes.get(&prefix).copied() else {
            return Ok(false);
        };
        let key = (entry, source);
        let Some(references) = self.source_references.get_mut(&key) else {
            return Err(FibError::SourceMissing);
        };
        if *references > 1 {
            *references -= 1;
            return Ok(true);
        }
        let record = &mut self.entries[entry as usize];
        let last_source = record.sources.len() == 1;
        if last_source {
            self.backend
                .remove_entry(prefix, entry)
                .map_err(|_| FibError::BackendRejected)?;
        }
        self.source_references.remove(&key);
        record.sources.retain(|(candidate, _)| *candidate != source);
        if last_source {
            self.prefixes.remove(&prefix);
        }
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum FibError {
    #[error("FIB source reference count overflow")]
    ReferenceCountOverflow,
    #[error("FIB source is not registered for this entry")]
    SourceMissing,
    #[error("FIB backend rejected the mutation")]
    BackendRejected,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_record_retains_references_and_replaces_extensions() {
        let source = FibSource::API;
        let mut record = FibEntrySrc::<u32, u8, u16, u16>::new(source, 7);
        assert_eq!(record.ref_count, 1);
        assert!(record.add_reference());
        assert_eq!(record.ref_count, 2);
        assert!(record.remove_reference());
        assert_eq!(record.ref_count, 1);
        let path = FibPath {
            sw_if_index: 3,
            table_id: 4,
            rpf_id: 5,
            weight: 1,
            preference: 0,
            flags: 0,
            next_hop: 10,
        };
        record.path_exts.insert(FibPathExt {
            path,
            path_index: 9,
            data: 1,
        });
        record.path_exts.insert(FibPathExt {
            path,
            path_index: 9,
            data: 2,
        });
        assert_eq!(record.path_exts.iter().len(), 1);
        assert_eq!(record.path_exts.find(9).unwrap().data, 2);
    }

    #[test]
    fn graph_flags_keep_declared_widths() {
        assert_eq!(core::mem::size_of::<FibEntryFlags>(), 2);
        assert_eq!(core::mem::size_of::<FibEntrySrcFlags>(), 2);
        assert_eq!(core::mem::size_of::<FibPathListFlags>(), 1);
        assert!(FibEntryFlags::INTERPOSE.bits() != 0);
    }
}
