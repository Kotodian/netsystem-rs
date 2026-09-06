use std::cell::Ref;
use std::collections::BTreeMap;

use hammer_runtime::RuntimeResult;

use super::dpo::DpoId;

/// Baked reverse-path interfaces. Published lists are immutable and shared by
/// path lists and load-balances, independently of their DPO child references.
#[derive(Debug)]
pub struct FibUrpfList {
    interfaces: Vec<u32>,
    lock_count: u32,
}

impl FibUrpfList {
    pub fn interfaces(&self) -> &[u32] {
        &self.interfaces
    }
}

impl super::NetMain {
    pub fn create_urpf_list(&self, mut interfaces: Vec<u32>) -> RuntimeResult<u32> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        interfaces.sort_unstable();
        interfaces.dedup();
        Ok(self.urpf_lists.borrow_mut().insert(FibUrpfList {
            interfaces,
            lock_count: 1,
        }))
    }

    pub fn urpf_list(&self, index: u32) -> Option<Ref<'_, FibUrpfList>> {
        hammer_runtime::ensure_main_thread()
            .expect("uRPF control-plane borrow requires the main thread");
        Ref::filter_map(self.urpf_lists.borrow(), |pool| pool.get(index)).ok()
    }

    pub fn lock_urpf_list(&self, index: u32) {
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("uRPF reference acquisition requires the publication scope");
        let mut pool = self.urpf_lists.borrow_mut();
        let list = pool
            .get_mut(index)
            .expect("referenced uRPF list must exist");
        list.lock_count = list
            .lock_count
            .checked_add(1)
            .expect("uRPF reference count must remain representable");
    }

    pub fn unlock_urpf_list(&self, index: u32) {
        if index == u32::MAX {
            return;
        }
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("uRPF retirement requires the publication scope");
        let mut pool = self.urpf_lists.borrow_mut();
        let list = pool.get_mut(index).expect("owned uRPF list must exist");
        list.lock_count = list
            .lock_count
            .checked_sub(1)
            .expect("uRPF retirement requires an owned reference");
        if list.lock_count == 0 {
            drop(pool.remove(index));
        }
    }

    #[inline(always)]
    pub fn urpf_size(&self, index: u32) -> Option<usize> {
        if hammer_runtime::ensure_main_thread().is_ok() {
            return self.urpf_list(index).map(|list| list.interfaces.len());
        }
        hammer_runtime::with_data_plane_main(|runtime| {
            assert_ne!(
                runtime.thread_index(),
                0,
                "uRPF packet read requires a Data Worker"
            );
            // SAFETY: the worker cannot acknowledge a barrier inside this
            // synchronous read. Main mutates only after all acknowledgements;
            // only the interface count escapes.
            unsafe {
                (&*self.urpf_lists.as_ptr())
                    .get(index)
                    .map(|list| list.interfaces.len())
            }
        })
    }

    #[inline(always)]
    pub fn urpf_check(&self, index: u32, sw_if_index: u32) -> Option<bool> {
        if hammer_runtime::ensure_main_thread().is_ok() {
            return self
                .urpf_list(index)
                .map(|list| list.interfaces.binary_search(&sw_if_index).is_ok());
        }
        hammer_runtime::with_data_plane_main(|runtime| {
            assert_ne!(
                runtime.thread_index(),
                0,
                "uRPF packet read requires a Data Worker"
            );
            // SAFETY: main cannot change or reclaim the list until this
            // worker acknowledges its barrier. Only membership is returned.
            unsafe {
                (&*self.urpf_lists.as_ptr())
                    .get(index)
                    .map(|list| list.interfaces.binary_search(&sw_if_index).is_ok())
            }
        })
    }
}

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

#[derive(Debug)]
pub struct FibPathList<N, F> {
    pub paths: Vec<FibPath<N, F>>,
    pub key_flags: FibPathListFlags,
    pub flags: FibPathListFlags,
    pub source_count: u32,
    pub child_count: u32,
    pub children: Vec<(u16, u32)>,
    urpf_index: Option<u32>,
}

impl<N, F> FibPathList<N, F> {
    pub fn new(paths: Vec<FibPath<N, F>>, key_flags: FibPathListFlags) -> Self {
        Self {
            paths,
            key_flags,
            flags: key_flags,
            source_count: 0,
            child_count: 0,
            children: Vec::new(),
            urpf_index: None,
        }
    }

    /// Path owners supply accepting interfaces according to their path semantics,
    /// including unresolved attached paths and non-looped recursive via entries.
    pub fn bake_urpf(&mut self, interfaces: Vec<u32>) -> RuntimeResult<()> {
        let net = super::NetMain::global()?;
        if self.key_flags.contains(FibPathListFlags::NO_URPF) {
            hammer_runtime::ensure_main_thread_with_barrier()?;
            if let Some(old) = self.urpf_index {
                net.unlock_urpf_list(old);
                self.urpf_index = None;
            }
            return Ok(());
        }
        let new = net.create_urpf_list(interfaces)?;
        if let Some(old) = self.urpf_index.replace(new) {
            net.unlock_urpf_list(old);
        }
        Ok(())
    }

    pub fn urpf_index(&self) -> Option<u32> {
        self.urpf_index
    }
}

impl<N, F> Drop for FibPathList<N, F> {
    fn drop(&mut self) {
        if let Some(index) = self.urpf_index {
            super::NetMain::global()
                .expect("FIB owner must outlive its path lists")
                .unlock_urpf_list(index);
        }
    }
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
    type Error: std::error::Error + 'static;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32>;
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32>;
    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)>;
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn remove_entry(&mut self, prefix: Self::Prefix, entry: u32) -> Result<(), Self::Error>;
    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId>;
    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId) -> Result<(), Self::Error>;
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    ) -> Result<(), Self::Error>;
    /// Some transfers one acquired DPO reference to the FIB. None transfers
    /// none; an error must leave any candidate references with the backend.
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
    source_forwarding: BTreeMap<(P, FibSource), DpoId>,
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
            source_forwarding: BTreeMap::new(),
        }
    }

    pub fn backend(&self) -> &B {
        &self.backend
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

    pub fn forwarding_lookup(&self, address: B::PacketAddress) -> Option<DpoId> {
        self.backend.forwarding_lookup(address)
    }

    pub fn add_route(
        &mut self,
        prefix: P,
        source: FibSource,
        forwarding: DpoId,
    ) -> Result<u32, FibError<B::Error>>
    where
        B: Clone,
    {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let net = super::NetMain::global()?;
        // These are non-owning projections. The source roots remain untouched
        // until backend validation and publication have both succeeded.
        let previous = (
            self.backend.clone(),
            self.entries.clone(),
            self.prefixes.clone(),
            self.source_references.clone(),
        );
        let result = (|| {
            let entry = self.add_source(prefix, source)?;
            let mut projection = self.entries[entry as usize].clone();
            projection.forwarding = Some(forwarding);
            let forwarding = self
                .backend
                .project_forwarding(&projection, source)
                .map_err(FibError::Backend)?;
            let selected =
                self.entries[entry as usize]
                    .sources
                    .first()
                    .and_then(|(candidate, _)| {
                        if *candidate == source {
                            forwarding
                        } else {
                            self.source_forwarding.get(&(prefix, *candidate)).copied()
                        }
                    });
            let publication = if let Some(selected) = selected {
                self.backend.forwarding_update(prefix, selected)
            } else if let Some(old) = self.entries[entry as usize].forwarding {
                let cover = self
                    .backend
                    .less_specific(prefix)
                    .and_then(|(prefix, entry)| {
                        self.entries[entry as usize]
                            .forwarding
                            .map(|dpo| (prefix, dpo))
                    });
                self.backend.forwarding_remove(prefix, old, cover)
            } else {
                Ok(())
            };
            if let Err(error) = publication {
                if let Some(candidate) = forwarding {
                    net.unlock_dpo(candidate);
                }
                return Err(FibError::Backend(error));
            }
            self.entries[entry as usize].forwarding = selected;
            Ok((entry, forwarding))
        })();
        let (entry, forwarding) = match result {
            Ok(result) => result,
            Err(error) => {
                (
                    self.backend,
                    self.entries,
                    self.prefixes,
                    self.source_references,
                ) = previous;
                return Err(error);
            }
        };
        let old = if let Some(forwarding) = forwarding {
            self.source_forwarding.insert((prefix, source), forwarding)
        } else {
            self.source_forwarding.remove(&(prefix, source))
        };
        if let Some(old) = old {
            net.unlock_dpo(old);
        }
        Ok(entry)
    }

    pub fn remove_route(&mut self, prefix: P, source: FibSource) -> Result<bool, FibError<B::Error>>
    where
        B: Clone,
    {
        let Some(entry) = self.prefixes.get(&prefix).copied() else {
            return Ok(false);
        };
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let net = super::NetMain::global()?;
        let previous = (
            self.backend.clone(),
            self.entries.clone(),
            self.prefixes.clone(),
            self.source_references.clone(),
        );
        let old = self.entries[entry as usize].forwarding;
        let result = (|| {
            let removed = self.remove_source(prefix, source)?;
            if !removed {
                return Ok(false);
            }
            let source_remains = self.source_references.contains_key(&(entry, source));
            if source_remains {
                return Ok(false);
            }
            let selected =
                self.entries[entry as usize]
                    .sources
                    .first()
                    .and_then(|(candidate, _)| {
                        self.source_forwarding.get(&(prefix, *candidate)).copied()
                    });
            if let Some(selected) = selected {
                self.backend
                    .forwarding_update(prefix, selected)
                    .map_err(FibError::Backend)?;
            } else if let Some(old) = old {
                let cover =
                    self.backend
                        .less_specific(prefix)
                        .and_then(|(cover_prefix, cover_entry)| {
                            self.entries
                                .get(cover_entry as usize)
                                .and_then(|entry| entry.forwarding)
                                .map(|dpo| (cover_prefix, dpo))
                        });
                self.backend
                    .forwarding_remove(prefix, old, cover)
                    .map_err(FibError::Backend)?;
            }
            self.entries[entry as usize].forwarding = selected;
            Ok(true)
        })();
        match result {
            Ok(source_removed) => {
                if source_removed {
                    if let Some(old) = self.source_forwarding.remove(&(prefix, source)) {
                        net.unlock_dpo(old);
                    }
                }
                Ok(true)
            }
            Err(error) => {
                (
                    self.backend,
                    self.entries,
                    self.prefixes,
                    self.source_references,
                ) = previous;
                Err(error)
            }
        }
    }

    fn add_source(&mut self, prefix: P, source: FibSource) -> Result<u32, FibError<B::Error>> {
        if let Some(entry) = self.prefixes.get(&prefix).copied() {
            if self
                .source_references
                .get(&(entry, source))
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .is_none()
            {
                return Err(FibError::ReferenceCountOverflow);
            }
        }
        let entry = if let Some(index) = self.prefixes.get(&prefix).copied() {
            index
        } else {
            let index =
                u32::try_from(self.entries.len()).map_err(|_| FibError::EntryIndexOverflow {
                    count: self.entries.len(),
                })?;
            self.backend
                .insert_entry(prefix, index)
                .map_err(FibError::Backend)?;
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
        *references += 1;
        if *references == 1 {
            self.entries[entry as usize].sources.push((source, entry));
            self.entries[entry as usize]
                .sources
                .sort_by_key(|(candidate, _)| candidate.priority);
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

    fn remove_source(&mut self, prefix: P, source: FibSource) -> Result<bool, FibError<B::Error>> {
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
                .map_err(FibError::Backend)?;
        }
        self.source_references.remove(&key);
        record.sources.retain(|(candidate, _)| *candidate != source);
        if last_source {
            self.prefixes.remove(&prefix);
        }
        Ok(true)
    }
}

impl<P, B> Drop for FibTable<P, B>
where
    P: Copy + Ord,
    B: FibTableBackend<Prefix = P>,
{
    fn drop(&mut self) {
        if self.source_forwarding.is_empty() {
            return;
        }
        let net = super::NetMain::global().expect("FIB roots must not outlive their net owner");
        hammer_runtime::ensure_main_thread_with_barrier()
            .expect("FIB destruction requires the main-thread publication scope");
        for (_, forwarding) in std::mem::take(&mut self.source_forwarding) {
            net.unlock_dpo(forwarding);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FibError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Runtime(#[from] hammer_runtime::RuntimeError),
    #[error("FIB source reference count overflow")]
    ReferenceCountOverflow,
    #[error("FIB source is not registered for this entry")]
    SourceMissing,
    #[error("FIB backend mutation failed")]
    Backend(#[source] E),
    #[error("FIB entry count {count} exceeds the index representation")]
    EntryIndexOverflow { count: usize },
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
