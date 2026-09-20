use std::cell::Ref;
use std::collections::BTreeMap;

use hammer_runtime::RuntimeResult;

use super::dpo::DpoId;
use super::fib_node::{FibNode, FibNodeSibling, FibNodeType};

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
        // SAFETY: the worker cannot acknowledge a barrier inside this
        // synchronous read. Main mutates only after all acknowledgements;
        // only the interface count escapes.
        unsafe {
            (&*self.urpf_lists.as_ptr())
                .get(index)
                .map(|list| list.interfaces.len())
        }
    }

    #[inline(always)]
    pub fn urpf_check(&self, index: u32, sw_if_index: u32) -> Option<bool> {
        if hammer_runtime::ensure_main_thread().is_ok() {
            return self
                .urpf_list(index)
                .map(|list| list.interfaces.binary_search(&sw_if_index).is_ok());
        }
        // SAFETY: main cannot change or reclaim the list until this worker
        // acknowledges its barrier. Only membership is returned.
        unsafe {
            (&*self.urpf_lists.as_ptr())
                .get(index)
                .map(|list| list.interfaces.binary_search(&sw_if_index).is_ok())
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FibSource(u8);

impl FibSource {
    pub const INVALID: Self = Self(0);
    pub const INTERFACE: Self = Self(4);
    pub const API: Self = Self(8);
    pub const ADJACENCY: Self = Self(15);
    pub const ATTACHED_EXPORT: Self = Self(17);

    pub const fn new(value: u8) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u8 {
        self.0
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibEntrySourceBehaviorId(u8);

impl FibEntrySourceBehaviorId {
    pub const SIMPLE: Self = Self(0);
    pub const API: Self = Self(1);
    pub const INTERFACE: Self = Self(2);
    pub const ADJACENCY: Self = Self(3);
    pub const fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FibSourceRegistration {
    pub name: &'static str,
    pub priority: u8,
    pub priority_slot: u8,
    pub behavior: FibEntrySourceBehaviorId,
}

impl FibSourceRegistration {
    pub const fn new(name: &'static str, priority: u8, behavior: FibEntrySourceBehaviorId) -> Self {
        Self {
            name,
            priority,
            priority_slot: 0,
            behavior,
        }
    }
}

pub struct FibSourceMain {
    registrations: Vec<Option<FibSourceRegistration>>,
    next_source: u8,
    priority_slots: [u8; 256],
}

impl Default for FibSourceMain {
    fn default() -> Self {
        Self::new()
    }
}

impl FibSourceMain {
    pub fn new() -> Self {
        Self {
            registrations: Vec::new(),
            next_source: 22,
            priority_slots: [0; 256],
        }
    }

    pub fn register(
        &mut self,
        source: FibSource,
        mut registration: FibSourceRegistration,
    ) -> FibSource {
        let index = usize::from(source.get());
        assert_ne!(
            source,
            FibSource::INVALID,
            "invalid FIB source cannot register"
        );
        if self.registrations.len() <= index {
            self.registrations.resize(index + 1, None);
        }
        let slot = &mut self.priority_slots[registration.priority as usize];
        registration.priority_slot = *slot;
        *slot = slot
            .checked_add(1)
            .expect("FIB priority slot space exhausted");
        self.registrations[index] = Some(registration);
        source
    }

    pub fn allocate(&mut self, registration: FibSourceRegistration) -> FibSource {
        if self.next_source == u8::MAX {
            return FibSource::INVALID;
        }
        let source = FibSource::new(self.next_source);
        self.next_source += 1;
        self.register(source, registration)
    }

    pub fn registration(&self, source: FibSource) -> FibSourceRegistration {
        self.registrations
            .get(usize::from(source.get()))
            .and_then(|entry| *entry)
            .expect("FIB source must be registered before a route is added")
    }
}

#[derive(hammer_component_macros::FibSource)]
#[fib_source(source = FibSource::INTERFACE, name = "interface", priority = 0x03, behavior = FibEntrySourceBehaviorId::INTERFACE)]
struct InterfaceSourceRegistration;

#[derive(hammer_component_macros::FibSource)]
#[fib_source(source = FibSource::API, name = "api", priority = 0x80, behavior = FibEntrySourceBehaviorId::API)]
struct ApiSourceRegistration;

#[derive(hammer_component_macros::FibSource)]
#[fib_source(source = FibSource::ADJACENCY, name = "adjacency", priority = 0xd0, behavior = FibEntrySourceBehaviorId::ADJACENCY)]
struct AdjacencySourceRegistration;

#[derive(hammer_component_macros::FibSource)]
#[fib_source(source = FibSource::ATTACHED_EXPORT, name = "attached-export", priority = 0xf0, behavior = FibEntrySourceBehaviorId::SIMPLE)]
struct AttachedExportSourceRegistration;

pub fn register_fib_sources(main: &mut FibSourceMain) {
    InterfaceSourceRegistration::register_fib_source(main);
    ApiSourceRegistration::register_fib_source(main);
    AdjacencySourceRegistration::register_fib_source(main);
    AttachedExportSourceRegistration::register_fib_source(main);
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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FibPathMode {
    AttachedNextHop = 0,
    Attached = 1,
    Special = 3,
    Receive = 8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FibRoutePath<P, N, F> {
    pub connected: Option<P>,
    pub sw_if_index: u32,
    pub table_id: u32,
    pub rpf_id: u32,
    pub weight: u8,
    pub preference: u8,
    pub flags: F,
    pub next_hop: N,
}

pub struct FibPath<P, N, F> {
    node: FibNode,
    path_list_index: u32,
    sibling: Option<FibNodeSibling>,
    forwarding: Option<DpoId>,
    resolved: bool,
    mode: FibPathMode,
    route: FibRoutePath<P, N, F>,
}

impl<P: Clone, N: Clone, F: Clone> FibPath<P, N, F> {
    fn copy_configuration(&self, node_type: FibNodeType, path_list_index: u32) -> Self {
        Self {
            node: FibNode::new(node_type),
            path_list_index,
            sibling: None,
            forwarding: None,
            resolved: false,
            mode: self.mode,
            route: self.route.clone(),
        }
    }
}

pub struct FibPathExt<P, N, F, E> {
    pub route: FibRoutePath<P, N, F>,
    pub path_index: Option<u32>,
    pub data: E,
}

pub struct FibPathExtList<P, N, F, E> {
    entries: Vec<FibPathExt<P, N, F, E>>,
}

impl<P, N, F, E> Default for FibPathExtList<P, N, F, E> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<P, N, F, E> FibPathExtList<P, N, F, E> {
    pub fn insert(&mut self, extension: FibPathExt<P, N, F, E>) {
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

    pub fn remove(&mut self, path_index: u32) -> Option<FibPathExt<P, N, F, E>> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.path_index == Some(path_index))?;
        Some(self.entries.remove(position))
    }

    pub fn find(&self, path_index: u32) -> Option<&FibPathExt<P, N, F, E>> {
        self.entries
            .iter()
            .find(|entry| entry.path_index == Some(path_index))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &FibPathExt<P, N, F, E>> {
        self.entries.iter()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

pub struct FibEntrySrc<S, P, N, F, E> {
    pub path_exts: FibPathExtList<P, N, F, E>,
    pub path_list: Option<u32>,
    pub entry_flags: FibEntryFlags,
    pub source: FibSource,
    pub flags: FibEntrySrcFlags,
    pub ref_count: u8,
    pub source_state: S,
}

impl<S, P, N, F, E> FibEntrySrc<S, P, N, F, E> {
    pub fn new(source: FibSource, source_state: S) -> Self {
        Self {
            path_exts: FibPathExtList::default(),
            path_list: None,
            entry_flags: FibEntryFlags::empty(),
            source,
            flags: FibEntrySrcFlags::ADDED,
            ref_count: 1,
            source_state,
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
pub struct FibPathList {
    node: FibNode,
    paths: Vec<u32>,
    key_flags: FibPathListFlags,
    flags: FibPathListFlags,
    urpf_index: Option<u32>,
}

impl FibPathList {
    pub fn new(node_type: FibNodeType, paths: Vec<u32>, key_flags: FibPathListFlags) -> Self {
        Self {
            node: FibNode::new(node_type),
            paths,
            key_flags,
            flags: key_flags,
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

impl Drop for FibPathList {
    fn drop(&mut self) {
        self.node.assert_detached();
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
                Ok(source_removed)
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
                .sort_by_key(|(candidate, _)| {
                    let registration = super::NetMain::global()
                        .expect("FIB source requires network Main")
                        .fib_source(*candidate);
                    (registration.priority, registration.priority_slot)
                });
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
            return Ok(false);
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
    #[error("FIB backend mutation failed")]
    Backend(#[source] E),
    #[error("FIB entry count {count} exceeds the index representation")]
    EntryIndexOverflow { count: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_priority_slots_are_unique_within_a_class() {
        let mut sources = FibSourceMain::new();
        let registration = FibSourceRegistration::new("route", 0x80, FibEntrySourceBehaviorId::API);
        let earlier = sources.allocate(registration);
        let later = sources.allocate(registration);
        assert_eq!((earlier.get(), later.get()), (22, 23));
        assert_eq!(sources.registration(earlier).priority_slot, 0);
        assert_eq!(sources.registration(later).priority_slot, 1);
        sources.register(earlier, registration);
        assert_eq!(sources.registration(earlier).priority_slot, 2);
    }

    #[test]
    fn source_record_retains_references_and_replaces_extensions() {
        let source = FibSource::API;
        let mut record = FibEntrySrc::<u32, (), u8, u16, u16>::new(source, 7);
        assert_eq!(record.ref_count, 1);
        assert!(record.add_reference());
        assert_eq!(record.ref_count, 2);
        assert!(record.remove_reference());
        assert_eq!(record.ref_count, 1);
        let route = FibRoutePath {
            connected: None,
            sw_if_index: 3,
            table_id: 4,
            rpf_id: 5,
            weight: 1,
            preference: 0,
            flags: 0,
            next_hop: 10,
        };
        record.path_exts.insert(FibPathExt {
            route: route.clone(),
            path_index: Some(9),
            data: 1,
        });
        record.path_exts.insert(FibPathExt {
            route,
            path_index: Some(9),
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
