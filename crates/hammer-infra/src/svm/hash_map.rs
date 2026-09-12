//! Offset-based hash map for SVM region heaps.
//!
//! Semantics mirror `std::collections::HashMap`: the same replacement,
//! removal, counting, capacity, and iteration behavior. The difference is the
//! storage: slots and key bytes live in a [`SvmRegionHeap`], the table owns its
//! key bytes, and every stored location is an offset, so the descriptor can sit
//! in shared memory (ADR-0011 section 12.5).
//!
//! The descriptor never holds a process-private pointer to the bytes it
//! manages, so callers pass the current mapping view alongside it, exactly as
//! [`SvmRegionHeap`] does.
//!
//! Failure is not recoverable. Heap exhaustion terminates with a
//! `SvmRegionHeapViolation`; a capacity that cannot be represented, an
//! impossible slot state, and a misaligned bucket array panic with the
//! offending identity.

use std::alloc::Layout;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::slice::{Iter, IterMut};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::svm::region_heap::SvmRegionHeap;

/// Smallest bucket array: the table never shrinks below this.
pub const SVM_HASH_MAP_MIN_BUCKETS: u64 = 64;
/// Smallest element count that may trigger an automatic shrink.
const SHRINK_FLOOR: u64 = 32;
/// Growth starts when `occupied + vacant` reaches this fraction of the buckets.
const LOAD_NUMERATOR: u64 = 3;
const LOAD_DENOMINATOR: u64 = 4;

const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const SLOT_VACANT: u8 = 2;

/// One bucket.
///
/// `state` stays a raw byte instead of a Rust enum because the slot array is
/// cast from shared bytes, where only a type whose every bit pattern is valid
/// may be materialized; the map rejects any other value with the slot identity.
#[repr(C)]
#[derive(Clone, Copy)]
struct SvmSlot<V> {
    state: u8,
    padding: [u8; 7],
    hash: u64,
    name_offset: u64,
    name_len: u64,
    value: V,
}

/// Offset-based table mapping names to `V`.
///
/// The descriptor is a field of its owner (the root region's `SvmRegionMain`),
/// never part of the storage it manages, and has no destructor: releasing the
/// table means `clear` plus freeing the bucket array by its owner, because any
/// single process dropping it would corrupt the other processes mapping the
/// same region.
#[repr(C)]
pub struct SvmHashMap<V> {
    buckets_offset: u64,
    bucket_capacity: u64,
    occupied: u64,
    vacant: u64,
    values: PhantomData<V>,
}

impl<V> SvmHashMap<V> {
    /// Creates an empty descriptor without a bucket array.
    pub const fn new() -> Self {
        Self {
            buckets_offset: 0,
            bucket_capacity: 0,
            occupied: 0,
            vacant: 0,
            values: PhantomData,
        }
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.occupied as usize
    }

    /// Reports whether the table holds no entry.
    pub fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// Entries the table holds before it grows its bucket array.
    pub fn capacity(&self) -> usize {
        (self.bucket_capacity * LOAD_NUMERATOR / LOAD_DENOMINATOR) as usize
    }

    /// Element count at which the table grows.
    fn fill_limit(&self) -> u64 {
        self.bucket_capacity * LOAD_NUMERATOR / LOAD_DENOMINATOR
    }
}

impl<V> Default for SvmHashMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> SvmHashMap<V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    /// Creates an empty table whose bucket array can hold `capacity` entries
    /// without growing.
    pub fn with_capacity(heap: &mut SvmRegionHeap, arena: &mut [u8], capacity: usize) -> Self {
        let mut map = Self::new();
        map.reserve(heap, arena, capacity);
        map
    }

    /// Reports whether `name` is stored.
    pub fn contains_key(&self, heap: &SvmRegionHeap, arena: &[u8], name: &str) -> bool {
        self.find_occupied(heap, arena, name).is_some()
    }

    /// Borrows the value stored for `name`.
    pub fn get<'a>(&self, heap: &SvmRegionHeap, arena: &'a [u8], name: &str) -> Option<&'a V> {
        let index = self.find_occupied(heap, arena, name)?;
        Some(&self.slot_at(heap, arena, index).value)
    }

    /// Mutably borrows the value stored for `name`.
    pub fn get_mut<'a>(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &'a mut [u8],
        name: &str,
    ) -> Option<&'a mut V> {
        let index = self.find_occupied(heap, arena, name)?;
        Some(&mut self.slot_at_mut(heap, arena, index).value)
    }

    /// Borrows the stored key and value for `name`.
    pub fn get_key_value<'a>(
        &self,
        heap: &SvmRegionHeap,
        arena: &'a [u8],
        name: &str,
    ) -> Option<(&'a str, &'a V)> {
        let index = self.find_occupied(heap, arena, name)?;
        let slot = self.slot_at(heap, arena, index);
        let key = name_at(heap, arena, slot.name_offset, slot.name_len);
        Some((key, &slot.value))
    }

    /// Stores `value` under `name`, returning the value it replaced.
    ///
    /// A stored key is replaced in place, so a repeat `insert` neither
    /// allocates nor changes the iteration set.
    pub fn insert(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &mut [u8],
        name: &str,
        value: V,
    ) -> Option<V> {
        if let Some(index) = self.find_occupied(heap, arena, name) {
            let slot = self.slot_at_mut(heap, arena, index);
            return Some(std::mem::replace(&mut slot.value, value));
        }
        self.insert_new(heap, arena, name, value);
        None
    }

    /// Removes the entry stored under `name`, returning its value.
    pub fn remove(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8], name: &str) -> Option<V> {
        let index = self.find_occupied(heap, arena, name)?;
        Some(self.remove_at(heap, arena, index))
    }

    /// Returns the entry for `name`, which is vacant when the name is absent.
    pub fn entry<'a>(
        &'a mut self,
        heap: &'a mut SvmRegionHeap,
        arena: &'a mut [u8],
        name: &'a str,
    ) -> SvmEntry<'a, V> {
        match self.find_occupied(heap, arena, name) {
            Some(index) => SvmEntry::Occupied(SvmOccupiedEntry {
                map: self,
                heap,
                arena,
                index,
                name,
            }),
            None => SvmEntry::Vacant(SvmVacantEntry {
                map: self,
                heap,
                arena,
                name,
            }),
        }
    }

    /// Removes every entry and releases every key block; the bucket array stays
    /// allocated, exactly as `HashMap::clear` keeps its capacity.
    pub fn clear(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8]) {
        if self.buckets_offset == 0 {
            self.occupied = 0;
            self.vacant = 0;
            return;
        }
        for index in 0..self.bucket_capacity {
            let (name_offset, name_len) = {
                let slot = self.slot_at(heap, arena, index);
                require_slot_state(slot, index);
                (slot.name_offset, slot.name_len)
            };
            if name_offset != 0 {
                heap.deallocate(arena, name_offset, key_layout(name_len));
            }
        }
        let length = bucket_array_bytes::<V>(self.bucket_capacity) as u64;
        heap.bytes_at_mut(arena, self.buckets_offset, length)
            .fill(0);
        self.occupied = 0;
        self.vacant = 0;
    }

    /// Makes room for `additional` more entries without growing again.
    pub fn reserve(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8], additional: usize) {
        let additional = additional as u64;
        let required = self.occupied.checked_add(additional).unwrap_or_else(|| {
            panic!(
                "SVM hash map cannot reserve {additional} more entries on top of {}",
                self.occupied
            )
        });
        let buckets = buckets_for(required);
        if buckets > self.bucket_capacity {
            self.rehash(heap, arena, buckets);
        } else if self.occupied + self.vacant + additional > self.fill_limit() {
            self.rehash(heap, arena, self.bucket_capacity);
        }
    }

    /// Shrinks the bucket array to the smallest one that still holds every entry.
    pub fn shrink_to_fit(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8]) {
        let buckets = buckets_for(self.occupied);
        if self.buckets_offset != 0 && buckets < self.bucket_capacity {
            self.rehash(heap, arena, buckets);
        }
    }

    /// Iterates over every stored key and value.
    pub fn iter<'a>(&self, heap: &'a SvmRegionHeap, arena: &'a [u8]) -> SvmIter<'a, V> {
        SvmIter {
            heap,
            arena,
            slots: self.slots(heap, arena).iter(),
            index: 0,
        }
    }

    /// Iterates over every stored key.
    pub fn keys<'a>(&self, heap: &'a SvmRegionHeap, arena: &'a [u8]) -> SvmKeys<'a, V> {
        SvmKeys {
            heap,
            arena,
            slots: self.slots(heap, arena).iter(),
            index: 0,
        }
    }

    /// Iterates over every stored value.
    pub fn values<'a>(&self, heap: &SvmRegionHeap, arena: &'a [u8]) -> SvmValues<'a, V> {
        SvmValues {
            slots: self.slots(heap, arena).iter(),
            index: 0,
        }
    }

    /// Mutably iterates over every stored value.
    pub fn values_mut<'a>(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &'a mut [u8],
    ) -> SvmValuesMut<'a, V> {
        SvmValuesMut {
            slots: self.slots_mut(heap, arena).iter_mut(),
            index: 0,
        }
    }

    /// Stores `value` under a name that is known to be absent.
    fn insert_new<'a>(
        &mut self,
        heap: &mut SvmRegionHeap,
        arena: &'a mut [u8],
        name: &str,
        value: V,
    ) -> &'a mut V {
        self.grow_for_insert(heap, arena);
        let index = self.find_insert_slot(heap, arena, name);
        let name_offset = allocate_key(heap, arena, name);
        let hash = hash_name(name);
        let reused_vacant = self.slot_at(heap, arena, index).state == SLOT_VACANT;
        let length = name.len() as u64;
        let slot = self.slot_at_mut(heap, arena, index);
        *slot = SvmSlot {
            state: SLOT_OCCUPIED,
            padding: [0; 7],
            hash,
            name_offset,
            name_len: length,
            value,
        };
        if reused_vacant {
            self.vacant -= 1;
        }
        self.occupied += 1;
        &mut self.slot_at_mut(heap, arena, index).value
    }

    /// Removes the entry in `index`, which is known to be occupied.
    fn remove_at(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8], index: u64) -> V {
        let (value, name_offset, name_len) = {
            let slot = self.slot_at(heap, arena, index);
            (slot.value, slot.name_offset, slot.name_len)
        };
        heap.deallocate(arena, name_offset, key_layout(name_len));
        let slot = self.slot_at_mut(heap, arena, index);
        slot.state = SLOT_VACANT;
        slot.name_offset = 0;
        slot.name_len = 0;
        self.occupied -= 1;
        self.vacant += 1;
        self.shrink_after_remove(heap, arena);
        value
    }

    /// Grows or compacts the bucket array before one more entry is stored.
    ///
    /// Tombstones are reclaimed in place first. `capacity` counts entries, so a
    /// table whose live entries still fit must not allocate a larger array just
    /// because it once held more; the array doubles only when the live entries
    /// themselves reach the load limit.
    fn grow_for_insert(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8]) {
        if self.buckets_offset == 0 {
            self.rehash(heap, arena, SVM_HASH_MAP_MIN_BUCKETS);
            return;
        }
        if self.occupied + self.vacant < self.fill_limit() {
            return;
        }
        let buckets = if self.occupied < self.fill_limit() {
            self.bucket_capacity
        } else {
            self.bucket_capacity * 2
        };
        self.rehash(heap, arena, buckets);
    }

    /// Shrinks after a removal, matching the `HashMap` growth policy in reverse.
    fn shrink_after_remove(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8]) {
        if self.occupied <= SHRINK_FLOOR || self.occupied * LOAD_DENOMINATOR >= self.bucket_capacity
        {
            return;
        }
        let buckets = buckets_for(self.occupied);
        if buckets < self.bucket_capacity {
            self.rehash(heap, arena, buckets);
        }
    }

    /// Rebuilds the table into a bucket array of `bucket_capacity`, dropping
    /// tombstones; the key blocks stay where they are, so only the array moves.
    fn rehash(&mut self, heap: &mut SvmRegionHeap, arena: &mut [u8], bucket_capacity: u64) {
        let old_offset = self.buckets_offset;
        let old_capacity = self.bucket_capacity;
        let new_offset = allocate_bucket_array::<V>(heap, arena, bucket_capacity);
        if old_offset != 0 {
            let (old_slots, new_slots) = split_bucket_arrays::<V>(
                arena,
                (old_offset, old_capacity),
                (new_offset, bucket_capacity),
            );
            for (index, slot) in old_slots.iter().enumerate() {
                if slot.state == SLOT_OCCUPIED {
                    let bucket = empty_bucket_index(new_slots, slot.hash, bucket_capacity);
                    new_slots[bucket] = *slot;
                } else {
                    require_slot_state(slot, index as u64);
                }
            }
            heap.deallocate(arena, old_offset, bucket_array_layout::<V>(old_capacity));
        }
        self.buckets_offset = new_offset;
        self.bucket_capacity = bucket_capacity;
        self.vacant = 0;
    }

    /// Finds the bucket holding `name`, following the linear probe until an
    /// empty bucket ends the search.
    fn find_occupied(&self, heap: &SvmRegionHeap, arena: &[u8], name: &str) -> Option<u64> {
        let capacity = self.bucket_capacity;
        if capacity == 0 {
            return None;
        }
        let slots = self.slots(heap, arena);
        let hash = hash_name(name);
        let mut index = probe_start(hash, capacity);
        for _ in 0..capacity {
            let slot = &slots[index as usize];
            match slot.state {
                SLOT_EMPTY => return None,
                SLOT_OCCUPIED => {
                    if slot.hash == hash && self.slot_holds_name(heap, arena, slot, name) {
                        return Some(index);
                    }
                }
                SLOT_VACANT => {}
                state => {
                    panic!("SVM hash map slot {index} of {capacity} has unknown state {state}")
                }
            }
            index = advance_probe(index, capacity);
        }
        None
    }

    /// Finds the bucket a new entry named `name` must use: the bucket holding
    /// the name, otherwise the first tombstone, otherwise the terminating
    /// empty bucket.
    fn find_insert_slot(&self, heap: &SvmRegionHeap, arena: &[u8], name: &str) -> u64 {
        let capacity = self.bucket_capacity;
        let slots = self.slots(heap, arena);
        let hash = hash_name(name);
        let mut index = probe_start(hash, capacity);
        let mut first_vacant = None;
        for _ in 0..capacity {
            let slot = &slots[index as usize];
            match slot.state {
                SLOT_EMPTY => return first_vacant.unwrap_or(index),
                SLOT_OCCUPIED => {
                    if slot.hash == hash && self.slot_holds_name(heap, arena, slot, name) {
                        return index;
                    }
                }
                SLOT_VACANT => {
                    if first_vacant.is_none() {
                        first_vacant = Some(index);
                    }
                }
                state => {
                    panic!("SVM hash map slot {index} of {capacity} has unknown state {state}")
                }
            }
            index = advance_probe(index, capacity);
        }
        first_vacant.unwrap_or_else(|| {
            panic!(
                "SVM hash map of {capacity} buckets holds {} entries and no empty bucket",
                self.occupied
            )
        })
    }

    /// Compares a stored key with `name` by length and bytes.
    fn slot_holds_name(
        &self,
        heap: &SvmRegionHeap,
        arena: &[u8],
        slot: &SvmSlot<V>,
        name: &str,
    ) -> bool {
        if slot.name_len as usize != name.len() {
            return false;
        }
        heap.bytes_at(arena, slot.name_offset, slot.name_len) == name.as_bytes()
    }

    fn slots<'a>(&self, heap: &SvmRegionHeap, arena: &'a [u8]) -> &'a [SvmSlot<V>] {
        if self.buckets_offset == 0 {
            return &[];
        }
        let length = bucket_array_bytes::<V>(self.bucket_capacity);
        let bytes = heap.bytes_at(arena, self.buckets_offset, length as u64);
        array_from_bytes(bytes, self.buckets_offset, self.bucket_capacity)
    }

    fn slots_mut<'a>(&self, heap: &mut SvmRegionHeap, arena: &'a mut [u8]) -> &'a mut [SvmSlot<V>] {
        if self.buckets_offset == 0 {
            return &mut [];
        }
        let length = bucket_array_bytes::<V>(self.bucket_capacity);
        let bytes = heap.bytes_at_mut(arena, self.buckets_offset, length as u64);
        array_from_bytes_mut(bytes, self.buckets_offset, self.bucket_capacity)
    }

    fn slot_at<'a>(&self, heap: &SvmRegionHeap, arena: &'a [u8], index: u64) -> &'a SvmSlot<V> {
        &self.slots(heap, arena)[index as usize]
    }

    fn slot_at_mut<'a>(
        &self,
        heap: &mut SvmRegionHeap,
        arena: &'a mut [u8],
        index: u64,
    ) -> &'a mut SvmSlot<V> {
        &mut self.slots_mut(heap, arena)[index as usize]
    }
}

/// Entry in a [`SvmHashMap`], either occupied by a stored key or vacant.
pub enum SvmEntry<'a, V> {
    /// The key is already stored.
    Occupied(SvmOccupiedEntry<'a, V>),
    /// The key is absent.
    Vacant(SvmVacantEntry<'a, V>),
}

impl<'a, V> SvmEntry<'a, V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    /// Borrows the key this entry was looked up with.
    pub fn key(&self) -> &str {
        match self {
            Self::Occupied(entry) => entry.key(),
            Self::Vacant(entry) => entry.key(),
        }
    }

    /// Returns the stored value, inserting `value` when the key is absent.
    pub fn or_insert(self, value: V) -> &'a mut V {
        match self {
            Self::Occupied(entry) => entry.into_mut(),
            Self::Vacant(entry) => entry.insert(value),
        }
    }
}

/// Entry for a key that is already stored.
pub struct SvmOccupiedEntry<'a, V> {
    map: &'a mut SvmHashMap<V>,
    heap: &'a mut SvmRegionHeap,
    arena: &'a mut [u8],
    index: u64,
    name: &'a str,
}

impl<'a, V> SvmOccupiedEntry<'a, V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    /// Borrows the stored key.
    pub fn key(&self) -> &str {
        let (name_offset, name_len) = {
            let slot = self.map.slot_at(self.heap, self.arena, self.index);
            (slot.name_offset, slot.name_len)
        };
        name_at(self.heap, self.arena, name_offset, name_len)
    }

    /// Borrows the stored value.
    pub fn get(&self) -> &V {
        &self.map.slot_at(self.heap, self.arena, self.index).value
    }

    /// Mutably borrows the stored value for the duration of this borrow.
    pub fn get_mut(&mut self) -> &mut V {
        &mut self
            .map
            .slot_at_mut(self.heap, self.arena, self.index)
            .value
    }

    /// Mutably borrows the stored value for the lifetime of the region bytes.
    pub fn into_mut(self) -> &'a mut V {
        let Self {
            map,
            heap,
            arena,
            index,
            ..
        } = self;
        &mut map.slot_at_mut(heap, arena, index).value
    }

    /// Replaces the stored value, returning the previous one.
    pub fn insert(&mut self, value: V) -> V {
        let slot = self.map.slot_at_mut(self.heap, self.arena, self.index);
        std::mem::replace(&mut slot.value, value)
    }

    /// Removes the entry and returns its value.
    pub fn remove(self) -> V {
        let Self {
            map,
            heap,
            arena,
            index,
            name,
        } = self;
        let _ = name;
        map.remove_at(heap, arena, index)
    }
}

/// Entry for a key that is absent from the table.
///
/// The handle owns nothing until [`Self::insert`] runs, so a vacant entry that
/// is dropped without inserting leaves no allocation behind.
pub struct SvmVacantEntry<'a, V> {
    map: &'a mut SvmHashMap<V>,
    heap: &'a mut SvmRegionHeap,
    arena: &'a mut [u8],
    name: &'a str,
}

impl<'a, V> SvmVacantEntry<'a, V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    /// Borrows the absent key.
    pub fn key(&self) -> &str {
        self.name
    }

    /// Stores `value` under this key and returns a borrow of it.
    pub fn insert(self, value: V) -> &'a mut V {
        let Self {
            map,
            heap,
            arena,
            name,
        } = self;
        map.insert_new(heap, arena, name, value)
    }
}

/// Iterator over the stored keys and values.
pub struct SvmIter<'a, V> {
    heap: &'a SvmRegionHeap,
    arena: &'a [u8],
    slots: Iter<'a, SvmSlot<V>>,
    index: u64,
}

impl<'a, V> Iterator for SvmIter<'a, V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    type Item = (&'a str, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let slot = self.slots.next()?;
            let bucket = self.index;
            self.index += 1;
            if slot.state == SLOT_OCCUPIED {
                let key = name_at(self.heap, self.arena, slot.name_offset, slot.name_len);
                return Some((key, &slot.value));
            }
            require_slot_state(slot, bucket);
        }
    }
}

/// Iterator over the stored keys.
pub struct SvmKeys<'a, V> {
    heap: &'a SvmRegionHeap,
    arena: &'a [u8],
    slots: Iter<'a, SvmSlot<V>>,
    index: u64,
}

impl<'a, V> Iterator for SvmKeys<'a, V>
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let slot = self.slots.next()?;
            let bucket = self.index;
            self.index += 1;
            if slot.state == SLOT_OCCUPIED {
                return Some(name_at(
                    self.heap,
                    self.arena,
                    slot.name_offset,
                    slot.name_len,
                ));
            }
            require_slot_state(slot, bucket);
        }
    }
}

/// Iterator over the stored values.
pub struct SvmValues<'a, V> {
    slots: Iter<'a, SvmSlot<V>>,
    index: u64,
}

impl<'a, V> Iterator for SvmValues<'a, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let slot = self.slots.next()?;
            let bucket = self.index;
            self.index += 1;
            if slot.state == SLOT_OCCUPIED {
                return Some(&slot.value);
            }
            require_slot_state(slot, bucket);
        }
    }
}

/// Mutable iterator over the stored values.
pub struct SvmValuesMut<'a, V> {
    slots: IterMut<'a, SvmSlot<V>>,
    index: u64,
}

impl<'a, V> Iterator for SvmValuesMut<'a, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let slot = self.slots.next()?;
            let bucket = self.index;
            self.index += 1;
            if slot.state == SLOT_OCCUPIED {
                return Some(&mut slot.value);
            }
            require_slot_state(slot, bucket);
        }
    }
}

/// Rejects a bucket whose state byte is not one the map writes.
fn require_slot_state<V>(slot: &SvmSlot<V>, index: u64) {
    match slot.state {
        SLOT_EMPTY | SLOT_OCCUPIED | SLOT_VACANT => {}
        state => panic!("SVM hash map slot {index} has unknown state {state}"),
    }
}

/// Fixed-seed, cross-process-stable hash of a name.
///
/// `RandomState` is unusable here: its per-process seed would make another
/// process probe different buckets for the same name.
fn hash_name(name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

/// First bucket probed for `hash`.
fn probe_start(hash: u64, bucket_capacity: u64) -> u64 {
    hash & (bucket_capacity - 1)
}

/// Next bucket in the linear probe.
fn advance_probe(index: u64, bucket_capacity: u64) -> u64 {
    (index + 1) & (bucket_capacity - 1)
}

/// Smallest bucket count that holds `entries` entries at the load factor.
fn buckets_for(entries: u64) -> u64 {
    let needed = entries
        .checked_mul(LOAD_DENOMINATOR)
        .unwrap_or_else(|| panic!("SVM hash map cannot size a table for {entries} entries"))
        .div_ceil(LOAD_NUMERATOR);
    let mut buckets = SVM_HASH_MAP_MIN_BUCKETS;
    while buckets < needed {
        buckets = buckets
            .checked_mul(2)
            .unwrap_or_else(|| panic!("SVM hash map cannot size a table for {entries} entries"));
    }
    buckets
}

/// Bytes of a bucket array holding `capacity` slots.
fn bucket_array_bytes<V>(capacity: u64) -> usize {
    let capacity = usize::try_from(capacity).unwrap_or_else(|_| {
        panic!("SVM hash map bucket capacity {capacity} does not fit this address space")
    });
    let slot = size_of::<SvmSlot<V>>();
    capacity
        .checked_mul(slot)
        .unwrap_or_else(|| panic!("SVM hash map bucket array of {capacity} slots overflows"))
}

/// Layout of a bucket array holding `capacity` slots.
fn bucket_array_layout<V>(capacity: u64) -> Layout {
    Layout::from_size_align(bucket_array_bytes::<V>(capacity), align_of::<SvmSlot<V>>())
        .unwrap_or_else(|_| {
            panic!("SVM hash map bucket array layout for {capacity} buckets is invalid")
        })
}

/// Layout of a key block holding `length` name bytes.
fn key_layout(length: u64) -> Layout {
    Layout::from_size_align(length as usize, 1)
        .unwrap_or_else(|_| panic!("SVM hash map key layout for {length} bytes is invalid"))
}

/// Reinterprets the bytes of a bucket array as slots.
fn array_from_bytes<V>(bytes: &[u8], offset: u64, capacity: u64) -> &[SvmSlot<V>] {
    let alignment = align_of::<SvmSlot<V>>();
    if !(bytes.as_ptr() as usize).is_multiple_of(alignment) {
        panic!("SVM hash map bucket array at {offset} is not aligned to {alignment}");
    }
    // SAFETY: the heap returned the live bounds of an array the map allocated
    // with `align_of::<SvmSlot<V>>()`, and every bit pattern of `SvmSlot<V>` is
    // valid because `V: FromBytes` and the remaining fields are integers.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<SvmSlot<V>>(), capacity as usize) }
}

/// Mutable form of [`array_from_bytes`].
fn array_from_bytes_mut<V>(bytes: &mut [u8], offset: u64, capacity: u64) -> &mut [SvmSlot<V>] {
    let alignment = align_of::<SvmSlot<V>>();
    if !(bytes.as_ptr() as usize).is_multiple_of(alignment) {
        panic!("SVM hash map bucket array at {offset} is not aligned to {alignment}");
    }
    // SAFETY: see `array_from_bytes`; the borrow is exclusive because the heap
    // handed out a mutable view of this live array.
    unsafe {
        std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast::<SvmSlot<V>>(), capacity as usize)
    }
}

/// Splits the arena into the bytes of the old bucket array and of the new one,
/// which the heap allocated as two disjoint live blocks.
fn split_bucket_arrays<V>(
    arena: &mut [u8],
    old: (u64, u64),
    new: (u64, u64),
) -> (&[SvmSlot<V>], &mut [SvmSlot<V>]) {
    let (old_start, old_capacity) = (old.0 as usize, old.1);
    let (new_start, new_capacity) = (new.0 as usize, new.1);
    let old_length = bucket_array_bytes::<V>(old.1);
    let new_length = bucket_array_bytes::<V>(new.1);
    if old_start > new_start {
        let (head, tail) = arena.split_at_mut(old_start);
        let new_bytes = &mut head[new_start..new_start + new_length];
        let old_bytes = &tail[..old_length];
        return (
            array_from_bytes::<V>(old_bytes, old.0, old_capacity),
            array_from_bytes_mut::<V>(new_bytes, new.0, new_capacity),
        );
    }
    let (head, tail) = arena.split_at_mut(new_start);
    let old_bytes = &head[old_start..old_start + old_length];
    let new_bytes = &mut tail[..new_length];
    (
        array_from_bytes::<V>(old_bytes, old.0, old_capacity),
        array_from_bytes_mut::<V>(new_bytes, new.0, new_capacity),
    )
}

/// Allocates a zeroed bucket array and returns its offset.
fn allocate_bucket_array<V>(heap: &mut SvmRegionHeap, arena: &mut [u8], bucket_capacity: u64) -> u64
where
    V: Copy + FromBytes + IntoBytes + Immutable + KnownLayout,
{
    heap.allocate(arena, bucket_array_layout::<V>(bucket_capacity))
}

/// Allocates a zeroed key block holding `name` and returns its offset.
fn allocate_key(heap: &mut SvmRegionHeap, arena: &mut [u8], name: &str) -> u64 {
    let offset = heap.allocate(arena, key_layout(name.len() as u64));
    heap.bytes_at_mut(arena, offset, name.len() as u64)
        .copy_from_slice(name.as_bytes());
    offset
}

/// Finds the bucket for `hash` in a tombstone-free array.
fn empty_bucket_index<V>(slots: &[SvmSlot<V>], hash: u64, bucket_capacity: u64) -> usize {
    let mut index = probe_start(hash, bucket_capacity);
    for _ in 0..bucket_capacity {
        if slots[index as usize].state == SLOT_EMPTY {
            return index as usize;
        }
        index = advance_probe(index, bucket_capacity);
    }
    panic!("SVM hash map of {bucket_capacity} buckets has no empty bucket to rehash into")
}

/// Borrows a stored key as a string.
fn name_at<'a>(heap: &SvmRegionHeap, arena: &'a [u8], offset: u64, length: u64) -> &'a str {
    let bytes = heap.bytes_at(arena, offset, length);
    std::str::from_utf8(bytes).unwrap_or_else(|_| {
        panic!("SVM hash map key at offset {offset} of {length} bytes is not UTF-8")
    })
}
