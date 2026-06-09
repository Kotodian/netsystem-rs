use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;

use crate::vec::Vec;

pub struct Descriptor<Tag> {
    raw: u64,
    _marker: PhantomData<fn() -> Tag>,
}

impl<Tag> Copy for Descriptor<Tag> {}

impl<Tag> Clone for Descriptor<Tag> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> PartialEq for Descriptor<Tag> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<Tag> Eq for Descriptor<Tag> {}

impl<Tag> Hash for Descriptor<Tag> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

impl<Tag> Descriptor<Tag> {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn from_parts(slot: u32, generation: u32) -> Self {
        Self::new(((generation as u64) << 32) | (slot as u64))
    }

    #[inline]
    pub const fn value(self) -> u64 {
        self.raw
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.raw as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.raw >> 32) as u32
    }
}

impl<Tag> fmt::Debug for Descriptor<Tag> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Descriptor")
            .field("raw", &self.raw)
            .field("slot", &self.slot())
            .field("generation", &self.generation())
            .finish()
    }
}

#[derive(Debug)]
struct DescriptorSlot<T> {
    generation: u32,
    value: Option<T>,
}

impl<T> DescriptorSlot<T> {
    #[inline]
    const fn empty() -> Self {
        Self {
            generation: 0,
            value: None,
        }
    }
}

pub struct DescriptorTable<T, Tag> {
    slots: Vec<DescriptorSlot<T>>,
    free: Vec<u32>,
    len: usize,
    _marker: PhantomData<fn() -> Tag>,
}

impl<T, Tag> DescriptorTable<T, Tag> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut table = Self::new();
        table.slots.reserve(capacity);
        table.free.reserve(capacity);
        table
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn insert(&mut self, value: T) -> Descriptor<Tag> {
        let slot = self
            .take_free_slot()
            .unwrap_or_else(|| self.push_new_slot()) as usize;
        let entry = &mut self.slots[slot];
        entry.generation = entry.generation.wrapping_add(1).max(1);
        entry.value = Some(value);
        self.len += 1;
        Descriptor::from_parts(slot as u32, entry.generation)
    }

    #[inline]
    pub fn get(&self, descriptor: Descriptor<Tag>) -> Option<&T> {
        self.validate(descriptor)
            .and_then(|slot| self.slots[slot].value.as_ref())
    }

    #[inline]
    pub fn get_mut(&mut self, descriptor: Descriptor<Tag>) -> Option<&mut T> {
        self.validate(descriptor)
            .and_then(|slot| self.slots[slot].value.as_mut())
    }

    #[inline]
    pub fn contains(&self, descriptor: Descriptor<Tag>) -> bool {
        self.validate(descriptor).is_some()
    }

    #[inline]
    pub fn remove(&mut self, descriptor: Descriptor<Tag>) -> Option<T> {
        let slot = self.validate(descriptor)?;
        let value = self.slots[slot].value.take()?;
        self.free.push(slot as u32);
        self.len -= 1;
        Some(value)
    }

    #[inline]
    fn validate(&self, descriptor: Descriptor<Tag>) -> Option<usize> {
        let slot = descriptor.slot() as usize;
        let entry = self.slots.get(slot)?;
        (entry.generation == descriptor.generation() && entry.value.is_some()).then_some(slot)
    }

    #[inline]
    fn push_new_slot(&mut self) -> u32 {
        let slot = u32::try_from(self.slots.len()).expect("descriptor slot index fits u32");
        self.slots.push(DescriptorSlot::empty());
        slot
    }

    #[inline]
    fn take_free_slot(&mut self) -> Option<u32> {
        let mut best_index = None;
        let mut best_slot = 0;
        for (index, slot) in self.free.iter().copied().enumerate() {
            if best_index.is_none() || slot < best_slot {
                best_index = Some(index);
                best_slot = slot;
            }
        }
        let index = best_index?;
        Some(
            self.free
                .drain(index..index + 1)
                .next()
                .expect("free slot exists"),
        )
    }
}

impl<T, Tag> Default for DescriptorTable<T, Tag> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug, Tag> fmt::Debug for DescriptorTable<T, Tag> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DescriptorTable")
            .field("len", &self.len)
            .field("slots", &self.slots.len())
            .field("free", &self.free)
            .finish()
    }
}
