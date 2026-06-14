use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};

use crate::align::{self, CACHE_LINE};
use crate::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index {
    slot: u32,
    generation: u32,
}

impl Index {
    #[inline(always)]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }

    #[inline(always)]
    pub const fn slot(self) -> u32 {
        self.slot
    }

    #[inline(always)]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

pub struct Pool<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    stride: usize,
    layout: Option<Layout>,
    free: Vec<u32>,
    generations: Vec<u32>,
    allocated: Vec<bool>,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send, const ALIGN: usize> Send for Pool<T, ALIGN> {}
unsafe impl<T: Sync, const ALIGN: usize> Sync for Pool<T, ALIGN> {}

impl<T, const ALIGN: usize> Pool<T, ALIGN> {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let stride = align::slot_stride::<T, ALIGN>();
        let layout = if capacity == 0 {
            None
        } else {
            let size = stride
                .checked_mul(capacity)
                .expect("aligned pool allocation size overflow");
            Some(
                Layout::from_size_align(size, align::allocation_align::<T, ALIGN>())
                    .expect("valid aligned pool layout"),
            )
        };
        let ptr = match layout {
            Some(layout) => {
                // SAFETY: `layout` is valid and non-zero-sized by construction.
                let ptr = unsafe { alloc(layout) };
                match NonNull::new(ptr) {
                    Some(ptr) => ptr,
                    None => handle_alloc_error(layout),
                }
            }
            None => NonNull::dangling(),
        };
        let free = (0..capacity)
            .rev()
            .map(|slot| u32::try_from(slot).expect("pool slot index fits u32"))
            .collect::<Vec<_>>();

        Self {
            ptr,
            capacity,
            len: 0,
            stride,
            layout,
            free,
            generations: (0..capacity).map(|_| 0).collect(),
            allocated: (0..capacity).map(|_| false).collect(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn insert(&mut self, value: T) -> Option<Index> {
        let slot = self.free.pop()?;
        let slot_index = slot as usize;
        let generation = self.generations[slot_index].wrapping_add(1).max(1);
        self.generations[slot_index] = generation;
        self.allocated[slot_index] = true;
        self.len += 1;
        unsafe { self.slot_ptr_unchecked(slot_index).write(value) };
        Some(Index { slot, generation })
    }

    #[inline]
    pub fn get(&self, index: Index) -> Option<&T> {
        self.validate(index)
            .map(|slot| unsafe { &*self.slot_ptr_unchecked(slot) })
    }

    #[inline]
    pub fn get_mut(&mut self, index: Index) -> Option<&mut T> {
        self.validate(index)
            .map(|slot| unsafe { &mut *self.slot_ptr_unchecked(slot) })
    }

    #[inline]
    pub fn remove(&mut self, index: Index) -> Option<T> {
        let slot = self.validate(index)?;
        self.allocated[slot] = false;
        self.free.push(slot as u32);
        self.len -= 1;
        Some(unsafe { self.slot_ptr_unchecked(slot).read() })
    }

    #[inline]
    pub fn contains_key(&self, index: Index) -> bool {
        self.validate(index).is_some()
    }

    #[inline]
    pub fn iter(&self) -> PoolIter<'_, T, ALIGN> {
        PoolIter {
            pool: self,
            next_slot: 0,
        }
    }

    #[inline]
    pub fn slot_ptr(&self, index: Index) -> Option<*const T> {
        self.validate(index)
            .map(|slot| self.slot_ptr_unchecked(slot).cast_const())
    }

    #[inline]
    fn validate(&self, index: Index) -> Option<usize> {
        let slot = index.slot as usize;
        (slot < self.capacity && self.allocated[slot] && self.generations[slot] == index.generation)
            .then_some(slot)
    }

    #[inline(always)]
    fn slot_ptr_unchecked(&self, slot: usize) -> *mut T {
        debug_assert!(slot < self.capacity);
        unsafe { self.ptr.as_ptr().add(slot * self.stride).cast::<T>() }
    }
}

pub struct PoolIter<'a, T, const ALIGN: usize = CACHE_LINE> {
    pool: &'a Pool<T, ALIGN>,
    next_slot: usize,
}

impl<'a, T, const ALIGN: usize> Iterator for PoolIter<'a, T, ALIGN> {
    type Item = (Index, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_slot < self.pool.capacity {
            let slot = self.next_slot;
            self.next_slot += 1;
            if !self.pool.allocated[slot] {
                continue;
            }
            let index = Index::new(slot as u32, self.pool.generations[slot]);
            let value = unsafe { &*self.pool.slot_ptr_unchecked(slot) };
            return Some((index, value));
        }
        None
    }
}

impl<T, const ALIGN: usize> Drop for Pool<T, ALIGN> {
    fn drop(&mut self) {
        for slot in 0..self.capacity {
            if self.allocated[slot] {
                unsafe { ptr::drop_in_place(self.slot_ptr_unchecked(slot)) };
            }
        }
        if let Some(layout) = self.layout {
            unsafe { dealloc(self.ptr.as_ptr(), layout) };
        }
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for Pool<T, ALIGN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .finish()
    }
}
