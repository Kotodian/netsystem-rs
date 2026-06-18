use core::fmt;

use crate::vec::Vec;

pub struct FifoQueue<T> {
    front: Vec<T>,
    back: Vec<T>,
}

impl<T> FifoQueue<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            front: Vec::new(),
            back: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            front: Vec::with_capacity(capacity),
            back: Vec::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.front.len() + self.back.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.front.is_empty() && self.back.is_empty()
    }

    #[inline]
    pub fn push_back(&mut self, value: T) {
        self.back.push(value);
    }

    #[inline]
    pub fn front(&self) -> Option<&T> {
        self.front.last().or_else(|| self.back.as_slice().first())
    }

    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.front.is_empty() {
            self.move_back_to_front();
        }
        self.front.as_mut_slice().last_mut()
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        if self.front.is_empty() {
            self.move_back_to_front();
        }
        self.front.pop()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.front.clear();
        self.back.clear();
    }

    fn move_back_to_front(&mut self) {
        while let Some(value) = self.back.pop() {
            self.front.push(value);
        }
    }
}

impl<T> Default for FifoQueue<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for FifoQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FifoQueue")
            .field("len", &self.len())
            .finish()
    }
}
