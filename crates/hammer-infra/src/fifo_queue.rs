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
    pub fn insert(&mut self, index: usize, value: T) {
        assert!(index <= self.len(), "insert index out of bounds");
        if index == self.len() {
            self.push_back(value);
            return;
        }
        let front_len = self.front.len();
        if index < front_len {
            let storage_index = front_len - index;
            self.front.push(value);
            let len = self.front.len();
            self.front.as_mut_slice()[storage_index..len].rotate_right(1);
            return;
        }
        let storage_index = index - front_len;
        self.back.push(value);
        let len = self.back.len();
        self.back.as_mut_slice()[storage_index..len].rotate_right(1);
    }

    #[inline]
    pub fn front(&self) -> Option<&T> {
        self.front.last().or_else(|| self.back.as_slice().first())
    }

    #[inline]
    pub fn back(&self) -> Option<&T> {
        self.back.last().or_else(|| self.front.as_slice().first())
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len() {
            return None;
        }
        let front_len = self.front.len();
        if index < front_len {
            return self.front.as_slice().get(front_len - 1 - index);
        }
        self.back.as_slice().get(index - front_len)
    }

    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.front.is_empty() {
            self.move_back_to_front();
        }
        self.front.as_mut_slice().last_mut()
    }

    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len() {
            return None;
        }
        let front_len = self.front.len();
        if index < front_len {
            return self.front.as_mut_slice().get_mut(front_len - 1 - index);
        }
        self.back.as_mut_slice().get_mut(index - front_len)
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        if self.front.is_empty() {
            self.move_back_to_front();
        }
        self.front.pop()
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len(), "remove index out of bounds");
        let front_len = self.front.len();
        if index < front_len {
            let storage_index = front_len - 1 - index;
            return self.front.remove(storage_index);
        }
        self.back.remove(index - front_len)
    }

    #[inline]
    pub fn clear(&mut self) {
        self.front.clear();
        self.back.clear();
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            front: self.front.as_slice().iter().rev(),
            back: self.back.as_slice().iter(),
        }
    }

    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            front: self.front.as_mut_slice().iter_mut().rev(),
            back: self.back.as_mut_slice().iter_mut(),
        }
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

pub struct Iter<'a, T> {
    front: core::iter::Rev<core::slice::Iter<'a, T>>,
    back: core::slice::Iter<'a, T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.front.next().or_else(|| self.back.next())
    }
}

pub struct IterMut<'a, T> {
    front: core::iter::Rev<core::slice::IterMut<'a, T>>,
    back: core::slice::IterMut<'a, T>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.front.next().or_else(|| self.back.next())
    }
}

impl<T: fmt::Debug> fmt::Debug for FifoQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FifoQueue")
            .field("len", &self.len())
            .finish()
    }
}
