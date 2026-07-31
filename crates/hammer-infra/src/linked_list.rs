use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};

/// A generic doubly linked list.
///
/// `LinkedList` is infrastructure only. It owns no session, worker, MQ, or
/// scheduling semantics; callers place their own values in it.
pub struct LinkedList<T> {
    head: Option<NonNull<Node<T>>>,
    tail: Option<NonNull<Node<T>>>,
    len: usize,
}

unsafe impl<T: Send> Send for LinkedList<T> {}
unsafe impl<T: Sync> Sync for LinkedList<T> {}

impl<T> LinkedList<T> {
    /// Creates an empty list.
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,
        }
    }

    /// Appends a value to the tail of the list.
    #[inline]
    pub fn push_back(&mut self, value: T) {
        let node = Self::allocate_node(value, self.tail, None);
        if let Some(tail) = self.tail {
            // SAFETY: `tail` is owned by this list and remains live until it
            // is unlinked below.
            unsafe { (*tail.as_ptr()).next = Some(node) };
        } else {
            self.head = Some(node);
        }
        self.tail = Some(node);
        self.len += 1;
    }

    /// Prepends a value to the head of the list.
    #[inline]
    pub fn push_front(&mut self, value: T) {
        let node = Self::allocate_node(value, None, self.head);
        if let Some(head) = self.head {
            // SAFETY: `head` is owned by this list and remains live until it
            // is unlinked below.
            unsafe { (*head.as_ptr()).prev = Some(node) };
        } else {
            self.tail = Some(node);
        }
        self.head = Some(node);
        self.len += 1;
    }

    /// Removes and returns the head value, if any.
    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        self.head.map(|node| self.unlink(node))
    }

    /// Removes and returns the tail value, if any.
    #[inline]
    pub fn pop_back(&mut self) -> Option<T> {
        self.tail.map(|node| self.unlink(node))
    }

    /// Returns the head value, if any.
    #[inline]
    pub fn front(&self) -> Option<&T> {
        self.head
            .as_ref()
            .map(|node| unsafe { &node.as_ref().value })
    }

    /// Returns the head value for mutation, if any.
    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        self.head
            .as_mut()
            .map(|node| unsafe { &mut node.as_mut().value })
    }

    /// Returns the tail value, if any.
    #[inline]
    pub fn back(&self) -> Option<&T> {
        self.tail
            .as_ref()
            .map(|node| unsafe { &node.as_ref().value })
    }

    /// Returns the tail value for mutation, if any.
    #[inline]
    pub fn back_mut(&mut self) -> Option<&mut T> {
        self.tail
            .as_mut()
            .map(|node| unsafe { &mut node.as_mut().value })
    }

    /// Iterates over values from head to tail.
    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            next: self.head,
            _marker: PhantomData,
        }
    }

    /// Iterates over values from head to tail for mutation.
    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            next: self.head,
            _marker: PhantomData,
        }
    }

    /// Returns the number of values in the list.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the list contains no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes all values from the list.
    #[inline]
    pub fn clear(&mut self) {
        while self.pop_front().is_some() {}
    }

    #[inline]
    fn allocate_node(
        value: T,
        prev: Option<NonNull<Node<T>>>,
        next: Option<NonNull<Node<T>>>,
    ) -> NonNull<Node<T>> {
        let layout = Layout::new::<Node<T>>();
        // SAFETY: `alloc` returns memory with the requested layout, or null.
        let ptr = unsafe { alloc(layout) }.cast::<Node<T>>();
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout);
        };
        // SAFETY: the allocation is valid, suitably aligned, and uninitialized.
        unsafe {
            ptr.as_ptr().write(Node { value, prev, next });
        }
        ptr
    }

    #[inline]
    fn unlink(&mut self, node: NonNull<Node<T>>) -> T {
        // SAFETY: `node` is owned by this list and is not used after dealloc.
        unsafe {
            let prev = (*node.as_ptr()).prev;
            let next = (*node.as_ptr()).next;
            if let Some(prev) = prev {
                (*prev.as_ptr()).next = next;
            } else {
                self.head = next;
            }
            if let Some(next) = next {
                (*next.as_ptr()).prev = prev;
            } else {
                self.tail = prev;
            }

            let value = ptr::read(&(*node.as_ptr()).value);
            dealloc(node.as_ptr().cast::<u8>(), Layout::new::<Node<T>>());
            self.len -= 1;
            value
        }
    }
}

impl<T> Default for LinkedList<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for LinkedList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

struct Node<T> {
    value: T,
    prev: Option<NonNull<Node<T>>>,
    next: Option<NonNull<Node<T>>>,
}

/// Iterator over `LinkedList` values.
pub struct Iter<'a, T> {
    next: Option<NonNull<Node<T>>>,
    _marker: PhantomData<&'a Node<T>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.next?;
        // SAFETY: `node` is owned by the list for the duration of the borrow
        // represented by `Iter<'a>`.
        unsafe {
            let node = node.as_ref();
            self.next = node.next;
            Some(&node.value)
        }
    }
}

/// Mutable iterator over `LinkedList` values.
pub struct IterMut<'a, T> {
    next: Option<NonNull<Node<T>>>,
    _marker: PhantomData<&'a mut Node<T>>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut node = self.next?;
        // SAFETY: `node` is owned by the list for the duration of the borrow
        // represented by `IterMut<'a>`, and each yielded node is distinct.
        unsafe {
            let node = node.as_mut();
            self.next = node.next;
            Some(&mut node.value)
        }
    }
}
