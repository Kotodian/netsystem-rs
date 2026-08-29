//! Fixed-capacity byte buffer used for worker-local packet scratch.

use std::ops::{Deref, DerefMut};

use bytes::BytesMut;

/// Fixed-capacity byte storage allocated from the Hammer Main Heap.
///
/// The buffer does not grow after construction. Callers use the `Deref`
/// implementation or [`BytesBuffer::as_mut_slice`] to access storage.
#[derive(Debug)]
pub struct BytesBuffer {
    bytes: BytesMut,
}

impl BytesBuffer {
    /// Allocates `capacity` bytes of Main Heap scratch.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: BytesMut::with_capacity(capacity),
        }
    }

    /// Fixed capacity retained for the lifetime of this buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    /// Number of initialized bytes currently visible.
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether no initialized bytes are visible.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Resets the visible length without releasing capacity.
    #[inline]
    pub fn clear(&mut self) {
        self.bytes.clear();
    }

    /// Truncates the visible length without releasing capacity.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.bytes.truncate(len);
    }

    /// Resizes the visible length, filling new bytes with `value`.
    #[inline]
    pub fn resize(&mut self, len: usize, value: u8) {
        self.bytes.resize(len, value);
    }

    /// Appends bytes without releasing capacity.
    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    /// Access to the initialized bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..]
    }

    /// Mutable access to the initialized bytes.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes[..]
    }
}

impl Deref for BytesBuffer {
    type Target = BytesMut;

    #[inline]
    fn deref(&self) -> &BytesMut {
        &self.bytes
    }
}

impl DerefMut for BytesBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut BytesMut {
        &mut self.bytes
    }
}
