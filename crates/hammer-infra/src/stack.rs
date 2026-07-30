/// A last-in-first-out collection.
///
/// `Stack` is infrastructure only. It owns values and does not encode worker,
/// protocol, session, or scheduling semantics.
#[derive(Debug, Default)]
pub struct Stack<T> {
    values: Vec<T>,
}

impl<T> Stack<T> {
    #[inline]
    pub const fn new() -> Self {
        Self { values: Vec::new() }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.values.push(value);
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }

    #[inline]
    pub fn first(&self) -> Option<&T> {
        self.values.first()
    }

    #[inline]
    pub fn last(&self) -> Option<&T> {
        self.values.last()
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.values
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl<T> FromIterator<T> for Stack<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}
