//! VPP-style sparsely indexed vectors.
//!
//! The sparse index space is represented by a bitmap and per-word member
//! counts. Values are kept densely and are allocated only for registered
//! indices. This is the ownership and lookup shape of VPP `sparse_vec_t`.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseVec<T> {
    members: Vec<u64>,
    member_counts: Vec<u32>,
    values: Vec<T>,
    index_bits: u8,
}

impl<T> SparseVec<T> {
    #[inline]
    pub fn with_index_bits(index_bits: u8) -> Self {
        assert!(index_bits <= usize::BITS as u8);
        let words = if index_bits == 0 {
            1
        } else {
            1usize << index_bits.saturating_sub(6)
        };
        Self {
            members: vec![0; words],
            member_counts: vec![0; words],
            values: Vec::new(),
            index_bits,
        }
    }

    #[inline]
    pub fn with_capacity(max_index: usize) -> Self {
        let index_bits = usize::BITS - max_index.saturating_add(1).leading_zeros();
        Self::with_index_bits(index_bits as u8)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[inline]
    pub fn contains(&self, index: usize) -> bool {
        self.member(index).is_some()
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        let dense = self.member(index)?;
        self.values.get(dense)
    }

    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let dense = self.member(index)?;
        self.values.get_mut(dense)
    }

    #[inline]
    pub fn insert(&mut self, index: usize, value: T) -> Option<T> {
        self.assert_index(index);
        let word = index / 64;
        let bit = index % 64;
        let mask = 1u64 << bit;
        if self.members[word] & mask != 0 {
            let dense = self.dense_index(word, bit);
            return Some(std::mem::replace(&mut self.values[dense], value));
        }

        let dense = self.dense_index(word, bit);
        self.members[word] |= mask;
        for count in &mut self.member_counts[word..] {
            *count = count
                .checked_add(1)
                .expect("SparseVec member count exceeds u32");
        }
        self.values.insert(dense, value);
        None
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> Option<T> {
        let word = index / 64;
        let bit = index % 64;
        if word >= self.members.len() || self.members[word] & (1u64 << bit) == 0 {
            return None;
        }
        let dense = self.dense_index(word, bit);
        self.members[word] &= !(1u64 << bit);
        for count in &mut self.member_counts[word..] {
            *count = count
                .checked_sub(1)
                .expect("SparseVec member count underflow");
        }
        Some(self.values.remove(dense))
    }

    #[inline]
    fn member(&self, index: usize) -> Option<usize> {
        let word = index / 64;
        let bit = index % 64;
        if word >= self.members.len() || self.members[word] & (1u64 << bit) == 0 {
            return None;
        }
        Some(self.dense_index(word, bit))
    }

    #[inline]
    fn dense_index(&self, word: usize, bit: usize) -> usize {
        let lower = if bit == 0 {
            0
        } else {
            (self.members[word] & ((1u64 << bit) - 1)).count_ones() as usize
        };
        let prior = self.member_counts[..word].last().copied().unwrap_or(0) as usize;
        prior + lower
    }

    #[inline]
    fn assert_index(&self, index: usize) {
        assert!(
            index < (1usize << self.index_bits.min(usize::BITS as u8 - 1)),
            "SparseVec index exceeds configured sparse index space"
        );
    }
}
