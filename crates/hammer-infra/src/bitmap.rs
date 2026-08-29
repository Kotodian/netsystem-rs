use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitmap<I = usize> {
    words: Vec<u64>,
    index: PhantomData<fn(I) -> I>,
}

impl<I> Default for Bitmap<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Bitmap<I> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            words: Vec::new(),
            index: PhantomData,
        }
    }

    #[inline]
    pub fn with_capacity(bits: usize) -> Self {
        let mut words = Vec::with_capacity(words_for(bits));
        words.resize(words_for(bits), 0);
        Self {
            words,
            index: PhantomData,
        }
    }

    #[inline]
    pub fn count_set(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    #[inline]
    pub fn clear_all(&mut self) {
        for word in self.words.iter_mut() {
            *word = 0;
        }
    }

    #[inline]
    pub fn word_len(&self) -> usize {
        self.words.len()
    }

    pub(crate) fn try_reserve_bits(
        &mut self,
        bits: usize,
    ) -> Result<(), std::collections::TryReserveError> {
        let required = words_for(bits);
        if required <= self.words.len() {
            return Ok(());
        }
        let additional = required - self.words.len();
        self.words.try_reserve(additional)?;
        for _ in 0..additional {
            self.words.push(0);
        }
        Ok(())
    }

    fn ensure_bit(&mut self, bit: usize) {
        let required = bit / u64::BITS as usize + 1;
        if required <= self.words.len() {
            return;
        }
        self.words.reserve(required - self.words.len());
        while self.words.len() < required {
            self.words.push(0);
        }
    }
}

impl<I: Into<usize>> Bitmap<I> {
    #[inline]
    pub fn set(&mut self, index: I) -> bool {
        let bit = index.into();
        self.ensure_bit(bit);
        let word = bit / u64::BITS as usize;
        let mask = 1u64 << (bit % u64::BITS as usize);
        let was_set = self.words[word] & mask != 0;
        self.words[word] |= mask;
        !was_set
    }

    #[inline]
    pub fn clear(&mut self, index: I) -> bool {
        let bit = index.into();
        let word = bit / u64::BITS as usize;
        if word >= self.words.len() {
            return false;
        }
        let mask = 1u64 << (bit % u64::BITS as usize);
        let was_set = self.words[word] & mask != 0;
        self.words[word] &= !mask;
        was_set
    }

    #[inline]
    pub fn is_set(&self, index: I) -> bool {
        let bit = index.into();
        let word = bit / u64::BITS as usize;
        if word >= self.words.len() {
            return false;
        }
        let mask = 1u64 << (bit % u64::BITS as usize);
        self.words[word] & mask != 0
    }
}

impl Bitmap<usize> {
    pub(crate) fn first_clear_from(&self, start: usize, limit: usize) -> Option<usize> {
        if start >= limit {
            return None;
        }

        let bits_per_word = u64::BITS as usize;
        let mut word_index = start / bits_per_word;
        let bit_in_word = start % bits_per_word;
        let mut available = !self.words.get(word_index).copied().unwrap_or(0);
        available &= !0_u64 << bit_in_word;

        loop {
            if available != 0 {
                let position = word_index
                    .saturating_mul(bits_per_word)
                    .saturating_add(available.trailing_zeros() as usize);
                if position < limit {
                    return Some(position);
                }
                return None;
            }
            word_index = word_index.checked_add(1)?;
            available = !self.words.get(word_index).copied().unwrap_or(0);
        }
    }

    pub(crate) fn next_clear_after(&self, index: usize, limit: usize) -> Option<usize> {
        index
            .checked_add(1)
            .and_then(|start| self.first_clear_from(start, limit))
    }

    #[inline]
    pub fn first_set(&self) -> Option<usize> {
        self.next_set_from(0)
    }

    #[inline]
    pub fn next_set(&self, index: usize) -> Option<usize> {
        index
            .checked_add(1)
            .and_then(|next| self.next_set_from(next))
    }

    #[inline]
    pub fn iter_set(&self) -> SetBits<'_> {
        SetBits {
            bitmap: self,
            next: self.first_set(),
        }
    }

    fn next_set_from(&self, bit: usize) -> Option<usize> {
        let bits_per_word = u64::BITS as usize;
        let mut word_index = bit / bits_per_word;
        if word_index >= self.words.len() {
            return None;
        }
        let bit_in_word = bit % bits_per_word;
        let mut word = self.words[word_index] & (!0u64 << bit_in_word);
        loop {
            if word != 0 {
                let bit = word_index * bits_per_word + word.trailing_zeros() as usize;
                return Some(bit);
            }
            word_index += 1;
            if word_index >= self.words.len() {
                return None;
            }
            word = self.words[word_index];
        }
    }
}

pub struct SetBits<'a> {
    bitmap: &'a Bitmap,
    next: Option<usize>,
}

impl Iterator for SetBits<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        self.next = self.bitmap.next_set(current);
        Some(current)
    }
}

#[inline]
const fn words_for(bits: usize) -> usize {
    bits.div_ceil(u64::BITS as usize)
}
