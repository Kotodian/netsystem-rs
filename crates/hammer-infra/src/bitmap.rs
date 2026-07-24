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
    #[inline]
    pub fn first_set(&self) -> Option<usize> {
        self.next_set_from(0)
    }

    #[inline]
    pub fn next_set(&self, index: usize) -> Option<usize> {
        index.checked_add(1).and_then(|next| self.next_set_from(next))
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

#[cfg(test)]
mod tests {
    use super::Bitmap;

    #[derive(Clone, Copy)]
    struct TestIndex(usize);

    impl From<TestIndex> for usize {
        fn from(index: TestIndex) -> Self {
            index.0
        }
    }

    #[test]
    fn bitmap_first_and_next_set_follow_dense_ready_indices() {
        let mut bitmap = Bitmap::with_capacity(130);
        assert_eq!(bitmap.first_set(), None);

        assert!(bitmap.set(3));
        assert!(bitmap.set(64));
        assert!(bitmap.set(129));

        assert_eq!(bitmap.first_set(), Some(3));
        assert_eq!(bitmap.next_set(3), Some(64));
        assert_eq!(bitmap.next_set(64), Some(129));
        assert_eq!(bitmap.next_set(129), None);
    }

    #[test]
    fn bitmap_clear_removes_bits_without_touching_neighbors() {
        let mut bitmap = Bitmap::new();
        bitmap.set(1);
        bitmap.set(2);
        bitmap.set(63);

        assert!(bitmap.clear(2));
        assert!(!bitmap.is_set(2));
        assert!(bitmap.is_set(1));
        assert!(bitmap.is_set(63));
    }

    #[test]
    fn bitmap_clear_all_preserves_word_capacity_and_clears_iteration_state() {
        let mut bitmap = Bitmap::with_capacity(130);
        bitmap.set(5);
        bitmap.set(64);
        bitmap.set(129);
        let words = bitmap.word_len();

        bitmap.clear_all();

        assert_eq!(bitmap.word_len(), words);
        assert_eq!(bitmap.first_set(), None);
        assert!(!bitmap.is_set(5));
        assert!(!bitmap.is_set(64));
        assert!(!bitmap.is_set(129));
    }

    #[test]
    fn bitmap_counts_and_iterates_set_bits() {
        let mut bitmap = Bitmap::new();
        bitmap.set(2);
        bitmap.set(65);

        assert_eq!(bitmap.count_set(), 2);
        assert_eq!(bitmap.iter_set().collect::<Vec<_>>(), vec![2, 65]);
    }

    #[test]
    fn bitmap_accepts_a_typed_index() {
        let mut bitmap = Bitmap::<TestIndex>::new();
        bitmap.set(TestIndex(7));
        bitmap.set(TestIndex(71));

        assert!(bitmap.is_set(TestIndex(7)));
        assert!(!bitmap.is_set(TestIndex(8)));
        assert_eq!(bitmap.count_set(), 2);
    }
}
