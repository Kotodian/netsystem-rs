use crate::vec::Vec;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bitmap {
    words: Vec<u64>,
}

impl Bitmap {
    #[inline]
    pub const fn new() -> Self {
        Self { words: Vec::new() }
    }

    #[inline]
    pub fn with_capacity(bits: usize) -> Self {
        let mut words = Vec::with_capacity(words_for(bits));
        words.resize(words_for(bits), 0);
        Self { words }
    }

    #[inline]
    pub fn set(&mut self, bit: usize) -> bool {
        self.ensure_bit(bit);
        let word = bit / u64::BITS as usize;
        let mask = 1u64 << (bit % u64::BITS as usize);
        let was_set = self.words[word] & mask != 0;
        self.words[word] |= mask;
        !was_set
    }

    #[inline]
    pub fn clear(&mut self, bit: usize) -> bool {
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
    pub fn is_set(&self, bit: usize) -> bool {
        let word = bit / u64::BITS as usize;
        if word >= self.words.len() {
            return false;
        }
        let mask = 1u64 << (bit % u64::BITS as usize);
        self.words[word] & mask != 0
    }

    #[inline]
    pub fn first_set(&self) -> Option<usize> {
        self.next_set_from(0)
    }

    #[inline]
    pub fn next_set(&self, bit: usize) -> Option<usize> {
        bit.checked_add(1).and_then(|next| self.next_set_from(next))
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
                return Some(word_index * bits_per_word + word.trailing_zeros() as usize);
            }
            word_index += 1;
            if word_index >= self.words.len() {
                return None;
            }
            word = self.words[word_index];
        }
    }
}

#[inline]
const fn words_for(bits: usize) -> usize {
    bits.div_ceil(u64::BITS as usize)
}

#[cfg(test)]
mod tests {
    use super::Bitmap;

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
}
