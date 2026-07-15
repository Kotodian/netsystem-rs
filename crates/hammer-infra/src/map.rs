#[derive(Debug, Clone, Default)]
pub struct PrefixLengthSearchOrder {
    prefix_lengths: Box<[u8]>,
}

impl PrefixLengthSearchOrder {
    #[inline]
    pub fn empty() -> Self {
        Self::default()
    }

    #[inline]
    pub fn insert(&mut self, prefix_len: u8) {
        if self.prefix_lengths.contains(&prefix_len) {
            return;
        }
        let mut prefix_lengths = self.prefix_lengths.to_vec();
        prefix_lengths.push(prefix_len);
        prefix_lengths.sort_unstable_by(|left, right| right.cmp(left));
        self.prefix_lengths = prefix_lengths.into_boxed_slice();
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.prefix_lengths
    }
}
