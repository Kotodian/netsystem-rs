use super::*;

#[derive(Debug)]
pub struct BufferFrame {
    indices: Vec<Index>,
    /// Logical graph Frame maximum. Independent of the growable vector's
    /// reserved capacity.
    limit: usize,
}

macro_rules! retain_ladder {
    ($read:ident, $write:ident, $len:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($read:ident, $write:ident, $len:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder!($read, $write, $len, 2, $step);
    };
}

macro_rules! retain_ladder_prefetch {
    ($self:expr, $read:ident, $write:ident, $len:ident, $prefetch:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $self.prefetch_indices($read + 2, 2, $prefetch);
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($self:expr, $read:ident, $write:ident, $len:ident, $prefetch:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $self.prefetch_indices($read + 4, 4, $prefetch);
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder_prefetch!($self, $read, $write, $len, $prefetch, 2, $step);
    };
}

macro_rules! retain_ladder_state_prefetch {
    ($self:expr, $read:ident, $write:ident, $len:ident, $state:ident, $prefetch:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $self.prefetch_indices_state($read + 2, 2, $state, $prefetch);
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($self:expr, $read:ident, $write:ident, $len:ident, $state:ident, $prefetch:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $self.prefetch_indices_state($read + 4, 4, $state, $prefetch);
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        retain_ladder_state_prefetch!($self, $read, $write, $len, $state, $prefetch, 2, $step);
    };
}

macro_rules! rewrite_ladder {
    ($read:ident, $write:ident, $len:ident, 2, $step:expr) => {
        while $read + 2 <= $len {
            $step(0)?;
            $step(1)?;
            $read += 2;
        }
        if $read < $len {
            $step(0)?;
            $read += 1;
        }
    };
    ($read:ident, $write:ident, $len:ident, 4, $step:expr) => {
        while $read + 4 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $read += 4;
        }
        rewrite_ladder!($read, $write, $len, 2, $step);
    };
    ($read:ident, $write:ident, $len:ident, 8, $step:expr) => {
        while $read + 8 <= $len {
            $step(0)?;
            $step(1)?;
            $step(2)?;
            $step(3)?;
            $step(4)?;
            $step(5)?;
            $step(6)?;
            $step(7)?;
            $read += 8;
        }
        rewrite_ladder!($read, $write, $len, 4, $step);
    };
}

impl BufferFrame {
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "frame capacity must be non-zero");
        Self {
            indices: Vec::with_capacity(capacity),
            limit: capacity,
        }
    }

    #[inline]
    pub fn push_index(&mut self, index: Index) -> DataPlaneResult<()> {
        if self.indices.len() == self.limit {
            return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
        }
        self.indices.push(index);
        Ok(())
    }

    #[inline]
    pub fn push_indices(
        &mut self,
        indices: impl IntoIterator<Item = Index>,
    ) -> DataPlaneResult<()> {
        let indices = indices.into_iter();
        let (lower, upper) = indices.size_hint();
        if let Some(upper) = upper {
            if self.indices.len() + upper > self.limit {
                return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
            }
        } else if self.indices.len() + lower > self.limit {
            return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
        }

        let original_len = self.indices.len();
        for index in indices {
            if self.indices.len() == self.limit {
                self.indices.truncate(original_len);
                return Err(DataPlaneError::BufferFrameCapacityExceeded.into());
            }
            self.indices.push(index);
        }
        Ok(())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.limit
    }

    #[inline]
    pub(crate) fn reset_for_pool_reuse(&mut self) {
        self.indices.clear();
    }

    #[inline]
    pub fn indices(&self) -> &[Index] {
        &self.indices
    }

    #[inline]
    pub(crate) fn drain_indices(&mut self) -> std::vec::Drain<'_, Index> {
        self.indices.drain(..)
    }

    #[inline]
    pub fn discard_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let count = count.min(self.indices.len());
        drop(self.indices.drain(..count));
    }

    #[inline]
    pub fn retain_indices(
        &mut self,
        mut keep: impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let mut write = 0usize;
        for read in 0..self.indices.len() {
            let index = self.indices[read];
            if keep(index)? {
                if write != read {
                    self.indices[write] = index;
                }
                write += 1;
            }
        }
        self.indices.truncate(write);
        Ok(())
    }

    #[inline(always)]
    pub fn retain_indices_batched(
        &mut self,
        width: FrameBatchWidth,
        mut keep: impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        match width {
            FrameBatchWidth::Octo => self.retain_indices_octo(&mut keep),
            FrameBatchWidth::Quad => self.retain_indices_quad(&mut keep),
            FrameBatchWidth::Pair => self.retain_indices_pair(&mut keep),
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch(
        &mut self,
        width: FrameBatchWidth,
        mut prefetch: impl FnMut(Index),
        mut keep: impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch(&mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch(&mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn retain_indices_batched_with_prefetch_state<S>(
        &mut self,
        width: FrameBatchWidth,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, Index),
        mut keep: impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn buffer_node_inline<S>(
        &mut self,
        width: FrameBatchWidth,
        state: &mut S,
        mut prefetch: impl FnMut(&mut S, Index),
        mut keep: impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        match width {
            FrameBatchWidth::Quad => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Pair => {
                self.retain_indices_pair_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
            FrameBatchWidth::Octo => {
                self.retain_indices_quad_with_prefetch_state_lazy(state, &mut prefetch, &mut keep)
            }
        }
    }

    #[inline(always)]
    pub fn rewrite_indices_batched(
        &mut self,
        width: FrameBatchWidth,
        mut rewrite: impl FnMut(Index) -> DataPlaneResult<Option<Index>>,
    ) -> DataPlaneResult<()> {
        match width {
            FrameBatchWidth::Quad => self.rewrite_indices_quad(&mut rewrite),
            FrameBatchWidth::Pair => self.rewrite_indices_pair(&mut rewrite),
            FrameBatchWidth::Octo => self.rewrite_indices_octo(&mut rewrite),
        }
    }

    #[inline(always)]
    fn retain_indices_quad(
        &mut self,
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        while read + 4 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
            ];
            let mask = movemask_4([
                keep(chunk[0])?,
                keep(chunk[1])?,
                keep(chunk[2])?,
                keep(chunk[3])?,
            ]);
            if mask == 0b1111 && write == read {
                write += 4;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 4;
        }
        if read + 2 <= len {
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one(read, &mut write, keep)?;
            read += 1;
        }
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch(
        &mut self,
        prefetch: &mut impl FnMut(Index),
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_prefetch!(self, read, write, len, prefetch, 4, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair(
        &mut self,
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder!(read, write, len, 2, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_octo(
        &mut self,
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        while read + 8 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
                self.indices[read + 4],
                self.indices[read + 5],
                self.indices[read + 6],
                self.indices[read + 7],
            ];
            let k0 = keep(chunk[0])?;
            let k1 = keep(chunk[1])?;
            let k2 = keep(chunk[2])?;
            let k3 = keep(chunk[3])?;
            let k4 = keep(chunk[4])?;
            let k5 = keep(chunk[5])?;
            let k6 = keep(chunk[6])?;
            let k7 = keep(chunk[7])?;
            let mask = (k0 as u8)
                | ((k1 as u8) << 1)
                | ((k2 as u8) << 2)
                | ((k3 as u8) << 3)
                | ((k4 as u8) << 4)
                | ((k5 as u8) << 5)
                | ((k6 as u8) << 6)
                | ((k7 as u8) << 7);
            if mask == 0xff && write == read {
                write += 8;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 8;
        }
        if read + 4 <= len {
            let chunk = [
                self.indices[read],
                self.indices[read + 1],
                self.indices[read + 2],
                self.indices[read + 3],
            ];
            let mask = movemask_4([
                keep(chunk[0])?,
                keep(chunk[1])?,
                keep(chunk[2])?,
                keep(chunk[3])?,
            ]);
            if mask == 0b1111 && write == read {
                write += 4;
            } else {
                let mut m = mask;
                while m != 0 {
                    let lsb = m.trailing_zeros();
                    self.indices[write] = chunk[lsb as usize];
                    write += 1;
                    m &= m - 1;
                }
            }
            read += 4;
        }
        if read + 2 <= len {
            self.retain_one(read, &mut write, keep)?;
            self.retain_one(read + 1, &mut write, keep)?;
            read += 2;
        }
        while read < len {
            self.retain_one(read, &mut write, keep)?;
            read += 1;
        }
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch(
        &mut self,
        prefetch: &mut impl FnMut(Index),
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_prefetch!(self, read, write, len, prefetch, 2, |offset| {
            self.retain_one(read + offset, &mut write, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch_state<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 4, |offset| {
            self.retain_one_state(read + offset, &mut write, state, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch_state<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 2, |offset| {
            self.retain_one_state(read + offset, &mut write, state, keep)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_quad_with_prefetch_state_lazy<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 4, |offset| {
            self.retain_one_state_lazy(read + offset, &mut write, state, keep)
        });
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_indices_pair_with_prefetch_state_lazy<S>(
        &mut self,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, Index),
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = None;
        retain_ladder_state_prefetch!(self, read, write, len, state, prefetch, 2, |offset| {
            self.retain_one_state_lazy(read + offset, &mut write, state, keep)
        });
        self.finish_retain_lazy(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_quad(
        &mut self,
        rewrite: &mut impl FnMut(Index) -> DataPlaneResult<Option<Index>>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 4, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_octo(
        &mut self,
        rewrite: &mut impl FnMut(Index) -> DataPlaneResult<Option<Index>>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 8, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn rewrite_indices_pair(
        &mut self,
        rewrite: &mut impl FnMut(Index) -> DataPlaneResult<Option<Index>>,
    ) -> DataPlaneResult<()> {
        let len = self.indices.len();
        let mut read = 0usize;
        let mut write = 0usize;
        rewrite_ladder!(read, write, len, 2, |offset| {
            self.rewrite_one(read + offset, &mut write, rewrite)
        });
        self.finish_retain(write);
        Ok(())
    }

    #[inline(always)]
    fn retain_one(
        &mut self,
        read: usize,
        write: &mut usize,
        keep: &mut impl FnMut(Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let index = self.indices[read];
        if keep(index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn retain_one_state<S>(
        &mut self,
        read: usize,
        write: &mut usize,
        state: &mut S,
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let index = self.indices[read];
        if keep(state, index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn retain_one_state_lazy<S>(
        &mut self,
        read: usize,
        write: &mut Option<usize>,
        state: &mut S,
        keep: &mut impl FnMut(&mut S, Index) -> DataPlaneResult<bool>,
    ) -> DataPlaneResult<()> {
        let index = self.indices[read];
        if keep(state, index)? {
            if let Some(write) = write {
                self.indices[*write] = index;
                *write += 1;
            }
        } else if write.is_none() {
            *write = Some(read);
        }
        Ok(())
    }

    #[inline(always)]
    fn rewrite_one(
        &mut self,
        read: usize,
        write: &mut usize,
        rewrite: &mut impl FnMut(Index) -> DataPlaneResult<Option<Index>>,
    ) -> DataPlaneResult<()> {
        let index = self.indices[read];
        if let Some(index) = rewrite(index)? {
            self.indices[*write] = index;
            *write += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn prefetch_indices(&self, offset: usize, width: usize, prefetch: &mut impl FnMut(Index)) {
        let end = (offset + width).min(self.indices.len());
        for index in self.indices[offset..end].iter().copied() {
            prefetch(index);
        }
    }

    #[inline(always)]
    fn prefetch_indices_state<S>(
        &self,
        offset: usize,
        width: usize,
        state: &mut S,
        prefetch: &mut impl FnMut(&mut S, Index),
    ) {
        let end = (offset + width).min(self.indices.len());
        for index in self.indices[offset..end].iter().copied() {
            prefetch(state, index);
        }
    }

    #[inline(always)]
    fn finish_retain(&mut self, len: usize) {
        self.indices.truncate(len);
    }

    #[inline(always)]
    fn finish_retain_lazy(&mut self, len: Option<usize>) {
        if let Some(len) = len {
            self.finish_retain(len);
        }
    }
}
