use super::*;

pub(crate) struct BufferChain<'pool> {
    pool: Option<&'pool BufferPool>,
    next: Option<Index>,
    failed: bool,
    error: Option<DataPlaneError>,
}

impl<'pool> BufferChain<'pool> {
    #[inline]
    pub(crate) fn new(pool: DataPlaneResult<&'pool BufferPool>, index: Index) -> Self {
        match pool {
            Ok(pool) => Self {
                pool: Some(pool),
                next: Some(index),
                failed: false,
                error: None,
            },
            Err(error) => Self {
                pool: None,
                next: None,
                failed: false,
                error: Some(error),
            },
        }
    }
}

impl<'pool> Iterator for BufferChain<'pool> {
    type Item = DataPlaneResult<BufferRef<'pool>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        if self.failed {
            return None;
        }
        let current = self.next?;
        let pool = self.pool?;
        let guard = pool.arena.inner.read();
        self.next = match guard.next_buffer(current) {
            Ok(next) => next,
            Err(err) => {
                self.failed = true;
                return Some(Err(err));
            }
        };
        Some(Ok(BufferRef {
            guard: spinning_top::guard::RwSpinlockReadGuard::map(guard, |pool| {
                pool.buffer(current)
                    .expect("buffer index was validated before mapping")
            }),
        }))
    }
}

impl DataPlaneBuffers {
    #[inline]
    pub fn chain_buffer(&self, head: Index, tail: Index) -> DataPlaneResult<()> {
        self.try_buffers()?.chain_buffer(head, tail)
    }
}

impl BufferPool {
    #[inline]
    fn chain_buffer(&self, head: Index, tail: Index) -> DataPlaneResult<()> {
        self.arena.inner.write().chain_buffer(head, tail)
    }
}

impl BufferPoolInner {
    #[inline]
    fn chain_buffer(&mut self, head: Index, tail: Index) -> DataPlaneResult<()> {
        self.ensure_writable(head)?;
        self.buffer(tail)?;
        let tail_len = {
            let tail_buffer = self.buffer(tail)?;
            tail_buffer
                .current_len()
                .checked_add(tail_buffer.total_len_not_including_first())
                .ok_or(BufferInvariant::ChainLengthOverflow)?
        };
        let mut last = head;
        while let Some(next) = self.next_buffer(last)? {
            self.ensure_writable(next)?;
            last = next;
        }
        self.buffer_mut(last)?.set_next_buffer(Some(tail));
        let total_tail_len = self
            .buffer(head)?
            .total_len_not_including_first()
            .checked_add(tail_len)
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        self.buffer_mut(head)?
            .set_total_len_not_including_first(total_tail_len)
    }
}
