use super::*;

impl DataPlaneBuffers {
    #[inline]
    pub fn attach_clone(&self, head: Index, tail: Index) -> DataPlaneResult<()> {
        self.try_buffers()?.attach_clone(head, tail)
    }
}

impl BufferPool {
    #[inline]
    fn attach_clone(&self, head: Index, tail: Index) -> DataPlaneResult<()> {
        self.arena.inner.write().attach_clone(head, tail)
    }
}

impl BufferPoolInner {
    #[inline]
    fn attach_clone(&mut self, head: Index, tail: Index) -> DataPlaneResult<()> {
        if head == tail {
            return Err(BufferInvariant::CloneRequiresDistinctBuffers.into());
        }
        self.ensure_header_exclusive(head)?;
        if self.next_buffer(head)?.is_some() {
            return Err(BufferInvariant::CloneHeadHasNextBuffer.into());
        }
        let tail_len = {
            let tail_buffer = self.buffer(tail)?;
            tail_buffer
                .current_len()
                .checked_add(tail_buffer.total_len_not_including_first())
                .ok_or(BufferInvariant::ChainLengthOverflow)?
        };
        {
            let head_buffer = self.buffer_mut(head)?;
            head_buffer.set_next_buffer(Some(tail));
        }
        let mut current = Some(tail);
        while let Some(current_index) = current {
            let next = self.next_buffer(current_index)?;
            let buffer = self.buffer_mut(current_index)?;
            buffer.cacheline0.ref_count = buffer
                .cacheline0
                .ref_count
                .checked_add(1)
                .ok_or(BufferInvariant::RefCountOverflow)?;
            current = next;
        }
        self.buffer_mut(head)?
            .set_total_len_not_including_first(tail_len)
    }
}
