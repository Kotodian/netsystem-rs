use super::*;

pub(super) struct BufferChain<'a> {
    buffers: &'a DataPlaneBuffers,
    next: Option<u32>,
    remaining: usize,
}

impl<'a> BufferChain<'a> {
    pub(super) fn new(buffers: &'a DataPlaneBuffers, index: u32) -> Self {
        Self {
            buffers,
            next: Some(index),
            remaining: BufferMain::global()
                .pools
                .iter()
                .map(|pool| pool.buffer_count)
                .sum(),
        }
    }
}

impl<'a> Iterator for BufferChain<'a> {
    type Item = DataPlaneResult<BufferRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.next?;
        assert_ne!(self.remaining, 0, "Buffer chain is acyclic");
        self.remaining -= 1;
        let buffer = self.buffers.get_buffer(current);
        self.next = match &buffer {
            Ok(buffer) => buffer.next_buffer_slot(),
            Err(_) => None,
        };
        Some(buffer)
    }
}

impl DataPlaneBuffers {
    pub fn chain_buffer(&self, head: u32, tail: u32) -> DataPlaneResult<()> {
        assert_ne!(head, tail, "exclusive chain requires distinct Buffers");
        let mut tail_len = 0usize;
        for buffer in self.chain(tail) {
            let buffer = buffer?;
            assert_eq!(
                buffer.ref_count(),
                1,
                "exclusive chain requires exclusive tail segments"
            );
            tail_len = tail_len
                .checked_add(buffer.current_len())
                .ok_or(BufferInvariant::ChainLengthOverflow)?;
        }
        let mut last = head;
        let mut head_tail_len = 0usize;
        let mut remaining: usize = BufferMain::global()
            .pools
            .iter()
            .map(|pool| pool.buffer_count)
            .sum();
        loop {
            assert_ne!(remaining, 0, "Buffer chain is acyclic");
            remaining -= 1;
            assert_ne!(last, tail, "exclusive chains must not overlap");
            let buffer = self.get_buffer(last)?;
            assert_eq!(
                buffer.ref_count(),
                1,
                "exclusive chain requires exclusive head segments"
            );
            if last != head {
                head_tail_len = head_tail_len
                    .checked_add(buffer.current_len())
                    .ok_or(BufferInvariant::ChainLengthOverflow)?;
            }
            match buffer.next_buffer_slot() {
                Some(next) => last = next,
                None => break,
            }
        }
        // If the tails overlap, their final segment is identical.
        let mut tail_last = tail;
        loop {
            let next = self.get_buffer(tail_last)?.next_buffer_slot();
            match next {
                Some(next) => tail_last = next,
                None => break,
            }
        }
        assert_ne!(last, tail_last, "exclusive chains must not overlap");
        let total = head_tail_len
            .checked_add(tail_len)
            .filter(|length| u32::try_from(*length).is_ok())
            .ok_or(BufferInvariant::ChainLengthOverflow)?;
        self.get_buffer_mut(last)?.set_next_buffer(Some(tail));
        self.get_buffer_mut(head)?
            .set_total_len_not_including_first(total)
    }
}
