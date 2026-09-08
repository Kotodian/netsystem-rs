use super::*;

impl DataPlaneBuffers {
    pub fn attach_clone(&self, head: u32, tail: u32) -> DataPlaneResult<()> {
        assert_ne!(head, tail, "clone requires distinct Buffers");
        let pool = BufferMain::global().pool(head);
        assert_eq!(
            pool.index,
            BufferMain::global().pool(tail).index,
            "clone head and tail use the same Pool"
        );
        let mut state = pool.free.write();
        let head_buffer = pool.buffer(head, &state.1);
        assert_eq!(head_buffer.ref_count(), 1, "clone head is exclusive");
        assert!(
            head_buffer.next_buffer_slot().is_none(),
            "clone head has no next segment"
        );
        let mut current = Some(tail);
        let mut tail_len = 0usize;
        let mut remaining = pool.buffer_count;
        // Validate every refcount and length before the first mutation.
        while let Some(index) = current {
            assert_ne!(remaining, 0, "clone tail is acyclic");
            remaining -= 1;
            assert_ne!(index, head, "clone head cannot occur in its tail");
            let buffer = pool.buffer(index, &state.1);
            assert!(
                buffer.ref_count() < u8::MAX,
                "clone refcount must not overflow"
            );
            tail_len = tail_len
                .checked_add(buffer.current_len())
                .filter(|length| u32::try_from(*length).is_ok())
                .ok_or(BufferInvariant::ChainLengthOverflow)?;
            current = buffer.next_buffer_slot();
        }
        current = Some(tail);
        while let Some(index) = current {
            let buffer = pool.buffer_mut(index, &mut state.1);
            buffer.cacheline0.ref_count += 1;
            current = buffer.next_buffer_slot();
        }
        let head_buffer = pool.buffer_mut(head, &mut state.1);
        head_buffer.set_next_buffer(Some(tail));
        head_buffer.set_total_len_not_including_first(tail_len)
    }
}
