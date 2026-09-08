use super::{BufferFlags, BufferMain};
use core::sync::atomic::Ordering;

impl BufferMain {
    #[doc(hidden)]
    pub fn default_pool(&self, numa_node: u32) -> u8 {
        let pool = self.default_pool_by_numa[numa_node as usize];
        assert_ne!(pool, u8::MAX, "NUMA node has a Buffer Pool");
        pool
    }

    #[doc(hidden)]
    pub fn alloc_from_pool(&self, thread_index: u32, indices: &mut [u32], pool_index: u8) -> usize {
        self.pools[usize::from(pool_index)].alloc_indices(thread_index, indices)
    }

    // Traversal has a fixed upper bound from physical Pool capacity, so a
    // malformed cycle cannot strand a Worker in the packet loop.
    fn chain_last(&self, first: u32, exclusive: bool) -> u32 {
        let mut current = first;
        let capacity: usize = self.pools.iter().map(|pool| pool.buffer_count).sum();
        for _ in 0..capacity {
            // SAFETY: this operation retains the caller's chain obligation;
            // no reference escapes and mutation follows the completed borrow.
            let buffer = unsafe { self.buffer(current) };
            if exclusive {
                assert_eq!(buffer.ref_count(), 1, "chain segments are exclusive");
            }
            match buffer.next_buffer_slot() {
                Some(next) => current = next,
                None => return current,
            }
        }
        panic!("Buffer chain is acyclic");
    }

    #[doc(hidden)]
    pub fn chain_init(&self, first: u32) {
        // SAFETY: the caller owns the exclusive chain head.
        let buffer = unsafe { self.buffer_mut(first) };
        assert!(
            buffer.next_buffer_slot().is_none(),
            "chain init cannot discard a retained tail"
        );
        buffer.cacheline0.current_length = 0;
        buffer.set_next_buffer(None);
        buffer
            .set_total_len_not_including_first(0)
            .expect("zero chain length");
    }

    #[doc(hidden)]
    pub fn chain_link(&self, last: u32, next: u32) -> u32 {
        assert_ne!(last, next, "chain segments are distinct");
        assert_eq!(
            self.chain_last(last, true),
            last,
            "last segment has no next"
        );
        assert_eq!(
            self.chain_last(next, true),
            next,
            "appended segment has no retained tail"
        );
        // SAFETY: both exclusive unchained segments were validated above;
        // their borrows are sequential and their indices are distinct.
        unsafe {
            let buffer = self.buffer_mut(next);
            buffer.cacheline0.current_length = 0;
            buffer.set_next_buffer(None);
            self.buffer_mut(last).set_next_buffer(Some(next));
        }
        next
    }

    #[doc(hidden)]
    pub fn chain_append(&self, first: u32, last: u32, data: &[u8]) -> usize {
        assert_eq!(
            self.chain_last(first, true),
            last,
            "last belongs to the exclusive chain"
        );
        self.append_last(first, last, data)
    }

    fn append_last(&self, first: u32, last: u32, data: &[u8]) -> usize {
        // SAFETY: traversal established exclusive ownership and the last segment.
        let tail = unsafe { self.buffer(last) };
        let count = data
            .len()
            .min(tail.space_left_at_end())
            .min(u16::MAX as usize - tail.current_len());
        let total = if first != last {
            // SAFETY: the same validated chain retains first during this operation.
            unsafe { self.buffer(first) }
                .total_len_not_including_first()
                .checked_add(count)
                .filter(|len| u32::try_from(*len).is_ok())
                .expect("chain length fits u32")
        } else {
            0
        };
        // SAFETY: no shared Buffer borrows survive the mutation; both segments
        // are exclusive and the data slice belongs to the caller.
        unsafe {
            self.buffer_mut(last)
                .put_uninit(count as u16)
                .copy_from_slice(&data[..count]);
            if first != last {
                self.buffer_mut(first).total_length_not_including_first = total as u32;
            }
        }
        count
    }

    #[doc(hidden)]
    pub fn chain_append_with_alloc(
        &self,
        thread_index: u32,
        first: u32,
        last: &mut u32,
        data: &[u8],
    ) -> usize {
        let pool_index = self.pool(first).index;
        self.append_from_pool(thread_index, first, last, data, pool_index)
    }

    fn append_from_pool(
        &self,
        thread_index: u32,
        first: u32,
        last: &mut u32,
        data: &[u8],
        pool_index: u8,
    ) -> usize {
        assert_eq!(
            self.chain_last(first, true),
            *last,
            "last belongs to the exclusive chain"
        );
        let mut copied = 0;
        while copied < data.len() {
            copied += self.append_last(first, *last, &data[copied..]);
            if copied == data.len() {
                break;
            }
            let mut next = [0];
            if self.alloc_from_pool(thread_index, &mut next, pool_index) == 0 {
                break;
            }
            *last = self.chain_link(*last, next[0]);
        }
        copied
    }

    #[doc(hidden)]
    pub fn add_data(
        &self,
        thread_index: u32,
        numa_node: u32,
        head: &mut u32,
        data: &[u8],
    ) -> usize {
        let pool_index = self.default_pool(numa_node);
        if *head == u32::MAX
            && self.alloc_from_pool(thread_index, core::slice::from_mut(head), pool_index) == 0
        {
            return 0;
        }
        let mut last = self.chain_last(*head, true);
        // SAFETY: validation retains the exclusive chain; add_data deliberately
        // invalidates the cached total and does not use its retained bytes.
        unsafe { self.buffer_mut(*head) }
            .cacheline0
            .flags
            .remove(BufferFlags::TOTAL_LENGTH_VALID);
        let mut copied = 0;
        loop {
            // SAFETY: last is the exclusive last segment, updated immediately
            // after each allocation is linked into the caller's chain.
            let buffer = unsafe { self.buffer_mut(last) };
            let count = (data.len() - copied)
                .min(buffer.space_left_at_end())
                .min(u16::MAX as usize - buffer.current_len());
            buffer
                .put_uninit(count as u16)
                .copy_from_slice(&data[copied..copied + count]);
            copied += count;
            if copied == data.len() {
                return copied;
            }
            let mut next = [0];
            if self.alloc_from_pool(thread_index, &mut next, pool_index) == 0 {
                return copied;
            }
            last = self.chain_link(last, next[0]);
        }
    }

    #[doc(hidden)]
    pub fn attach_clone(&self, head: u32, tail: u32) {
        assert_ne!(head, tail, "clone head is distinct from tail");
        assert_eq!(
            self.pool(head).index,
            self.pool(tail).index,
            "clone head and tail belong to the same Pool"
        );
        assert_eq!(self.chain_last(head, true), head, "clone head has no next");
        let tail_last = self.chain_last(tail, false);
        assert_ne!(
            tail_last, head,
            "clone tail cannot contain its unchained head"
        );
        let mut current = Some(tail);
        while let Some(index) = current {
            // SAFETY: caller retains the tail chain while attaching a clone.
            let buffer = unsafe { self.buffer(index) };
            assert!(
                buffer.ref_count() < u8::MAX,
                "clone reference count cannot overflow"
            );
            current = buffer.next_buffer_slot();
        }
        // SAFETY: the validated readable tail remains owned throughout attachment.
        let buffer = unsafe { self.buffer(tail) };
        let total = buffer
            .current_len()
            .checked_add(buffer.total_len_not_including_first())
            .filter(|len| u32::try_from(*len).is_ok())
            .expect("clone chain length fits u32");
        let length_valid = buffer.flags().contains(BufferFlags::TOTAL_LENGTH_VALID);
        current = Some(tail);
        while let Some(index) = current {
            // SAFETY: only the atomic reference count changes on shared tails.
            let buffer = unsafe { self.buffer(index) };
            // AcqRel publishes the retained reference; the matching free decrement
            // acquires prior releases before the final owner restores the template.
            buffer
                .cacheline0
                .ref_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    count.checked_add(1)
                })
                .expect("clone reference count cannot overflow");
            current = buffer.next_buffer_slot();
        }
        // SAFETY: the validated unchained head is exclusively owned.
        let head = unsafe { self.buffer_mut(head) };
        head.set_next_buffer(Some(tail));
        head.set_total_len_not_including_first(total)
            .expect("validated clone length");
        head.cacheline0.flags.remove(BufferFlags::EXT_HDR_VALID);
        if !length_valid {
            head.cacheline0
                .flags
                .remove(BufferFlags::TOTAL_LENGTH_VALID);
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    // vlib_test.c forces a full head before chain append. Pressure and clone
    // assertions below are derived from buffer.c and buffer_funcs.h themselves.
    pub(in crate::buffer) fn allocation_chains_and_reference_release() {
        let main = BufferMain::global();
        let mut heads = [0; 2];
        assert_eq!(main.alloc_from_pool(1, &mut heads, 0), 2);
        let mut tail = [0];
        assert_eq!(main.alloc_from_pool(1, &mut tail, 1), 1);
        main.chain_init(heads[0]);
        assert_eq!(main.chain_append(heads[0], heads[0], &[1, 2, 3, 4]), 4);
        assert_eq!(main.chain_link(heads[0], tail[0]), tail[0]);
        assert_eq!(main.chain_append(heads[0], tail[0], &[5, 6, 7, 8]), 4);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                main.attach_clone(heads[1], tail[0]);
            }))
            .is_err()
        );
        // SAFETY: this test exclusively owns heads[0]; manufacture the exact
        // one-byte counter boundary to verify rejection before head publication.
        unsafe { main.buffer(heads[0]) }
            .cacheline0
            .ref_count
            .store(u8::MAX, Ordering::Relaxed);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                main.attach_clone(heads[1], heads[0]);
            }))
            .is_err()
        );
        // SAFETY: the rejected attachment retained both original obligations.
        unsafe { main.buffer(heads[0]) }
            .cacheline0
            .ref_count
            .store(1, Ordering::Relaxed);
        main.attach_clone(heads[1], heads[0]);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // SAFETY: attempt only the checked borrow boundary; it must reject
                // this shared segment before returning any mutable reference.
                unsafe { main.buffer_mut(heads[0]) };
            }))
            .is_err()
        );
        // SAFETY: the test owns both head obligations; these temporary borrows
        // finish before either release and no shared tail mutation occurs.
        unsafe {
            assert_eq!(main.buffer(heads[0]).ref_count(), 2);
            assert_eq!(main.buffer(tail[0]).ref_count(), 2);
            assert_eq!(main.buffer(heads[1]).total_len_not_including_first(), 8);
        }
        main.free_buffers(1, &heads[..1], true, |_| {});
        // SAFETY: the clone still retains both tail segments.
        unsafe {
            assert_eq!(main.buffer(heads[0]).ref_count(), 1);
            assert_eq!(main.buffer(tail[0]).current(), &[5, 6, 7, 8]);
        }
        main.free_buffers(1, &heads[1..], true, |_| {});
        let mut recycled = [0];
        assert_eq!(main.alloc_from_pool(1, &mut recycled, 1), 1);
        assert_eq!(recycled, tail);
        main.free_buffers(1, &recycled, false, |_| {});
        #[cfg(debug_assertions)]
        {
            // buffer.c::vlib_buffer_validate_alloc_free rejects a repeated
            // release before publishing the same index to a free cache again.
            let cached_free = main.cached_free_buffers(1, 1);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    main.free_buffers(1, &recycled, false, |_| {});
                }))
                .is_err()
            );
            assert_eq!(main.cached_free_buffers(1, 1), cached_free);
        }

        let capacity = main.pools[0].buffer_count;
        let mut retained = vec![u32::MAX; capacity + 1];
        assert_eq!(main.alloc_from_pool(1, &mut retained, 0), capacity);
        assert_eq!(retained[capacity], u32::MAX);
        let mut unavailable = [u32::MAX; 2];
        assert_eq!(main.alloc_from_pool(1, &mut unavailable, 0), 0);
        assert_eq!(unavailable, [u32::MAX; 2]);
        main.free_buffers(1, &retained[capacity - 1..capacity], false, |_| {});
        let data_size = main.pools[0].data_size;
        let data = vec![0x5a; data_size + 1];
        let mut head = u32::MAX;
        assert_eq!(main.add_data(1, 0, &mut head, &data), data_size);
        assert_ne!(head, u32::MAX);
        // SAFETY: add_data published its partial chain into head even though
        // the next allocation failed; the test retains it until explicit free.
        unsafe {
            assert_eq!(main.buffer(head).current(), &data[..data_size]);
            assert!(
                !main
                    .buffer(head)
                    .flags()
                    .contains(BufferFlags::TOTAL_LENGTH_VALID)
            );
            assert_eq!(main.buffer(head).next_buffer_slot(), None);
        }
        main.free_buffers(1, &[head], true, |_| {});
        main.free_buffers(1, &retained[..capacity - 1], false, |_| {});
    }
}
