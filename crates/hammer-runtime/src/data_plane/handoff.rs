use super::*;
use crate::handoff::{
    HANDOFF_SLOT_INDICES, HandoffAllocQueuesArgs, HandoffQueue, HandoffQueueMain, slot_first,
};
use hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY;
use std::sync::atomic::Ordering;

/// VPP vlib_handoff_queue_copy_pending_slots, called while workers are stopped.
fn handoff_queue_copy_pending_slots(
    main: &HandoffQueueMain,
    old: &HandoffQueue,
    next: &mut HandoffQueue,
    pending: usize,
) {
    let head = old.head.load(Ordering::Relaxed);
    let tail = head + pending as u64;
    next.dequeue_vector_limit = old.dequeue_vector_limit;
    next.head.store(head, Ordering::Relaxed);
    next.tail.store(tail, Ordering::Relaxed);
    next.trace_stop.store(
        old.trace_stop.load(Ordering::Relaxed).min(tail),
        Ordering::Relaxed,
    );
    next.n_dropped
        .store(old.n_dropped.load(Ordering::Relaxed), Ordering::Relaxed);
    next.n_vectors
        .store(old.n_vectors.load(Ordering::Relaxed), Ordering::Relaxed);
    for offset in 0..pending {
        let old_slot = ((head as usize) + offset) & (old.size - 1);
        let next_slot = ((head as usize) + offset) & (next.size - 1);
        let source = unsafe {
            core::ptr::addr_of!((*old.data[old_slot].get()).buffer_indices).cast::<u32>()
        };
        let destination = unsafe {
            core::ptr::addr_of_mut!((*next.data[next_slot].get()).buffer_indices).cast::<u32>()
        };
        unsafe { core::ptr::copy_nonoverlapping(source, destination, HANDOFF_SLOT_INDICES) };
        if main.with_aux {
            let source = unsafe {
                core::ptr::addr_of!((*old.data[old.size + old_slot].get()).buffer_indices)
                    .cast::<u32>()
            };
            let destination = unsafe {
                core::ptr::addr_of_mut!((*next.data[next.size + next_slot].get()).buffer_indices)
                    .cast::<u32>()
            };
            unsafe { core::ptr::copy_nonoverlapping(source, destination, HANDOFF_SLOT_INDICES) };
        }
    }
}

impl DataPlaneMain {
    /// VPP vlib_handoff_alloc_queues. The target Node and all destination
    /// rings are established before the queue index becomes visible.
    pub fn handoff_alloc_queues(&self, args: HandoffAllocQueuesArgs) -> RuntimeResult<u32> {
        crate::ensure_main_thread_with_barrier()?;
        let queue_size = if args.queue_size == 0 {
            4096
        } else {
            args.queue_size
        };
        let dequeue_vector_limit = if args.dequeue_vector_limit == 0 {
            2 * DEFAULT_BUFFER_FRAME_CAPACITY as u32
        } else {
            args.dequeue_vector_limit
        };
        assert!(queue_size >= HANDOFF_SLOT_INDICES as u32 && queue_size.is_power_of_two());
        let (_, vector_size, aux_size) = self.nodes.frame_args_size(args.node_index)?;
        assert_eq!(
            vector_size, 4,
            "handoff destination Frame carries Buffer indices"
        );
        assert!(aux_size == 0 || aux_size == 4, "handoff aux values are u32");
        let size = queue_size as usize / HANDOFF_SLOT_INDICES;
        let mut directory = self.handoff_queue_mains.borrow_mut();
        let index = u32::try_from(directory.len()).expect("handoff queue count fits u32");
        let queue_bit = if index < 64 { 1u64 << index } else { u64::MAX };
        let threads = crate::ThreadMain::global();
        let queues_by_thread = (0..threads.worker_count())
            .map(|_| HandoffQueue::new(size, dequeue_vector_limit as usize))
            .collect::<Box<[_]>>();
        let queue = Arc::new(HandoffQueueMain {
            index,
            node_index: args.node_index,
            size,
            queue_bit,
            with_aux: aux_size != 0,
            queues_by_thread,
        });
        directory.push(Arc::clone(&queue));
        drop(directory);
        threads.set_handoff_queue(queue);
        Ok(index)
    }

    /// VPP vlib_handoff_queue_resize. Validate before replacing the directory
    /// while the Worker Barrier stops consumers.
    pub fn handoff_queue_resize(&mut self, index: u32, queue_size: u32) -> RuntimeResult<()> {
        crate::ensure_main_thread_with_barrier()?;
        let directory = self.handoff_queue_mains.get_mut();
        let old = directory
            .get(index as usize)
            .ok_or(RuntimeError::HandoffQueueIndexInvalid {
                index,
                queue_count: directory.len(),
            })?;
        if queue_size < HANDOFF_SLOT_INDICES as u32 || !queue_size.is_power_of_two() {
            return Err(RuntimeError::HandoffQueueSizeInvalid {
                size: queue_size,
                minimum: HANDOFF_SLOT_INDICES,
            });
        }
        let size = queue_size as usize / HANDOFF_SLOT_INDICES;
        let mut replacement = Vec::with_capacity(old.queues_by_thread.len());
        for (worker_slot, old_queue) in old.queues_by_thread.iter().enumerate() {
            let head = old_queue.head.load(Ordering::Relaxed);
            let tail = old_queue.tail.load(Ordering::Relaxed);
            let pending = (head..tail)
                .take_while(|sequence| {
                    let slot = &old_queue.data[(*sequence as usize) & (old_queue.size - 1)];
                    slot_first(slot).load(Ordering::Acquire) != 0
                })
                .count();
            assert!(
                pending as u64 == tail - head,
                "barrier cannot interrupt a producer reservation"
            );
            assert!(
                pending <= old_queue.size,
                "queue reservation exceeds ring capacity"
            );
            if pending > size {
                return Err(RuntimeError::HandoffQueueCapacityInsufficient {
                    index,
                    thread_index: worker_slot as u32 + 1,
                    pending: pending * HANDOFF_SLOT_INDICES,
                    size: queue_size,
                });
            }
            let mut next = HandoffQueue::new(size, old_queue.dequeue_vector_limit);
            handoff_queue_copy_pending_slots(&old, old_queue, &mut next, pending);
            replacement.push(next);
        }
        let queue = Arc::new(HandoffQueueMain {
            index,
            node_index: old.node_index,
            size,
            queue_bit: old.queue_bit,
            with_aux: old.with_aux,
            queues_by_thread: replacement.into_boxed_slice(),
        });
        directory[index as usize] = Arc::clone(&queue);
        crate::ThreadMain::global().set_handoff_queue(queue);
        Ok(())
    }

    pub fn run_ready_nodes(&mut self) -> RuntimeResult<usize> {
        self.run_ready_function_nodes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resize_rejects_invalid_inputs_without_replacing_queue() {
        crate::thread_main::install_main_thread().unwrap();
        let mut runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
        assert!(matches!(
            runtime.handoff_queue_resize(0, 32),
            Err(RuntimeError::HandoffQueueIndexInvalid {
                index: 0,
                queue_count: 0
            })
        ));

        let old_queue = HandoffQueue::new(2, 64);
        old_queue.tail.store(2, Ordering::Relaxed);
        slot_first(&old_queue.data[0]).store(11, Ordering::Release);
        slot_first(&old_queue.data[1]).store(12, Ordering::Release);
        let old = Arc::new(HandoffQueueMain {
            index: 0,
            node_index: NodeId::new(0),
            size: 2,
            queue_bit: 1,
            with_aux: false,
            queues_by_thread: Box::new([old_queue]),
        });
        runtime
            .handoff_queue_mains
            .borrow_mut()
            .push(Arc::clone(&old));

        assert!(matches!(
            runtime.handoff_queue_resize(0, 33),
            Err(RuntimeError::HandoffQueueSizeInvalid {
                size: 33,
                minimum: HANDOFF_SLOT_INDICES
            })
        ));
        assert!(matches!(
            runtime.handoff_queue_resize(0, 32),
            Err(RuntimeError::HandoffQueueCapacityInsufficient {
                index: 0,
                thread_index: 1,
                pending: 64,
                size: 32
            })
        ));
        assert!(Arc::ptr_eq(&runtime.handoff_queue_mains.borrow()[0], &old));
        assert_eq!(old.queues_by_thread[0].head.load(Ordering::Relaxed), 0);
        assert_eq!(old.queues_by_thread[0].tail.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn resize_copy_keeps_wrapped_indices_aux_and_counters() {
        let main = HandoffQueueMain {
            index: 0,
            node_index: NodeId::new(0),
            size: 2,
            queue_bit: 1,
            with_aux: true,
            queues_by_thread: Box::new([]),
        };
        let old = HandoffQueue::new(2, 64);
        old.head.store(1, Ordering::Relaxed);
        old.tail.store(3, Ordering::Relaxed);
        old.trace_stop.store(9, Ordering::Relaxed);
        old.n_dropped.store(5, Ordering::Relaxed);
        old.n_vectors.store(7, Ordering::Relaxed);
        unsafe {
            (*old.data[1].get()).buffer_indices = [11; HANDOFF_SLOT_INDICES];
            (*old.data[0].get()).buffer_indices = [17; HANDOFF_SLOT_INDICES];
            (*old.data[old.size + 1].get()).buffer_indices = [23; HANDOFF_SLOT_INDICES];
            (*old.data[old.size].get()).buffer_indices = [29; HANDOFF_SLOT_INDICES];
        }
        let mut next = HandoffQueue::new(4, 128);
        handoff_queue_copy_pending_slots(&main, &old, &mut next, 2);
        assert_eq!(next.dequeue_vector_limit, 64);
        assert_eq!(next.head.load(Ordering::Relaxed), 1);
        assert_eq!(next.tail.load(Ordering::Relaxed), 3);
        assert_eq!(next.trace_stop.load(Ordering::Relaxed), 3);
        assert_eq!(next.n_dropped.load(Ordering::Relaxed), 5);
        assert_eq!(next.n_vectors.load(Ordering::Relaxed), 7);
        unsafe {
            assert_eq!(
                (*next.data[1].get()).buffer_indices,
                [11; HANDOFF_SLOT_INDICES]
            );
            assert_eq!(
                (*next.data[2].get()).buffer_indices,
                [17; HANDOFF_SLOT_INDICES]
            );
            assert_eq!(
                (*next.data[next.size + 1].get()).buffer_indices,
                [23; HANDOFF_SLOT_INDICES]
            );
            assert_eq!(
                (*next.data[next.size + 2].get()).buffer_indices,
                [29; HANDOFF_SLOT_INDICES]
            );
        }
    }
}
