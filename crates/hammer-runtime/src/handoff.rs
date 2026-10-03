use std::cell::UnsafeCell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::{DataPlaneMain, NodeRuntime, ThreadMain};
use hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY;
use hammer_core::data_plane::{Frame, NodeId};
use hammer_infra::align::{CACHE_LINE, CacheLineAlignMark};
use hammer_infra::mask_compare::{compress_u32, mask_compare_u16};

pub const HANDOFF_SLOT_INDICES: usize = 32;
const HANDOFF_INVALID_INDEX: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DataWorkerId(u32);

impl DataWorkerId {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }

    #[inline(always)]
    pub const fn thread_index(self) -> u32 {
        self.0.saturating_add(1)
    }
}

impl From<DataWorkerId> for usize {
    #[inline]
    fn from(worker: DataWorkerId) -> Self {
        worker.slot()
    }
}

impl TryFrom<u32> for DataWorkerId {
    type Error = crate::error::RuntimeError;

    #[inline(always)]
    fn try_from(thread_index: u32) -> Result<Self, Self::Error> {
        thread_index
            .checked_sub(1)
            .map(Self)
            .ok_or(crate::error::RuntimeError::DataWorkerIdUnavailable { thread_index })
    }
}

// VPP vlib/handoff.h: one aligned 128-byte slot for 32 Buffer indices.
#[repr(C, align(128))]
pub(crate) union HandoffQueueSlot {
    pub(crate) buffer_indices: [u32; HANDOFF_SLOT_INDICES],
    as_u32x4: [wide::u32x4; HANDOFF_SLOT_INDICES / 4],
    as_u32x8: [wide::u32x8; HANDOFF_SLOT_INDICES / 8],
    as_u32x16: [wide::u32x16; HANDOFF_SLOT_INDICES / 16],
}

const _: () = {
    assert!(core::mem::size_of::<HandoffQueueSlot>() == 128);
    assert!(core::mem::align_of::<HandoffQueueSlot>() == 128);
};

#[repr(C, align(64))]
pub(crate) struct HandoffQueue {
    pub(crate) dequeue_vector_limit: usize,
    pub(crate) size: usize,
    cacheline1: CacheLineAlignMark,
    pub(crate) tail: AtomicU64,
    pub(crate) trace_stop: AtomicU64,
    pub(crate) n_dropped: AtomicU64,
    cacheline2: CacheLineAlignMark,
    pub(crate) head: AtomicU64,
    pub(crate) n_vectors: AtomicU64,
    pub(crate) data: Box<[UnsafeCell<HandoffQueueSlot>]>,
}

const _: () = {
    assert!(core::mem::align_of::<HandoffQueue>() == CACHE_LINE);
    assert!(core::mem::offset_of!(HandoffQueue, tail) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(HandoffQueue, head) % CACHE_LINE == 0);
    assert!(core::mem::offset_of!(HandoffQueue, tail) != core::mem::offset_of!(HandoffQueue, head));
};

// SAFETY: a producer writes only slots it reserved by tail CAS. The first
// index is release-published last; the consumer acquires it before reading
// the slot and release-advances head before any producer can reuse it.
unsafe impl Sync for HandoffQueue {}

impl HandoffQueue {
    pub(crate) fn new(size: usize, dequeue_vector_limit: usize) -> Self {
        assert!(size.is_power_of_two());
        assert!(dequeue_vector_limit != 0);
        let data = (0..size * 2)
            .map(|_| {
                UnsafeCell::new(HandoffQueueSlot {
                    buffer_indices: [0; HANDOFF_SLOT_INDICES],
                })
            })
            .collect::<Box<[_]>>();
        Self {
            dequeue_vector_limit,
            size,
            cacheline1: CacheLineAlignMark,
            tail: AtomicU64::new(0),
            trace_stop: AtomicU64::new(0),
            n_dropped: AtomicU64::new(0),
            cacheline2: CacheLineAlignMark,
            head: AtomicU64::new(0),
            n_vectors: AtomicU64::new(0),
            data,
        }
    }
}

pub(crate) struct HandoffQueueMain {
    pub(crate) index: u32,
    pub(crate) node_index: NodeId,
    pub(crate) size: usize,
    pub(crate) queue_bit: u64,
    pub(crate) with_aux: bool,
    pub(crate) queues_by_thread: Box<[HandoffQueue]>,
}

pub struct HandoffAllocQueuesArgs {
    pub node_index: NodeId,
    pub queue_size: u32,
    pub dequeue_vector_limit: u32,
}

#[inline(always)]
pub(crate) fn slot_first(slot: &UnsafeCell<HandoffQueueSlot>) -> &AtomicU32 {
    // SAFETY: the union starts with an aligned u32, initialized for every
    // slot. Only atomic access touches this word while producer and consumer
    // may run concurrently.
    unsafe { AtomicU32::from_ptr(core::ptr::addr_of_mut!((*slot.get()).buffer_indices).cast()) }
}

/// Baseline slot-prefix reader, corresponding to VPP's 128-bit branch.
#[inline]
pub(crate) unsafe fn copy_indices_from_slot(
    destination: &mut [u32],
    slot: &UnsafeCell<HandoffQueueSlot>,
) -> usize {
    assert!(destination.len() >= HANDOFF_SLOT_INDICES);
    assert_ne!(
        slot_first(slot).load(Ordering::Acquire),
        HANDOFF_INVALID_INDEX
    );
    let source = unsafe { &(*slot.get()).buffer_indices };
    let vectors = unsafe { &(*slot.get()).as_u32x4 };
    let invalid = wide::u32x4::splat(HANDOFF_INVALID_INDEX);
    let output = destination.as_mut_ptr().cast::<wide::u32x4>();
    if vectors[7].simd_eq(invalid).to_bitmask() == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), vectors[3]);
            core::ptr::write_unaligned(output.add(4), vectors[4]);
            core::ptr::write_unaligned(output.add(5), vectors[5]);
            core::ptr::write_unaligned(output.add(6), vectors[6]);
            core::ptr::write_unaligned(output.add(7), vectors[7]);
        }
        return HANDOFF_SLOT_INDICES;
    }
    let mut count = 0;
    loop {
        let vector = vectors[count / 4];
        if vector.simd_eq(invalid).to_bitmask() != 0 {
            break;
        }
        unsafe { core::ptr::write_unaligned(output.add(count / 4), vector) };
        count += 4;
    }
    while count < HANDOFF_SLOT_INDICES && source[count] != HANDOFF_INVALID_INDEX {
        destination[count] = source[count];
        count += 1;
    }
    count
}

/// VPP's 256-bit slot-prefix branch without masked stores.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
pub(crate) unsafe fn copy_indices_from_slot_v3(
    destination: &mut [u32],
    slot: &UnsafeCell<HandoffQueueSlot>,
) -> usize {
    assert!(destination.len() >= HANDOFF_SLOT_INDICES);
    assert_ne!(
        slot_first(slot).load(Ordering::Acquire),
        HANDOFF_INVALID_INDEX
    );
    let source = unsafe { &(*slot.get()).buffer_indices };
    let vectors = unsafe { &(*slot.get()).as_u32x8 };
    let invalid = wide::u32x8::splat(HANDOFF_INVALID_INDEX);
    let output = destination.as_mut_ptr().cast::<wide::u32x8>();
    if unsafe { vectors[3].is_equal_mask_avx2(invalid) } == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), vectors[3]);
        }
        return HANDOFF_SLOT_INDICES;
    }
    let mut count = 0;
    loop {
        let vector = vectors[count / 8];
        if unsafe { vector.is_equal_mask_avx2(invalid) } != 0 {
            break;
        }
        unsafe { core::ptr::write_unaligned(output.add(count / 8), vector) };
        count += 8;
    }
    while count < HANDOFF_SLOT_INDICES && source[count] != HANDOFF_INVALID_INDEX {
        destination[count] = source[count];
        count += 1;
    }
    count
}

/// VPP's 256-bit slot-prefix branch with AVX-512VL masked stores.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512vl")]
#[inline]
pub(crate) unsafe fn copy_indices_from_slot_v4(
    destination: &mut [u32],
    slot: &UnsafeCell<HandoffQueueSlot>,
) -> usize {
    assert!(destination.len() >= HANDOFF_SLOT_INDICES);
    assert_ne!(
        slot_first(slot).load(Ordering::Acquire),
        HANDOFF_INVALID_INDEX
    );
    let vectors = unsafe { &(*slot.get()).as_u32x8 };
    let invalid = wide::u32x8::splat(HANDOFF_INVALID_INDEX);
    let output = destination.as_mut_ptr().cast::<wide::u32x8>();
    if unsafe { vectors[3].is_equal_mask_avx512vl(invalid) } == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), vectors[3]);
        }
        return HANDOFF_SLOT_INDICES;
    }
    let mut count = 0;
    loop {
        let vector = vectors[count / 8];
        let mask = unsafe { vector.is_equal_mask_avx512vl(invalid) };
        if mask != 0 {
            unsafe { vector.mask_store_avx512vl(&mut destination[count..], !mask) };
            return count + mask.trailing_zeros() as usize;
        }
        unsafe { core::ptr::write_unaligned(output.add(count / 8), vector) };
        count += 8;
    }
}

#[hammer_component_macros::march_functions(
    entries = [handoff_queues_dequeue, buffer_enqueue_to_thread,
               buffer_enqueue_to_single_thread, buffer_enqueue_to_thread_with_aux],
    slot_copy = [copy_indices_from_slot, copy_indices_from_slot_v3,
                 copy_indices_from_slot_v4],
)]
mod handoff_arch {
    /// Copy a reserved batch, then publish its first Buffer index last.
    /// VPP: vlib_handoff_queue_copy_and_publish_indices_to_slots.
    #[inline]
    pub(super) unsafe fn copy_and_publish_indices_to_slots(
        queue: &HandoffQueue,
        start: u64,
        indices: &[u32],
    ) {
        assert!(!indices.is_empty());
        let mask = queue.size - 1;
        let first_slot_index = (start as usize) & mask;
        let first_slot = &queue.data[first_slot_index];
        let first = indices[0];
        assert_ne!(first, HANDOFF_INVALID_INDEX);
        let first_count = indices.len().min(HANDOFF_SLOT_INDICES);
        // The tail reservation grants this producer its slots; the first word
        // stays atomic and invalid until the final Release store.
        let first_words =
            unsafe { core::ptr::addr_of_mut!((*first_slot.get()).buffer_indices).cast::<u32>() };
        if first_count < HANDOFF_SLOT_INDICES {
            unsafe { core::ptr::write_bytes(first_words.add(1), 0, HANDOFF_SLOT_INDICES - 1) };
        }
        unsafe {
            core::ptr::copy_nonoverlapping(
                indices.as_ptr().add(1),
                first_words.add(1),
                first_count - 1,
            )
        };
        let mut copied = first_count;
        let mut slot_index = (first_slot_index + 1) & mask;
        while indices.len() - copied >= HANDOFF_SLOT_INDICES {
            let slot = &queue.data[slot_index];
            let destination =
                unsafe { core::ptr::addr_of_mut!((*slot.get()).buffer_indices).cast::<u32>() };
            unsafe {
                core::ptr::copy_nonoverlapping(
                    indices.as_ptr().add(copied),
                    destination,
                    HANDOFF_SLOT_INDICES,
                )
            };
            copied += HANDOFF_SLOT_INDICES;
            slot_index = (slot_index + 1) & mask;
        }
        if copied < indices.len() {
            let slot = &queue.data[slot_index];
            let destination =
                unsafe { core::ptr::addr_of_mut!((*slot.get()).buffer_indices).cast::<u32>() };
            unsafe {
                core::ptr::write_bytes(destination, 0, HANDOFF_SLOT_INDICES);
                core::ptr::copy_nonoverlapping(
                    indices.as_ptr().add(copied),
                    destination,
                    indices.len() - copied,
                );
            }
        }
        slot_first(first_slot).store(first, Ordering::Release);
    }

    /// VPP vlib_handoff_enqueue_one_thread: one tail reservation for a group.
    #[inline]
    unsafe fn handoff_enqueue_one_thread(
        runtime: &mut DataPlaneMain,
        node: &NodeRuntime,
        queue_index: u32,
        indices: &[u32],
        thread_index: u16,
        with_aux: bool,
        aux: Option<&[u32]>,
    ) -> usize {
        if indices.is_empty() {
            return 0;
        }
        let (accepted, queue_bit, wake) = {
            let directory = runtime.handoff_queue_mains.borrow();
            let main = &directory[queue_index as usize];
            assert_eq!(main.with_aux, with_aux);
            assert_eq!(with_aux, aux.is_some());
            let worker_slot = usize::from(thread_index.checked_sub(1).expect("handoff targets a Data Worker"));
            let queue = &main.queues_by_thread[worker_slot];
            let requested = indices.len().div_ceil(HANDOFF_SLOT_INDICES);
            let (start, slots, head) = loop {
                let tail = queue.tail.load(Ordering::Relaxed);
                let head = queue.head.load(Ordering::Acquire);
                // Independent snapshots may straddle a completed dequeue; the
                // tail CAS still decides whether this reservation is current.
                if head > tail {
                    std::hint::spin_loop();
                    continue;
                }
                let available = (queue.size as u64).saturating_sub(tail - head) as usize;
                let slots = requested.min(available);
                if slots == 0 {
                    break (tail, 0, head);
                }
                if queue
                    .tail
                    .compare_exchange_weak(
                        tail,
                        tail + slots as u64,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break (tail, slots, head);
                }
            };
            let accepted = if slots == requested {
                indices.len()
            } else {
                slots * HANDOFF_SLOT_INDICES
            };
            if slots != 0 {
                if with_aux {
                    let aux = aux.expect("aux queue has aux indices");
                    assert_eq!(aux.len(), indices.len());
                    for (offset, chunk) in aux[..accepted].chunks(HANDOFF_SLOT_INDICES).enumerate()
                    {
                        let slot_index = ((start as usize) + offset) & (queue.size - 1);
                        let slot = &queue.data[queue.size + slot_index];
                        let destination = unsafe { &mut (*slot.get()).buffer_indices };
                        unsafe {
                            core::ptr::copy_nonoverlapping(
                                chunk.as_ptr(),
                                destination.as_mut_ptr(),
                                chunk.len(),
                            )
                        };
                    }
                }
                if node.trace_enabled() {
                    queue
                        .trace_stop
                        .fetch_max(start + slots as u64, Ordering::Relaxed);
                }
                unsafe { copy_and_publish_indices_to_slots(queue, start, &indices[..accepted]) };
            }
            let dropped = indices.len() - accepted;
            if dropped != 0 {
                queue.n_dropped.fetch_add(dropped as u64, Ordering::Relaxed);
            }
            (accepted, main.queue_bit, slots != 0 && start == head)
        };
        if accepted < indices.len() {
            runtime.buffer_free(&indices[accepted..]);
        }
        if accepted != 0 {
            let target = ThreadMain::global()
                .thread_by_index(u32::from(thread_index))
                .expect("registered handoff destination thread");
            target
                .handoff_pending_bmp()
                .fetch_or(queue_bit, Ordering::SeqCst);
            if wake {
                target.wake_for_runtime();
            }
        }
        accepted
    }

    #[inline]
    unsafe fn buffer_enqueue_to_thread_inline(
        runtime: &mut DataPlaneMain,
        node: &NodeRuntime,
        queue: u32,
        indices: &[u32],
        threads: &[u16],
        with_aux: bool,
        aux: Option<&[u32]>,
    ) -> usize {
        assert_eq!(with_aux, aux.is_some());
        let mut used = [0u64; 4];
        let mut mask = [0u64; 4];
        let mut grouped = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut grouped_aux = [0u32; DEFAULT_BUFFER_FRAME_CAPACITY];
        let mut accepted = 0;
        let mut remaining = indices.len();
        let mut first = 0;
        while remaining != 0 {
            let thread = threads[first];
            let count = mask_compare_u16(thread, threads, &mut mask) as usize;
            assert_eq!(compress_u32(&mut grouped, indices, &mask), count);
            let grouped_aux = aux.map(|aux| {
                assert_eq!(compress_u32(&mut grouped_aux, aux, &mask), count);
                &grouped_aux[..count]
            });
            accepted += unsafe {
                handoff_enqueue_one_thread(
                    runtime,
                    node,
                    queue,
                    &grouped[..count],
                    thread,
                    with_aux,
                    grouped_aux,
                )
            };
            for word in 0..4 {
                used[word] |= mask[word];
            }
            remaining -= count;
            if remaining != 0 {
                let word = used
                    .iter()
                    .position(|&bits| bits != u64::MAX)
                    .expect("unhandled frame element exists");
                first = word * 64 + (!used[word]).trailing_zeros() as usize;
                assert!(first < indices.len());
            }
        }
        accepted
    }

    /// VPP vlib_buffer_enqueue_to_thread: transfer one Frame by target thread.
    pub(super) unsafe fn buffer_enqueue_to_thread(
        runtime: &mut DataPlaneMain,
        node: &NodeRuntime,
        queue: u32,
        indices: &[u32],
        threads: &[u16],
    ) -> usize {
        assert_eq!(indices.len(), threads.len());
        assert!(!runtime.handoff_queue_mains.borrow()[queue as usize].with_aux);
        indices
            .chunks(DEFAULT_BUFFER_FRAME_CAPACITY)
            .zip(threads.chunks(DEFAULT_BUFFER_FRAME_CAPACITY))
            .map(|(indices, threads)| unsafe {
                buffer_enqueue_to_thread_inline(runtime, node, queue, indices, threads, false, None)
            })
            .sum()
    }

    /// VPP vlib_buffer_enqueue_to_single_thread.
    pub(super) unsafe fn buffer_enqueue_to_single_thread(
        runtime: &mut DataPlaneMain,
        node: &NodeRuntime,
        queue: u32,
        indices: &[u32],
        thread: u16,
    ) -> usize {
        assert!(!runtime.handoff_queue_mains.borrow()[queue as usize].with_aux);
        indices
            .chunks(DEFAULT_BUFFER_FRAME_CAPACITY)
            .map(|indices| unsafe {
                handoff_enqueue_one_thread(runtime, node, queue, indices, thread, false, None)
            })
            .sum()
    }

    /// VPP vlib_buffer_enqueue_to_thread_with_aux.
    pub(super) unsafe fn buffer_enqueue_to_thread_with_aux(
        runtime: &mut DataPlaneMain,
        node: &NodeRuntime,
        queue: u32,
        indices: &[u32],
        aux: &[u32],
        threads: &[u16],
    ) -> usize {
        assert_eq!(indices.len(), aux.len());
        assert_eq!(indices.len(), threads.len());
        assert!(runtime.handoff_queue_mains.borrow()[queue as usize].with_aux);
        indices
            .chunks(DEFAULT_BUFFER_FRAME_CAPACITY)
            .zip(aux.chunks(DEFAULT_BUFFER_FRAME_CAPACITY))
            .zip(threads.chunks(DEFAULT_BUFFER_FRAME_CAPACITY))
            .map(|((indices, aux), threads)| unsafe {
                buffer_enqueue_to_thread_inline(
                    runtime,
                    node,
                    queue,
                    indices,
                    threads,
                    true,
                    Some(aux),
                )
            })
            .sum()
    }

    #[inline]
    unsafe fn handoff_queue_dequeue_inline(
        runtime: &DataPlaneMain,
        main: &HandoffQueueMain,
        with_aux: bool,
    ) -> usize {
        assert_eq!(main.with_aux, with_aux);
        let worker_slot = runtime
            .data_worker_id()
            .expect("handoff dequeue runs only on a Data Worker")
            .slot();
        let queue = &main.queues_by_thread[worker_slot];
        let scalar_size = runtime
            .nodes
            .frame_args_size(main.node_index)
            .expect("registered handoff destination")
            .0;
        let mut head = queue.head.load(Ordering::Relaxed);
        let mut count = 0;
        let mut frame: Option<Box<Frame>> = None;
        while count < queue.dequeue_vector_limit {
            let slot_index = (head as usize) & (queue.size - 1);
            let slot = &queue.data[slot_index];
            if slot_first(slot).load(Ordering::Acquire) == HANDOFF_INVALID_INDEX {
                break;
            }
            if frame.as_ref().is_some_and(|frame| {
                DEFAULT_BUFFER_FRAME_CAPACITY - frame.len() < HANDOFF_SLOT_INDICES
            }) {
                runtime
                    .put_frame_to_node(main.node_index, frame.take().unwrap())
                    .expect("registered handoff destination accepts its Frame");
            }
            let current = frame.get_or_insert_with(|| {
                runtime
                    .get_frame_to_node(main.node_index)
                    .expect("registered handoff destination allocates a Frame")
            });
            let offset = current.len();
            if queue.trace_stop.load(Ordering::Relaxed) > head {
                current.frame_flags |= 1 << 5;
            }
            let written = if with_aux {
                let (vectors, aux) = current.next_args_mut::<u32, u32>(scalar_size);
                let written = unsafe { slot_copy(vectors, slot) };
                let aux_slot = &queue.data[queue.size + slot_index];
                let source = unsafe { &(*aux_slot.get()).buffer_indices };
                let destination = &mut aux.expect("u32 aux layout")[..written];
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        source.as_ptr(),
                        destination.as_mut_ptr(),
                        written,
                    )
                };
                written
            } else {
                let (vectors, _) = current.next_args_mut::<u32, ()>(scalar_size);
                unsafe { slot_copy(vectors, slot) }
            };
            current.set_vector_count(offset + written);
            if current.len() == DEFAULT_BUFFER_FRAME_CAPACITY {
                runtime
                    .put_frame_to_node(main.node_index, frame.take().unwrap())
                    .expect("registered handoff destination accepts its Frame");
            }
            slot_first(slot).store(HANDOFF_INVALID_INDEX, Ordering::Relaxed);
            head += 1;
            count += written;
        }
        if count != 0 {
            queue.n_vectors.fetch_add(count as u64, Ordering::Relaxed);
            queue.head.store(head, Ordering::Release);
            if count >= queue.dequeue_vector_limit {
                runtime
                    .handoff_queue_pending_bmp
                    .fetch_or(main.queue_bit, Ordering::Relaxed);
            }
        }
        if let Some(frame) = frame {
            runtime
                .put_frame_to_node(main.node_index, frame)
                .expect("registered handoff destination accepts its Frame");
        }
        count
    }

    /// VPP vlib_handoff_queues_dequeue_fn, before File poll and graph dispatch.
    pub(super) unsafe fn handoff_queues_dequeue(runtime: &mut DataPlaneMain) {
        let pending = runtime.handoff_queue_pending_bmp.swap(0, Ordering::Relaxed);
        if pending == 0 {
            return;
        }
        let mut count = 0;
        let directory = runtime.handoff_queue_mains.borrow();
        if pending == u64::MAX {
            for queue in directory.iter() {
                count += unsafe { handoff_queue_dequeue_inline(runtime, queue, queue.with_aux) };
            }
        } else {
            let mut bits = pending;
            while bits != 0 {
                let index = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let queue = &directory[index];
                count += unsafe { handoff_queue_dequeue_inline(runtime, queue, queue.with_aux) };
            }
        }
        drop(directory);
        if count != 0 {
            runtime.file_poll_no_sleep_epolls = 512;
        }
    }
}

type EnqueueToThread = unsafe fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], &[u16]) -> usize;
type EnqueueToSingleThread = unsafe fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], u16) -> usize;
type EnqueueToThreadWithAux =
    unsafe fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], &[u32], &[u16]) -> usize;

struct BufferFunctionMain {
    buffer_enqueue_to_thread: EnqueueToThread,
    buffer_enqueue_to_single_thread: EnqueueToSingleThread,
    buffer_enqueue_to_thread_with_aux: EnqueueToThreadWithAux,
}

static BUFFER_FUNCTION_MAIN: OnceLock<BufferFunctionMain> = OnceLock::new();

pub(crate) fn init_buffer_functions() {
    assert!(
        BUFFER_FUNCTION_MAIN
            .set(BufferFunctionMain {
                buffer_enqueue_to_thread: select_buffer_enqueue_to_thread(),
                buffer_enqueue_to_single_thread: select_buffer_enqueue_to_single_thread(),
                buffer_enqueue_to_thread_with_aux: select_buffer_enqueue_to_thread_with_aux(),
            })
            .is_ok(),
        "Buffer functions initialize once before Worker launch"
    );
}

#[inline]
pub fn buffer_enqueue_to_thread(
    runtime: &mut DataPlaneMain,
    node: &NodeRuntime,
    queue: u32,
    indices: &[u32],
    threads: &[u16],
) -> usize {
    let functions = BUFFER_FUNCTION_MAIN
        .get()
        .expect("Buffer functions initialize before dispatch");
    unsafe { (functions.buffer_enqueue_to_thread)(runtime, node, queue, indices, threads) }
}

#[inline]
pub fn buffer_enqueue_to_single_thread(
    runtime: &mut DataPlaneMain,
    node: &NodeRuntime,
    queue: u32,
    indices: &[u32],
    thread: u16,
) -> usize {
    let functions = BUFFER_FUNCTION_MAIN
        .get()
        .expect("Buffer functions initialize before dispatch");
    unsafe { (functions.buffer_enqueue_to_single_thread)(runtime, node, queue, indices, thread) }
}

#[inline]
pub fn buffer_enqueue_to_thread_with_aux(
    runtime: &mut DataPlaneMain,
    node: &NodeRuntime,
    queue: u32,
    indices: &[u32],
    aux: &[u32],
    threads: &[u16],
) -> usize {
    let functions = BUFFER_FUNCTION_MAIN
        .get()
        .expect("Buffer functions initialize before dispatch");
    unsafe {
        (functions.buffer_enqueue_to_thread_with_aux)(runtime, node, queue, indices, aux, threads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeDescriptor;
    use crate::trace::{HandoffTrace, HandoffTraceNode, TRACE_INDEX_LIMIT, TRACE_THREAD_SHIFT};
    use hammer_core::data_plane::{NodeKind, NodeRegistration};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

    #[repr(C)]
    #[derive(KnownLayout, FromBytes, IntoBytes, Immutable)]
    struct HandoffPacketTrace {
        buffer_index: u32,
    }

    fn handoff_trace_target(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        assert!(node.trace_enabled());
        for &index in frame.vector_args() {
            if runtime.buffer(index).trace_handle().is_some() {
                runtime
                    .add_trace::<HandoffPacketTrace>(node, index)
                    .expect("traced Buffer receives a destination record")
                    .buffer_index = index;
            }
        }
        frame.len()
    }

    fn handoff_target(
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        assert!(
            node.trace_enabled(),
            "trace_stop marks the destination Frame"
        );
        let count = frame.len();
        runtime.buffer_free(frame.vector_args());
        count
    }

    struct HandoffAuxNode;

    impl HandoffAuxNode {
        const NODE_NAME: &'static str = "handoff-aux-target";
    }

    #[hammer_component_macros::node_function(node = HandoffAuxNode)]
    fn handoff_aux_target(
        runtime: &mut DataPlaneMain,
        _: &mut NodeRuntime,
        frame: &mut Frame<(), u32, u32>,
    ) -> usize {
        let indices = frame.vector_args();
        let aux = frame.aux_args().expect("destination Frame has u32 aux");
        assert_eq!(aux, &[11, 17, 23]);
        let count = indices.len();
        runtime.buffer_free(indices);
        count
    }

    fn handoff_aux_fallback(_: &mut DataPlaneMain, _: &mut NodeRuntime, _: &mut Frame) -> usize {
        panic!("typed handoff destination is installed before dispatch")
    }

    #[test]
    fn dequeue_writes_aux_into_the_destination_frame() {
        if !crate::run_buffer_test_process(
            "handoff::tests::dequeue_writes_aux_into_the_destination_frame",
        ) {
            return;
        }
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        let target = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_aux_fallback,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next(HandoffAuxNode::NODE_NAME, 0)),
                    &[],
                    None,
                    false,
                )
                .with_frame_args::<(), u32, u32>(),
            )
            .unwrap();
        runtime
            .nodes()
            .install_node_function(
                target,
                __NODE_FUNCTION_HANDOFF_AUX_TARGET_VARIANTS.iter().copied(),
                handoff_aux_fallback,
            )
            .unwrap();
        let queue = HandoffQueue::new(2, 64);
        let mut indices = [0u32; 3];
        assert_eq!(runtime.buffer_alloc(&mut indices), indices.len());
        let aux_slot = &queue.data[queue.size];
        let destination = unsafe { &mut (*aux_slot.get()).buffer_indices };
        destination[0] = 11;
        destination[1] = 17;
        destination[2] = 23;
        queue.tail.store(1, Ordering::Relaxed);
        unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 0, &indices) };
        runtime
            .handoff_queue_mains
            .borrow_mut()
            .push(Arc::new(HandoffQueueMain {
                index: 0,
                node_index: target,
                size: 2,
                queue_bit: 1,
                with_aux: true,
                queues_by_thread: Box::new([queue]),
            }));
        runtime.handoff_queue_pending_bmp.store(1, Ordering::SeqCst);
        unsafe { select_handoff_queues_dequeue()(&mut runtime) };
        assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
        assert_eq!(runtime.nodes().frames_in_use(), 0);
        crate::finish_buffer_test_process();
    }

    #[test]
    fn dequeue_schedules_a_traced_destination_frame() {
        if !crate::run_buffer_test_process(
            "handoff::tests::dequeue_schedules_a_traced_destination_frame",
        ) {
            return;
        }
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        let target = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_target,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("handoff-target", 0)),
                    &[],
                    None,
                    true,
                ),
            )
            .unwrap();
        let queue = HandoffQueue::new(2, 64);
        let mut indices = [0u32; 33];
        assert_eq!(runtime.buffer_alloc(&mut indices), indices.len());
        queue.tail.store(2, Ordering::Relaxed);
        queue.trace_stop.store(2, Ordering::Relaxed);
        unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 0, &indices) };
        runtime
            .handoff_queue_mains
            .borrow_mut()
            .push(Arc::new(HandoffQueueMain {
                index: 0,
                node_index: target,
                size: 2,
                queue_bit: 1,
                with_aux: false,
                queues_by_thread: Box::new([queue]),
            }));
        runtime.handoff_queue_pending_bmp.store(1, Ordering::SeqCst);
        unsafe { select_handoff_queues_dequeue()(&mut runtime) };
        assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
        {
            let directory = runtime.handoff_queue_mains.borrow();
            let queue = &directory[0].queues_by_thread[0];
            assert_eq!(queue.head.load(Ordering::Acquire), 2);
            assert_eq!(queue.n_vectors.load(Ordering::Relaxed), 33);
        }
        assert_eq!(runtime.nodes().frames_in_use(), 0);
        crate::finish_buffer_test_process();
    }

    #[test]
    fn handoff_dispatch_records_only_traced_buffers_on_the_destination() {
        if !crate::run_buffer_test_process(
            "handoff::tests::handoff_dispatch_records_only_traced_buffers_on_the_destination",
        ) {
            return;
        }
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut source = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        let mut destination = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 2,
            ..Default::default()
        });
        let source_next = source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_trace_target,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("handoff-source-next", 0)),
                    &[],
                    None,
                    true,
                ),
            )
            .unwrap();
        let source_node = source
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_trace_target,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("handoff-source", 1)),
                    &[source_next],
                    None,
                    true,
                ),
            )
            .unwrap();
        let target = destination
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_trace_target,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("handoff-trace-target", 0)),
                    &[],
                    None,
                    true,
                ),
            )
            .unwrap();
        destination.handoff_trace_node = destination
            .nodes()
            .try_register_internal_with_next_names(HandoffTraceNode, &["handoff-trace-target"])
            .unwrap();
        destination.nodes().resolve_named_next_nodes().unwrap();
        source.trace_main.trace_enable = true;
        destination.trace_main.trace_enable = true;

        let mut indices = [0u32; 2];
        assert_eq!(source.buffer_alloc(&mut indices), indices.len());
        let mut head = indices[0];
        assert_eq!(source.buffer_add_data(&mut head, b"handoff"), 7);
        let packet_address = source.buffer(indices[0]).current().as_ptr();
        let source_runtime = source.nodes().node_runtime_data(source_node).unwrap();
        assert!(source.trace_buffer(&source_runtime, 0, indices[0], false));
        let source_handle = source.buffer(indices[0]).trace_handle().unwrap();
        assert!(source.buffer(indices[1]).trace_handle().is_none());

        let queue = HandoffQueue::new(1, HANDOFF_SLOT_INDICES);
        queue.tail.store(1, Ordering::Relaxed);
        queue.trace_stop.store(1, Ordering::Relaxed);
        unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 0, &indices) };
        destination
            .handoff_queue_mains
            .borrow_mut()
            .push(Arc::new(HandoffQueueMain {
                index: 0,
                node_index: target,
                size: 1,
                queue_bit: 1,
                with_aux: false,
                queues_by_thread: Box::new([
                    HandoffQueue::new(1, HANDOFF_SLOT_INDICES),
                    queue,
                ]),
            }));
        destination
            .handoff_queue_pending_bmp
            .store(1, Ordering::SeqCst);
        unsafe { select_handoff_queues_dequeue()(&mut destination) };
        assert_eq!(destination.run_ready_nodes().unwrap(), 1);

        let target_handle = destination.buffer(indices[0]).trace_handle().unwrap();
        assert_eq!(target_handle >> TRACE_THREAD_SHIFT, 2);
        assert_eq!(
            destination.buffer(indices[0]).current().as_ptr(),
            packet_address
        );
        assert_eq!(destination.buffer(indices[0]).current(), b"handoff");
        assert!(destination.buffer(indices[1]).trace_handle().is_none());
        assert_eq!(
            source
                .trace_main
                .trace_buffer_pool
                .get(source_handle & TRACE_INDEX_LIMIT)
                .unwrap()
                .len(),
            0
        );
        let records = destination
            .trace_main
            .trace_buffer_pool
            .get(target_handle & TRACE_INDEX_LIMIT)
            .unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].node_index, destination.handoff_trace_node.slot());
        let (handoff, _) = HandoffTrace::ref_from_prefix(records[1..2].as_bytes()).unwrap();
        assert_eq!(handoff.prev_thread, 1);
        assert_eq!(handoff.prev_trace_index, source_handle & TRACE_INDEX_LIMIT);
        assert_eq!(records[2].node_index, target.slot());
        let (packet, _) = HandoffPacketTrace::ref_from_prefix(records[3..4].as_bytes()).unwrap();
        assert_eq!(packet.buffer_index, indices[0]);
        destination.buffer_free(&indices);
        crate::finish_buffer_test_process();
    }

    #[test]
    fn queue_sixty_four_scans_the_directory_and_dispatches_its_frame() {
        if !crate::run_buffer_test_process(
            "handoff::tests::queue_sixty_four_scans_the_directory_and_dispatches_its_frame",
        ) {
            return;
        }
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        let target = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    handoff_target,
                    NodeRuntime::empty(),
                    Some(NodeRegistration::next("handoff-all-queues-target", 0)),
                    &[],
                    None,
                    true,
                ),
            )
            .unwrap();
        let mut index = [0u32; 1];
        assert_eq!(runtime.buffer_alloc(&mut index), 1);
        for queue_index in 0..=64 {
            let queue = HandoffQueue::new(1, HANDOFF_SLOT_INDICES);
            if queue_index == 64 {
                queue.tail.store(1, Ordering::Relaxed);
                queue.trace_stop.store(1, Ordering::Relaxed);
                unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 0, &index) };
            }
            runtime
                .handoff_queue_mains
                .borrow_mut()
                .push(Arc::new(HandoffQueueMain {
                    index: queue_index,
                    node_index: target,
                    size: 1,
                    queue_bit: if queue_index < 64 {
                        1u64 << queue_index
                    } else {
                        u64::MAX
                    },
                    with_aux: false,
                    queues_by_thread: Box::new([queue]),
                }));
        }
        runtime
            .handoff_queue_pending_bmp
            .store(u64::MAX, Ordering::SeqCst);
        unsafe { select_handoff_queues_dequeue()(&mut runtime) };
        assert_eq!(runtime.run_ready_nodes().unwrap(), 1);
        let directory = runtime.handoff_queue_mains.borrow();
        assert_eq!(
            directory[64].queues_by_thread[0]
                .head
                .load(Ordering::Acquire),
            1
        );
        assert_eq!(runtime.nodes().frames_in_use(), 0);
        crate::finish_buffer_test_process();
    }

    #[test]
    fn slot_prefix_preserves_all_batch_boundaries() {
        let queue = HandoffQueue::new(8, 256);
        for length in [1_usize, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 256] {
            let indices = (1..=length as u32).collect::<Vec<u32>>();
            let start = queue.tail.load(Ordering::Relaxed);
            let slots = length.div_ceil(HANDOFF_SLOT_INDICES);
            queue.tail.store(start + slots as u64, Ordering::Relaxed);
            unsafe {
                handoff_arch_base::copy_and_publish_indices_to_slots(&queue, start, &indices)
            };
            let mut received = Vec::new();
            for offset in 0..slots {
                let slot = &queue.data[((start as usize) + offset) & (queue.size - 1)];
                let expected = (length - offset * HANDOFF_SLOT_INDICES).min(HANDOFF_SLOT_INDICES);
                let mut baseline = [u32::MAX; HANDOFF_SLOT_INDICES];
                let count = unsafe { copy_indices_from_slot(&mut baseline, slot) };
                assert_eq!(count, expected);
                assert!(baseline[count..].iter().all(|&index| index == u32::MAX));
                #[cfg(target_arch = "x86_64")]
                {
                    if std::is_x86_feature_detected!("avx2") {
                        let mut v3 = [u32::MAX; HANDOFF_SLOT_INDICES];
                        assert_eq!(unsafe { copy_indices_from_slot_v3(&mut v3, slot) }, count);
                        assert_eq!(v3, baseline);
                    }
                    if std::is_x86_feature_detected!("avx2")
                        && std::is_x86_feature_detected!("avx512f")
                        && std::is_x86_feature_detected!("avx512vl")
                    {
                        let mut v4 = [u32::MAX; HANDOFF_SLOT_INDICES];
                        assert_eq!(unsafe { copy_indices_from_slot_v4(&mut v4, slot) }, count);
                        assert_eq!(v4, baseline);
                    }
                }
                received.extend_from_slice(&baseline[..count]);
                slot_first(slot).store(HANDOFF_INVALID_INDEX, Ordering::Relaxed);
            }
            queue.head.store(start + slots as u64, Ordering::Release);
            assert_eq!(received, indices);
        }
    }

    #[test]
    fn later_published_slot_does_not_expose_an_unpublished_prefix() {
        let queue = HandoffQueue::new(2, 64);
        queue.tail.store(2, Ordering::Relaxed);
        let later = [41, 42];
        unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 1, &later) };
        assert_eq!(
            slot_first(&queue.data[0]).load(Ordering::Acquire),
            HANDOFF_INVALID_INDEX
        );
        let earlier = [11, 12];
        unsafe { handoff_arch_base::copy_and_publish_indices_to_slots(&queue, 0, &earlier) };
        let mut received = [0; HANDOFF_SLOT_INDICES];
        assert_eq!(
            unsafe { copy_indices_from_slot(&mut received, &queue.data[0]) },
            2
        );
        assert_eq!(&received[..2], &earlier);
        assert_eq!(
            unsafe { copy_indices_from_slot(&mut received, &queue.data[1]) },
            2
        );
        assert_eq!(&received[..2], &later);
    }

    #[test]
    fn full_queue_frees_rejected_buffers_and_counts_them() {
        if !crate::run_buffer_test_process(
            "handoff::tests::full_queue_frees_rejected_buffers_and_counts_them",
        ) {
            return;
        }
        crate::BUFFER_MAIN_INIT.call_once(|| {
            hammer_core::buffer::BufferMain::new(
                64,
                1024,
                &[0],
                3,
                hammer_infra::PageSize::Default,
            )
            .unwrap();
        });
        let mut runtime = DataPlaneMain::new(crate::DataPlaneBufferConfig {
            thread_index: 1,
            ..Default::default()
        });
        let queue = HandoffQueue::new(1, HANDOFF_SLOT_INDICES);
        let mut indices = [0u32; 3];
        assert_eq!(runtime.buffer_alloc(&mut indices), indices.len());
        queue.tail.store(1, Ordering::Relaxed);
        slot_first(&queue.data[0]).store(indices[0], Ordering::Release);
        runtime
            .handoff_queue_mains
            .borrow_mut()
            .push(Arc::new(HandoffQueueMain {
                index: 0,
                node_index: NodeId::new(0),
                size: 1,
                queue_bit: 1,
                with_aux: false,
                queues_by_thread: Box::new([queue]),
            }));
        let cached_before = runtime.cached_free_buffers();
        let accepted = unsafe {
            handoff_arch_base::buffer_enqueue_to_single_thread(
                &mut runtime,
                &NodeRuntime::empty(),
                0,
                &indices[1..],
                1,
            )
        };
        assert_eq!(accepted, 0);
        assert_eq!(runtime.cached_free_buffers(), cached_before + 2);
        {
            let directory = runtime.handoff_queue_mains.borrow();
            let queue = &directory[0].queues_by_thread[0];
            assert_eq!(queue.n_dropped.load(Ordering::Relaxed), 2);
        }
        runtime.buffer_free_one(indices[0]);
        crate::finish_buffer_test_process();
    }

    #[test]
    fn multiple_producers_publish_only_complete_slots() {
        const PRODUCERS: u32 = 4;
        const BATCHES: u32 = 64;
        let queue = Arc::new(HandoffQueue::new(8, 256));
        let deadline = Instant::now() + Duration::from_secs(10);
        std::thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let queue = Arc::clone(&queue);
                scope.spawn(move || {
                    for batch in 0..BATCHES {
                        let first = 1 + (producer * BATCHES + batch) * HANDOFF_SLOT_INDICES as u32;
                        let indices: [u32; HANDOFF_SLOT_INDICES] =
                            core::array::from_fn(|lane| first + lane as u32);
                        let start = loop {
                            let tail = queue.tail.load(Ordering::Relaxed);
                            let head = queue.head.load(Ordering::Acquire);
                            if head > tail {
                                std::hint::spin_loop();
                                continue;
                            }
                            if tail - head >= queue.size as u64 {
                                assert!(Instant::now() < deadline, "consumer stalled");
                                std::hint::spin_loop();
                                continue;
                            }
                            if queue
                                .tail
                                .compare_exchange_weak(
                                    tail,
                                    tail + 1,
                                    Ordering::Relaxed,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                break tail;
                            }
                        };
                        unsafe {
                            handoff_arch_base::copy_and_publish_indices_to_slots(
                                &queue, start, &indices,
                            )
                        };
                    }
                });
            }
            let total = PRODUCERS as usize * BATCHES as usize * HANDOFF_SLOT_INDICES;
            let mut received = Vec::with_capacity(total);
            let mut head = 0u64;
            while received.len() < total {
                let slot = &queue.data[(head as usize) & (queue.size - 1)];
                if slot_first(slot).load(Ordering::Acquire) == HANDOFF_INVALID_INDEX {
                    assert!(Instant::now() < deadline, "producer stalled");
                    std::hint::spin_loop();
                    continue;
                }
                let mut indices = [0; HANDOFF_SLOT_INDICES];
                assert_eq!(
                    unsafe { copy_indices_from_slot(&mut indices, slot) },
                    HANDOFF_SLOT_INDICES
                );
                received.extend_from_slice(&indices);
                slot_first(slot).store(HANDOFF_INVALID_INDEX, Ordering::Relaxed);
                head += 1;
                queue.head.store(head, Ordering::Release);
            }
            received.sort_unstable();
            assert_eq!(received, (1..=received.len() as u32).collect::<Vec<_>>());
        });
    }
}
