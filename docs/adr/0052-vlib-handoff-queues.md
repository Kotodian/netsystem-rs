# ADR-0052: VLIB-style Worker Handoff Queues

Status: proposed. The handoff design is not implemented. The local
`third_party/wide` fork contains compare-mask and masked load/store entry
points; complete native unaligned vector load/store and handoff callers are
still required.

## Scope and source

This ADR replaces Hammer's `DataPlaneHandoff`, per-worker
`ArrayQueue<HandoffFrame>`, `HandoffSlot { len, indices }`, per-Node remote
interrupt bits, `handoff_index`, and `handoff_frame`. It covers the whole
generic VLIB handoff path, not VNET's policy for selecting a worker. A handoff
transfers ownership of Buffer **indices**, not a copy of packet bytes.

| VPP source | Contract used here |
| --- | --- |
| `third_party/vpp/src/vlib/main.h:232-237` | Each `vlib_main_t` directly has `handoff_queue_mains` and `handoff_queue_pending_bmp`. |
| `src/vlib/handoff.h:10-82` | One 128-byte slot holds 32 indices; a queue main names the target Node and owns one ring per runtime thread. Producer and consumer fields occupy separate cache lines. |
| `src/vlib/handoff.c:40-218` | Slot-prefix copy, acquire on the first index, bounded dequeue straight into a destination Frame, aux copy, trace watermark and head advance. |
| `src/vlib/handoff.c:221-264` | Bitmap exchange dispatches only indicated queues; all-ones scans every queue; dequeue activity delays File sleep. |
| `src/vlib/handoff.c:268-426` | Reserve multiple slots with one tail CAS, copy aux then indices, publish the first index last, free the rejected suffix, set bitmap, wake on empty-to-nonempty. |
| `src/vlib/handoff.c:428-553` | Three batch enqueue entry points: per-packet thread, single thread, and per-packet thread plus aux. Each full Frame and final partial Frame is handled separately. |
| `src/vlib/handoff.c:556-752` | Allocate all thread rings before publishing a queue index; resize copies the published prefix while workers cannot use the old rings. |
| `src/vlib/buffer_funcs.c:391-415`, `src/vlib/buffer_node.h:441-475`, `src/vlib/main.c:1444-1452` | The three enqueue entries belong to the process-global Buffer function main; dequeue is selected into a **loop-local** variable. None is a `vlib_main_t` field. |
| `src/vlib/buffer_funcs.h:134-173` | `vlib_buffer_copy_indices` delegates to `clib_memcpy_u32`; the ring variant splits a wrapped aux copy into at most two index copies. |
| `src/cmake/cpu.cmake:182-197`, `src/vppinfra/cpu.h:375-394` | The default x86 variants are baseline v2, v3 and v4. The v4 selector does not require BITALG. |
| `src/vppinfra/vector.h:23-34,196-204`, `src/vppinfra/vector_avx512.h:294-301` | AVX2 enables 256-bit vectors. BITALG enables `CLIB_HAVE_VEC512`; AVX-512F/VL also makes 256-bit masked load/store available without BITALG. |
| `src/vppinfra/vector_avx512.h:236-266,371-391` | 16-lane compare-mask returns `u16`; 16-lane masked load/store needs AVX-512F, 8-lane masked load/store needs AVX-512VL. |
| `src/vppinfra/memcpy.h:55-194` | Full `u32` copies use unaligned vectors; BITALG builds use a 16-lane masked tail, default v4 uses an 8-lane masked tail, and v3/base finish with smaller vectors and a scalar tail. |
| `src/vlib/main.c:1496-1625`, `src/vlib/file.c:108-221` | RPC, worker barrier, handoff dequeue, worker callbacks, File poll, then graph dispatch; File advertises sleep and checks the bitmap again before a timed wait. |
| `src/vnet/handoff.c:99-204,317-345` | Interface handoff is an internal Node: choose a worker for every input Buffer, enqueue the whole Frame once, then count congestion drops. Its queue targets `ethernet-input`. |
| `src/vnet/ip/reass/ip4_full_reass.c:1242-1279,1604-1616,1886-2038` | Reassembly sets owner thread in Buffer metadata and sends to a distinct handoff Node for each entry path; each queue targets its corresponding reassembly Node. |
| `src/vnet/tcp/tcp_inlines.h:282-367`, `src/vnet/tcp/tcp_input.c:2729-2745,2780-2891` | TCP lookup reports wrong-thread and TCP input drops it. VPP has no TCP-input handoff Node. |

Current Hammer evidence: `crates/hammer-runtime/src/handoff.rs` owns a
different per-worker `ArrayQueue`; `src/data_plane/handoff.rs` enqueues
one index/slot at a time and builds a Frame later; `src/main_loop.rs:275-367`
uses remote Node interrupts and drains after File readiness. TCP input and IP
reassembly call these old APIs. The target is not a SIMD patch on that design:
those containers and callers must be replaced together.

## Ownership and layout

`handoff_queue_mains` belongs **directly to each `DataPlaneMain`**, just as
it belongs to each `vlib_main_t`. There is no `HandoffMain`, central queue
registry or handoff scheduler Node. Entry `i` has the same identity in all
thread mains and may target the same Node as another entry. One entry owns
`thread_count` MPSC rings, indexed by **destination runtime thread index**,
including thread 0. All mains share the registered entry and rings; each main
owns its own pending bitmap. The only fixed cross-thread reference in
`ThreadMain` is to those bitmap atomics, so a producer never borrows another
thread's mutable `DataPlaneMain`.

```rust
// hammer-runtime::data_plane; VPP vlib/main.h:232-237.
struct DataPlaneMain {
    // Existing fields omitted.
    handoff_queue_mains: RefCell<Vec<Arc<HandoffQueueMain>>>,
    handoff_queue_pending_bmp: Arc<AtomicU64>,
    file_poll_no_sleep_epolls: u32,
}

// The existing thread descriptor retains only a reference to its main's bit.
// VPP obtains the target vlib_main_t by thread index instead.
struct WorkerThread {
    // Existing fields omitted.
    handoff_queue_pending_bmp: Arc<AtomicU64>,
}

impl WorkerThread {
    #[inline(always)]
    fn handoff_pending_bmp(&self) -> &AtomicU64 {
        &self.handoff_queue_pending_bmp
    }
}

// hammer-runtime::handoff; VPP vlib/handoff.h:10-82.
const HANDOFF_SLOT_INDICES: usize = 32;
const HANDOFF_INVALID_INDEX: u32 = 0; // BufferMain never allocates index 0.

#[repr(C, align(128))]
union HandoffQueueSlot {
    buffer_indices: [u32; HANDOFF_SLOT_INDICES],
    as_u32x4: [wide::u32x4; HANDOFF_SLOT_INDICES / 4],
    as_u32x8: [wide::u32x8; HANDOFF_SLOT_INDICES / 8],
    as_u32x16: [wide::u32x16; HANDOFF_SLOT_INDICES / 16],
}

#[repr(C, align(64))]
struct HandoffQueue {
    dequeue_vector_limit: usize,
    size: usize, // power-of-two number of slots, not Buffer indices
    cacheline1: CacheLineAlignMark,
    tail: AtomicU64,
    trace_stop: AtomicU64,
    n_dropped: AtomicU64,
    cacheline2: CacheLineAlignMark,
    head: AtomicU64,
    n_vectors: AtomicU64,
    data: Box<[UnsafeCell<HandoffQueueSlot>]>, // indices, then parallel aux slots
}

struct HandoffQueueMain {
    index: u32,
    node_index: NodeId,
    size: usize,
    queue_bit: u64,
    with_aux: bool,
    queues_by_thread: Box<[HandoffQueue]>,
}

pub struct HandoffAllocQueuesArgs {
    pub node_index: NodeId,
    pub queue_size: u32,           // index capacity; 0 means 4096
    pub dequeue_vector_limit: u32, // 0 means 2 * FRAME_VECTOR_CAPACITY
}
```

There is no `HandoffQueueIndex` wrapper, `HandoffMarchFns` aggregate,
handoff frame carrier or second queue-directory owner. Queue identity is the
VPP-style `u32` index. `RefCell` is required only because existing
`NodeEntry::init` receives `&DataPlaneMain`; packet-path `&mut
DataPlaneMain` uses `get_mut()`, not `borrow_mut()`. `Arc` retains the
shared queue entry through all main lists, not through a per-packet clone.
VPP's `handoff_queue_main_t.with_aux` records the registered destination
Frame layout (`handoff.h:62-71`); the enqueue/dequeue inline `with_aux`
parameter selects the branch in the compiled body (`handoff.c:140-141,340-343`).
Hammer keeps both roles distinct.

The slot layout assertion is `size_of == align_of == 128`; the two
`CacheLineAlignMark` offsets are checked against 64-byte boundaries and
`head` is not on `tail`'s line. VPP uses `0xffff_ffff` as its invalid
Buffer index; Hammer's current Buffer Main explicitly never allocates index
zero (`hammer-core/src/buffer/main.rs:238-239`), so zero is the local slot
sentinel. The sentinel is **not** a packet Buffer.

This is VPP's slot union, with `align(128)` and the same 32-index capacity.
The queue's `UnsafeCell` permits reserved-slot mutation. Its first `u32`
is addressed through `AtomicU32::from_ptr` for Release/Acquire and reset;
full `wide` loads/stores that include it occur only **after** the producer
owns the reserved slot or the consumer has acquired its first word, and
before the consumer release-advances `head`. The producer acquire-loads
`head` before reusing that slot. Thus atomic and non-atomic views of the
first word are never concurrent and are ordered by the queue protocol;
without this proof Rust cannot use the union view safely. Only
`HandoffQueue` needs a private `unsafe impl Sync`, with this invariant
documented beside it. No volatile access or extra lock is involved.

## Slot copy: exact data movement

These slot operations copy only `u32` Buffer identities and optional `u32`
aux values. VPP's free function `vlib_buffer_copy_indices` calls
`clib_memcpy_u32` (`vlib/buffer_funcs.h:134-137`,
`vppinfra/memcpy.h:55-194`); `vlib_handoff_queue_slot_t` is a data-only union,
not a method owner. Hammer does not add a thin `hammer-core::buffer` forwarder.
Rust's own `copy_from_slice` implementation for `Copy` elements checks equal
lengths and calls `ptr::copy_nonoverlapping`; the pointer function delegates
to the compiler intrinsic (installed Rust source docs:
`core/src/slice/mod.rs:5567-5592`, `core/src/ptr/mod.rs:527-549`). The
handoff copies are contiguous, non-overlapping `u32` ranges, so the ADR uses
`ptr::copy_nonoverlapping` where an MPSC slot must not be borrowed as a whole
mutable slice. This gives the compiler its optimized memory-copy lowering;
it does **not** guarantee a particular AVX width without inspecting emitted
code. No custom contiguous-copy API is added to Hammer or `wide`. The local
`wide` fork is used directly
only for the VPP slot-prefix compare/masked-store operations, whose lane masks
are part of the algorithm.

The fork already has `is_equal_mask_sse2`, `is_equal_mask_avx2`,
`is_equal_mask_avx512vl`, and checked AVX-512VL masked stores. The selected
slot reader uses those existing operations directly. It still follows VPP's
128/256-bit branch order for finding the first invalid index; ordinary
contiguous copies use Rust's memory-copy intrinsic, not a second SIMD API.

VPP's aux ring helper splits a wrapped write into two index copies. Hammer
performs those same copies over **only the reserved aux slots** inside the
enqueue body. Forming one mutable slice over the entire MPSC aux ring would
claim exclusivity over slots reserved by other producers, so the ADR does not
add a whole-ring `&mut [u32]` API.

```rust
// VPP handoff.h:10-29, handoff.c:157-160,199,335-336.
impl HandoffQueueSlot {
    fn new() -> Self {
        Self { buffer_indices: [HANDOFF_INVALID_INDEX; 32] }
    }
}

#[inline(always)]
fn slot_first(slot: &UnsafeCell<HandoffQueueSlot>) -> &AtomicU32 {
    // SAFETY: the union starts with an aligned u32. Every competing access
    // follows the tail reservation and head reuse protocol described above.
    unsafe { AtomicU32::from_ptr(slot.get().cast::<u32>()) }
}

// VPP handoff.c:268-337. Private inline body inside handoff_arch.
#[inline]
unsafe fn copy_and_publish_indices_to_slots(
    queue: &HandoffQueue, start: u64, source: &[u32],
) {
    assert!(!source.is_empty());
    let mask = queue.size - 1;
    let first_slot_index = (start as usize) & mask;
    let first = &queue.data[first_slot_index];
    let first_index = source[0];
    let first_count = source.len().min(HANDOFF_SLOT_INDICES);

    // Do not form &mut over word zero: the consumer polls it atomically.
    let words = core::ptr::addr_of_mut!((*first.get()).buffer_indices);
    let tail = unsafe {
        core::slice::from_raw_parts_mut(words.cast::<u32>().add(1), 31)
    };
    if first_count < HANDOFF_SLOT_INDICES {
        tail.fill(HANDOFF_INVALID_INDEX);
    }
    unsafe {
        core::ptr::copy_nonoverlapping(
            source.as_ptr().add(1), tail.as_mut_ptr(), first_count - 1,
        );
    }

    let mut copied = first_count;
    let mut slot_index = (first_slot_index + 1) & mask;
    while source.len() - copied >= HANDOFF_SLOT_INDICES {
        let slot = &queue.data[slot_index];
        let values = unsafe { &mut (*slot.get()).buffer_indices };
        let chunk = &source[copied..copied + HANDOFF_SLOT_INDICES];
        unsafe { core::ptr::copy_nonoverlapping(chunk.as_ptr(), values.as_mut_ptr(), 32) };
        copied += HANDOFF_SLOT_INDICES;
        slot_index = (slot_index + 1) & mask;
    }
    if copied < source.len() {
        let slot = &queue.data[slot_index];
        let values = unsafe { &mut (*slot.get()).buffer_indices };
        values.fill(HANDOFF_INVALID_INDEX);
        let chunk = &source[copied..];
        unsafe {
            core::ptr::copy_nonoverlapping(chunk.as_ptr(), values.as_mut_ptr(), chunk.len())
        };
    }

    // The single Release store publishes all reserved index and aux slots.
    slot_first(first).store(first_index, Ordering::Release);
}
```

The enqueue body writes aux directly into its reserved ring, then calls this
one copy-and-publish operation for Buffer indices. This is VPP's helper
boundary; there is no separate copy-only slot helper or macro. The first
slot stays invalid while its 31 following words are written. Every
later slot can receive its first word with a relaxed store because the first
slot's final release publishes the *whole reservation*. A partial last slot
has invalid padding, so the consumer copies only the valid prefix. The
consumer clears each consumed slot's first word, then release-advances
`head`; a producer acquire-loads `head` before reusing those slots. The
aux ring is parallel, not a second Buffer allocation.

VPP replaces lane zero in its first full vector and stores that whole vector.
Hammer deliberately leaves the first word untouched until the atomic Release
store, because an ordinary SIMD store overlapping the consumer's concurrent
`AtomicU32` load would violate Rust's aliasing and data-race rules. The
remaining 31 words use `ptr::copy_nonoverlapping`, which cannot touch the
atomic first word. The slot-prefix reader still uses `wide` compare masks;
the attribute emits one concrete helper per march variant without a runtime
ISA choice in that loop.

## Queue allocation and resize

The queue directory is populated after a target NodeId exists. This may
occur before workers start or under the existing Worker Barrier. The main
thread first constructs **all** rings, then appends one `Arc<HandoffQueueMain>`
to thread zero and every Worker main in the same index position. VPP copies
the `handoff_queue_mains` vector pointer to every `vlib_main_t` at
`handoff.c:736-749`; Hammer copies the Arc entry because Rust must also
retain its lifetime. `ThreadMain::worker_main_at_barrier` is the existing
access path for live Worker mains. Startup `for_worker` copies the allocated
entries from thread zero; each prebuilt `WorkerThread` descriptor and its
`DataPlaneMain` hold clones of the same per-thread bitmap Arc. The
descriptor creates its Arc when `ThreadMain` builds the fixed thread list;
`DataPlaneMain::new_main(&threads)` takes thread zero's Arc; Worker main
construction takes the Arc indexed by its `thread_index`. There is no
post-launch bitmap setup or per-thread directory.

```rust
// VPP handoff.c:556-573,699-752. Called by the graph registration owner.
impl HandoffQueue {
    fn new(size: usize, dequeue_vector_limit: usize) -> Self {
        assert!(size.is_power_of_two());
        let data = (0..2 * size)
            .map(|_| UnsafeCell::new(HandoffQueueSlot::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
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

impl DataPlaneMain {
    pub fn handoff_alloc_queues(&self, args: HandoffAllocQueuesArgs) -> u32 {
        ensure_main_thread_with_barrier()
            .expect("queue allocation requires the main control scope");
        let queue_size = if args.queue_size == 0 { 4096 } else { args.queue_size };
        assert!(queue_size >= 32 && queue_size.is_power_of_two());
        self.nodes.validate_node(args.node_index)
            .expect("handoff destination was registered");
        let (_, vector_size, aux_size) = self.nodes.frame_args_size(args.node_index)
            .expect("registered destination has Frame layout");
        assert_eq!(vector_size, 4, "handoff Frame vectors are u32");
        assert!(aux_size == 0 || aux_size == 4, "handoff aux is u32");
        let limit = if args.dequeue_vector_limit == 0 {
            2 * FRAME_VECTOR_CAPACITY
        } else {
            args.dequeue_vector_limit as usize
        };
        let index = self.handoff_queue_mains.borrow().len() as u32;
        let queue_bit = if index < 64 { 1u64 << index } else { u64::MAX };
        let size = queue_size as usize / 32;
        let queues_by_thread = (0..ThreadMain::global().thread_count())
            .map(|_| HandoffQueue::new(size, limit))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let entry = Arc::new(HandoffQueueMain {
            index, node_index: args.node_index, size, queue_bit,
            with_aux: aux_size != 0, queues_by_thread,
        });
        // This impl is placed beside ThreadMain::worker_mains so it can
        // iterate the already-owned main array, just as VPP iterates
        // vlib_global_main_t::vlib_mains in handoff.c:742-749.
        let threads = ThreadMain::global();
        let worker_mains = unsafe { &*threads.worker_mains.get() };
        let mut mains = self.handoff_queue_mains.borrow_mut();
        mains.reserve(1);
        for worker in worker_mains {
            let worker_main = unsafe { &mut **worker.get() };
            assert_eq!(worker_main.handoff_queue_mains.get_mut().len(), index as usize);
            worker_main.handoff_queue_mains.get_mut().reserve(1);
        }
        mains.push(Arc::clone(&entry));
        for worker in worker_mains {
            let worker_main = unsafe { &mut **worker.get() };
            worker_main.handoff_queue_mains.get_mut().push(Arc::clone(&entry));
        }
        index
    }
}
```

Before Worker main construction the loop is empty and `for_worker` clones
thread zero's list. During live graph mutation the existing main control
scope stops Worker dispatch before any list changes; no handoff-specific
barrier assertion or second synchronization object is introduced. VPP's
`vlib_handoff_queue_resize` returns `clib_error_t` for an invalid index,
size or capacity below pending slots (`handoff.c:649-677`). This ADR does
not add Hammer handoff Error variants. The runtime copy/replace operation
below is internal and requires its control caller to validate those inputs
while the Worker Barrier is held; failed validation leaves the old directory
unchanged before calling it.

```rust
// VPP handoff.c:599-632. Called only while the Worker Barrier stops queue use.
fn handoff_queue_copy_pending_slots(
    main: &HandoffQueueMain, old: &HandoffQueue,
    new: &mut HandoffQueue, pending: usize,
) {
    let head = old.head.load(Ordering::Relaxed);
    let tail = head + pending as u64;
    new.dequeue_vector_limit = old.dequeue_vector_limit;
    new.head.store(head, Ordering::Relaxed);
    new.tail.store(tail, Ordering::Relaxed);
    new.trace_stop.store(
        old.trace_stop.load(Ordering::Relaxed).min(tail),
        Ordering::Relaxed,
    );
    new.n_dropped.store(old.n_dropped.load(Ordering::Relaxed), Ordering::Relaxed);
    new.n_vectors.store(old.n_vectors.load(Ordering::Relaxed), Ordering::Relaxed);
    for offset in 0..pending {
        let old_slot = ((head as usize) + offset) & (old.size - 1);
        let new_slot = ((head as usize) + offset) & (new.size - 1);
        let source = unsafe { &(*old.data[old_slot].get()).buffer_indices };
        let destination = unsafe { &mut (*new.data[new_slot].get()).buffer_indices };
        unsafe {
            core::ptr::copy_nonoverlapping(source.as_ptr(), destination.as_mut_ptr(), 32)
        };
        if main.with_aux {
            let source = unsafe { &(*old.data[old.size + old_slot].get()).buffer_indices };
            let destination = unsafe { &mut (*new.data[new.size + new_slot].get()).buffer_indices };
            unsafe {
                core::ptr::copy_nonoverlapping(source.as_ptr(), destination.as_mut_ptr(), 32)
            };
        }
    }
}

// VPP handoff.c:576-695. Called on main under the Worker Barrier.
impl DataPlaneMain {
    fn handoff_queue_resize(&mut self, index: u32, queue_size: u32) {
        ensure_main_thread_with_barrier()
            .expect("queue resize requires the main control scope");
        let old = self.handoff_queue_mains.get_mut()
            .get(index as usize)
            .expect("control caller validated handoff queue index");
        assert!(queue_size >= 32 && queue_size.is_power_of_two(),
            "control caller validated handoff queue size");
        let size = queue_size as usize / 32;
        let mut replacement = Vec::with_capacity(old.queues_by_thread.len());
        for old_queue in old.queues_by_thread.iter() {
            let head = old_queue.head.load(Ordering::Relaxed);
            let tail = old_queue.tail.load(Ordering::Relaxed);
            let mask = old_queue.size - 1;
            let pending = (head..tail)
                .take_while(|sequence| {
                    slot_first(&old_queue.data[(*sequence as usize) & mask])
                        .load(Ordering::Acquire) != HANDOFF_INVALID_INDEX
                })
                .count();
            assert_eq!(pending as u64, tail - head,
                "barrier cannot interrupt a producer's reservation");
            assert!(pending <= size, "control caller validated pending slot capacity");
            let mut next = HandoffQueue::new(size, old_queue.dequeue_vector_limit);
            handoff_queue_copy_pending_slots(old, old_queue, &mut next, pending);
            replacement.push(next);
        }
        let next = Arc::new(HandoffQueueMain {
            index, node_index: old.node_index, size, queue_bit: old.queue_bit,
            with_aux: old.with_aux, queues_by_thread: replacement.into_boxed_slice(),
        });
        let threads = ThreadMain::global();
        let worker_mains = unsafe { &*threads.worker_mains.get() };
        self.handoff_queue_mains.get_mut()[index as usize] = Arc::clone(&next);
        for worker in worker_mains {
            let worker_main = unsafe { &mut **worker.get() };
            worker_main.handoff_queue_mains.get_mut()[index as usize] =
                Arc::clone(&next);
        }
        // The final old Arc drops its rings while Worker dispatch is stopped.
    }
}
```

Resize does not copy an unpublished reservation: at a Worker barrier such a
reservation is a violated scheduling invariant, not a packet error to hide.
All pending slots, aux and counters survive; old rings cannot be freed until
all per-main Arcs are replaced while workers are stopped. No RwLock,
ArcSwap, lazy queue allocation or per-packet resize retry is added.

## Enqueue and dequeue methods

The three public entry signatures match VPP's `vm/node/queue/indices/
thread_indices` roles; Rust slices carry `n_packets`. `NodeRuntime`
supplies only the **source** trace flag. These APIs return the count actually
accepted; the queue itself frees rejected Buffer indices. Empty input returns
zero without indexing `thread_indices[0]`.

The following private functions and four selected free functions are
fragments of **one** `handoff_arch` module. The proposed module attribute
clones that module for baseline, v3 and v4, substitutes only the ISA-specific
`slot_copy` import, and registers only the named four entries.
Private functions have ordinary `#[inline]`, not `#[march_function]`.

```rust
// hammer-runtime::handoff; VPP handoff.c:39-553.
#[hammer_component_macros::march_functions(
    entries = [handoff_queues_dequeue, buffer_enqueue_to_thread,
               buffer_enqueue_to_single_thread, buffer_enqueue_to_thread_with_aux],
    slot_copy = [copy_indices_from_slot, copy_indices_from_slot_v3,
                 copy_indices_from_slot_v4],
)]
mod handoff_arch {
    // The function bodies are shown below; no per-helper march attribute.
}
```

The generated v3/v4 entry functions and private functions have the matching
`#[target_feature]`. The three enqueue entries are selected once at init in
the process-global Buffer function owner, matching VPP's
`vlib_buffer_func_main`; `handoff_queues_dequeue` is selected once into a
local variable when each main/worker loop starts. **No selected handoff
function pointer, callback, or selector field belongs to `DataPlaneMain`,
`WorkerThread`, or an individual handoff queue.** The existing Node dispatch
function is a separate graph contract and is not changed by this ADR.
`slot_copy` is a direct import in each generated module, not a per-slot
function-pointer dispatch. This module attribute is a proposed
API, not an existing macro or compilable declaration today.

```rust
// hammer-runtime::handoff; VPP buffer_funcs.c:391-415, buffer_node.h:441-475.
// One immutable process-global owner, initialized before worker launch.
struct BufferFunctionMain {
    buffer_enqueue_to_thread:
        fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], &[u16]) -> usize,
    buffer_enqueue_to_single_thread:
        fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], u16) -> usize,
    buffer_enqueue_to_thread_with_aux:
        fn(&mut DataPlaneMain, &NodeRuntime, u32, &[u32], &[u32], &[u16]) -> usize,
}

// Public free-function facades read the selected global entry. They do not
// look up a function pointer through runtime or select ISA per packet.
fn buffer_enqueue_to_thread(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], threads: &[u16],
) -> usize;
fn buffer_enqueue_to_single_thread(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], thread: u16,
) -> usize;
fn buffer_enqueue_to_thread_with_aux(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], aux: &[u32], threads: &[u16],
) -> usize;
```

The four same-named functions inside `handoff_arch` below are the selected
implementation bodies, not these public facades. The global owner and three
facades are proposed API: the existing `DataPlaneMain` cannot own them
without violating VPP's placement, while direct calls to ISA-specific bodies
would bypass runtime CPU selection. They require explicit approval before
implementation. The owner is initialized once in the existing runtime init
phase; it owns no packet or queue state.

```rust
// VPP handoff.c:339-426. Private inline body inside handoff_arch.
#[inline]
fn handoff_enqueue_one_thread(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue_index: u32,
    indices: &[u32], thread_index: u16, with_aux: bool,
    aux: Option<&[u32]>,
) -> usize {
    if indices.is_empty() { return 0; }
    let (accepted, queue_bit, wake) = {
        let mains = runtime.handoff_queue_mains.get_mut();
        let queue_main = &mains[queue_index as usize];
        assert_eq!(queue_main.with_aux, with_aux);
        assert_eq!(with_aux, aux.is_some());
        let queue = &queue_main.queues_by_thread[thread_index as usize];
        let requested = indices.len().div_ceil(HANDOFF_SLOT_INDICES);
        let (start, slots, head) = loop {
            let tail = queue.tail.load(Ordering::Relaxed);
            let head = queue.head.load(Ordering::Acquire);
            let available = (head + queue.size as u64 - tail) as usize;
            let slots = requested.min(available);
            if slots == 0 { break (tail, 0, head); }
            if queue.tail.compare_exchange_weak(
                tail, tail + slots as u64,
                Ordering::Relaxed, Ordering::Relaxed,
            ).is_ok() {
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
                let mask = queue.size - 1;
                let first_slot = (start as usize) & mask;
                for (offset, chunk) in aux[..accepted].chunks(HANDOFF_SLOT_INDICES).enumerate() {
                    let slot = &queue.data[queue.size + ((first_slot + offset) & mask)];
                    let values = unsafe { &mut (*slot.get()).buffer_indices };
                    unsafe {
                        core::ptr::copy_nonoverlapping(
                            chunk.as_ptr(), values.as_mut_ptr(), chunk.len(),
                        )
                    };
                }
            }
            if node.trace_enabled() {
                queue.trace_stop.fetch_max(start + slots as u64, Ordering::Relaxed);
            }
            unsafe { copy_and_publish_indices_to_slots(queue, start, &indices[..accepted]) };
        }
        let dropped = indices.len() - accepted;
        if dropped != 0 {
            queue.n_dropped.fetch_add(dropped as u64, Ordering::Relaxed);
        }
        (accepted, queue_main.queue_bit, slots != 0 && start == head)
    };
    if accepted < indices.len() {
        runtime.buffer_free(&indices[accepted..]); // exactly once, same as VPP
    }
    if accepted != 0 {
        ThreadMain::global().thread_by_index(thread_index as u32)
            .expect("registered target thread")
            .handoff_pending_bmp()
            .fetch_or(queue_bit, Ordering::SeqCst);
        if wake {
            ThreadMain::global().thread_by_index(thread_index as u32)
                .expect("registered target thread").wake_for_runtime();
        }
    }
    accepted
}

// VPP handoff.c:428-468. The scratch arrays hold indices/aux, not packets.
#[inline]
fn buffer_enqueue_to_thread_inline(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], threads: &[u16], with_aux: bool,
    aux: Option<&[u32]>,
) -> usize {
    assert_eq!(with_aux, aux.is_some());
    let mut used = [0u64; 4];
    let mut mask = [0u64; 4];
    let mut grouped = [0u32; FRAME_VECTOR_CAPACITY];
    let mut grouped_aux = [0u32; FRAME_VECTOR_CAPACITY];
    let mut accepted = 0;
    let mut remaining = indices.len();
    while remaining != 0 {
        let first = (0..indices.len())
            .find(|&i| used[i / 64] & (1u64 << (i % 64)) == 0)
            .expect("remaining group has a first element");
        let thread = threads[first];
        let count = mask_compare_u16(thread, threads, &mut mask);
        let count = compress_u32(&mut grouped, indices, &mask);
        if let Some(aux) = aux {
            assert_eq!(compress_u32(&mut grouped_aux, aux, &mask), count);
        }
        accepted += handoff_enqueue_one_thread(
            runtime, node, queue, &grouped[..count], thread, with_aux,
            aux.map(|_| &grouped_aux[..count]),
        );
        for word in 0..4 { used[word] |= mask[word]; }
        remaining -= count;
    }
    accepted
}

// VPP handoff.c:469-553. These are the three selected free-function entries.
fn buffer_enqueue_to_thread(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], threads: &[u16],
) -> usize {
    assert_eq!(indices.len(), threads.len());
    assert!(!runtime.handoff_queue_mains.get_mut()[queue as usize].with_aux);
    indices.chunks(FRAME_VECTOR_CAPACITY)
        .zip(threads.chunks(FRAME_VECTOR_CAPACITY))
        .map(|(indices, threads)|
            buffer_enqueue_to_thread_inline(runtime, node, queue, indices, threads, false, None))
        .sum()
}

fn buffer_enqueue_to_single_thread(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], thread: u16,
) -> usize {
    assert!(!runtime.handoff_queue_mains.get_mut()[queue as usize].with_aux);
    indices.chunks(FRAME_VECTOR_CAPACITY)
        .map(|indices| handoff_enqueue_one_thread(
            runtime, node, queue, indices, thread, false, None))
        .sum()
}

fn buffer_enqueue_to_thread_with_aux(
    runtime: &mut DataPlaneMain, node: &NodeRuntime, queue: u32,
    indices: &[u32], aux: &[u32], threads: &[u16],
) -> usize {
    assert_eq!(indices.len(), aux.len());
    assert_eq!(indices.len(), threads.len());
    assert!(runtime.handoff_queue_mains.get_mut()[queue as usize].with_aux);
    indices.chunks(FRAME_VECTOR_CAPACITY)
        .zip(aux.chunks(FRAME_VECTOR_CAPACITY))
        .zip(threads.chunks(FRAME_VECTOR_CAPACITY))
        .map(|((indices, aux), threads)|
            buffer_enqueue_to_thread_inline(runtime, node, queue, indices, threads, true, Some(aux)))
        .sum()
}
```

The selected queue, thread and Frame layouts are registration invariants;
an invalid packet-path identity is an assertion. The source Node accounts once for
`indices.len() - accepted` as congestion drop; it must not free that suffix
again or retry it. `mask_compare_u16` and `compress_u32` preserve order
within each target thread, matching `handoff.c:441-466`.

```rust
// VPP handoff.c:140-264; private inline body inside handoff_arch.
// Frame::next_args_mut exposes writable vector/aux suffixes.
#[inline]
fn handoff_queue_dequeue_inline(
    runtime: &DataPlaneMain, main: &HandoffQueueMain, with_aux: bool,
) -> usize {
        assert_eq!(main.with_aux, with_aux);
        let queue = &main.queues_by_thread[runtime.thread_index() as usize];
        let scalar_size = runtime.nodes.frame_args_size(main.node_index)
            .expect("registered handoff destination").0;
        let mut head = queue.head.load(Ordering::Relaxed);
        let mut count = 0;
        let mut frame = None;
        while count < queue.dequeue_vector_limit {
            let slot_index = (head as usize) & (queue.size - 1);
            let slot = &queue.data[slot_index];
            if slot_first(slot).load(Ordering::Acquire) == HANDOFF_INVALID_INDEX {
                break;
            }
            if frame.as_ref().is_some_and(|f: &Box<Frame>|
                FRAME_VECTOR_CAPACITY - f.len() < 32)
            {
                runtime.put_frame_to_node(main.node_index, frame.take().unwrap())
                    .expect("registered destination accepts Frame");
            }
            let current = frame.get_or_insert_with(|| runtime.get_frame_to_node(main.node_index)
                .expect("registered destination allocates Frame"));
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
                        source.as_ptr(), destination.as_mut_ptr(), written,
                    )
                };
                written
            } else {
                let (vectors, _) = current.next_args_mut::<u32, ()>(scalar_size);
                unsafe { slot_copy(vectors, slot) }
            };
            current.set_vector_count(offset + written);
            if current.len() == FRAME_VECTOR_CAPACITY {
                runtime.put_frame_to_node(main.node_index, frame.take().unwrap())
                    .expect("registered destination accepts Frame");
            }
            slot_first(slot).store(HANDOFF_INVALID_INDEX, Ordering::Relaxed);
            head += 1;
            count += written;
        }
        if count != 0 {
            queue.n_vectors.fetch_add(count as u64, Ordering::Relaxed);
            queue.head.store(head, Ordering::Release);
            if count >= queue.dequeue_vector_limit {
                runtime.handoff_queue_pending_bmp
                    .fetch_or(main.queue_bit, Ordering::Relaxed);
            }
        }
        if let Some(frame) = frame {
            runtime.put_frame_to_node(main.node_index, frame)
                .expect("registered destination accepts Frame");
        }
        count
}

// VPP handoff.c:221-266: selected free-function entry.
fn handoff_queues_dequeue(runtime: &mut DataPlaneMain) {
        let pending = runtime.handoff_queue_pending_bmp.swap(0, Ordering::Relaxed);
        if pending == 0 { return; }
        let mut count = 0;
        let mains = runtime.handoff_queue_mains.borrow();
        if pending == u64::MAX {
            for main in mains.iter() {
                if main.with_aux {
                    count += handoff_queue_dequeue_inline(runtime, main, true);
                } else {
                    count += handoff_queue_dequeue_inline(runtime, main, false);
                }
            }
        } else {
            let mut bits = pending;
            while bits != 0 {
                let queue_index = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let main = &mains[queue_index];
                if main.with_aux {
                    count += handoff_queue_dequeue_inline(runtime, main, true);
                } else {
                    count += handoff_queue_dequeue_inline(runtime, main, false);
                }
            }
        }
        drop(mains);
        if count != 0 {
            runtime.file_poll_no_sleep_epolls = 512;
        }
}
```

`handoff_queue_dequeue_inline` takes `&DataPlaneMain`: the existing NodeMain Frame
methods use interior mutability. The directory is borrowed once per bitmap
dispatch, not once per slot; there is no Arc clone or intermediate index
vector in the hot path.
The target Frame's writable vector and aux suffixes are written **directly**.
The one allocation permitted here is VPP's normal `get_frame_to_node`
Frame acquisition. A short final slot may make the dequeue count exceed
the limit by at most 31, exactly as VPP's `while (n_deq < limit)`.
Multiple queues targeting one Node remain separate until Node scheduling.

The code blocks above are the intended algorithm, not a claim that the
current Frame/Thread APIs already have these exact signatures. In particular
the barrier-held assertion and generic `wake_for_runtime` operation are
narrow integration points at their existing runtime owners, not separate
handoff state types.

## Multi-arch copy and scheduling

The vendored VPP source gives the exact compilation boundary:

| VPP `handoff.c` | Hammer compilation |
| --- | --- |
| `221-266`, `469-553`: dequeue and three public enqueue functions use `CLIB_MULTIARCH_FN` plus registration. | One module-level `#[march_functions]` declaration names exactly these four selected entries. |
| `39-137`, `140-218`, `268-337`, `339-468`: slot reader, dequeue inline, copy-and-publish, one-thread enqueue and grouping are `static_always_inline` in each compiled variant. | Private functions are cloned with their module; they have no individual march attribute, registration or function-pointer selection. |
| `555-753`: allocation, pending-slot count/copy, resize and queue registration are under `#ifndef CLIB_MARCH_VARIANT`. | One ordinary implementation; no ISA attribute or duplicate symbol. |

`#[hammer_component_macros::march_functions]` is a **proposed** module-level
attribute, not an existing crate API. It clones the contained private
functions and the four entries, but registers and selects only those four.
For each clone, `slot_copy = [...]` binds the matching slot-prefix reader
directly within the same module. Contiguous index/aux copies use
`ptr::copy_nonoverlapping` in every variant. There is no per-slot function
pointer, `match`, or recursive public entry dispatch. The v3 and v4 modules
are gated by
`#[cfg(target_arch = "x86_64")]`; their `#[target_feature]` sets are respectively
AVX2 and AVX2+AVX-512F/VL. The v4 selector already checks the broader VPP
x86-64-v4 feature set, but does **not** imply BITALG.

This is the Rust placement of VPP's gates:

| VPP gate | Rust placement |
| --- | --- |
| `CLIB_MULTIARCH_FN` at `handoff.c:221-266,469-553` | Three candidate bodies for each of the four public entries; only these entries get selected function pointers. One module-level attribute declares the four names. |
| `#if CLIB_HAVE_VEC256` and `#if CLIB_HAVE_VEC256_MASK_LOAD_STORE` inside `static_always_inline` bodies | The v3 and v4 private clones bind different calls at expansion time. No `cfg(target_feature)` inside a clone. |
| `#if CLIB_HAVE_VEC512` guarded by `__AVX512BITALG__` | The 16-lane path belongs only to a separately BITALG-enabled native build: `#[cfg(target_feature = "avx512bitalg")]` is valid there because the **whole crate** is compiled with that feature. It is not registered by this ADR's three-candidate dispatcher. |
| `#ifndef CLIB_MARCH_VARIANT` at `handoff.c:555-753` | Layout, queue allocation, pending-slot copy, resize and registration compile once, without `target_feature` or architecture gates. |

Rust `#[cfg(target_feature = "avx2")]` is evaluated for the **whole crate**;
placing `#[target_feature(enable = "avx2")]` on a function does not change
that `cfg` inside it. `cfg(target_feature)` is appropriate only for an
explicit whole-crate native-only build, not to distinguish generated v3/v4
functions. `#[cfg(target_arch = "x86_64")]` excludes x86-specific wide
methods, copy functions and candidate bodies on other targets; the queue
layout and generic operations do not need it. `#[target_feature]` is placed
on each generated candidate and feature-specific copy function; the process
Buffer entries and loop-local dequeue pointer are selected after the CPU/OS
feature check. Rust forbids
combining it with `#[inline(always)]`; private same-ISA callees use
`#[inline]`, while the four large selected entries have no inline attribute.
Generated assembly must show native compare/copy
instructions along the selected call path, not a fallback into the ordinary
copy. This is the Rust equivalent of VPP compiling `handoff.c` separately
under each `CLIB_MARCH_VARIANT`.

The slot-copy lowering follows VPP's branch order at `handoff.c:44-136`:
test the last vector for a full 32-index fast path, otherwise scan vectors
from the front. Default v4 uses an AVX-512VL mask store for the valid prefix
and `u8::trailing_zeros`; v3 and base copy the scalar tail until the first
invalid index. A BITALG-enabled native build may instead use VPP's 16-lane
mask and `u16::trailing_zeros`, but this ADR does not select that build.
The first slot word is acquired
before reading the union vectors. The
producer must have finished the entire reservation before that acquire, and
the consumer finishes these reads before advancing the head with Release.
`wide::u32x4/u32x8` express the selected vector widths; the optional
BITALG path uses `wide::u32x16`. Hammer does not contain architecture
intrinsics. The fork's `mask_store_avx512vl` uses a native masked instruction
even when `wide` itself was compiled for the baseline target. Only the
selected v4 body calls it after checking AVX-512F/VL. Pure v3 follows VPP's
scalar tail after its 8-lane scan. The
methods accept a short destination slice if every enabled lane fits; disabled lanes are never read
or written. The fork also supplies corresponding masked zero-load methods
for VPP's `clib_memcpy_u32` tail, with the same bounds contract.

```rust
// VPP handoff.c:39-137. Default v4 has 256-bit vectors plus VL mask store.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512vl")]
#[inline]
unsafe fn copy_indices_from_slot_v4(destination: &mut [u32], slot: &UnsafeCell<HandoffQueueSlot>) -> usize {
    assert!(destination.len() >= 32);
    assert_ne!(slot_first(slot).load(Ordering::Acquire), HANDOFF_INVALID_INDEX);
    let vectors = unsafe { &(*slot.get()).as_u32x8 };
    let invalid = wide::u32x8::splat(HANDOFF_INVALID_INDEX);
    let last = vectors[3];
    let output = destination.as_mut_ptr().cast::<wide::u32x8>();
    if unsafe { last.is_equal_mask_avx512vl(invalid) } == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), last);
        }
        return 32;
    }
    let mut count = 0usize;
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn copy_indices_from_slot_v3(destination: &mut [u32], slot: &UnsafeCell<HandoffQueueSlot>) -> usize {
    assert!(destination.len() >= 32);
    assert_ne!(slot_first(slot).load(Ordering::Acquire), HANDOFF_INVALID_INDEX);
    let source = unsafe { &(*slot.get()).buffer_indices };
    let vectors = unsafe { &(*slot.get()).as_u32x8 };
    let invalid = wide::u32x8::splat(HANDOFF_INVALID_INDEX);
    let last = vectors[3];
    let output = destination.as_mut_ptr().cast::<wide::u32x8>();
    if unsafe { last.is_equal_mask_avx2(invalid) } == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), last);
        }
        return 32;
    }
    let mut count = 0usize;
    loop {
        let vector = vectors[count / 8];
        if unsafe { vector.is_equal_mask_avx2(invalid) } != 0 { break; }
        unsafe { core::ptr::write_unaligned(output.add(count / 8), vector) };
        count += 8;
    }
    while count < 32 && source[count] != HANDOFF_INVALID_INDEX {
        destination[count] = source[count];
        count += 1;
    }
    count
}

#[inline(always)]
fn copy_indices_from_slot(destination: &mut [u32], slot: &UnsafeCell<HandoffQueueSlot>) -> usize {
    use wide::CmpEq;
    assert!(destination.len() >= 32);
    assert_ne!(slot_first(slot).load(Ordering::Acquire), HANDOFF_INVALID_INDEX);
    let source = unsafe { &(*slot.get()).buffer_indices };
    let vectors = unsafe { &(*slot.get()).as_u32x4 };
    let invalid = wide::u32x4::splat(HANDOFF_INVALID_INDEX);
    let last = vectors[7];
    let output = destination.as_mut_ptr().cast::<wide::u32x4>();
    if last.simd_eq(invalid).to_bitmask() == 0 {
        unsafe {
            core::ptr::write_unaligned(output.add(0), vectors[0]);
            core::ptr::write_unaligned(output.add(1), vectors[1]);
            core::ptr::write_unaligned(output.add(2), vectors[2]);
            core::ptr::write_unaligned(output.add(3), vectors[3]);
            core::ptr::write_unaligned(output.add(4), vectors[4]);
            core::ptr::write_unaligned(output.add(5), vectors[5]);
            core::ptr::write_unaligned(output.add(6), vectors[6]);
            core::ptr::write_unaligned(output.add(7), last);
        }
        return 32;
    }
    let mut count = 0usize;
    loop {
        let vector = vectors[count / 4];
        if vector.simd_eq(invalid).to_bitmask() != 0 { break; }
        unsafe { core::ptr::write_unaligned(output.add(count / 4), vector) };
        count += 4;
    }
    while count < 32 && source[count] != HANDOFF_INVALID_INDEX {
        destination[count] = source[count];
        count += 1;
    }
    count
}
```

The selected body must call the fork's native methods only after its CPU
feature check. Default v4's 8-lane AVX-512VL comparison returns the
instruction `u8` mask directly; v3's 8-lane AVX2 comparison uses vector
compare plus movemask. The fork's 16-lane AVX-512F comparison returns a
`u16` mask, but this is only useful to the separately BITALG-enabled path.
Ordinary `wide` arithmetic still follows the dependency's compile-time target:
a `#[target_feature]` on Hammer does not recompile the dependency. Generated
assembly must show the selected body's expected vector compare and mask
instructions before it is described as a full v3/v4 implementation; otherwise
retain the ordinary candidate. The copy-and-publish helper copies the 31
non-atomic first-slot words and aux values through `ptr::copy_nonoverlapping`,
then stores the first index with Release. There
is one helper operation, not a separate copy-only slot helper.
`#[inline(always)]` is for bounded slot-read helpers, not
the four large selected entry bodies. Empty/full, partial slot,
aux and trace remain the unlikely branches.

## Main-loop and File integration

VPP selects `handoff_queues_dequeue` **once per loop invocation** at
`main.c:1444-1452`, then does RPC, worker barrier check, pending-bitmap
test/dequeue, worker callbacks, File poll, and graph dispatch in that order
at `main.c:1496-1625`. Dequeue obtains the pending bits with an exchange,
creates direct destination Frames, and re-arms a queue bit when its dequeue
budget is reached (`handoff.c:221-266`). It does not dispatch the target Node
itself. Its Frames run at the ordinary graph-dispatch point **after** File
poll. `run_ready_nodes` must therefore stop draining old `HandoffFrame`
objects; a second dequeue after File poll or inside graph dispatch is not
VPP's ordering.

```rust
// hammer-runtime::main_loop; VPP main.c:1444-1452,1496-1625.
fn data_plane_main_loop(worker: &WorkerThread) -> i32 {
    let handoff_queues_dequeue = select_handoff_queues_dequeue();
    loop {
        // Existing worker RPC handling precedes this barrier check.
        worker.check_barrier();
        let main = worker.main_mut();
        if main.handoff_queue_pending_bmp.load(Ordering::Relaxed) != 0 {
            handoff_queues_dequeue(main);
        }
        main.run_worker_main_loop_callbacks();
        main.poll_file_when_due();
        main.dispatch_ready_graph_nodes();
        main.advance_node_timers();
        main.finish_main_loop_iteration();
    }
}
```

This is a control-flow sketch: `worker.main_mut`, `poll_file_when_due`, and
the dispatch names stand for the existing runtime operations, not proposed
public APIs. The loop-local selector is the only selected dequeue pointer;
`DataPlaneMain` has none. The current Hammer worker loop instead calls
`schedule_remote_interrupts` before File readiness and drains old handoff
frames from `run_ready_nodes`. Both paths are removed in the migration.

VPP's `file.c:140-196` first applies its busy/polling/interrupt checks. If
`file_poll_no_sleep_epolls != 0`, it decrements that counter, **still polls
File nonblocking**, and does not sleep. Otherwise it publishes
`thread_sleeps = true` with SeqCst, rechecks
`handoff_queue_pending_bmp` with SeqCst, and on a pending bit clears sleep
and enters a nonblocking poll. Only with no pending bit does it wait, bounded
by the next main timer and the File maximum wait. Producer's SeqCst pending
bit OR (`handoff.c:420`) pairs with that recheck; an empty-to-nonempty
transition wakes a sleeping target. File poll clears sleeping/wakeup state
after wait and dispatches File completions. The next loop exchanges the
pending bits and creates Frames; File readiness never consumes handoff slots.
`file_poll_no_sleep_epolls` is distinct from existing
`file_poll_skip_loops`: the former suppresses sleep, not File polling.

Hammer's thread-zero `run_main_until` is an async Process/File loop, whereas
VPP runs the same packet-graph loop on thread zero. Because queue registration
allocates a ring for thread zero as well, the async loop must select its own
local dequeue entry on entry, check and drain its bitmap **before** async
File/Process waiting, and dispatch resulting Frames through thread zero's
normal graph path. It must end the `RefCell` mutable borrow before `.await`.
Its readiness selection must include the existing runtime wake source;
after advertising sleep, it rechecks the bitmap before waiting, just as the
worker File path does. Until that integration exists, a producer must not
target thread zero. Dropping thread-zero packets silently or claiming the
worker loop covers thread zero is not acceptable.

The pending bitmap is a scheduling hint, not payload publication; the slot's
first-word Release/Acquire is payload publication. The consumer's Release
head and producer's Acquire head protect slot reuse. The tail CAS only
reserves disjoint slots and can remain Relaxed. The sleeping flag and bitmap
recheck prevent missed wakeups; neither substitutes for slot publication.

## Handoff trace contract

ADR-0051 owns `TraceMain`, `HandoffTrace`, the `handoff-trace` Node and
`DataPlaneMain::add_trace<T>`. This ADR does not introduce another trace
owner, record-copy path or special handoff trace API. The queue has only
the VPP `trace_stop` watermark and the existing source `NodeRuntime` trace
flag. Source and destination do different work:

1. The **business source Node** uses its own VPP trace point and record
   layout. IP full-reassembly handoff checks both the Node Frame trace bit
   and each Buffer's traced flag inside its owner-selection loop
   (`ip4_full_reass.c:1927-1943`). VNET `worker-handoff` instead calls its
   trace-frame routine after filling all thread indices, guarded by the
   Node trace bit (`vnet/handoff.c:146-150`). In either case, the Node writes
   directly into its own `add_trace<T>` record and registers that record's
   formatter through `Node::node_trace_formatter`; neither path calls
   `trace_buffer` merely to transfer a packet.
2. The **queue producer** tests the source Node's `trace_enabled()` once for
   each accepted group. If true, it advances `trace_stop` to the end of the
   accepted slot range before Release-publishing the first Buffer index
   (`vlib/handoff.c:389-406`). It does not inspect or copy any Buffer's
   trace handle. Rejected indices do not extend the watermark.
3. The **queue consumer** tests `trace_stop > head` for each acquired slot
   and sets the direct destination Frame's trace flag before copying that
   slot's indices (`vlib/handoff.c:169-186`). It never scans the slot's
   Buffers for trace flags and never allocates a trace record. A watermark
   may conservatively mark a Frame containing untraced Buffers; the target
   Node's per-Buffer guard filters those out.
4. The **destination Node**, when it first calls its ordinary
   `add_trace<T>` for a traced Buffer, lets the existing ADR-0051 method
   detect the previous thread in the Buffer handle. That method creates a
   local trace pool entry through `handoff-trace`, writes the existing
   `HandoffTrace { prev_thread, prev_trace_index }` record, rebinds the
   Buffer/chain handle, then lends the destination Node its own record.
   The old thread's trace pool entry remains intact. There is no eager
   handoff record creation in dequeue and no cross-thread record copy.

The enqueue and dequeue Rust bodies above show the exact watermark update
and Frame flag assignment; they are the implementation points. The existing
ADR-0051 `DataPlaneMain::add_trace<T>` body handles the later cross-thread
transition, so no second queue-level trace method is proposed.

`trace_stop` is an absolute slot sequence, not a boolean or a per-packet
trace handle. Its conservative marking may span another producer's earlier
reserved slots, as in VPP. The slot's Release/Acquire publication orders
the producer's watermark update before the consumer reads it; Relaxed is
therefore sufficient for the watermark itself. Resize preserves the pending
watermark relative to copied slots; it must not reset the target Frame flag
for an in-flight traced Buffer. This replaces ADR-0051's earlier proposed
receiver-side slot scan, not its trace pool or `add_trace<T>` contract.

## Business handoff Nodes

Queue registration belongs to the plugin owning the destination Node.
Generic runtime handoff never looks up an IP tuple, TCP connection,
interface hash or reassembly owner. VPP's `worker-handoff`
(`vnet/handoff.c:99-242`) is an internal Node: choose one destination thread
per input Buffer, optionally trace it, call `vlib_buffer_enqueue_to_thread`
**once per Frame**, count `frame_len - n_enq` as congestion drops, and return
`frame_len`. The queue owns or frees every Buffer after the call. Its index
is kept in the owning VNET Main, initialized invalid and assigned after the
destination Node exists. Its queue targets `ethernet-input`; this ADR does
not add a generic runtime intermediary Node or pretend Hammer's L3 TUN is
that L2 feature. This interface Node calls `worker_handoff_trace_frame`
after filling the entire destination-thread array and before enqueue
(`vnet/handoff.c:146-150`). That is **not** the trace timing of the IP
reassembly handoff Nodes.

Current Hammer business callers are IP reassembly and TCP input. VPP
`ip4_full_reass.c:1242-1279,1375-1457,1604-1616,1886-2038` sets
`ip.reass.owner_thread_index` in the reassembly Node and sends to a distinct
handoff Node for each entry path. That Node reads the owner from Buffer
metadata and batches the entire Frame into the queue targeting its
corresponding **reassembly** Node, not `ip4-input`. A completed packet sent
back to its original thread re-enters reassembly and takes the normal
pass-through next. IPv6 follows the same ownership pattern in
`ip6_full_reass.c` with its own queue.

```rust
// hammer-plugin-ip::reassembly; VPP ip4_full_reass.c:1242-1279,
// 1604-1616,1886-2038 and ip6_full_reass.c equivalents.
#[node_next]
enum Ip4ReassemblyNext {
    Input,
    Handoff, // "ip4-full-reassembly-handoff"
    Drop,
}

#[graph_node(
    graph = ip, name = "ip4-full-reassembly-handoff",
    init = register_ip4_reassembly_handoff, role = internal,
)]
struct Ip4ReassemblyHandoffNode;

#[graph_node(
    graph = ip, name = "ip6-full-reassembly-handoff",
    init = register_ip6_reassembly_handoff, role = internal,
)]
struct Ip6ReassemblyHandoffNode;

// IPv6 declares its own Node and next arc. Local, feature and custom entry
// variants get separate nodes and queues only when those entries exist.
struct IpReassemblyMain {
    // Existing owner fields omitted.
    ip4_handoff_queue_index: u32,
    ip6_handoff_queue_index: u32,
}

#[repr(u16)]
enum IpReassemblyHandoffError {
    CongestionDrop,
}

#[repr(C)]
#[derive(zerocopy::KnownLayout, zerocopy::FromBytes, zerocopy::IntoBytes,
         zerocopy::Immutable)]
struct IpReassemblyHandoffTrace {
    next_worker_index: u32,
}

fn format_ip4_reassembly_handoff_trace(bytes: &[u8]) -> String {
    let (trace, _) = IpReassemblyHandoffTrace::ref_from_prefix(bytes)
        .expect("IP handoff trace has its registered layout");
    format!("ip4-full-reassembly-handoff: next-worker {}", trace.next_worker_index)
}

fn format_ip6_reassembly_handoff_trace(bytes: &[u8]) -> String {
    let (trace, _) = IpReassemblyHandoffTrace::ref_from_prefix(bytes)
        .expect("IP handoff trace has its registered layout");
    format!("ip6-full-reassembly-handoff: next-worker {}", trace.next_worker_index)
}

fn register_ip4_reassembly_handoff(runtime: &DataPlaneMain) -> RuntimeResult<NodeId>;
fn register_ip6_reassembly_handoff(runtime: &DataPlaneMain) -> RuntimeResult<NodeId>;

impl Node for Ip4ReassemblyHandoffNode {
    fn process(
        runtime: &mut DataPlaneMain, node: &mut NodeRuntime, frame: &mut Frame,
    ) -> usize {
        let indices = frame.vector_args();
        let mut threads = [0u16; FRAME_VECTOR_CAPACITY];
        for (position, &index) in indices.iter().enumerate() {
            threads[position] = reassembly_owner_thread(runtime.buffer(index));
            if unlikely(node.trace_enabled() && runtime.buffer(index).trace_handle().is_some()) {
                if let Some(trace) = runtime.add_trace::<IpReassemblyHandoffTrace>(node, index) {
                    trace.next_worker_index = u32::from(threads[position]);
                }
            }
        }
        let accepted = buffer_enqueue_to_thread(
            runtime, node, IpReassemblyMain::global().ip4_handoff_queue_index,
            indices, &threads[..indices.len()],
        );
        runtime.record_current_node_error_count(
            IpReassemblyHandoffError::CongestionDrop,
            (indices.len() - accepted) as u64,
        ).expect("handoff Node registered its congestion counter");
        indices.len()
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip4_reassembly_handoff_trace)
    }
}

impl Node for Ip6ReassemblyHandoffNode {
    fn process(
        runtime: &mut DataPlaneMain, node: &mut NodeRuntime, frame: &mut Frame,
    ) -> usize {
        let indices = frame.vector_args();
        let mut threads = [0u16; FRAME_VECTOR_CAPACITY];
        for (position, &index) in indices.iter().enumerate() {
            threads[position] = reassembly_owner_thread(runtime.buffer(index));
            if unlikely(node.trace_enabled() && runtime.buffer(index).trace_handle().is_some()) {
                if let Some(trace) = runtime.add_trace::<IpReassemblyHandoffTrace>(node, index) {
                    trace.next_worker_index = u32::from(threads[position]);
                }
            }
        }
        let accepted = buffer_enqueue_to_thread(
            runtime, node, IpReassemblyMain::global().ip6_handoff_queue_index,
            indices, &threads[..indices.len()],
        );
        runtime.record_current_node_error_count(
            IpReassemblyHandoffError::CongestionDrop,
            (indices.len() - accepted) as u64,
        ).expect("handoff Node registered its congestion counter");
        indices.len()
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_ip6_reassembly_handoff_trace)
    }
}
```

This describes the Node body and registration contract, not a claim that the
current `#[graph_node]` macro accepts an empty struct or that those owner
fields exist today. Queue allocation runs after the destination NodeIds have
been registered, in the existing startup/barrier graph registration scope;
`handoff_alloc_queues` returns each index, stored once in `IpReassemblyMain`.
The source reassembly Node sets the existing `NetworkReassemblyOpaque` owner
field to a **runtime thread index** before choosing `Handoff`. The current
`handoff_source_worker` interpretation must be replaced at its owner, not
reinterpreted in runtime. The handoff Node neither looks up the fragment
directory nor borrows another worker's pool. Both families need a real
handoff Node, a registered fixed-layout trace formatter and `CongestionDrop`
node error. VPP `ip4_full_reass.c:1927-1943` checks the Node trace bit **and**
the Buffer traced bit in the per-Buffer owner-selection loop, then calls
`vlib_add_trace` and writes only `next_worker_index`. Hammer uses the existing
`NodeRuntime::trace_enabled`, `Buffer::trace_handle` and
`DataPlaneMain::add_trace::<T>` at that same point; `add_trace` lends the
record directly, with no temporary payload or packet copy. The Node's
`node_trace_formatter` registers that same record layout as ADR-0051
requires. This is a middle Node: it does not override `trace_supported`,
never calls `trace_buffer` or consumes a source trace quota. Local,
feature and custom entries get distinct queue indices only with their real
graph entries, as in VPP. Queue-full is a counted packet drop, not a returned
`RuntimeError`; the queue frees rejected indices exactly once.

VPP `tcp_inlines.h:282-367` returns
`SESSION_LOOKUP_RESULT_WRONG_THREAD` for an established TCP session on
another thread, and `tcp_input.c:2729-2745` maps it to TCP's drop next/error.
It registers **no TCP-input handoff Node**. Remove Hammer's three
`handoff_index` branches in `transport/tcp/src/input.rs`; `tcp4-input`,
`tcp6-input` and both nolookup variants classify a wrong owner as TCP's
existing wrong-thread packet error and drop. Ingress must keep each flow
on its owner worker. Any new L3 ingress steering belongs to the IP/device
feature before TCP, with its own explicit Node and queue, not a hidden TCP
retry or runtime callback. Removing the current cross-worker rescue changes
multi-worker behavior, so the migration must verify owner-affine ingress
before that branch is deleted.

## Migration and verification

| Owner | Required change |
| --- | --- |
| `hammer-component-macros` | Add the proposed module-level `#[march_functions]` attribute: clone private inline functions with their module but register only its four named entries; generated x86 candidates are target-feature gated. |
| `hammer-runtime` | Delete old handoff containers, `HandoffFrame`, `handoff_index`/`handoff_frame`, remote per-Node handoff interrupts and their scheduling paths. Add only the queue directory, bitmap and no-sleep count to `DataPlaneMain`; put the three selected enqueue pointers in the process-global Buffer function owner and select dequeue into each loop's local variable. Worker and thread-zero loops drain before File/Process wait, then dispatch direct destination Frames. File polling performs the sleeping-flag/bitmap recheck and honors the separate no-sleep count. |
| `third_party/wide` | Keep the local fork and Cargo override for its existing checked compare-mask and masked-store methods used by the slot reader. Add no generic copy or unaligned load/store API. |
| IP reassembly | Register IPv4 and IPv6 handoff Nodes and their queue indices after the target reassembly Nodes. Reassembly writes the owner thread into Buffer opaque and uses a Handoff next arc. The handoff Node makes one batch enqueue call per Frame, traces selected worker, records one `CongestionDrop` batch count and returns the original Frame length; no extra source-side free. Remove `IpReassemblyHandoff`'s old target-Node/worker carrier and all three per-packet enqueue sites. |
| TCP input | Delete all three `handoff_index` sites and the `handoff_worker` retry path. Wrong-thread lookup uses the existing TCP packet-error/drop contract; no TCP handoff queue/Node is registered. Verify ingress worker affinity separately. |
| Old handoff errors | Remove the retired runtime queue-full/`NotConfigured` `Result` variants with their callers. The IP Node's typed `CongestionDrop` is a VPP-style packet counter, not a new control-plane error. |
| ADR-0007 / ADR-0051 | Replace old handoff queue-full retry ownership and receiver trace wording with this queue's drop and `trace_stop` contract. |

| Verification case | Observable requirement |
| --- | --- |
| Registration and ownership | Two queues to the same Node retain distinct indices; every main has the same directory order and every destination has its own ring/bitmap. |
| Index and aux copy | 1, 31, 32, 33 and 256 values preserve order; partial slot has invalid padding; aux matches its Buffer index; no packet payload copy or intermediate dequeue vector. |
| MPSC publication | Two producers may reserve out of order; consumer stops at unpublished first slot and later sees all indices/aux after release/acquire, with no duplicate release. |
| Full queue | Only the accepted prefix transfers; rejected suffix is freed exactly once and source Node's congestion count equals the rejection count. |
| Dequeue, trace and File | Direct Frame receives indices; trace watermark applies to that Frame; budget re-arms pending; queue 64 forces all-queue scan; adaptive File wait cannot lose a new queue bit. |
| Handoff trace | Source business record uses its own Node's VPP timing and formatter; queue watermark marks an accepted mixed traced/untraced slot without scanning Buffers; target Frame has trace bit; only traced Buffers call target `add_trace<T>`, which first writes `HandoffTrace` with the previous thread/index and leaves the source pool unchanged. |
| Main-loop order | RPC and worker barrier precede one pending-bitmap dequeue; File readiness follows it; the destination Node runs at normal graph dispatch. Thread-zero async loop also drains before await or rejects targets until implemented; no second drain in `run_ready_nodes`. |
| IP handoff Nodes | IPv4/IPv6 queue indices target their reassembly Nodes; one Frame yields one batch enqueue, only traced Buffers receive worker trace, congestion count equals the queue-rejected suffix, and a sendout handoff re-enters reassembly before its ordinary next. |
| TCP wrong thread | Normal and nolookup input paths count/drop a wrong-owner packet without remote enqueue; an owner-affine ingress configuration keeps established flows on the right worker. |
| Resize | Pending index/aux prefix, trace watermark and counts survive; insufficient capacity leaves every old queue unchanged; barrier prevents old-ring use after replacement. |
| Multi-arch | Three enqueue pointers exist only in the process-global Buffer function owner; dequeue pointer exists only in each loop's local variable; none exists on `DataPlaneMain`. Private helpers have no march attribute or registration; unsupported x86 candidates are absent or unselected; emitted v3/v4 calls use the promised instructions. |
| Masked primitives | Zero mask does not access memory; every selected lane is in bounds and every unselected lane remains untouched; default v4 uses AVX-512VL 8-lane masks, v3 uses a scalar tail, and the BITALG-only 16-lane path is not selected by default. |

The handoff path remains a design. No compilation, tests, static checks or CI
were run for the local `wide` fork or this ADR.
